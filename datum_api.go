package main

// The datum API (per-datum statistics): listing, inspection, and restart
// of a job's datum set. The engine itself lives in datum_engine.go.

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"sandman/client"
)

// ---- the datum API ----

// writeStatsCommit publishes a job's per-datum records as a commit on the
// output repo's "stats" branch: one file per datum, named by its index,
// containing the record. A stats-enabled pipeline maintains the stats
// branch as an additional ordinary output branch — one commit per job,
// distinct and queryable separately from the normal output tree — and its
// change propagates through the DAG so downstream pipelines can consume
// it like any branch and run on it.
func (d *daemon) writeStatsCommit(pl pipelineRec, dedup map[string]datumState, datums []datum) string {
	m := d.repoLock(pl.Pipeline.Name)
	m.Lock()
	defer m.Unlock()
	cm, err := d.store.StartCommit(pl.Pipeline.Name, "stats", "")
	if err != nil {
		return ""
	}
	for i, dt := range datums {
		b, err := json.Marshal(dedup[dt.ID])
		if err != nil {
			continue
		}
		if err := d.store.OverwriteFile(cm.ID, fmt.Sprintf("%06d", i), b); err != nil {
			d.store.FinishCommit(cm.ID, "", true)
			return ""
		}
	}
	fin, err := d.store.FinishCommit(cm.ID, "", false)
	if err != nil {
		return ""
	}
	return fin.ID
}

// restartDatum aborts a datum's current processing and starts it over
// from scratch: the running container is killed, the datum's record is
// reset, and the worker re-queues it, so the next status observation
// shows it running with a strictly later start time. The restart must
// land even when it races the datum's pick-up — the worker-registry entry
// can lag the datum's "running" state and the container may still be
// starting — so both the lookup and the kill are retried, and the job
// still completes successfully with exactly one output commit.
func (d *daemon) restartDatum(jobID, datumID string) error {
	d.jobsMu.Lock()
	rj, ok := d.running[jobID]
	var jx *jobExec
	if ok {
		jx = rj.jx
	}
	d.jobsMu.Unlock()
	if !ok || jx == nil {
		// not running, or still queued behind the pipeline gate (its
		// execution context is not built yet)
		return fmt.Errorf("job %q is not running", jobID)
	}
	// the worker registry entry can lag the datum record's "running"
	// state by a few hundred ms (the record is written on pick-up, the
	// worker's jx entry just after): retry the lookup briefly so a
	// restart issued the instant a datum appears never 400s on the gap
	var cname string
	for i := 0; i < 25; i++ { // ~2.5s
		jx.workersMu.Lock()
		for _, ws := range jx.workers {
			if ws.Datum == datumID {
				cname = ws.Cname
				break
			}
		}
		jx.workersMu.Unlock()
		if cname != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if cname == "" {
		return fmt.Errorf("datum %q is not currently being processed", datumID)
	}
	jx.requestRestart(datumID)
	// the container may still be starting (the record is written on
	// pick-up, before docker run creates it): retry the kill until it
	// lands
	for i := 0; i < 50; i++ { // ~10s
		if d.runner.Kill(cname) == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("datum %q container %s did not terminate", datumID, cname)
}

// statsEnabled reports whether the pipeline currently records per-datum
// statistics (the one-way flag).
func (d *daemon) statsEnabled(pipeline string) bool {
	rec, err := d.loadPipeline(pipeline)
	return err == nil && rec.Pipeline.EnableStats
}

// datumStateRank orders the listing: failed first, skipped last — a
// failed datum leads regardless of its input position, and processed
// datums come before skipped ones.
func datumStateRank(outcome string) int {
	switch outcome {
	case stateFailed:
		return 0
	case stateRecovered:
		return 1
	case stateSuccess:
		return 2
	default: // skipped, or an in-flight datum
		return 3
	}
}

func datumInfo(id string, st datumState) client.DatumInfo {
	info := client.DatumInfo{ID: id}
	state := st.Outcome
	if state == "" {
		state = stateRunning // picked up, not yet settled
	}
	info.State = state
	for _, f := range st.InputFiles {
		info.InputFiles = append(info.InputFiles, client.DatumFile{Path: f.Path, Hash: f.Hash})
	}
	for _, f := range st.Files {
		info.OutputFiles = append(info.OutputFiles, client.DatumFile{Path: f.Path, Hash: f.Hash})
	}
	info.ProcessTime = st.ProcessTime
	info.Started = st.Started
	info.Finished = st.Finished
	info.Worker = st.Worker
	info.Reason = st.Reason
	return info
}

// listDatums serves a job's datum listing: the job's datum set with each
// datum's record — the listing is complete and queryable both during and
// after execution, so the in-progress datum appears before the job
// completes — state-ordered (failed < recovered < success < skipped, then
// id) and paginated by page size and zero-based page index, always
// reporting TotalPages and the served Page. A page index at or beyond the
// total page count errors rather than returning an empty page, and state
// ordering is stable across pages so no datum straddles or duplicates;
// limit 0 requests everything. The listing includes both the datums
// processed in this job and the datums carried from a previous job that
// were skipped because their input was unchanged, ordered processed before
// skipped with a distinguishable state — never omitted, so per-datum
// history carries across jobs. Without statistics the datums are listable
// by identity only.
func (d *daemon) listDatums(jobID string, limit, page int) (client.DatumPage, error) {
	rec, err := d.loadJobRec(jobID)
	if err != nil {
		return client.DatumPage{}, err
	}
	dedup := d.loadDedup(rec.Pipeline)
	detailed := d.statsEnabled(rec.Pipeline)

	datums := append([]string{}, rec.DatumIDs...)
	stateOf := func(id string) string {
		if s, ok := rec.DatumStates[id]; ok {
			return s
		}
		return dedup[id].Outcome
	}
	rank := map[string]int{}
	for _, id := range datums {
		rank[id] = datumStateRank(stateOf(id))
	}
	sort.SliceStable(datums, func(i, j int) bool {
		if rank[datums[i]] != rank[datums[j]] {
			return rank[datums[i]] < rank[datums[j]]
		}
		return datums[i] < datums[j]
	})

	n := len(datums)
	totalPages := 1
	if limit > 0 {
		totalPages = (n + limit - 1) / limit
		if n == 0 {
			totalPages = 0
		}
	}
	if limit > 0 && n > 0 && page >= totalPages {
		return client.DatumPage{}, fmt.Errorf("page %d out of range: %d page(s)", page, totalPages)
	}
	start, end := 0, n
	if limit > 0 {
		start = page * limit
		if end > start+limit {
			end = start + limit
		}
	}
	out := client.DatumPage{TotalPages: totalPages, Page: page}
	for _, id := range datums[start:end] {
		if !detailed {
			// without statistics the datums are listable by identity only
			out.Datums = append(out.Datums, client.DatumInfo{ID: id})
			continue
		}
		st, ok := dedup[id]
		if !ok {
			// queued: part of the job's datum set, not yet picked up by a
			// worker
			out.Datums = append(out.Datums, client.DatumInfo{ID: id, State: stateRunning})
			continue
		}
		info := datumInfo(id, st)
		if s, ok := rec.DatumStates[id]; ok {
			info.State = s
		}
		out.Datums = append(out.Datums, info)
	}
	return out, nil
}

// inspectDatum returns one datum's record; without per-datum statistics no
// per-datum detail exists and the inspection errors.
func (d *daemon) inspectDatum(jobID, datumID string) (client.DatumInfo, error) {
	rec, err := d.loadJobRec(jobID)
	if err != nil {
		return client.DatumInfo{}, err
	}
	if !d.statsEnabled(rec.Pipeline) {
		return client.DatumInfo{}, fmt.Errorf("per-datum statistics are not enabled for pipeline %q", rec.Pipeline)
	}
	dedup := d.loadDedup(rec.Pipeline)
	st, ok := dedup[datumID]
	if !ok {
		return client.DatumInfo{}, notFound("datum %q not found in job %q", datumID, jobID)
	}
	info := datumInfo(datumID, st)
	if s, ok := rec.DatumStates[datumID]; ok {
		info.State = s
	}
	return info, nil
}
