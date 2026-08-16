package main

// Job execution: runJob and the execution-backend seam — the
// containerRunner/processRunner that run one pipeline transform, and the
// datum execution glue. The rest of the control plane lives in
// pipeline.go (spec validation + CRUD) and jobs.go (lifecycle).

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"sandman/client"
	"sandman/internal/store"
)

// outputBranch returns the branch a pipeline's output commits land on
// (default "master"). A pipeline triggers only on commits on the branches
// it watches, and writes its output commits to its configured output
// branch. Because the output lands on a possibly non-watched branch, a
// downstream pipeline that watches the default branch does not run until
// that branch is promoted onto the output commit (via CreateBranch
// retargeting) — scheduling keys off branch pointers rather than commit
// existence.
func outputBranch(pl pipelineRec) string {
	if pl.Pipeline.OutputBranch == "" {
		return defaultBranch
	}
	return pl.Pipeline.OutputBranch
}

// runJob coordinates one job: enumerate the input sides' datums, take
// their cartesian product, run the datums with a bounded worker pool, merge
// their outputs into the single output commit, and record the per-datum
// outcomes in the pipeline's dedup table. heads is the input pairing — one
// commit per side, empty where a side has no head (a lone cross side whose
// partner has no head yet still gets a job; its cross contributes no
// datums).
// Datum dedup: a datum whose content hash is unchanged from a previous
// successful run is skipped rather than re-executed — the job still
// produces its output commit but does not re-run the transform for
// unchanged datums, so the job completes promptly. Skip detection compares
// the datum's final content against the last successful processing, not
// the sequence of intermediate changes, so a file deleted and re-added
// with byte-identical content is skipped, not reprocessed; the skip is
// recorded on the job, never on the shared record, so the record keeps its
// last successful outcome for later jobs to skip on. A pipeline configured
// to reprocess on every job instead re-executes all of its datums on each
// job, regenerating each datum's output from the data current at
// processing time rather than carrying forward prior output.
// Failure propagation: when a job's upstream stage failed, the propagated
// failure marks this stage failed without executing, and its empty output
// commit keeps the DAG continuous so the failure reaches every downstream
// stage and the flush can walk the chain; the terminal state is durably
// recorded before the output commit finishes. A failed output is still a
// revision, so each downstream stage is triggered and fails in turn, and
// flushing the failing commit returns every stage's job with its terminal
// failed state rather than erroring.
// One-commit contract: each finished input commit triggers exactly one job
// per pipeline, and a job that recursively copies whole input directories
// (even a full repository directory, spanning many files) completes to
// exactly one output commit serving the full cumulative branch state — the
// file added by an earlier commit stays readable with its exact content
// from a later job's output; no commit merges with another or is dropped.
// Whole-job deadline: a job whose cumulative execution exceeds its
// configured job timeout is killed at the boundary — its active containers
// are killed and it settles as killed, never a plain failure — with the
// recorded start-to-finish duration equal to the configured timeout; the
// job stays observable while running even though it is destined to be
// killed.
// Cross resolution: when a cross member is a union of two branches of one
// repository, the union's branches are resolved and keyed by branch
// (unionBranchKey) so they stay distinct in the views, and the union
// member contributes its own merged datums even though it has no single
// head commit; the cross's other legs resolve to the current head of their
// branch at job-creation time, not to a provenance-derived commit, and
// exactly one job is created for the flush.
// Output-repo survival: when a running pipeline's output repository is
// force-deleted, the job must not silently resurrect the repo — the
// output-repo existence check fails the job and marks the pipeline failed
// with a reason, and the scheduler survives (no crash loop) so a later
// healthy pipeline on the same input runs to completion; recovery requires
// operator action: recreate the repository, then update the pipeline.
// Placement: a pipeline may require its work to run on a host bearing a
// placement label, never enumerating a host address or identity; a job
// completes only once a host bearing the required label is available.
// Until such a host has registered, the job waits — its record was saved
// at trigger time, so the pending job and its input revision are durable
// across the outage — and the pipeline surfaces the outage as the crashed
// state with a recorded reason instead of hanging. When a host bearing
// the label later registers, the pending work re-places automatically and
// the same job completes producing exactly one output commit with correct
// content, and the pipeline returns from crashed to running without
// recreation or manual re-trigger.
func (d *daemon) runJob(pl pipelineRec, heads []client.Commit, id, propagated string, pre *jobRec, rj *runningJob) {
	sides := inputSides(pl.Pipeline.Input)
	for i := range sides {
		if sides[i].Name == "" {
			sides[i].Name = sides[i].Repo
		}
	}
	dir := d.jobDir(id)
	outDir := filepath.Join(dir, "out")
	// the defers must register before any failure path: spawnJob already
	// put the job in the running map, and an early return without them
	// leaks the running handle (a later cancel waits 30s and errors) and
	// the standby activation count (the pipeline never returns to standby)
	defer d.unregisterRunning(id, rj)
	// a standby pipeline returns to standby once its work settles; the
	// defer covers every terminal path (success, failure, killed)
	defer d.standbySettle(pl.Pipeline.Name)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return
	}

	rec := pre
	if rec == nil {
		rec = newJobRec(pl, heads, id)
	}
	d.saveJob(rec)

	// Per-pipeline serialization: one job at a time, in spawn
	// order — the record is already saved, so a queued job is visible and
	// cancellable. A cancel that arrived while queued settles the job
	// killed without doing any work and passes the slot on.
	gate := d.jobGate(pl.Pipeline.Name)
	if !gate.enter(rj) {
		rec.State = stateKilled
		rec.Reason = reasonJobCancelled
		rec.Finished = now()
		if _, err := os.Stat(filepath.Join(dir, "job.json")); err == nil {
			// the record may have been deleted while the job was queued
			// (deleteJob removes the whole job directory); never resurrect
			d.saveJob(rec)
		}
		return
	}
	defer gate.release()

	// the slot is ours: the queued record becomes running
	rec.State = stateRunning
	d.saveJob(rec)

	if propagated != "" {
		// an upstream stage failed, so this stage fails too — recorded,
		// never executed. The empty output commit keeps the DAG's
		// commits continuous, so the failure reaches every downstream stage
		// and the flush can walk the chain.
		rec.State = stateFailure
		rec.Reason = propagated
		rec.Finished = now()
		d.saveJob(rec) // terminal state durable before the commit finishes
		if oc, err := d.store.StartCommit(pl.Pipeline.Name, outputBranch(pl), ""); err == nil {
			rec.OutputCommit = oc.ID
			d.saveJob(rec)
			d.finishOutput(pl, oc, "", true)
			d.recordProvenance(oc.ID, rec.InputCommits)
		}
		return
	}

	fail := func(reason string) {
		rec.State = stateFailure
		rec.Reason = reason
		rec.Finished = now()
		d.saveJob(rec)
	}

	// Placement: a pipeline may require its work to run on a host bearing
	// a placement label. Until such a host has registered, the job waits —
	// its record was saved at trigger time, so the pending work is
	// durable — and the pipeline surfaces the outage as the crashed state
	// instead of hanging silently. When a host bearing the label
	// registers, the wait re-places automatically and the pipeline
	// recovers: the same job, the same input revision, exactly one output
	// commit. A cancel while unplaced settles the job killed like any
	// other in-flight cancel.
	var placedHost *execHost
	if pl.Pipeline.Placement != "" {
		for {
			if h, ok := d.hosts.pick(pl.Pipeline.Placement); ok {
				placedHost = &h
				break
			}
			d.markPipelineCrashed(pl.Pipeline.Name,
				fmt.Sprintf("no execution host bearing placement label %q", pl.Pipeline.Placement))
			select {
			case <-rj.cancelCh:
				rec.State = stateKilled
				rec.Reason = reasonJobCancelled
				rec.Finished = now()
				d.saveJob(rec)
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
		d.markPipelineRunning(pl.Pipeline.Name)
	}

	// Resolve each side's input revision and enumerate its datums; the
	// job's datum set depends on the input kind: a cross takes the
	// cartesian product, a join pairs files by their join key, a group
	// collects files by their group key. A side without a head contributes
	// no datums, so the product is empty.
	views := map[string]map[string]store.ViewEntry{}
	sideLists := make([][]datumSide, len(sides))
	in := pl.Pipeline.Input
	// Resolve every consumed repo's head into the views: union branches
	// nested anywhere — including inside a cross — contribute their own
	// branches' heads, keyed by branch so two branches of one repo stay
	// distinct. Sides already covered by the pairing heads are
	// left to the loop below — a manual run pins the job to specific
	// commits, and the current head must not leak into the recorded
	// input set. The full resolved input set is recorded on the
	// job so the flush can find it.
	seenInput := map[string]bool{}
	covered := map[string]bool{} // repo/branch covered by the pairing heads
	for _, h := range heads {
		if h.ID != "" {
			seenInput[h.ID] = true
			covered[h.Repo+"/"+h.Branch] = true
		}
	}
	resolvedHead := map[string]client.Commit{} // side key → head picked up while queued
	var resolve func(s client.Input, key string)
	resolve = func(s client.Input, key string) {
		if key == "" {
			key = s.Name
			if key == "" {
				key = s.Repo
			}
		}
		switch {
		case s.Repo != "":
			if covered[s.Repo+"/"+inputBranch(s)] {
				return
			}
			if h := d.finishedHead(s.Repo, inputBranch(s)); h.ID != "" && !seenInput[h.ID] {
				if v, err := d.store.ResolveViewByID(h.ID); err == nil {
					views[key] = v
					seenInput[h.ID] = true
					rec.InputCommits = append(rec.InputCommits, h.ID)
					// a side whose spawn-time pairing was empty can pick
					// up the head that finished while the job was queued:
					// the record grows to the full pairing, and the datum
					// loop below must enumerate it too — otherwise the
					// record claims a pairing the job never executed and
					// the trigger dedup suppresses the real job
					resolvedHead[key] = h
				}
			}
		case len(s.Union) > 0:
			for _, b := range s.Union {
				resolve(b, unionBranchKey(b))
			}
		case len(s.Cross) > 0:
			for i := range s.Cross {
				resolve(s.Cross[i], "")
			}
		}
	}
	resolve(*in, "")
	d.saveJob(rec)
	// A late head picked up while queued must not duplicate a pairing a
	// sibling job already covers: the lone side job and the pairing job
	// race — whichever runs first claims the pairing, the other settles as
	// the lone job with no output: the lone C job for E1 must not produce
	// a second wave-1 output after the B1×E1 pairing job spawned; when no
	// sibling exists the lone job grows into the pairing itself and the
	// trigger's dedup suppresses the duplicate. The check
	// runs under the trigger mutex, so it cannot interleave with the
	// trigger's own dedup+save.
	if len(resolvedHead) > 0 {
		triggerMu.Lock()
		dup := d.hasJobWithExactInputs(rec.Pipeline, rec.InputCommits, rec.ID)
		triggerMu.Unlock()
		if dup {
			late := map[string]bool{}
			for key, h := range resolvedHead {
				late[h.ID] = true
				delete(views, key)
			}
			var keep []string
			for _, id := range rec.InputCommits {
				if !late[id] {
					keep = append(keep, id)
				}
			}
			rec.InputCommits = keep
			resolvedHead = map[string]client.Commit{}
			d.saveJob(rec)
		}
	}
	d.saveJob(rec)
	for i, s := range sides {
		if len(s.Union) > 0 {
			// a union member of a cross contributes its own merged datums
			// even though it has no single head commit
			var us []datumSide
			for _, dt := range d.unionDatums(views, &s) {
				us = append(us, dt.Sides[0])
			}
			sideLists[i] = us
			continue
		}
		head := client.Commit{}
		if i < len(heads) && heads[i].ID != "" {
			head = heads[i]
		} else if h, ok := resolvedHead[sideKey(s)]; ok {
			head = h // the side's head finished while the job was queued
		}
		if head.ID == "" {
			continue
		}
		view, err := d.store.ResolveViewByID(head.ID)
		if err != nil {
			fail("materialize input: " + err.Error())
			return
		}
		views[s.Name] = view
		sd := enumerateDatums(view, s.Glob)
		sd = chunkSideDatums(sd, pl.Pipeline.ChunkSpec, view)
		for j := range sd {
			sd[j].Name = s.Name
		}
		sideLists[i] = sd
	}
	var datums []datum
	switch {
	case len(in.Union) > 0:
		datums = d.unionDatums(views, in)
	case len(in.Join) > 0:
		datums = joinDatums(views, sides)
	case len(in.Group) > 0:
		datums = groupDatums(views, sides)
	default:
		datums = crossDatums(sideLists)
	}
	for i := range datums {
		if len(datums[i].Sides) == 1 && datums[i].Sides[0].Merge != nil {
			continue // a union datum already carries its merged content hash
		}
		datums[i].Hash = datumHash(d.store, views, datums[i])
	}
	for _, dt := range datums {
		rec.DatumIDs = append(rec.DatumIDs, dt.ID)
	}
	d.saveJob(rec)
	// the datum set for log filters is the first side's full input files;
	// cross jobs filter by their sides' files.
	var logDatums []datumRef
	for i := range sides {
		head := client.Commit{}
		if i < len(heads) && heads[i].ID != "" {
			head = heads[i]
		} else if h, ok := resolvedHead[sideKey(sides[i])]; ok {
			head = h
		}
		if head.ID == "" {
			continue
		}
		if vd, err := d.store.ViewDatums(head.ID); err == nil {
			logDatums = append(logDatums, vd...)
		}
	}
	rec.Datums = logDatums
	d.saveJob(rec)

	// a job whose inputs contribute no datums settles successful with
	// nothing to produce — no output commit, so an empty wave never
	// propagates through the DAG (exactly one commit per wave; lone cross
	// jobs produce nothing downstream)
	if len(datums) == 0 {
		rec.State = stateSuccess
		rec.Finished = now()
		d.saveJob(rec)
		return
	}

	// The job's container output is captured into the log store as it is
	// produced. A capture failure degrades to no logs, never to a broken
	// job: execution is the control plane's job, logs are the meta plane's.
	outCommit, err := d.store.StartCommit(pl.Pipeline.Name, outputBranch(pl), "")
	if err != nil {
		fail("start output commit: " + err.Error())
		if errors.Is(err, errNotFound) || errors.Is(err, store.ErrNotFound) {
			// the output repository vanished: the pipeline fails with
			// a recorded reason and stops scheduling
			d.markPipelineFailed(pl.Pipeline.Name, "output repository missing")
		}
		return
	}
	rec.OutputCommit = outCommit.ID
	d.saveJob(rec)

	// Dedup: a datum whose content is unchanged from a previous successful
	// run is skipped — the pipeline does not pay for data it already
	// processed — unless the pipeline reprocesses every job. The skip is
	// recorded on the job, never on the shared record: the record must
	// keep its last successful outcome so later jobs can still skip on it.
	// Every datum gets a placeholder record so the job's full datum set is
	// listable mid-flight with its input files.
	dedup := d.loadDedup(pl.Pipeline.Name)
	reprocess := pl.Pipeline.Reprocess
	rec.DatumStates = map[string]string{}
	skipped := map[string]bool{}
	var todo []datum
	for _, dt := range datums {
		if st, ok := dedup[dt.ID]; ok && !reprocess && st.Outcome == stateSuccess && st.Hash == dt.Hash {
			rec.DatumStates[dt.ID] = stateSkipped
			skipped[dt.ID] = true
			continue
		}
		if _, ok := dedup[dt.ID]; !ok {
			var inputFiles []fileRef
			for _, sd := range dt.Sides {
				for _, f := range sd.Files {
					e := views[sd.Name][f]
					if h, err := e.Hash(d.store); err == nil {
						inputFiles = append(inputFiles, fileRef{Path: f, Hash: h, Size: e.Size()})
					}
				}
			}
			dedup[dt.ID] = datumState{Hash: dt.Hash, InputFiles: inputFiles}
		}
		todo = append(todo, dt)
	}

	jx := &jobExec{d: d, pl: pl, id: id, outDir: outDir, views: views,
		viewDirs: map[string]string{}, dedup: dedup, rj: rj, host: placedHost,
		transformHash: transformHash(pl.Pipeline.Transform)}
	jx.env = d.jobEnv(pl, id, outCommit.ID, sides, heads)
	// apply the pipeline's execution-environment customization: the
	// document's env vars join the job environment and
	// its volumes become mounts at /sandman/volumes/<name> — an
	// emptyDir volume is a fresh per-job directory
	if custom, err := parseCustomization(pl.Pipeline.Transform); err != nil {
		// validated at creation; reaching here is a provisioning failure
		fail("customization: " + err.Error())
		d.finishOutput(pl, outCommit, "", true)
		return
	} else if custom != nil {
		for k, v := range custom.Env {
			if !reservedEnv[k] {
				jx.extraEnv = append(jx.extraEnv, k+"="+v)
			}
		}
		for name, vol := range custom.Volumes {
			host := vol.HostPath
			if vol.EmptyDir {
				host = filepath.Join(d.jobDir(id), "volumes", name)
				os.MkdirAll(host, 0o755)
			}
			jx.extraMounts = append(jx.extraMounts, "-v", host+":/sandman/volumes/"+name)
		}
	}
	// secret bindings: each reference's key is
	// written as a file at MountPath/<key> and/or injected as the env
	// var, so secret values reach the execution environment before the
	// job starts. References sharing a MountPath merge into one bind
	// mount: the pachyderm-style {name, mountPath} pattern declares
	// several keys at one path, and docker rejects duplicate mount
	// points for the same container path (exit 125). A key mounted
	// twice on one path (from two secrets, or twice from one) is an
	// ambiguity — rejected rather than silently overwritten.
	mountDirs := map[string]string{}            // mountPath -> host dir
	mountKeys := map[string]map[string]string{} // host dir -> key -> secret name
	for _, m := range pl.Pipeline.Transform.Secrets {
		if !store.ValidName(m.Name) {
			fail("invalid secret name: " + m.Name)
			d.finishOutput(pl, outCommit, "", true)
			return
		}
		srec, err := d.loadSecret(m.Name)
		if err != nil {
			fail("secret: " + err.Error())
			d.finishOutput(pl, outCommit, "", true)
			return
		}
		if m.EnvVar != "" {
			v, ok := srec.Data[m.Key]
			if !ok {
				fail(fmt.Sprintf("secret %q has no key %q", m.Name, m.Key))
				d.finishOutput(pl, outCommit, "", true)
				return
			}
			jx.extraEnv = append(jx.extraEnv, m.EnvVar+"="+v)
		}
		if m.MountPath != "" {
			dir, ok := mountDirs[m.MountPath]
			if !ok {
				dir = filepath.Join(d.jobDir(id), "secrets", strconv.Itoa(len(mountDirs)))
				os.MkdirAll(dir, 0o755)
				mountDirs[m.MountPath] = dir
				mountKeys[dir] = map[string]string{}
				jx.extraMounts = append(jx.extraMounts, "-v", dir+":"+m.MountPath)
			}
			mount := func(key, val string) bool {
				if owner, dup := mountKeys[dir][key]; dup {
					fail(fmt.Sprintf("secret mount: key %q at %q already mounted from secret %q", key, m.MountPath, owner))
					d.finishOutput(pl, outCommit, "", true)
					return false
				}
				mountKeys[dir][key] = m.Name
				os.WriteFile(filepath.Join(dir, key), []byte(val), 0o644)
				return true
			}
			if m.Key != "" {
				v, ok := srec.Data[m.Key]
				if !ok {
					fail(fmt.Sprintf("secret %q has no key %q", m.Name, m.Key))
					d.finishOutput(pl, outCommit, "", true)
					return
				}
				if !mount(m.Key, v) {
					return
				}
			} else {
				for k, v := range srec.Data {
					if !mount(k, v) {
						return
					}
				}
			}
		}
	}
	// the live execution context is visible to the datum API (a restart
	// aborts a datum's processing and re-runs it from scratch) while the
	// job runs; it leaves the registry with the running handle when the
	// job settles
	d.setJobExec(id, jx)

	// Whole-job deadline: at the boundary the job is cancelled and
	// its active containers killed; it settles as killed, never as a plain
	// failure. A job that already settled is unaffected (its containers are
	// unregistered by then). The timer is stopped when the job settles
	// (runJob returns after unregisterRunning): the late-fire guard would
	// no-op on done, but a pending timer would otherwise hold the closure
	// (and its rj references) live until the deadline.
	if tr := pl.Pipeline.Transform; tr.JobTimeout != "" {
		if dur, err := time.ParseDuration(tr.JobTimeout); err == nil {
			jobTimer := time.AfterFunc(dur, func() {
				select {
				case <-rj.done:
					return
				default:
				}
				rj.cancelled.Store(true)
				for _, n := range rj.containerNames() {
					exec.Command("docker", "kill", n).Run()
				}
			})
			defer jobTimer.Stop()
		}
	}
	failedAny := d.runDatums(jx, todo)

	for _, dt := range datums {
		if _, ok := rec.DatumStates[dt.ID]; !ok {
			rec.DatumStates[dt.ID] = dedup[dt.ID].Outcome
		}
		switch rec.DatumStates[dt.ID] {
		case stateSuccess:
			rec.Processed++
		case stateRecovered:
			rec.Recovered++
		case "failed":
			rec.Failed++
		case stateSkipped:
			rec.Skipped++
		}
	}

	if failedAny {
		// All-or-nothing output: finish the commit explicitly empty. A
		// failed datum still leaves the job inspectable and the pipeline
		// schedulable.
		killed := rj.cancelled.Load()
		if killed {
			rec.State = stateKilled
			rec.Reason = reasonJobCancelled
		} else {
			rec.State = stateFailure
			rec.Reason = failedDatumReason(dedup, datums)
		}
		rec.Finished = now()
		// the terminal state is durable before the output commit finishes,
		// so the downstream trigger observes the failure
		d.saveJob(rec)
		d.finishOutput(pl, outCommit, "", true)
		d.recordProvenance(outCommit.ID, rec.InputCommits)
		if !killed && !rec.Manual {
			// a failed output is still a revision: every downstream stage
			// is triggered and fails in turn. A killed job's empty
			// output is not a processing event — stopping a pipeline must
			// not create spurious downstream commits; neither is a
			// manual run's.
			if fin, err := d.store.InspectCommit(outCommit.ID); err == nil {
				d.triggerForCommit(fin)
			}
		}
		if pl.Pipeline.EnableStats {
			// the failed job's datum records are still published on the
			// stats branch (output + statistics commits)
			if statsID := d.writeStatsCommit(pl, dedup, datums); statsID != "" {
				rec.StatsCommit = statsID
			}
		}
		d.saveDedup(pl.Pipeline.Name, dedup)
		if rec.StatsCommit != "" {
			if sc, err := d.store.InspectCommit(rec.StatsCommit); err == nil {
				d.triggerForCommit(sc)
			}
		}
		return
	}

	// Merge every datum's contribution into the output directory — a
	// processed datum's fresh files, a skipped datum's carried files.
	if err := d.mergeOutputs(jx, datums, skipped); err != nil {
		d.finishOutput(pl, outCommit, "", true)
		fail("merge output: " + err.Error())
		d.saveDedup(pl.Pipeline.Name, dedup)
		return
	}

	// Upload OUT into the output commit in one batch, then finish it (which
	// may trigger downstream pipelines). The output repository may have
	// been force-deleted while the job ran: that fails the job and
	// the pipeline rather than silently resurrecting the repo.
	if _, err := os.Stat(d.store.RepoDir(pl.Pipeline.Name)); err != nil {
		d.finishOutput(pl, outCommit, "", true)
		fail("output repository missing: " + err.Error())
		d.markPipelineFailed(pl.Pipeline.Name, "output repository deleted while running")
		return
	}
	fin, err := d.finishOutput(pl, outCommit, outDir, false)
	if err != nil {
		fail("upload output: " + err.Error())
		return
	}
	d.recordProvenance(fin.ID, rec.InputCommits)
	// statistics-enabled pipelines also produce a per-job statistics
	// commit on the output repo's "stats" branch, consumable downstream
	// (two commits per job: output + statistics)
	if pl.Pipeline.EnableStats {
		if statsID := d.writeStatsCommit(pl, dedup, datums); statsID != "" {
			rec.StatsCommit = statsID
		}
	}
	// the egress step runs after the output commit succeeds: a failure to
	// write the external destination fails the job with an egress-related
	// reason — output success alone does not make the job successful
	if pl.Pipeline.Egress != nil {
		if err := d.runEgress(pl, fin); err != nil {
			rec.State = stateFailure
			rec.Reason = "egress: " + err.Error()
			rec.Finished = now()
			d.saveJob(rec)
			d.saveDedup(pl.Pipeline.Name, dedup)
			return
		}
	}
	rec.State = stateSuccess
	rec.Finished = now()
	d.saveJob(rec)
	d.saveDedup(pl.Pipeline.Name, dedup)

	// The output commit is a real revision of the output repo: propagate —
	// unless this was a manual run, whose output is not a processing wave.
	if rec.Manual {
		return
	}
	d.triggerForCommit(fin)
	if rec.StatsCommit != "" {
		if sc, err := d.store.InspectCommit(rec.StatsCommit); err == nil {
			d.triggerForCommit(sc)
		}
	}
}

// copyDir copies every file under src into dst, preserving relative paths.
// It is the default entry point: a pipeline declared with no command and
// no stdin runs this, so a single-input pipeline's output commit contains
// one file per input file, same relative name and identical content (the
// core copy pipeline contract; a bare default command must never drop or
// alter a matched input file). Returns 0 on success, 1 on any failure.
func copyDir(src, dst string) int {
	ok := true
	filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			ok = false
			return nil
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			ok = false
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			ok = false
			return nil
		}
		target := filepath.Join(dst, rel)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			ok = false
			return nil
		}
		if err := os.WriteFile(target, data, 0o644); err != nil {
			ok = false
		}
		return nil
	})
	if !ok {
		return 1
	}
	return 0
}

// datumSpec describes one command run through the execution-backend seam:
// the transform's image, argv, env, mounts, resources, identity,
// and stdin, plus the PathMap translating the execution-internal paths
// (/sandman/out, /sandman/in/<side>, /sandman/view/<side>, /tmp) to the
// staging directories the process backend runs against directly.
func datumSpec(tr *client.Transform, nodeName, cname string, env []string, mounts []string, outDir string, capture io.Writer, argv, stdin []string) JobSpec {
	pathMap := map[string]string{"/sandman/out": outDir}
	for i := 0; i+1 < len(mounts); i += 2 {
		if !strings.HasPrefix(mounts[i], "-v") {
			continue
		}
		host, rest, ok := strings.Cut(mounts[i+1], ":")
		if !ok {
			continue
		}
		container, _, _ := strings.Cut(rest, ":") // drop a ":ro"/":rw" mode
		pathMap[container] = host
	}
	return JobSpec{
		Image:            tr.Image,
		NodeName:         nodeName,
		Name:             cname,
		Cmd:              argv,
		Stdin:            stdin,
		Env:              env,
		Mounts:           mounts,
		OutDir:           outDir,
		Capture:          capture,
		Workdir:          tr.Workdir,
		User:             tr.User,
		ResourceLimits:   tr.ResourceLimits,
		ResourceRequests: tr.ResourceRequests,
		PathMap:          pathMap,
	}
}

// runSpec executes one command on the daemon's execution backend (the
// control plane's executor; the worker's executor is runDatumContainer).
func (d *daemon) runSpec(tr *client.Transform, nodeName, cname string, env []string, mounts []string, outDir string, capture io.Writer, argv, stdin []string) (int, string) {
	res := d.runner.Run(datumSpec(tr, nodeName, cname, env, mounts, outDir, capture, argv, stdin))
	return res.Code, res.Tail
}

// runDatumContainer runs one command through the container backend — the
// remote execution host's worker executor: nodeName is the host
// identity the container is labelled with.
func runDatumContainer(tr *client.Transform, nodeName, cname string, env []string, mounts []string, outDir string, capture io.Writer, argv, stdin []string) (int, string) {
	res := containerRunner{}.Run(datumSpec(tr, nodeName, cname, env, mounts, outDir, capture, argv, stdin))
	return res.Code, res.Tail
}

// shQuote single-quotes s for /bin/sh (busybox included).
func shQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// joinSh renders argv as a single shell command, each element quoted.
func joinSh(argv []string) string {
	parts := make([]string, len(argv))
	for i, a := range argv {
		parts[i] = shQuote(a)
	}
	return strings.Join(parts, " ")
}
