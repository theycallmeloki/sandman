package main

// Jobs: the job lifecycle — spawn, trigger, gate, cancel, output finish —
// and the durable job records under <state>/jobs/<id>/job.json. Splitting
// the control plane into pipeline.go (spec validation + CRUD), jobs.go
// (lifecycle) and execute.go (runJob + the execution-backend seam) keeps
// each file under ~1300 lines and map-pable.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sandman/client"
	"sandman/internal/store"
)

// ---- jobs ----

// datumRef is one input file of a job — its path and content hash — the
// per-datum handle for log filters (SB-060). A job's datum set is the
// input revision's full view. The type is the store's DatumRef.
type datumRef = store.DatumRef

// jobRec is the persisted form of a job. Version, Transform, and Input are
// snapshots of the pipeline version that created the job: historical jobs
// keep their original transform across pipeline updates (SB-040, SB-143).
type jobRec struct {
	ID           string            `json:"id"`
	Pipeline     string            `json:"pipeline"`
	State        string            `json:"state"` // running | success | failure | killed | skipped
	Reason       string            `json:"reason,omitempty"`
	InputCommits []string          `json:"inputCommits,omitempty"`
	OutputCommit string            `json:"outputCommit,omitempty"`
	Started      string            `json:"started,omitempty"`
	Finished     string            `json:"finished,omitempty"`
	Version      int               `json:"version,omitempty"`
	Transform    *client.Transform `json:"transform,omitempty"`
	Input        *client.Input     `json:"input,omitempty"`
	Datums       []datumRef        `json:"datums,omitempty"`
	DatumIDs     []string          `json:"datumIds,omitempty"`
	DatumStates  map[string]string `json:"datumStates,omitempty"`
	StatsCommit  string            `json:"statsCommit,omitempty"`
	// Manual marks a job spawned by an explicit pipeline run: its output
	// never propagates downstream (SB-010).
	Manual    bool `json:"manual,omitempty"`
	Processed int  `json:"processed,omitempty"`
	Recovered int  `json:"recovered,omitempty"`
	Failed    int  `json:"failed,omitempty"`
	Skipped   int  `json:"skipped,omitempty"`
}

func (d *daemon) jobDir(id string) string {
	return filepath.Join(d.state, "jobs", id)
}

func (rec *jobRec) job() client.Job {
	return client.Job{
		ID:           rec.ID,
		Pipeline:     rec.Pipeline,
		State:        rec.State,
		Reason:       rec.Reason,
		InputCommits: rec.InputCommits,
		OutputCommit: rec.OutputCommit,
		Started:      rec.Started,
		Finished:     rec.Finished,
		Version:      rec.Version,
		StatsCommit:  rec.StatsCommit,
		Processed:    rec.Processed,
		Recovered:    rec.Recovered,
		Failed:       rec.Failed,
		Skipped:      rec.Skipped,
	}
}

func (d *daemon) saveJob(rec *jobRec) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	// atomic write: a concurrent reader (the trigger dedup's mustListJobs,
	// flushes, listings) must see the record complete or not at all — a
	// plain WriteFile lets a racing read decode a half-written record and
	// silently drop the job, which makes the duplicate-pairing guard miss
	// (SB-056/SB-019: two triggers for the same input set)
	dir := d.jobDir(rec.ID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(dir, "job.json.tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, "job.json")); err != nil {
		return err
	}
	// every job-record change can advance a flush (a new job, a datum
	// state, a terminal transition): wake the blocking waits (D-23 R-5)
	d.stateChanged.signal()
	return nil
}

func (d *daemon) listJobs() ([]client.Job, error) {
	return d.listJobsFiltered("", "", nil, false, nil, nil)
}

// requirePipeline fails when the named pipeline does not exist or its
// definition record is corrupted (the listing of jobs or logs of a missing
// pipeline is an error, not an empty result; SB-026/027).
func (d *daemon) requirePipeline(pipeline string) error {
	if _, err := d.loadPipeline(pipeline); err != nil {
		if _, statErr := os.Stat(d.pipelinePath(pipeline)); statErr == nil {
			return fmt.Errorf("pipeline %q is incomplete", pipeline)
		}
		return notFound("pipeline %q not found", pipeline)
	}
	return nil
}

// listJobsFiltered lists jobs, applying the pipeline, output-commit, and
// inclusive state-set filters (SB-093, SB-095), plus a history depth
// (0 = current version only, N = N most recent versions, -1 = every
// version, SB-143). Full listings carry each job's own version's transform
// and input snapshots (SB-094, SB-040). Listing jobs for a pipeline that
// does not exist is an error, not an empty list (SB-026/027).
func (d *daemon) listJobsFiltered(pipeline, outputCommit string, states []string, full bool, history *int, inputCommits []string) ([]client.Job, error) {
	if pipeline != "" {
		if err := d.requirePipeline(pipeline); err != nil {
			return nil, err
		}
	}
	entries, err := os.ReadDir(filepath.Join(d.state, "jobs"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]client.Job, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(d.state, "jobs", e.Name(), "job.json"))
		if err != nil {
			continue
		}
		var rec jobRec
		if json.Unmarshal(b, &rec) != nil {
			continue
		}
		if pipeline != "" && rec.Pipeline != pipeline {
			continue
		}
		if outputCommit != "" && rec.OutputCommit != outputCommit {
			continue
		}
		if len(inputCommits) > 0 {
			want := map[string]bool{}
			for _, ic := range inputCommits {
				want[ic] = true
			}
			got := map[string]bool{}
			for _, jic := range rec.InputCommits {
				if want[jic] {
					got[jic] = true
				}
			}
			if len(got) != len(want) {
				continue
			}
		}
		if len(states) > 0 {
			match := false
			for _, s := range states {
				if s == rec.State {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		if history != nil {
			h := *history
			keep := h < 0
			if !keep {
				if cur, err := d.loadPipeline(rec.Pipeline); err == nil {
					// depth 0 = current version only; depth N = the current
					// version plus N older versions (the record's exact
					// counts: depth 1 lists the two most recent versions)
					keep = rec.Version >= cur.Version-h
				}
			}
			if !keep {
				continue
			}
		}
		if full {
			j := rec.job()
			j.Transform = rec.Transform
			j.Input = rec.Input
			out = append(out, j)
		} else {
			out = append(out, rec.job())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Started > out[j].Started })
	return out, nil
}
func (d *daemon) inspectJob(id string) (client.Job, error) {
	var info client.Job
	if b, err := os.ReadFile(filepath.Join(d.jobDir(id), "job.json")); err == nil {
		var rec jobRec
		if json.Unmarshal(b, &rec) == nil {
			info = rec.job()
		}
	}
	// a job may also be keyed by the output commit it produced (SB-135)
	if info.ID == "" {
		for _, j := range d.mustListJobs() {
			if j.OutputCommit == id {
				info = j
				break
			}
		}
	}
	if info.ID == "" {
		return client.Job{}, notFound("job %q not found: specify a Job or an OutputCommit", id)
	}
	// the live per-worker status (SB-065/097)
	if b, err := os.ReadFile(filepath.Join(d.jobDir(info.ID), "workers.json")); err == nil {
		var ws []workerStatus
		if json.Unmarshal(b, &ws) == nil {
			for _, w := range ws {
				info.Workers = append(info.Workers, client.WorkerStatus{
					Worker: w.Worker, Datum: w.Datum, Started: w.Started, Queue: w.Queue,
				})
			}
		}
	}
	return info, nil
}

// markStaleJobsFailed repairs the state after a daemon restart: jobs that
// were running when the daemon died can never finish here (their containers
// were orphaned and will be pruned), so they are recorded as failed. A
// standby pipeline whose in-flight work was lost that way has no pending
// work left and returns to standby.
func (d *daemon) markStaleJobsFailed() {
	for _, j := range d.mustListJobs() {
		if j.State == stateRunning {
			rec := jobRec{ID: j.ID, Pipeline: j.Pipeline, State: stateFailure,
				Reason: reasonDaemonRestarted, InputCommits: j.InputCommits,
				OutputCommit: j.OutputCommit, Started: j.Started, Finished: now()}
			d.saveJob(&rec)
			if p, err := d.loadPipeline(j.Pipeline); err == nil && p.Pipeline.Standby && p.State == stateRunning {
				p.State = stateStandby
				d.savePipeline(p)
			}
		}
	}
}

func (d *daemon) mustListJobs() []client.Job {
	jobs, _ := d.listJobs()
	return jobs
}

// jobByOutput finds the job that produced a commit, if any. Commits with
// no producing job — user commits and spec commits — are not failures.
func (d *daemon) jobByOutput(commitID string) (client.Job, bool) {
	for _, j := range d.mustListJobs() {
		if j.OutputCommit == commitID {
			return j, true
		}
	}
	return client.Job{}, false
}

// newJobID builds a job's unique id: <node>-<hex> (kept byte-identical
// to the historical format).
func newJobID(node string) string { return newID(node, "") }

// newJobRec builds a job's initial record: the running state, the input
// pairing it consumed, and the pipeline-version snapshots (SB-040/143).
func newJobRec(pl pipelineRec, heads []client.Commit, id string) *jobRec {
	rec := &jobRec{ID: id, Pipeline: pl.Pipeline.Name, State: stateRunning,
		Started: now(),
		Version: pl.Version, Transform: pl.Pipeline.Transform, Input: pl.Pipeline.Input}
	seen := map[string]bool{}
	for _, h := range heads {
		if h.ID != "" && !seen[h.ID] {
			seen[h.ID] = true
			rec.InputCommits = append(rec.InputCommits, h.ID)
		}
	}
	return rec
}

// commitRevision writes and finishes one revision and triggers the
// pipelines that consume it — the shared tail of every commit-and-trigger
// path (cron tick, size-trigger fire, spout cycle, git push). The writer
// fills the commit's tree and reports whether the revision is complete:
// false abandons the commit (no finish, no trigger), so a caller can
// refuse to publish a partial revision. finishCommit and the consumer
// trigger are one step so no caller can produce a finished commit that
// never triggers. Provenance (optional) is stamped before the trigger
// (spout's epoch anchor, SB-139 clause 7).
func (d *daemon) commitRevision(repo, branch string, write func(commitID string) bool, provenance []string) bool {
	cm, err := d.store.StartCommit(repo, branch, "")
	if err != nil {
		return false
	}
	if write != nil && !write(cm.ID) {
		return false
	}
	fin, err := d.store.FinishCommit(cm.ID, "", false)
	if err != nil {
		return false
	}
	if len(provenance) > 0 {
		if rec, err := d.store.LoadCommitByID(fin.ID); err == nil {
			rec.Provenance = provenance
			d.store.SaveCommit(rec)
		}
	}
	d.triggerForCommit(fin)
	return true
}

// triggerForCommit launches one job per running pipeline subscribed to the
// commit's repo. Jobs run in their own goroutines; the trigger never
// blocks. It is called by the commit-finish callers (the HTTP handler,
// job output, cron, spout, git push).
func (d *daemon) triggerForCommit(cm client.Commit) {
	// size triggers watching the commit's branch accumulate their bytes and
	// may fire (SB-160); the trigger commits they create re-enter here
	d.accumulateTriggers(cm)
	pipes, _ := d.listPipelinesFiltered(nil, "", false)
	for _, p := range pipes {
		if p.State == stateFailure || p.State == stateCrashed {
			continue
		}
		// A pipeline whose transaction head job has not been scheduled yet
		// must not be triggered twice: the transaction coordinator is the
		// sole scheduler for it (SB-162: exactly one job per update).
		if txNoteTrigger(p.Name) {
			continue
		}
		rec, err := d.loadPipeline(p.Name)
		if err != nil || rec.Pipeline.Input == nil {
			continue
		}
		if rec.Stopped {
			continue // stopped pipelines ignore new commits (SB-048)
		}
		if rec.Pipeline.Service != nil {
			// a service is never triggered per commit: its single
			// long-lived job refreshes its served input itself (SB-100)
			continue
		}
		if !pipelineConsumes(rec.Pipeline.Input, cm.Repo, cm.Branch) {
			continue
		}
		// the pairing at trigger time: the new commit on its side, each
		// other side at its current head (SB-120)
		heads := d.pairHeads(rec.Pipeline.Input)
		// a commit produced by a failed job propagates the failure: the
		// consuming pipeline's job is recorded failed without executing
		// (SB-022)
		propagated := ""
		if j, ok := d.jobByOutput(cm.ID); ok && j.State == stateFailure {
			propagated = "upstream job " + j.ID + " failed: " + j.Reason
		}
		// exactly one job per input pairing: two sides' commits landing
		// near-simultaneously can each pair with the other's fresh head,
		// and the second trigger must not spawn a duplicate job for the
		// same input set (SB-056: one job per wave, never extra). The
		// duplicate check and the job record's creation are atomic, so a
		// racing trigger sees the record instead of double-spawning.
		triggerMu.Lock()
		if d.hasRunningJobWithInputs(rec.Pipeline.Name, heads) {
			triggerMu.Unlock()
			continue
		}
		// provenance consistency: two sides that derive from the same
		// source branch must pair at the same source revision. A trigger
		// that pairs a fresh commit with the other side's still-stale head
		// (the other side has not caught up yet) is deferred instead of
		// spawning a mismatched job — the catch-up trigger will form the
		// coherent pairing (SB-018/019 diamonds: one commit per source
		// revision, never one per dependency path).
		if !d.crossPairingConsistent(heads) {
			triggerMu.Unlock()
			continue
		}
		id := newJobID(d.name)
		jr := newJobRec(*rec, heads, id)
		d.saveJob(jr) // creates the job dir atomically
		triggerMu.Unlock()
		d.spawnJob(rec, heads, propagated, id, jr)
	}
	// a finished commit always wakes the blocking waits, even when no
	// job spawned (a service pipeline consumes the commit by refreshing
	// its served input; an empty flush re-checks its closure): services
	// and flushes advance on the signal
	d.stateChanged.signal()
}

// finishedHead returns the branch's finished head commit, or the zero
// Commit when the branch has none or its head is unfinished — the empty
// contribution in input pairings (pairHeads) and the no-view case in
// input enumeration. The shared query shape: headCommitRec + Finished
// check.
func (d *daemon) finishedHead(repo, branch string) client.Commit {
	if h, err := d.store.HeadCommitRec(repo, branch); err == nil && h.Finished {
		return h
	}
	return client.Commit{}
}

// pairHeads resolves the current finished head of every input side, in
// declaration order; a side with no finished head yields an empty commit —
// its contribution to the cross is no datums.
func (d *daemon) pairHeads(in *client.Input) []client.Commit {
	sides := inputSides(in)
	heads := make([]client.Commit, len(sides))
	for i, s := range sides {
		heads[i] = d.finishedHead(s.Repo, inputBranch(s))
	}
	return heads
}

// pipelineConsumes reports whether any input side subscribes to the
// (repo, branch) pair — the trigger condition for a commit. Union and
// cross branches are walked recursively, so a union of crosses still
// triggers on its members' repos (SB-078).
func pipelineConsumes(in *client.Input, repo, branch string) bool {
	for _, s := range inputSides(in) {
		if s.Repo != "" {
			if s.Repo == repo && inputBranch(s) == branch {
				return true
			}
			continue
		}
		if len(s.Cross) > 0 || len(s.Union) > 0 {
			if pipelineConsumes(&s, repo, branch) {
				return true
			}
		}
	}
	return false
}

// inputConsumesRepo reports whether any input side reads from repo.
func inputConsumesRepo(in *client.Input, repo string) bool {
	for _, s := range inputSides(in) {
		if s.Repo == repo {
			return true
		}
		if s.Repo == "" && (len(s.Cross) > 0 || len(s.Union) > 0) && inputConsumesRepo(&s, repo) {
			return true
		}
	}
	return false
}

// triggerMu serializes the duplicate check and the job record's creation
// in triggerForCommit: a racing trigger for the same input pairing must
// observe the record the first trigger just saved (SB-056).
var triggerMu sync.Mutex

// hasRunningJobWithInputs reports whether the pipeline already has a
// non-terminal job consuming exactly this input set — the guard against
// duplicate pairing jobs when two input sides' commits land together.
func (d *daemon) hasRunningJobWithInputs(pipeline string, heads []client.Commit) bool {
	set := map[string]bool{}
	for _, h := range heads {
		if h.ID != "" {
			set[h.ID] = true
		}
	}
	for _, j := range d.mustListJobs() {
		if j.Pipeline != pipeline || j.State != stateRunning {
			continue
		}
		if len(j.InputCommits) != len(set) {
			continue
		}
		all := true
		for _, ic := range j.InputCommits {
			if !set[ic] {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// hasJobWithExactInputs reports whether the pipeline already has any job —
// in any state, including terminal — consuming exactly this input set,
// other than the excluded job itself. The lone job uses it to decide
// whether a late head's pairing is a sibling's work (the sibling may
// already have settled, so state is not a filter); the exclusion keeps a
// job from matching itself.
func (d *daemon) hasJobWithExactInputs(pipeline string, inputIDs []string, excludeID string) bool {
	set := map[string]bool{}
	for _, id := range inputIDs {
		if id != "" {
			set[id] = true
		}
	}
	for _, j := range d.mustListJobs() {
		if j.ID == excludeID || j.Pipeline != pipeline {
			continue
		}
		if len(j.InputCommits) != len(set) {
			continue
		}
		all := true
		for _, ic := range j.InputCommits {
			if !set[ic] {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// crossPairingConsistent reports whether a cross pairing's sides reference
// each source branch at exactly one revision. Two sides deriving from the
// same source must pair at the same source revision — pairing a fresh
// commit with the other side's stale head would process a mismatched
// revision pair (SB-018/019). Disjoint sources are always consistent.
func (d *daemon) crossPairingConsistent(heads []client.Commit) bool {
	sources := map[string]string{} // repo+branch → source commit
	for _, h := range heads {
		if h.ID == "" {
			continue // an empty side contributes no source
		}
		for _, leaf := range d.provenanceOf(h.ID, map[string]bool{}) {
			rec, err := d.store.LoadCommitByID(leaf)
			if err != nil {
				continue
			}
			key := rec.Repo + "/" + rec.Branch
			if prev, ok := sources[key]; ok && prev != leaf {
				return false
			}
			sources[key] = leaf
		}
	}
	return true
}

// provenanceOf expands a commit to the leaf source commits it derives
// from: the commit itself when no job produced it, otherwise its producing
// job's inputs, recursively.
func (d *daemon) provenanceOf(commitID string, seen map[string]bool) []string {
	if seen[commitID] {
		return nil
	}
	seen[commitID] = true
	if j, ok := d.jobByOutput(commitID); ok {
		var out []string
		for _, ic := range j.InputCommits {
			out = append(out, d.provenanceOf(ic, seen)...)
		}
		return out
	}
	return []string{commitID}
}

// runningJob is the handle on an in-flight job's execution; pipeline is the
// owning pipeline (for update/delete cancellation, SB-045/026), done signals
// the job goroutine has settled, cancelled distinguishes a deliberate kill
// from a plain failure (SB-122). containers tracks the live datum
// container names so a cancel can kill every one of them. jx is the live
// execution context while the job runs (datum API restart, SB-064),
// attached under d.jobsMu by setJobExec.
type runningJob struct {
	pipeline   string
	cancelled  atomic.Bool
	cancelCh   chan struct{}
	cancelOnce sync.Once
	started    atomic.Bool // the job passed the pipeline's gate
	done       chan struct{}

	jx *jobExec

	containersMu sync.Mutex
	containers   map[string]struct{}
}

func (rj *runningJob) registerContainer(name string) {
	rj.containersMu.Lock()
	defer rj.containersMu.Unlock()
	if rj.containers == nil {
		rj.containers = map[string]struct{}{}
	}
	rj.containers[name] = struct{}{}
}

func (rj *runningJob) unregisterContainer(name string) {
	rj.containersMu.Lock()
	defer rj.containersMu.Unlock()
	delete(rj.containers, name)
}

func (rj *runningJob) containerNames() []string {
	rj.containersMu.Lock()
	defer rj.containersMu.Unlock()
	names := make([]string, 0, len(rj.containers))
	for n := range rj.containers {
		names = append(names, n)
	}
	return names
}

// registerRunning puts a job in the live registry (d.running, under
// d.jobsMu). The handle exists from spawn to settlement: a cancel
// arriving in that window finds it, and waitJobSettled/countRunningJobs
// see the job.
func (d *daemon) registerRunning(id, pipeline string) *runningJob {
	rj := &runningJob{pipeline: pipeline, cancelCh: make(chan struct{}), done: make(chan struct{})}
	d.jobsMu.Lock()
	d.running[id] = rj
	d.jobsMu.Unlock()
	return rj
}

// unregisterRunning removes the job from the live registry and signals
// settlement (the JobTimeout kill-select and waitJobSettled select on
// rj.done).
func (d *daemon) unregisterRunning(id string, rj *runningJob) {
	d.jobsMu.Lock()
	delete(d.running, id)
	d.jobsMu.Unlock()
	close(rj.done)
}

// setJobExec attaches the execution context to the running handle (the
// datum API reads it via restartDatum). The job may have settled between
// registerRunning and the jx being built (an early failure path) — the
// handle is then gone and the jx is simply dropped.
func (d *daemon) setJobExec(id string, jx *jobExec) {
	d.jobsMu.Lock()
	if rj, ok := d.running[id]; ok {
		rj.jx = jx
	}
	d.jobsMu.Unlock()
}

// cancelPipelineJobs cancels every in-flight job of the pipeline and waits
// for each to settle (used by update and delete).
func (d *daemon) cancelPipelineJobs(pipeline string) {
	d.jobsMu.Lock()
	var ids []string
	for id, rj := range d.running {
		if rj.pipeline == pipeline {
			ids = append(ids, id)
		}
	}
	d.jobsMu.Unlock()
	for _, id := range ids {
		d.cancelJob(id)
	}
}

// markPipelineFailed records a pipeline-level failure with a reason; the
// pipeline stops scheduling until repaired (D-10).
func (d *daemon) markPipelineFailed(name, reason string) {
	pipelineRecMu.Lock()
	defer pipelineRecMu.Unlock()
	if rec, err := d.loadPipeline(name); err == nil && !rec.Stopped {
		rec.State = stateFailure
		rec.Reason = reason
		d.savePipeline(rec)
	}
}

// markPipelineCrashed records that a pipeline's execution environment could
// not be provisioned (SB-043, SB-091).
func (d *daemon) markPipelineCrashed(name, reason string) {
	pipelineRecMu.Lock()
	defer pipelineRecMu.Unlock()
	if rec, err := d.loadPipeline(name); err == nil && !rec.Stopped {
		rec.State = stateCrashed
		rec.Reason = reason
		d.savePipeline(rec)
	}
}

// markPipelineRunning clears the placement-outage crash once a host bearing
// the pipeline's label has registered: the crashed state was only the
// unplaced outage, and placement has become possible again (SB-169).
func (d *daemon) markPipelineRunning(name string) {
	pipelineRecMu.Lock()
	defer pipelineRecMu.Unlock()
	if rec, err := d.loadPipeline(name); err == nil && rec.State == stateCrashed {
		rec.State = stateRunning
		rec.Reason = ""
		d.savePipeline(rec)
	}
}

// outputMu serializes each pipeline's output-commit write phase. Output
// commits are opened at job start (their id goes into the job's
// environment); concurrent jobs of one pipeline would otherwise parent
// against the same stale head and the last finisher would orphan the other
// off the branch. Under the lock the open commit is re-parented to the
// current head, so the branch stays linear whatever order the jobs finish.
var (
	outputMuGuard sync.Mutex
	outputMu      = map[string]*sync.Mutex{}
)

// jobGate serializes a pipeline's jobs in spawn order: strictly one job at
// a time, the slot handed to the oldest waiter (SB-123). With parallelism
// 1 the jobs of successive commits therefore come up in commit order and
// only one runs at a time; cancelling the running job lets the next queued
// job start, and cancelling one job never touches the others. Serializing
// every job of a pipeline also keeps its output commits strictly ordered,
// so a later commit's content is never stranded under an earlier commit's
// output.
type jobGate struct {
	mu   sync.Mutex
	head *jobGateNode // the next waiter to receive the slot
	tail *jobGateNode
	held bool // the slot is taken by a running job
}

type jobGateNode struct {
	ch   chan struct{}
	next *jobGateNode
}

var (
	jobGateGuard sync.Mutex
	jobGates     = map[string]*jobGate{}
)

func (d *daemon) jobGate(pipeline string) *jobGate {
	jobGateGuard.Lock()
	defer jobGateGuard.Unlock()
	g, ok := jobGates[pipeline]
	if !ok {
		g = &jobGate{}
		jobGates[pipeline] = g
	}
	return g
}

// enter queues the job and blocks until it is handed the slot. It returns
// false when a cancel arrived while the job was queued (or raced the slot
// hand-off): the job never ran, and the slot was released or passed on.
func (g *jobGate) enter(rj *runningJob) bool {
	n := &jobGateNode{ch: make(chan struct{})}
	g.mu.Lock()
	if g.tail != nil {
		g.tail.next = n
	} else {
		g.head = n
	}
	g.tail = n
	start := false
	if !g.held {
		// the gate is free: hand the slot to this job immediately
		g.held = true
		g.head = n.next
		if g.head == nil {
			g.tail = nil
		}
		start = true
	}
	g.mu.Unlock()
	if !start {
		select {
		case <-n.ch:
		case <-rj.cancelCh:
			// cancelled while queued: unlink so the slot reaches the next
			// waiter when the current holder releases it. If the holder
			// released at the same instant (both channels ready and the
			// select picked us), the node is already unlinked and the slot
			// was handed to THIS job: pass it on instead of dropping it.
			if !g.remove(n) {
				g.release()
			}
			return false
		}
	}
	rj.started.Store(true)
	if rj.cancelled.Load() {
		// a cancel raced the hand-off: decline the slot
		g.release()
		return false
	}
	return true
}

// remove unlinks a still-queued node; the jobs behind it move up. It
// reports whether the node was still queued — false means the slot had
// already been handed to it (release popped it).
func (g *jobGate) remove(n *jobGateNode) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	var prev *jobGateNode
	for cur := g.head; cur != nil; cur = cur.next {
		if cur == n {
			if prev != nil {
				prev.next = cur.next
			} else {
				g.head = cur.next
			}
			if g.tail == cur {
				g.tail = prev
			}
			return true
		}
		prev = cur
	}
	return false
}

// release hands the slot to the next queued job, or frees it when the
// queue is empty.
func (g *jobGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.head != nil {
		n := g.head
		g.head = n.next
		if g.head == nil {
			g.tail = nil
		}
		close(n.ch)
		return
	}
	g.held = false
}

func (d *daemon) repoLock(repo string) *sync.Mutex {
	outputMuGuard.Lock()
	defer outputMuGuard.Unlock()
	m, ok := outputMu[repo]
	if !ok {
		m = &sync.Mutex{}
		outputMu[repo] = m
	}
	return m
}

// finishOutput finalizes a job's output commit under the pipeline's commit
// lock: re-parent to the current head, upload OUT unless the commit is an
// explicit empty finish, and close it.
func (d *daemon) finishOutput(pl pipelineRec, outCommit client.Commit, outDir string, empty bool) (client.Commit, error) {
	m := d.repoLock(pl.Pipeline.Name)
	m.Lock()
	defer m.Unlock()
	outBranch := outputBranch(pl)
	if head := d.store.HeadCommit(pl.Pipeline.Name, outBranch); head != "" && head != outCommit.ParentID {
		if err := d.store.Reparent(outCommit.ID, head); err != nil {
			return client.Commit{}, err
		}
	}
	if !empty {
		if err := d.store.AddFilesFromDir(outCommit.ID, outDir); err != nil {
			// all-or-nothing: a failed upload closes the commit empty
			d.store.FinishCommit(outCommit.ID, "", true)
			return client.Commit{}, err
		}
		// deletions propagate to the output revision: paths that were in
		// the parent's view and are gone from this output are tombstoned
		// (SB-007 — a deleted input file is absent, not stale)
		d.store.TombstoneRemoved(outCommit.ID, outDir)
	}
	return d.store.FinishCommit(outCommit.ID, "", empty)
}

// recordProvenance stamps a finished output commit with its derivation:
// its input commits and their own provenance, transitively. The recorded
// provenance makes spout epochs and spec-commit subvenance observable
// (SB-139 clause 7, SB-140 clauses 1/3): a downstream commit's recorded
// provenance includes its upstream spout commit AND that spout's
// specification commit, so the spec commit's subvenants are exactly the
// spout output and the downstream output.
func (d *daemon) recordProvenance(commitID string, inputs []string) {
	if commitID == "" {
		return
	}
	seen := map[string]bool{}
	var prov []string
	var expand func(id string)
	expand = func(id string) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		prov = append(prov, id)
		if cm, err := d.store.LoadCommitByID(id); err == nil {
			for _, p := range cm.Provenance {
				expand(p)
			}
		}
	}
	for _, in := range inputs {
		expand(in)
	}
	if len(prov) == 0 {
		return
	}
	if cm, err := d.store.LoadCommitByID(commitID); err == nil {
		cm.Provenance = prov
		d.store.SaveCommit(cm)
	}
}

// cancelAllRunningJobs cancels every in-flight job; the kill loops run
// asynchronously (cancelJob spawns its own retry goroutine), so a
// shutdown path calls this and then waits on countRunningJobs draining.
func (d *daemon) cancelAllRunningJobs() {
	d.jobsMu.Lock()
	ids := make([]string, 0, len(d.running))
	for id := range d.running {
		ids = append(ids, id)
	}
	d.jobsMu.Unlock()
	for _, id := range ids {
		d.cancelJob(id)
	}
}

// countRunningJobs reports the number of in-flight jobs (shutdown drain).
func (d *daemon) countRunningJobs() int {
	d.jobsMu.Lock()
	defer d.jobsMu.Unlock()
	return len(d.running)
}

// cancelJob kills a running job and waits for it to settle as KILLED. A
// terminal job cancels to a no-op. The kill retries: a job can be cancelled
// the instant it appears, before its container exists (docker run still
// starting), and a single kill would be silently lost.
func (d *daemon) cancelJob(id string) error {
	// the live running handle is the authority — a cancel must not abort
	// on a transiently unreadable job record (a concurrent save can race
	// the read; the job then escapes the cancel and runs the old version
	// indefinitely, SB-045)
	d.jobsMu.Lock()
	rj, ok := d.running[id]
	d.jobsMu.Unlock()
	if !ok {
		if _, err := d.inspectJob(id); err != nil {
			return err
		}
		return nil // already terminal
	}
	rj.cancelled.Store(true)
	rj.cancelOnce.Do(func() { close(rj.cancelCh) })
	if !rj.started.Load() {
		// the job is queued behind the pipeline's gate: it has no work to
		// kill, and it will settle as killed when it reaches the slot's
		// front (SB-123). Returning now lets a later job start as soon as
		// the running one settles — a queued cancel never blocks on it.
		return nil
	}
	go func() {
		for i := 0; i < 120; i++ { // ~30s of retries, or until the job settles
			select {
			case <-rj.done:
				return
			default:
			}
			// kill every container the job has registered (per-datum
			// execution runs several concurrently); a single kill can be
			// lost the instant the job appears, before its containers exist
			names := rj.containerNames()
			if len(names) == 0 {
				time.Sleep(250 * time.Millisecond)
				continue
			}
			all := true
			for _, n := range names {
				if d.runner.Kill(n) != nil {
					all = false
				}
			}
			if all {
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
	}()
	select {
	case <-rj.done:
		return nil
	case <-time.After(30 * time.Second):
		return fmt.Errorf("job %q did not settle after cancel", id)
	}
}

// deleteJob removes a job's record. A running job is first cancelled, which
// finalizes its output revision (SB-057).
func (d *daemon) deleteJob(id string) error {
	if _, err := d.inspectJob(id); err != nil {
		return err
	}
	if err := d.cancelJob(id); err != nil {
		return err
	}
	os.Remove(d.logPath(id))
	return os.RemoveAll(d.jobDir(id))
}

// checkMetadata verifies the control-plane metadata parses, tolerating
// missing records but not corrupted ones.
func (d *daemon) checkMetadata() error {
	pdir := filepath.Join(d.state, "pipelines")
	if entries, err := os.ReadDir(pdir); err == nil {
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), ".json") {
				continue
			}
			if b, err := os.ReadFile(filepath.Join(pdir, e.Name())); err == nil && !json.Valid(b) {
				return fmt.Errorf("pipeline record %s is not valid JSON", e.Name())
			}
		}
	}
	jdir := filepath.Join(d.state, "jobs")
	if entries, err := os.ReadDir(jdir); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			p := filepath.Join(jdir, e.Name(), "job.json")
			if b, err := os.ReadFile(p); err == nil && !json.Valid(b) {
				return fmt.Errorf("job record %s is not valid JSON", e.Name())
			}
		}
	}
	return nil
}

// reset removes every repository, pipeline, and job, and is idempotent
// (SB-037). Corrupted metadata aborts the reset instead of being wiped
// around (product decision D-08).
func (d *daemon) reset() error {
	if err := d.checkMetadata(); err != nil {
		return fmt.Errorf("reset aborted: corrupted metadata (%w)", err)
	}
	// cancel in-flight work so no goroutine writes into removed state
	d.jobsMu.Lock()
	var ids []string
	for id := range d.running {
		ids = append(ids, id)
	}
	d.jobsMu.Unlock()
	for _, id := range ids {
		d.cancelJob(id)
	}
	os.RemoveAll(filepath.Join(d.state, "repos"))
	os.RemoveAll(filepath.Join(d.state, "pipelines"))
	os.RemoveAll(filepath.Join(d.state, "jobs"))
	os.RemoveAll(filepath.Join(d.state, "logs"))
	os.RemoveAll(filepath.Join(d.state, "transactions"))
	os.RemoveAll(filepath.Join(d.state, "dedup"))
	if err := os.MkdirAll(filepath.Join(d.state, "repos"), 0o755); err != nil {
		return err
	}
	// the spec repository is internal and recreated empty (SB-127)
	return d.store.CreateRepo("spec")
}

func (d *daemon) loadPipeline(name string) (*pipelineRec, error) {
	b, err := os.ReadFile(d.pipelinePath(name))
	if err != nil {
		return nil, err
	}
	var rec pipelineRec
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// runJob executes one job: materialize the input view, run the transform
// (or the default copy entry point), upload the OUT directory into a new
// commit of the pipeline's output repo, and finish it. All or nothing: a
// failed job's output commit is finished empty, so partial output is never
// observable. The job id is supplied by the caller so schedulers can track
// the job they spawned.
// jobEnv builds the job-scoped execution environment: each input side's
// commit, the output directory, and job identity (SB-051, SB-101,
// SB-128). The input directory variables are per datum (each datum's own
// staging mount) and are appended by the datum executor.
func (d *daemon) jobEnv(pl pipelineRec, id, outCommit string, sides []client.Input, heads []client.Commit) []string {
	env := []string{
		"OUT=/sandman/out",
		"JOB_ID=" + id,
		"OUTPUT_COMMIT=" + outCommit,
	}
	for i, s := range sides {
		if i < len(heads) && heads[i].ID != "" {
			env = append(env, s.Name+"_COMMIT="+heads[i].ID)
		}
	}
	for k, v := range pl.Pipeline.Transform.Env {
		if !reservedEnv[k] {
			env = append(env, k+"="+v)
		}
	}
	return env
}

// failedDatumReason summarizes a job's failed datums for the job reason —
// the failure is attributed to the datum that failed (SB-011).
func failedDatumReason(dedup map[string]datumState, datums []datum) string {
	var parts []string
	for _, dt := range datums {
		st := dedup[dt.ID]
		if st.Outcome != "failed" {
			continue
		}
		r := "datum " + dt.ID + " failed"
		if st.Reason != "" {
			r += ": " + st.Reason
		}
		parts = append(parts, r)
	}
	reason := strings.Join(parts, "; ")
	if len(reason) > 4000 {
		reason = reason[len(reason)-4000:]
	}
	return reason
}

// resolveCommitRef resolves a commit reference: a commit id, or
// repo@branch meaning that branch's head.
func (d *daemon) resolveCommitRef(ref string) (*store.CommitRec, error) {
	if repo, branch, ok := strings.Cut(ref, "@"); ok {
		head, err := d.store.HeadCommitRec(repo, branch)
		if err != nil {
			return nil, err
		}
		return d.store.LoadCommitByID(head.ID)
	}
	return d.store.LoadCommitByID(ref)
}

// allCommitRecs enumerates every commit record in every repository,
// including the internal spec repository (its commits reference spec
// blobs that garbage collection must keep — SB-079).
func (d *daemon) allCommitRecs() []*store.CommitRec {
	var out []*store.CommitRec
	repos, _ := d.store.ListRepos()
	for _, r := range repos {
		out = append(out, d.repoCommitRecs(r.Name)...)
	}
	// the spec repository is internal and not listed as a user repo
	// (SB-127), but its commits are still durable references
	out = append(out, d.repoCommitRecs("spec")...)
	return out
}

func (d *daemon) repoCommitRecs(repo string) []*store.CommitRec {
	var out []*store.CommitRec
	dir := filepath.Join(d.store.RepoDir(repo), "commits")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		if b, err := os.ReadFile(filepath.Join(dir, e.Name())); err == nil {
			var rec store.CommitRec
			if json.Unmarshal(b, &rec) == nil {
				out = append(out, &rec)
			}
		}
	}
	return out
}

// deleteCommit removes a commit and everything derived from it (SB-124):
// the commit's own record, every commit in any repository whose
// provenance includes it (transitively through the DAG), and the jobs
// that consumed any of them. Surviving commits whose parent was removed
// get their parent link cleared — the survivor becomes the first commit
// of its branch — and branch heads that pointed at a removed commit move
// to the nearest surviving ancestor or disappear. Deleting a branch head
// supersedes an in-flight job processing it (SB-125). Deletion never
// triggers pipelines: the surviving revisions were already processed.
func (d *daemon) deleteCommit(ref string) error {
	rec, err := d.resolveCommitRef(ref)
	if err != nil {
		return err
	}
	// the deletion set: the commit and every commit derived from it
	deleted := map[string]bool{rec.ID: true}
	for {
		grown := false
		for _, cm := range d.allCommitRecs() {
			if deleted[cm.ID] {
				continue
			}
			for _, leaf := range d.provenanceOf(cm.ID, map[string]bool{}) {
				if deleted[leaf] {
					deleted[cm.ID] = true
					grown = true
					break
				}
			}
		}
		if !grown {
			break
		}
	}
	// cancel in-flight jobs that consumed a deleted commit, then remove
	// every affected job record (SB-124: job history reflects the removal;
	// SB-125: the in-flight job is superseded, not left running)
	for _, j := range d.mustListJobs() {
		hit := false
		for _, ic := range j.InputCommits {
			if deleted[ic] {
				hit = true
				break
			}
		}
		if !hit {
			continue
		}
		d.cancelJob(j.ID) // a no-op for terminal jobs
		os.RemoveAll(d.jobDir(j.ID))
		os.Remove(d.logPath(j.ID))
	}
	// branch heads that point at a removed commit move to the nearest
	// surviving ancestor, or the ref goes away. The repair is computed
	// before the records are removed — a deleted head's parent chain is
	// unrecoverable afterwards.
	type headFix struct {
		repo, branch, newHead string
	}
	var fixes []headFix
	// every repo directory — including the internal "spec" definition
	// repository, which listRepos hides (SB-127): a deleted spec commit
	// must not leave a stale branch head (SB-164 abort cleanup)
	repoDirs, err := os.ReadDir(d.store.Dir())
	if err == nil {
		for _, rd := range repoDirs {
			if !rd.IsDir() || strings.HasPrefix(rd.Name(), ".") {
				continue
			}
			refsDir := filepath.Join(d.store.RepoDir(rd.Name()), "refs")
			entries, err := os.ReadDir(refsDir)
			if err != nil {
				continue
			}
			for _, e := range entries {
				head := d.store.HeadCommit(rd.Name(), e.Name())
				if head == "" || !deleted[head] {
					continue
				}
				newHead := ""
				for cur := head; ; {
					cm, err := d.store.LoadCommit(rd.Name(), cur)
					if err != nil || cm.ParentID == "" {
						break
					}
					cur = cm.ParentID
					if !deleted[cur] {
						newHead = cur
						break
					}
				}
				fixes = append(fixes, headFix{repo: rd.Name(), branch: e.Name(), newHead: newHead})
			}
		}
	}
	// repair surviving commits: a removed parent is relinked to the
	// nearest surviving ancestor (or cleared when none exists), so the
	// branch chain stays connected. The walk needs the removed commits'
	// records, so the fixes are captured before they are removed.
	type parentFix struct {
		cm     *store.CommitRec
		newPar string
	}
	var pfixes []parentFix
	for _, cm := range d.allCommitRecs() {
		if deleted[cm.ID] || cm.ParentID == "" {
			continue
		}
		if deleted[cm.ParentID] {
			newPar := ""
			for cur := cm.ParentID; cur != ""; {
				if !deleted[cur] {
					newPar = cur
					break
				}
				prec, err := d.store.LoadCommit(cm.Repo, cur)
				if err != nil || prec.ParentID == "" {
					break
				}
				cur = prec.ParentID
			}
			pfixes = append(pfixes, parentFix{cm: cm, newPar: newPar})
		}
	}
	// remove the commit records, then apply the parent repairs
	for _, cm := range d.allCommitRecs() {
		if deleted[cm.ID] {
			os.Remove(d.store.CommitPath(cm.Repo, cm.ID))
		}
	}
	for _, pf := range pfixes {
		pf.cm.ParentID = pf.newPar
		d.store.SaveCommit(pf.cm)
	}
	// apply the captured head repairs
	for _, fx := range fixes {
		if fx.newHead == "" {
			os.Remove(filepath.Join(d.store.RepoDir(fx.repo), "refs", fx.branch))
		} else {
			d.store.SetHead(fx.repo, fx.branch, fx.newHead)
		}
	}
	return nil
}
