package main

// M10 — batched cancels must settle under one shared deadline, not the
// sum of per-job 30s waits: cancelPipelineJobs (update/delete),
// cancelAllRunningJobs (shutdown), and deleteCommit each cancelled
// in-flight jobs sequentially, blocking the API handler past the client
// timeout whenever a handful of jobs were in flight.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sandman/internal/store"
)

// TestDeleteCommitClosureLinear — deleteCommit's derived-commit closure
// must complete in one indexed pass. The old computation re-read every
// job record per provenance hop (jobByOutput -> mustListJobs) inside
// repeated full scans: quadratic in commits x jobs, and a mid-suite
// delete with a couple hundred commits wedged the API handler past the
// client timeout (M10). The chain here is linear: commit c000 feeds a
// job whose output c001 feeds the next, up to c200.
func TestDeleteCommitClosureLinear(t *testing.T) {
	dir := t.TempDir()
	d := &daemon{state: dir, store: store.New(dir), runner: processRunner{}, running: map[string]*runningJob{}}
	const n = 200
	commitDir := filepath.Join(dir, "repos", "r", "commits")
	if err := os.MkdirAll(commitDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "repos", "r", "refs"), 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= n; i++ {
		id := fmt.Sprintf("c%03d", i)
		parent := ""
		if i > 0 {
			parent = fmt.Sprintf("c%03d", i-1)
		}
		b, _ := json.Marshal(store.CommitRec{ID: id, Repo: "r", Branch: "master", ParentID: parent, Started: true, Finished: true, CreatedAt: now()})
		if err := os.WriteFile(filepath.Join(commitDir, id+".json"), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= n; i++ {
		jid := fmt.Sprintf("j%03d", i)
		jdir := filepath.Join(dir, "jobs", jid)
		if err := os.MkdirAll(jdir, 0o755); err != nil {
			t.Fatal(err)
		}
		rec := jobRec{ID: jid, Pipeline: "p", State: "success",
			InputCommits: []string{fmt.Sprintf("c%03d", i-1)},
			OutputCommit: fmt.Sprintf("c%03d", i),
			Started:      now(),
			Finished:     now(),
		}
		b, _ := json.Marshal(rec)
		if err := os.WriteFile(filepath.Join(jdir, "job.json"), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	head := fmt.Sprintf("c%03d", n)
	if err := os.WriteFile(filepath.Join(dir, "repos", "r", "refs", "master"), []byte(head), 0o644); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := d.deleteCommit("c000"); err != nil {
		t.Fatalf("deleteCommit: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("deleteCommit took %v for a %d-commit chain; the closure re-scans per hop (M10)", elapsed, n)
	}
	// the whole chain was removed: every job record and commit is gone,
	// and the branch head ref went away with them
	for i := 1; i <= n; i++ {
		if _, err := os.Stat(filepath.Join(dir, "jobs", fmt.Sprintf("j%03d", i))); err == nil {
			t.Fatalf("job j%03d not removed", i)
		}
	}
	for i := 0; i <= n; i++ {
		if _, err := os.Stat(filepath.Join(commitDir, fmt.Sprintf("c%03d.json", i))); err == nil {
			t.Fatalf("commit c%03d not removed", i)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "repos", "r", "refs", "master")); !os.IsNotExist(err) {
		t.Fatalf("branch head ref survived deleting the whole chain (err %v)", err)
	}
}

// failRunner never kills anything, so a cancelled job's kill loop keeps
// retrying and the job never settles: the wedged-job shape.
type failRunner struct{}

func (failRunner) Run(JobSpec) RunResult { return RunResult{} }
func (failRunner) Kill(string) error     { return errors.New("no such container") }

func stuckJob(pipeline, id string) *runningJob {
	rj := &runningJob{pipeline: pipeline, cancelCh: make(chan struct{}), done: make(chan struct{})}
	rj.started.Store(true)
	return rj
}

// TestCancelPipelineJobsSharedBudget — cancelling a batch of stuck
// in-flight jobs must return within the shared budget. The old code
// waited 30s per job sequentially: with 3 stuck jobs the handler was
// wedged for 90s, blowing the client timeout (M10).
func TestCancelPipelineJobsSharedBudget(t *testing.T) {
	old := cancelSettleBudget
	cancelSettleBudget = 200 * time.Millisecond
	defer func() { cancelSettleBudget = old }()

	d := &daemon{runner: failRunner{}, running: map[string]*runningJob{}}
	for i := 0; i < 3; i++ {
		rj := stuckJob("p", fmt.Sprintf("j%d", i))
		d.running[fmt.Sprintf("j%d", i)] = rj
	}
	start := time.Now()
	d.cancelPipelineJobs("p")
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("cancelPipelineJobs took %v with 3 stuck jobs; the per-job waits serialize (M10)", elapsed)
	}
	for i := 0; i < 3; i++ {
		if !d.running[fmt.Sprintf("j%d", i)].cancelled.Load() {
			t.Fatalf("job j%d not marked cancelled", i)
		}
	}
}

// TestCancelAllRunningJobsSharedBudget — the shutdown sweep has the same
// shape and the same bound.
func TestCancelAllRunningJobsSharedBudget(t *testing.T) {
	old := cancelSettleBudget
	cancelSettleBudget = 200 * time.Millisecond
	defer func() { cancelSettleBudget = old }()

	d := &daemon{runner: failRunner{}, running: map[string]*runningJob{}}
	for i := 0; i < 3; i++ {
		rj := stuckJob("p", fmt.Sprintf("j%d", i))
		d.running[fmt.Sprintf("j%d", i)] = rj
	}
	start := time.Now()
	d.cancelAllRunningJobs()
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("cancelAllRunningJobs took %v with 3 stuck jobs (M10)", elapsed)
	}
}
