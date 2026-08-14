// Error handling and execution bounds: the error-handling command recovers
// failing datums, and per-datum / per-job timeouts cut execution at the
// boundary (the datum-timeout's stats-dependent assertions live in the
// stats batch).
package conformance

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// TestErrorHandlingCommands — a secondary command runs when the
// primary fails for a datum: recovered datums count toward success,
// failed datums fail the job, and updating the pipeline re-runs recovered
// and failed datums while unchanged successful ones are skipped.
func TestErrorHandlingCommands(t *testing.T) {
	// ---- (ErrCmd) three files, primary succeeds only for file1, error
	// handler succeeds only for file3 ----
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file1": "a", "file2": "b", "file3": "c"})

	pipe := uniq(t)
	primary := func(name string) []string {
		return []string{"sh", "-c", fmt.Sprintf("test -f ${%s}/file1", name)}
	}
	recoverFile3 := func(name string) []string {
		return []string{"sh", "-c", fmt.Sprintf("test -f ${%s}/file3", name)}
	}
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image:  "alpine:3.21",
			Cmd:    primary(repo),
			ErrCmd: recoverFile3(repo),
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	})

	jobs, err := c.Flush(cm.ID, 60*time.Second)
	if err != nil {
		t.Fatalf("flush v1: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("v1: %d jobs, want 1", len(jobs))
	}
	j1 := jobs[0]
	if j1.State != "failure" {
		t.Fatalf("v1 job state = %s, want failure (file2 failed)", j1.State)
	}
	if j1.Processed != 1 || j1.Recovered != 1 || j1.Failed != 1 || j1.Skipped != 0 {
		t.Fatalf("v1 counters = p%d r%d f%d s%d, want 1/1/1/0",
			j1.Processed, j1.Recovered, j1.Failed, j1.Skipped)
	}

	// v2: the error handler always succeeds — the same input re-runs
	// recovered/failed datums under the new definition; file1 (unchanged,
	// successful) is skipped
	mustUpdate(t, pipe, &client.Transform{
		Image:  "alpine:3.21",
		Cmd:    primary(repo),
		ErrCmd: []string{"sh", "-c", "true"},
	}, &client.Input{Repo: repo, Glob: "/*"}, false)
	jobs, err = c.Flush(cm.ID, 60*time.Second)
	if err != nil {
		t.Fatalf("flush v2: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("v2: %d jobs, want 1", len(jobs))
	}
	j2 := jobs[0]
	if j2.State != "success" {
		t.Fatalf("v2 job state = %s, want success (reason %q)", j2.State, j2.Reason)
	}
	if j2.Processed != 0 || j2.Recovered != 2 || j2.Failed != 0 || j2.Skipped != 1 {
		t.Fatalf("v2 counters = p%d r%d f%d s%d, want 0/2/0/1",
			j2.Processed, j2.Recovered, j2.Failed, j2.Skipped)
	}

	// ---- (RecoveredDatums) one file; primary always fails, handler
	// always succeeds: the datum is recovered, the job succeeds ----
	repo2 := uniq(t)
	mustRepo(t, repo2)
	cm2 := commitFiles(t, repo2, "master", map[string]string{"file": "x"})
	pipe2 := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe2,
		Transform: &client.Transform{
			Image:  "alpine:3.21",
			Cmd:    []string{"sh", "-c", "exit 1"},
			ErrCmd: []string{"sh", "-c", "true"},
		},
		Input: &client.Input{Repo: repo2, Glob: "/*"},
	})
	jobs, err = c.Flush(cm2.ID, 60*time.Second)
	if err != nil {
		t.Fatalf("flush recovered v1: %v", err)
	}
	if len(jobs) != 1 || jobs[0].State != "success" {
		t.Fatalf("recovered v1 job = %+v, want one success", jobs)
	}
	if jobs[0].Processed != 0 || jobs[0].Recovered != 1 || jobs[0].Failed != 0 {
		t.Fatalf("recovered v1 counters = p%d r%d f%d, want 0/1/0",
			jobs[0].Processed, jobs[0].Recovered, jobs[0].Failed)
	}

	// v2: the primary now succeeds and there is no error handler — the
	// previously recovered datum is processed, not skipped, because its
	// outcome was not a clean success
	mustUpdate(t, pipe2, &client.Transform{
		Image: "alpine:3.21",
		Cmd:   []string{"sh", "-c", "true"},
	}, &client.Input{Repo: repo2, Glob: "/*"}, false)
	jobs, err = c.Flush(cm2.ID, 60*time.Second)
	if err != nil {
		t.Fatalf("flush recovered v2: %v", err)
	}
	if len(jobs) != 1 || jobs[0].State != "success" {
		t.Fatalf("recovered v2 job = %+v, want one success", jobs)
	}
	if jobs[0].Processed != 1 || jobs[0].Recovered != 0 || jobs[0].Failed != 0 {
		t.Fatalf("recovered v2 counters = p%d r%d f%d, want 1/0/0",
			jobs[0].Processed, jobs[0].Recovered, jobs[0].Failed)
	}
}

// TestDatumTimeoutControl — a datum that finishes inside its
// configured per-datum timeout is unaffected and the job succeeds.
func TestDatumTimeoutControl(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image:        "alpine:3.21",
			Cmd:          []string{"sh", "-c", fmt.Sprintf("sleep 10; cp -r ${%s}/* ${OUT}/", repo)},
			DatumTimeout: "20s",
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	})

	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want 1", len(jobs))
	}
	got, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil || string(got) != "foo" {
		t.Fatalf("output = %q (err %v), want foo (10s work inside the 20s window)", got, err)
	}
}

// TestJobTimeout — a whole-job timeout kills the job at the
// boundary: state KILLED (not FAILED), duration equal to the timeout
// within tolerance, and the job is observable while running.
func TestJobTimeout(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file1": "a", "file2": "b"})
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image:      "alpine:3.21",
			Cmd:        []string{"sh", "-c", "sleep 20"},
			JobTimeout: "20s",
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	})

	// the job is visible while running, even though it is destined to die
	job := waitJobFor(t, pipe, 30*time.Second)
	pollFor(t, "job running", 30*time.Second, func() bool {
		j, err := c.InspectJob(job.ID)
		return err == nil && j.State == "running"
	})

	jobs, err := c.Flush(cm.ID, 90*time.Second)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want 1", len(jobs))
	}
	j := jobs[0]
	if j.State != "killed" {
		t.Fatalf("job state = %s, want killed (a job timeout is not a failure)", j.State)
	}
	started, err1 := time.Parse(time.RFC3339Nano, j.Started)
	finished, err2 := time.Parse(time.RFC3339Nano, j.Finished)
	if err1 != nil || err2 != nil {
		t.Fatalf("job timestamps unparsable: %q %q", j.Started, j.Finished)
	}
	dur := finished.Sub(started)
	if d := dur - 20*time.Second; d > time.Second || d < -time.Second {
		t.Fatalf("job duration = %s, want 20s +- 1s (killed at the boundary)", dur)
	}
	if !strings.Contains(j.Reason, "cancelled") && !strings.Contains(j.Reason, "killed") {
		t.Fatalf("killed job reason %q should reflect cancellation", j.Reason)
	}
}
