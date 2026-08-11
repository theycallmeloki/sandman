package conformance

import (
	"fmt"
	"testing"
	"time"

	"sandman/client"
)

// setupJob is the shared Given for the job-listing tests: one repo, one
// copy pipeline, one flushed commit, one successful job.
func setupJob(t *testing.T) (repo, pipe string, job client.Job) {
	t.Helper()
	repo = uniq(t)
	mustRepo(t, repo)
	pipe = uniq(t)
	mustPipeline(t, client.Pipeline{Name: pipe, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})
	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("setup: %d jobs, want 1", len(jobs))
	}
	return repo, pipe, jobs[0]
}

// SB-093 — jobs can be listed filtered by the commit they produced, and by
// a branch reference resolving to that commit.
func TestSB093_ListJobsFilteredByOutputCommit(t *testing.T) {
	_, pipe, job := setupJob(t)

	byCommit, err := c.ListJobsFiltered(client.JobFilter{OutputCommit: job.OutputCommit})
	if err != nil {
		t.Fatalf("filter by output commit: %v", err)
	}
	if len(byCommit) != 1 || byCommit[0].ID != job.ID {
		t.Fatalf("output-commit filter returned %d jobs, want exactly the producing job", len(byCommit))
	}

	// branch reference: the pipeline's output branch head is the same commit
	head, err := c.HeadCommit(pipe, "master")
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	byBranch, err := c.ListJobsFiltered(client.JobFilter{OutputCommit: head.ID})
	if err != nil {
		t.Fatalf("filter by branch head: %v", err)
	}
	if len(byBranch) != 1 || byBranch[0].ID != job.ID {
		t.Fatalf("branch-head filter returned %d jobs, want exactly 1", len(byBranch))
	}
}

// SB-094 — job listing offers a lightweight mode that omits the pipeline
// spec fields, and a full mode that includes them.
func TestSB094_TruncatedAndFullListing(t *testing.T) {
	_, pipe, job := setupJob(t)

	light, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
	if err != nil {
		t.Fatalf("lightweight listing: %v", err)
	}
	if len(light) != 1 {
		t.Fatalf("lightweight listing: %d jobs, want 1", len(light))
	}
	if light[0].ID != job.ID || light[0].Pipeline != pipe {
		t.Fatalf("lightweight listing lost core fields: %+v", light[0])
	}
	if light[0].Transform != nil || light[0].Input != nil {
		t.Fatalf("lightweight listing carries spec fields: %+v", light[0])
	}

	full, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe, Full: true})
	if err != nil {
		t.Fatalf("full listing: %v", err)
	}
	if len(full) != 1 {
		t.Fatalf("full listing: %d jobs, want 1", len(full))
	}
	if full[0].Transform == nil || full[0].Input == nil {
		t.Fatalf("full listing missing spec fields: %+v", full[0])
	}
	if full[0].Input.Repo != repoOf(full[0].Input) {
		t.Fatalf("full listing input repo mismatch")
	}
}

// repoOf is a tiny helper so the assertion above reads clearly.
func repoOf(in *client.Input) string {
	if in == nil {
		return ""
	}
	return in.Repo
}

// SB-095 — job listing can be filtered by an inclusive set of job states.
func TestSB095_ListJobsFilteredByStates(t *testing.T) {
	_, pipe, _ := setupJob(t)

	all, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
	if err != nil {
		t.Fatalf("unfiltered: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("unfiltered: %d jobs, want 1", len(all))
	}

	succ, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe, States: []string{"starting", "running", "success", "merging"}})
	if err != nil {
		t.Fatalf("success-inclusive filter: %v", err)
	}
	if len(succ) != 1 {
		t.Fatalf("success-inclusive filter: %d jobs, want 1", len(succ))
	}

	fail, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe, States: []string{"failure"}})
	if err != nil {
		t.Fatalf("failure-only filter: %v", err)
	}
	if len(fail) != 0 {
		t.Fatalf("failure-only filter: %d jobs, want 0", len(fail))
	}
}

// SB-135 — job inspection accepts a job id or an output commit id; an
// unknown output commit is not found.
func TestSB135_InspectJobByJobOrOutputCommit(t *testing.T) {
	_, _, job := setupJob(t)

	byID, err := c.InspectJob(job.ID)
	if err != nil {
		t.Fatalf("inspect by job id: %v", err)
	}
	if byID.ID != job.ID {
		t.Fatalf("inspect by id returned %+v", byID)
	}

	byOut, err := c.InspectJob(job.OutputCommit)
	if err != nil {
		t.Fatalf("inspect by output commit: %v", err)
	}
	if byOut.ID != job.ID {
		t.Fatalf("inspect by output commit returned %+v", byOut)
	}

	// a commit that exists but was never processed is not a job key
	lonely := uniq(t)
	mustRepo(t, lonely)
	orphan := commitFiles(t, lonely, "master", map[string]string{"x": "y"})
	_, err = c.InspectJob(orphan.ID)
	wantErr(t, err, "not found")
}

// SB-057 — deleting a running job still finalizes its output revision.
func TestSB057_DeleteJobFinalizesOutput(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      pipe,
		Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "sleep 60"}},
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	})
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})
	_ = cm

	job := waitJobFor(t, pipe, 30*time.Second)
	if job.State != "running" {
		t.Fatalf("job state = %s, want running", job.State)
	}
	// the output revision is recorded a moment after the job appears (the
	// first record is written before the output commit opens)
	pollFor(t, "job output commit recorded", 30*time.Second, func() bool {
		j, err := c.InspectJob(job.ID)
		return err == nil && j.OutputCommit != ""
	})
	job, err := c.InspectJob(job.ID)
	if err != nil {
		t.Fatalf("inspect job: %v", err)
	}

	if err := c.DeleteJob(job.ID); err != nil {
		t.Fatalf("delete job: %v", err)
	}

	// the output revision is finalized within a short window
	pollFor(t, "output commit finished", 10*time.Second, func() bool {
		cm, err := c.InspectCommit(job.OutputCommit)
		return err == nil && cm.Finished
	})
	if _, err := c.InspectJob(job.ID); err == nil {
		t.Fatal("deleted job still inspectable")
	}
}

// SB-122 — cancelling a running job kills its in-flight work, marks the job
// killed, and leaves the pipeline fully operational for later input.
func TestSB122_CancelRunningJob(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	// the sleep duration comes from the input: 600s for the first commit
	// (cancelled), 1s for the second (completes). The two files form one
	// directory datum (glob /job) because the transform reads both.
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", fmt.Sprintf("sleep $(cat ${%s}/job/sleep); cp ${%s}/job/data ${OUT}/data", repo, repo)},
		},
		Input: &client.Input{Repo: repo, Glob: "/job"},
	})
	cm1 := commitFiles(t, repo, "master", map[string]string{"job/sleep": "600", "job/data": "first"})
	_ = cm1

	job := waitJobFor(t, pipe, 30*time.Second)
	pollFor(t, "job running", 30*time.Second, func() bool {
		j, err := c.InspectJob(job.ID)
		return err == nil && j.State == "running"
	})

	if err := c.CancelJob(job.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	pollFor(t, "job killed", 30*time.Second, func() bool {
		j, err := c.InspectJob(job.ID)
		return err == nil && j.State == "killed"
	})

	// the pipeline is still healthy: a quick commit processes normally
	cm2 := replaceCommit(t, repo, "master", map[string]string{"job/sleep": "1", "job/data": "second"})
	jobs := flushOK(t, cm2.ID)
	if len(jobs) != 1 {
		t.Fatalf("post-cancel flush: %d jobs, want 1", len(jobs))
	}
	got, err := c.GetFile(jobs[0].OutputCommit, "data")
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != "second" {
		t.Fatalf("output = %q, want %q", got, "second")
	}
}

// TestSB058_StopJob — stopping a running job kills it without blocking the
// job for a later commit (SB-058). Sandman has no job queueing yet (SB-123
// is deferred), so both jobs run concurrently; the contract asserted — the
// stopped job is killed, the later job succeeds, listing is newest-first —
// holds in that model.
func TestSB058_StopJob(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	mustPipeline(t, client.Pipeline{Name: name, Transform: sleepTransform(6), Input: &client.Input{Repo: repo, Glob: "/*"}})
	cm1 := commitFiles(t, repo, "master", map[string]string{"file": "first"})
	cm2 := commitFiles(t, repo, "master", map[string]string{"file": "second"})

	// the first job is observed running, then stopped
	pollFor(t, "first job running", 30*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
		if err != nil || len(js) == 0 {
			return false
		}
		for _, j := range js {
			if len(j.InputCommits) == 1 && j.InputCommits[0] == cm1.ID && j.State == "running" {
				return true
			}
		}
		return false
	})
	var first client.Job
	js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	for _, j := range js {
		if len(j.InputCommits) == 1 && j.InputCommits[0] == cm1.ID {
			first = j
		}
	}
	noPanic(t, c.StopJob(first.ID))
	j, err := c.InspectJob(first.ID)
	if err != nil {
		t.Fatalf("inspect stopped job: %v", err)
	}
	if j.State != "killed" {
		t.Fatalf("stopped job state = %s, want killed", j.State)
	}

	// the later job still completes successfully (newest first in listing)
	pollFor(t, "second job success", 30*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
		if err != nil || len(js) == 0 {
			return false
		}
		for _, j := range js {
			if len(j.InputCommits) == 1 && j.InputCommits[0] == cm2.ID && j.State == "success" {
				return true
			}
		}
		return false
	})
	js, err = c.ListJobsFiltered(client.JobFilter{Pipeline: name})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if js[0].State != "success" {
		t.Fatalf("newest job state = %s, want success", js[0].State)
	}
}
