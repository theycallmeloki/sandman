// Job queueing: a pipeline's jobs run one at a time in commit order, and
// the system stays correct under a burst of rapid revisions (SB-123,
// SB-121).
package conformance

import (
	"fmt"
	"testing"
	"time"

	"sandman/client"
)

// TestSB123_QueueSerializes — with parallelism 1, jobs run strictly one at
// a time; cancelling the running job lets the next queued job start, and
// cancelling one job never cancels the queued others (SB-123).
func TestSB123_QueueSerializes(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "sleep 600"},
		},
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		Parallelism: &client.Parallelism{Constant: 1},
	})

	const n = 10
	var cms []client.Commit
	for i := 0; i < n; i++ {
		cms = append(cms, commitFiles(t, repo, "master", map[string]string{fmt.Sprintf("f%d", i): "x"}))
	}

	// Each commit's job comes up strictly one at a time, in commit order.
	// The active job is the one with a started output commit; queued jobs
	// have records but no output commit yet, terminal jobs are settled.
	var active client.Job
	for i, cm := range cms {
		pollFor(t, fmt.Sprintf("job of commit %d to be the sole active job", i), 60*time.Second, func() bool {
			jobs, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
			if err != nil {
				return false
			}
			var actives []client.Job
			for _, j := range jobs {
				if j.State == "running" && j.OutputCommit != "" {
					actives = append(actives, j)
				}
			}
			if len(actives) != 1 {
				return false // only one job may run at a time
			}
			for _, ic := range actives[0].InputCommits {
				if ic == cm.ID {
					active = actives[0]
					return true
				}
			}
			return false // a different commit's job is active — wrong order
		})
		if err := c.CancelJob(active.ID); err != nil {
			t.Fatalf("cancel job %s: %v", active.ID, err)
		}
		pollFor(t, fmt.Sprintf("job %s killed after cancel", active.ID), 60*time.Second, func() bool {
			j, err := c.InspectJob(active.ID)
			return err == nil && j.State == "killed"
		})
	}

	// the pipeline is fully operational after the burst of cancellations:
	// a fresh commit still schedules and runs
	cm := commitFiles(t, repo, "master", map[string]string{"final": "y"})
	pollFor(t, "post-burst job to become the sole active job", 60*time.Second, func() bool {
		jobs, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		if err != nil {
			return false
		}
		var actives []client.Job
		for _, j := range jobs {
			if j.State == "running" && j.OutputCommit != "" {
				actives = append(actives, j)
			}
		}
		if len(actives) != 1 {
			return false
		}
		for _, ic := range actives[0].InputCommits {
			if ic == cm.ID {
				active = actives[0]
				return true
			}
		}
		return false
	})
	if err := c.CancelJob(active.ID); err != nil {
		t.Fatalf("cancel post-burst job %s: %v", active.ID, err)
	}
	pollFor(t, "post-burst job killed", 60*time.Second, func() bool {
		j, err := c.InspectJob(active.ID)
		return err == nil && j.State == "killed"
	})
}

// TestSB121_BurstManyPipelines — a burst of rapid revisions across many
// pipelines is fully consumed: every revision gets a job, the final
// revision converges, and the job index stays queryable (SB-121). Scaled
// from the reference's 5000 commits × 10 pipelines — skipped upstream as
// too long for CI — to 120 × 3, exercising the same queue mechanics.
func TestSB121_BurstManyPipelines(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	const pipes = 3
	const commits = 120
	var names []string
	for i := 0; i < pipes; i++ {
		name := uniq(t)
		names = append(names, name)
		mustPipeline(t, client.Pipeline{
			Name:        name,
			Transform:   &client.Transform{Image: "alpine:3.21"}, // default entry point: fast copy
			Input:       &client.Input{Repo: repo, Glob: "/*"},
			Parallelism: &client.Parallelism{Constant: 1},
		})
	}
	var last client.Commit
	for i := 0; i < commits; i++ {
		last = commitFiles(t, repo, "master", map[string]string{fmt.Sprintf("f%03d", i): "x"})
	}

	// the final revision is eventually processed to completion: flush
	// converges on the head job of every pipeline
	jobs := flushOK(t, last.ID)
	if len(jobs) != pipes {
		t.Fatalf("flush returned %d jobs, want one per pipeline (%d)", len(jobs), pipes)
	}

	// every revision was accepted and consumed: one job per commit per
	// pipeline, and the whole index stays listable
	all, err := c.ListJobs() // no filter: the index must stay queryable
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(all) < pipes*commits {
		t.Fatalf("job listing has %d jobs, want at least %d", len(all), pipes*commits)
	}
	for _, name := range names {
		jobs, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
		if err != nil {
			t.Fatalf("list jobs of %s: %v", name, err)
		}
		if len(jobs) != commits {
			t.Fatalf("pipeline %s has %d jobs, want %d (one per revision)", name, len(jobs), commits)
		}
	}

	// the final output of every pipeline holds the final revision's data
	for _, j := range jobs {
		b, err := c.GetFile(j.OutputCommit, fmt.Sprintf("f%03d", commits-1))
		if err != nil {
			t.Fatalf("read final file from %s: %v", j.OutputCommit, err)
		}
		if string(b) != "x" {
			t.Fatalf("final file = %q, want x", string(b))
		}
	}
}
