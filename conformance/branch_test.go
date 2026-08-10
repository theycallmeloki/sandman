// SB-142 — pipelines trigger on watched branch heads; output lands on the
// configured output branch; a downstream stage does not run until its
// watched branch is pointed at the output (branch promotion).
package conformance

import (
	"testing"
	"time"

	"sandman/client"
)

func TestSB142_DeferredProcessingAcrossBranches(t *testing.T) {
	data := uniq(t)
	mustRepo(t, data)
	// pipeline1 watches the default branch of data, but writes to "staging"
	p1 := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:         p1,
		Transform:    &client.Transform{Image: "alpine"},
		Input:        &client.Input{Repo: data, Glob: "/*"},
		OutputBranch: "staging",
	})
	// pipeline2 watches pipeline1's default branch — it must not run until
	// that branch is promoted onto the output commit
	p2 := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      p2,
		Transform: &client.Transform{Image: "alpine"},
		Input:     &client.Input{Repo: p1, Glob: "/*"},
	})

	// a commit on the non-watched "staging" branch of the data repo
	// produces no jobs at all
	cm := commitFiles(t, data, "staging", map[string]string{"file": "x"})
	if jobs, err := c.Flush(cm.ID, 5*time.Second); err != nil {
		t.Fatalf("flush of the non-watched-branch commit: %v", err)
	} else if len(jobs) != 0 {
		t.Fatalf("non-watched commit produced %d jobs, want 0", len(jobs))
	}

	// retargeting the watched branch onto the commit makes pipeline1
	// process it exactly once, writing its output to "staging"
	if err := c.CreateBranch(data, "master", cm.ID); err != nil {
		t.Fatalf("retarget data master: %v", err)
	}
	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("after retarget: %d jobs, want 1 (pipeline1 only)", len(jobs))
	}
	outCommit := jobs[0].OutputCommit
	if outCommit == "" {
		t.Fatalf("pipeline1 produced no output commit")
	}
	// the output landed on the configured branch, not the default
	if cm2, err := c.InspectCommit(outCommit); err != nil {
		t.Fatalf("inspect output: %v", err)
	} else if cm2.Branch != "staging" {
		t.Fatalf("output commit is on branch %q, want staging", cm2.Branch)
	}
	// pipeline2 watches p1's default branch: still nothing (count 1 total)
	time.Sleep(500 * time.Millisecond)
	all, err := c.ListJobs()
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("jobs after pipeline1 ran = %d, want 1 (pipeline2 waits for promotion)", len(all))
	}

	// promoting pipeline1's default branch onto the output commit lets
	// pipeline2 run: the same flush now reports both pipelines
	if err := c.CreateBranch(p1, "master", outCommit); err != nil {
		t.Fatalf("promote p1 master: %v", err)
	}
	jobs2 := flushOK(t, cm.ID)
	if len(jobs2) != 2 {
		t.Fatalf("after promotion: %d jobs, want 2 (one per triggered pipeline)", len(jobs2))
	}
	// the final output carries the file through both stages
	var p2Job client.Job
	for _, j := range jobs2 {
		if j.Pipeline == p2 {
			p2Job = j
		}
	}
	if p2Job.ID == "" {
		t.Fatalf("no pipeline2 job after promotion")
	}
	b, err := c.GetFile(p2Job.OutputCommit, "file")
	if err != nil {
		t.Fatalf("read p2 output: %v", err)
	}
	if string(b) != "x" {
		t.Fatalf("p2 output = %q, want x", string(b))
	}
}
