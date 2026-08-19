package conformance

// The job API's live progress snapshot (InspectJob.Progress): a
// finished job reports the full datum count, every datum done, nothing
// running or queued, and a mean process time — the dashboard's progress
// bar and ETA inputs. The snapshot is computed from the job record, the
// pipeline dedup, and the per-worker status, so it must hold for jobs
// both with and without statistics enabled.

import (
	"testing"

	"sandman/client"
)

func TestJobProgressSnapshot(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      pipe,
		Transform: copyTransform(repo),
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	})

	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	for i, p := range []string{"a.txt", "b.txt", "c.txt"} {
		if err := c.PutFile(cm.ID, p, []byte("content-"+string(rune('a'+i)))); err != nil {
			t.Fatalf("put %s: %v", p, err)
		}
	}
	if _, err := c.FinishCommit(cm.ID, "", false); err != nil {
		t.Fatalf("finish commit: %v", err)
	}

	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want 1", len(jobs))
	}
	insp, err := c.InspectJob(jobs[0].ID)
	if err != nil {
		t.Fatalf("inspect job: %v", err)
	}
	if insp.Progress == nil {
		t.Fatalf("inspect job %s: Progress is nil", jobs[0].ID)
	}
	p := insp.Progress
	if p.Total != 3 {
		t.Fatalf("progress total = %d, want 3", p.Total)
	}
	if p.Done != 3 || p.Running != 0 || p.Queued != 0 {
		t.Fatalf("progress done/running/queued = %d/%d/%d, want 3/0/0",
			p.Done, p.Running, p.Queued)
	}
	if p.Failed != 0 {
		t.Fatalf("progress failed = %d, want 0", p.Failed)
	}
	if p.AvgProcessTime <= 0 {
		t.Fatalf("progress avgProcessTime = %v, want > 0", p.AvgProcessTime)
	}

	// the snapshot rides inspect only: lightweight listings stay lean
	listed, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(listed) == 0 || listed[0].Progress != nil {
		t.Fatalf("listed job carries Progress (%+v), want nil in listings", listed[0].Progress)
	}
}
