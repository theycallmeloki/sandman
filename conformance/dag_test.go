// DAG/provenance semantics: chains propagate one job per stage per wave,
// failures propagate forward, and mid-DAG commits never create extra
// waves (SB-021, SB-022, SB-056).
package conformance

import (
	"fmt"
	"testing"
	"time"

	"sandman/client"
)

// TestSB021_FlushChainRepeated — a 5-stage linear chain produces exactly
// one commit and one job per stage for each of 10 successive source
// commits (SB-021).
func TestSB021_FlushChainRepeated(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	var names []string
	prev := repo
	for i := 0; i < 5; i++ {
		name := uniq(t)
		names = append(names, name)
		mustPipeline(t, client.Pipeline{
			Name:      name,
			Transform: &client.Transform{Image: "alpine"}, // default entry point: copy
			Input:     &client.Input{Repo: prev, Glob: "/*"},
		})
		prev = name
	}

	for i := 0; i < 10; i++ {
		cm := commitFiles(t, repo, "master", map[string]string{"file": "foo\n"})
		jobs := flushOK(t, cm.ID)
		if len(jobs) != 5 {
			t.Fatalf("commit %d: flush returned %d jobs, want 5 (one per stage)", i, len(jobs))
		}
		// the file traversed the whole chain: the last stage's output has it
		var last client.Job
		for _, j := range jobs {
			if j.Pipeline == names[4] {
				last = j
			}
		}
		if last.ID == "" {
			t.Fatalf("commit %d: no job for the last stage", i)
		}
		b, err := c.GetFile(last.OutputCommit, "file")
		if err != nil {
			t.Fatalf("commit %d: read last-stage file: %v", i, err)
		}
		if string(b) != "foo\n" {
			t.Fatalf("commit %d: last-stage file = %q, want %q", i, string(b), "foo\n")
		}
	}
}

// TestSB022_FailedJobFailsDownstream — a failing stage fails every
// downstream stage, and the flush reports all three jobs with their
// terminal states instead of erroring (SB-022).
func TestSB022_FailedJobFailsDownstream(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pa, pb, pc := uniq(t), uniq(t), uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      pa,
		Transform: &client.Transform{Image: "alpine"},
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	})
	// B copies A's output but fails whenever the trigger file appears
	mustPipeline(t, client.Pipeline{
		Name: pb,
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", fmt.Sprintf("if [ -f ${%s}/trigger ]; then exit 1; fi; cp -r ${%s}/* ${OUT}/", pa, pa)},
		},
		Input: &client.Input{Repo: pa, Glob: "/*"},
	})
	mustPipeline(t, client.Pipeline{
		Name:      pc,
		Transform: &client.Transform{Image: "alpine"},
		Input:     &client.Input{Repo: pb, Glob: "/*"},
	})

	cm1 := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	jobs1 := flushOK(t, cm1.ID)
	if len(jobs1) != 3 {
		t.Fatalf("first flush returned %d jobs, want 3 (one per stage)", len(jobs1))
	}

	cm2 := commitFiles(t, repo, "master", map[string]string{"trigger": "boom"})
	jobs2, err := c.Flush(cm2.ID, 60*time.Second)
	if err != nil {
		t.Fatalf("flush of the failing commit: %v", err)
	}
	if len(jobs2) != 3 {
		t.Fatalf("second flush returned %d jobs, want 3", len(jobs2))
	}
	state := map[string]string{}
	for _, j := range jobs2 {
		state[j.Pipeline] = j.State
	}
	if state[pa] != "success" || state[pb] != "failure" || state[pc] != "failure" {
		t.Fatalf("chain states = %v, want %s=success, %s=failure, %s=failure", state, pa, pb, pc)
	}
}

// TestSB056_MidDAGCommitOneWavePerStage — in a DAG (A → B, cross(B × E)
// → C, C → D), the initial wave and each subsequent mid-DAG commit
// produce exactly one commit and one job per downstream stage, and
// unrelated stages are never re-triggered (SB-056).
func TestSB056_MidDAGCommitOneWavePerStage(t *testing.T) {
	repoA, repoE := uniq(t)+"a", uniq(t)+"e"
	mustRepo(t, repoA)
	mustRepo(t, repoE)
	pb, pc, pd := uniq(t), uniq(t), uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      pb,
		Transform: &client.Transform{Image: "alpine"},
		Input:     &client.Input{Repo: repoA, Glob: "/*"},
	})
	mustPipeline(t, client.Pipeline{
		Name: pc,
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", fmt.Sprintf("cat ${%s}/* ${%s}/* > ${OUT}/combined", pb, repoE)},
		},
		Input: &client.Input{Cross: []client.Input{
			{Name: pb, Repo: pb, Glob: "/*"},
			{Name: repoE, Repo: repoE, Glob: "/*"},
		}},
	})
	mustPipeline(t, client.Pipeline{
		Name:      pd,
		Transform: &client.Transform{Image: "alpine"},
		Input:     &client.Input{Repo: pc, Glob: "/*"},
	})

	// wave 1: initial commits of A and E
	a1 := commitFiles(t, repoA, "master", map[string]string{"afile": "a1"})
	e1 := commitFiles(t, repoE, "master", map[string]string{"efile": "e1"})
	flushOK(t, a1.ID) // B, C's pairing job, D — the wave's chain
	flushOK(t, e1.ID) // C's lone job settles too, producing nothing

	jobsD := func() []client.Job {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pd})
		if err != nil {
			t.Fatalf("list D jobs: %v", err)
		}
		return js
	}
	// exactly one job per downstream pipeline after wave 1
	if n := len(jobsD()); n != 1 {
		t.Fatalf("wave 1: D has %d jobs, want 1", n)
	}
	// wave 2: a mid-DAG commit to E — one new job in C and D, none in B
	e2 := commitFiles(t, repoE, "master", map[string]string{"efile": "e2"})
	jobs2 := flushOK(t, e2.ID)
	if len(jobs2) != 2 {
		t.Fatalf("wave 2 flush returned %d jobs, want 2 (C pairing job + D)", len(jobs2))
	}
	ds := jobsD()
	if len(ds) != 2 {
		t.Fatalf("after wave 2: D has %d jobs, want 2 (one per wave)", len(ds))
	}
	bs, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pb})
	if err != nil {
		t.Fatalf("list B jobs: %v", err)
	}
	if len(bs) != 1 {
		t.Fatalf("B has %d jobs, want 1 (E is not its input — never re-triggered)", len(bs))
	}
	// D's head output is coherent: both sides of the latest wave
	b, err := c.GetFile(ds[0].OutputCommit, "combined")
	if err != nil {
		t.Fatalf("read D output: %v", err)
	}
	if string(b) != "a1e2" {
		t.Fatalf("D output = %q, want %q", string(b), "a1e2")
	}
}
