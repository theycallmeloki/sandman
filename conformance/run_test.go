// SB-010 — manual pipeline runs: a new job on demand, explicit provenance
// selects exact revisions, runs never propagate downstream, provenance
// validation, and reruns of failing pipelines.
package conformance

import (
	"testing"
	"time"

	"sandman/client"
)

func TestSB010_RunPipeline(t *testing.T) {
	t.Run("cross run with provenance", func(t *testing.T) {
		repo := uniq(t)
		mustRepo(t, repo)
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name: pipe,
			Transform: &client.Transform{
				Image: "alpine",
				Cmd:   []string{"sh", "-c", "cat ${a}/file ${b}/file > ${OUT}/file"},
			},
			Input: &client.Input{Cross: []client.Input{
				{Name: "a", Repo: repo, Glob: "/*", Branch: "branchA"},
				{Name: "b", Repo: repo, Glob: "/*", Branch: "branchB"},
			}},
		})

		a1 := commitFiles(t, repo, "branchA", map[string]string{"file": "data A\n"})
		b1 := commitFiles(t, repo, "branchB", map[string]string{"file": "data B\n"})
		// the normal wave settles: a1's lone job plus the a1×b1 pairing
		flushOK(t, a1.ID)
		flushOK(t, b1.ID)
		n := func() int {
			js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
			if err != nil {
				t.Fatalf("list jobs: %v", err)
			}
			return len(js)
		}
		if n() != 2 {
			t.Fatalf("before any run: %d jobs, want 2", n())
		}

		// a run with no provenance creates a new job over the current heads
		j1, err := c.RunPipeline(pipe, nil, "")
		if err != nil {
			t.Fatalf("run with no provenance: %v", err)
		}
		waitTerminal(t, j1.ID)
		if n() != 3 {
			t.Fatalf("after a plain run: %d jobs, want 3", n())
		}

		// a run with explicit provenance processes exactly those revisions
		a2 := replaceCommit(t, repo, "branchA", map[string]string{"file": "data A2\n"})
		b2 := replaceCommit(t, repo, "branchB", map[string]string{"file": "data B2\n"})
		flushOK(t, a2.ID)
		flushOK(t, b2.ID)
		j2, err := c.RunPipeline(pipe, []string{a1.ID, b2.ID}, "")
		if err != nil {
			t.Fatalf("run with provenance: %v", err)
		}
		waitTerminal(t, j2.ID)
		j2i, err := c.InspectJob(j2.ID)
		if err != nil {
			t.Fatalf("inspect run job: %v", err)
		}
		if j2i.State != "success" {
			t.Fatalf("run job state = %s (reason %q)", j2i.State, j2i.Reason)
		}
		// the requested revisions, not the heads (which would include data A2)
		b, err := c.GetFile(j2i.OutputCommit, "file")
		if err != nil {
			t.Fatalf("read run output: %v", err)
		}
		if string(b) != "data A\ndata B2\n" {
			t.Fatalf("run output = %q, want the exact requested revisions (data A + data B2)", string(b))
		}
	})

	t.Run("runs do not propagate", func(t *testing.T) {
		repo := uniq(t)
		mustRepo(t, repo)
		up, down := uniq(t), uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      up,
			Transform: &client.Transform{Image: "alpine"},
			Input:     &client.Input{Repo: repo, Glob: "/*"},
		})
		mustPipeline(t, client.Pipeline{
			Name:      down,
			Transform: &client.Transform{Image: "alpine"},
			Input:     &client.Input{Repo: up, Glob: "/*"},
		})
		cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
		flushOK(t, cm.ID)

		count := func(name string) int {
			js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
			if err != nil {
				t.Fatalf("list %s jobs: %v", name, err)
			}
			return len(js)
		}
		if count(down) != 1 {
			t.Fatalf("downstream jobs before run = %d, want 1", count(down))
		}
		// a manual upstream run must not create downstream jobs
		j, err := c.RunPipeline(up, nil, "")
		if err != nil {
			t.Fatalf("run upstream: %v", err)
		}
		waitTerminal(t, j.ID)
		if count(down) != 1 {
			t.Fatalf("downstream jobs after upstream run = %d, want still 1", count(down))
		}
		// running the downstream manually with its own job id re-executes
		// the pairing, adding a job rather than replacing it
		downJob := latestJob(t, down)
		j2, err := c.RunPipeline(down, nil, downJob.ID)
		if err != nil {
			t.Fatalf("run downstream with job id: %v", err)
		}
		waitTerminal(t, j2.ID)
		if count(down) != 2 {
			t.Fatalf("downstream jobs after manual rerun = %d, want 2", count(down))
		}
	})

	t.Run("validation", func(t *testing.T) {
		repo := uniq(t)
		mustRepo(t, repo)
		unrelated := uniq(t)
		mustRepo(t, unrelated)
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine"},
			Input:     &client.Input{Repo: repo, Glob: "/*"},
		})
		cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
		flushOK(t, cm.ID)
		uc := commitFiles(t, unrelated, "master", map[string]string{"f": "y"})

		// a pipeline with no input commits cannot be run
		empty := uniq(t)
		mustRepo(t, empty)
		pipe2 := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe2,
			Transform: &client.Transform{Image: "alpine"},
			Input:     &client.Input{Repo: empty, Glob: "/*"},
		})
		if _, err := c.RunPipeline(pipe2, nil, ""); err == nil {
			t.Fatalf("run with no input commits: expected error")
		}

		// a provenance commit outside the input lineage errors
		if _, err := c.RunPipeline(pipe, []string{uc.ID}, ""); err == nil {
			t.Fatalf("run with an unrelated provenance commit: expected error")
		} else if !containsStr(err.Error(), "not part of the pipeline's input") {
			t.Fatalf("unrelated-commit error = %q", err.Error())
		}
		// two commits of the same branch error
		cm2 := commitFiles(t, repo, "master", map[string]string{"file": "x2"})
		flushOK(t, cm2.ID)
		if _, err := c.RunPipeline(pipe, []string{cm.ID, cm2.ID}, ""); err == nil {
			t.Fatalf("run with two commits of one branch: expected error")
		} else if !containsStr(err.Error(), "two commits") {
			t.Fatalf("same-branch error = %q", err.Error())
		}
	})

	t.Run("rerun failing pipeline", func(t *testing.T) {
		repo := uniq(t)
		mustRepo(t, repo)
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name: pipe,
			Transform: &client.Transform{
				Image: "alpine",
				Cmd:   []string{"sh", "-c", "exit 7"},
			},
			Input: &client.Input{Repo: repo, Glob: "/*"},
		})
		cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
		if _, err := c.Flush(cm.ID, 60*time.Second); err != nil {
			t.Fatalf("flush of the failing pipeline: %v", err)
		}

		j, err := c.RunPipeline(pipe, nil, "")
		if err != nil {
			t.Fatalf("run failing pipeline: %v", err)
		}
		waitTerminal(t, j.ID)
		ji, _ := c.InspectJob(j.ID)
		if ji.State != "failure" {
			t.Fatalf("rerun state = %s, want failure", ji.State)
		}
		// deleting the pipeline twice is not an error (SB-010 clause 11)
		if err := c.DeletePipeline(pipe, false, false); err != nil {
			t.Fatalf("first delete: %v", err)
		}
		if err := c.DeletePipeline(pipe, false, false); err != nil {
			t.Fatalf("second delete of the same pipeline: %v", err)
		}
	})

	t.Run("empty upstream output still processes downstream union", func(t *testing.T) {
		// SB-010 clause 9 / RunPipelineEmptyUpstream: a downstream union
		// pipeline succeeds when its upstream produced an EMPTY output
		// (the upstream cross had data on only one side), and after the
		// second branch lands the union combines: branch input + upstream
		// output = "data A\ndata A\ndata B\n".
		repo := uniq(t)
		mustRepo(t, repo)
		up, down := uniq(t), uniq(t)
		mustPipeline(t, client.Pipeline{
			Name: up,
			Transform: &client.Transform{
				Image: "alpine",
				Cmd:   []string{"sh", "-c", "cat ${a}/file ${b}/file > ${OUT}/file"},
			},
			Input: &client.Input{Cross: []client.Input{
				{Name: "a", Repo: repo, Glob: "/*", Branch: "branchA"},
				{Name: "b", Repo: repo, Glob: "/*", Branch: "branchB"},
			}},
		})
		mustPipeline(t, client.Pipeline{
			Name: down,
			Transform: &client.Transform{
				Image: "alpine",
				Cmd:   []string{"sh", "-c", "cp ${u}/file ${OUT}/file"},
			},
			Input: &client.Input{Name: "u", Union: []client.Input{
				{Name: "a", Repo: repo, Glob: "/*", Branch: "branchA"},
				{Name: "up", Repo: up, Glob: "/*"},
			}},
		})

		// wave 1: only branchA has data — the upstream cross has nothing
		// to process (empty output), yet the downstream union still
		// succeeds on its direct branch input
		a1 := commitFiles(t, repo, "branchA", map[string]string{"file": "data A\n"})
		flushOK(t, a1.ID)
		latest := func() string {
			j := latestJob(t, down)
			if j.OutputCommit == "" {
				t.Fatalf("downstream job %s has no output commit", j.ID)
			}
			b, err := c.GetFile(j.OutputCommit, "file")
			if err != nil {
				t.Fatalf("read downstream output: %v", err)
			}
			return string(b)
		}
		if got := latest(); got != "data A\n" {
			t.Fatalf("wave-1 downstream output = %q, want %q", got, "data A\n")
		}

		// wave 2: branchB lands — the upstream produces its cross output
		// and the union combines branch input + upstream output
		b1 := commitFiles(t, repo, "branchB", map[string]string{"file": "data B\n"})
		flushOK(t, b1.ID)
		if got := latest(); got != "data A\ndata A\ndata B\n" {
			t.Fatalf("wave-2 downstream output = %q, want %q", got, "data A\ndata A\ndata B\n")
		}
	})

	t.Run("run with statistics", func(t *testing.T) {
		repo := uniq(t)
		mustRepo(t, repo)
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:        pipe,
			Transform:   &client.Transform{Image: "alpine"},
			Input:       &client.Input{Repo: repo, Glob: "/*"},
			EnableStats: true,
		})
		cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
		flushOK(t, cm.ID)

		j, err := c.RunPipeline(pipe, nil, "")
		if err != nil {
			t.Fatalf("run stats pipeline: %v", err)
		}
		waitTerminal(t, j.ID)
		if ji, _ := c.InspectJob(j.ID); ji.State != "success" {
			t.Fatalf("stats run state = %s", ji.State)
		}
		// the input commit can still be flushed afterwards (no hang)
		if _, err := c.Flush(cm.ID, 60*time.Second); err != nil {
			t.Fatalf("flush after run: %v", err)
		}
	})
}

// waitTerminal blocks until the job settles.
func waitTerminal(t *testing.T, id string) {
	t.Helper()
	pollFor(t, "job "+id+" to settle", 90*time.Second, func() bool {
		j, err := c.InspectJob(id)
		return err == nil && j.State != "running"
	})
}

// latestJob returns the most recently started job of a pipeline.
func latestJob(t *testing.T, pipeline string) client.Job {
	t.Helper()
	js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipeline})
	if err != nil {
		t.Fatalf("list %s jobs: %v", pipeline, err)
	}
	if len(js) == 0 {
		t.Fatalf("no jobs for %s", pipeline)
	}
	latest := js[0]
	for _, j := range js[1:] {
		if j.Started > latest.Started {
			latest = j
		}
	}
	return latest
}
