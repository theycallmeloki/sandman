package conformance

import (
	"fmt"
	"testing"
	"time"

	"sandman/client"
)

// SB-023 — a pipeline created after its input history exists processes the
// current branch head: the head job sees the full accumulated content and
// flush of the head completes.
func TestSB023_PipelineAfterHistoryProcessesHead(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)

	var head client.Commit
	for i := 1; i <= 10; i++ {
		cm := commitFiles(t, repo, "master", map[string]string{fmt.Sprintf("f%d", i): fmt.Sprintf("%d", i)})
		head = cm
	}

	p := client.Pipeline{Name: uniq(t), Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}
	mustPipeline(t, p)

	jobs := flushOK(t, head.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want 1", len(jobs))
	}
	out := jobs[0].OutputCommit
	for i := 1; i <= 10; i++ {
		got, err := c.GetFile(out, fmt.Sprintf("f%d", i))
		if err != nil {
			t.Fatalf("read f%d: %v", i, err)
		}
		if string(got) != fmt.Sprintf("%d", i) {
			t.Fatalf("f%d content = %q, want %q", i, got, fmt.Sprintf("%d", i))
		}
	}
}

// SB-053 — a pipeline created over existing history processes only the
// current head, in one output commit, with the full accumulated content.
func TestSB053_HeadOnlyOneOutputCommit(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	commitFiles(t, repo, "master", map[string]string{"file": "foo\n"})
	head := overwriteCommit(t, repo, "master", map[string]string{"file": "foo\nbar\n"})

	name := uniq(t)
	p := client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}
	mustPipeline(t, p)

	jobs := flushOK(t, head.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want 1", len(jobs))
	}
	got, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != "foo\nbar\n" {
		t.Fatalf("output = %q, want %q", got, "foo\nbar\n")
	}
	hist, err := c.CommitHistory(name, "master")
	if err != nil {
		t.Fatalf("output history: %v", err)
	}
	if len(hist) != 1 {
		t.Fatalf("output history has %d commits, want exactly 1", len(hist))
	}
}

// SB-024 — deleting and recreating a pipeline reprocesses its input,
// producing fresh output and jobs; recreation is not blocked by the old
// incarnation.
func TestSB024_DeleteAndRecreateReprocesses(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})

	name := uniq(t)
	mk := func() {
		t.Helper()
		p := client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}
		mustPipeline(t, p)
	}

	mk()
	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("first incarnation: %d jobs, want 1", len(jobs))
	}
	if err := c.DeletePipeline(name, false, false); err != nil {
		t.Fatalf("delete: %v", err)
	}

	mk() // recreate with the same name
	jobs = flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("second incarnation: %d jobs, want exactly 1 (the fresh job)", len(jobs))
	}
	got, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != "foo" {
		t.Fatalf("output = %q, want %q", got, "foo")
	}
}

// SB-028 — pipeline lifecycle: running after creation, stopped/paused after
// stop (persistent Stopped flag), running again after start.
func TestSB028_LifecycleStates(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	p := client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}
	mustPipeline(t, p)

	info, err := c.InspectPipeline(name)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if info.State != "running" || info.Stopped {
		t.Fatalf("after create: state=%q stopped=%v, want running/false", info.State, info.Stopped)
	}

	if err := c.StopPipeline(name); err != nil {
		t.Fatalf("stop: %v", err)
	}
	pollFor(t, "pipeline stopped", 15*time.Second, func() bool {
		i, err := c.InspectPipeline(name)
		return err == nil && i.Stopped && i.State == "paused"
	})

	if err := c.StartPipeline(name); err != nil {
		t.Fatalf("start: %v", err)
	}
	pollFor(t, "pipeline running", 15*time.Second, func() bool {
		i, err := c.InspectPipeline(name)
		return err == nil && !i.Stopped && i.State == "running"
	})
}

// SB-048 — a stopped pipeline ignores new commits; restarting it processes
// the backlog of commits finished while it was stopped.
func TestSB048_StoppedIgnoresRestartBacklog(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	p := client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}
	mustPipeline(t, p)
	if err := c.StopPipeline(name); err != nil {
		t.Fatalf("stop: %v", err)
	}

	// while stopped: a commit must produce no job and no output commit
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo\n"})
	time.Sleep(1 * time.Second) // grace: give any (wrong) trigger time to appear
	jobs, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("stopped pipeline produced %d jobs, want 0", len(jobs))
	}

	if err := c.StartPipeline(name); err != nil {
		t.Fatalf("start: %v", err)
	}
	jobs = flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("backlog flush: %d jobs, want 1", len(jobs))
	}
	got, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != "foo\n" {
		t.Fatalf("output = %q, want %q", got, "foo\n")
	}
}

// SB-020 — stopping an intermediate pipeline of a chain does not create a
// spurious commit downstream; downstream commit counts are unaffected.
func TestSB020_StopMidChainNoSpuriousDownstreamCommit(t *testing.T) {
	repoA := uniq(t)
	mustRepo(t, repoA)
	cm1 := commitFiles(t, repoA, "master", map[string]string{"file": "foo"})

	pipeB := uniq(t)
	mustPipeline(t, client.Pipeline{Name: pipeB, Transform: copyTransform(repoA), Input: &client.Input{Repo: repoA, Glob: "/*"}})
	flushOK(t, cm1.ID) // B processes commit 1; repo B now exists

	// C consumes B's output repo
	pipeC := uniq(t)
	mustPipeline(t, client.Pipeline{Name: pipeC, Transform: copyTransform(pipeB), Input: &client.Input{Repo: pipeB, Glob: "/*"}})
	jobs := flushOK(t, cm1.ID) // now the whole chain: B's job and C's (backfilled) job
	if len(jobs) != 2 {
		t.Fatalf("chain flush returned %d jobs, want 2 (B and C)", len(jobs))
	}

	if err := c.StopPipeline(pipeB); err != nil {
		t.Fatalf("stop B: %v", err)
	}

	cm2 := commitFiles(t, repoA, "master", map[string]string{"file": "bar"})
	time.Sleep(1 * time.Second) // grace for any wrong trigger
	histC, err := c.CommitHistory(pipeC, "master")
	if err != nil {
		t.Fatalf("C history: %v", err)
	}
	if len(histC) != 1 {
		t.Fatalf("C has %d commits after stopping B, want exactly 1", len(histC))
	}
	histB, err := c.CommitHistory(pipeB, "master")
	if err != nil {
		t.Fatalf("B history: %v", err)
	}
	if len(histB) != 1 {
		t.Fatalf("B has %d commits after stop, want exactly 1", len(histB))
	}
	_ = cm2
}

// SB-034/SB-035 — restarting the daemon loses no pipelines, repos, or
// commits: the pipeline returns to running and everything inspects cleanly.
func TestSB034_RestartKeepsDataAndPipelines(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	p := client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}
	mustPipeline(t, p)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})
	jobs := flushOK(t, cm.ID)
	out := jobs[0].OutputCommit

	restartDaemon(t)

	pollFor(t, "pipeline running after restart", 15*time.Second, func() bool {
		i, err := c.InspectPipeline(name)
		return err == nil && i.State == "running"
	})
	if _, err := c.InspectRepo(repo); err != nil {
		t.Fatalf("repo lost on restart: %v", err)
	}
	if got, err := c.InspectCommit(cm.ID); err != nil || !got.Finished {
		t.Fatalf("commit lost on restart: %v (finished=%v)", err, got.Finished)
	}
	data, err := c.GetFile(out, "file")
	if err != nil {
		t.Fatalf("output lost on restart: %v", err)
	}
	if string(data) != "foo" {
		t.Fatalf("output = %q after restart, want %q", data, "foo")
	}
}

func TestSB035_RestartSingleInstance(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	p := client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}
	mustPipeline(t, p)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})
	flushOK(t, cm.ID)

	restartDaemon(t)

	if _, err := c.InspectPipeline(name); err != nil {
		t.Fatalf("pipeline lost on restart: %v", err)
	}
	if _, err := c.InspectRepo(repo); err != nil {
		t.Fatalf("repo lost on restart: %v", err)
	}
	if _, err := c.InspectCommit(cm.ID); err != nil {
		t.Fatalf("commit lost on restart: %v", err)
	}
}
