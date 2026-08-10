package conformance

// Standby family (SB-025/049/050/158, D-09/11/12): a standby-enabled
// pipeline idles in the standby state and activates only when work
// arrives, returning to standby once the work settles. D-11: no fixed cap
// on concurrent activation (staggering is a tuning choice, not asserted).
// D-12: warm participant reuse is an implementation detail, not a
// requirement. D-09: there is no degraded/crashing standby state —
// partial-capacity conditions surface as failure (or crashed for
// provisioning errors); the standby contract is the lifecycle itself.

import (
	"fmt"
	"testing"
	"time"

	"sandman/client"
)

// standbyTransform is the standard copy transform (per SB-001).
func standbyTransform(inputName string) *client.Transform {
	return copyTransform(inputName)
}

// chainTransform copies the input view including the empty case: the
// glob form (`${in}/*`) fails when the view is empty, and an empty commit
// in a standby chain produces empty views downstream.
func chainTransform(inputName string) *client.Transform {
	return &client.Transform{
		Image: "alpine",
		Cmd:   []string{"sh", "-c", fmt.Sprintf("cp -r ${%s}/. ${OUT}/", inputName)},
	}
}

func standbyState(t *testing.T, name string) string {
	t.Helper()
	p, err := c.InspectPipeline(name)
	if err != nil {
		return ""
	}
	return p.State
}

func TestSB025_DeleteStandbyPipeline(t *testing.T) {
	repo := uniq(t) + "r"
	pipe := uniq(t) + "p"
	mustRepo(t, repo)
	mustPipeline(t, client.Pipeline{Name: pipe, Standby: true, Transform: standbyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})

	// settles into standby shortly after creation, without any input
	pollFor(t, "pipeline "+pipe+" in standby", 30*time.Second, func() bool {
		return standbyState(t, pipe) == "standby"
	})

	// new input wakes it; the flush completes
	cm := commitFiles(t, repo, "", map[string]string{"file": "foo\n"})
	flushOK(t, cm.ID)
	pollFor(t, "pipeline "+pipe+" back in standby", 30*time.Second, func() bool {
		return standbyState(t, pipe) == "standby"
	})

	// deletion completes and inspection fails within a bounded window; the
	// system stays healthy (no leaked monitoring wedging the controller)
	if err := c.DeletePipeline(pipe, false, false); err != nil {
		t.Fatalf("delete standby pipeline: %v", err)
	}
	pipelineGone(t, pipe)

	next := uniq(t) + "q"
	mustPipeline(t, client.Pipeline{Name: next, Transform: standbyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	flushOK(t, cm.ID) // the replacement pipeline still works end to end
}

func TestSB049_StandbyChainAndManyCommits(t *testing.T) {
	repo := uniq(t) + "r"
	mustRepo(t, repo)

	// a chain of 10 standby pipelines, created together in one transaction:
	// each consumes the previous one, whose output repo does not exist yet
	// (SB-162 cross-references). With no input history nothing is
	// scheduled, so all 10 settle into standby.
	tx, err := c.StartTransaction()
	if err != nil {
		t.Fatalf("start transaction: %v", err)
	}
	const n = 10
	var names [n]string
	prev := repo
	for i := 0; i < n; i++ {
		names[i] = uniq(t) + fmt.Sprintf("p%d", i)
		if err := c.CreatePipelineTx(client.Pipeline{Name: names[i], Standby: true,
			Transform: chainTransform(prev), Input: &client.Input{Repo: prev, Glob: "/*"}}, tx); err != nil {
			t.Fatalf("stage chain pipeline %d: %v", i, err)
		}
		prev = names[i]
	}
	if err := c.FinishTransaction(tx); err != nil {
		t.Fatalf("finish transaction: %v", err)
	}
	pollFor(t, "all 10 standby pipelines in standby", 60*time.Second, func() bool {
		for _, name := range names {
			if standbyState(t, name) != "standby" {
				return false
			}
		}
		return true
	})

	// new input (an empty commit) wakes the chain: the flush completes end
	// to end and every pipeline returns to standby. D-11: the reference's
	// "at most 2 active" cap is NOT contractual — no fixed cap is asserted.
	empty := emptyCommit(t, repo)
	flushOK(t, empty.ID)
	pollFor(t, "chain back in standby after processing", 60*time.Second, func() bool {
		for _, name := range names {
			if standbyState(t, name) != "standby" {
				return false
			}
		}
		return true
	})

	// many consecutive commits: 100 empty commits wake one standby
	// pipeline 100 times; every job succeeds and the pipeline rests again.
	// D-12: the reference's same-participant marker is an implementation
	// detail, not a requirement — the observable contract is that the work
	// is processed without per-job reconfiguration.
	repo2 := uniq(t) + "m"
	mustRepo(t, repo2)
	mp := uniq(t) + "mp"
	mustPipeline(t, client.Pipeline{Name: mp, Standby: true, Transform: chainTransform(repo2), Input: &client.Input{Repo: repo2, Glob: "/*"}})
	pollFor(t, "pipeline "+mp+" in standby", 30*time.Second, func() bool {
		return standbyState(t, mp) == "standby"
	})
	for i := 0; i < 100; i++ {
		emptyCommit(t, repo2)
		time.Sleep(10 * time.Millisecond) // keep the docker stampede gentle
	}
	pollFor(t, "100 jobs all success", 90*time.Second, func() bool {
		jobs, err := c.ListJobsFiltered(client.JobFilter{Pipeline: mp})
		if err != nil || len(jobs) != 100 {
			return false
		}
		for _, j := range jobs {
			if j.State != "success" {
				return false
			}
		}
		return true
	})
	pollFor(t, "pipeline "+mp+" back in standby", 30*time.Second, func() bool {
		return standbyState(t, mp) == "standby"
	})
}

// emptyCommit starts and finishes a commit with no files.
func emptyCommit(t *testing.T, repo string) client.Commit {
	t.Helper()
	cm, err := c.StartCommit(repo, "", "")
	if err != nil {
		t.Fatalf("start empty commit: %v", err)
	}
	fin, err := c.FinishCommit(cm.ID, "", false)
	if err != nil {
		t.Fatalf("finish empty commit: %v", err)
	}
	return fin
}

func TestSB050_StopStandbyPipeline(t *testing.T) {
	repo := uniq(t) + "r"
	pipe := uniq(t) + "p"
	mustRepo(t, repo)
	mustPipeline(t, client.Pipeline{Name: pipe, Standby: true, Transform: standbyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})

	// one data write runs to completion, then the pipeline rests
	cm1 := commitFiles(t, repo, "", map[string]string{"file": "foo\n"})
	flushOK(t, cm1.ID)
	pollFor(t, "standby after the initial run", 30*time.Second, func() bool {
		return standbyState(t, pipe) == "standby"
	})

	// stopping transitions it to paused
	if err := c.StopPipeline(pipe); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if st := standbyState(t, pipe); st != "paused" {
		t.Fatalf("after stop: state = %s, want paused", st)
	}

	// commits written while paused never wake it: no new jobs, state never
	// leaves paused
	for i := 0; i < 3; i++ {
		commitFiles(t, repo, "", map[string]string{fmt.Sprintf("f%d", i): "x\n"})
	}
	time.Sleep(2 * time.Second) // grace: any wrong wake-up would appear here
	jobs, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("paused pipeline produced %d jobs, want exactly 1 (the pre-stop run)", len(jobs))
	}
	if st := standbyState(t, pipe); st != "paused" {
		t.Fatalf("while paused: state = %s, want paused", st)
	}

	// starting processes the accumulated commits together: exactly one
	// additional output commit, so the history totals exactly 2
	if err := c.StartPipeline(pipe); err != nil {
		t.Fatalf("start: %v", err)
	}
	head, err := c.HeadCommit(repo, "master")
	if err != nil {
		t.Fatalf("input head: %v", err)
	}
	flushOK(t, head.ID)
	hist, err := c.CommitHistory(pipe, "master")
	if err != nil {
		t.Fatalf("output history: %v", err)
	}
	if len(hist) != 2 {
		t.Fatalf("output history has %d commits, want exactly 2 (pre-stop run + post-restart run)", len(hist))
	}
	// and the accumulated content made it through (the restart job's view
	// is the head, so all three paused commits were consumed together)
	if data, err := c.GetFile(hist[len(hist)-1].ID, "f2"); err != nil || string(data) != "x\n" {
		t.Fatalf("restart output f2 = %q (err %v), want %q", data, err, "x\n")
	}
	pollFor(t, "standby again after restart", 30*time.Second, func() bool {
		return standbyState(t, pipe) == "standby"
	})
}

func TestSB158_StandbyLifecycle(t *testing.T) {
	// D-09: no degraded/crashing standby state in Sandman — partial
	// capacity surfaces as failure or crashed; the extracted contract is
	// the lifecycle: idle in standby, wake on input, rest after the work.
	repo := uniq(t) + "r"
	pipe := uniq(t) + "p"
	mustRepo(t, repo)
	mustPipeline(t, client.Pipeline{Name: pipe, Standby: true, Transform: standbyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	pollFor(t, "idle in standby", 30*time.Second, func() bool {
		return standbyState(t, pipe) == "standby"
	})

	// input activates: the running state is observable while the job runs
	cm := commitFiles(t, repo, "", map[string]string{"file": "foo\n"})
	pollFor(t, "running while the job runs", 30*time.Second, func() bool {
		return standbyState(t, pipe) == "running"
	})
	flushOK(t, cm.ID)
	pollFor(t, "resting in standby again", 30*time.Second, func() bool {
		return standbyState(t, pipe) == "standby"
	})

	// a provisioning failure degrades to crashed (SB-043), never to a
	// standby-resting state — the D-09 mapping of partial capacity
	bad := uniq(t) + "bad"
	mustPipeline(t, client.Pipeline{Name: bad, Standby: true,
		Transform: &client.Transform{Image: "INVALID_IMAGE_REF", Cmd: []string{"true"}},
		Input:     &client.Input{Repo: repo, Glob: "/*"}})
	cm2 := commitFiles(t, repo, "", map[string]string{"file": "bar\n"})
	_ = cm2
	pollFor(t, "pipeline "+bad+" crashed", 60*time.Second, func() bool {
		p, err := c.InspectPipeline(bad)
		return err == nil && p.State == "crashed"
	})
}
