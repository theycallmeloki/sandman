package conformance

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// shq single-quotes s for /bin/sh.
func shq(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// hist returns a pointer to n for history-depth filters.
func hist(n int) *int { return &n }

// echoTransform writes content into the output as file "file".
func echoTransform(content string) *client.Transform {
	return &client.Transform{
		Image: "alpine",
		Cmd:   []string{"sh", "-c", fmt.Sprintf("echo -n %s > ${OUT}/file", shq(content))},
	}
}

// mustUpdate applies a new version of the pipeline (SB-040).
func mustUpdate(t *testing.T, name string, tr *client.Transform, in *client.Input, reprocess bool) {
	t.Helper()
	p := client.Pipeline{Name: name, Transform: tr, Input: in, Update: true, Reprocess: reprocess}
	if err := c.CreatePipeline(p); err != nil {
		t.Fatalf("update pipeline %s: %v", name, err)
	}
}

// commitCount returns the number of commits on a branch; an empty repo is 0.
func commitCount(t *testing.T, repo, branch string) int {
	t.Helper()
	chain, err := c.CommitHistory(repo, branch)
	if err != nil {
		if strings.Contains(err.Error(), "no head") || strings.Contains(err.Error(), "not found") {
			return 0
		}
		t.Fatalf("commit history of %s: %v", repo, err)
	}
	return len(chain)
}

// containerNames lists containers tagged with this daemon's node label
// (used to assert versioned participant replacement, SB-040).
func containerNames(t *testing.T) []string {
	t.Helper()
	out, err := exec.Command("docker", "ps", "-a",
		"--filter", "label=sandman.node="+daemonName,
		"--format", "{{.Names}}").Output()
	if err != nil {
		t.Fatalf("docker ps: %v", err)
	}
	var names []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line != "" {
			names = append(names, line)
		}
	}
	return names
}

// TestSB032_UpdatePipelineWithOnlyFailedJob — updating a pipeline whose only
// history is a failed job with no output commit must succeed (SB-032).
func TestSB032_UpdatePipelineWithOnlyFailedJob(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	exit1 := &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "exit 1"}}
	mustPipeline(t, client.Pipeline{Name: name, Transform: exit1, Input: &client.Input{Repo: repo, Glob: "/*"}})
	commitFiles(t, repo, "master", map[string]string{"file": "x"})
	pollFor(t, "job failed", 30*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
		return err == nil && len(js) > 0 && js[0].State == "failure"
	})
	// the update must not depend on any prior output commit
	mustUpdate(t, name, exit1, &client.Input{Repo: repo, Glob: "/*"}, false)
}

// TestSB033_AcceptReturnCode — a declared acceptable exit code turns a
// non-zero job exit into a success that still produces its output (SB-033).
func TestSB033_AcceptReturnCode(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	tr := &client.Transform{
		Image:            "alpine",
		Cmd:              []string{"sh", "-c", "echo -n ok > ${OUT}/file; exit 1"},
		AcceptReturnCode: 1,
	}
	mustPipeline(t, client.Pipeline{Name: name, Transform: tr, Input: &client.Input{Repo: repo, Glob: "/*"}})
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want 1", len(jobs))
	}
	if jobs[0].State != "success" {
		t.Fatalf("job state = %s, want success (reason %q)", jobs[0].State, jobs[0].Reason)
	}
	got, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("output = %q, want %q", got, "ok")
	}
}

// TestSB040_UpdateChangesTransform — an update swaps the transform for new
// jobs, preserves old jobs' transform in history, and provisions fresh
// participants (SB-040).
func TestSB040_UpdateChangesTransform(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	in := &client.Input{Repo: repo, Glob: "/*"}
	// created with the update flag on a nonexistent name: plain create
	mustPipeline(t, client.Pipeline{Name: name, Transform: echoTransform("foo"), Input: in, Update: true})
	cm1 := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	jobs1 := flushOK(t, cm1.ID)
	got, err := c.GetFile(jobs1[0].OutputCommit, "file")
	if err != nil || string(got) != "foo" {
		t.Fatalf("first output = %q (err %v), want foo", got, err)
	}

	mustUpdate(t, name, echoTransform("bar"), in, false)
	cm2 := commitFiles(t, repo, "master", map[string]string{"file": "y"})
	jobs2 := flushOK(t, cm2.ID)
	got, err = c.GetFile(jobs2[0].OutputCommit, "file")
	if err != nil || string(got) != "bar" {
		t.Fatalf("second output = %q (err %v), want bar", got, err)
	}

	// job history: 3 jobs newest-first; the two newest carry the new
	// transform, the original keeps its own (SB-040 clause 3)
	js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name, Full: true})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(js) != 3 {
		t.Fatalf("job list has %d jobs, want 3 (update reprocesses the head)", len(js))
	}
	for i, want := range []string{"bar", "bar", "foo"} {
		if js[i].Transform == nil || len(js[i].Transform.Cmd) != 3 || !strings.Contains(js[i].Transform.Cmd[2], want) {
			t.Fatalf("job %d transform = %+v, want echo %s", i, js[i].Transform, want)
		}
	}
	if js[0].Version != 2 || js[1].Version != 2 || js[2].Version != 1 {
		t.Fatalf("job versions = %d/%d/%d, want 2/2/1", js[0].Version, js[1].Version, js[2].Version)
	}

	// participants: no container of any version remains after settling
	pollFor(t, "no stale containers", 30*time.Second, func() bool {
		return len(containerNames(t)) == 0
	})

	// reprocessing update re-runs the head under the new transform (clause 5)
	mustUpdate(t, name, echoTransform("buzz"), in, true)
	flushOK(t, cm2.ID)
	head, err := c.HeadCommit(name, "master")
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	got, err = c.GetFile(head.ID, "file")
	if err != nil || string(got) != "buzz" {
		t.Fatalf("reprocessed output = %q (err %v), want buzz", got, err)
	}
}

// TestSB041_UpdateFinalizesPendingWork — updating while work is pending
// must not wedge later processing (SB-041; the stats-commit half is N/A
// until the stats branch exists).
func TestSB041_UpdateFinalizesPendingWork(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	slow := &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "sleep 5"}}
	in := &client.Input{Repo: repo, Glob: "/*"}
	mustPipeline(t, client.Pipeline{Name: name, Transform: slow, Input: in})
	cm1 := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	flushOK(t, cm1.ID)

	// a commit whose job is still in flight, then an update (a forcing
	// function for pending work)
	commitFiles(t, repo, "master", map[string]string{"file": "y"})
	pollFor(t, "job in flight", 30*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
		return err == nil && len(js) > 0 && js[0].State == "running"
	})
	mustUpdate(t, name, copyTransform(repo), in, false)

	cm3 := commitFiles(t, repo, "master", map[string]string{"file": "z"})
	jobs := flushOK(t, cm3.ID)
	if len(jobs) != 1 {
		t.Fatalf("post-update flush: %d jobs, want 1", len(jobs))
	}
}

// TestSB042_ManyUpdates — a pipeline updated many times in a row produces a
// new job and output commit per update, with monotonically growing history
// (SB-042; upstream runs this manually only).
func TestSB042_ManyUpdates(t *testing.T) {
	for _, reprocess := range []bool{true, false} {
		t.Run(fmt.Sprintf("reprocess=%v", reprocess), func(t *testing.T) {
			repo := uniq(t)
			mustRepo(t, repo)
			name := uniq(t)
			in := &client.Input{Repo: repo, Glob: "/*"}
			mustPipeline(t, client.Pipeline{Name: name, Transform: copyTransform(repo), Input: in})
			cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
			flushOK(t, cm.ID)
			prev := 1
			for i := 0; i < 8; i++ {
				mustUpdate(t, name, copyTransform(repo), in, reprocess)
				jobs := flushOK(t, cm.ID) // each update reprocesses the head
				if len(jobs) < 1 {
					t.Fatalf("update %d: flush yielded no job", i)
				}
				js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
				if err != nil {
					t.Fatalf("list jobs: %v", err)
				}
				if len(js) <= prev {
					t.Fatalf("update %d: job count %d did not grow past %d", i, len(js), prev)
				}
				prev = len(js)
			}
		})
	}
}

// TestSB043_CrashThenUpdate — a pipeline whose execution environment cannot
// be provisioned enters the crashed state with a reason; updating it to a
// working configuration returns it to running (SB-043).
func TestSB043_CrashThenUpdate(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	bad := &client.Transform{Image: "INVALID_IMAGE_REF", Cmd: []string{"sh", "-c", "true"}}
	mustPipeline(t, client.Pipeline{Name: name, Transform: bad, Input: &client.Input{Repo: repo, Glob: "/*"}})
	commitFiles(t, repo, "master", map[string]string{"file": "x"})
	// the first job cannot be provisioned: the pipeline crashes
	pollFor(t, "pipeline crashed", 30*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State == "crashed" && info.Reason != ""
	})
	// crashing pipelines do not schedule
	time.Sleep(500 * time.Millisecond)
	if js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: name}); len(js) == 0 {
		t.Fatalf("no job record after crash attempt")
	}

	mustUpdate(t, name, copyTransform(repo), &client.Input{Repo: repo, Glob: "/*"}, false)
	pollFor(t, "pipeline running", 30*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State == "running"
	})
	cm2 := commitFiles(t, repo, "master", map[string]string{"file": "y"})
	jobs := flushOK(t, cm2.ID)
	if len(jobs) != 1 {
		t.Fatalf("post-recovery flush: %d jobs, want 1", len(jobs))
	}
	got, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil || string(got) != "y" {
		t.Fatalf("output = %q (err %v), want y", got, err)
	}
}

// TestSB044_UpdateStoppedPipeline — updating a stopped pipeline applies the
// new version without restarting it; the paused backlog is processed on
// start (SB-044).
func TestSB044_UpdateStoppedPipeline(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	in := &client.Input{Repo: repo, Glob: "/*"}
	mustPipeline(t, client.Pipeline{Name: name, Transform: copyTransform(repo), Input: in})
	cm1 := commitFiles(t, repo, "master", map[string]string{"file": "foo"})
	flushOK(t, cm1.ID)
	if got := commitCount(t, name, "master"); got != 1 {
		t.Fatalf("output commits after first input: %d, want 1", got)
	}
	if err := c.StopPipeline(name); err != nil {
		t.Fatalf("stop: %v", err)
	}

	mustUpdate(t, name, copyTransform(repo), in, false)
	info, err := c.InspectPipeline(name)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if info.State != "paused" {
		t.Fatalf("state after update = %s, want paused (update must not restart)", info.State)
	}
	if info.Version != 2 {
		t.Fatalf("version after update = %d, want 2", info.Version)
	}
	if got := commitCount(t, name, "master"); got != 1 {
		t.Fatalf("output commits while stopped+updated: %d, want 1", got)
	}

	// input written while paused is accumulated, not processed
	cm2 := commitFiles(t, repo, "master", map[string]string{"file": "foobar"})
	if err := c.StartPipeline(name); err != nil {
		t.Fatalf("start: %v", err)
	}
	jobs := flushOK(t, cm2.ID)
	if len(jobs) != 1 {
		t.Fatalf("backlog flush: %d jobs, want 1", len(jobs))
	}
	if got := commitCount(t, name, "master"); got != 2 {
		t.Fatalf("output commits after start: %d, want 2", got)
	}
	got, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil || string(got) != "foobar" {
		t.Fatalf("output = %q (err %v), want foobar", got, err)
	}
}

// TestSB045_UpdateKillsInFlight — updating a pipeline with in-flight jobs
// terminates them as killed and completes the head commit under the new
// transform (SB-045).
func TestSB045_UpdateKillsInFlight(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	sleep1000 := &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "sleep 1000"}}
	in := &client.Input{Repo: repo, Glob: "/*"}
	mustPipeline(t, client.Pipeline{Name: name, Transform: sleep1000, Input: in})
	commitFiles(t, repo, "master", map[string]string{"file": "x"})
	cm2 := commitFiles(t, repo, "master", map[string]string{"file": "y"})
	pollFor(t, "two jobs in flight", 30*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
		return err == nil && len(js) == 2 && js[0].State == "running" && js[1].State == "running"
	})

	mustUpdate(t, name, copyTransform(repo), in, false)
	flushOK(t, cm2.ID)

	js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(js) != 3 {
		t.Fatalf("job list has %d jobs, want 3", len(js))
	}
	want := []string{"success", "killed", "killed"}
	for i, w := range want {
		if js[i].State != w {
			t.Fatalf("job %d state = %s, want %s", i, js[i].State, w)
		}
	}
}

// TestSB092_UpdateFixesFailingPipeline — updating a failing pipeline's
// command produces a new, successful job for the same input without
// creating a duplicate pipeline identity (SB-092).
func TestSB092_UpdateFixesFailingPipeline(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	exit1 := &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "exit 1"}}
	in := &client.Input{Repo: repo, Glob: "/*"}
	mustPipeline(t, client.Pipeline{Name: name, Transform: exit1, Input: in})
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	pollFor(t, "job failed", 30*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
		return err == nil && len(js) == 1 && js[0].State == "failure"
	})

	fixed := &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "echo -n bar > ${OUT}/file"}}
	mustUpdate(t, name, fixed, in, false) // no reprocess flag
	flushOK(t, cm.ID)                     // the update's head job runs under the fix

	js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(js) != 2 {
		t.Fatalf("job list has %d jobs, want 2", len(js))
	}
	if js[0].State != "success" {
		t.Fatalf("newest job state = %s, want success (reason %q)", js[0].State, js[0].Reason)
	}
	pipes, err := c.ListPipelines()
	if err != nil {
		t.Fatalf("list pipelines: %v", err)
	}
	count := 0
	for _, p := range pipes {
		if p.Name == name {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("pipeline %s appears %d times, want exactly 1 identity", name, count)
	}
}

// TestSB136_VersionAncestry — every update preserves a version addressable
// by ancestry depth; the current version is the most recent (SB-136).
func TestSB136_VersionAncestry(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo) // no commits: the pipeline never runs
	name := uniq(t)
	in := &client.Input{Repo: repo, Glob: "/*"}
	for i := 0; i < 5; i++ {
		tr := &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", fmt.Sprintf("echo %d > ${OUT}/file", i)}}
		if i == 0 {
			mustPipeline(t, client.Pipeline{Name: name, Transform: tr, Input: in, Update: true})
		} else {
			mustUpdate(t, name, tr, in, false)
		}
	}
	for k := 0; k < 5; k++ {
		info, err := c.InspectPipelineVersion(name, k)
		if err != nil {
			t.Fatalf("inspect ancestry %d: %v", k, err)
		}
		want := fmt.Sprintf("echo %d > ${OUT}/file", 4-k) // ancestry 0 = newest = version 5
		if info.Transform == nil || len(info.Transform.Cmd) != 3 || info.Transform.Cmd[2] != want {
			t.Fatalf("ancestry %d command = %+v, want %q", k, info.Transform, want)
		}
		if info.Version != 5-k {
			t.Fatalf("ancestry %d version = %d, want %d", k, info.Version, 5-k)
		}
	}
	// current version is referenced without ancestry
	cur, err := c.InspectPipeline(name)
	if err != nil {
		t.Fatalf("inspect current: %v", err)
	}
	if cur.Version != 5 {
		t.Fatalf("current version = %d, want 5", cur.Version)
	}
}

// TestSB143_VersionedHistory — updates create versioned history; job and
// pipeline listings honor history depth with exact boundaries, and one
// pipeline's activity leaves another's counts untouched (SB-143). The
// upstream counts differ in the middle stages because their scheduler
// re-attributes some jobs; the contract asserted here — one new job and one
// new output commit per version transition, monotonic accumulation, and
// exact depth boundaries — matches the record's stated invariants.
func TestSB143_VersionedHistory(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	in := &client.Input{Repo: repo, Glob: "/*"}
	mustPipeline(t, client.Pipeline{Name: name, Transform: copyTransform(repo), Input: in})
	cm1 := commitFiles(t, repo, "master", map[string]string{"file": "a"})
	flushOK(t, cm1.ID)
	if got := commitCount(t, name, "master"); got != 1 {
		t.Fatalf("commits after v1: %d, want 1", got)
	}

	mustUpdate(t, name, copyTransform(repo), in, false) // v2: head job
	flushOK(t, cm1.ID)
	if got := commitCount(t, name, "master"); got != 2 {
		t.Fatalf("commits after v2: %d, want 2", got)
	}
	cm2 := commitFiles(t, repo, "master", map[string]string{"file": "b"})
	flushOK(t, cm2.ID)
	mustUpdate(t, name, copyTransform(repo), in, false) // v3: head job, no new input
	flushOK(t, cm2.ID)
	if got := commitCount(t, name, "master"); got != 4 {
		t.Fatalf("commits after v3: %d, want 4", got)
	}

	depth := func(n int) []client.Job {
		t.Helper()
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name, History: &n})
		if err != nil {
			t.Fatalf("history %d: %v", n, err)
		}
		return js
	}
	d0 := depth(0) // current version only
	if len(d0) != 1 {
		t.Fatalf("history 0: %d jobs, want 1 (current version only)", len(d0))
	}
	if len(depth(1)) != 3 {
		t.Fatalf("history 1: %d jobs, want 3 (two most recent versions)", len(depth(1)))
	}
	if len(depth(2)) != 4 {
		t.Fatalf("history 2: %d jobs, want 4", len(depth(2)))
	}
	if len(depth(-1)) != 4 {
		t.Fatalf("history -1: %d jobs, want 4 (all versions)", len(depth(-1)))
	}
	all, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
	if err != nil || len(all) != 4 {
		t.Fatalf("plain listing: %d jobs (err %v), want 4", len(all), err)
	}

	// an unrelated pipeline leaves the first pipeline's counts untouched
	// (counts are scoped to this test's pipelines: the daemon is shared)
	other := uniq(t)
	mustPipeline(t, client.Pipeline{Name: other, Transform: copyTransform(repo), Input: in})
	countNamed := func(pipes []client.PipelineInfo, names ...string) int {
		n := 0
		for _, p := range pipes {
			for _, nm := range names {
				if p.Name == nm {
					n++
				}
			}
		}
		return n
	}
	pipes, err := c.ListPipelines()
	if err != nil || countNamed(pipes, name, other) != 2 {
		t.Fatalf("pipelines: %v entries for the test (err %v), want 2", countNamed(pipes, name, other), err)
	}
	neg1, err := c.ListPipelinesFiltered(hist(-1), "", false)
	if err != nil || countNamed(neg1, name, other) != 4 {
		t.Fatalf("pipeline history -1: %d for the test (err %v), want 4 (3 versions + 1)", countNamed(neg1, name, other), err)
	}
	one, err := c.ListPipelinesFiltered(hist(1), "", false)
	if err != nil || countNamed(one, name, other) != 2 {
		t.Fatalf("pipeline history 1: %d for the test (err %v), want 2 (latest per pipeline)", countNamed(one, name, other), err)
	}
	justName, err := c.ListPipelinesFiltered(hist(-1), name, false)
	if err != nil || len(justName) != 3 {
		t.Fatalf("history of %s: %d (err %v), want 3", name, len(justName), err)
	}
	if all2, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name}); err != nil || len(all2) != 4 {
		t.Fatalf("first pipeline jobs after second creation: %d (err %v), want 4", len(all2), err)
	}
}

// TestSB164_SpecCommitCleanup — pipeline definitions live in the spec
// repository, one commit per definition; a failed duplicate create leaves
// no extra spec commit (SB-164).
func TestSB164_SpecCommitCleanup(t *testing.T) {
	noPanic(t, c.Reset()) // fresh spec repository
	if got := commitCount(t, "spec", "master"); got != 0 {
		t.Fatalf("spec commits before any pipeline: %d, want 0", got)
	}
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	mustPipeline(t, client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	if got := commitCount(t, "spec", "master"); got != 1 {
		t.Fatalf("spec commits after create: %d, want 1", got)
	}
	// duplicate create without the update flag: error, no leak
	if err := c.CreatePipeline(client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}); err == nil {
		t.Fatalf("duplicate create without update succeeded")
	}
	if got := commitCount(t, "spec", "master"); got != 1 {
		t.Fatalf("spec commits after failed duplicate: %d, want 1 (no leak)", got)
	}
	// a failed creation (missing input repo) also leaves no spec commit
	if err := c.CreatePipeline(client.Pipeline{Name: uniq(t), Transform: copyTransform(repo), Input: &client.Input{Repo: uniq(t), Glob: "/*"}}); err == nil {
		t.Fatalf("create with missing input repo succeeded")
	}
	if got := commitCount(t, "spec", "master"); got != 1 {
		t.Fatalf("spec commits after failed create: %d, want 1", got)
	}
}
