package conformance

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// sleepTransform sleeps for the given seconds (keeps jobs in flight).
func sleepTransform(secs int) *client.Transform {
	return &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "sleep " + strconv.Itoa(secs)}}
}

// pipelineGone polls until the pipeline disappears from listing.
func pipelineGone(t *testing.T, name string) {
	t.Helper()
	pollFor(t, "pipeline "+name+" deleted", 30*time.Second, func() bool {
		pipes, err := c.ListPipelines()
		if err != nil {
			return false
		}
		for _, p := range pipes {
			if p.Name == name {
				return false
			}
		}
		return true
	})
}

// TestSB026_DeleteMidDAGGuard — a pipeline whose output feeds a downstream
// pipeline cannot be deleted without force; leaf-first deletion succeeds
// and removes the jobs; listing jobs of a deleted pipeline errors (SB-026).
func TestSB026_DeleteMidDAGGuard(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	up := uniq(t)
	down := uniq(t)
	mustPipeline(t, client.Pipeline{Name: up, Transform: sleepTransform(3), Input: &client.Input{Repo: repo, Glob: "/*"}})
	flushOK(t, cm.ID)
	mustPipeline(t, client.Pipeline{Name: down, Transform: copyTransform(up), Input: &client.Input{Repo: up, Glob: "/*"}})

	// non-forced delete of the mid-DAG pipeline is refused
	wantErr(t, c.DeletePipeline(up, false, false), "downstream")
	if _, err := c.InspectPipeline(up); err != nil {
		t.Fatalf("guarded pipeline must remain: %v", err)
	}

	// leaf-first deletion succeeds and converges
	noPanic(t, c.DeletePipeline(down, false, false))
	pipelineGone(t, down)
	noPanic(t, c.DeletePipeline(up, false, false)) // cancels the in-flight job
	pipelineGone(t, up)

	// jobs of deleted pipelines are gone; listing for a deleted pipeline errors
	for _, p := range []string{up, down} {
		if _, err := c.ListJobsFiltered(client.JobFilter{Pipeline: p}); err == nil {
			t.Fatalf("job listing for deleted pipeline %s did not error", p)
		}
	}
	if js, err := c.ListJobs(); err != nil {
		t.Fatalf("list jobs: %v", err)
	} else {
		for _, j := range js {
			if j.Pipeline == up || j.Pipeline == down {
				t.Fatalf("orphan job %s of deleted pipeline %s", j.ID, j.Pipeline)
			}
		}
	}

	// recreate both, then force-delete the mid-DAG pipeline. The upstream's
	// output repository reappears when its backfill job starts; the
	// downstream needs it to exist before it can be created.
	mustPipeline(t, client.Pipeline{Name: up, Transform: sleepTransform(3), Input: &client.Input{Repo: repo, Glob: "/*"}})
	pollFor(t, "upstream output repo recreated", 30*time.Second, func() bool {
		_, err := c.InspectRepo(up)
		return err == nil
	})
	mustPipeline(t, client.Pipeline{Name: down, Transform: copyTransform(up), Input: &client.Input{Repo: up, Glob: "/*"}})
	noPanic(t, c.DeletePipeline(up, true, false))
	pipelineGone(t, up)
	noPanic(t, c.DeletePipeline(down, false, false)) // cleanup: down is a leaf
	pipelineGone(t, down)
}

// TestSB027_SplitTransactionDelete — the same deletion contract holds when
// each pipeline is deleted as a multi-step transaction: jobs first, then
// the pipeline (SB-027).
func TestSB027_SplitTransactionDelete(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	up := uniq(t)
	down := uniq(t)
	mustPipeline(t, client.Pipeline{Name: up, Transform: sleepTransform(3), Input: &client.Input{Repo: repo, Glob: "/*"}})
	flushOK(t, cm.ID)
	mustPipeline(t, client.Pipeline{Name: down, Transform: copyTransform(up), Input: &client.Input{Repo: up, Glob: "/*"}})

	wantErr(t, c.DeletePipeline(up, false, false), "downstream")

	splitDelete := func(name string) {
		t.Helper()
		for {
			js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
			if err != nil {
				break // jobs already gone
			}
			if len(js) == 0 {
				break
			}
			if err := c.DeleteJob(js[0].ID); err != nil {
				t.Fatalf("delete job %s: %v", js[0].ID, err)
			}
		}
		noPanic(t, c.DeletePipeline(name, false, false))
	}
	splitDelete(down)
	pipelineGone(t, down)
	splitDelete(up)
	pipelineGone(t, up)

	for _, p := range []string{up, down} {
		if _, err := c.ListJobsFiltered(client.JobFilter{Pipeline: p}); err == nil {
			t.Fatalf("job listing for deleted pipeline %s did not error", p)
		}
	}
	if js, err := c.ListJobs(); err != nil {
		t.Fatalf("list jobs: %v", err)
	} else {
		for _, j := range js {
			if j.Pipeline == up || j.Pipeline == down {
				t.Fatalf("orphan job %s of deleted pipeline %s", j.ID, j.Pipeline)
			}
		}
	}

	// recreate, then force-delete the mid-DAG pipeline
	mustPipeline(t, client.Pipeline{Name: up, Transform: sleepTransform(3), Input: &client.Input{Repo: repo, Glob: "/*"}})
	pollFor(t, "upstream output repo recreated", 30*time.Second, func() bool {
		_, err := c.InspectRepo(up)
		return err == nil
	})
	mustPipeline(t, client.Pipeline{Name: down, Transform: copyTransform(up), Input: &client.Input{Repo: up, Glob: "/*"}})
	noPanic(t, c.DeletePipeline(up, true, false))
	pipelineGone(t, up)
	noPanic(t, c.DeletePipeline(down, false, false))
	pipelineGone(t, down)
}

// TestSB030_DeleteRepoAfterMembershipChange — a repository with a finished
// commit stays deletable after the serving membership changes (approximated
// for a single-node fabric by a control-plane restart; SB-030).
func TestSB030_DeleteRepoAfterMembershipChange(t *testing.T) {
	for pass := 0; pass < 2; pass++ {
		repo := uniq(t)
		mustRepo(t, repo)
		commitFiles(t, repo, "master", map[string]string{"file": "x"})
		restartDaemon(t) // serving membership changed
		noPanic(t, c.DeleteRepo(repo, false))
		repos, err := c.ListRepos()
		if err != nil {
			t.Fatalf("list repos: %v", err)
		}
		for _, r := range repos {
			if r.Name == repo {
				t.Fatalf("repo %s still listed after delete", repo)
			}
		}
	}
}

// TestSB037_FullReset — a reset removes every repository, pipeline, and
// job, and is idempotent (SB-037).
func TestSB037_FullReset(t *testing.T) {
	withIsolatedDaemon(t) // resetting the shared daemon would wipe every test's state (M10)
	noPanic(t, c.Reset()) // the test itself begins with a reset
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	mustPipeline(t, client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	flushOK(t, cm.ID)

	noPanic(t, c.Reset())
	for what, n := range map[string]int{
		"repos":     len(mustList(t, c.ListRepos)),
		"pipelines": len(mustList(t, c.ListPipelines)),
	} {
		if n != 0 {
			t.Fatalf("%s after reset: %d entries, want 0", what, n)
		}
	}
	if js, err := c.ListJobs(); err != nil || len(js) != 0 {
		t.Fatalf("jobs after reset: %d (err %v), want 0", len(js), err)
	}
	noPanic(t, c.Reset()) // idempotent
	mustRepo(t, repo)     // names are reusable (SB-130)
}

// mustList applies f and returns the entries, failing on error.
func mustList[T any](t *testing.T, f func() ([]T, error)) []T {
	t.Helper()
	out, err := f()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	return out
}

// TestSB127_SpecRepoProtected — the internal specification repository
// cannot be deleted through the public repository API (SB-127).
func TestSB127_SpecRepoProtected(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	mustPipeline(t, client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})

	wantErr(t, c.DeleteRepo("spec", false), "cannot be deleted")
	// normal user repositories remain deletable
	noPanic(t, c.DeleteRepo(repo, false))
	// the pipeline stays the source of truth
	if _, err := c.InspectPipeline(name); err != nil {
		t.Fatalf("pipeline lost after spec-repo guard: %v", err)
	}
}

// TestSB131_ResetRobustness — reset always completes with healthy metadata
// and names stay reusable (SB-130/131). Sandman's product decision D-08
// overrides the reference's corruption tolerance: corrupted metadata makes
// the reset error rather than being silently ignored, and removing the
// corrupted record restores the reset path.
func TestSB131_ResetRobustness(t *testing.T) {
	withIsolatedDaemon(t) // corrupts daemon state; never the shared one (M10)
	for i := 0; i < 3; i++ {
		repo := uniq(t)
		mustRepo(t, repo)
		name := uniq(t)
		mustPipeline(t, client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
		commitFiles(t, repo, "master", map[string]string{"file": "x"})
		noPanic(t, c.Reset())
	}
	// D-08: corrupted metadata is an error for the reset path
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	mustPipeline(t, client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	if err := os.WriteFile(filepath.Join(daemonStateDir, "pipelines", name+".json"), []byte("{corrupt"), 0o644); err != nil {
		t.Fatalf("corrupt record: %v", err)
	}
	wantErr(t, c.Reset(), "metadata")
	// removing the corrupted record restores the recovery path
	if err := os.Remove(filepath.Join(daemonStateDir, "pipelines", name+".json")); err != nil {
		t.Fatalf("remove corrupt record: %v", err)
	}
	noPanic(t, c.Reset())
}

// TestSB144_IncompletePipeline — a pipeline whose definition content is
// lost becomes incomplete: unlistable and un-updatable in ordinary modes,
// listed by name with AllowIncomplete, and deletable (SB-144).
func TestSB144_IncompletePipeline(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	mustPipeline(t, client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})

	// lose the definition content: the record becomes unreadable
	if err := os.WriteFile(filepath.Join(daemonStateDir, "pipelines", name+".json"), []byte("{"), 0o644); err != nil {
		t.Fatalf("corrupt record: %v", err)
	}
	if _, err := c.InspectPipeline(name); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("inspect of incomplete pipeline: err = %v, want incomplete error", err)
	}
	if _, err := c.ListPipelines(); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("ordinary listing with incomplete pipeline: err = %v, want error", err)
	}
	pipes, err := c.ListPipelinesFiltered(nil, "", true)
	if err != nil {
		t.Fatalf("allowIncomplete listing: %v", err)
	}
	found := 0
	for _, p := range pipes {
		if p.Name == name {
			found++
			if p.State != "" {
				t.Fatalf("incomplete pipeline carries state %q; only the name is recoverable", p.State)
			}
		}
	}
	if found != 1 {
		t.Fatalf("allowIncomplete listing shows %d entries for %s, want exactly 1", found, name)
	}
	// update must not repair or recreate it
	if err := c.CreatePipeline(client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}, Update: true}); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("update of incomplete pipeline: err = %v, want incomplete error", err)
	}
	// deletion does not require the missing definition
	noPanic(t, c.DeletePipeline(name, false, false))
	pipes, err = c.ListPipelines()
	if err != nil {
		t.Fatalf("list pipelines after delete: %v", err)
	}
	for _, p := range pipes {
		if p.Name == name {
			t.Fatalf("incomplete pipeline still listed after delete")
		}
	}
}

// TestSB146_SurvivesDeletedOutputRepo — force-deleting a running pipeline's
// output repository fails the pipeline with a reason, does not wedge the
// scheduler, and the pipeline recovers after the repository is recreated
// and the pipeline updated (SB-146, product decision D-10).
func TestSB146_SurvivesDeletedOutputRepo(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm1 := commitFiles(t, repo, "master", map[string]string{"file": "1"})
	name := uniq(t)
	slow := &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "sleep 5; cp -r ${" + repo + "}/* ${OUT}/"}}
	mustPipeline(t, client.Pipeline{Name: name, Transform: slow, Input: &client.Input{Repo: repo, Glob: "/*"}})
	flushOK(t, cm1.ID)

	cm2 := commitFiles(t, repo, "master", map[string]string{"file2": "2"})
	// the job's record is saved at spawn, before its output commit is
	// opened — deleting in that window would be silently absorbed
	// (startCommit recreates a missing repo). Wait for the commit to be
	// open (OutputCommit set) so the force-delete lands mid-execution and
	// the execute-side repo check (execute.go) fails the pipeline.
	pollFor(t, "job output commit open", 30*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
		if err != nil {
			return false
		}
		for _, j := range js {
			if j.State == "running" && j.OutputCommit != "" {
				return true
			}
		}
		return false
	})
	noPanic(t, c.DeleteRepo(name, true)) // force-delete the output repo mid-job

	// the record sets no bound on the failure observation; the in-flight
	// container must finish its sleep and exit first, and under heavy
	// suite load a container start can starve for minutes — the poll is
	// generous (a 60s bound has flaked twice on slow daemons)
	pollFor(t, "pipeline failed with reason", 180*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State == "failure" && info.Reason != ""
	})
	// the failed job is terminal; flushing the commit terminates cleanly
	jobs, err := c.Flush(cm2.ID, 30*time.Second)
	if err != nil {
		t.Fatalf("flush to damaged pipeline: %v", err)
	}
	for _, j := range jobs {
		if j.Pipeline == name && j.State != "failure" {
			t.Fatalf("damaged pipeline job state = %s, want failure", j.State)
		}
	}

	// a healthy pipeline on the same input still works (no crash loop)
	other := uniq(t)
	mustPipeline(t, client.Pipeline{Name: other, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	jobs, err = c.Flush(cm2.ID, 30*time.Second)
	if err != nil {
		t.Fatalf("flush through healthy pipeline: %v", err)
	}
	var otherJob *client.Job
	for i := range jobs {
		if jobs[i].Pipeline == other {
			otherJob = &jobs[i]
		}
	}
	if otherJob == nil || otherJob.State != "success" {
		t.Fatalf("healthy pipeline job missing or not success: %+v", jobs)
	}
	for path, want := range map[string]string{"file": "1", "file2": "2"} {
		got, err := c.GetFile(otherJob.OutputCommit, path)
		if err != nil {
			t.Fatalf("read %s from healthy output: %v", path, err)
		}
		if string(got) != want {
			t.Fatalf("output %s = %q, want %q", path, got, want)
		}
	}

	// recovery (D-10): recreate the repository, then update the pipeline
	noPanic(t, c.CreateRepo(name))
	mustUpdate(t, name, copyTransform(repo), &client.Input{Repo: repo, Glob: "/*"}, false)
	pollFor(t, "pipeline running again", 30*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State == "running"
	})
	cm3 := commitFiles(t, repo, "master", map[string]string{"file3": "3"})
	flushOK(t, cm3.ID)
}

// TestSB157_KeepRepoOnDelete — deleting a pipeline with KeepRepo preserves
// its output repository; recreating the pipeline reuses it; deleting
// without the flag removes the repository too (SB-157).
func TestSB157_KeepRepoOnDelete(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	in := &client.Input{Repo: repo, Glob: "/*"}
	mustPipeline(t, client.Pipeline{Name: name, Transform: copyTransform(repo), Input: in})
	cm1 := commitFiles(t, repo, "master", map[string]string{"file": "foo"})
	flushOK(t, cm1.ID)

	noPanic(t, c.DeletePipeline(name, false, true)) // keep the output repo
	if _, err := c.InspectRepo(name); err != nil {
		t.Fatalf("output repo gone after keep-delete: %v", err)
	}
	head, err := c.HeadCommit(name, "master")
	if err != nil {
		t.Fatalf("head of preserved repo: %v", err)
	}
	if got, err := c.GetFile(head.ID, "file"); err != nil || string(got) != "foo" {
		t.Fatalf("preserved output = %q (err %v), want foo", got, err)
	}

	// re-creation attaches to the existing repository
	mustPipeline(t, client.Pipeline{Name: name, Transform: copyTransform(repo), Input: in})
	cm2 := commitFiles(t, repo, "master", map[string]string{"file2": "bar"})
	flushOK(t, cm2.ID)
	head, err = c.HeadCommit(name, "master")
	if err != nil {
		t.Fatalf("head after recreate: %v", err)
	}
	for path, want := range map[string]string{"file": "foo", "file2": "bar"} {
		got, err := c.GetFile(head.ID, path)
		if err != nil || string(got) != want {
			t.Fatalf("%s = %q (err %v), want %q", path, got, err, want)
		}
	}

	// the final delete without keep removes the repository too
	noPanic(t, c.DeletePipeline(name, false, false))
	if _, err := c.InspectRepo(name); err == nil {
		t.Fatalf("output repo survived the no-keep delete")
	}
}
