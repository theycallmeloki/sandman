// Commit deletion: deleting an input commit cascades through the whole
// downstream DAG with parent-link and branch-head repair, and deleting a
// branch head supersedes the in-flight job processing it (SB-124, SB-125).
package conformance

import (
	"testing"
	"time"

	"sandman/client"
)

func TestSB124_DeleteCommitCascadesThroughDAG(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	p0, p1 := uniq(t), uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      p0,
		Transform: &client.Transform{Image: "alpine"},
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	})
	mustPipeline(t, client.Pipeline{
		Name:      p1,
		Transform: &client.Transform{Image: "alpine"},
		Input:     &client.Input{Repo: p0, Glob: "/*"},
	})

	c1 := commitFiles(t, repo, "master", map[string]string{"file": "one"})
	flushOK(t, c1.ID) // one job per pipeline stage
	c2 := commitFiles(t, repo, "master", map[string]string{"file": "two"})
	flushOK(t, c2.ID)

	chainLen := func(repoName string) int {
		ch, err := c.CommitHistory(repoName, "master")
		if err != nil {
			t.Fatalf("history of %s: %v", repoName, err)
		}
		return len(ch)
	}
	parentless := func(repoName string) bool {
		ch, err := c.CommitHistory(repoName, "master")
		if err != nil || len(ch) != 1 {
			t.Fatalf("history of %s: %v (%d commits)", repoName, err, len(ch))
		}
		return ch[0].ParentID == ""
	}

	// delete the first (non-head) input commit by id
	if err := c.DeleteCommit(c1.ID); err != nil {
		t.Fatalf("delete parent commit: %v", err)
	}
	for _, r := range []string{repo, p0, p1} {
		if n := chainLen(r); n != 1 {
			t.Fatalf("%s has %d commits after deleting the parent, want 1", r, n)
		}
		if !parentless(r) {
			t.Fatalf("%s's surviving commit still has a parent link", r)
		}
	}
	p0jobs, err := c.ListJobsFiltered(client.JobFilter{Pipeline: p0})
	if err != nil {
		t.Fatalf("list p0 jobs: %v", err)
	}
	if len(p0jobs) != 1 {
		t.Fatalf("p0 has %d jobs after deleting the parent, want 1", len(p0jobs))
	}

	// delete the branch head by branch reference
	if err := c.DeleteCommit(repo + "@master"); err != nil {
		t.Fatalf("delete head commit: %v", err)
	}
	for _, r := range []string{repo, p0, p1} {
		if _, err := c.HeadCommit(r, "master"); err == nil {
			t.Fatalf("%s still has a master head after deleting it", r)
		}
	}

	// the DAG stays fully functional
	c3 := commitFiles(t, repo, "master", map[string]string{"file": "three"})
	jobs := flushOK(t, c3.ID)
	if len(jobs) != 2 {
		t.Fatalf("flush after deletion returned %d jobs, want 2 (both stages)", len(jobs))
	}
	var last client.Job
	for _, j := range jobs {
		if j.Pipeline == p1 {
			last = j
		}
	}
	b, err := c.GetFile(last.OutputCommit, "file")
	if err != nil {
		t.Fatalf("read p1 output: %v", err)
	}
	if string(b) != "three" {
		t.Fatalf("p1 output = %q, want three", string(b))
	}
}

func TestSB125_DeleteHeadSupersedesInflightJob(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", "sleep $(cat ${" + repo + "}/sleep); cp ${" + repo + "}/file ${OUT}/file"},
		},
		Input: &client.Input{Repo: repo, Glob: "/"}, // the whole commit is one datum
	})

	c1 := commitFiles(t, repo, "master", map[string]string{"file": "one", "sleep": "1"})
	flushOK(t, c1.ID)
	c2 := commitFiles(t, repo, "master", map[string]string{"file": "two", "sleep": "600"})

	// wait for c2's job to be actively running (started output commit)
	pollFor(t, "c2 job active", 60*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		if err != nil {
			return false
		}
		for _, j := range js {
			if j.State == "running" && j.OutputCommit != "" {
				for _, ic := range j.InputCommits {
					if ic == c2.ID {
						return true
					}
				}
			}
		}
		return false
	})

	// deleting the branch head supersedes the in-flight job
	if err := c.DeleteCommit(c2.ID); err != nil {
		t.Fatalf("delete head commit: %v", err)
	}
	pollFor(t, "exactly one job, over commit1", 60*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		if err != nil || len(js) != 1 {
			return false
		}
		for _, ic := range js[0].InputCommits {
			if ic == c1.ID {
				return true
			}
		}
		return false
	})
	js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
	if len(js) != 1 || js[0].State != "success" {
		t.Fatalf("jobs after deletion = %+v, want one successful job over commit1", js)
	}

	// the branch serves the previous commit's data
	head, err := c.HeadCommit(repo, "master")
	if err != nil {
		t.Fatalf("head after deletion: %v", err)
	}
	if head.ID != c1.ID {
		t.Fatalf("branch head = %s, want commit1", head.ID)
	}
	b, err := c.GetFile(head.ID, "file")
	if err != nil {
		t.Fatalf("read branch file: %v", err)
	}
	if string(b) != "one" {
		t.Fatalf("branch file = %q, want commit1's data", string(b))
	}
	// flushing commit1 yields exactly one output commit with its data
	flushed := flushOK(t, c1.ID)
	if len(flushed) != 1 {
		t.Fatalf("flush(commit1) = %d jobs, want 1", len(flushed))
	}
	if ob, err := c.GetFile(flushed[0].OutputCommit, "file"); err != nil || string(ob) != "one" {
		t.Fatalf("flushed output = %q (%v), want commit1's data", string(ob), err)
	}

	// the pipeline remains fully operational
	c3 := commitFiles(t, repo, "master", map[string]string{"file": "three", "sleep": "1"})
	j3 := flushOK(t, c3.ID)
	if len(j3) != 1 {
		t.Fatalf("post-deletion flush = %d jobs, want 1", len(j3))
	}
	if ob, err := c.GetFile(j3[0].OutputCommit, "file"); err != nil || string(ob) != "three" {
		t.Fatalf("post-deletion output = %q (%v), want three", string(ob), err)
	}
}
