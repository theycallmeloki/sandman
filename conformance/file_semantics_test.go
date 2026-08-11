package conformance

// FILE_SEMANTICS.md contract (Q5, 2026-08-10): within-commit append
// (FS-1), ancestry accumulation (FS-2), overwrite (FS-3), tombstone
// (FS-4), job-output replacement with same-path datum concatenation
// (FS-5/FS-6), split-upload numbering (FS-7), empty files (FS-8), and
// mid-commit visibility (FS-9). FS-10 (chunking never changes content) is
// N-A by design: sandman stores whole-file blobs.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

func TestFS1_AppendWithinCommit(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// two writes to the same path in one commit append in write order
	if err := c.PutFile(cm.ID, "x", []byte("foo")); err != nil {
		t.Fatalf("put 1: %v", err)
	}
	if err := c.PutFile(cm.ID, "x", []byte("foo")); err != nil {
		t.Fatalf("put 2: %v", err)
	}
	if b, err := c.GetFile(cm.ID, "x"); err != nil || string(b) != "foofoo" {
		t.Fatalf("x after two puts = %q (err %v), want foofoo", string(b), err)
	}
	// an overwrite replaces even within the same commit (FS-3)
	if err := c.PutFileOverwrite(cm.ID, "x", []byte("bar")); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if b, err := c.GetFile(cm.ID, "x"); err != nil || string(b) != "bar" {
		t.Fatalf("x after overwrite = %q (err %v), want bar", string(b), err)
	}
	// a path that is both a file and a directory prefix is a type
	// conflict; finishing fails (FS-1 edge)
	if err := c.PutFile(cm.ID, "y", []byte("1")); err != nil {
		t.Fatalf("put y: %v", err)
	}
	if err := c.PutFile(cm.ID, "y/z", []byte("2")); err != nil {
		t.Fatalf("put y/z: %v", err)
	}
	if _, err := c.FinishCommit(cm.ID, "", false); err == nil {
		t.Fatal("finishing a file/dir type conflict succeeded, want error")
	}
}

func TestFS2_AccumulateAcrossCommits(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	c1 := commitFiles(t, repo, "master", map[string]string{"x": "foo"})
	// a child commit's plain write to the same path appends to the
	// parent's content (FS-2)
	commitFiles(t, repo, "master", map[string]string{"x": "bar"})
	head, err := c.HeadCommit(repo, "master")
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if b, err := c.GetFile(head.ID, "x"); err != nil || string(b) != "foobar" {
		t.Fatalf("head x = %q (err %v), want foobar", string(b), err)
	}
	// reading an ancestor revision shows only that revision's own snapshot
	if b, err := c.GetFile(c1.ID, "x"); err != nil || string(b) != "foo" {
		t.Fatalf("c1 x = %q (err %v), want foo", string(b), err)
	}
	// a commit that writes nothing still inherits its parent's content
	cm3, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := c.FinishCommit(cm3.ID, "", false); err != nil {
		t.Fatalf("finish empty-write commit: %v", err)
	}
	if b, err := c.GetFile(cm3.ID, "x"); err != nil || string(b) != "foobar" {
		t.Fatalf("no-write commit x = %q (err %v), want inherited foobar", string(b), err)
	}
}

func TestFS3_OverwriteReplaces(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	c1 := commitFiles(t, repo, "master", map[string]string{"x": "foo"})
	// an explicit overwrite replaces the accumulated content (FS-3); a
	// plain put would append
	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := c.PutFileOverwrite(cm.ID, "x", []byte("bar")); err != nil {
		t.Fatalf("overwrite x: %v", err)
	}
	if err := c.PutFile(cm.ID, "y", []byte("fresh")); err != nil {
		t.Fatalf("put y: %v", err)
	}
	if _, err := c.FinishCommit(cm.ID, "", false); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if b, err := c.GetFile(cm.ID, "x"); err != nil || string(b) != "bar" {
		t.Fatalf("head x = %q (err %v), want the overwritten bar", string(b), err)
	}
	// the parent revision still shows the pre-overwrite content
	if b, err := c.GetFile(c1.ID, "x"); err != nil || string(b) != "foo" {
		t.Fatalf("c1 x = %q (err %v), want foo", string(b), err)
	}
}

func TestFS4_DeleteSemantics(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	commitFiles(t, repo, "master", map[string]string{"x": "foo", "y": "a"})
	// delete then write in one commit: the write wins with only its own
	// content (the tombstone removed the inherited content first)
	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := c.DeleteFile(cm.ID, "x"); err != nil {
		t.Fatalf("delete x: %v", err)
	}
	if err := c.PutFile(cm.ID, "x", []byte("new")); err != nil {
		t.Fatalf("put x: %v", err)
	}
	if _, err := c.FinishCommit(cm.ID, "", false); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if b, err := c.GetFile(cm.ID, "x"); err != nil || string(b) != "new" {
		t.Fatalf("x after delete-then-write = %q (err %v), want new", string(b), err)
	}
	// write then delete in one commit: the path is absent
	cm2, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := c.PutFile(cm2.ID, "y", []byte("b")); err != nil {
		t.Fatalf("put y: %v", err)
	}
	if err := c.DeleteFile(cm2.ID, "y"); err != nil {
		t.Fatalf("delete y: %v", err)
	}
	if _, err := c.FinishCommit(cm2.ID, "", false); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if _, err := c.GetFile(cm2.ID, "y"); err == nil {
		t.Fatal("y readable after write-then-delete, want absent")
	}
	// deleting a nonexistent path is a no-op
	cm3, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := c.DeleteFile(cm3.ID, "nope"); err != nil {
		t.Fatalf("delete nonexistent: %v", err)
	}
	if _, err := c.FinishCommit(cm3.ID, "", false); err != nil {
		t.Fatalf("finish: %v", err)
	}
	// deleting from an already-finished commit is an error
	if err := c.DeleteFile(cm.ID, "x"); err == nil {
		t.Fatal("delete from a finished commit succeeded, want error")
	}
}

func TestFS5_JobSamePathDatumConcat(t *testing.T) {
	t.Run("concatenates in datum order", func(t *testing.T) {
		repo := uniq(t)
		mustRepo(t, repo)
		commitFiles(t, repo, "master", map[string]string{"a": "A\n", "b": "B\n"})
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "for f in ${in}/*; do cat \"$f\" >> ${OUT}/merged; done"}},
			Input:     &client.Input{Name: "in", Repo: repo, Glob: "/*"},
		})
		head, err := c.HeadCommit(repo, "master")
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		jobs := flushOK(t, head.ID)
		if len(jobs) != 1 {
			t.Fatalf("jobs = %d, want 1", len(jobs))
		}
		b, err := c.GetFile(jobs[0].OutputCommit, "merged")
		if err != nil {
			t.Fatalf("read merged: %v", err)
		}
		if len(b) != 4 || !strings.Contains(string(b), "A\n") || !strings.Contains(string(b), "B\n") {
			t.Fatalf("merged = %q, want both datums' content (A\\n + B\\n in completion order)", string(b))
		}
	})
	t.Run("file/dir type conflict fails the job", func(t *testing.T) {
		repo := uniq(t)
		mustRepo(t, repo)
		commitFiles(t, repo, "master", map[string]string{"a": "x", "b": "y"})
		pipe := uniq(t)
		// datum a writes a file at x; datum b writes x/y as a directory:
		// the job's assembled output has a file and a dir at the same path
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "case \"$(ls ${in})\" in a) echo a > ${OUT}/x;; b) mkdir -p ${OUT}/x && echo b > ${OUT}/x/y;; esac"}},
			Input:     &client.Input{Name: "in", Repo: repo, Glob: "/*"},
		})
		head, err := c.HeadCommit(repo, "master")
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		jobs, err := c.Flush(head.ID, 60*time.Second)
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
		if len(jobs) != 1 {
			t.Fatalf("jobs = %d, want 1", len(jobs))
		}
		if jobs[0].State != "failure" {
			t.Fatalf("job state = %s, want failure (file/dir type conflict)", jobs[0].State)
		}
	})
}

func TestFS6_ReprocessReplacesPriorOutput(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	in := &client.Input{Repo: repo, Glob: "/*"}
	mustPipeline(t, client.Pipeline{Name: pipe, Transform: copyTransform(repo), Input: in})
	cm1 := commitFiles(t, repo, "master", map[string]string{"file": "foo"})
	flushOK(t, cm1.ID)
	// a reprocess update re-runs the same datum: its prior output path is
	// replaced with the fresh output, not accumulated (FS-6 — without the
	// replacement the path would double)
	mustUpdate(t, pipe, copyTransform(repo), in, true)
	jobs := flushOK(t, cm1.ID)
	if len(jobs) != 1 {
		t.Fatalf("reprocess jobs = %d, want 1", len(jobs))
	}
	if b, err := c.GetFile(jobs[0].OutputCommit, "file"); err != nil || string(b) != "foo" {
		t.Fatalf("reprocessed output = %q (err %v), want foo (not foofoo)", string(b), err)
	}
}

func TestFS7_SplitNumberingAcrossCommits(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	upload := func(header string, rows ...string) client.Commit {
		t.Helper()
		cm, err := c.StartCommit(repo, "master", "")
		if err != nil {
			t.Fatalf("start: %v", err)
		}
		data := header + "\n" + strings.Join(rows, "\n")
		if err := c.PutFileSplit(cm.ID, "d", []byte(data), "\n", true); err != nil {
			t.Fatalf("split upload: %v", err)
		}
		fin, err := c.FinishCommit(cm.ID, "", false)
		if err != nil {
			t.Fatalf("finish: %v", err)
		}
		return fin
	}
	upload("HDR", "r1", "r2")
	cm2 := upload("HDR", "r3")
	// numbering continues across upload calls and across child commits
	if b, err := c.GetFile(cm2.ID, "d/2"); err != nil || string(b) != "HDR\nr3" {
		t.Fatalf("d/2 = %q (err %v), want the third record at index 2", string(b), err)
	}
	// a changed header swaps the header for every record without changing
	// the record count or numbering (FS-7)
	cm3 := upload("HDR2", "r1", "r2", "r3")
	for i, want := range []string{"HDR2\nr1", "HDR2\nr2", "HDR2\nr3"} {
		if b, err := c.GetFile(cm3.ID, fmt.Sprintf("d/%d", i)); err != nil || string(b) != want {
			t.Fatalf("d/%d after header swap = %q (err %v), want %q", i, string(b), err, want)
		}
	}
	files, err := c.ListFiles(cm3.ID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var recs []string
	for _, f := range files {
		if strings.HasPrefix(f.Path, "d/") {
			recs = append(recs, f.Path)
		}
	}
	if len(recs) != 3 {
		t.Fatalf("record count after header swap = %d, want 3 (unchanged)", len(recs))
	}
}

func TestFS8_EmptyFilesAreReal(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := c.PutFile(cm.ID, "e", []byte{}); err != nil {
		t.Fatalf("put empty: %v", err)
	}
	if _, err := c.FinishCommit(cm.ID, "", false); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if b, err := c.GetFile(cm.ID, "e"); err != nil || len(b) != 0 {
		t.Fatalf("empty file = %q (err %v), want 0 bytes present", string(b), err)
	}
	files, err := c.ListFiles(cm.ID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	found := false
	for _, f := range files {
		if f.Path == "e" && f.Size == 0 {
			found = true
		}
	}
	if !found {
		t.Fatalf("listing %v does not contain the 0-byte file e", files)
	}
}

func TestFS9_MidCommitVisibility(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// a user-initiated open commit is readable immediately after the write
	if err := c.PutFile(cm.ID, "x", []byte("foo")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if b, err := c.GetFile(cm.ID, "x"); err != nil || string(b) != "foo" {
		t.Fatalf("open commit read = %q (err %v), want foo", string(b), err)
	}
	fin, err := c.FinishCommit(cm.ID, "", false)
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	// writing to an already-finished commit is an error
	if err := c.PutFile(fin.ID, "y", []byte("bar")); err == nil {
		t.Fatal("put into a finished commit succeeded, want error")
	}

	// pipeline output commits are NOT readable until finished (FS-9):
	// the output commit is opened at job start but carries nothing until
	// the job finishes assembling it
	repo2 := uniq(t)
	mustRepo(t, repo2)
	commitFiles(t, repo2, "master", map[string]string{"file": "x"})
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      pipe,
		Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", fmt.Sprintf("cp -r ${%s}/* ${OUT}/ && sleep 5", repo2)}},
		Input:     &client.Input{Repo: repo2, Glob: "/*"},
	})
	head, err := c.HeadCommit(repo2, "master")
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	var outCommit string
	pollFor(t, "job running with an open output commit", 30*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		if err != nil || len(js) == 0 {
			return false
		}
		if js[0].State == "running" && js[0].OutputCommit != "" {
			outCommit = js[0].OutputCommit
			return true
		}
		return false
	})
	if _, err := c.GetFile(outCommit, "file"); err == nil {
		t.Fatal("unfinished output commit is readable, want not-found until finished")
	}
	flushOK(t, head.ID)
	if b, err := c.GetFile(outCommit, "file"); err != nil || string(b) != "x" {
		t.Fatalf("finished output = %q (err %v), want x", string(b), err)
	}
}
