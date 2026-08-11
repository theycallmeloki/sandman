package conformance

import (
	"bytes"
	"fmt"
	"testing"

	"sandman/client"
)

// SB-001 — a single input pipeline copies input files into its output
// repository: one output commit per input commit; file name and content
// preserved end to end.
func TestSB001_SingleInputPipelineCopiesInputFiles(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)

	p := client.Pipeline{
		Name:      uniq(t),
		Transform: copyTransform(repo),
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	}
	mustPipeline(t, p)

	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})

	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want exactly 1", len(jobs))
	}
	out := jobs[0].OutputCommit
	if out == "" {
		t.Fatal("job has no output commit")
	}
	got, err := c.GetFile(out, "file")
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if string(got) != "foo" {
		t.Fatalf("output content = %q, want %q", got, "foo")
	}
}

// SB-002 — a repository's reported size equals the total size of the files
// in its main branch head revision.
func TestSB002_RepoSizeEqualsHeadFileBytes(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	commitFiles(t, repo, "master", map[string]string{"a": "aaa", "b": "bbbbb"})

	r, err := c.InspectRepo(repo)
	if err != nil {
		t.Fatalf("inspect repo: %v", err)
	}
	if r.SizeBytes != 8 {
		t.Fatalf("repo size = %d, want 8", r.SizeBytes)
	}
}

// SB-003 — a pipeline with a single named input processes and preserves the
// input file; the input's name (not the repo name) addresses its data.
func TestSB003_NamedInputPreservesFile(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)

	p := client.Pipeline{
		Name:      uniq(t),
		Transform: copyTransform("in"),
		Input:     &client.Input{Name: "in", Repo: repo, Glob: "/*"},
	}
	mustPipeline(t, p)

	cm := commitFiles(t, repo, "master", map[string]string{"file": "content"})

	jobs := flushOK(t, cm.ID)
	out := jobs[0].OutputCommit
	got, err := c.GetFile(out, "file")
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if string(got) != "content" {
		t.Fatalf("output content = %q, want %q", got, "content")
	}
}

// SB-005 — large input files pass through the pipeline byte-for-byte with
// correct size metadata.
func TestSB005_LargeFilesPassThroughByteForByte(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)

	// 50 MiB of a deterministic repeating pattern.
	const size = 50 << 20
	pattern := bytes.Repeat([]byte("0123456789abcdef"), 1<<16)
	data := make([]byte, size)
	for i := range data {
		data[i] = pattern[i%len(pattern)]
	}

	p := client.Pipeline{
		Name:      uniq(t),
		Transform: copyTransform(repo),
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	}
	mustPipeline(t, p)

	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	if err := c.PutFile(cm.ID, "big", data); err != nil {
		t.Fatalf("put large file: %v", err)
	}
	if _, err := c.FinishCommit(cm.ID, "", false); err != nil {
		t.Fatalf("finish commit: %v", err)
	}

	jobs := flushOK(t, cm.ID)
	out := jobs[0].OutputCommit
	got, err := c.GetFile(out, "big")
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if len(got) != size {
		t.Fatalf("output size = %d, want %d", len(got), size)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("output content differs from input")
	}
}

// SB-016 — empty files are real, zero-length inputs that reach execution
// intact.
func TestSB016_EmptyFilesReachExecution(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)

	p := client.Pipeline{
		Name:      uniq(t),
		Transform: copyTransform(repo),
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	}
	mustPipeline(t, p)

	cm := commitFiles(t, repo, "master", map[string]string{"empty": ""})

	jobs := flushOK(t, cm.ID)
	out := jobs[0].OutputCommit
	got, err := c.GetFile(out, "empty")
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("output file size = %d, want 0", len(got))
	}
}

// SB-046 — a single commit can hold thousands of files and list them all.
func TestSB046_ThousandsOfFilesInOneCommit(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)

	// the reference's scale is 5000 files in one commit; the test
	// exercises the same shape at that scale (SB-046)
	const n = 5000
	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("f-%04d", i)
		if err := c.PutFile(cm.ID, p, []byte(fmt.Sprintf("%d", i))); err != nil {
			t.Fatalf("put file %s: %v", p, err)
		}
	}
	if _, err := c.FinishCommit(cm.ID, "", false); err != nil {
		t.Fatalf("finish commit: %v", err)
	}

	files, err := c.ListFiles(cm.ID)
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(files) != n {
		t.Fatalf("listed %d files, want %d", len(files), n)
	}
	// every file must be readable with its own content
	got, err := c.GetFile(cm.ID, "f-1999")
	if err != nil {
		t.Fatalf("read f-1999: %v", err)
	}
	if string(got) != "1999" {
		t.Fatalf("f-1999 content = %q, want %q", got, "1999")
	}
}

// SB-117 — commits carry an optional description settable at start and
// finish; a finish-time description overrides the start-time one.
func TestSB117_CommitDescriptionStartFinishOverride(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)

	// case 1: start D1, finish without → D1
	c1, err := c.StartCommit(repo, "master", "D1")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	if _, err := c.FinishCommit(c1.ID, "", false); err != nil {
		t.Fatalf("finish: %v", err)
	}
	// case 2: start without, finish D2 → D2
	c2, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	if _, err := c.FinishCommit(c2.ID, "D2", false); err != nil {
		t.Fatalf("finish: %v", err)
	}
	// case 3: start D3, finish D4 → D4
	c3, err := c.StartCommit(repo, "master", "D3")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	if _, err := c.FinishCommit(c3.ID, "D4", false); err != nil {
		t.Fatalf("finish: %v", err)
	}

	want := []struct {
		id   string
		desc string
	}{{c1.ID, "D1"}, {c2.ID, "D2"}, {c3.ID, "D4"}}
	for _, w := range want {
		got, err := c.InspectCommit(w.id)
		if err != nil {
			t.Fatalf("inspect commit %s: %v", w.id, err)
		}
		if got.Description != w.desc {
			t.Fatalf("commit %s description = %q, want %q", w.id, got.Description, w.desc)
		}
	}
}

// SB-118 — a commit finished as explicitly empty carries no file content:
// files from its parent revision are not readable through it, even at the
// branch head.
func TestSB118_EmptyCommitBlocksParentReads(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	commitFiles(t, repo, "master", map[string]string{"file": "data contents"})

	// start and finish a second commit with the empty flag, no file ops
	c2, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	if _, err := c.FinishCommit(c2.ID, "", true); err != nil {
		t.Fatalf("finish empty commit: %v", err)
	}

	head, err := c.HeadCommit(repo, "master")
	if err != nil {
		t.Fatalf("head commit: %v", err)
	}
	if head.ID != c2.ID {
		t.Fatalf("head = %s, want the empty commit %s", head.ID, c2.ID)
	}
	if !head.Empty {
		t.Fatalf("head commit not marked empty")
	}
	_, err = c.GetFile(head.ID, "file")
	wantErr(t, err, "not found")
}
