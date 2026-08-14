// Symlinked output: a pipeline may emit its output as symbolic links to
// its input files and directories (and to external files it creates); the
// output revision contains the linked content, and a linked file's stored
// content is identical to the input's — no duplicated copy..
package conformance

import (
	"fmt"
	"testing"

	"sandman/client"
)

func TestSymlinkOutputs(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	// the pipeline is created before the input commit exists
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd: []string{"sh", "-c", fmt.Sprintf(
				"echo buzz > /tmp/sandman-sb054-$$; "+
					"ln -s ${%s}/foo ${OUT}/foo; "+
					"ln -s ${%s}/dir1/bar ${OUT}/bar; "+
					"ln -s ${%s} ${OUT}/dir; "+
					"ln -s /tmp/sandman-sb054-$$ ${OUT}/buzz", repo, repo, repo)},
		},
		Input:     &client.Input{Repo: repo, Glob: "/*"},
		ChunkSpec: &client.ChunkSpec{Number: 1}, // one datum carrying the whole input
	})

	cm := commitFiles(t, repo, "master", map[string]string{"foo": "foo", "dir1/bar": "bar", "dir2/foo": "foo"})
	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want exactly 1 output commit", len(jobs))
	}
	out := jobs[0].OutputCommit

	read := func(p string) string {
		t.Helper()
		b, err := c.GetFile(out, p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		return string(b)
	}
	// individual files, a symlinked directory's contents, and an external
	// file the transform created
	if got := read("foo"); got != "foo" {
		t.Fatalf("foo = %q, want foo", got)
	}
	if got := read("bar"); got != "bar" {
		t.Fatalf("bar = %q, want bar", got)
	}
	if got := read("dir/dir2/foo"); got != "foo" {
		t.Fatalf("dir/dir2/foo = %q, want foo (through the directory symlink)", got)
	}
	if got := read("buzz"); got != "buzz\n" {
		t.Fatalf("buzz = %q, want %q", got, "buzz\n")
	}

	// a linked output file stores the same content units as its input
	// (content identity, not a re-uploaded copy)
	hashOf := func(commit, p string) string {
		t.Helper()
		infos, err := c.ListFiles(commit)
		if err != nil {
			t.Fatalf("list %s: %v", commit, err)
		}
		for _, f := range infos {
			if f.Path == p {
				return f.Hash
			}
		}
		t.Fatalf("file %s not in %s", p, commit)
		return ""
	}
	for _, pair := range [][2]string{
		{"foo", "foo"},
		{"bar", "dir1/bar"},
		{"dir/dir2/foo", "dir2/foo"},
	} {
		outHash := hashOf(out, pair[0])
		inHash := hashOf(cm.ID, pair[1])
		if outHash == "" || outHash != inHash {
			t.Fatalf("output %s hash %q != input %s hash %q (content must be referenced, not copied)", pair[0], outHash, pair[1], inHash)
		}
	}
}
