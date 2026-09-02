// patch verb delta computation: a real git checkout's worktree edits
// become the sandman delta (files with full content, deleted paths, base
// and revision revisions). Pure — no control plane involved — so it runs
// wherever git does.
package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// mkGitRepo builds a throwaway repo with one commit and an ssh origin
// remote (the normalization path), returning its directory.
func mkGitRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	runGit := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	runGit("init", "-q", "-b", "master")
	runGit("config", "user.email", "test@sandman")
	runGit("config", "user.name", "sandman test")
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("one\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("kept\n"), 0o644)
	runGit("add", ".")
	runGit("commit", "-q", "-m", "base")
	runGit("remote", "add", "origin", "git@github.com:theycallmeloki/sandman.git")
	return dir
}

func gitHead(t *testing.T, dir string) string {
	t.Helper()
	out, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func TestPatchDeltaUncommittedEdits(t *testing.T) {
	dir := mkGitRepo(t)
	head := gitHead(t, dir)

	// uncommitted edits: modify, delete, add-untracked
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("two\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "keep.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "new.txt"), []byte("fresh\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	files, deleted, revision, base, _, err := buildPatchDelta(dir, "", "")
	if err != nil {
		t.Fatalf("buildPatchDelta: %v", err)
	}
	if base != head || revision != head {
		t.Fatalf("base %q revision %q, want both HEAD %q (uncommitted-edits mode)", base, revision, head)
	}
	if got := files["a.txt"]; got != "two\n" {
		t.Fatalf("modified file content = %q, want two\\n", got)
	}
	if got := files["new.txt"]; got != "fresh\n" {
		t.Fatalf("untracked file content = %q, want fresh\\n", got)
	}
	if len(deleted) != 1 || deleted[0] != "keep.txt" {
		t.Fatalf("deleted = %v, want [keep.txt]", deleted)
	}
	if _, ok := files["keep.txt"]; ok {
		t.Fatalf("deleted file present in files map")
	}
}

func TestPatchDeltaCommittedEdits(t *testing.T) {
	dir := mkGitRepo(t)
	base := gitHead(t, dir)
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("committed\n"), 0o644)
	cmd := exec.Command("git", "-C", dir, "commit", "-aqm", "edit")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("commit: %v\n%s", err, out)
	}
	head := gitHead(t, dir)

	// committed edits: --base must name the fork revision
	files, deleted, revision, gotBase, _, err := buildPatchDelta(dir, "", base)
	if err != nil {
		t.Fatalf("buildPatchDelta: %v", err)
	}
	if revision != head {
		t.Fatalf("revision = %q, want the new HEAD %q", revision, head)
	}
	if gotBase != base {
		t.Fatalf("base = %q, want the fork %q", gotBase, base)
	}
	if got := files["a.txt"]; got != "committed\n" {
		t.Fatalf("committed edit content = %q", got)
	}
	if len(deleted) != 0 {
		t.Fatalf("deleted = %v, want none", deleted)
	}
}

func TestPatchDeltaNoChanges(t *testing.T) {
	dir := mkGitRepo(t)
	files, deleted, _, _, _, err := buildPatchDelta(dir, "", "")
	if err != nil {
		t.Fatalf("buildPatchDelta: %v", err)
	}
	if len(files) != 0 || len(deleted) != 0 {
		t.Fatalf("clean checkout produced a delta: %d files %d deleted", len(files), len(deleted))
	}
}

func TestHttpsRepoURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"https://github.com/theycallmeloki/sandman.git", "https://github.com/theycallmeloki/sandman.git"},
		{"git@github.com:theycallmeloki/sandman.git", "https://github.com/theycallmeloki/sandman.git"},
		{"ssh://git@gitlab.winehq.org/wine/wine.git", "https://gitlab.winehq.org/wine/wine.git"},
		{"git://host.example/repo.git", "https://host.example/repo.git"},
	}
	for _, tc := range cases {
		got, err := httpsRepoURL(tc.in)
		if err != nil {
			t.Fatalf("httpsRepoURL(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("httpsRepoURL(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if _, err := httpsRepoURL("http://github.com/x/y.git"); err == nil {
		t.Fatal("http remote accepted, want refusal (sandman takes https only)")
	}
	if _, err := httpsRepoURL("file:///srv/git/x.git"); err == nil {
		t.Fatal("file remote accepted, want refusal")
	}
}
