package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The case-sensitivity guards are filesystem-dependent by design: on Linux
// (case-sensitive) they must be inert — both spellings stay distinct names —
// and on macOS's default APFS (case-insensitive) they must refuse the pair.
// Every test below asserts the behaviour of the filesystem it runs on,
// decided by an independent probe rather than by GOOS, so the same file is
// meaningful on both runners.

// foldsCase reports whether dir folds case, using an independent check (not
// the code under test): a fresh file is created and its uppercased name
// looked up.
func foldsCase(t *testing.T, dir string) bool {
	t.Helper()
	f, err := os.CreateTemp(dir, "independent-*")
	if err != nil {
		t.Fatalf("create probe: %v", err)
	}
	name := f.Name()
	f.Close()
	_, err = os.Stat(filepath.Join(dir, strings.ToUpper(filepath.Base(name))))
	return err == nil
}

func TestCaseInsensitiveAgreesWithFilesystem(t *testing.T) {
	dir := t.TempDir()
	if got, want := CaseInsensitive(dir), foldsCase(t, dir); got != want {
		t.Fatalf("CaseInsensitive(%s) = %v, want %v", dir, got, want)
	}
	if got := CaseInsensitive(dir); got != foldsCase(t, dir) {
		t.Fatalf("cached answer %v disagrees with the filesystem", got)
	}
}

// An unprobeable directory reports case-sensitive (the guards must never
// reject a name they cannot prove ambiguous) but the answer must not be
// cached: the state directory is created after the store is, and the
// question has to be asked again once it exists.
func TestCaseInsensitiveUnprobeableDirIsNotCached(t *testing.T) {
	parent := t.TempDir()
	missing := filepath.Join(parent, "state")
	resetCaseCache()
	if CaseInsensitive(missing) {
		t.Fatal("a directory that does not exist reported case-insensitive")
	}
	if err := os.MkdirAll(missing, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if got, want := CaseInsensitive(missing), foldsCase(t, missing); got != want {
		t.Fatalf("after creation CaseInsensitive = %v, want %v (the unprobeable answer was cached)", got, want)
	}
}

func TestCaseNameCollisionNamesTheExistingEntry(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Foo.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err := CaseNameCollision("pipeline", dir, "foo", ".json")
	if !foldsCase(t, dir) {
		if err != nil {
			t.Fatalf("case-sensitive filesystem reported a collision: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatal("case-insensitive filesystem accepted a differently-cased name")
	}
	// the message has to name the entry the name resolves to, without the
	// on-disk suffix, and say why
	if !strings.Contains(err.Error(), `existing pipeline "Foo"`) {
		t.Fatalf("error does not name the existing pipeline: %v", err)
	}
	// an exact-name match is the caller's own "already exists", not a
	// collision: it must stay silent here
	if err := CaseNameCollision("pipeline", dir, "Foo", ".json"); err != nil {
		t.Fatalf("exact-name match reported as a collision: %v", err)
	}
}

func TestMaterializeViewRefusesCaseTwinPaths(t *testing.T) {
	state := t.TempDir()
	s := New(state)
	sha, err := s.WriteBlob([]byte("payload"))
	if err != nil {
		t.Fatalf("write blob: %v", err)
	}
	view := map[string]ViewEntry{
		"Data.txt": {parts: []ViewPart{{SHA: sha, Size: 7, Overwrite: true}}},
		"data.txt": {parts: []ViewPart{{SHA: sha, Size: 7, Overwrite: true}}},
	}
	dst := filepath.Join(state, "view")
	err = s.MaterializeView(view, dst)
	if !foldsCase(t, state) {
		if err != nil {
			t.Fatalf("case-sensitive filesystem refused a legitimate revision: %v", err)
		}
		for _, p := range []string{"Data.txt", "data.txt"} {
			if _, serr := os.Stat(filepath.Join(dst, p)); serr != nil {
				t.Fatalf("case-sensitive filesystem did not materialize %s: %v", p, serr)
			}
		}
		return
	}
	if err == nil {
		t.Fatal("materialized a revision whose paths differ only in case")
	}
	for _, want := range []string{`"Data.txt"`, `"data.txt"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error does not name %s: %v", want, err)
		}
	}
	// the refusal must be a refusal: nothing may have been written, or the
	// caller would still be reading one file where the revision holds two
	if _, serr := os.Stat(filepath.Join(dst, "Data.txt")); serr == nil {
		t.Fatal("paths were materialized despite the refusal")
	}
}

// The batch uploader is the path a job's output takes: a transform that
// emits a path differing only in case from one already in the revision
// (inherited from the previous output commit) would otherwise land on the
// same host file as the inherited one and be read back as a single file.
func TestAddFilesFromDirRefusesCaseTwinOfInheritedPath(t *testing.T) {
	state := t.TempDir()
	s := New(state)
	if err := s.CreateRepo("out"); err != nil {
		t.Fatalf("create repo: %v", err)
	}
	first, err := s.StartCommit("out", "master", "")
	if err != nil {
		t.Fatalf("start first commit: %v", err)
	}
	if err := s.AddFilesFromDir(first.ID, writeTree(t, map[string]string{"Data.txt": "inherited"})); err != nil {
		t.Fatalf("first upload: %v", err)
	}
	if _, err := s.FinishCommit(first.ID, "", false); err != nil {
		t.Fatalf("finish first commit: %v", err)
	}

	second, err := s.StartCommit("out", "master", "")
	if err != nil {
		t.Fatalf("start second commit: %v", err)
	}
	err = s.AddFilesFromDir(second.ID, writeTree(t, map[string]string{"data.txt": "fresh"}))
	if !foldsCase(t, state) {
		if err != nil {
			t.Fatalf("case-sensitive filesystem refused a distinct path: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatal("uploaded a path that differs only in case from the inherited one")
	}
	if !strings.Contains(err.Error(), `"Data.txt"`) {
		t.Fatalf("error does not name the inherited path: %v", err)
	}
}

// writeTree lays out a temp directory holding the given files.
func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir for %s: %v", name, err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	return dir
}

// A branch is one ref file, so on a case-insensitive filesystem a
// differently-cased branch name is the same ref: retargeting it would
// silently move the other branch's head and answer success. SetHead is the
// single funnel for branch writes (commit finish, branch create, job output
// advance), so the guard belongs there.
func TestSetHeadRefusesCaseTwinBranch(t *testing.T) {
	state := t.TempDir()
	s := New(state)
	if err := s.CreateRepo("r"); err != nil {
		t.Fatalf("create repo: %v", err)
	}
	cm, err := s.StartCommit("r", "feature", "")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	if _, err := s.FinishCommit(cm.ID, "", false); err != nil {
		t.Fatalf("finish commit: %v", err)
	}
	err = s.SetHead("r", "Feature", cm.ID)
	if !foldsCase(t, state) {
		if err != nil {
			t.Fatalf("case-sensitive filesystem refused a distinct branch: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatal("retargeted a branch name that differs only in case from an existing ref")
	}
	if !strings.Contains(err.Error(), `"feature"`) {
		t.Fatalf("error does not name the existing branch: %v", err)
	}
	// the exact name stays a normal retarget
	if err := s.SetHead("r", "feature", cm.ID); err != nil {
		t.Fatalf("retarget of the same branch: %v", err)
	}
}

// The filesystem is a property of the mount, not of a directory: a
// materialization destination that does not exist yet must be judged by the
// mount it lands on (egress creates its target directory).
func TestCaseInsensitiveAtProbesTheNearestExistingAncestor(t *testing.T) {
	parent := t.TempDir()
	missing := filepath.Join(parent, "a", "b", "c")
	if got, want := caseInsensitiveAt(missing), foldsCase(t, parent); got != want {
		t.Fatalf("caseInsensitiveAt(absent destination) = %v, want the ancestor's answer %v", got, want)
	}
}

func TestPutFileRefusesCaseTwinPath(t *testing.T) {
	state := t.TempDir()
	s := New(state)
	if err := s.CreateRepo("demo"); err != nil {
		t.Fatalf("create repo: %v", err)
	}
	cm, err := s.StartCommit("demo", "master", "")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	if err := s.PutFile(cm.ID, "Data.txt", []byte("one")); err != nil {
		t.Fatalf("first put: %v", err)
	}
	err = s.PutFile(cm.ID, "data.txt", []byte("two"))
	if !foldsCase(t, state) {
		if err != nil {
			t.Fatalf("case-sensitive filesystem refused a distinct path: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatal("accepted a path that differs only in case from one already in the revision")
	}
	if !strings.Contains(err.Error(), `"Data.txt"`) {
		t.Fatalf("error does not name the colliding path: %v", err)
	}
	// the same spelling stays a normal overwrite
	if err := s.PutFile(cm.ID, "Data.txt", []byte("three")); err != nil {
		t.Fatalf("re-put of the same path: %v", err)
	}
}
