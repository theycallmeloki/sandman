package store

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Case-insensitive state directories.
//
// Every name and path this package stores — repo names, commit paths, tags,
// and (through CaseNameCollision) the daemon's pipelines and secrets — is a
// case-sensitive string in Go, but each one lands on whatever filesystem the
// state directory happens to sit on. macOS's default APFS volume is
// case-INsensitive: there "Data.txt" and "data.txt" are one file, so the
// second write silently replaces the first and a revision that holds both
// spells only one of them. Linux is case-sensitive, and that behaviour is
// load-bearing for existing installs, so nothing below applies unless the
// filesystem is actually case-insensitive — the decision is made by probing
// the state directory, never by GOOS (a Mac with a case-sensitive volume
// keeps the Linux behaviour, and so does a Linux mount of an APFS disk).
//
// Where the filesystem cannot hold a pair of names, the pair is refused at
// ingest with an error naming both spellings, instead of being accepted and
// discovered later as a missing file.

var (
	caseMu    sync.Mutex
	caseCache = map[string]bool{}
)

// CaseInsensitive reports whether dir is on a case-insensitive filesystem. A
// uniquely named probe file is created and its name uppercased and stat'd
// back: on a case-insensitive filesystem that resolves to the probe itself,
// on a case-sensitive one it does not exist. A directory that cannot be
// probed (not created yet, unreadable, read-only) reports false — treated as
// case-sensitive, so the guards below never reject a name they cannot prove
// ambiguous — and is not cached, so the question is settled by the next call
// once the directory exists. A settled answer is cached per directory: the
// state directory does not move between filesystems while the process runs.
func CaseInsensitive(dir string) bool {
	caseMu.Lock()
	defer caseMu.Unlock()
	if v, ok := caseCache[dir]; ok {
		return v
	}
	v, ok := probeCaseInsensitive(dir)
	if !ok {
		// not probeable yet (the state directory may not have been created):
		// treat as case-sensitive without caching, so the answer is settled
		// by a later call once the directory exists
		return false
	}
	caseCache[dir] = v
	return v
}

// resetCaseCache forgets the probe results: tests that place a state
// directory on a different filesystem, and nothing in production.
func resetCaseCache() {
	caseMu.Lock()
	defer caseMu.Unlock()
	caseCache = map[string]bool{}
}

func probeCaseInsensitive(dir string) (insensitive, ok bool) {
	f, err := os.CreateTemp(dir, ".case-probe-*")
	if err != nil {
		return false, false
	}
	name := f.Name()
	f.Close()
	defer os.Remove(name)
	// the probe's name in the other case: the same file exactly when the
	// filesystem folds case. ToUpper leaves digits and dashes alone, so the
	// only characters that change are the random hex letters.
	_, err = os.Stat(filepath.Join(dir, strings.ToUpper(filepath.Base(name))))
	switch {
	case err == nil:
		return true, true
	case errors.Is(err, fs.ErrNotExist):
		// the other-case name is absent, so this filesystem distinguishes
		// case — a settled answer
		return false, true
	default:
		// an unreadable or otherwise failing stat proves nothing: report
		// "cannot tell" so the caller neither guards nor caches
		return false, false
	}
}

// caseInsensitiveAt reports whether the filesystem that would hold dir folds
// case, asking the nearest existing ancestor when dir itself is not there
// yet: folding is a property of the mount, and the destination of a
// materialization often does not exist before it is materialized. A dir that
// cannot be reached at all reports false — no guard, as before.
func caseInsensitiveAt(dir string) bool {
	for p := dir; ; {
		if _, err := os.Stat(p); err == nil {
			return CaseInsensitive(p)
		}
		parent := filepath.Dir(p)
		if parent == p {
			return false
		}
		p = parent
	}
}

// CaseNameCollision returns the error to report when name — whose on-disk
// entry inside dir is name+suffix — resolves to an existing sibling that
// differs only in case (macOS's APFS makes "Foo" and "foo" one entry, so the
// second create would write through the first). It returns nil when dir is
// case-sensitive, when no sibling collides, or when dir cannot be read: an
// exact-name match is the caller's own "already exists" case, and a
// case-sensitive filesystem keeps both names as distinct entries.
func CaseNameCollision(kind, dir, name, suffix string) error {
	if !CaseInsensitive(dir) {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	want := name + suffix
	for _, e := range entries {
		got := e.Name()
		if got == want || !strings.EqualFold(got, want) {
			continue
		}
		return fmt.Errorf("%s %q collides with existing %s %q: %s is on a case-insensitive filesystem, where the two names are one path — rename one of them",
			kind, name, kind, strings.TrimSuffix(got, suffix), dir)
	}
	return nil
}

// caseTwin returns the path already in view that differs from p only in
// case, or "" when none does. The caller decides whether the store's
// filesystem makes that a collision.
func caseTwin(view map[string]ViewEntry, p string) string {
	for q := range view {
		if q != p && strings.EqualFold(q, p) {
			return q
		}
	}
	return ""
}

// casePathConflict is the error for a path that would share a file with
// another path of the same revision on a case-insensitive filesystem.
func casePathConflict(p, twin, dir string) error {
	return fmt.Errorf("path %q collides with %q in the same revision: %s is on a case-insensitive filesystem, where the two paths are one file — rename one of them", p, twin, dir)
}

// caseInsensitive is the store's own filesystem: the state directory, which
// also holds the job staging directories these views are materialized into.
func (s *Store) caseInsensitive() bool { return CaseInsensitive(s.state) }

// caseInsensitiveDest reports whether the filesystem that would hold dir
// folds case. A destination inside the state directory — every job staging
// directory — is on the store's own filesystem, whose answer is already
// cached, so that path costs one map lookup and no syscalls. An egress
// destination can be a different mount, so it is judged by its nearest
// existing ancestor.
func (s *Store) caseInsensitiveDest(dir string) bool {
	if rel, err := filepath.Rel(s.state, dir); err == nil &&
		rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return s.caseInsensitive()
	}
	return caseInsensitiveAt(dir)
}

// checkCommitCase refuses a path that collides, ignoring case, with a
// different path of the same revision — the revision's view is what a job
// materializes, so accepting it would silently drop one of the two files.
//
// The view is resolved inside the gate, never by the caller: on a
// case-sensitive filesystem this costs one cached map lookup and the
// ancestry walk (the expensive part) does not happen at all. Passing a view
// as an argument would evaluate it on every put on Linux.
func (s *Store) checkCommitCase(rec *CommitRec, p string) error {
	if !s.caseInsensitive() {
		return nil
	}
	if twin := caseTwin(s.ResolveView(rec), p); twin != "" {
		return casePathConflict(p, twin, s.state)
	}
	return nil
}

// checkCommitEntriesCase is checkCommitCase for a batch of pending file ops:
// the path slice is built inside the gate, so a case-sensitive filesystem
// (the Linux default) allocates nothing for the guard.
func (s *Store) checkCommitEntriesCase(rec *CommitRec, entries []fileOp) error {
	if !s.caseInsensitive() {
		return nil
	}
	paths := make([]string, 0, len(entries))
	for _, e := range entries {
		paths = append(paths, e.Path)
	}
	return s.checkPathsCase(s.ResolveView(rec), paths)
}

// checkPathsCase is the batch comparison against an already-resolved view
// (the caller had one for its own reasons, e.g. an overwrite check).
func (s *Store) checkPathsCase(view map[string]ViewEntry, paths []string) error {
	if !s.caseInsensitive() {
		return nil
	}
	seen := make(map[string]string, len(view)+len(paths))
	for p := range view {
		seen[strings.ToLower(p)] = p
	}
	for _, p := range paths {
		k := strings.ToLower(p)
		if prev, ok := seen[k]; ok && prev != p {
			return casePathConflict(p, prev, s.state)
		}
		seen[k] = p
	}
	return nil
}

// viewCaseConflict reports the first pair of paths in view that differ only
// in case, or "" when the view holds no such pair.
func viewCaseConflict(view map[string]ViewEntry) (string, string) {
	if len(view) < 2 {
		return "", ""
	}
	seen := make(map[string]string, len(view))
	for p := range view {
		k := strings.ToLower(p)
		if prev, ok := seen[k]; ok && prev != p {
			// deterministic order: report the lexicographically smaller
			// path first, so the message is stable across map iteration
			if prev < p {
				return prev, p
			}
			return p, prev
		}
		seen[k] = p
	}
	return "", ""
}
