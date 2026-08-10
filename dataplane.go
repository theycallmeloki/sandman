package main

// Data plane: repositories, revisions (commits), and files, stored as plain
// files under <state>/repos/<repo>/ — Rule of Transparency. The layout is
// git-like: branch refs are one-line files, every commit is a JSON record
// listing the files written in it, and file content is stored once,
// content-addressed by sha256.
//
//	repos/<repo>/default          primary branch name (first committed)
//	repos/<repo>/refs/<branch>    commit id at the branch head
//	repos/<repo>/commits/<id>.json
//	repos/<repo>/objects/<aa>/<bbbb…>   blob content
//
// A commit's files are the paths written during it; the readable view at a
// commit merges its parents' files (child wins). A commit finished with the
// empty flag has no view at all: nothing is readable through it, even at
// the branch head (SB-118).

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"sandman/client"
)

// apiStore is the daemon's data-plane store. One global mutex is plenty:
// the API surface is a single writer and readers resolve views from
// immutable commit records.
type apiStore struct {
	dir string
	mu  sync.RWMutex
}

// fileEntry is one file written in a commit.
type fileEntry struct {
	Path string `json:"path"`
	SHA  string `json:"sha"` // hex sha256 of the content
	Size uint64 `json:"size"`
}

// commitRec is the persisted form of a commit.
type commitRec struct {
	ID          string      `json:"id"`
	Repo        string      `json:"repo"`
	Branch      string      `json:"branch"`
	Description string      `json:"description,omitempty"`
	ParentID    string      `json:"parentId,omitempty"`
	Started     bool        `json:"started"`
	Finished    bool        `json:"finished"`
	Empty       bool        `json:"empty"`
	Files       []fileEntry `json:"files,omitempty"`
}

const defaultBranch = "master"

func newAPIStore(stateDir string) *apiStore {
	return &apiStore{dir: filepath.Join(stateDir, "repos")}
}

// ---- name and path validation ----

// validName rejects names that could escape the store directory or collide
// with store internals.
func validName(name string) bool {
	return name != "" && name != "." && name != ".." &&
		!strings.HasPrefix(name, ".") &&
		!strings.ContainsAny(name, `/\`)
}

// validPath rejects file paths that could escape a directory when
// materialized on disk.
func validPath(p string) bool {
	if p == "" || strings.HasPrefix(p, "/") {
		return false
	}
	for _, comp := range strings.Split(p, "/") {
		if comp == "" || comp == "." || comp == ".." {
			return false
		}
	}
	return true
}

// ---- low-level record I/O ----

func (s *apiStore) repoDir(name string) string {
	return filepath.Join(s.dir, name)
}

func (s *apiStore) commitPath(repo, id string) string {
	return filepath.Join(s.repoDir(repo), "commits", id+".json")
}

func (s *apiStore) objectPath(sha string) string {
	return filepath.Join(s.dir, ".objects", sha[:2], sha[2:])
}

func (s *apiStore) loadCommit(repo, id string) (*commitRec, error) {
	b, err := os.ReadFile(s.commitPath(repo, id))
	if err != nil {
		return nil, err
	}
	var rec commitRec
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *apiStore) saveCommit(rec *commitRec) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	dir := filepath.Join(s.repoDir(rec.Repo), "commits")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(dir, rec.ID+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.commitPath(rec.Repo, rec.ID))
}

// writeBlob stores content under its sha256 and returns the hex digest.
func (s *apiStore) writeBlob(data []byte) (string, error) {
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])
	p := s.objectPath(sha)
	if _, err := os.Stat(p); err == nil {
		return sha, nil // already stored (dedupe)
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		return "", err
	}
	return sha, nil
}

func (s *apiStore) readBlob(sha string) ([]byte, error) {
	return os.ReadFile(s.objectPath(sha))
}

// ---- repositories ----

func (s *apiStore) createRepo(name string) error {
	if !validName(name) {
		return fmt.Errorf("invalid repo name %q", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := s.repoDir(name)
	if _, err := os.Stat(dir); err == nil {
		return fmt.Errorf("repo %q already exists", name)
	}
	return os.MkdirAll(dir, 0o755)
}

func (s *apiStore) deleteRepo(name string) error {
	if !validName(name) {
		return fmt.Errorf("invalid repo name %q", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := os.Stat(s.repoDir(name)); err != nil {
		return fmt.Errorf("repo %q not found", name)
	}
	return os.RemoveAll(s.repoDir(name))
}

// branches lists the branch names of a repo from its refs directory.
func (s *apiStore) branches(name string) []string {
	entries, err := os.ReadDir(filepath.Join(s.repoDir(name), "refs"))
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// inspectRepo reports the repo with its primary branch's head size.
func (s *apiStore) inspectRepo(name string) (client.Repo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dir := s.repoDir(name)
	if _, err := os.Stat(dir); err != nil {
		return client.Repo{}, fmt.Errorf("repo %q not found", name)
	}
	r := client.Repo{Name: name, Branches: s.branches(name)}
	if headID := s.primaryHead(name); headID != "" {
		if rec, err := s.loadCommit(name, headID); err == nil {
			for _, f := range s.resolveView(rec) {
				r.SizeBytes += f.Size
			}
		}
	}
	return r, nil
}

func (s *apiStore) listRepos() ([]client.Repo, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []client.Repo
	for _, e := range entries {
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			if r, err := s.inspectRepo(e.Name()); err == nil {
				out = append(out, r)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// primaryHead returns the id of the head commit of the primary branch.
func (s *apiStore) primaryHead(repo string) string {
	b, err := os.ReadFile(filepath.Join(s.repoDir(repo), "default"))
	if err != nil {
		return ""
	}
	return s.headCommit(repo, strings.TrimSpace(string(b)))
}

func (s *apiStore) headCommit(repo, branch string) string {
	b, err := os.ReadFile(filepath.Join(s.repoDir(repo), "refs", branch))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// setHead moves a branch ref, creating the refs dir and default marker on
// the first commit.
func (s *apiStore) setHead(repo, branch, id string) error {
	refs := filepath.Join(s.repoDir(repo), "refs")
	if err := os.MkdirAll(refs, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(refs, branch), []byte(id+"\n"), 0o644); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(s.repoDir(repo), "default")); os.IsNotExist(err) {
		if err := os.WriteFile(filepath.Join(s.repoDir(repo), "default"), []byte(branch+"\n"), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// ---- commits ----

func newCommitID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// startCommit opens a new revision on the branch. The repo is created if it
// does not exist (pipeline output repos are born this way). The parent is
// the branch's finished head at start time.
func (s *apiStore) startCommit(repo, branch, description string) (client.Commit, error) {
	if !validName(repo) {
		return client.Commit{}, fmt.Errorf("invalid repo name %q", repo)
	}
	if branch == "" {
		branch = defaultBranch
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(s.repoDir(repo), 0o755); err != nil {
		return client.Commit{}, err
	}
	rec := &commitRec{
		ID:          newCommitID(),
		Repo:        repo,
		Branch:      branch,
		Description: description,
		ParentID:    s.headCommit(repo, branch),
		Started:     true,
	}
	if err := s.saveCommit(rec); err != nil {
		return client.Commit{}, err
	}
	return rec.commit(), nil
}

// putFile writes one file into an open commit.
func (s *apiStore) putFile(commitID, p string, data []byte) error {
	if !validPath(p) {
		return fmt.Errorf("invalid file path %q", p)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.loadCommitByID(commitID)
	if err != nil {
		return fmt.Errorf("commit %q not found", commitID)
	}
	if !rec.Started || rec.Finished {
		return fmt.Errorf("commit %q is not open for writes", commitID)
	}
	sha, err := s.writeBlob(data)
	if err != nil {
		return err
	}
	// replace any earlier write to the same path in this commit
	files := make([]fileEntry, 0, len(rec.Files)+1)
	for _, f := range rec.Files {
		if f.Path != p {
			files = append(files, f)
		}
	}
	rec.Files = append(files, fileEntry{Path: p, SHA: sha, Size: uint64(len(data))})
	return s.saveCommit(rec)
}

// finishCommit closes the commit, advances the branch ref, and reports the
// final record. An explicitly empty commit (empty=true) is a real commit
// whose view is nothing — parent files are not readable through it (SB-118).
func (s *apiStore) finishCommit(commitID, description string, empty bool) (client.Commit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.loadCommitByID(commitID)
	if err != nil {
		return client.Commit{}, fmt.Errorf("commit %q not found", commitID)
	}
	if !rec.Started {
		return client.Commit{}, fmt.Errorf("commit %q is not open", commitID)
	}
	if rec.Finished {
		return client.Commit{}, fmt.Errorf("commit %q already finished", commitID)
	}
	if description != "" {
		rec.Description = description
	}
	rec.Empty = empty
	rec.Finished = true
	if err := s.saveCommit(rec); err != nil {
		return client.Commit{}, err
	}
	if err := s.setHead(rec.Repo, rec.Branch, rec.ID); err != nil {
		return client.Commit{}, err
	}
	return rec.commit(), nil
}

func (s *apiStore) inspectCommit(commitID string) (client.Commit, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, err := s.loadCommitByID(commitID)
	if err != nil {
		return client.Commit{}, fmt.Errorf("commit %q not found", commitID)
	}
	return rec.commit(), nil
}

func (s *apiStore) headCommitRec(repo, branch string) (client.Commit, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := os.Stat(s.repoDir(repo)); err != nil {
		return client.Commit{}, fmt.Errorf("repo %q not found", repo)
	}
	id := s.headCommit(repo, branch)
	if id == "" {
		return client.Commit{}, fmt.Errorf("branch %q has no head", branch)
	}
	rec, err := s.loadCommit(repo, id)
	if err != nil {
		return client.Commit{}, err
	}
	return rec.commit(), nil
}

// loadCommitByID finds a commit record across all repos by id.
func (s *apiStore) loadCommitByID(commitID string) (*commitRec, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if rec, err := s.loadCommit(e.Name(), commitID); err == nil {
			return rec, nil
		}
	}
	return nil, fmt.Errorf("commit %q not found", commitID)
}

func (rec *commitRec) commit() client.Commit {
	return client.Commit{
		ID:          rec.ID,
		Repo:        rec.Repo,
		Branch:      rec.Branch,
		Description: rec.Description,
		Started:     rec.Started,
		Finished:    rec.Finished,
		Empty:       rec.Empty,
		ParentID:    rec.ParentID,
	}
}

// resolveView merges the commit's own files over its ancestors' (child
// wins). An explicitly empty commit is a barrier: its view is nothing and
// nothing below it merges in — a child of an empty commit shows only its
// own files (SB-118).
func (s *apiStore) resolveView(rec *commitRec) map[string]fileEntry {
	view := map[string]fileEntry{}
	for cur := rec; cur != nil; {
		if cur.Empty {
			break
		}
		for _, f := range cur.Files {
			view[f.Path] = f
		}
		if cur.ParentID == "" {
			break
		}
		parent, err := s.loadCommit(cur.Repo, cur.ParentID)
		if err != nil {
			break
		}
		cur = parent
	}
	return view
}

// resolveViewByID is resolveView for a commit id (must be called under the
// store lock).
func (s *apiStore) resolveViewByID(commitID string) (map[string]fileEntry, error) {
	rec, err := s.loadCommitByID(commitID)
	if err != nil {
		return nil, err
	}
	return s.resolveView(rec), nil
}

// ---- file access ----

func (s *apiStore) listFiles(commitID string) ([]client.FileInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	view, err := s.resolveViewByID(commitID)
	if err != nil {
		return nil, err
	}
	out := make([]client.FileInfo, 0, len(view))
	for p, f := range view {
		out = append(out, client.FileInfo{Path: p, Size: f.Size})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

func (s *apiStore) getFile(commitID, p string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	view, err := s.resolveViewByID(commitID)
	if err != nil {
		return nil, err
	}
	f, ok := view[p]
	if !ok {
		return nil, fmt.Errorf("file %q not found", p)
	}
	return s.readBlob(f.SHA)
}

// materializeInput writes the full view of a commit into dir, preserving
// relative paths — the job's view of its input.
func (s *apiStore) materializeInput(commitID, dir string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	view, err := s.resolveViewByID(commitID)
	if err != nil {
		return err
	}
	for p, f := range view {
		data, err := s.readBlob(f.SHA)
		if err != nil {
			return err
		}
		dst := filepath.Join(dir, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(dst, data, 0o644); err != nil {
			return err
		}
	}
	return nil
}
