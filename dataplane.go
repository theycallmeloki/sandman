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
	"time"

	"sandman/client"
)

// apiStore is the daemon's data-plane store. One global mutex is plenty:
// the API surface is a single writer and readers resolve views from
// immutable commit records.
type apiStore struct {
	dir string
	mu  sync.RWMutex
	// onFinish, when set, is called after every commit finish — the
	// daemon's state-change broadcast for the blocking waits (D-23 R-5).
	onFinish func()
}

// fileEntry is one file written in a commit (legacy supersede model,
// decoded only for pre-append records).
type fileEntry struct {
	Path string `json:"path"`
	SHA  string `json:"sha"` // hex sha256 of the content
	Size uint64 `json:"size"`
}

// fileOp is one write operation in a commit, in write order: an append
// (FS-1), an overwrite that replaces accumulated content (FS-3), or a
// delete tombstone (FS-4). Order within the commit is load-bearing — a
// delete then a write leaves only the write; a write then a delete leaves
// nothing (FS-4 edges).
type fileOp struct {
	Path      string `json:"path"`
	SHA       string `json:"sha"` // hex sha256 of the written bytes ("" for a delete)
	Size      uint64 `json:"size"`
	Overwrite bool   `json:"overwrite,omitempty"` // replace accumulated content (FS-3)
	Delete    bool   `json:"delete,omitempty"`    // tombstone (FS-4)
}

// viewPart is one contribution to a path's accumulated content in the
// resolved view: an append contributes its bytes, an overwrite resets the
// accumulation and starts a new one (FS-2/FS-3).
type viewPart struct {
	SHA       string
	Size      uint64
	Overwrite bool
}

// viewEntry is a path's accumulated content in a resolved view. Content
// is resolved lazily (bytes/hash on demand): the view itself only records
// the ordered contributions, so path-only consumers (globs, sizes) never
// pay for concatenation.
type viewEntry struct {
	parts []viewPart
}

// size is the accumulated byte count: an overwrite resets the sum, an
// append adds to it (zero-byte parts are no-ops either way, FS-8).
func (e viewEntry) size() uint64 {
	var n uint64
	for _, p := range e.parts {
		if p.Overwrite {
			n = p.Size
		} else {
			n += p.Size
		}
	}
	return n
}

// bytes concatenates the accumulated content in order, resetting the
// buffer at each overwrite contribution.
func (e viewEntry) bytes(s *apiStore) ([]byte, error) {
	var buf []byte
	for _, p := range e.parts {
		b, err := s.readBlob(p.SHA)
		if err != nil {
			return nil, err
		}
		if p.Overwrite {
			buf = buf[:0]
		}
		buf = append(buf, b...)
	}
	return buf, nil
}

// hash is the hex sha256 of the accumulated content — the path's content
// identity at this revision (datum dedup keys, SB-054 hash equality).
func (e viewEntry) hash(s *apiStore) (string, error) {
	b, err := e.bytes(s)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// commitRec is the persisted form of a commit.
type commitRec struct {
	ID          string `json:"id"`
	Repo        string `json:"repo"`
	Branch      string `json:"branch"`
	Description string `json:"description,omitempty"`
	ParentID    string `json:"parentId,omitempty"`
	Started     bool   `json:"started"`
	// CreatedAt is the commit's creation time (UTC RFC3339Nano), the
	// ordering key for schedules like cron ticks (SB-089/133).
	CreatedAt string   `json:"createdAt,omitempty"`
	Finished  bool     `json:"finished"`
	Empty     bool     `json:"empty"`
	Ops       []fileOp `json:"ops,omitempty"`
	// Provenance is the revision's derivation: the source commits it
	// consumes, transitively. A spout commit records its pipeline's
	// specification commit (the epoch it belongs to, SB-140); a job's
	// output commit records its input commits and their own provenance.
	// Inspect exposes it so epochs and subvenance are observable.
	Provenance []string `json:"provenance,omitempty"`
	// Files/Deleted are the legacy supersede-model fields, decoded only
	// for records written before the append model (loadCommit converts
	// them to ops; new records never set them).
	Files   []fileEntry `json:"files,omitempty"`
	Deleted []string    `json:"deleted,omitempty"`
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

// loadCommit reads a commit record, converting the legacy supersede
// model (Files + Deleted) to the ordered-ops model on the way in: old
// records wrote child-wins, which the op model expresses as overwrites
// with the deletions applied first (matching the old resolveView order).
func (s *apiStore) loadCommit(repo, id string) (*commitRec, error) {
	b, err := os.ReadFile(s.commitPath(repo, id))
	if err != nil {
		return nil, err
	}
	var rec commitRec
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, err
	}
	if len(rec.Ops) == 0 && (len(rec.Files) > 0 || len(rec.Deleted) > 0) {
		for _, d := range rec.Deleted {
			rec.Ops = append(rec.Ops, fileOp{Path: d, Delete: true})
		}
		for _, f := range rec.Files {
			rec.Ops = append(rec.Ops, fileOp{Path: f.Path, SHA: f.SHA, Size: f.Size, Overwrite: true})
		}
		rec.Files = nil
		rec.Deleted = nil
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

func (s *apiStore) deleteRepo(name string, force bool) error {
	if !validName(name) {
		return fmt.Errorf("invalid repo name %q", name)
	}
	if name == "spec" {
		// the internal pipeline-specification repository is protected
		// unconditionally (SB-127)
		return fmt.Errorf("the spec repo cannot be deleted")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := os.Stat(s.repoDir(name)); err != nil {
		return fmt.Errorf("repo %q not found", name)
	}
	if !force {
		// a pipeline's output repository is protected from accidental
		// deletion; force is the explicit override (SB-146)
		if _, err := os.Stat(filepath.Join(filepath.Dir(s.dir), "pipelines", name+".json")); err == nil {
			return fmt.Errorf("repo %q is the output of pipeline %q; force required", name, name)
		}
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
	if !validName(name) {
		return client.Repo{}, fmt.Errorf("invalid repo name %q", name)
	}
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
				r.SizeBytes += f.size()
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
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") && e.Name() != "spec" {
			// "spec" is the internal pipeline-definition repository and is
			// not a user repository (SB-127)
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
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := s.saveCommit(rec); err != nil {
		return client.Commit{}, err
	}
	return rec.commit(), nil
}

// putFile writes one file into an open commit as an append (FS-1): a
// path already holding content — in this commit or its ancestry — grows
// by the new bytes at this revision. Replacing content is an explicit
// overwrite (overwriteFile, FS-3) or a delete-then-write (FS-4).
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
	rec.Ops = append(rec.Ops, fileOp{Path: p, SHA: sha, Size: uint64(len(data))})
	return s.saveCommit(rec)
}

// overwriteFile writes one file into an open commit replacing any
// accumulated content at the path (FS-3): the path's prior content — in
// this commit or its ancestry — is removed and the new bytes become the
// entire content at this revision.
func (s *apiStore) overwriteFile(commitID, p string, data []byte) error {
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
	rec.Ops = append(rec.Ops, fileOp{Path: p, SHA: sha, Size: uint64(len(data)), Overwrite: true})
	return s.saveCommit(rec)
}

// deleteFile tombstones a path in an open commit (FS-4): the path is
// removed from the branch's view at this revision, whether its content
// came from this commit or the ancestry. Write order within the commit
// decides conflicts — a later write recreates the path with only its own
// content; a later delete removes it again.
func (s *apiStore) deleteFile(commitID, p string) error {
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
	rec.Ops = append(rec.Ops, fileOp{Path: p, Delete: true})
	return s.saveCommit(rec)
}

// tombstoneRemoved records in a commit the paths that vanished from its
// parent's view: the output side of a deletion (SB-007 — a pipeline's
// output revision reflects the deletion, so the deleted file is genuinely
// absent, not stale).
func (s *apiStore) tombstoneRemoved(commitID, outDir string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.loadCommitByID(commitID)
	if err != nil {
		return err
	}
	var parent map[string]viewEntry
	if rec.ParentID != "" {
		if p, err := s.loadCommit(rec.Repo, rec.ParentID); err == nil {
			parent = s.resolveView(p)
		}
	}
	newPaths := map[string]bool{}
	_ = walkFiles(outDir, nil, func(rel string, _ []byte) error {
		newPaths[rel] = true
		return nil
	})
	var removed []string
	for p := range parent {
		if !newPaths[p] {
			removed = append(removed, p)
		}
	}
	sort.Strings(removed)
	for _, p := range removed {
		rec.Ops = append(rec.Ops, fileOp{Path: p, Delete: true})
	}
	return s.saveCommit(rec)
}

// walkFiles visits every file under dir with its content, following
// symlinks: a symlink to a file yields the target's content at the link's
// path, and a symlink to a directory yields the target's files under the
// link's path prefix (SB-054 — a pipeline may emit symlinked output). A
// depth cap breaks link cycles. linkTarget maps a symlink's target to a
// host path when the target is container-internal (e.g. /sandman/in/...);
// an empty result falls back to the native resolution.
func walkFiles(dir string, linkTarget func(string) string, visit func(rel string, data []byte) error) error {
	return walkFilesDepth(dir, "", 0, linkTarget, visit)
}

func walkFilesDepth(p, rel string, depth int, linkTarget func(string) string, visit func(rel string, data []byte) error) error {
	if depth > 64 {
		return fmt.Errorf("symlink depth exceeded at %q", p)
	}
	entries, err := os.ReadDir(p) // follows a symlinked directory
	if err != nil {
		return err
	}
	for _, e := range entries {
		child := filepath.Join(p, e.Name())
		crel := filepath.ToSlash(filepath.Join(rel, e.Name()))
		info, err := os.Lstat(child)
		if err != nil {
			return err
		}
		resolved := child
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(child)
			if err != nil {
				return err
			}
			if linkTarget != nil {
				if mapped := linkTarget(target); mapped != "" {
					resolved = mapped
				}
			}
			info, err = os.Stat(resolved) // follows the (possibly mapped) target
			if err != nil {
				return err
			}
		}
		if info.IsDir() {
			if err := walkFilesDepth(resolved, crel, depth+1, linkTarget, visit); err != nil {
				return err
			}
			continue
		}
		if !info.Mode().IsRegular() {
			// pipes, sockets, devices: reading a FIFO would block forever
			// and the content is not storable — the job must fail rather
			// than hang or upload garbage (SB-017)
			return fmt.Errorf("output contains special file %q", crel)
		}
		data, err := os.ReadFile(resolved)
		if err != nil {
			return err
		}
		if err := visit(crel, data); err != nil {
			return err
		}
	}
	return nil
}

// addFilesFromDir stores every file under dir into an open commit in one
// batch — the job output uploader (a single job can produce tens of
// thousands of files, SB-047). Each path is written as an overwrite: job
// output assembly replaces a path's prior content with the job's fresh
// output (FS-6 — a reprocessed datum's output replaces its prior output;
// unchanged datums' paths are absent from the job's output and carry
// forward through the ancestry). The directory holds the fully assembled
// output, so it must not accumulate over the parent's.
func (s *apiStore) addFilesFromDir(commitID, dir string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.loadCommitByID(commitID)
	if err != nil {
		return fmt.Errorf("commit %q not found", commitID)
	}
	if !rec.Started || rec.Finished {
		return fmt.Errorf("commit %q is not open for writes", commitID)
	}
	var entries []fileOp
	walkErr := walkFiles(dir, nil, func(rel string, data []byte) error {
		sha, err := s.writeBlob(data)
		if err != nil {
			return err
		}
		entries = append(entries, fileOp{Path: rel, SHA: sha, Size: uint64(len(data)), Overwrite: true})
		return nil
	})
	if walkErr != nil {
		return walkErr
	}
	rec.Ops = append(rec.Ops, entries...)
	return s.saveCommit(rec)
}

// copyFile copies a file or directory subtree from srcCommit into an open
// dstCommit at dstPath. The destination must not exist anywhere in the
// destination commit's view (parents included) unless overwrite is set —
// overwrite protection (SB-156); with overwrite the copy replaces the
// destination's accumulated content (FS-3). A directory copy lands each
// contained file at dstPath/<relative path>.
func (s *apiStore) copyFile(dstCommitID, dstPath, srcCommitID, srcPath string, overwrite bool) error {
	if !validPath(dstPath) {
		return fmt.Errorf("invalid file path %q", dstPath)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dstRec, err := s.loadCommitByID(dstCommitID)
	if err != nil {
		return fmt.Errorf("commit %q not found", dstCommitID)
	}
	if !dstRec.Started || dstRec.Finished {
		return fmt.Errorf("commit %q is not open for writes", dstCommitID)
	}
	srcView, err := s.resolveViewByID(srcCommitID)
	if err != nil {
		return err
	}
	type move struct{ src, dst string }
	var moves []move
	if f, ok := srcView[srcPath]; ok && !strings.HasSuffix(srcPath, "/") {
		moves = append(moves, move{src: srcPath, dst: dstPath})
		_ = f
	} else {
		prefix := srcPath + "/"
		found := false
		for p := range srcView {
			if strings.HasPrefix(p, prefix) {
				moves = append(moves, move{src: p, dst: dstPath + "/" + p[len(prefix):]})
				found = true
			}
		}
		if !found {
			return fmt.Errorf("path %q not found", srcPath)
		}
	}
	dstView := s.resolveView(dstRec)
	for _, m := range moves {
		if _, exists := dstView[m.dst]; exists && !overwrite {
			return fmt.Errorf("path %q already exists in commit %q", m.dst, dstCommitID)
		}
	}
	for _, m := range moves {
		e := srcView[m.src]
		data, err := e.bytes(s)
		if err != nil {
			return err
		}
		sha, err := s.writeBlob(data) // dedupes: identical content, same object
		if err != nil {
			return err
		}
		op := fileOp{Path: m.dst, SHA: sha, Size: e.size()}
		if overwrite {
			op.Overwrite = true
		}
		dstRec.Ops = append(dstRec.Ops, op)
	}
	return s.saveCommit(dstRec)
}

// finishCommit closes the commit, advances the branch ref, and reports the
// final record. An explicitly empty commit (empty=true) is a real commit
// whose view is nothing — parent files are not readable through it (SB-118).
// reparent retargets an open commit's parent to the current branch head.
// Output commits are opened at job start, before the head is final; the
// last writer must re-parent so the branch stays linear and no finished
// commit is orphaned off the chain (concurrent jobs of one pipeline).
func (s *apiStore) reparent(commitID, parentID string) error {
	if parentID == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.loadCommitByID(commitID)
	if err != nil {
		return err
	}
	if rec.Finished {
		return fmt.Errorf("commit %q already finished", commitID)
	}
	if rec.ParentID == parentID {
		return nil
	}
	rec.ParentID = parentID
	return s.saveCommit(rec)
}

// finishCommit closes a commit as a real revision or as an explicit empty
// barrier (SB-118), advancing the branch head.
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
	// a path that is both a file and a directory prefix of another path is
	// a type conflict; finishing fails (FS-1/FS-5 edges — "x" then "x/y")
	if conflict := s.pathConflict(rec); conflict != "" {
		return client.Commit{}, fmt.Errorf("type conflict at path %q", conflict)
	}
	rec.Empty = empty
	rec.Finished = true
	if err := s.saveCommit(rec); err != nil {
		return client.Commit{}, err
	}
	if err := s.setHead(rec.Repo, rec.Branch, rec.ID); err != nil {
		return client.Commit{}, err
	}
	if s.onFinish != nil {
		s.onFinish()
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
	if !validName(repo) {
		return client.Commit{}, fmt.Errorf("invalid repo name %q", repo)
	}
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
		CreatedAt:   rec.CreatedAt,
		Finished:    rec.Finished,
		Empty:       rec.Empty,
		ParentID:    rec.ParentID,
		Provenance:  rec.Provenance,
	}
}

// pathConflict reports a path that is both a file and a directory prefix
// of another path in the commit's resolved view — a type conflict that
// must fail finishing (FS-1 edge: writing "x" then "x/y" in one commit;
// FS-2: a child writing "x/y" over an inherited file "x").
func (s *apiStore) pathConflict(rec *commitRec) string {
	view := s.resolveView(rec)
	paths := make([]string, 0, len(view))
	for p := range view {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for i := 1; i < len(paths); i++ {
		if strings.HasPrefix(paths[i], paths[i-1]+"/") {
			return paths[i-1]
		}
	}
	return ""
}

// resolveView resolves a commit's accumulated view (FS-1..FS-4): walking
// the ancestry oldest first, each commit's write operations replay in
// order — an append contributes its bytes to the path's accumulated
// content, an overwrite resets it, a delete removes the path (removing
// inherited content too). An explicitly empty commit is a barrier: its
// view is nothing and nothing below it merges in — a child of an empty
// commit shows only its own ops (SB-118).
func (s *apiStore) resolveView(rec *commitRec) map[string]viewEntry {
	var chain []*commitRec
	for cur := rec; cur != nil && !cur.Empty; {
		chain = append(chain, cur)
		if cur.ParentID == "" {
			break
		}
		parent, err := s.loadCommit(cur.Repo, cur.ParentID)
		if err != nil {
			break
		}
		cur = parent
	}
	view := map[string]viewEntry{}
	for i := len(chain) - 1; i >= 0; i-- { // oldest first, so the newest ops apply last
		for _, op := range chain[i].Ops {
			switch {
			case op.Delete:
				delete(view, op.Path)
			case op.Overwrite:
				view[op.Path] = viewEntry{parts: []viewPart{{SHA: op.SHA, Size: op.Size, Overwrite: true}}}
			default:
				e := view[op.Path]
				e.parts = append(e.parts, viewPart{SHA: op.SHA, Size: op.Size})
				view[op.Path] = e
			}
		}
	}
	return view
}

// resolveViewByID is resolveView for a commit id (must be called under the
// store lock).
func (s *apiStore) resolveViewByID(commitID string) (map[string]viewEntry, error) {
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
		h, err := f.hash(s)
		if err != nil {
			return nil, err
		}
		out = append(out, client.FileInfo{Path: p, Size: f.size(), Hash: h})
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
	return f.bytes(s)
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
	return s.materializeView(view, dir)
}

// materializeView writes an already-resolved view into dir.
func (s *apiStore) materializeView(view map[string]viewEntry, dir string) error {
	for p, f := range view {
		data, err := f.bytes(s)
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

// viewDatums lists the input files of a commit with their content hashes —
// the datum set of a job processing that commit (SB-060 log filters). The
// whole view is the datum set: a job runs over the full input revision.
func (s *apiStore) viewDatums(commitID string) ([]datumRef, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	view, err := s.resolveViewByID(commitID)
	if err != nil {
		return nil, err
	}
	out := make([]datumRef, 0, len(view))
	for p, f := range view {
		h, err := f.hash(s)
		if err != nil {
			return nil, err
		}
		out = append(out, datumRef{Path: p, Hash: h})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

// chainFromHead lists the commit ids of a branch from the head down to
// (excluding) stopAt, oldest first — the backlog a stopped pipeline must
// process on restart (SB-048).
func (s *apiStore) chainFromHead(repo, branch, stopAt string) []string {
	var chain []string
	id := s.headCommit(repo, branch)
	for id != "" && id != stopAt {
		chain = append(chain, id)
		rec, err := s.loadCommit(repo, id)
		if err != nil {
			break
		}
		id = rec.ParentID
	}
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain
}

// ---- tags (SB-150) ----
//
// Tags are durable global names bound to file content: <state>/tags/<name>
// holds the sha of the tagged blob, which lives in the content-addressed
// object store. Listing a tag yields its reference; retrieval resolves it
// byte-for-byte.

type tagInfo struct {
	Name string `json:"name"`
	Ref  string `json:"ref"`
}

func (s *apiStore) tagPath(name string) string {
	return filepath.Join(filepath.Dir(s.dir), "tags", name)
}

func (s *apiStore) putTag(name string, data []byte) error {
	if !validName(name) {
		return fmt.Errorf("invalid tag name %q", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sha, err := s.writeBlob(data)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.tagPath(name)), 0o755); err != nil {
		return err
	}
	return os.WriteFile(s.tagPath(name), []byte(sha), 0o644)
}

func (s *apiStore) getTag(name string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sha, err := os.ReadFile(s.tagPath(name))
	if err != nil {
		return nil, fmt.Errorf("tag %q not found", name)
	}
	return s.readBlob(strings.TrimSpace(string(sha)))
}

func (s *apiStore) listTags() ([]tagInfo, error) {
	entries, err := os.ReadDir(filepath.Dir(s.dir) + "/tags")
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]tagInfo, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		ref, err := os.ReadFile(filepath.Join(filepath.Dir(s.dir), "tags", e.Name()))
		if err != nil {
			continue
		}
		out = append(out, tagInfo{Name: e.Name(), Ref: strings.TrimSpace(string(ref))})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
