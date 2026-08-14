package store

// The data store: repositories, revisions (commits), and files, stored as
// plain files under <state>/repos/<repo>/ — Rule of Transparency. The
// layout is git-like: branch refs are one-line files, every commit is a
// JSON record listing the files written in it, and file content is stored
// once, content-addressed by sha256.
//
//	repos/<repo>/default          primary branch name (first committed)
//	repos/<repo>/refs/<branch>    commit id at the branch head
//	repos/<repo>/commits/<id>.json
//	repos/<repo>/objects/<aa>/<bbbb…>   blob content
//
// A commit's files are the paths written during it; the readable view at a
// commit merges its parents' files (child wins). A commit finished with the
// empty flag has no view at all: nothing is readable through it, even at
// the branch head.
import (
	"archive/tar"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"sandman/client"
)

// Store is the daemon's data-plane store. One global mutex is plenty:
// the API surface is a single writer and readers resolve views from
// immutable commit records.
type Store struct {
	dir string
	mu  sync.RWMutex
	// onFinish, when set, is called after every commit finish — the
	// daemon's state-change broadcast for the blocking waits.
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
// an overwrite that replaces accumulated content, or a
// delete tombstone. Order within the commit is load-bearing — a
// delete then a write leaves only the write; a write then a delete leaves
// nothing.
type fileOp struct {
	Path      string `json:"path"`
	SHA       string `json:"sha"` // hex sha256 of the written bytes ("" for a delete)
	Size      uint64 `json:"size"`
	Overwrite bool   `json:"overwrite,omitempty"` // replace accumulated content
	Delete    bool   `json:"delete,omitempty"`    // tombstone
}

// ViewPart is one contribution to a path's accumulated content in the
// resolved view: an append contributes its bytes, an overwrite resets the
// accumulation and starts a new one ().
type ViewPart struct {
	SHA       string
	Size      uint64
	Overwrite bool
}

// ViewEntry is a path's accumulated content in a resolved view. Content
// is resolved lazily (bytes/hash on demand): the view itself only records
// the ordered contributions, so path-only consumers (globs, sizes) never
// pay for concatenation.
type ViewEntry struct {
	parts []ViewPart
}

// Parts is the entry's write history: the file parts (overwrite/append
// segments) that produced its current content, in order.
func (e ViewEntry) Parts() []ViewPart { return e.parts }

// size is the accumulated byte count: an overwrite resets the sum, an
// append adds to it (zero-byte parts are no-ops either way.
func (e ViewEntry) Size() uint64 {
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
func (e ViewEntry) Bytes(s *Store) ([]byte, error) {
	var buf []byte
	for _, p := range e.parts {
		b, err := s.ReadBlob(p.SHA)
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
// identity at this revision (datum dedup keys, hash equality).
func (e ViewEntry) Hash(s *Store) (string, error) {
	b, err := e.Bytes(s)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// CommitRec is the persisted form of a commit.
type CommitRec struct {
	ID          string `json:"id"`
	Repo        string `json:"repo"`
	Branch      string `json:"branch"`
	Description string `json:"description,omitempty"`
	ParentID    string `json:"parentId,omitempty"`
	Started     bool   `json:"started"`
	// CreatedAt is the commit's creation time (UTC RFC3339Nano), the
	// ordering key for schedules like cron ticks.
	CreatedAt string   `json:"createdAt,omitempty"`
	Finished  bool     `json:"finished"`
	Empty     bool     `json:"empty"`
	Ops       []fileOp `json:"ops,omitempty"`
	// Provenance is the revision's derivation: the source commits it
	// consumes, transitively. A spout commit records its pipeline's
	// specification commit (the epoch it belongs to); a job's
	// output commit records its input commits and their own provenance.
	// Inspect exposes it so epochs and subvenance are observable.
	Provenance []string `json:"provenance,omitempty"`
	// Files/Deleted are the legacy supersede-model fields, decoded only
	// for records written before the append model (loadCommit converts
	// them to ops; new records never set them).
	Files   []fileEntry `json:"files,omitempty"`
	Deleted []string    `json:"deleted,omitempty"`
}

const DefaultBranch = "master"

func New(stateDir string) *Store {
	return &Store{dir: filepath.Join(stateDir, "repos")}
}

// SetOnFinish installs the callback invoked after every commit finish
// (the daemon's state-change broadcast for the blocking waits).
func (s *Store) SetOnFinish(fn func()) { s.onFinish = fn }

// Dir is the store's root: the parent of every repo directory, including
// the hidden internal repositories (the consistency check walks all of
// them, not just the visible listing).
func (s *Store) Dir() string { return s.dir }

// ---- name and path validation ----

// ErrNotFound marks a store error as "the named resource does not exist".
// The daemon's HTTP classifier maps it to 404 by type (errors.Is), never
// by message text.
var ErrNotFound = errors.New("not found")

// notFound marks a store error as not-found. The returned error's message
// is the caller's message, byte-identical to an unwrapped fmt.Errorf.
func notFound(format string, args ...any) error {
	return &markerError{msg: fmt.Sprintf(format, args...), marker: ErrNotFound}
}

type markerError struct {
	msg    string
	marker error
}

func (e *markerError) Error() string { return e.msg }
func (e *markerError) Unwrap() error { return e.marker }

// Now returns the current UTC time in the wire format every durable
// record timestamp uses (RFC3339Nano).
func Now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// ValidName rejects names that could escape the store directory or collide
// with store internals. It accepts any name that is a safe state-dir path
// component — no path separators, not "..", no leading dot — so pipeline
// names may contain hyphens and underscores: names like "my-pipeline_01"
// are legal.
func ValidName(name string) bool {
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

// TarDir writes dir's tree to tw as tar entries rooted at prefix. Missing
// dirs and non-dir roots are skipped; files ending in .tmp (mid-rename
// scratch) never enter an archive.
func (s *Store) TarDir(tw *tar.Writer, dir, prefix string) error {
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !info.IsDir() {
		return nil
	}
	prev := ""
	return filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() && p == dir {
			return nil // children carry the prefix
		}
		if !fi.IsDir() && strings.HasSuffix(fi.Name(), ".tmp") {
			return nil
		}
		defer func() { prev = p }()
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		hdr := &tar.Header{
			Name:    prefix + "/" + filepath.ToSlash(rel),
			Mode:    int64(fi.Mode().Perm()),
			ModTime: fi.ModTime(),
		}
		if fi.IsDir() {
			hdr.Typeflag = tar.TypeDir
			hdr.Name += "/"
		} else {
			hdr.Typeflag = tar.TypeReg
			hdr.Size = fi.Size()
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return fmt.Errorf("backup %s (after %s): %w", p, prev, err)
		}
		if fi.IsDir() {
			return nil
		}
		// Copy exactly the stat'd size: the daemon-owned dirs are not
		// under the store lock, and a transform writing into a job's out
		// dir can change a file between stat and copy. A grown file is
		// snapshotted at its stat-time prefix (consistent point-in-time
		// semantics); a shrunk file (truncate+rewrite) is retried once —
		// atomic tmp+rename writers make the retry stable.
		size := fi.Size()
		for attempt := 0; ; attempt++ {
			f, err := os.Open(p)
			if err != nil {
				return err
			}
			_, err = io.CopyN(tw, f, size)
			f.Close()
			if err == nil {
				return nil
			}
			if err == io.EOF && attempt == 0 {
				continue
			}
			return fmt.Errorf("backup %s (after %s): %w", p, prev, err)
		}
	})
}

// BackupTar writes the store's durable state — repositories and tags —
// into tw (the caller owns the tar stream). The store's write lock is
// held for the whole copy: in the single-writer design the mutex is the
// buffer — pending repo writes queue on it and land after the snapshot,
// so a captured ref can never point at a commit missing from the archive.
func (s *Store) BackupTar(tw *tar.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.TarDir(tw, s.dir, "repos"); err != nil {
		return err
	}
	if err := s.TarDir(tw, filepath.Join(filepath.Dir(s.dir), "tags"), "tags"); err != nil {
		return err
	}
	return nil
}

func (s *Store) RepoDir(name string) string {
	return filepath.Join(s.dir, name)
}

func (s *Store) CommitPath(repo, id string) string {
	return filepath.Join(s.RepoDir(repo), "commits", id+".json")
}

func (s *Store) ObjectPath(sha string) string {
	return filepath.Join(s.dir, ".objects", sha[:2], sha[2:])
}

// loadCommit reads a commit record, converting the legacy supersede
// model (Files + Deleted) to the ordered-ops model on the way in: old
// records wrote child-wins, which the op model expresses as overwrites
// with the deletions applied first (matching the old resolveView order).
func (s *Store) LoadCommit(repo, id string) (*CommitRec, error) {
	b, err := os.ReadFile(s.CommitPath(repo, id))
	if err != nil {
		return nil, err
	}
	var rec CommitRec
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

func (s *Store) SaveCommit(rec *CommitRec) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	dir := filepath.Join(s.RepoDir(rec.Repo), "commits")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(dir, rec.ID+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.CommitPath(rec.Repo, rec.ID))
}

// writeBlob stores content under its sha256 and returns the hex digest.
func (s *Store) WriteBlob(data []byte) (string, error) {
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])
	p := s.ObjectPath(sha)
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

func (s *Store) ReadBlob(sha string) ([]byte, error) {
	return os.ReadFile(s.ObjectPath(sha))
}

// ---- repositories ----

func (s *Store) CreateRepo(name string) error {
	if !ValidName(name) {
		return fmt.Errorf("invalid repo name %q", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := s.RepoDir(name)
	if _, err := os.Stat(dir); err == nil {
		return fmt.Errorf("repo %q already exists", name)
	}
	return os.MkdirAll(dir, 0o755)
}

// DeleteRepo removes a repository. Deletion must not depend on the serving
// membership that existed at creation: a repo with a finished commit remains
// deletable after the set of serving instances has changed, because the
// registry retains no stale placement that breaks the delete, and non-forced
// deletion succeeds and removes the repo from listing. The internal
// pipeline-specification repository is protected unconditionally: any
// deletion attempt through the public repository API is rejected with an
// error, it is never listed as a user repo, it is recreated empty on daemon
// start and reset, and its commits remain durable references.
func (s *Store) DeleteRepo(name string, force bool) error {
	if !ValidName(name) {
		return fmt.Errorf("invalid repo name %q", name)
	}
	if name == "spec" {
		// the internal pipeline-specification repository is protected
		// unconditionally
		return fmt.Errorf("the spec repo cannot be deleted")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := os.Stat(s.RepoDir(name)); err != nil {
		return notFound("repo %q not found", name)
	}
	if !force {
		// a pipeline's output repository is protected from accidental
		// deletion; force is the explicit override
		if _, err := os.Stat(filepath.Join(filepath.Dir(s.dir), "pipelines", name+".json")); err == nil {
			return fmt.Errorf("repo %q is the output of pipeline %q; force required", name, name)
		}
	}
	return os.RemoveAll(s.RepoDir(name))
}

// branches lists the branch names of a repo from its refs directory.
func (s *Store) Branches(name string) []string {
	entries, err := os.ReadDir(filepath.Join(s.RepoDir(name), "refs"))
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

// inspectRepo reports the repo with its primary branch's head size: the
// reported size is the total byte count of the files in the primary branch's
// head revision — the head commit's resolved view is summed, so files on
// other branches never count and files written through implicit commits do.
// Pipeline output repositories report the same accounting, so an output repo
// reports the size of its head (never 0 after processing); deleting a head
// commit shrinks both the input and the output repository by exactly the
// contributed bytes.
func (s *Store) InspectRepo(name string) (client.Repo, error) {
	if !ValidName(name) {
		return client.Repo{}, fmt.Errorf("invalid repo name %q", name)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	dir := s.RepoDir(name)
	if _, err := os.Stat(dir); err != nil {
		return client.Repo{}, notFound("repo %q not found", name)
	}
	r := client.Repo{Name: name, Branches: s.Branches(name)}
	if headID := s.PrimaryHead(name); headID != "" {
		if rec, err := s.LoadCommit(name, headID); err == nil {
			for _, f := range s.ResolveView(rec) {
				r.SizeBytes += f.Size()
			}
		}
	}
	return r, nil
}

func (s *Store) ListRepos() ([]client.Repo, error) {
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
			// not a user repository
			if r, err := s.InspectRepo(e.Name()); err == nil {
				out = append(out, r)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// primaryHead returns the id of the head commit of the primary branch.
func (s *Store) PrimaryHead(repo string) string {
	b, err := os.ReadFile(filepath.Join(s.RepoDir(repo), "default"))
	if err != nil {
		return ""
	}
	return s.HeadCommit(repo, strings.TrimSpace(string(b)))
}

func (s *Store) HeadCommit(repo, branch string) string {
	b, err := os.ReadFile(filepath.Join(s.RepoDir(repo), "refs", branch))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// setHead moves a branch ref, creating the refs dir and default marker on
// the first commit.
func (s *Store) SetHead(repo, branch, id string) error {
	refs := filepath.Join(s.RepoDir(repo), "refs")
	if err := os.MkdirAll(refs, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(refs, branch), []byte(id+"\n"), 0o644); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(s.RepoDir(repo), "default")); os.IsNotExist(err) {
		if err := os.WriteFile(filepath.Join(s.RepoDir(repo), "default"), []byte(branch+"\n"), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// validBranchName rejects names that would escape the refs dir or embed a
// path separator: a branch is exactly one ref file.
func validBranchName(b string) bool {
	return b != "" && b != "." && b != ".." && !strings.ContainsAny(b, "/\\")
}

// branchRefs lists the repo's branches with their head commit ids (one
// ref file per branch). A fresh repo with no finished commit has no refs
// dir and lists empty.
func (s *Store) BranchRefs(repo string) ([]client.Branch, error) {
	if !ValidName(repo) {
		return nil, fmt.Errorf("invalid repo name %q", repo)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := os.Stat(s.RepoDir(repo)); err != nil {
		return nil, notFound("repo %q not found", repo)
	}
	entries, err := os.ReadDir(filepath.Join(s.RepoDir(repo), "refs"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]client.Branch, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		id, err := os.ReadFile(filepath.Join(s.RepoDir(repo), "refs", e.Name()))
		if err != nil {
			continue
		}
		out = append(out, client.Branch{Repo: repo, Branch: e.Name(), Head: strings.TrimSpace(string(id))})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Branch < out[j].Branch })
	return out, nil
}

// branchHead reads one branch's head commit id.
func (s *Store) BranchHead(repo, branch string) (string, error) {
	if !ValidName(repo) {
		return "", fmt.Errorf("invalid repo name %q", repo)
	}
	if !validBranchName(branch) {
		return "", fmt.Errorf("invalid branch name %q", branch)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := os.Stat(s.RepoDir(repo)); err != nil {
		return "", notFound("repo %q not found", repo)
	}
	id, err := os.ReadFile(filepath.Join(s.RepoDir(repo), "refs", branch))
	if err != nil {
		return "", notFound("branch %q not found", branch)
	}
	return strings.TrimSpace(string(id)), nil
}

// deleteBranch removes the branch ref. The repo's default branch is
// protected: the default marker must keep pointing at an existing ref.
// The branch's commits stay addressable by id.
func (s *Store) DeleteBranch(repo, branch string) error {
	if !ValidName(repo) {
		return fmt.Errorf("invalid repo name %q", repo)
	}
	if !validBranchName(branch) {
		return fmt.Errorf("invalid branch name %q", branch)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := os.Stat(s.RepoDir(repo)); err != nil {
		return notFound("repo %q not found", repo)
	}
	if def, err := os.ReadFile(filepath.Join(s.RepoDir(repo), "default")); err == nil && strings.TrimSpace(string(def)) == branch {
		return fmt.Errorf("cannot delete the default branch %q", branch)
	}
	path := filepath.Join(s.RepoDir(repo), "refs", branch)
	if _, err := os.Stat(path); err != nil {
		return notFound("branch %q not found", branch)
	}
	return os.Remove(path)
}

// ---- commits ----

func newCommitID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// IsCommitID reports whether s has the commit-id shape (16 hex chars —
// newCommitID). It disambiguates a ref segment between a commit id and a
// branch name: a commit-id ref must address the commit, never
// materialize a phantom branch of that name (F14 — the CLI open-commit
// flow misplaced data by treating the started commit's id as a branch).
func IsCommitID(s string) bool {
	if len(s) != 16 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// startCommit opens a new revision on the branch. The repo is created if it
// does not exist (pipeline output repos are born this way). The parent is
// the branch's finished head at start time.
func (s *Store) StartCommit(repo, branch, description string) (client.Commit, error) {
	if !ValidName(repo) {
		return client.Commit{}, fmt.Errorf("invalid repo name %q", repo)
	}
	if branch == "" {
		branch = DefaultBranch
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(s.RepoDir(repo), 0o755); err != nil {
		return client.Commit{}, err
	}
	rec := &CommitRec{
		ID:          newCommitID(),
		Repo:        repo,
		Branch:      branch,
		Description: description,
		ParentID:    s.HeadCommit(repo, branch),
		Started:     true,
		CreatedAt:   Now(),
	}
	if err := s.SaveCommit(rec); err != nil {
		return client.Commit{}, err
	}
	return rec.Commit(), nil
}

// putFile writes one file into an open commit as an append: a
// path already holding content — in this commit or its ancestry — grows
// by the new bytes at this revision. Replacing content is an explicit
// overwrite(overwriteFile) or a delete-then-write.
func (s *Store) PutFile(commitID, p string, data []byte) error {
	if !validPath(p) {
		return fmt.Errorf("invalid file path %q", p)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.LoadCommitByID(commitID)
	if err != nil {
		return notFound("commit %q not found", commitID)
	}
	if !rec.Started || rec.Finished {
		return fmt.Errorf("commit %q is not open for writes", commitID)
	}
	sha, err := s.WriteBlob(data)
	if err != nil {
		return err
	}
	rec.Ops = append(rec.Ops, fileOp{Path: p, SHA: sha, Size: uint64(len(data))})
	return s.SaveCommit(rec)
}

// overwriteFile writes one file into an open commit replacing any
// accumulated content at the path: the path's prior content — in
// this commit or its ancestry — is removed and the new bytes become the
// entire content at this revision.
func (s *Store) OverwriteFile(commitID, p string, data []byte) error {
	if !validPath(p) {
		return fmt.Errorf("invalid file path %q", p)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.LoadCommitByID(commitID)
	if err != nil {
		return notFound("commit %q not found", commitID)
	}
	if !rec.Started || rec.Finished {
		return fmt.Errorf("commit %q is not open for writes", commitID)
	}
	sha, err := s.WriteBlob(data)
	if err != nil {
		return err
	}
	rec.Ops = append(rec.Ops, fileOp{Path: p, SHA: sha, Size: uint64(len(data)), Overwrite: true})
	return s.SaveCommit(rec)
}

// deleteFile tombstones a path in an open commit: the path is
// removed from the branch's view at this revision, whether its content
// came from this commit or the ancestry. Write order within the commit
// decides conflicts — a later write recreates the path with only its own
// content; a later delete removes it again.
func (s *Store) DeleteFile(commitID, p string) error {
	if !validPath(p) {
		return fmt.Errorf("invalid file path %q", p)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.LoadCommitByID(commitID)
	if err != nil {
		return notFound("commit %q not found", commitID)
	}
	if !rec.Started || rec.Finished {
		return fmt.Errorf("commit %q is not open for writes", commitID)
	}
	rec.Ops = append(rec.Ops, fileOp{Path: p, Delete: true})
	return s.SaveCommit(rec)
}

// tombstoneRemoved records in a commit the paths that vanished from its
// parent's view: the output side of a deletion. The output revision reflects
// input modifications — a replaced file carries the new content, an added
// file appears, and a deleted file is genuinely absent (tombstoned), never
// stale. Paths present in the parent's view but absent from this output are
// recorded as deletions, so reading the deleted path errors rather than
// returning old or empty content; one output commit is produced per input
// commit, including deletion-only commits.
func (s *Store) TombstoneRemoved(commitID, outDir string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.LoadCommitByID(commitID)
	if err != nil {
		return err
	}
	var parent map[string]ViewEntry
	if rec.ParentID != "" {
		if p, err := s.LoadCommit(rec.Repo, rec.ParentID); err == nil {
			parent = s.ResolveView(p)
		}
	}
	newPaths := map[string]bool{}
	_ = WalkFiles(outDir, nil, func(rel string, _ []byte) error {
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
	return s.SaveCommit(rec)
}

// walkFiles visits every file under dir with its content, following
// symlinks: a symlink to a file yields the target's content at the link's
// path, and a symlink to a directory yields the target's files under the
// link's path prefix (a pipeline may emit symlinked output). A
// depth cap breaks link cycles. linkTarget maps a symlink's target to a
// host path when the target is container-internal (e.g. /sandman/in/...);
// an empty result falls back to the native resolution.
func WalkFiles(dir string, linkTarget func(string) string, visit func(rel string, data []byte) error) error {
	return walkFilesDepth(dir, "", 0, linkTarget, visit)
}

// walkFilesDepth visits files under p with their content, following
// symlinks. Output collection must reject any non-regular file (pipe,
// socket, device) before uploading: reading a FIFO would block forever and
// its content is not storable, so the walk fails with a reason naming the
// special file rather than hanging or uploading garbage.
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
			// than hang or upload garbage
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
// thousands of files). Each path is written as an overwrite: job
// output assembly replaces a path's prior content with the job's fresh
// output (a a reprocessed datum's output replaces its prior output;
// unchanged datums' paths are absent from the job's output and carry
// forward through the ancestry). The directory holds the fully assembled
// output, so it must not accumulate over the parent's.
func (s *Store) AddFilesFromDir(commitID, dir string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.LoadCommitByID(commitID)
	if err != nil {
		return notFound("commit %q not found", commitID)
	}
	if !rec.Started || rec.Finished {
		return fmt.Errorf("commit %q is not open for writes", commitID)
	}
	var entries []fileOp
	walkErr := WalkFiles(dir, nil, func(rel string, data []byte) error {
		sha, err := s.WriteBlob(data)
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
	return s.SaveCommit(rec)
}

// copyFile copies a file or directory subtree from srcCommit into an open
// dstCommit at dstPath, landing each contained file at dstPath/<relative
// path>. An existing destination path is protected: without overwrite, a
// copy onto a path already in the destination's view (parents included) is
// rejected, while new paths are always writable; with overwrite the copy
// replaces the destination's accumulated content.
func (s *Store) CopyFile(dstCommitID, dstPath, srcCommitID, srcPath string, overwrite bool) error {
	if !validPath(dstPath) {
		return fmt.Errorf("invalid file path %q", dstPath)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dstRec, err := s.LoadCommitByID(dstCommitID)
	if err != nil {
		return notFound("commit %q not found", dstCommitID)
	}
	if !dstRec.Started || dstRec.Finished {
		return fmt.Errorf("commit %q is not open for writes", dstCommitID)
	}
	srcView, err := s.ResolveViewByID(srcCommitID)
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
			return notFound("path %q not found", srcPath)
		}
	}
	dstView := s.ResolveView(dstRec)
	for _, m := range moves {
		if _, exists := dstView[m.dst]; exists && !overwrite {
			return fmt.Errorf("path %q already exists in commit %q", m.dst, dstCommitID)
		}
	}
	for _, m := range moves {
		e := srcView[m.src]
		data, err := e.Bytes(s)
		if err != nil {
			return err
		}
		sha, err := s.WriteBlob(data) // dedupes: identical content, same object
		if err != nil {
			return err
		}
		op := fileOp{Path: m.dst, SHA: sha, Size: e.Size()}
		if overwrite {
			op.Overwrite = true
		}
		dstRec.Ops = append(dstRec.Ops, op)
	}
	return s.SaveCommit(dstRec)
}

// finishCommit closes the commit, advances the branch ref, and reports the
// final record. An explicitly empty commit (empty=true) is a real commit
// whose view is nothing — parent files are not readable through it.
// reparent retargets an open commit's parent to the current branch head.
// Output commits are opened at job start, before the head is final; the
// last writer must re-parent so the branch stays linear and no finished
// commit is orphaned off the chain (concurrent jobs of one pipeline).
func (s *Store) Reparent(commitID, parentID string) error {
	if parentID == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.LoadCommitByID(commitID)
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
	return s.SaveCommit(rec)
}

// finishCommit closes a commit as a real revision or as an explicit empty
// barrier, advancing the branch head. Finishing with the empty flag produces
// a real revision that introduces no file content of its own: paths that
// existed in its parent are not readable through it — the empty commit's
// resolved view is nothing, so reads at the empty head fail with not-found
// rather than falling through to the parent. A commit also carries an
// optional user description attachable at start and at finish: a description
// supplied at either lifecycle point is persisted, and when both are
// supplied the finish-time description deterministically replaces (never
// concatenates with) the start-time one; inspection returns the effective
// description.
func (s *Store) FinishCommit(commitID, description string, empty bool) (client.Commit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.LoadCommitByID(commitID)
	if err != nil {
		return client.Commit{}, notFound("commit %q not found", commitID)
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
	// a type conflict; finishing fails ("x" then "x/y" type conflict)
	if conflict := s.PathConflict(rec); conflict != "" {
		return client.Commit{}, fmt.Errorf("type conflict at path %q", conflict)
	}
	rec.Empty = empty
	rec.Finished = true
	if err := s.SaveCommit(rec); err != nil {
		return client.Commit{}, err
	}
	if err := s.SetHead(rec.Repo, rec.Branch, rec.ID); err != nil {
		return client.Commit{}, err
	}
	if s.onFinish != nil {
		s.onFinish()
	}
	return rec.Commit(), nil
}

func (s *Store) InspectCommit(commitID string) (client.Commit, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, err := s.LoadCommitByID(commitID)
	if err != nil {
		return client.Commit{}, notFound("commit %q not found", commitID)
	}
	return rec.Commit(), nil
}

func (s *Store) HeadCommitRec(repo, branch string) (client.Commit, error) {
	if !ValidName(repo) {
		return client.Commit{}, fmt.Errorf("invalid repo name %q", repo)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := os.Stat(s.RepoDir(repo)); err != nil {
		return client.Commit{}, notFound("repo %q not found", repo)
	}
	id := s.HeadCommit(repo, branch)
	if id == "" {
		return client.Commit{}, fmt.Errorf("branch %q has no head", branch)
	}
	rec, err := s.LoadCommit(repo, id)
	if err != nil {
		return client.Commit{}, err
	}
	return rec.Commit(), nil
}

// loadCommitByID finds a commit record across all repos by id.
func (s *Store) LoadCommitByID(commitID string) (*CommitRec, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if rec, err := s.LoadCommit(e.Name(), commitID); err == nil {
			return rec, nil
		}
	}
	return nil, notFound("commit %q not found", commitID)
}

func (rec *CommitRec) Commit() client.Commit {
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
// must fail finishing ("x" then "x/y" in one commit;
// a child writing "x/y" over an inherited file "x").
func (s *Store) PathConflict(rec *CommitRec) string {
	view := s.ResolveView(rec)
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

// resolveView resolves a commit's accumulated view: walking
// the ancestry oldest first, each commit's write operations replay in
// order — an append contributes its bytes to the path's accumulated
// content, an overwrite resets it, a delete removes the path (removing
// inherited content too). An explicitly empty commit is a barrier: its
// view is nothing and nothing below it merges in — a child of an empty
// commit shows only its own ops.
func (s *Store) ResolveView(rec *CommitRec) map[string]ViewEntry {
	var chain []*CommitRec
	for cur := rec; cur != nil && !cur.Empty; {
		chain = append(chain, cur)
		if cur.ParentID == "" {
			break
		}
		parent, err := s.LoadCommit(cur.Repo, cur.ParentID)
		if err != nil {
			break
		}
		cur = parent
	}
	view := map[string]ViewEntry{}
	for i := len(chain) - 1; i >= 0; i-- { // oldest first, so the newest ops apply last
		for _, op := range chain[i].Ops {
			switch {
			case op.Delete:
				delete(view, op.Path)
			case op.Overwrite:
				view[op.Path] = ViewEntry{parts: []ViewPart{{SHA: op.SHA, Size: op.Size, Overwrite: true}}}
			default:
				e := view[op.Path]
				e.parts = append(e.parts, ViewPart{SHA: op.SHA, Size: op.Size})
				view[op.Path] = e
			}
		}
	}
	return view
}

// resolveViewByID is resolveView for a commit id (must be called under the
// store lock).
func (s *Store) ResolveViewByID(commitID string) (map[string]ViewEntry, error) {
	rec, err := s.LoadCommitByID(commitID)
	if err != nil {
		return nil, err
	}
	return s.ResolveView(rec), nil
}

// ---- file access ----

// ListFiles returns the complete set of files in a commit's resolved view:
// a single commit may hold thousands of files, and every path written before
// finishing appears exactly once in the listing, with no loss or
// duplication. Each listed file remains readable with its own content.
func (s *Store) ListFiles(commitID string) ([]client.FileInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	view, err := s.ResolveViewByID(commitID)
	if err != nil {
		return nil, err
	}
	out := make([]client.FileInfo, 0, len(view))
	for p, f := range view {
		h, err := f.Hash(s)
		if err != nil {
			return nil, err
		}
		out = append(out, client.FileInfo{Path: p, Size: f.Size(), Hash: h})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

func (s *Store) GetFile(commitID, p string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	view, err := s.ResolveViewByID(commitID)
	if err != nil {
		return nil, err
	}
	f, ok := view[p]
	if !ok {
		return nil, notFound("file %q not found", p)
	}
	return f.Bytes(s)
}

// materializeInput writes the full view of a commit into dir, preserving
// relative paths — the job's view of its input.
func (s *Store) MaterializeInput(commitID, dir string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	view, err := s.ResolveViewByID(commitID)
	if err != nil {
		return err
	}
	return s.MaterializeView(view, dir)
}

// materializeView writes an already-resolved view into dir.
func (s *Store) MaterializeView(view map[string]ViewEntry, dir string) error {
	for p, f := range view {
		data, err := f.Bytes(s)
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

// DatumRef is one input file of a revision — its path and content hash —
// the per-file handle for log filters. A job's datum set is the
// input revision's full view.
type DatumRef struct {
	Path string `json:"path"`
	Hash string `json:"hash"`
}

// ViewDatums lists the input files of a commit with their content hashes —
// the datum set of a job processing that commit (log filters). The
// whole view is the datum set: a job runs over the full input revision.
func (s *Store) ViewDatums(commitID string) ([]DatumRef, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	view, err := s.ResolveViewByID(commitID)
	if err != nil {
		return nil, err
	}
	out := make([]DatumRef, 0, len(view))
	for p, f := range view {
		h, err := f.Hash(s)
		if err != nil {
			return nil, err
		}
		out = append(out, DatumRef{Path: p, Hash: h})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

// chainFromHead lists the commit ids of a branch from the head down to
// (excluding) stopAt, oldest first — the backlog a stopped pipeline must
// process on restart.
func (s *Store) ChainFromHead(repo, branch, stopAt string) []string {
	var chain []string
	id := s.HeadCommit(repo, branch)
	for id != "" && id != stopAt {
		chain = append(chain, id)
		rec, err := s.LoadCommit(repo, id)
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

// ---- tags ----
//
// Tags are durable global names bound to file content: <state>/tags/<name>
// holds the sha of the tagged blob, which lives in the content-addressed
// object store. Listing a tag yields its reference; retrieval resolves it
// byte-for-byte.

type tagInfo struct {
	Name string `json:"name"`
	Ref  string `json:"ref"`
}

func (s *Store) TagPath(name string) string {
	return filepath.Join(filepath.Dir(s.dir), "tags", name)
}

// PutTag binds a durable global name to file content: the content's
// reference is stored under the tag name, and getting the tag returns the
// exact bytes. Listing enumerates every stored tag, each with a non-empty
// object reference. Tagged objects survive garbage collection — a tag holds
// a reference to its blob, so collection never reclaims reachable tagged
// data.
func (s *Store) PutTag(name string, data []byte) error {
	if !ValidName(name) {
		return fmt.Errorf("invalid tag name %q", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sha, err := s.WriteBlob(data)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.TagPath(name)), 0o755); err != nil {
		return err
	}
	return os.WriteFile(s.TagPath(name), []byte(sha), 0o644)
}

func (s *Store) GetTag(name string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sha, err := os.ReadFile(s.TagPath(name))
	if err != nil {
		return nil, notFound("tag %q not found", name)
	}
	return s.ReadBlob(strings.TrimSpace(string(sha)))
}

// deleteTag removes the tag ref; the blob it pointed at becomes
// unreferenced and is reclaimed by the next GC (a tag holds a reference to
// its blob, so collection never reclaims reachable tagged data).
func (s *Store) DeleteTag(name string) error {
	if !ValidName(name) {
		return fmt.Errorf("invalid tag name %q", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.TagPath(name)
	if _, err := os.Stat(path); err != nil {
		return notFound("tag %q not found", name)
	}
	return os.Remove(path)
}

func (s *Store) ListTags() ([]tagInfo, error) {
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
