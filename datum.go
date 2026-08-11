// The datum engine: a job processes its input as a set of datums — units
// of work matched by the input glob — executed by a bounded pool of
// workers, each datum's output merged into the job's single output commit.
//
// Model
//
//   - A datum is one glob match: a file, or a directory and its whole
//     subtree. Its identity is the matched path; its content hash is a
//     digest of its files' content hashes.
//   - Dedup (SB-006/084/085, D-13): a datum whose content hash is
//     unchanged from a previous SUCCESSFUL run is skipped, unless the
//     pipeline's Reprocess flag forces every job to re-execute everything
//     (SB-166). A skipped datum's output is carried forward.
//   - Per-datum records live in a per-pipeline dedup table — the durable
//     memory of what was processed, against which hash, with what outcome
//     (success | recovered | failed | skipped).
//   - Parallelism bounds the worker pool (SB-004/103). Workers execute
//     datums independently; each writes into its own staging directory.
//   - The job's output commit merges every datum's contribution: a
//     processed datum's fresh files, a skipped datum's carried files.
//     Files at the same relative path from different datums concatenate
//     in datum order (SB-063: "each datum's output in sequence"; the
//     order itself is not contractual, D-14, but the merge is
//     deterministic). A failed datum contributes nothing — all-or-nothing
//     per datum, matching the job-level convention.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sandman/client"
)

// datum is one unit of work: one input-side contribution per declared
// input side (a single-input job has one side), the files each side reads,
// and a stable identity.
type datum struct {
	ID    string      // identity key (single input: the matched path; cross: the joined side keys)
	Sides []datumSide // in declaration order
	Hash  string      // content hash of all sides' files
}

// datumSide is one input side's contribution to a datum.
type datumSide struct {
	Name  string   // the input's environment variable name
	ID    string   // the matched path within this side's view
	Files []string // file paths in this side's view
	// Merge, when set, overrides Files' content for the listed paths: the
	// side's file at each path is the concatenation of the copies (a
	// union's branches merging by path, SB-077/078).
	Merge map[string][]fileRef
}

// unionDatums builds a union input's datum set (SB-077/078): each branch
// contributes its files at their namespaced paths (a plain repo at its
// file paths, a cross at memberName/file, a nested union under its name),
// and files at the same path merge by concatenation in branch order — one
// datum per distinct path. The merged content hash is computed from the
// copies, so dedup tracks the union's merged state.
func (d *daemon) unionDatums(views map[string]map[string]fileEntry, union *client.Input) []datum {
	merged := map[string][]fileRef{}
	var order []string
	seen := map[string]bool{}
	for _, branch := range union.Union {
		for path, refs := range d.unionBranchPaths(views, branch) {
			if !seen[path] {
				seen[path] = true
				order = append(order, path)
			}
			merged[path] = append(merged[path], refs...)
		}
	}
	var out []datum
	for _, path := range order {
		refs := merged[path]
		sum := sha256.New()
		for _, r := range refs {
			if b, err := d.store.readBlob(r.Hash); err == nil {
				sum.Write(b)
			}
		}
		out = append(out, datum{
			ID: path,
			Sides: []datumSide{{
				Name:  union.Name,
				ID:    path,
				Files: []string{path},
				Merge: map[string][]fileRef{path: refs},
			}},
			Hash: hex.EncodeToString(sum.Sum(nil)),
		})
	}
	return out
}

// unionBranchPaths exposes one union branch's files at their namespaced
// paths, one copy per contributing datum combination.
func (d *daemon) unionBranchPaths(views map[string]map[string]fileEntry, branch client.Input) map[string][]fileRef {
	out := map[string][]fileRef{}
	switch {
	case len(branch.Cross) > 0:
		// the cross's combinations: each combination contributes every
		// member's file at memberName/file, once per combination
		var memberFiles [][]string
		for _, m := range branch.Cross {
			var fs []string
			for p := range views[m.Name] {
				if globMatches(m.Glob, p) {
					fs = append(fs, p)
				}
			}
			sort.Strings(fs)
			memberFiles = append(memberFiles, fs)
		}
		// cartesian product of the members' file lists
		var combos [][]string
		combos = [][]string{{}}
		for _, fs := range memberFiles {
			var next [][]string
			for _, combo := range combos {
				for _, f := range fs {
					next = append(next, append(append([]string{}, combo...), f))
				}
			}
			combos = next
		}
		for _, combo := range combos {
			for j, f := range combo {
				m := branch.Cross[j]
				path := m.Name + "/" + f
				e := views[m.Name][f]
				out[path] = append(out[path], fileRef{Path: path, Hash: e.SHA, Size: e.Size})
			}
		}
	case len(branch.Union) > 0:
		// nested union: its merged files exposed under the branch's name
		for _, dt := range d.unionDatums(views, &branch) {
			for path, refs := range dt.Sides[0].Merge {
				ns := branch.Name + "/" + path
				out[ns] = append(out[ns], refs...)
			}
		}
	default:
		v, ok := views[unionBranchKey(branch)]
		if !ok {
			v = views[branch.Name]
		}
		for p := range v {
			if globMatches(branch.Glob, p) {
				e := v[p]
				out[p] = append(out[p], fileRef{Path: p, Hash: e.SHA, Size: e.Size})
			}
		}
	}
	return out
}

// unionBranchKey names the view key of one union branch: the branch's
// name (or repo) plus its branch, so two branches of one repo stay
// distinct in the views (SB-141).
func unionBranchKey(b client.Input) string {
	n := b.Name
	if n == "" {
		n = b.Repo
	}
	return n + "@" + client.InputBranch(b)
}

// datumState is the durable per-datum record (the dedup table).
type datumState struct {
	Hash        string    `json:"hash"`
	Outcome     string    `json:"outcome"` // success | recovered | failed | skipped
	InputFiles  []fileRef `json:"inputFiles,omitempty"`
	Files       []fileRef `json:"files,omitempty"` // output files (path → content hash)
	Tries       int       `json:"tries,omitempty"`
	Started     string    `json:"started,omitempty"`
	Finished    string    `json:"finished,omitempty"`
	ProcessTime float64   `json:"processTime,omitempty"` // seconds
	Worker      int       `json:"worker,omitempty"`
	Reason      string    `json:"reason,omitempty"`
}

// fileRef points at stored content: a relative path and its sha256.
type fileRef struct {
	Path string `json:"path"`
	Hash string `json:"hash"`
	Size uint64 `json:"size,omitempty"`
}

// appendLogLine writes one timestamped line to a job's log (the same
// format the log capture produces), used for engine-authored entries like
// per-attempt failure markers (SB-134).
func (d *daemon) appendLogLine(id, line string) {
	path := d.logPath(id)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	b, _ := json.Marshal(logLineRec{T: time.Now().UTC().Format(time.RFC3339Nano), Line: line})
	b = append(b, '\n')
	f.Write(b)
}

// globMatches reports whether a relative view path matches a pfs-style
// glob. Patterns are root-anchored ("/dirA/*" matches "dirA/file"); "**"
// matches across directories, "*" within one.
func globMatches(pattern, path string) bool {
	pattern = strings.TrimPrefix(pattern, "/")
	if path == "" {
		// the root (whole-commit) datum: selected only by the whole-commit
		// globs "/" (trimmed to "") and "**"; "/*" must not catch it
		// (filepath.Match("*", "") would)
		return pattern == "" || pattern == "**"
	}
	if pattern == "**" {
		return true
	}
	ok, err := filepath.Match(pattern, path)
	return err == nil && ok
}

// enumerateDatums resolves one side's datum set from its view: every path
// the glob matches is a datum — a directory match is a datum of its whole
// subtree, and files swallowed by a directory datum are not separate
// datums. Datums are ordered by id (D-14: execution order is not
// contractual, but the output merge must be deterministic).
func enumerateDatums(view map[string]fileEntry, glob string) []datumSide {
	// candidate paths: every file path and every ancestor directory; the
	// root "" is always a candidate, so glob "/" selects the whole commit
	// as one datum (SB-015)
	candSet := map[string]bool{"": true}
	for p := range view {
		dir := p
		for {
			candSet[dir] = true
			i := strings.LastIndexByte(dir, '/')
			if i < 0 {
				break
			}
			dir = dir[:i]
		}
	}
	cands := make([]string, 0, len(candSet))
	for c := range candSet {
		cands = append(cands, c)
	}
	sort.Strings(cands) // directories sort before their contents
	var out []datumSide
	shadowed := map[string]bool{} // files consumed by a directory datum
	for _, c := range cands {
		if shadowed[c] || !globMatches(glob, c) {
			continue
		}
		var files []string
		if _, isFile := view[c]; isFile {
			files = []string{c}
		} else {
			prefix := c
			if prefix != "" {
				prefix += "/"
			}
			for p := range view {
				if strings.HasPrefix(p, prefix) {
					files = append(files, p)
				}
			}
			sort.Strings(files)
			for _, f := range files {
				shadowed[f] = true
			}
		}
		out = append(out, datumSide{ID: c, Files: files})
	}
	return out
}

// crossDatums builds a job's datum set from its sides' datum lists: the
// cartesian product over the sides, one contribution per side (SB-063,
// SB-161). A side with no datums makes the product empty. A single side
// keeps the plain matched paths as datum ids; a cross datum's id joins the
// side keys so identity stays stable across jobs.
func crossDatums(sideLists [][]datumSide) []datum {
	if len(sideLists) == 1 {
		out := make([]datum, 0, len(sideLists[0]))
		for _, sd := range sideLists[0] {
			out = append(out, datum{ID: sd.ID, Sides: []datumSide{sd}})
		}
		return out
	}
	var out []datum
	var rec func(int, []datumSide)
	rec = func(i int, acc []datumSide) {
		if i == len(sideLists) {
			id := ""
			for _, sd := range acc {
				if id != "" {
					id += "+"
				}
				id += sd.Name + "=" + sd.ID
			}
			out = append(out, datum{ID: id, Sides: acc})
			return
		}
		for _, sd := range sideLists[i] {
			rec(i+1, append(append([]datumSide{}, acc...), sd))
		}
	}
	rec(0, nil)
	return out
}

// datumHash digests a datum's files across all sides: the sorted
// "side:path:hash" triples, so the hash changes exactly when the datum's
// content changes.
func datumHash(views map[string]map[string]fileEntry, dt datum) string {
	h := sha256.New()
	for _, sd := range dt.Sides {
		if sd.Merge != nil {
			// a union side combined into a cross/join datum: the merged
			// copies carry the content (the branches' views live under
			// branch keys, not the union's name, SB-141)
			paths := make([]string, 0, len(sd.Merge))
			for p := range sd.Merge {
				paths = append(paths, p)
			}
			sort.Strings(paths)
			for _, p := range paths {
				for _, r := range sd.Merge[p] {
					h.Write([]byte(sd.Name))
					h.Write([]byte{0})
					h.Write([]byte(p))
					h.Write([]byte{0})
					h.Write([]byte(r.Hash))
					h.Write([]byte{0})
				}
			}
			continue
		}
		for _, f := range sd.Files {
			h.Write([]byte(sd.Name))
			h.Write([]byte{0})
			h.Write([]byte(f))
			h.Write([]byte{0})
			h.Write([]byte(views[sd.Name][f].SHA))
			h.Write([]byte{0})
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ---- the dedup table ----

// dedupPath is the per-pipeline datum record file. It is the engine's
// durable memory: what each datum hash produced and with what outcome.
func (d *daemon) dedupPath(pipeline string) string {
	return filepath.Join(d.state, "dedup", pipeline+".json")
}

func (d *daemon) loadDedup(pipeline string) map[string]datumState {
	b, err := os.ReadFile(d.dedupPath(pipeline))
	if err != nil {
		return map[string]datumState{}
	}
	m := map[string]datumState{}
	if json.Unmarshal(b, &m) != nil {
		// a torn write loses dedup memory: the consequence is reprocessing,
		// never a wrong skip (fail open).
		return map[string]datumState{}
	}
	return m
}

func (d *daemon) saveDedup(pipeline string, m map[string]datumState) {
	b, err := json.Marshal(m)
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(d.dedupPath(pipeline)), 0o755); err != nil {
		return
	}
	// write to a temp name then rename: a reader never sees a torn file
	tmp := d.dedupPath(pipeline) + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil {
		os.Rename(tmp, d.dedupPath(pipeline))
	}
}

// ---- execution ----

// jobExec is the per-job execution context shared by the datum workers.
type jobExec struct {
	d      *daemon
	pl     pipelineRec
	id     string
	outDir string
	views  map[string]map[string]fileEntry // per input side: its view
	env    []string                        // job-scoped environment; the input dir vars are per datum
	rj     *runningJob

	// viewDirs are the sides' materialized full views, mounted read-only
	// into every container at /sandman/view/<name> alongside the datum's
	// own files: a datum may read data outside its own datum set (SB-166).
	viewMu   sync.Mutex
	viewDirs map[string]string

	// host is the execution host the job was placed on (SB-167): non-nil
	// only for a pipeline with a placement label, in which case every
	// datum attempt is shipped to that host's exec endpoint instead of
	// running on the control plane.
	host *execHost

	// tmpDir is the job's temp directory, mounted at the container's /tmp:
	// temp files a transform creates are host-readable, so symlinks to
	// them resolve in the output scan (SB-054).
	tmpMu   sync.Mutex
	tmpDir  string
	tmpOnce sync.Once

	// live worker status, persisted per event (SB-065/097)
	workersMu sync.Mutex
	workers   []workerStatus

	// restart requests (SB-064): a datum id requested to abort and re-run
	restartMu sync.Mutex
	restart   map[string]bool

	dedupMu sync.Mutex
	dedup   map[string]datumState
}

// requestRestart asks that a datum's current processing be aborted and
// restarted (SB-064).
func (jx *jobExec) requestRestart(datumID string) {
	jx.restartMu.Lock()
	defer jx.restartMu.Unlock()
	if jx.restart == nil {
		jx.restart = map[string]bool{}
	}
	jx.restart[datumID] = true
}

// restartRequested reports (and clears) a pending restart for the datum.
func (jx *jobExec) restartRequested(datumID string) bool {
	jx.restartMu.Lock()
	defer jx.restartMu.Unlock()
	if jx.restart[datumID] {
		delete(jx.restart, datumID)
		return true
	}
	return false
}

func (jx *jobExec) canceled() bool {
	return jx.rj.cancelled.Load()
}

// setDatum records one datum's state and persists the pipeline's dedup
// table: the datum API reads it live, so an in-flight job's records — the
// datum currently being processed included — are queryable (SB-114).
func (jx *jobExec) setDatum(id string, st datumState) {
	jx.dedupMu.Lock()
	jx.dedup[id] = st
	b, err := json.Marshal(jx.dedup)
	jx.dedupMu.Unlock()
	if err != nil {
		return
	}
	p := jx.d.dedupPath(jx.pl.Pipeline.Name)
	if os.MkdirAll(filepath.Dir(p), 0o755) == nil {
		tmp := p + ".tmp"
		if os.WriteFile(tmp, b, 0o644) == nil {
			os.Rename(tmp, p)
		}
	}
}

// registerContainer / unregisterContainer track a job's live containers on
// the running-job handle so a cancel can kill all of them (per-datum
// execution has many).
func (jx *jobExec) registerContainer(name string) {
	jx.rj.registerContainer(name)
}

func (jx *jobExec) unregisterContainer(name string) {
	jx.rj.unregisterContainer(name)
}

// runDatums executes every datum with a bounded worker pool and reports
// whether any datum ended failed. The pool size is the pipeline's
// parallelism constant (the autoscaling cap), capped at the datum count
// (SB-165: never more workers than datums). Each worker has a bounded
// queue of at most maxQueueSize (default 1) pending datums (SB-097); the
// coordinator feeds the queues round-robin, blocking on full ones.
func (d *daemon) runDatums(jx *jobExec, todo []datum) bool {
	workers := 1
	if jx.pl.Pipeline.Parallelism != nil && jx.pl.Pipeline.Parallelism.Constant > 0 {
		workers = jx.pl.Pipeline.Parallelism.Constant
	}
	if workers > len(todo) {
		workers = len(todo)
	}
	if workers < 1 {
		return false
	}
	bound := 1
	if jx.pl.Pipeline.MaxQueueSize > 0 {
		bound = jx.pl.Pipeline.MaxQueueSize
	}
	chans := make([]chan int, workers)
	jx.initWorkers(workers)
	for w := range chans {
		chans[w] = make(chan int, bound)
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range chans[w] {
				// a restart (SB-064) re-runs the datum in place: no
				// channel round-trip, so a re-queue can never hit a closed
				// worker channel
				for d.execDatum(jx, todo[i], w, i) {
				}
			}
		}(w)
	}
	for i := range todo {
		chans[i%workers] <- i
		jx.setQueue(i%workers, len(chans[i%workers]))
	}
	for _, ch := range chans {
		close(ch)
	}
	wg.Wait()
	for _, dt := range todo {
		if jx.dedup[dt.ID].Outcome == "failed" {
			return true
		}
	}
	return false
}

// workerStatus is one worker's live state, persisted at <jobDir>/workers.json
// so job inspection sees it mid-flight (SB-065/097).
type workerStatus struct {
	Worker  int    `json:"worker"`
	Datum   string `json:"datum,omitempty"`
	Started string `json:"started,omitempty"`
	Queue   int    `json:"queue"`
	Cname   string `json:"cname,omitempty"` // the active container, for restart
}

func (jx *jobExec) initWorkers(n int) {
	jx.workersMu.Lock()
	defer jx.workersMu.Unlock()
	jx.workers = make([]workerStatus, n)
	for i := range jx.workers {
		jx.workers[i].Worker = i
	}
	jx.saveWorkersLocked()
}

func (jx *jobExec) saveWorkers() {
	jx.workersMu.Lock()
	defer jx.workersMu.Unlock()
	jx.saveWorkersLocked()
}

func (jx *jobExec) saveWorkersLocked() {
	b, err := json.Marshal(jx.workers)
	if err != nil {
		return
	}
	p := filepath.Join(jx.d.jobDir(jx.id), "workers.json")
	// write to a temp name then rename: an inspection never reads a torn
	// file (the status is read live while the workers update it)
	tmp := p + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil {
		os.Rename(tmp, p)
	}
}

// setActive records the datum a worker is processing (or "" when it
// finished) with its start time and the active container.
func (jx *jobExec) setActive(worker int, datumID, started, cname string) {
	jx.workersMu.Lock()
	if worker < len(jx.workers) {
		jx.workers[worker].Datum = datumID
		jx.workers[worker].Started = started
		jx.workers[worker].Cname = cname
	}
	jx.saveWorkersLocked()
	jx.workersMu.Unlock()
}

func (jx *jobExec) setQueue(worker, queue int) {
	jx.workersMu.Lock()
	if worker < len(jx.workers) {
		jx.workers[worker].Queue = queue
	}
	jx.saveWorkersLocked()
	jx.workersMu.Unlock()
}

// execDatum processes one datum: up to DatumTries attempts of the primary
// command, each logged to the job log (SB-134), then the record is
// finalized. A cancelled job stops starting attempts; a restart request
// (SB-064) aborts the datum mid-flight and returns true so the worker
// re-queues it.
func (d *daemon) execDatum(jx *jobExec, dt datum, worker, index int) (requeue bool) {
	tr := jx.pl.Pipeline.Transform
	tries := tr.DatumTries
	if tries < 1 {
		tries = 1
	}
	started := time.Now().UTC()
	var inputFiles []fileRef
	for _, sd := range dt.Sides {
		for _, f := range sd.Files {
			inputFiles = append(inputFiles, fileRef{Path: f, Hash: jx.views[sd.Name][f].SHA, Size: jx.views[sd.Name][f].Size})
		}
	}
	rec := datumState{
		Hash:       dt.Hash,
		InputFiles: inputFiles,
		Started:    started.Format(time.RFC3339Nano),
		Worker:     worker,
	}
	jx.setDatum(dt.ID, rec) // live record: the datum is in progress
	var lastReason string
	attempt := 1
	for {
		if jx.canceled() {
			rec.Outcome = "failed"
			rec.Reason = "job cancelled"
			rec.Tries = attempt - 1
			rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
			rec.ProcessTime = time.Since(started).Seconds()
			jx.setDatum(dt.ID, rec)
			return false
		}
		if jx.restartRequested(dt.ID) {
			// the datum's processing was aborted: re-run it from scratch
			// with fresh progress (SB-064) — checked even after the last
			// attempt, since the abort lands mid-attempt
			jx.setDatum(dt.ID, datumState{Hash: dt.Hash, InputFiles: inputFiles})
			return true
		}
		if attempt > tries {
			break // all tries exhausted: finalize as failed below
		}
		outcome, reason, files := d.runDatumAttempt(jx, dt, index, attempt, started, worker)
		if outcome == "success" || outcome == "recovered" {
			// the datum's output files are already content-addressed blobs
			// (storeOutput): the record's references stay readable for
			// carry-forward and the output merge
			rec.Outcome = outcome
			rec.Tries = attempt
			rec.Files = files
			rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
			rec.ProcessTime = time.Since(started).Seconds()
			jx.setDatum(dt.ID, rec)
			return false
		}
		lastReason = reason
		d.appendLogLine(jx.id, fmt.Sprintf("datum %s: errored running user code after %d attempt(s)", dt.ID, attempt))
		attempt++
	}
	rec.Outcome = "failed"
	rec.Tries = tries
	rec.Reason = lastReason
	rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
	rec.ProcessTime = time.Since(started).Seconds()
	jx.setDatum(dt.ID, rec)
	// a provisioning failure (the image cannot be obtained at all) is an
	// environment problem, not a user-code failure: the pipeline enters
	// the crashed state (SB-043, SB-091).
	if isProvisioningError(lastReason) {
		d.markPipelineCrashed(jx.pl.Pipeline.Name, "datum "+dt.ID+": "+strings.TrimSpace(lastReason))
	}
	return false
}

// runDatumAttempt materializes one datum's per-side input files and runs
// one attempt of the transform: the primary command, and — when it fails —
// the error-handling command (SB-012), which may recover the datum. A
// datum that exceeds its per-datum timeout is killed at the boundary
// (SB-113). Returns the outcome, a diagnostic reason for failures, and the
// produced files (nil for a failed attempt — its partial output is
// discarded).
func (d *daemon) runDatumAttempt(jx *jobExec, dt datum, index, attempt int, started time.Time, worker int) (outcome, reason string, files []fileRef) {
	// A placed job (SB-167) executes each attempt on its execution host:
	// the datum's files and the transform are shipped there, the host
	// runs the container and returns the produced files, and the control
	// plane stores them into the output commit exactly as a local run's
	// would be.
	if jx.host != nil {
		return d.runRemoteAttempt(jx, dt, index, attempt, started, worker)
	}
	// per-attempt staging, keyed by the datum's index so concurrent and
	// repeated datums never share a directory
	dir := filepath.Join(d.jobDir(jx.id), "datum", fmt.Sprintf("%d-%d", index, attempt))
	outDir := filepath.Join(dir, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "failed", err.Error(), nil
	}
	// materialize each side's files into its own input directory
	var mounts []string
	env := append([]string{}, jx.env...)
	jx.tmpOnce.Do(func() {
		jx.tmpDir = filepath.Join(d.jobDir(jx.id), "tmp")
		os.MkdirAll(jx.tmpDir, 0o755)
	})
	if jx.tmpDir != "" {
		mounts = append(mounts, "-v", jx.tmpDir+":/tmp")
	}
	for _, sd := range dt.Sides {
		inDir := filepath.Join(dir, "in", sd.Name)
		if err := os.MkdirAll(inDir, 0o755); err != nil {
			return "failed", err.Error(), nil
		}
		for _, f := range sd.Files {
			data, err := d.sideFileData(jx, sd, f)
			if err != nil {
				return "failed", "materialize input: " + err.Error(), nil
			}
			dst := filepath.Join(inDir, filepath.FromSlash(f))
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return "failed", err.Error(), nil
			}
			if err := os.WriteFile(dst, data, 0o644); err != nil {
				return "failed", err.Error(), nil
			}
		}
		env = append(env, sd.Name+"=/sandman/in/"+sd.Name)
		mounts = append(mounts, "-v", inDir+":/sandman/in/"+sd.Name+":ro")
		// the full side view is available to containers: a datum can read
		// data outside its own datum set (SB-166)
		if vd := d.ensureView(jx, sd.Name); vd != "" {
			mounts = append(mounts, "-v", vd+":/sandman/view/"+sd.Name+":ro")
		}
	}

	// the container is registered so a cancel can kill it mid-flight; the
	// worker status exposes the datum in progress (SB-064/065/097)
	cname := fmt.Sprintf("sandman-%s-%d-%d", jx.id, index, attempt)
	jx.registerContainer(cname)
	defer jx.unregisterContainer(cname)
	jx.setActive(worker, dt.ID, started.Format(time.RFC3339Nano), cname)
	defer jx.setActive(worker, "", "", "")

	// symlinks the transform creates point at the container-internal input
	// paths (/sandman/in/<name>/...) or at temp files it wrote (the job's
	// /tmp mount); the host-side scan resolves both to host paths (SB-054)
	sideDirs := map[string]string{}
	for _, sd := range dt.Sides {
		sideDirs[sd.Name] = filepath.Join(dir, "in", sd.Name)
	}
	link := func(target string) string {
		for _, prefix := range []string{"/sandman/in/", "/sandman/view/"} {
			if strings.HasPrefix(target, prefix) {
				rest := strings.TrimPrefix(target, prefix)
				name, file, hasFile := strings.Cut(rest, "/")
				var base string
				if b, ok := sideDirs[name]; ok {
					base = b
				} else {
					jx.viewMu.Lock()
					base = jx.viewDirs[name]
					jx.viewMu.Unlock()
				}
				if base == "" {
					return ""
				}
				if !hasFile {
					return base // the whole side (a directory symlink)
				}
				return filepath.Join(base, filepath.FromSlash(file))
			}
		}
		if strings.HasPrefix(target, "/tmp/") {
			return filepath.Join(jx.tmpDir, filepath.FromSlash(strings.TrimPrefix(target, "/tmp/")))
		}
		return ""
	}

	capture, capErr := newLogCapture(d.logPath(jx.id))
	if capErr != nil {
		capture = nil
	}
	tr := jx.pl.Pipeline.Transform
	var timedOut atomic.Bool
	if tr.DatumTimeout != "" {
		if dur, err := time.ParseDuration(tr.DatumTimeout); err == nil {
			time.AfterFunc(dur, func() {
				timedOut.Store(true)
				exec.Command("docker", "kill", cname).Run()
			})
		}
	}

	run := func(cname string, argv, stdin []string) (int, string) {
		if len(argv) == 0 && len(stdin) == 0 {
			// default entry point: copy every side's files to OUT
			code := 0
			for _, sd := range dt.Sides {
				inDir := filepath.Join(dir, "in", sd.Name)
				if c := copyDir(inDir, outDir); c != 0 {
					code = c
				}
			}
			return code, ""
		}
		return runDatumContainer(jx.pl.Pipeline.Transform, d.name, cname, env, mounts, outDir, capture, argv, stdin)
	}

	var code int
	var tail string
	if len(tr.Cmd) == 0 && len(tr.Stdin) == 0 {
		code, tail = run(cname, nil, nil) // default entry point: copy every side
	} else {
		code, tail = run(cname, tr.Cmd, tr.Stdin)
	}
	if capture != nil {
		capture.Close()
	}
	accepted := tr.AcceptReturnCode != 0 && code == tr.AcceptReturnCode
	if code == 0 || accepted {
		files, err := d.storeOutput(outDir, link)
		if err != nil {
			return "failed", "scan output: " + err.Error(), nil
		}
		return "success", "", files
	}

	// primary failed: the error-handling command runs in the same output
	// directory and may recover the datum (SB-012)
	if len(tr.ErrCmd) > 0 || len(tr.ErrStdin) > 0 {
		ecname := cname + "-err"
		jx.registerContainer(ecname)
		ecode, etail := runDatumContainer(jx.pl.Pipeline.Transform, d.name, ecname, env, mounts, outDir, capture, tr.ErrCmd, tr.ErrStdin)
		jx.unregisterContainer(ecname)
		if ecode == 0 {
			files, err := d.storeOutput(outDir, link)
			if err != nil {
				return "failed", "scan output: " + err.Error(), nil
			}
			return "recovered", "", files
		}
		tail += etail
	}

	reason = fmt.Sprintf("exited with status %d", code)
	if timedOut.Load() {
		reason = fmt.Sprintf("exceeded the %s datum timeout", tr.DatumTimeout)
	}
	if tail != "" {
		if r := strings.TrimSpace(tail); len(r) > 2000 {
			r = r[len(r)-2000:]
		}
		reason += ": " + strings.TrimSpace(tail)
	}
	return "failed", reason, nil
}

// sideFileData reads one side file's content: a union side's file is the
// concatenation of its branch copies (SB-077/078), any other file is its
// single content blob. Shared by the local materialization and the
// remote-execution shipping (SB-167).
func (d *daemon) sideFileData(jx *jobExec, sd datumSide, f string) ([]byte, error) {
	if refs, ok := sd.Merge[f]; ok {
		var buf []byte
		for _, r := range refs {
			b, err := d.store.readBlob(r.Hash)
			if err != nil {
				return nil, err
			}
			buf = append(buf, b...)
		}
		return buf, nil
	}
	return d.store.readBlob(jx.views[sd.Name][f].SHA)
}

// runRemoteAttempt executes one datum attempt on the job's execution host
// (SB-167): the datum's per-side files and the transform are shipped to
// the host, which materializes them, runs the container (primary, then
// the error-handling command on failure, SB-012), and returns the
// produced files. The control plane stores the returned files as blobs —
// the output commit is assembled exactly as for a locally executed datum,
// so the job's result is indistinguishable by content.
func (d *daemon) runRemoteAttempt(jx *jobExec, dt datum, index, attempt int, started time.Time, worker int) (outcome, reason string, files []fileRef) {
	tr := jx.pl.Pipeline.Transform
	req := execRequest{
		JobID:            jx.id,
		Index:            index,
		Attempt:          attempt,
		Cname:            fmt.Sprintf("sandman-%s-%d-%d", jx.id, index, attempt),
		Image:            tr.Image,
		Cmd:              tr.Cmd,
		Stdin:            tr.Stdin,
		ErrCmd:           tr.ErrCmd,
		ErrStdin:         tr.ErrStdin,
		Env:              append([]string{}, jx.env...),
		DatumTimeout:     tr.DatumTimeout,
		AcceptReturnCode: tr.AcceptReturnCode,
		User:             tr.User,
		Workdir:          tr.Workdir,
	}
	if tr.ResourceLimits != nil {
		req.Memory = tr.ResourceLimits.Memory
		req.CPU = tr.ResourceLimits.CPU
	}
	if tr.ResourceRequests != nil {
		req.MemoryReservation = tr.ResourceRequests.Memory
		if tr.ResourceRequests.CPU > 0 && req.CPU == 0 {
			// docker expresses a CPU request only as an allocation; a
			// request without a limit sets it (SB-068, sandbox deviation)
			req.CPU = tr.ResourceRequests.CPU
		}
	}
	for _, sd := range dt.Sides {
		side := execSide{Name: sd.Name}
		for _, f := range sd.Files {
			data, err := d.sideFileData(jx, sd, f)
			if err != nil {
				return "failed", "materialize input: " + err.Error(), nil
			}
			side.Files = append(side.Files, shipFile{Path: f, Data: data})
		}
		req.Sides = append(req.Sides, side)
		req.Env = append(req.Env, sd.Name+"=/sandman/in/"+sd.Name)
	}

	code, errCode, tail, timedOut, outputs, err := d.execOnHost(jx.host, req)
	if err != nil {
		// the host is unreachable or the attempt could not be produced:
		// an environment problem, not a user-code failure — the pipeline
		// surfaces the crash (SB-091, and SB-169's visible-failure state)
		d.markPipelineCrashed(jx.pl.Pipeline.Name, "execution host "+jx.host.Name+": "+err.Error())
		return "failed", "execution host: " + err.Error(), nil
	}
	accepted := tr.AcceptReturnCode != 0 && code == tr.AcceptReturnCode
	if code == 0 || accepted || errCode == 0 {
		for _, f := range outputs {
			sum := sha256.Sum256(f.Data)
			d.store.writeBlob(f.Data)
			files = append(files, fileRef{Path: f.Path, Hash: hex.EncodeToString(sum[:]), Size: uint64(len(f.Data))})
		}
		sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
		if code != 0 && errCode == 0 {
			return "recovered", "", files
		}
		return "success", "", files
	}
	// the worker's container output is journaled into the job's log store
	// like a local run's capture would be
	if tail != "" {
		for _, ln := range strings.Split(strings.TrimRight(tail, "\n"), "\n") {
			d.appendLogLine(jx.id, ln)
		}
	}
	reason = fmt.Sprintf("exited with status %d", code)
	if timedOut {
		reason = fmt.Sprintf("exceeded the %s datum timeout", tr.DatumTimeout)
	}
	if tail != "" {
		reason += ": " + strings.TrimSpace(tail)
	}
	return "failed", reason, nil
}

// ensureView materializes an input side's full view once into the job's
// view directory (mounted into containers at /sandman/view/<name>).
func (d *daemon) ensureView(jx *jobExec, side string) string {
	jx.viewMu.Lock()
	defer jx.viewMu.Unlock()
	if dir, ok := jx.viewDirs[side]; ok {
		return dir
	}
	dir := filepath.Join(d.jobDir(jx.id), "view", side)
	if d.store.materializeView(jx.views[side], dir) != nil {
		return ""
	}
	jx.viewDirs[side] = dir
	return dir
}

// ---- join and group inputs (SB-074/075/076) ----

// globRegex converts a glob pattern with capture groups into an anchored
// regexp: ? matches one character, * any run, (...) is a capture group,
// and literal dots are escaped. The captured groups feed join-on and
// group-by selectors.
func globRegex(glob string) *regexp.Regexp {
	p := strings.TrimPrefix(glob, "/")
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(p); i++ {
		switch p[i] {
		case '(':
			b.WriteString("(")
		case ')':
			b.WriteString(")")
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteString(".")
		case '.':
			b.WriteString(`\.`)
		default:
			b.WriteByte(p[i])
		}
	}
	b.WriteString("$")
	return regexp.MustCompile(b.String())
}

// captureKey extracts a path's join/group key: the glob's captured groups
// selected by the selector ("$1", "$1$3", ...), concatenated in selector
// order. ok is false when the path does not match the glob or the
// selector names a missing group.
func captureKey(glob, selector, path string) (string, bool) {
	m := globRegex(glob).FindStringSubmatch(path)
	if m == nil {
		return "", false
	}
	var b strings.Builder
	for _, tok := range strings.Split(selector, "$") {
		if tok == "" {
			continue
		}
		n, err := strconv.Atoi(tok)
		if err != nil || n <= 0 || n >= len(m) {
			return "", false
		}
		b.WriteString(m[n])
	}
	return b.String(), true
}

// joinDatums builds a join input's datum set (SB-074/075): each member's
// files are bucketed by their join key; a key present in every member
// yields the cross product of the members' file lists for that key; an
// outer member's unmatched keys each form a datum carrying only that
// member's file (the absent members' directories are not exposed).
func joinDatums(views map[string]map[string]fileEntry, members []client.Input) []datum {
	buckets := make([]map[string][]datumSide, len(members))
	keySet := map[string]bool{}
	for i, m := range members {
		buckets[i] = map[string][]datumSide{}
		for p := range views[m.Name] {
			if key, ok := captureKey(m.Glob, m.JoinOn, p); ok {
				buckets[i][key] = append(buckets[i][key], datumSide{ID: p, Files: []string{p}, Name: m.Name})
				keySet[key] = true
			}
		}
	}
	// inner keys: present in every member
	inner := map[string]bool{}
	for k := range keySet {
		all := true
		for _, b := range buckets {
			if len(b[k]) == 0 {
				all = false
				break
			}
		}
		if all {
			inner[k] = true
		}
	}
	var out []datum
	for _, k := range sortedKeys(inner) {
		out = append(out, joinProduct(k, buckets)...)
	}
	// outer members' unmatched keys each form their own datum
	for i, m := range members {
		if !m.Outer {
			continue
		}
		for k, files := range buckets[i] {
			if inner[k] {
				continue
			}
			for _, sd := range files {
				id := sd.Name + "=" + sd.ID
				out = append(out, datum{ID: id, Sides: []datumSide{sd}})
			}
		}
	}
	return out
}

// joinProduct is the cross product of the members' files for one key, one
// datum per combination.
func joinProduct(key string, buckets []map[string][]datumSide) []datum {
	var out []datum
	var rec func(int, []datumSide)
	rec = func(i int, acc []datumSide) {
		if i == len(buckets) {
			id := ""
			for _, sd := range acc {
				if id != "" {
					id += "+"
				}
				id += sd.Name + "=" + sd.ID
			}
			out = append(out, datum{ID: id, Sides: acc})
			return
		}
		for _, sd := range buckets[i][key] {
			rec(i+1, append(append([]datumSide{}, acc...), sd))
		}
	}
	rec(0, nil)
	return out
}

// groupDatums builds a group input's datum set (SB-076): files across all
// members are collected by their group-by capture value — a key present in
// any member forms a group containing every file with that key from every
// member (union, never a cross product). A member with a join-on joins
// first: its files pair with the other join members by their join keys,
// and the whole pairs are then grouped.
func groupDatums(views map[string]map[string]fileEntry, members []client.Input) []datum {
	// group of join: pair first, then bucket the whole pairs
	joined := false
	for _, m := range members {
		if m.JoinOn != "" {
			joined = true
			break
		}
	}
	if joined {
		units := joinUnits(views, members)
		return groupUnits(units, members)
	}
	// plain group: bucket each member's files by its group key
	groups := map[string][]datumSide{}
	for _, m := range members {
		for p := range views[m.Name] {
			if key, ok := captureKey(m.Glob, m.GroupBy, p); ok {
				groups[key] = append(groups[key], datumSide{ID: p, Files: []string{p}, Name: m.Name})
			}
		}
	}
	var out []datum
	for _, k := range sortedSideKeys(groups) {
		sides := groups[k]
		id := ""
		for _, sd := range sides {
			if id != "" {
				id += "+"
			}
			id += sd.Name + "=" + sd.ID
		}
		out = append(out, datum{ID: id, Sides: sides})
	}
	return out
}

// joinUnits pairs the join members by their join keys (inner semantics,
// cross product within a key), returning one unit per pair — the unit is
// the members' files of that pair.
func joinUnits(views map[string]map[string]fileEntry, members []client.Input) [][]datumSide {
	buckets := make([]map[string][]datumSide, len(members))
	keySet := map[string]bool{}
	for i, m := range members {
		buckets[i] = map[string][]datumSide{}
		for p := range views[m.Name] {
			if key, ok := captureKey(m.Glob, m.JoinOn, p); ok {
				buckets[i][key] = append(buckets[i][key], datumSide{ID: p, Files: []string{p}, Name: m.Name})
				keySet[key] = true
			}
		}
	}
	var units [][]datumSide
	for k := range keySet {
		all := true
		for _, b := range buckets {
			if len(b[k]) == 0 {
				all = false
				break
			}
		}
		if all {
			for _, d := range joinProduct(k, buckets) {
				units = append(units, d.Sides)
			}
		}
	}
	return units
}

// groupUnits buckets joined units by their group-by values; a unit's key
// is its members' (agreeing) captured group values.
func groupUnits(units [][]datumSide, members []client.Input) []datum {
	groups := map[string][]datumSide{}
	order := []string{}
	for _, unit := range units {
		key := ""
		ok := true
		for i, sd := range unit {
			k, matched := captureKey(members[i].Glob, members[i].GroupBy, sd.ID)
			if !matched {
				ok = false
				break
			}
			if i == 0 {
				key = k
			} else if k != key {
				ok = false // the pair's sides disagree: split
				break
			}
		}
		if !ok {
			continue
		}
		if _, seen := groups[key]; !seen {
			order = append(order, key)
		}
		groups[key] = append(groups[key], unit...)
	}
	var out []datum
	for _, k := range order {
		sides := groups[k]
		id := ""
		for _, sd := range sides {
			if id != "" {
				id += "+"
			}
			id += sd.Name + "=" + sd.ID
		}
		out = append(out, datum{ID: id, Sides: sides})
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func sortedSideKeys(m map[string][]datumSide) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// storeOutput lists a staging directory's files with their content hashes
// and writes each file's content into the object store, so the returned
// references are readable by hash. Symlinks are followed: a link to a file
// stores the target's content, a link to a directory its files; linkTarget
// maps container-internal symlink targets to host paths (SB-054).
func (d *daemon) storeOutput(dir string, linkTarget func(string) string) ([]fileRef, error) {
	var out []fileRef
	walkErr := walkFiles(dir, linkTarget, func(rel string, data []byte) error {
		sum := sha256.Sum256(data)
		d.store.writeBlob(data)
		out = append(out, fileRef{Path: rel, Hash: hex.EncodeToString(sum[:]), Size: uint64(len(data))})
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, walkErr
}

// mergeOutputs writes the job's output commit content from every datum's
// contribution: processed datums contribute their fresh files, skipped
// datums their carried files, failed datums nothing. A file path produced
// by several datums concatenates their contents in datum order (SB-063).
func (d *daemon) mergeOutputs(jx *jobExec, datums []datum) error {
	for _, dt := range datums {
		st := jx.dedup[dt.ID]
		switch st.Outcome {
		case "success", "recovered", "skipped":
		default:
			continue // failed datums contribute nothing
		}
		for _, f := range st.Files {
			data, err := d.store.readBlob(f.Hash)
			if err != nil {
				return err
			}
			dst := filepath.Join(jx.outDir, filepath.FromSlash(f.Path))
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return err
			}
			if cur, err := os.ReadFile(dst); err == nil {
				if err := os.WriteFile(dst, append(cur, data...), 0o644); err != nil {
					return err
				}
			} else {
				if err := os.WriteFile(dst, data, 0o644); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// mergeSides combines same-name side datums into one (a chunk).
func mergeSides(sides []datumSide) datumSide {
	var out datumSide
	for _, s := range sides {
		out.Name = s.Name
		if out.ID != "" {
			out.ID += ","
		}
		out.ID += s.ID
		out.Files = append(out.Files, s.Files...)
	}
	return out
}

// sideSize sums a side datum's file sizes.
func sideSize(s datumSide, view map[string]fileEntry) uint64 {
	var n uint64
	for _, f := range s.Files {
		n += view[f].Size
	}
	return n
}

// chunkSideDatums groups a side's datums into chunks (SB-102): a target
// datum count (number) or a target chunk size in bytes. Files are never
// split; the grouped datum's identity joins its members', so dedup keys
// stay stable across jobs with the same chunking.
func chunkSideDatums(sd []datumSide, spec *client.ChunkSpec, view map[string]fileEntry) []datumSide {
	if spec == nil || (spec.Number <= 0 && spec.SizeBytes <= 0) {
		return sd
	}
	if spec.Number > 0 {
		per := (len(sd) + spec.Number - 1) / spec.Number
		if per < 1 {
			per = 1
		}
		var out []datumSide
		for i := 0; i < len(sd); i += per {
			end := i + per
			if end > len(sd) {
				end = len(sd)
			}
			out = append(out, mergeSides(sd[i:end]))
		}
		return out
	}
	var out []datumSide
	var cur []datumSide
	var curSize uint64
	for _, s := range sd {
		size := sideSize(s, view)
		if len(cur) > 0 && curSize+size > uint64(spec.SizeBytes) {
			out = append(out, mergeSides(cur))
			cur = nil
			curSize = 0
		}
		cur = append(cur, s)
		curSize += size
	}
	if len(cur) > 0 {
		out = append(out, mergeSides(cur))
	}
	return out
}

// ---- the datum API (per-datum statistics, SB-080/081/082/083/084) ----

// writeStatsCommit publishes a job's per-datum records as a commit on the
// output repo's "stats" branch: one file per datum, named by its index,
// containing the record. The branch is an ordinary branch — downstream
// pipelines can consume it (SB-086, SB-113).
func (d *daemon) writeStatsCommit(pl pipelineRec, dedup map[string]datumState, datums []datum) string {
	m := d.repoLock(pl.Pipeline.Name)
	m.Lock()
	defer m.Unlock()
	cm, err := d.store.startCommit(pl.Pipeline.Name, "stats", "")
	if err != nil {
		return ""
	}
	for i, dt := range datums {
		b, err := json.Marshal(dedup[dt.ID])
		if err != nil {
			continue
		}
		if err := d.store.putFile(cm.ID, fmt.Sprintf("%06d", i), b); err != nil {
			d.store.finishCommit(cm.ID, "", true)
			return ""
		}
	}
	fin, err := d.store.finishCommit(cm.ID, "", false)
	if err != nil {
		return ""
	}
	return fin.ID
}

// restartDatum aborts a datum's current processing and starts it over
// (SB-064): the running container is killed, the datum's record is reset,
// and the worker re-queues it, so the next status observation shows it
// running with a fresh, later start time.
func (d *daemon) restartDatum(jobID, datumID string) error {
	v, ok := d.liveJobs.Load(jobID)
	if !ok {
		return fmt.Errorf("job %q is not running", jobID)
	}
	jx := v.(*jobExec)
	var cname string
	jx.workersMu.Lock()
	for _, ws := range jx.workers {
		if ws.Datum == datumID {
			cname = ws.Cname
			break
		}
	}
	jx.workersMu.Unlock()
	if cname == "" {
		return fmt.Errorf("datum %q is not currently being processed", datumID)
	}
	jx.requestRestart(datumID)
	// the container may still be starting (the record is written on
	// pick-up, before docker run creates it): retry the kill until it
	// lands (SB-064)
	for i := 0; i < 50; i++ { // ~10s
		if exec.Command("docker", "kill", cname).Run() == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("datum %q container %s did not terminate", datumID, cname)
}

// statsEnabled reports whether the pipeline currently records per-datum
// statistics (the one-way flag, SB-081).
func (d *daemon) statsEnabled(pipeline string) bool {
	rec, err := d.loadPipeline(pipeline)
	return err == nil && rec.Pipeline.EnableStats
}

// datumStateRank orders the listing: failed first, skipped last
// (SB-082: the failed datum leads; SB-084: processed before skipped).
func datumStateRank(outcome string) int {
	switch outcome {
	case "failed":
		return 0
	case "recovered":
		return 1
	case "success":
		return 2
	default: // skipped, or an in-flight datum
		return 3
	}
}

func datumInfo(id string, st datumState) client.DatumInfo {
	info := client.DatumInfo{ID: id}
	state := st.Outcome
	if state == "" {
		state = "running" // picked up, not yet settled
	}
	info.State = state
	for _, f := range st.InputFiles {
		info.InputFiles = append(info.InputFiles, client.DatumFile{Path: f.Path, Hash: f.Hash})
	}
	for _, f := range st.Files {
		info.OutputFiles = append(info.OutputFiles, client.DatumFile{Path: f.Path, Hash: f.Hash})
	}
	info.ProcessTime = st.ProcessTime
	info.Started = st.Started
	info.Finished = st.Finished
	info.Worker = st.Worker
	info.Reason = st.Reason
	return info
}

// listDatums serves a job's datum listing: the job's datum set with each
// datum's record, state-ordered (failed < recovered < success < skipped,
// then id) and paginated. A page index at or beyond the page count errors
// (SB-083); limit 0 requests everything (SB-161). Without statistics the
// datums are listable by identity only (SB-081).
func (d *daemon) listDatums(jobID string, limit, page int) (client.DatumPage, error) {
	rec, err := d.loadJobRec(jobID)
	if err != nil {
		return client.DatumPage{}, err
	}
	dedup := d.loadDedup(rec.Pipeline)
	detailed := d.statsEnabled(rec.Pipeline)

	datums := append([]string{}, rec.DatumIDs...)
	stateOf := func(id string) string {
		if s, ok := rec.DatumStates[id]; ok {
			return s
		}
		return dedup[id].Outcome
	}
	rank := map[string]int{}
	for _, id := range datums {
		rank[id] = datumStateRank(stateOf(id))
	}
	sort.SliceStable(datums, func(i, j int) bool {
		if rank[datums[i]] != rank[datums[j]] {
			return rank[datums[i]] < rank[datums[j]]
		}
		return datums[i] < datums[j]
	})

	n := len(datums)
	totalPages := 1
	if limit > 0 {
		totalPages = (n + limit - 1) / limit
		if n == 0 {
			totalPages = 0
		}
	}
	if limit > 0 && n > 0 && page >= totalPages {
		return client.DatumPage{}, fmt.Errorf("page %d out of range: %d page(s)", page, totalPages)
	}
	start, end := 0, n
	if limit > 0 {
		start = page * limit
		if end > start+limit {
			end = start + limit
		}
	}
	out := client.DatumPage{TotalPages: totalPages, Page: page}
	for _, id := range datums[start:end] {
		if !detailed {
			// without statistics the datums are listable by identity only
			// (SB-081)
			out.Datums = append(out.Datums, client.DatumInfo{ID: id})
			continue
		}
		st, ok := dedup[id]
		if !ok {
			// queued: part of the job's datum set, not yet picked up by a
			// worker (SB-080's listing is complete during execution)
			out.Datums = append(out.Datums, client.DatumInfo{ID: id, State: "pending"})
			continue
		}
		info := datumInfo(id, st)
		if s, ok := rec.DatumStates[id]; ok {
			info.State = s
		}
		out.Datums = append(out.Datums, info)
	}
	return out, nil
}

// inspectDatum returns one datum's record; without per-datum statistics no
// per-datum detail exists and the inspection errors (SB-081).
func (d *daemon) inspectDatum(jobID, datumID string) (client.DatumInfo, error) {
	rec, err := d.loadJobRec(jobID)
	if err != nil {
		return client.DatumInfo{}, err
	}
	if !d.statsEnabled(rec.Pipeline) {
		return client.DatumInfo{}, fmt.Errorf("per-datum statistics are not enabled for pipeline %q", rec.Pipeline)
	}
	dedup := d.loadDedup(rec.Pipeline)
	st, ok := dedup[datumID]
	if !ok {
		return client.DatumInfo{}, fmt.Errorf("datum %q not found in job %q", datumID, jobID)
	}
	info := datumInfo(datumID, st)
	if s, ok := rec.DatumStates[datumID]; ok {
		info.State = s
	}
	return info, nil
}
