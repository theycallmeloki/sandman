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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
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
	// candidate paths: every file path and every ancestor directory
	candSet := map[string]bool{}
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
			prefix := c + "/"
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

	dedupMu sync.Mutex
	dedup   map[string]datumState
}

func (jx *jobExec) canceled() bool {
	return jx.rj.cancelled.Load()
}

func (jx *jobExec) setDatum(id string, st datumState) {
	jx.dedupMu.Lock()
	defer jx.dedupMu.Unlock()
	jx.dedup[id] = st
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
// parallelism constant, capped at the datum count (SB-165: never more
// workers than datums).
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
	work := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range work {
				d.execDatum(jx, todo[i], w, i)
			}
		}(w)
	}
	for i := range todo {
		work <- i
	}
	close(work)
	wg.Wait()
	for _, dt := range todo {
		if jx.dedup[dt.ID].Outcome == "failed" {
			return true
		}
	}
	return false
}

// execDatum processes one datum: up to DatumTries attempts of the primary
// command, each logged to the job log (SB-134), then the record is
// finalized. A cancelled job stops starting attempts.
func (d *daemon) execDatum(jx *jobExec, dt datum, worker, index int) {
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
	var lastCode int
	var lastTail string
	for attempt := 1; attempt <= tries; attempt++ {
		if jx.canceled() {
			rec.Outcome = "failed"
			rec.Reason = "job cancelled"
			rec.Tries = attempt - 1
			rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
			rec.ProcessTime = time.Since(started).Seconds()
			jx.setDatum(dt.ID, rec)
			return
		}
		ok, code, tail, files := d.runDatumAttempt(jx, dt, index, attempt)
		if ok {
			// the datum's output files are already content-addressed blobs
			// (storeOutput): the record's references stay readable for
			// carry-forward and the output merge
			rec.Outcome = "success"
			rec.Tries = attempt
			rec.Files = files
			rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
			rec.ProcessTime = time.Since(started).Seconds()
			jx.setDatum(dt.ID, rec)
			return
		}
		lastCode, lastTail = code, tail
		d.appendLogLine(jx.id, fmt.Sprintf("datum %s: errored running user code after %d attempt(s)", dt.ID, attempt))
	}
	rec.Outcome = "failed"
	rec.Tries = tries
	rec.Reason = fmt.Sprintf("exited with status %d", lastCode)
	if lastTail != "" {
		if r := strings.TrimSpace(lastTail); len(r) > 2000 {
			r = r[len(r)-2000:]
		}
		rec.Reason += ": " + strings.TrimSpace(lastTail)
	}
	rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
	rec.ProcessTime = time.Since(started).Seconds()
	jx.setDatum(dt.ID, rec)
	// a provisioning failure (the image cannot be obtained at all) is an
	// environment problem, not a user-code failure: the pipeline enters
	// the crashed state (SB-043, SB-091).
	if isProvisioningError(lastTail) {
		d.markPipelineCrashed(jx.pl.Pipeline.Name, "datum "+dt.ID+": "+strings.TrimSpace(lastTail))
	}
}

// runDatumAttempt materializes one datum's per-side input files, runs the
// transform against them (the container, or the default copy entry point),
// and returns success, the exit code, the output tail, and the produced
// files.
func (d *daemon) runDatumAttempt(jx *jobExec, dt datum, index, attempt int) (bool, int, string, []fileRef) {
	// per-attempt staging, keyed by the datum's index so concurrent and
	// repeated datums never share a directory; a failed attempt's partial
	// output is discarded
	dir := filepath.Join(d.jobDir(jx.id), "datum", fmt.Sprintf("%d-%d", index, attempt))
	outDir := filepath.Join(dir, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return false, 1, err.Error(), nil
	}
	// materialize each side's files into its own input directory
	var mounts []string
	env := append([]string{}, jx.env...)
	for _, sd := range dt.Sides {
		inDir := filepath.Join(dir, "in", sd.Name)
		if err := os.MkdirAll(inDir, 0o755); err != nil {
			return false, 1, err.Error(), nil
		}
		for _, f := range sd.Files {
			data, err := d.store.readBlob(jx.views[sd.Name][f].SHA)
			if err != nil {
				return false, 1, "materialize input: " + err.Error(), nil
			}
			dst := filepath.Join(inDir, filepath.FromSlash(f))
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return false, 1, err.Error(), nil
			}
			if err := os.WriteFile(dst, data, 0o644); err != nil {
				return false, 1, err.Error(), nil
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

	// the container is registered so a cancel can kill it mid-flight
	cname := fmt.Sprintf("sandman-%s-%d-%d", jx.id, index, attempt)
	jx.registerContainer(cname)
	defer jx.unregisterContainer(cname)

	capture, capErr := newLogCapture(d.logPath(jx.id))
	if capErr != nil {
		capture = nil
	}
	tr := jx.pl.Pipeline.Transform
	var code int
	var tail string
	if len(tr.Cmd) == 0 && len(tr.Stdin) == 0 {
		// default entry point: copy every side's files to OUT
		code = 0
		for _, sd := range dt.Sides {
			inDir := filepath.Join(dir, "in", sd.Name)
			if c := copyDir(inDir, outDir); c != 0 {
				code = c
			}
		}
	} else {
		code, tail = d.runPipelineContainerNamed(jx.pl, jx.id, cname, env, mounts, outDir, capture)
	}
	if capture != nil {
		capture.Close()
	}
	accepted := tr.AcceptReturnCode != 0 && code == tr.AcceptReturnCode
	if code != 0 && !accepted {
		return false, code, tail, nil
	}
	files, err := d.storeOutput(outDir)
	if err != nil {
		return false, 1, "scan output: " + err.Error(), nil
	}
	return true, 0, "", files
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

// storeOutput lists a staging directory's files with their content hashes
// and writes each file's content into the object store, so the returned
// references are readable by hash.
func (d *daemon) storeOutput(dir string) ([]fileRef, error) {
	var out []fileRef
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		d.store.writeBlob(data)
		out = append(out, fileRef{Path: filepath.ToSlash(rel), Hash: hex.EncodeToString(sum[:]), Size: uint64(len(data))})
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, err
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
