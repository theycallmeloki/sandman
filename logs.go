package main

// Meta plane, log store: every job's container output is captured as
// timestamped JSON lines under <state>/logs/<jobid>.jsonl and served
// through GET /api/v1/logs (SB-059/060/061, D-21). Plain files, no
// external backend: a job's logs are complete, streamable in follow mode
// at any volume, and searchable globally.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// logLineRec is one stored log line: the captured text and its emission
// time (for since-window filtering).
type logLineRec struct {
	T    string `json:"t"` // RFC3339Nano, capture time
	Line string `json:"line"`
}

func (d *daemon) logPath(id string) string {
	return filepath.Join(d.state, "logs", id+".jsonl")
}

// logCapture turns a job's container output into timestamped JSON lines in
// its log file. Only complete lines are ever appended (partials are held
// until the newline arrives or Close), so a reader can always parse the
// file tail — follow tails never need to handle torn lines. Writes are
// serialized: exec.Cmd happens to funnel both fds through one copy
// goroutine when Stdout and Stderr are the same writer, but that is a
// property of the caller's wiring, not of this type — concurrent writers
// must be safe (the partial-line buffer and the file are shared state).
type logCapture struct {
	f    *os.File
	mu   sync.Mutex
	part []byte
}

func newLogCapture(path string) (*logCapture, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &logCapture{f: f}, nil
}

func (l *logCapture) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	data := append(l.part, p...)
	l.part = l.part[:0]
	start := 0
	for {
		i := bytes.IndexByte(data[start:], '\n')
		if i < 0 {
			break
		}
		l.emit(string(data[start : start+i]))
		start += i + 1
	}
	l.part = append(l.part, data[start:]...)
	return len(p), nil
}

func (l *logCapture) emit(line string) {
	b, _ := json.Marshal(logLineRec{T: now(), Line: strings.TrimSuffix(line, "\r")})
	b = append(b, '\n')
	l.f.Write(b)
}

// Close flushes any unterminated line and closes the file.
func (l *logCapture) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.part) > 0 {
		l.emit(string(l.part))
		l.part = nil
	}
	l.f.Close()
}

// readLogLines decodes a job's log file. A missing file is an empty log;
// a line a hard crash left torn is skipped, not fatal.
func (d *daemon) readLogLines(id string) []logLineRec {
	b, err := os.ReadFile(d.logPath(id))
	if err != nil {
		return nil
	}
	var out []logLineRec
	for _, raw := range bytes.Split(b, []byte{'\n'}) {
		if len(raw) == 0 {
			continue
		}
		var r logLineRec
		if json.Unmarshal(raw, &r) != nil {
			continue
		}
		out = append(out, r)
	}
	return out
}

// sinceOK reports whether a line falls inside the since-window (cutoff is
// zero when no window is requested).
func sinceOK(r logLineRec, cutoff time.Time) bool {
	if cutoff.IsZero() {
		return true
	}
	t, err := time.Parse(time.RFC3339Nano, r.T)
	return err == nil && !t.Before(cutoff)
}

// ---- query surface ----

// logFilter is the resolved form of GET /api/v1/logs.
type logFilter struct {
	pipeline  string
	job       string // as given
	jobID     string // normalized (a job id or its output commit)
	datumPath string
	datum     string
	since     time.Time // cutoff: lines older are excluded
	follow    bool
}

func (d *daemon) resolveLogFilter(r *http.Request) (*logFilter, error) {
	q := r.URL.Query()
	f := &logFilter{
		pipeline:  q.Get("pipeline"),
		job:       q.Get("job"),
		datumPath: q.Get("datumPath"),
		datum:     q.Get("datum"),
		follow:    q.Get("follow") == "1",
	}
	if s := q.Get("since"); s != "" {
		win, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("invalid since %q: %v", s, err)
		}
		f.since = time.Now().UTC().Add(-win)
	}
	if f.job != "" {
		j, err := d.inspectJob(f.job) // also accepts an output commit (SB-135)
		if err != nil {
			return nil, err
		}
		f.jobID = j.ID
	} else if f.pipeline != "" {
		if err := d.requirePipeline(f.pipeline); err != nil {
			return nil, err
		}
	}
	if (f.datumPath != "" || f.datum != "") && f.jobID == "" && f.pipeline == "" {
		return nil, fmt.Errorf("a datum filter requires a pipeline or job")
	}
	return f, nil
}

// logJobIDs resolves the filter to the matching job ids. Follow mode
// re-resolves per tick so jobs that appear mid-stream are picked up.
func (d *daemon) logJobIDs(f *logFilter) ([]string, error) {
	var ids []string
	if f.jobID != "" {
		ids = []string{f.jobID}
	} else {
		for _, j := range d.mustListJobs() {
			if f.pipeline == "" || j.Pipeline == f.pipeline {
				ids = append(ids, j.ID)
			}
		}
	}
	if f.datumPath == "" && f.datum == "" {
		return ids, nil
	}
	var out []string
	for _, id := range ids {
		rec, err := d.loadJobRec(id)
		if err != nil {
			continue
		}
		if datumMatch(rec.Datums, f.datumPath, f.datum) {
			out = append(out, id)
		}
	}
	return out, nil
}

func (d *daemon) currentLogIDs(f *logFilter) []string {
	ids, _ := d.logJobIDs(f)
	return ids
}

// datumMatch narrows jobs by an input file: an explicit datumPath matches
// paths only; a bare datum value matches a path or its content hash. Path
// and hash filters therefore agree (SB-060).
func datumMatch(ds []datumRef, path, raw string) bool {
	if path != "" {
		for _, d := range ds {
			if d.Path == path {
				return true
			}
		}
		return false
	}
	for _, d := range ds {
		if d.Path == raw || d.Hash == raw {
			return true
		}
	}
	return false
}

func (d *daemon) loadJobRec(id string) (*jobRec, error) {
	b, err := os.ReadFile(filepath.Join(d.jobDir(id), "job.json"))
	if err != nil {
		return nil, err
	}
	var rec jobRec
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// collectLogs gathers the matching lines, oldest first.
func (d *daemon) collectLogs(f *logFilter) ([]string, error) {
	ids, err := d.logJobIDs(f)
	if err != nil {
		return nil, err
	}
	type ent struct {
		t    time.Time
		line string
	}
	var all []ent
	for _, id := range ids {
		for _, r := range d.readLogLines(id) {
			if !sinceOK(r, f.since) {
				continue
			}
			t, _ := time.Parse(time.RFC3339Nano, r.T)
			all = append(all, ent{t: t, line: r.Line})
		}
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].t.Before(all[j].t) })
	lines := make([]string, len(all))
	for i, e := range all {
		lines[i] = e.line
	}
	return lines, nil
}

// logsH serves GET /api/v1/logs: a JSON {"lines": [...]} listing, or a
// live newline-delimited stream in follow mode.
func (d *daemon) logsH(w http.ResponseWriter, r *http.Request) error {
	f, err := d.resolveLogFilter(r)
	if err != nil {
		return err
	}
	if f.follow {
		return d.followLogs(w, r, f)
	}
	lines, err := d.collectLogs(f)
	if err != nil {
		return err
	}
	writeJSON(w, map[string][]string{"lines": lines})
	return nil
}

// followLogs streams live log lines: only lines captured after the request
// began, as newline-delimited {"line": ...} objects. The stream ends when
// the client disconnects.
func (d *daemon) followLogs(w http.ResponseWriter, r *http.Request, f *logFilter) error {
	// Snapshot current file sizes first so pre-request lines are never
	// replayed: follow streams logs as they are produced (SB-060).
	offsets := map[string]int64{}
	for _, id := range d.currentLogIDs(f) {
		if st, err := os.Stat(d.logPath(id)); err == nil {
			offsets[d.logPath(id)] = st.Size()
		}
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	fl, ok := w.(http.Flusher)
	if !ok {
		return nil
	}
	fl.Flush()
	enc := struct {
		Line string `json:"line"`
	}{}
	for {
		for _, id := range d.currentLogIDs(f) {
			p := d.logPath(id)
			st, err := os.Stat(p)
			if err != nil {
				delete(offsets, p)
				continue
			}
			off := offsets[p]
			if st.Size() < off {
				delete(offsets, p) // truncated: restart from the top
				off = 0
			}
			if st.Size() == off {
				continue
			}
			fh, err := os.Open(p)
			if err != nil {
				continue
			}
			fh.Seek(off, 0)
			// ReadString has no token cap: a bufio.Scanner (64KB default
			// max token) stops on longer lines — ErrTooLong aborts the
			// scan mid-line, the recorded offset lands mid-line, and the
			// line is silently dropped (M9). ReadString grows as needed
			// and the offset stays exact (at EOF everything is consumed).
			rd := bufio.NewReader(fh)
			for {
				line, err := rd.ReadString('\n')
				if len(line) > 0 {
					var rec logLineRec
					if json.Unmarshal([]byte(line), &rec) == nil && sinceOK(rec, f.since) {
						enc.Line = rec.Line
						b, _ := json.Marshal(enc)
						b = append(b, '\n')
						if _, err := w.Write(b); err != nil {
							fh.Close()
							return nil // client gone; follow is best-effort
						}
					}
				}
				if err != nil {
					break // EOF or a read error: this poll is done
				}
			}
			pos, _ := fh.Seek(0, io.SeekCurrent)
			fh.Close()
			offsets[p] = pos
			fl.Flush()
		}
		select {
		case <-r.Context().Done():
			return nil
		case <-time.After(150 * time.Millisecond):
		}
	}
}
