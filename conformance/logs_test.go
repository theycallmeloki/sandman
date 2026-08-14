package conformance

// Logs family (SB-059/060/061, D-21): per-job capture with pipeline, job,
// and datum filters, since-windows, follow-mode streaming, complete
// retrieval at volume, and global searchability. The daemon's own log
// store implements the mechanism-free contract of SB-062/D-21 — no
// external backend (the reference's Loki collector is not contractual).
//
// Datum filters: with the datum engine deferred (D-13), a datum is an
// input file of the job — matched by path or by content hash (hex sha256,
// as exposed by file listings). A filter selects the jobs whose input
// contained that file; path and hash filters therefore agree exactly, and
// a nonexistent file matches nothing, per SB-060.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// logsEchoTransform emits the classic log lines: a literal value and a
// literal %s that must pass through verbatim (SB-060: no MISSING
// substitution), plus six numbered lines so follow mode has volume.
func logsEchoTransform(inName string) *client.Transform {
	return &client.Transform{
		Image: "alpine:3.21",
		Cmd: []string{"sh", "-c",
			fmt.Sprintf("echo foo; echo %%s; i=0; while [ $i -lt 6 ]; do echo line$i; i=$((i+1)); done")},
	}
}

// assertLogCore runs the SB-060 core contract (steps 1–7) against one
// pipeline and one flushed job: empty-query, nonexistent targets, by
// pipeline, by job, datum errors, path/hash equivalence, nonexistent file.
// SB-059 shares it: the record is the same contract with the pipeline's
// statistics flag disabled — this run disables EnableStats for parity with
// that record (the flag itself is asserted by the SB-041/SB-139 suites).
func assertLogCore(t *testing.T, pipe string, cm1 client.Commit, job1 client.Job) {
	t.Helper()

	// 1. an unconstrained query succeeds (the record's empty-system
	//    precondition cannot hold on a daemon shared with other tests; no
	//    error is the observable contract here)
	if _, err := c.Logs(client.LogParams{}); err != nil {
		t.Fatalf("unconstrained log query: %v", err)
	}

	// 2. nonexistent pipeline and job fail, mentioning the missing target
	if _, err := c.Logs(client.LogParams{Pipeline: pipe + "-missing"}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("nonexistent pipeline: %v, want not-found error", err)
	}
	if _, err := c.Logs(client.LogParams{Job: "missing-job"}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("nonexistent job: %v, want not-found error", err)
	}

	// 3. by pipeline: the job's user log lines, verbatim and non-empty
	lines, err := c.Logs(client.LogParams{Pipeline: pipe})
	if err != nil {
		t.Fatalf("logs by pipeline: %v", err)
	}
	if len(lines) < 2 {
		t.Fatalf("by pipeline: %d lines, want at least 2", len(lines))
	}
	seen := map[string]bool{}
	for _, l := range lines {
		if l == "" {
			t.Fatalf("empty log line returned")
		}
		if strings.Contains(l, "MISSING") {
			t.Fatalf("line %q contains a MISSING substitution placeholder", l)
		}
		seen[l] = true
	}
	if !seen["foo"] {
		t.Fatalf("pipeline logs %v missing the echoed value", lines)
	}
	if !seen["%s"] {
		t.Fatalf("pipeline logs %v: literal %%s was not emitted verbatim", lines)
	}

	// 4. by job id: non-empty
	jlines, err := c.Logs(client.LogParams{Job: job1.ID})
	if err != nil || len(jlines) == 0 {
		t.Fatalf("logs by job: %v (%d lines)", err, len(jlines))
	}

	// 5. a datum filter without a pipeline or job fails — as a file path
	//    and as a datum identifier
	if _, err := c.Logs(client.LogParams{DatumPath: "file"}); err == nil || !strings.Contains(err.Error(), "requires") {
		t.Fatalf("datumPath without pipeline/job: %v, want error", err)
	}
	if _, err := c.Logs(client.LogParams{Datum: "nope"}); err == nil || !strings.Contains(err.Error(), "requires") {
		t.Fatalf("datum without pipeline/job: %v, want error", err)
	}

	// 6. filtering a job's logs by the input file path and by its content
	//    hash returns exactly the same lines
	files, err := c.ListFiles(cm1.ID)
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	var hash string
	for _, f := range files {
		if f.Path == "file" {
			hash = f.Hash
		}
	}
	if hash == "" {
		t.Fatalf("no content hash recorded for the input file")
	}
	byPath, err := c.Logs(client.LogParams{Job: job1.ID, DatumPath: "file"})
	if err != nil {
		t.Fatalf("logs by datumPath: %v", err)
	}
	byHash, err := c.Logs(client.LogParams{Job: job1.ID, Datum: hash})
	if err != nil {
		t.Fatalf("logs by datum hash: %v", err)
	}
	if len(byPath) != len(byHash) {
		t.Fatalf("path filter returned %d lines, hash filter %d", len(byPath), len(byHash))
	}
	for i := range byPath {
		if byPath[i] != byHash[i] {
			t.Fatalf("path/hash filter disagreement at line %d: %q vs %q", i, byPath[i], byHash[i])
		}
	}

	// 7. a file path absent from the input: no logs, no error
	none, err := c.Logs(client.LogParams{Job: job1.ID, DatumPath: "no-such-file"})
	if err != nil {
		t.Fatalf("nonexistent-file filter: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("nonexistent-file filter returned %d lines", len(none))
	}
}

// followCollector reads newline-delimited {"line": ...} objects from a
// follow stream until n lines arrive; the body is closed once the target
// is reached.
func followCollector(rc io.ReadCloser, n int) chan []string {
	ch := make(chan []string, 1)
	go func() {
		defer rc.Close()
		var lines []string
		sc := bufio.NewScanner(rc)
		for sc.Scan() {
			var o struct {
				Line string `json:"line"`
			}
			if json.Unmarshal(sc.Bytes(), &o) == nil {
				lines = append(lines, o.Line)
				if len(lines) >= n {
					break
				}
			}
		}
		ch <- lines
	}()
	return ch
}

func waitFollow(t *testing.T, ch chan []string, want int, timeout time.Duration) []string {
	t.Helper()
	select {
	case lines := <-ch:
		return lines
	case <-time.After(timeout):
		t.Fatalf("follow stream delivered fewer than %d lines within %s", want, timeout)
		return nil
	}
}

func TestSB059_LogsWithoutStats(t *testing.T) {
	repo := uniq(t) + "r"
	pipe := uniq(t) + "p"
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "", map[string]string{"file": "foo\n"})
	mustPipeline(t, client.Pipeline{Name: pipe, Transform: logsEchoTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	jobs := flushOK(t, cm.ID)
	assertLogCore(t, pipe, cm, jobs[0])
}

func TestSB060_LogQueries(t *testing.T) {
	repo := uniq(t) + "r"
	pipe := uniq(t) + "p"
	mustRepo(t, repo)
	cm1 := commitFiles(t, repo, "", map[string]string{"file": "foo\n"})
	mustPipeline(t, client.Pipeline{Name: pipe, Transform: logsEchoTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	jobs := flushOK(t, cm1.ID)
	assertLogCore(t, pipe, cm1, jobs[0])

	// 8. follow mode streams logs as they are produced: a new input commit
	//    arriving mid-stream yields its lines (two commits x 8 lines each)
	rc, err := c.FollowLogs(client.LogParams{Pipeline: pipe})
	if err != nil {
		t.Fatalf("open follow stream: %v", err)
	}
	follow := followCollector(rc, 16)
	cm2 := commitFiles(t, repo, "", map[string]string{"file": "bar\n"})
	flushOK(t, cm2.ID)
	cm3 := commitFiles(t, repo, "", map[string]string{"file": "baz\n"})
	flushOK(t, cm3.ID)
	if got := waitFollow(t, follow, 16, 60*time.Second); len(got) < 16 {
		t.Fatalf("follow streamed %d lines, want at least 16", len(got))
	}

	// 9. a since-window excludes logs older than the window: after a quiet
	//    period longer than the window, the query returns zero lines; a
	//    window covering the whole history still returns them
	time.Sleep(3 * time.Second)
	recent, err := c.Logs(client.LogParams{Pipeline: pipe, Since: 2 * time.Second})
	if err != nil {
		t.Fatalf("since-window query: %v", err)
	}
	if len(recent) != 0 {
		t.Fatalf("since-window returned %d lines after the quiet period, want 0", len(recent))
	}
	all, err := c.Logs(client.LogParams{Pipeline: pipe, Since: time.Hour})
	if err != nil || len(all) == 0 {
		t.Fatalf("wide since-window: %v (%d lines)", err, len(all))
	}
}

func TestSB061_ManyLogs(t *testing.T) {
	repo := uniq(t) + "r"
	pipe := uniq(t) + "p"
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "", map[string]string{"file": "foo\n"})
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "seq 0 9999"},
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	})
	jobs := flushOK(t, cm.ID)
	job := jobs[0]

	// exactly 10,000 user log lines, retrieved within a bounded retry
	// window (the log store may lag the job's terminal record)
	pollFor(t, "10000 log lines", 30*time.Second, func() bool {
		lines, err := c.Logs(client.LogParams{Job: job.ID})
		return err == nil && len(lines) == 10000
	})
	lines, err := c.Logs(client.LogParams{Job: job.ID})
	if err != nil || len(lines) != 10000 {
		t.Fatalf("job logs: %v (%d lines, want 10000)", err, len(lines))
	}
	// numbered and in order: nothing lost, nothing truncated
	if lines[0] != "0" || lines[9999] != "9999" {
		t.Fatalf("log lines %q..%q, want \"0\"..\"9999\"", lines[0], lines[9999])
	}
}

func TestSB062_GlobalLogStore(t *testing.T) {
	// D-21: a global aggregated log store is required; the contract is
	// mechanism-free — a job's logs are complete (one line per datum) and
	// streamable in follow mode, and logs from all jobs are searchable
	// globally. The daemon's own store is the implementation.
	repo := uniq(t) + "r"
	pa := uniq(t) + "a"
	pb := uniq(t) + "b"
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "", map[string]string{"a": "1\n", "b": "2\n", "c": "3\n"})
	perDatum := &client.Transform{
		Image: "alpine:3.21",
		Cmd:   []string{"sh", "-c", fmt.Sprintf("for f in ${%s}/*; do echo foo; done", repo)},
	}
	mustPipeline(t, client.Pipeline{Name: pa, Transform: perDatum, Input: &client.Input{Repo: repo, Glob: "/*"}})
	mustPipeline(t, client.Pipeline{Name: pb, Transform: &client.Transform{Image: "alpine:3.21", Cmd: []string{"sh", "-c", "echo bar"}}, Input: &client.Input{Repo: repo, Glob: "/*"}})
	jobs := flushOK(t, cm.ID)
	var aJob client.Job
	seenA, seenB := false, false
	for _, j := range jobs {
		switch j.Pipeline {
		case pa:
			aJob, seenA = j, true
		case pb:
			seenB = true
		}
	}
	if !seenA || !seenB {
		t.Fatalf("flush jobs %+v did not cover both pipelines", jobs)
	}

	// complete per job: exactly one foo per datum (input file)
	alines, err := c.Logs(client.LogParams{Job: aJob.ID})
	if err != nil {
		t.Fatalf("job a logs: %v", err)
	}
	foos := 0
	for _, l := range alines {
		if l == "foo" {
			foos++
		}
	}
	if len(alines) != 3 || foos != 3 {
		t.Fatalf("job a logs: %d lines, %d foo (want 3 of each)", len(alines), foos)
	}

	// global search: one unconstrained query sees both pipelines' lines
	all, err := c.Logs(client.LogParams{})
	if err != nil {
		t.Fatalf("global logs: %v", err)
	}
	globalFoo, globalBar := 0, 0
	for _, l := range all {
		if l == "foo" {
			globalFoo++
		}
		if l == "bar" {
			globalBar++
		}
	}
	if globalFoo < 3 || globalBar < 1 {
		t.Fatalf("global logs missing lines: %d foo, %d bar (want >= 3 and >= 1)", globalFoo, globalBar)
	}

	// follow mode streams new lines as they are produced, across pipelines.
	// cm2 changes one file: pa's job runs the one changed datum (1 foo),
	// pb's job emits its bar — the stream sees both pipelines (D-21).
	rc, err := c.FollowLogs(client.LogParams{})
	if err != nil {
		t.Fatalf("open global follow stream: %v", err)
	}
	follow := followCollector(rc, 2)
	cm2 := commitFiles(t, repo, "", map[string]string{"a": "4\n"})
	flushOK(t, cm2.ID)
	got := waitFollow(t, follow, 2, 60*time.Second)
	foos, bars := 0, 0
	for _, l := range got {
		switch l {
		case "foo":
			foos++
		case "bar":
			bars++
		}
	}
	if foos < 1 || bars < 1 {
		t.Fatalf("follow stream: %d foo, %d bar (want >= 1 and >= 1, got %v)", foos, bars, got)
	}
}
