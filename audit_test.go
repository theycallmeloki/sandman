package main

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"sandman/internal/store"
)

// readAuditRecords parses the append-only JSONL trail. Every line must be
// valid JSON: a torn or interleaved line fails the test.
func readAuditRecords(t *testing.T, path string) []auditRecord {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open audit log: %v", err)
	}
	defer func() { _ = f.Close() }()
	var recs []auditRecord
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r auditRecord
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("audit line is not JSON (%q): %v", line, err)
		}
		recs = append(recs, r)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	return recs
}

func newAuditTestDaemon(t *testing.T) (*daemon, http.Handler) {
	t.Helper()
	state := t.TempDir()
	d := &daemon{state: state, store: store.New(state)}
	return d, d.apiHandler()
}

// TestAuditRecordsMutations pins the core contract: mutating methods
// (POST/PUT/DELETE/PATCH) append one record with the operation, method,
// path, source IP, status, and duration; a GET appends nothing. The 4xx
// mutation is recorded too — a rejected attempt is still a mutation
// attempt. The record contains no credentials, query string, or body.
func TestAuditRecordsMutations(t *testing.T) {
	d, h := newAuditTestDaemon(t)

	// successful POST: create a repo, with a query string and body that
	// must never reach the audit trail
	body := `{"name":"audited","token":"bodysecret"}`
	req := httptest.NewRequest("POST", "/api/v1/repos?token=querysecret", strings.NewReader(body))
	req.RemoteAddr = "203.0.113.9:54321"
	req.Header.Set("Authorization", "Basic YWxpY2U6aHVudGVyMg==")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create repo: status %d body %q", rec.Code, rec.Body.String())
	}

	// 4xx mutation: secret create with no name
	req = httptest.NewRequest("POST", "/api/v1/secrets", strings.NewReader(`{"data":{"token":"topsecret"}}`))
	req.RemoteAddr = "198.51.100.7:1234"
	req.Header.Set("Authorization", "Basic YWxpY2U6aHVudGVyMg==")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid secret create: status %d, want 400", rec.Code)
	}

	// a read must not be audited
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/version", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("version: status %d", rec.Code)
	}

	path := d.auditLogPath()
	recs := readAuditRecords(t, path)
	if len(recs) != 2 {
		t.Fatalf("audit records = %d, want 2 (POSTs only): %+v", len(recs), recs)
	}

	byStatus := map[int]auditRecord{}
	for _, r := range recs {
		byStatus[r.Status] = r
	}
	ok, bad := byStatus[200], byStatus[400]
	if ok.Operation != "repos.create" || ok.Method != "POST" || ok.Path != "/api/v1/repos" {
		t.Fatalf("success record = %+v", ok)
	}
	if ok.SourceIP != "203.0.113.9" {
		t.Fatalf("success source_ip = %q, want 203.0.113.9", ok.SourceIP)
	}
	if ok.Timestamp == "" || ok.DurationMS < 0 {
		t.Fatalf("success record lacks timestamp/duration: %+v", ok)
	}
	if bad.Operation != "secrets.create" || bad.Status != 400 || bad.SourceIP != "198.51.100.7" {
		t.Fatalf("4xx record = %+v", bad)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{"querysecret", "bodysecret", "topsecret", "Authorization", "hunter2"} {
		if strings.Contains(string(raw), leak) {
			t.Fatalf("audit log leaks %q: %s", leak, raw)
		}
	}

	// dir 0700, file 0600
	di, err := os.Stat(filepath.Join(d.state, "audit"))
	if err != nil {
		t.Fatal(err)
	}
	if got := di.Mode().Perm(); got != 0o700 {
		t.Fatalf("audit dir mode = %#o, want 0700", got)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Fatalf("audit file mode = %#o, want 0600", got)
	}
}

// TestAuditRecordsRejectedAuth pins that a mutating request rejected by
// HTTP Basic Auth is still audited (with the 401 status): the trail is a
// mutation history, not an authorized-actions-only log.
func TestAuditRecordsRejectedAuth(t *testing.T) {
	state := t.TempDir()
	d := &daemon{
		state: state,
		store: store.New(state),
		auth:  authConfig{User: "alice", Password: "s3cret"},
	}
	h := d.apiHandler()
	req := httptest.NewRequest("POST", "/api/v1/repos", strings.NewReader(`{"name":"nope"}`))
	req.RemoteAddr = "192.0.2.55:9999"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated mutation: status %d, want 401", rec.Code)
	}
	recs := readAuditRecords(t, d.auditLogPath())
	if len(recs) != 1 {
		t.Fatalf("audit records = %d, want 1: %+v", len(recs), recs)
	}
	if recs[0].Status != 401 || recs[0].Method != "POST" ||
		recs[0].Path != "/api/v1/repos" || recs[0].SourceIP != "192.0.2.55" {
		t.Fatalf("401 record = %+v", recs[0])
	}
	if recs[0].Operation == "" {
		t.Fatalf("401 record has no operation: %+v", recs[0])
	}
}

// TestAuditAppendsAcrossRestart pins durability: a second daemon over the
// same state dir appends to the existing trail instead of truncating it,
// and concurrent mutations never tear a line.
func TestAuditAppendsAcrossRestart(t *testing.T) {
	state := t.TempDir()
	d1 := &daemon{state: state, store: store.New(state)}
	h1 := d1.apiHandler()

	req := httptest.NewRequest("POST", "/api/v1/repos", strings.NewReader(`{"name":"one"}`))
	rec := httptest.NewRecorder()
	h1.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("first daemon create: %d %q", rec.Code, rec.Body.String())
	}

	// "restart": a fresh daemon over the same state dir
	d2 := &daemon{state: state, store: store.New(state)}
	h2 := d2.apiHandler()
	req = httptest.NewRequest("POST", "/api/v1/repos", strings.NewReader(`{"name":"two"}`))
	rec = httptest.NewRecorder()
	h2.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("second daemon create: %d %q", rec.Code, rec.Body.String())
	}

	path := d2.auditLogPath()
	if got := len(readAuditRecords(t, path)); got != 2 {
		t.Fatalf("records after restart = %d, want 2 (append, not truncate)", got)
	}

	// concurrent appends: every record survives as its own valid line
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := `{"name":"concurrent-` + strconv.Itoa(i) + `"}`
			req := httptest.NewRequest("POST", "/api/v1/repos", strings.NewReader(body))
			rec := httptest.NewRecorder()
			h2.ServeHTTP(rec, req)
		}()
	}
	wg.Wait()
	if got := len(readAuditRecords(t, path)); got != 18 {
		t.Fatalf("records after concurrent appends = %d, want 18", got)
	}
}
