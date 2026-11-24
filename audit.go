package main

// Mutation audit trail: an append-only JSONL record of every mutating HTTP
// operation the daemon serves. One line per POST/PUT/DELETE/PATCH handled
// through the instrument wrapper — timestamp, operation name, method, path,
// source IP, response status, and duration. Requests that only read (GET,
// HEAD, OPTIONS) leave no record.
//
// The log is deliberately value-free: it never carries request or response
// bodies, headers, credentials, query strings, or secret values. A record
// holds only the route path (no query), so an operator can correlate a
// mutation with a caller and an outcome without the log becoming a second
// place secrets live.
//
// Records append under <state>/audit/mutations.jsonl; the dir is 0700 and
// the file 0600 (private to the daemon's user). Appending — never
// rewriting — keeps the trail durable across restarts, and a mutex
// serializes concurrent handlers so interleaved writes never tear a line.
// No endpoint exposes the log.

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	auditDirName  = "audit"
	auditFileName = "mutations.jsonl"
)

// auditRecord is one JSONL line. Field names are stable: operators and
// log shippers read them directly.
type auditRecord struct {
	Timestamp  string `json:"timestamp"`
	Operation  string `json:"operation"`
	Method     string `json:"method"`
	Path       string `json:"path"`
	SourceIP   string `json:"source_ip"`
	Status     int    `json:"status"`
	DurationMS int64  `json:"duration_ms"`
}

// auditLog serializes appends so concurrent requests cannot interleave a
// line. The zero value is ready to use.
type auditLog struct {
	mu sync.Mutex
}

// isMutatingMethod reports whether the method changes control-plane state
// and therefore belongs in the audit trail.
func isMutatingMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch:
		return true
	}
	return false
}

// auditOpKey carries a per-request operation-name holder from the outer
// audit handler down to instrument, which knows the route's semantic name
// ("repos.create"). The outer handler records once, after the response, so
// a request is audited exactly once even though two wrappers see it.
type auditOpKey struct{}

type auditOp struct{ name string }

func auditOpFrom(ctx context.Context) *auditOp {
	op, _ := ctx.Value(auditOpKey{}).(*auditOp)
	return op
}

// auditHTTP is the central mutation wrapper: it times every mutating HTTP
// request (POST/PUT/DELETE/PATCH — reads pass straight through), lets the
// inner stack serve it, then appends one record. Wrapping outside auth
// means a rejected attempt (401) is still part of the mutation history.
// The operation name comes from instrument's handler when the route is a
// known API operation; otherwise the matched pattern or path identifies
// the target (MCP, unknown routes).
func (d *daemon) auditHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isMutatingMethod(r.Method) {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		op := &auditOp{}
		r = r.WithContext(context.WithValue(r.Context(), auditOpKey{}, op))
		sw := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(sw, r)
		name := op.name
		if name == "" {
			name = r.Pattern
		}
		if name == "" && r.URL != nil {
			name = r.URL.Path
		}
		d.recordMutation(name, r, sw.status, time.Since(start))
	})
}

// auditLogPath is the append-only trail's location under the state dir.
func (d *daemon) auditLogPath() string {
	return filepath.Join(d.state, auditDirName, auditFileName)
}

// requestSourceIP extracts the caller's IP from the transport-level
// RemoteAddr. Forwarding headers are intentionally ignored: they are
// caller-controlled, and the audit trail records who actually connected.
func requestSourceIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// recordMutation appends one record for a mutating HTTP operation. It is
// best-effort with respect to the request: an unwritable audit file is
// logged and never turns a successful mutation into a failure (the
// alternative — failing the write after the mutation landed — would lie
// about the operation's outcome). Only the route path is recorded, never
// the query string.
func (d *daemon) recordMutation(op string, r *http.Request, status int, dur time.Duration) {
	if d.state == "" || !isMutatingMethod(r.Method) {
		return
	}
	path := ""
	if r.URL != nil {
		path = r.URL.Path
	}
	rec := auditRecord{
		Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
		Operation:  op,
		Method:     r.Method,
		Path:       path,
		SourceIP:   requestSourceIP(r),
		Status:     status,
		DurationMS: dur.Milliseconds(),
	}
	line, err := json.Marshal(rec)
	if err != nil {
		log.Printf("audit: marshal record: %v", err)
		return
	}
	line = append(line, '\n')

	dir := filepath.Join(d.state, auditDirName)
	d.audit.mu.Lock()
	defer d.audit.mu.Unlock()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		log.Printf("audit: create dir: %v", err)
		return
	}
	// MkdirAll leaves an existing dir's mode alone; tighten it so a
	// pre-existing state dir (or one created with a wider umask) cannot
	// expose the trail.
	if err := os.Chmod(dir, 0o700); err != nil {
		log.Printf("audit: chmod dir: %v", err)
	}
	f, err := os.OpenFile(d.auditLogPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		log.Printf("audit: open log: %v", err)
		return
	}
	defer f.Close()
	if err := f.Chmod(0o600); err != nil {
		log.Printf("audit: chmod log: %v", err)
	}
	if _, err := f.Write(line); err != nil {
		log.Printf("audit: append record: %v", err)
	}
}
