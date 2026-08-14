package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// lockedRecorder is a race-free stand-in for the follow's ResponseWriter:
// the stream goroutine writes while the test polls, so the body buffer is
// mutex-guarded (httptest.NewRecorder's is not).
type lockedRecorder struct {
	mu   sync.Mutex
	body bytes.Buffer
}

func (l *lockedRecorder) Header() http.Header { return http.Header{} }
func (l *lockedRecorder) WriteHeader(int)     {}
func (l *lockedRecorder) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.body.Write(p)
}
func (l *lockedRecorder) Flush()           {}
func (l *lockedRecorder) snapshot() string { l.mu.Lock(); defer l.mu.Unlock(); return l.body.String() }

// TestFollowLogsLongLine pins M9: a log line over bufio's default 64KB
// token cap must stream through the follow, not silently drop. The old
// Scanner aborted mid-line on ErrTooLong, the recorded offset landed
// mid-line, and the line never appeared. The stream runs through the
// real follow loop (file written after the offsets snapshot, cancelled
// via the request context).
func TestFollowLogsLongLine(t *testing.T) {
	dir := t.TempDir()
	d := &daemon{state: dir}
	big := strings.Repeat("x", 100*1024)
	longRec, _ := json.Marshal(logLineRec{T: now(), Line: big})
	smallRec, _ := json.Marshal(logLineRec{T: now(), Line: "second"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest("GET", "/api/v1/logs?follow=1", nil).WithContext(ctx)
	rec := &lockedRecorder{}
	done := make(chan struct{})
	go func() {
		if err := d.followLogs(rec, req, &logFilter{jobID: "j1", follow: true}); err != nil {
			t.Errorf("followLogs: %v", err)
		}
		close(done)
	}()

	// the file appears after the follow's offsets snapshot (a small
	// delay guarantees the snapshot ran first — otherwise the whole file
	// counts as pre-request and is never streamed): both lines are new
	time.Sleep(300 * time.Millisecond)
	if err := os.MkdirAll(filepath.Join(dir, "logs"), 0o755); err != nil {
		t.Fatal(err)
	}
	body := append(append(longRec, '\n'), append(smallRec, '\n')...)
	if err := os.WriteFile(filepath.Join(dir, "logs", "j1.jsonl"), body, 0o644); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(rec.snapshot(), "second") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("follow did not stop after cancel")
	}

	out := rec.snapshot()
	if !strings.Contains(out, big) {
		t.Fatalf("100KB line dropped by the follow (body %d bytes, long line absent)", len(out))
	}
	if !strings.Contains(out, "second") {
		t.Fatalf("line after the long one missing (body %d bytes)", len(out))
	}
}
