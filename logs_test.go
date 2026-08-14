package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestLogCaptureConcurrentWrites pins the invariant that logCapture is
// safe under concurrent writers. exec.Cmd today funnels both fds through
// one copy goroutine because Stdout and Stderr are the same (comparable)
// *multiWriter, but that is caller wiring, not a property of this type —
// the shared partial-line buffer and file are unprotected without the
// mutex. Under -race this test reports the unsynchronized access on the
// old code (8 goroutines, 16k writes) and passes clean with it, and the
// content assertions catch torn/duplicated/lost lines: every unique line
// must land exactly once.
func TestLogCaptureConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "job.jsonl")
	lc, err := newLogCapture(path)
	if err != nil {
		t.Fatal(err)
	}
	defer lc.Close()

	const goroutines, perGoroutine = 8, 2000
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := range perGoroutine {
				if _, err := io.WriteString(lc, fmt.Sprintf("g%d-%d\n", g, k)); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	lc.Close()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, raw := range bytes.Split(b, []byte{'\n'}) {
		if len(raw) == 0 {
			continue
		}
		var r logLineRec
		if json.Unmarshal(raw, &r) != nil {
			t.Fatalf("torn line: %q", raw)
		}
		seen[r.Line]++
	}
	if len(seen) != goroutines*perGoroutine {
		t.Fatalf("got %d distinct lines, want %d (lost or duplicated)", len(seen), goroutines*perGoroutine)
	}
	for line, n := range seen {
		if n != 1 {
			t.Errorf("line %q written %d times, want exactly once", line, n)
		}
	}
}
