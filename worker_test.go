package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestRunExecTimeoutTimerStopsOnCompletion pins H4 on the worker side:
// runExec's per-attempt timer must be stopped when the attempt completes
// (a restart re-runs with the same container name; a stale timer would
// kill the restarted attempt's container). A fake docker in PATH records
// invocations — the only docker call on the default entry point is the
// timer's kill, so an empty record proves the timer never fired.
func TestRunExecTimeoutTimerStopsOnCompletion(t *testing.T) {
	rec := filepath.Join(t.TempDir(), "kills")
	bin := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "docker"),
		[]byte("#!/bin/sh\necho \"$@\" >> \"$REC\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REC", rec)
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))

	res := runExec("test-node", execRequest{
		Cname:        "sandman-j1-0-1",
		DatumTimeout: "50ms",
	})
	if res.PrimaryCode != 0 || res.Error != "" {
		t.Fatalf("runExec: code=%d err=%q", res.PrimaryCode, res.Error)
	}
	time.Sleep(150 * time.Millisecond)
	if b, err := os.ReadFile(rec); err == nil && len(b) > 0 {
		t.Fatalf("stale timeout timer ran: docker %s", b)
	}
}
