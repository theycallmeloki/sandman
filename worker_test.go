package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestRunExecTimeoutTimerStopsOnCompletion pins the rule on the worker side:
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

// TestRunExecCustomizationVolumes pins the worker side of the execution-
// environment customization: a pipeline's declared volumes must reach a
// worker-placed job mounted at /sandman/volumes/<name>, exactly as the control
// plane's local executor mounts them (hostPath as given, emptyDir per attempt).
func TestRunExecCustomizationVolumes(t *testing.T) {
	rec := filepath.Join(t.TempDir(), "runs")
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
		Cname: "sandman-j1-0-1",
		Cmd:   []string{"true"},
		Volumes: []execVolume{
			{Name: "milady-iso", HostPath: "/sandman/volumes/milady-iso"},
			{Name: "scratch", EmptyDir: true},
		},
	})
	if res.PrimaryCode != 0 || res.Error != "" {
		t.Fatalf("runExec: code=%d err=%q", res.PrimaryCode, res.Error)
	}
	b, err := os.ReadFile(rec)
	if err != nil {
		t.Fatalf("read docker record: %v", err)
	}
	got := string(b)
	if !strings.Contains(got, "/sandman/volumes/milady-iso:/sandman/volumes/milady-iso") {
		t.Fatalf("hostPath volume not mounted: %s", got)
	}
	if !strings.Contains(got, ":/sandman/volumes/scratch") {
		t.Fatalf("emptyDir volume not mounted: %s", got)
	}
}

// TestRunExecToStreamsLiveOutput pins the worker half of `sandman run
// --worker`: the live writer receives the container's output as it runs,
// and the captured log still carries it for the result. The fake docker is
// the same harness the other worker tests use — no real container.
func TestRunExecToStreamsLiveOutput(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "docker"),
		[]byte("#!/bin/sh\necho container-out\nexit 3\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))

	var live strings.Builder
	res := runExecTo("test-node", execRequest{
		Cname:  "sandman-exec-test",
		Image:  "alpine",
		Cmd:    []string{"true"},
		Stream: true,
	}, &live)
	if res.PrimaryCode != 3 {
		t.Fatalf("PrimaryCode = %d, want the container's 3", res.PrimaryCode)
	}
	if !strings.Contains(live.String(), "container-out") {
		t.Fatalf("live output = %q, want container-out", live.String())
	}
	if !strings.Contains(res.Log, "container-out") {
		t.Fatalf("result log = %q, want the captured output", res.Log)
	}
}
