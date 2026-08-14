package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"sandman/client"
	"sandman/internal/store"
)

// fakeDocker installs a docker script in PATH that records its arguments
// to rec and exits 0: the spout/service goroutines' container calls
// (run, inspect, kill) become observable no-ops, so a test can race a
// spawn without touching a real daemon.
func fakeDocker(t *testing.T, rec string) {
	t.Helper()
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
}

// assertPreRegisteredSettle pins the M1 invariant: when a spout/service
// spawn returns, its running handle must already be registered — a
// stop/delete in the window between the goroutine starting and the
// in-goroutine registration would find nothing to cancel, and the spout
// or service would then run (container up, port bound, cycles
// committing) against a stopped or deleted pipeline until the next stop
// or a daemon restart. The cancel must succeed and the job must settle.
func assertPreRegisteredSettle(t *testing.T, d *daemon, id string) {
	t.Helper()
	d.jobsMu.Lock()
	rj, ok := d.running[id]
	d.jobsMu.Unlock()
	if !ok {
		t.Fatal("running handle not registered when the spawn returned — a stop/delete in this window escapes the cancel")
	}
	if err := d.cancelJob(id); err != nil {
		t.Fatalf("cancel the instant after spawn: %v (stop/delete would report this error and the job would keep running)", err)
	}
	select {
	case <-rj.done:
	case <-time.After(5 * time.Second):
		t.Fatal("spawned job did not settle after cancel — it escaped and keeps running")
	}
}

func TestSpoutPreRegistersRunningHandle(t *testing.T) {
	rec := filepath.Join(t.TempDir(), "kills")
	fakeDocker(t, rec)
	d := &daemon{state: t.TempDir(), runner: &killRecorder{}, running: map[string]*runningJob{}}
	d.store = store.New(filepath.Join(d.state, "store"))
	pl := &pipelineRec{Pipeline: client.Pipeline{
		Name:      "sp1",
		Spout:     &client.Spout{},
		Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "true"}},
	}}
	for i := range 30 {
		id := d.spawnSpoutJob(pl, false)
		assertPreRegisteredSettle(t, d, id)
		if i == 29 {
			t.Logf("30/30 spawns: handle registered at return, cancel landed, job settled")
		}
	}
}

func TestServicePreRegistersRunningHandle(t *testing.T) {
	rec := filepath.Join(t.TempDir(), "kills")
	fakeDocker(t, rec)
	d := &daemon{state: t.TempDir(), runner: &killRecorder{}, running: map[string]*runningJob{}}
	d.store = store.New(filepath.Join(d.state, "store"))
	pl := &pipelineRec{Pipeline: client.Pipeline{
		Name:      "sv1",
		Service:   &client.Service{},
		Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "true"}},
	}}
	for i := range 30 {
		id := d.spawnServiceJob(pl)
		assertPreRegisteredSettle(t, d, id)
		if i == 29 {
			t.Logf("30/30 spawns: handle registered at return, cancel landed, job settled")
		}
	}
}
