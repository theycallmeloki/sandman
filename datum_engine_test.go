package main

import (
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sandman/client"
	"sandman/internal/store"
)

// killRecorder implements Runner, recording Kill calls: the fake backend
// lets a test observe the datum timeout timer without docker.
type killRecorder struct{ kills atomic.Int32 }

func (*killRecorder) Run(JobSpec) RunResult { return RunResult{} }
func (k *killRecorder) Kill(string) error   { k.kills.Add(1); return nil }

// ensureView materializes an input side's full view for the
// /sandman/view/<name> mount. A revision whose paths differ only in case
// cannot be written faithfully on a case-insensitive filesystem, and that
// refusal has to reach the caller — which fails the datum with the reason —
// rather than being swallowed into a silently absent mount, where the only
// symptom is a transform's own read error.
func TestEnsureViewReportsCaseCollisionInsteadOfSkippingTheMount(t *testing.T) {
	dir := t.TempDir()
	d := &daemon{state: dir, runner: processRunner{}}
	d.store = store.New(filepath.Join(dir, "store"))
	jx := &jobExec{
		d:  d,
		id: "j1",
		views: map[string]map[string]store.ViewEntry{
			"in": {"Data.txt": {}, "data.txt": {}},
		},
		viewDirs: map[string]string{},
	}
	vd, err := d.ensureView(jx, "in")
	if !store.CaseInsensitive(dir) {
		if err != nil {
			t.Fatalf("case-sensitive filesystem: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatal("a revision whose paths differ only in case materialized without error")
	}
	if vd != "" {
		t.Fatalf("a failed materialization returned the directory %q, want empty", vd)
	}
	for _, want := range []string{`"Data.txt"`, `"data.txt"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error does not name %s: %v", want, err)
		}
	}
}

// TestDatumTimeoutTimerStopsOnCompletion pins the rule that the per-attempt datum
// timeout timer must be stopped when the attempt completes. A datum
// restart re-runs from attempt=1 with the SAME container name
// (sandman-<job>-<index>-<attempt>), so a pending timer from a completed
// attempt would fire during the restarted attempt and kill its container
// mid-flight. The attempt completes well inside the 50ms timeout; a
// stale timer would fire right after and hit the recorder.
func TestDatumTimeoutTimerStopsOnCompletion(t *testing.T) {
	dir := t.TempDir()
	rec := &killRecorder{}
	d := &daemon{state: dir, runner: rec}
	d.store = store.New(filepath.Join(dir, "store"))
	jx := &jobExec{
		d:  d,
		id: "j1",
		pl: pipelineRec{Pipeline: client.Pipeline{
			Transform: &client.Transform{DatumTimeout: "50ms"},
		}},
		rj: &runningJob{},
	}
	outcome, reason, _ := d.runDatumAttempt(jx, datum{ID: "d1"}, 0, 1, time.Now().UTC(), 0)
	if outcome != stateSuccess {
		t.Fatalf("attempt outcome = %s (%s), want success", outcome, reason)
	}
	time.Sleep(150 * time.Millisecond)
	if n := rec.kills.Load(); n != 0 {
		t.Fatalf("stale timeout timer fired %d kill(s) after the attempt completed", n)
	}
}
