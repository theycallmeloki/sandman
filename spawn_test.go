package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"sandman/client"
	"sandman/internal/store"
)

// fakeDocker installs a docker script in PATH that records its arguments
// to rec and exits 0: the spout/service goroutines' container calls
// (run, inspect, kill) become observable no-ops, so a test can race a
// spawn without touching a real daemon. inspect reports the container
// running ("true") so the spout poll loop keeps polling until the cancel
// lands — a container that exits instantly would settle the job (and
// unregister its handle) before the pre-registration assert could
// observe it.
func fakeDocker(t *testing.T, rec string) {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "docker"),
		[]byte("#!/bin/sh\nif [ \"$1\" = \"inspect\" ]; then echo \"true\"; else echo \"$@\" >> \"$REC\"; fi\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REC", rec)
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
}

// assertPreRegisteredSettle pins the invariant: when a spout/service
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

// TestSpoutPassesTransformEnv pins the contract that a spout container
// receives the pipeline transform's static environment, like batch and
// service jobs do — a spout's TRACE_URL/POLL_SECS etc. come from the
// spec, and silently dropping them makes spouts unconfigurable.
func TestSpoutPassesTransformEnv(t *testing.T) {
	rec := filepath.Join(t.TempDir(), "runs")
	fakeDocker(t, rec)
	d := &daemon{state: t.TempDir(), runner: &killRecorder{}, running: map[string]*runningJob{}}
	d.store = store.New(filepath.Join(d.state, "store"))
	pl := &pipelineRec{Pipeline: client.Pipeline{
		Name:  "sp-env",
		Spout: &client.Spout{Marker: "seen"},
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", "true"},
			Env: map[string]string{
				"TRACE_URL": "http://192.168.1.147:8090/traces",
				"OUT":       "evil", // reserved — must not shadow the system value
				"MARKER":    "evil", // spout-owned — must not shadow
			},
		},
	}}
	id := d.spawnSpoutJob(pl, false)
	defer d.cancelJob(id)

	// the container launch runs in the spawn goroutine — poll for the
	// argv record instead of racing it
	var raw []byte
	for i := 0; i < 100; i++ {
		if b, err := os.ReadFile(rec); err == nil {
			raw = b
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if raw == nil {
		t.Fatalf("docker argv record never written to %s", rec)
	}
	argv := strings.Fields(string(raw))
	want := "-e"
	found := map[string]bool{}
	for i := 0; i+1 < len(argv); i++ {
		if argv[i] == want {
			found[argv[i+1]] = true
		}
	}
	if !found["TRACE_URL=http://192.168.1.147:8090/traces"] {
		t.Fatalf("spout container argv missing transform env; argv=%v", argv)
	}
	if found["OUT=evil"] {
		t.Fatalf("spout container must not shadow reserved OUT; argv=%v", argv)
	}
	if found["MARKER=evil"] {
		t.Fatalf("spout container must not shadow spout-owned MARKER; argv=%v", argv)
	}
	if !found["OUT=/sandman/out"] {
		t.Fatalf("spout container lost its system OUT; argv=%v", argv)
	}
}

// TestRespawnBackgroundJobs pins G2: after a daemon restart, spout and
// service pipelines whose records say running must come back up on boot —
// their jobs died with the old process, and startPipeline alone cannot
// resurrect them (a restart never sets Stopped, so its already-running
// guard is a no-op). Stopped pipelines stay down.
func TestRespawnBackgroundJobs(t *testing.T) {
	rec := filepath.Join(t.TempDir(), "runs")
	fakeDocker(t, rec)
	d := &daemon{state: t.TempDir(), runner: &blockingRunner{}, running: map[string]*runningJob{}}
	d.store = store.New(filepath.Join(d.state, "store"))
	sp := &pipelineRec{Pipeline: client.Pipeline{
		Name: "sp-respawn", Spout: &client.Spout{Marker: "seen"},
		Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "true"}},
	}, State: stateRunning, Version: 1}
	sv := &pipelineRec{Pipeline: client.Pipeline{
		Name: "sv-respawn", Service: &client.Service{},
		Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "true"}},
	}, State: stateRunning, Version: 1}
	stopped := &pipelineRec{Pipeline: client.Pipeline{
		Name: "sp-stopped", Spout: &client.Spout{},
		Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "true"}},
	}, State: statePaused, Stopped: true, Version: 1}
	for _, p := range []*pipelineRec{sp, sv, stopped} {
		if err := d.savePipeline(p); err != nil {
			t.Fatal(err)
		}
	}
	d.respawnBackgroundJobs()

	// the spout and service jobs must register and stay up (blockingRunner
	// holds the service; the fake docker inspect keeps the spout polling);
	// the stopped pipeline must not spawn — three registrations would
	// mean it did. The spout's container launch (goroutine-written) must
	// land in the argv record before we assert on it.
	deadline := time.Now().Add(3 * time.Second)
	var spoutArgv []byte
	for {
		d.jobsMu.Lock()
		n := len(d.running)
		d.jobsMu.Unlock()
		if b, err := os.ReadFile(rec); err == nil && strings.Contains(string(b), "-spout") {
			spoutArgv = b
		}
		if n == 2 && spoutArgv != nil {
			break
		}
		if n > 2 {
			t.Fatalf("respawn spawned %d background jobs (stopped pipeline resurrected?), want 2", n)
		}
		if time.Now().After(deadline) {
			t.Fatalf("respawn brought up %d background jobs, want 2 (spout argv recorded: %v)", n, spoutArgv != nil)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// clean up the goroutines so the test exits promptly
	d.jobsMu.Lock()
	ids := make([]string, 0, len(d.running))
	for id := range d.running {
		ids = append(ids, id)
	}
	d.jobsMu.Unlock()
	for _, id := range ids {
		d.cancelJob(id)
	}

	// exactly one spout container was launched, with its marker env; the
	// service job ran through the runner (no docker argv) and is covered
	// by the registration count above
	lines := strings.Split(strings.TrimSpace(string(spoutArgv)), "\n")
	var spoutRuns int
	for _, l := range lines {
		if !strings.HasPrefix(l, "run ") && !strings.Contains(l, " run ") {
			continue
		}
		if strings.Contains(l, "-spout") {
			spoutRuns++
			if !strings.Contains(l, "MARKER=/sandman/marker") {
				t.Fatalf("respawned spout lost its marker env: %s", l)
			}
		}
	}
	if spoutRuns != 1 {
		t.Fatalf("respawn launched %d spout containers, want 1; argv=%s", spoutRuns, spoutArgv)
	}
}

func TestServicePreRegistersRunningHandle(t *testing.T) {
	rec := filepath.Join(t.TempDir(), "kills")
	fakeDocker(t, rec)
	// the fake process must stay alive until the cancel lands: with a
	// runner that exits immediately the job legitimately settles (and
	// unregisters its handle) before the assert can observe the
	// registration — the invariant is then misreported as a failure
	// (observed once under -race on CI)
	d := &daemon{state: t.TempDir(), runner: &blockingRunner{}, running: map[string]*runningJob{}}
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

// blockingRunner is a runner whose Run blocks until the container is
// killed: the fake service process stays up for the job's lifetime, like
// a real one, so the pre-registration invariant is observable at the
// spawn return instead of racing a process that exits immediately.
type blockingRunner struct {
	mu    sync.Mutex
	alive map[string]chan struct{}
}

func (b *blockingRunner) Run(spec JobSpec) RunResult {
	ch := make(chan struct{})
	b.mu.Lock()
	if b.alive == nil {
		b.alive = map[string]chan struct{}{}
	}
	b.alive[spec.Name] = ch
	b.mu.Unlock()
	<-ch
	return RunResult{Code: 137}
}

func (b *blockingRunner) Kill(name string) error {
	b.mu.Lock()
	ch, ok := b.alive[name]
	delete(b.alive, name)
	b.mu.Unlock()
	if ok {
		close(ch)
	}
	return nil
}
