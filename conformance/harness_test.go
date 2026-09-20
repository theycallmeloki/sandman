// Package conformance is Sandman's black-box behaviour suite: one Go test
// per behaviour record, driving the system through the client package
// exactly as the spec describes — Given/When/Then against the observable
// surface.
//
// The suite is green: the HTTP API the tests exercise is implemented by
// the daemon, and these tests are the contract that keeps it that way.
package conformance

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"sandman/client"
)

// The full suite runs ~16 minutes (cron cadence waits, container daemons,
// spout cycles): go test's default 10m package timeout kills it mid-run
// (a panic with no failing test). Run locally with -timeout 40m, matching
// the CI workflow's conformance shard budget.
var (
	c          *testClient
	daemonCmd  *exec.Cmd
	daemonPort int
	daemonName string
	binPath    string
)

func TestMain(m *testing.M) {
	bin := os.Getenv("SANMAN_BIN")
	if bin == "" {
		bin = filepath.Join(testTempBase(), fmt.Sprintf("sandman-conformance-%d", os.Getpid()))
		build := exec.Command("go", "build", "-o", bin, ".")
		build.Dir = ".." // package dir is conformance/; the binary lives at the repo root
		build.Stderr = os.Stderr
		if err := build.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "harness: build failed (set SANMAN_BIN to skip):", err)
			os.Exit(1)
		}
		defer os.Remove(bin)
	}
	binPath = bin

	daemonPort = freePort()
	state := filepath.Join(testTempBase(), fmt.Sprintf("sandman-state-%d", os.Getpid()))
	defer os.RemoveAll(state)
	daemonStateDir = state
	daemonName = "conformance-" + strconv.Itoa(daemonPort)

	// Orphaned daemons/workers from interrupted runs (a test-timeout
	// SIGKILL kills the test binary, not its children; the kernel
	// reparents the children to init) hold the external ports and
	// poison later runs. The harness binary path sandman-conformance-
	// <pid> appears in every child's argv, so pgrep -f finds them; only
	// processes whose parent is DEAD (PPID 1) are orphans — a live
	// concurrent suite's daemons keep their parent and must never be
	// killed (a broad pkill here would SIGTERM a sibling suite's daemon:
	// daemon.go exits silently on SIGTERM). This must run BEFORE
	// startDaemon or we kill our own daemon.
	if out, err := exec.Command("pgrep", "-f", "sandman-conformance-").Output(); err == nil {
		for _, pid := range strings.Fields(string(out)) {
			if ppid := procPPID(pid); ppid == 1 || ppid == 0 {
				exec.Command("kill", pid).Run()
			}
		}
	}
	// Stale sandman containers from interrupted runs (a SIGKILLed daemon
	// cannot run its docker rm -f) hold external ports and poison later
	// runs. Scoped to the harness's own naming namespace
	// (sandman-conformance-*): the node label is an exact per-daemon
	// match, so it would miss leftovers from earlier ports, while an
	// unscoped name=sandman- sweep SIGKILLs foreign production services
	// on a shared dockerd (sandman-<id>-service — observed live:
	// "service process exited with code 137").
	if dockerAvailable() {
		if out, err := exec.Command("docker", "ps", "-aq", "--filter", "name=sandman-conformance-").Output(); err == nil {
			for _, id := range strings.Fields(string(out)) {
				exec.Command("docker", "rm", "-f", id).Run()
			}
		}
	}

	startDaemon(state)
	if !waitPort(daemonPort, 15*time.Second) {
		// the daemon is up but never bound its port: kill it before
		// exiting — os.Exit skips the suite's teardown, and a daemon left
		// behind outlives the run
		if daemonCmd != nil && daemonCmd.Process != nil {
			_ = daemonCmd.Process.Kill()
		}
		fmt.Fprintln(os.Stderr, "harness: daemon did not come up")
		os.Exit(1)
	}

	c = &testClient{client.New(fmt.Sprintf("127.0.0.1:%d", daemonPort))}
	code := m.Run()
	// os.Exit skips defers, so the daemon must die here or it keeps the
	// inherited stderr pipe open and go test waits out its WaitDelay.
	// Graceful first: a SIGKILLed daemon strands running spout
	// containers, orphaned with the daemon's stale label — they poison
	// later tests' container assertions (spouts are left mid-cycle at
	// teardown by design).
	stopDaemon()
	// belt and braces: whatever the daemon's exit left behind (a spout
	// straggler whose cleanup raced the process exit), remove it now —
	// the same scoped sweep the next run's startup would do, so a
	// batch's later tests never see a stale container.
	if dockerAvailable() {
		if out, err := exec.Command("docker", "ps", "-aq", "--filter", "name=sandman-conformance-").Output(); err == nil {
			for _, id := range strings.Fields(string(out)) {
				exec.Command("docker", "rm", "-f", id).Run()
			}
		}
	}
	os.RemoveAll(state)
	os.Exit(code)
}

// stopDaemon ends the harness daemon gracefully: SIGTERM runs the
// daemon's shutdown envelope (cancel in-flight jobs, kill their
// containers, bounded settle), then a bounded wait; SIGKILL only as a
// fallback. The daemon must be fully dead before a new one binds the
// port.
func stopDaemon() {
	if daemonCmd == nil || daemonCmd.Process == nil {
		return
	}
	_ = daemonCmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { _ = daemonCmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(testTimeout(20 * time.Second)):
		_ = daemonCmd.Process.Kill()
		<-done
	}
}

func startDaemon(state string) {
	// the matrix runs on the process backend: deterministic,
	// no container runtime required; the container-facing subset spins
	// its own container daemon (TestStandbyIdlesWithZeroContainers TestStandbyIdlesWithZeroContainers)
	cmd := exec.Command(binPath, "daemon", "-name", daemonName, "-port", strconv.Itoa(daemonPort), "-state", state, "-runner", "process")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "harness: daemon start failed:", err)
		os.Exit(1)
	}
	daemonCmd = cmd
}

// dockerAvailable reports whether the container runtime is present (the
// container-facing subset runs only when it is).
func dockerAvailable() bool {
	if _, err := exec.LookPath("docker"); err != nil {
		return false
	}
	return exec.Command("docker", "version").Run() == nil
}

// procPPID reads a process's parent pid. /proc is the Linux fast path;
// elsewhere it is read via ps. Reading /proc unconditionally made the
// orphan sweep a silent no-op off Linux — a daemon leaked by an interrupted
// run was never reclaimed on darwin, which is exactly what the sweep exists
// to prevent.
func procPPID(pid string) int {
	if b, err := os.ReadFile("/proc/" + pid + "/stat"); err == nil {
		// stat layout: pid (comm) state ppid ... — comm may contain spaces
		// and parens, so split after the LAST ')'.
		s := string(b)
		i := strings.LastIndexByte(s, ')')
		f := strings.Fields(s[i+1:])
		if len(f) < 2 {
			return -1
		}
		n, err := strconv.Atoi(f[1])
		if err != nil {
			return -1
		}
		return n
	}
	out, err := exec.Command("ps", "-o", "ppid=", "-p", pid).Output()
	if err != nil {
		return -1
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return -1
	}
	return n
}

// restartDaemon kills the daemon and starts a fresh one on the same port
// and state dir. The daemon must be fully dead before the
// new one binds the port.
func restartDaemon(t *testing.T) {
	t.Helper()
	// restart simulates a CRASH: SIGKILL, no graceful cleanup — the
	// tests assert the crash-recovery paths (a mid-flight job's record
	// is marked failed by the next daemon). The graceful stop
	// (stopDaemon) is only for the final teardown.
	_ = daemonCmd.Process.Kill()
	_ = daemonCmd.Wait()
	startDaemon(daemonStateDir)
	if !waitPort(daemonPort, 15*time.Second) {
		t.Fatal("daemon did not come back up after restart")
	}
}

// daemonStateDir is the daemon's state dir, kept for restart tests.
var daemonStateDir string

func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitPort(port int, timeout time.Duration) bool {
	deadline := time.Now().Add(testTimeout(timeout))
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// uniq derives a name-unique, shell-identifier-safe identifier from the
// test name. Repo names become environment variable names in jobs, so
// they must match [A-Za-z_][A-Za-z0-9_]* — docker rejects
// other characters in -e names and sh misparses hyphens in ${...}.
// Underscores stay; every other non-identifier character becomes one.
// (Hyphens/underscores for pipeline names are covered separately.)
var uniqN int

func uniq(t *testing.T) string {
	uniqN++
	s := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			return r
		default:
			return '_'
		}
	}, t.Name())
	if s == "" || (s[0] >= '0' && s[0] <= '9') {
		s = "sb_" + s
	}
	return fmt.Sprintf("%s_%d", s, uniqN)
}

// ---- Given helpers ----

func mustRepo(t *testing.T, name string) {
	t.Helper()
	if err := c.CreateRepo(name); err != nil {
		t.Fatalf("create repo %s: %v", name, err)
	}
}

// commitFiles starts a commit on the repo, puts the files, finishes it.
func commitFiles(t *testing.T, repo, branch string, files map[string]string) client.Commit {
	t.Helper()
	cm, err := c.StartCommit(repo, branch, "")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	for p, content := range files {
		if err := c.PutFile(cm.ID, p, []byte(content)); err != nil {
			t.Fatalf("put file %s: %v", p, err)
		}
	}
	fin, err := c.FinishCommit(cm.ID, "", false)
	if err != nil {
		t.Fatalf("finish commit: %v", err)
	}
	return fin
}

// replaceCommit commits each path as a replacement: tombstoned then
// re-written in the same commit, so the new content replaces the old
// (a a plain put would append to the accumulated content.
// Deleting a path that does not exist is a no-op, so new paths work too.
func replaceCommit(t *testing.T, repo, branch string, files map[string]string) client.Commit {
	t.Helper()
	cm, err := c.StartCommit(repo, branch, "")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	for p := range files {
		if err := c.DeleteFile(cm.ID, p); err != nil {
			t.Fatalf("delete %s: %v", p, err)
		}
	}
	for p, content := range files {
		if err := c.PutFile(cm.ID, p, []byte(content)); err != nil {
			t.Fatalf("put file %s: %v", p, err)
		}
	}
	fin, err := c.FinishCommit(cm.ID, "", false)
	if err != nil {
		t.Fatalf("finish commit: %v", err)
	}
	return fin
}

// overwriteCommit commits files with explicit overwrite semantics:
// each path's accumulated content is replaced, not appended to.
func overwriteCommit(t *testing.T, repo, branch string, files map[string]string) client.Commit {
	t.Helper()
	cm, err := c.StartCommit(repo, branch, "")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	for p, content := range files {
		if err := c.PutFile(cm.ID, p, []byte(content)); err != nil {
			t.Fatalf("put %s: %v", p, err)
		}
	}
	fin, err := c.FinishCommit(cm.ID, "", false)
	if err != nil {
		t.Fatalf("finish commit: %v", err)
	}
	return fin
}

func mustPipeline(t *testing.T, p client.Pipeline) {
	t.Helper()
	if err := c.CreatePipeline(p); err != nil {
		t.Fatalf("create pipeline %s: %v", p.Name, err)
	}
}

// flushOK flushes the commit and requires every triggered job to succeed.
func flushOK(t *testing.T, commitID string) []client.Job {
	t.Helper()
	jobs, err := c.Flush(commitID, 60*time.Second)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	for _, j := range jobs {
		if j.State != "success" {
			t.Fatalf("job %s (%s) state = %s, want success (reason %q)", j.ID, j.Pipeline, j.State, j.Reason)
		}
	}
	return jobs
}

// wantErr asserts err is non-nil and its message contains substr.
func wantErr(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", substr)
	}
	if !strings.Contains(err.Error(), substr) {
		t.Fatalf("error %q does not contain %q", err.Error(), substr)
	}
}

// noPanic asserts the call produced a well-formed HTTP response — a
// *client.Error (any status) or nil. Anything else is a transport-level
// failure, the signature of a panicking handler.
func noPanic(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	var ce *client.Error
	if errors.As(err, &ce) {
		return
	}
	t.Fatalf("transport error (possible panic in handler): %v", err)
}

// testTempBase is the parent directory for the suite's scratch state (the
// daemon's state dir, the built binary, backup fixtures). $SANDMAN_TEST_TMP
// overrides it: on macOS the default (/var/folders/…) is shared by Docker
// Desktop but not by colima, whose VM mounts $HOME only — the container-gated
// tests then fail with empty bind mounts (the transform sees no input) rather
// than with an obvious error. A colima user points this somewhere the VM
// shares, e.g. SANDMAN_TEST_TMP=$HOME/.cache/sandman-tests.
func testTempBase() string {
	if v := os.Getenv("SANDMAN_TEST_TMP"); v != "" {
		return v
	}
	return os.TempDir()
}

// testTimeout scales a wait budget by $SANDMAN_TEST_TIMEOUT_FACTOR (default
// 1). The budgets below are tuned for a native Linux runner; a container
// runtime behind a VM (Docker Desktop on macOS) starts and execs slower under
// load, and an overrun looks like a product failure — "timed out waiting for
// spout commits" — rather than a slow host.
func testTimeout(d time.Duration) time.Duration {
	f := 1.0
	if v := os.Getenv("SANDMAN_TEST_TIMEOUT_FACTOR"); v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil && parsed > 0 {
			f = parsed
		}
	}
	return time.Duration(float64(d) * f)
}

// testClient is the harness's client: it scales the blocking waits whose
// deadline travels to the server in the request — a flush, a job wait, a
// file fetch — so $SANDMAN_TEST_TIMEOUT_FACTOR applies to every call site
// without editing the ~60 of them, and to the server-side wait, which is
// the one that overruns first when the container runtime is behind a VM.
// Callers pass an unscaled budget (60*time.Second); the wrapper is the one
// place it is scaled.
type testClient struct{ *client.Client }

func (tc *testClient) Flush(commitID string, timeout time.Duration) ([]client.Job, error) {
	return tc.Client.Flush(commitID, testTimeout(timeout))
}

func (tc *testClient) FlushSet(commitIDs []string, timeout time.Duration) ([]client.Job, error) {
	return tc.Client.FlushSet(commitIDs, testTimeout(timeout))
}

func (tc *testClient) WaitJob(jobID string, timeout time.Duration) (client.Job, error) {
	return tc.Client.WaitJob(jobID, testTimeout(timeout))
}

func (tc *testClient) FetchFileTo(w io.Writer, commitID, p string, download bool, timeout time.Duration) (client.FileFetch, error) {
	return tc.Client.FetchFileTo(w, commitID, p, download, testTimeout(timeout))
}

// pollFor waits for a condition, failing the test with the budget it was
// given: the budget is scaled by testTimeout so a slower runtime is a
// configuration change, not a rewrite of every call site.
func pollFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout(timeout))
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// waitJobFor polls until a job for the pipeline exists and returns the
// first terminal-or-running job found.
func waitJobFor(t *testing.T, pipeline string, timeout time.Duration) client.Job {
	t.Helper()
	var found client.Job
	pollFor(t, "job of pipeline "+pipeline, timeout, func() bool {
		jobs, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipeline})
		if err != nil || len(jobs) == 0 {
			return false
		}
		found = jobs[0]
		return true
	})
	return found
}

// withIsolatedDaemon swaps the harness client for a fresh process-backed
// daemon on its own state dir, restored to the shared daemon on cleanup.
// Tests that reset the daemon (or corrupt its state) run under it: a
// mid-suite Reset on the shared daemon wipes every test's state and
// makes correctness depend on alphabetical execution order. The
// shared daemon keeps running untouched on its own port.
func withIsolatedDaemon(t *testing.T) {
	t.Helper()
	state := filepath.Join(testTempBase(), "sandman-isolated-"+uniq(t))
	os.MkdirAll(state, 0o755)
	port := freePort()
	cmd := exec.Command(binPath, "daemon", "-name", daemonName, "-port", strconv.Itoa(port), "-state", state, "-runner", "process")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start isolated daemon: %v", err)
	}
	if !waitPort(port, 15*time.Second) {
		_ = cmd.Process.Kill() // the child started; failing here must not leak it
		t.Fatalf("isolated daemon did not come up")
	}
	oldC, oldPort, oldState := c, daemonPort, daemonStateDir
	c = &testClient{client.New(fmt.Sprintf("127.0.0.1:%d", port))}
	daemonPort = port
	daemonStateDir = state
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(testTimeout(20 * time.Second)):
			_ = cmd.Process.Kill()
			<-done
		}
		os.RemoveAll(state)
		c, daemonPort, daemonStateDir = oldC, oldPort, oldState
	})
}

// cleanupPipeline registers deletion of the pipeline when the test ends.
// A cron pipeline left on the shared daemon ticks for the whole suite,
// spawning background jobs that pollute GC, metrics, and job-count
// assertions; force covers a downstream consuming the output repo.
func cleanupPipeline(t *testing.T, name string) {
	t.Helper()
	t.Cleanup(func() { noPanic(t, c.DeletePipeline(name, true, false)) })
}

// copyTransform is the standard pipeline transform: copy every input file
// matched by the glob into the output directory.
func copyTransform(inputName string) *client.Transform {
	return &client.Transform{
		Image: "alpine:3.21",
		Cmd:   []string{"sh", "-c", fmt.Sprintf("cp -r ${%s}/* ${OUT}/", inputName)},
	}
}
