// Package conformance is Sandman's black-box behaviour suite: one Go test
// per behaviour record (SB-NNN) from ../sandman-behaviour-notes, driving
// the system through the client package exactly as the spec describes —
// Given/When/Then against the observable surface.
//
// The suite is currently RED by design: the HTTP API the tests exercise
// does not exist yet. The tests are the contract; the interfaces phase
// implements the endpoints to green them.
package conformance

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

var (
	c          *client.Client
	daemonCmd  *exec.Cmd
	daemonPort int
	daemonName string
	binPath    string
)

func TestMain(m *testing.M) {
	bin := os.Getenv("SANMAN_BIN")
	if bin == "" {
		bin = filepath.Join(os.TempDir(), fmt.Sprintf("sandman-conformance-%d", os.Getpid()))
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
	state := filepath.Join(os.TempDir(), fmt.Sprintf("sandman-state-%d", os.Getpid()))
	defer os.RemoveAll(state)
	daemonStateDir = state
	daemonName = "conformance-" + strconv.Itoa(daemonPort)

	startDaemon(state)
	if !waitPort(daemonPort, 15*time.Second) {
		fmt.Fprintln(os.Stderr, "harness: daemon did not come up")
		os.Exit(1)
	}

	c = client.New(fmt.Sprintf("127.0.0.1:%d", daemonPort))
	code := m.Run()
	// os.Exit skips defers, so the daemon must die here or it keeps the
	// inherited stderr pipe open and go test waits out its WaitDelay.
	daemonCmd.Process.Kill()
	os.RemoveAll(state)
	os.Exit(code)
}

func startDaemon(state string) {
	cmd := exec.Command(binPath, "daemon", "-name", daemonName, "-port", strconv.Itoa(daemonPort), "-state", state)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "harness: daemon start failed:", err)
		os.Exit(1)
	}
	daemonCmd = cmd
}

// restartDaemon kills the daemon and starts a fresh one on the same port
// and state dir (SB-034, SB-035). The daemon must be fully dead before the
// new one binds the port.
func restartDaemon(t *testing.T) {
	t.Helper()
	daemonCmd.Process.Kill()
	daemonCmd.Wait()
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
	deadline := time.Now().Add(timeout)
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
// test name. Repo names become environment variable names in jobs
// (SB-096), so they must match [A-Za-z_][A-Za-z0-9_]* — docker rejects
// other characters in -e names and sh misparses hyphens in ${...}.
// Underscores stay; every other non-identifier character becomes one.
// (SB-172 covers hyphens/underscores for pipeline names separately.)
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
// failure, the signature of a panicking handler (SB-155).
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

// pollFor polls until cond returns true or the deadline passes.
func pollFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
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

// copyTransform is the standard pipeline transform: copy every input file
// matched by the glob into the output directory (per SB-001).
func copyTransform(inputName string) *client.Transform {
	return &client.Transform{
		Image: "alpine",
		Cmd:   []string{"sh", "-c", fmt.Sprintf("cp -r ${%s}/* ${OUT}/", inputName)},
	}
}
