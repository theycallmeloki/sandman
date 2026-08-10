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

var c *client.Client

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

	port := freePort()
	state := filepath.Join(os.TempDir(), fmt.Sprintf("sandman-state-%d", os.Getpid()))
	defer os.RemoveAll(state)

	cmd := exec.Command(bin, "daemon", "-port", strconv.Itoa(port), "-state", state)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "harness: daemon start failed:", err)
		os.Exit(1)
	}
	defer cmd.Process.Kill()

	if !waitPort(port, 15*time.Second) {
		fmt.Fprintln(os.Stderr, "harness: daemon did not come up")
		os.Exit(1)
	}

	c = client.New(fmt.Sprintf("127.0.0.1:%d", port))
	os.Exit(m.Run())
}

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

// uniq derives a name-unique, spec-legal identifier from the test name
// (hyphens and underscores are allowed per SB-172; slashes are not).
var uniqN int

func uniq(t *testing.T) string {
	uniqN++
	s := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, t.Name())
	return fmt.Sprintf("%s-%d", s, uniqN)
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

// copyTransform is the standard pipeline transform: copy every input file
// matched by the glob into the output directory (per SB-001).
func copyTransform(inputName string) client.Transform {
	return client.Transform{
		Cmd: []string{"sh", "-c", fmt.Sprintf("cp ${%s}/* ${OUT}/", inputName)},
	}
}
