// Package cli_test exercises the sandman binary's data-plane CLI
// end-to-end, one verb at a time, against a live container-backed daemon
// (the same binary the daemon itself is). It is a separate sequence from
// the conformance suite: conformance pins the behavior records through
// the client API; cli_test drives the actual command line the way a user
// would — create a repo, put a file, create a pipeline, get the output
// back, and check every step.
//
// The suite is docker-gated (the daemon runs its default container
// runner): without a runtime it self-skips, like the conformance
// container subset (D-23 R-4).
package cli_test

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
)

var (
	binPath   string
	addr      string
	token     = "cli-token"
	daemonCmd *exec.Cmd
)

func TestMain(m *testing.M) {
	if !dockerAvailable() {
		fmt.Fprintln(os.Stderr, "cli: no docker runtime — skipping (container carve-out)")
		os.Exit(0)
	}
	binPath = filepath.Join(os.TempDir(), fmt.Sprintf("sandman-cli-%d", os.Getpid()))
	build := exec.Command("go", "build", "-o", binPath, "..")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "cli: build failed:", err)
		os.Exit(1)
	}
	defer os.Remove(binPath)

	port := freePort()
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	state := filepath.Join(os.TempDir(), fmt.Sprintf("sandman-cli-state-%d", os.Getpid()))
	defer os.RemoveAll(state)

	// the daemon runs its default container runner: the CLI smoke flow is
	// a real end-to-end execution, not the deterministic process backend
	cmd := exec.Command(binPath, "daemon", "-name", "cli-"+strconv.Itoa(port), "-port", strconv.Itoa(port), "-state", state, "-authToken", token)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "cli: daemon start failed:", err)
		os.Exit(1)
	}
	daemonCmd = cmd
	if !waitPort(port, 20*time.Second) {
		fmt.Fprintln(os.Stderr, "cli: daemon did not come up")
		os.Exit(1)
	}

	code := m.Run()
	_ = daemonCmd.Process.Kill()
	_ = daemonCmd.Wait()
	os.RemoveAll(state)
	os.Exit(code)
}

func dockerAvailable() bool {
	if _, err := exec.LookPath("docker"); err != nil {
		return false
	}
	return exec.Command("docker", "version").Run() == nil
}

// runCLI executes the binary with the global -addr/-token flags and the
// verb args, returning stdout, stderr and the exit code.
func runCLI(t *testing.T, stdin string, args ...string) (string, string, int) {
	t.Helper()
	full := append([]string{"-addr", addr, "-token", token}, args...)
	cmd := exec.Command(binPath, full...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var out, errb strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	code := 0
	if err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("run %v: %v", args, err)
		}
		code = ee.ExitCode()
	}
	return out.String(), errb.String(), code
}

// mustCLI runs the CLI and fails the test unless it exits 0, returning
// stdout.
func mustCLI(t *testing.T, stdin string, args ...string) string {
	t.Helper()
	out, errs, code := runCLI(t, stdin, args...)
	if code != 0 {
		t.Fatalf("sandman %v: exit %d, stderr: %s", args, code, errs)
	}
	return out
}

// failCLI runs the CLI and fails the test unless it exits nonzero,
// returning stderr.
func failCLI(t *testing.T, args ...string) string {
	t.Helper()
	out, errs, code := runCLI(t, "", args...)
	if code == 0 {
		t.Fatalf("sandman %v: expected failure, got exit 0 (stdout %q)", args, out)
	}
	return errs
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

// TestCLI_SmokeFlow walks the primary user path one verb at a time:
// create a repo, put a file into it, read it back, create a pipeline
// over it, flush the input commit, and read the transform's output —
// every step through the binary's command line, plus the negative cases.
func TestCLI_SmokeFlow(t *testing.T) {
	// 1. create a repo
	out := mustCLI(t, "", "repo", "create", "r1")
	if !strings.Contains(out, "r1") {
		t.Fatalf("repo create output %q: want the repo name", out)
	}

	// 2. creating it again fails with an exists error
	errs := failCLI(t, "repo", "create", "r1")
	if !strings.Contains(errs, "already exists") {
		t.Fatalf("duplicate repo create stderr %q: want an exists error", errs)
	}

	// 3. put a file from stdin
	mustCLI(t, "hello world\n", "file", "put", "r1@master:/greeting.txt", "-")

	// 4. get it back — exact bytes
	got := mustCLI(t, "", "file", "get", "r1@master:/greeting.txt")
	if got != "hello world\n" {
		t.Fatalf("file get = %q, want %q", got, "hello world\n")
	}

	// 5. the file lists under the branch head
	l := mustCLI(t, "", "file", "list", "r1@master")
	if !strings.Contains(l, "greeting.txt") {
		t.Fatalf("file list %q: missing greeting.txt", l)
	}

	// 6. the commit history shows the branch's commit
	ch := mustCLI(t, "", "commit", "list", "r1@master")
	if !strings.Contains(ch, "master") || !strings.Contains(ch, "finished=true") {
		t.Fatalf("commit list %q: want a finished master commit", ch)
	}

	// 7. create a pipeline over the repo from a spec file
	spec := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(spec, []byte(`{
	  "name": "cap",
	  "transform": {"image": "alpine", "cmd": ["sh", "-c", "cat ${r1}/* > ${OUT}/all"]},
	  "input": {"repo": "r1", "glob": "/*"}
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	mustCLI(t, "", "pipeline", "create", "-f", spec)

	// 8. flush the input commit: the pipeline processes and settles
	f := mustCLI(t, "", "flush", "commit", "r1@master", "-timeout", "5m")
	if !strings.Contains(f, "cap") || !strings.Contains(f, "success") {
		t.Fatalf("flush output %q: want a success job for cap", f)
	}

	// 9. the job lists as success
	jl := mustCLI(t, "", "job", "list", "cap")
	if !strings.Contains(jl, "success") {
		t.Fatalf("job list %q: want a success job", jl)
	}

	// 10. the output file is the transform's result
	all := mustCLI(t, "", "file", "get", "cap@master:/all")
	if all != "hello world\n" {
		t.Fatalf("pipeline output = %q, want %q", all, "hello world\n")
	}

	// 11. pipeline inspect round-trips the name and state
	pi := mustCLI(t, "", "pipeline", "inspect", "cap")
	if !strings.Contains(pi, "cap") || !strings.Contains(pi, "state:") {
		t.Fatalf("pipeline inspect %q: want name and state", pi)
	}

	// 12. repo list shows the input and output repos
	rl := mustCLI(t, "", "repo", "list")
	for _, want := range []string{"r1", "cap"} {
		if !strings.Contains(rl, want) {
			t.Fatalf("repo list %q: missing %s", rl, want)
		}
	}

	// 13. negative: getting a missing file fails
	errs = failCLI(t, "file", "get", "r1@master:/nope")
	if !strings.Contains(errs, "not found") {
		t.Fatalf("missing file stderr %q: want a not-found error", errs)
	}

	// 14. negative: a spec without an input glob is rejected (SB-159)
	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte(`{"name": "bad", "transform": {"image": "alpine"}, "input": {"repo": "r1"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	errs = failCLI(t, "pipeline", "create", "-f", bad)
	if !strings.Contains(errs, "glob") {
		t.Fatalf("missing-glob stderr %q: want a glob error", errs)
	}

	// 15. overwrite semantics through the CLI (FS-3): the -o flag replaces
	// accumulated content at the path
	mustCLI(t, "bye\n", "file", "put", "-o", "r1@master:/greeting.txt", "-")
	if got := mustCLI(t, "", "file", "get", "r1@master:/greeting.txt"); got != "bye\n" {
		t.Fatalf("after overwrite file get = %q, want %q", got, "bye\n")
	}
}

// TestCLI_VerbCoverage walks the verbs the smoke flow does not reach,
// one at a time: inspect round-trips, the default-copy transform (no
// command), prefix file listing, and the delete verbs — each through the
// binary, self-contained (its own repos and pipeline).
func TestCLI_VerbCoverage(t *testing.T) {
	// 1. repo inspect round-trips the name
	mustCLI(t, "", "repo", "create", "r2")
	ri := mustCLI(t, "", "repo", "inspect", "r2")
	if !strings.Contains(ri, "name: r2") {
		t.Fatalf("repo inspect %q: want name: r2", ri)
	}

	// 2. put a file and list it with a path prefix
	mustCLI(t, "A", "file", "put", "r2@master:/a.txt", "-")
	fl := mustCLI(t, "", "file", "list", "r2@master:a*")
	if !strings.Contains(fl, "a.txt") {
		t.Fatalf("file list prefix %q: missing a.txt", fl)
	}

	// 3. commit inspect round-trips the repo
	cl := mustCLI(t, "", "commit", "list", "r2")
	id := strings.Fields(strings.Split(cl, "\n")[0])[0]
	ci := mustCLI(t, "", "commit", "inspect", id)
	if !strings.Contains(ci, "repo: r2") || !strings.Contains(ci, "finished: true") {
		t.Fatalf("commit inspect %q: want repo r2 finished", ci)
	}

	// 4. a pipeline with no command runs the default copy (SB-126)
	spec := filepath.Join(t.TempDir(), "spec2.json")
	if err := os.WriteFile(spec, []byte(`{
	  "name": "cap2",
	  "transform": {"image": "alpine"},
	  "input": {"repo": "r2", "glob": "/*"}
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	mustCLI(t, "", "pipeline", "create", "-f", spec)
	f := mustCLI(t, "", "flush", "commit", "r2@master", "-timeout", "5m")
	if !strings.Contains(f, "cap2") || !strings.Contains(f, "success") {
		t.Fatalf("flush output %q: want cap2 success", f)
	}
	if got := mustCLI(t, "", "file", "get", "cap2@master:/a.txt"); got != "A" {
		t.Fatalf("default-copy output = %q, want %q", got, "A")
	}

	// 5. job inspect round-trips the success state
	jl := mustCLI(t, "", "job", "list", "cap2")
	jid := strings.Fields(strings.Split(jl, "\n")[0])[0]
	ji := mustCLI(t, "", "job", "inspect", jid)
	if !strings.Contains(ji, "state: success") {
		t.Fatalf("job inspect %q: want state: success", ji)
	}

	// 6. pipeline delete removes it from the listing
	mustCLI(t, "", "pipeline", "delete", "cap2")
	pl := mustCLI(t, "", "pipeline", "list")
	if strings.Contains(pl, "cap2") {
		t.Fatalf("pipeline list %q after delete: cap2 still present", pl)
	}

	// 7. repo delete removes it from the listing
	mustCLI(t, "", "repo", "delete", "r2")
	rl := mustCLI(t, "", "repo", "list")
	if strings.Contains(rl, "r2") {
		t.Fatalf("repo list %q after delete: r2 still present", rl)
	}
}
