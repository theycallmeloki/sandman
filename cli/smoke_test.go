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
// container subset.
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
	cmd := exec.Command(binPath, "daemon", "-name", "cli-"+strconv.Itoa(port), "-port", strconv.Itoa(port), "-state", state)
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

// runCLI executes the binary with the global -addr flag and the
// verb args, returning stdout, stderr and the exit code.
func runCLI(t *testing.T, stdin string, args ...string) (string, string, int) {
	t.Helper()
	full := append([]string{"-addr", addr}, args...)
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
	if !strings.Contains(ch, "master") || !strings.Contains(ch, "FINISHED") {
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
	f := mustCLI(t, "", "flush", "commit", "r1@master", "--timeout", "5m")
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
	if !strings.Contains(pi, "cap") || !strings.Contains(pi, "state") {
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

	// 14. negative: a spec without an input glob is rejected
	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte(`{"name": "bad", "transform": {"image": "alpine"}, "input": {"repo": "r1"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	errs = failCLI(t, "pipeline", "create", "-f", bad)
	if !strings.Contains(errs, "glob") {
		t.Fatalf("missing-glob stderr %q: want a glob error", errs)
	}

	// 15. overwrite semantics through the CLI: the -o flag replaces
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
	if !strings.Contains(ri, "name") || !strings.Contains(ri, "r2") {
		t.Fatalf("repo inspect %q: want name r2", ri)
	}

	// 2. put a file and list it with a path prefix
	mustCLI(t, "A", "file", "put", "r2@master:/a.txt", "-")
	fl := mustCLI(t, "", "file", "list", "r2@master:a*")
	if !strings.Contains(fl, "a.txt") {
		t.Fatalf("file list prefix %q: missing a.txt", fl)
	}

	// 3. commit inspect round-trips the repo
	cl := mustCLI(t, "", "commit", "list", "r2")
	id := strings.Fields(strings.Split(cl, "\n")[1])[0]
	ci := mustCLI(t, "", "commit", "inspect", id)
	if !strings.Contains(ci, "repo") || !strings.Contains(ci, "r2") || !strings.Contains(ci, "finished") {
		t.Fatalf("commit inspect %q: want repo r2 finished", ci)
	}

	// 4. a pipeline with no command runs the default copy
	spec := filepath.Join(t.TempDir(), "spec2.json")
	if err := os.WriteFile(spec, []byte(`{
	  "name": "cap2",
	  "transform": {"image": "alpine"},
	  "input": {"repo": "r2", "glob": "/*"}
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	mustCLI(t, "", "pipeline", "create", "-f", spec)
	f := mustCLI(t, "", "flush", "commit", "r2@master", "--timeout", "5m")
	if !strings.Contains(f, "cap2") || !strings.Contains(f, "success") {
		t.Fatalf("flush output %q: want cap2 success", f)
	}
	if got := mustCLI(t, "", "file", "get", "cap2@master:/a.txt"); got != "A" {
		t.Fatalf("default-copy output = %q, want %q", got, "A")
	}

	// 4b. file inspect reports the size
	fi := mustCLI(t, "", "file", "inspect", "r2@master:/a.txt")
	if !strings.Contains(fi, "path") || !strings.Contains(fi, "size") || !strings.Contains(fi, "1 B") {
		t.Fatalf("file inspect %q: want path and size", fi)
	}

	// 4c. pipeline extract emits a round-trippable spec (update -f accepts it)
	ex := mustCLI(t, "", "pipeline", "extract", "cap2")
	if !strings.Contains(ex, `"name": "cap2"`) {
		t.Fatalf("pipeline extract %q: want cap2 spec JSON", ex)
	}
	spec3 := filepath.Join(t.TempDir(), "spec3.json")
	if err := os.WriteFile(spec3, []byte(ex), 0o644); err != nil {
		t.Fatal(err)
	}
	mustCLI(t, "", "pipeline", "update", "-f", spec3)

	// 4d. pipeline edit re-submits through $EDITOR
	t.Setenv("EDITOR", "true")
	mustCLI(t, "", "pipeline", "edit", "cap2")

	// 5. job inspect round-trips the success state
	jl := mustCLI(t, "", "job", "list", "cap2")
	jid := strings.Fields(strings.Split(jl, "\n")[1])[0]
	ji := pollCLI(t, func(out string) bool {
		return strings.Contains(out, "state") && strings.Contains(out, "success")
	}, 60*time.Second, "job", "inspect", jid)
	if !strings.Contains(ji, "state") || !strings.Contains(ji, "success") {
		t.Fatalf("job inspect %q: want state: success", ji)
	}
	// 5b. job inspect prints the per-datum aggregate counts. The
	// newest cap2 job is the update-triggered
	// re-run: its single datum is unchanged from the flush's successful
	// run, so dedup skips it — skipped 1, processed 0 — and
	// statsCommit stays absent (stats disabled here)
	norm := strings.Join(strings.Fields(ji), " ")
	if !strings.Contains(norm, "processed : 0") || !strings.Contains(norm, "failed : 0") || !strings.Contains(norm, "skipped : 1") {
		t.Fatalf("job inspect %q: want aggregate counts processed 0 / failed 0 / skipped 1 (dedup)", ji)
	}
	if strings.Contains(ji, "statsCommit") {
		t.Fatalf("job inspect %q: statsCommit printed without stats enabled", ji)
	}

	// 6. a downstream consumer refuses a plain delete and the error names
	// --force; --force deletes the mid-DAG pipeline anyway, leaving the
	// consumer with a dangling input — the CLI advertised the
	// flag before it existed, dead-ending the delete
	downSpec := filepath.Join(t.TempDir(), "down.json")
	if err := os.WriteFile(downSpec, []byte(`{
	  "name": "cap2down",
	  "transform": {"image": "alpine"},
	  "input": {"repo": "cap2", "glob": "/*"}
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	mustCLI(t, "", "pipeline", "create", "-f", downSpec)
	if errs := failCLI(t, "pipeline", "delete", "cap2"); !strings.Contains(errs, "downstream") {
		t.Fatalf("pipeline delete cap2 stderr %q: want downstream-consumers refusal", errs)
	}
	mustCLI(t, "", "pipeline", "delete", "cap2", "--force")
	pl := mustCLI(t, "", "pipeline", "list")
	if strings.Contains(pl, "cap2 ") { // padded name: cap2down must not match
		t.Fatalf("pipeline list %q after forced delete: cap2 still present", pl)
	}
	// cleanup: the dangling consumer is a leaf now
	mustCLI(t, "", "pipeline", "delete", "cap2down")

	// 6b. transaction resume stages subsequent creates into the active tx
	// (XDG_CONFIG_HOME isolates the active-tx marker)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	tid := strings.TrimSpace(mustCLI(t, "", "transaction", "start"))
	mustCLI(t, "", "transaction", "resume", tid)
	spec4 := filepath.Join(t.TempDir(), "spec4.json")
	if err := os.WriteFile(spec4, []byte(`{
	  "name": "cap3",
	  "transform": {"image": "alpine"},
	  "input": {"repo": "r2", "glob": "/*"}
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	mustCLI(t, "", "pipeline", "create", "-f", spec4)
	ti := mustCLI(t, "", "transaction", "inspect", tid)
	if !strings.Contains(ti, "create cap3") {
		t.Fatalf("transaction inspect %q: want the staged create cap3", ti)
	}
	tl := mustCLI(t, "", "transaction", "list")
	if !strings.Contains(tl, tid) {
		t.Fatalf("transaction list %q: missing %s", tl, tid)
	}
	mustCLI(t, "", "transaction", "finish", tid)
	pl2 := mustCLI(t, "", "pipeline", "list")
	if !strings.Contains(pl2, "cap3") {
		t.Fatalf("pipeline list %q after finish: cap3 missing", pl2)
	}
	mustCLI(t, "", "transaction", "stop")

	// 6b2. pipeline update --tx stages the update explicitly — the flag
	// existed only on create; the update handler read txID but the flag
	// was unreachable from the CLI (dead code), so only the resume flow
	// exercised update-in-transaction
	tid2 := strings.TrimSpace(mustCLI(t, "", "transaction", "start"))
	spec5 := filepath.Join(t.TempDir(), "spec5.json")
	if err := os.WriteFile(spec5, []byte(`{
	  "name": "cap3",
	  "description": "updated in tx",
	  "transform": {"image": "alpine"},
	  "input": {"repo": "r2", "glob": "/*"}
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	mustCLI(t, "", "pipeline", "update", "--tx", tid2, "-f", spec5)
	ti2 := mustCLI(t, "", "transaction", "inspect", tid2)
	if !strings.Contains(ti2, "update cap3") {
		t.Fatalf("transaction inspect %q: want the staged update cap3", ti2)
	}
	mustCLI(t, "", "transaction", "finish", tid2)
	pi2 := mustCLI(t, "", "pipeline", "inspect", "cap3")
	if !strings.Contains(pi2, "version") || !strings.Contains(pi2, "2") {
		t.Fatalf("pipeline inspect %q after tx update: want version 2", pi2)
	}

	// 6c. top-level `get file` recovers a file by ref (the nested `file
	// get` is equivalent); commit inspect accepts repo@branch
	if got := mustCLI(t, "", "get", "file", "r2@master:/a.txt"); got != "A" {
		t.Fatalf("get file r2@master:/a.txt = %q, want A", got)
	}
	ci2 := mustCLI(t, "", "commit", "inspect", "r2@master")
	if !strings.Contains(ci2, "repo") || !strings.Contains(ci2, "r2") {
		t.Fatalf("commit inspect r2@master %q: want repo r2", ci2)
	}

	// 6c2. the canonical explicit-commit flow (F14): commit start + file
	// put by commit id + commit finish. The put's @ segment must address
	// the open commit — a commit-id ref used to materialize a phantom
	// branch of the same name, silently finishing the started commit
	// empty and stranding the data on the phantom.
	fid := strings.TrimSpace(mustCLI(t, "", "commit", "start", "r2@master"))
	if got := mustCLI(t, "B\n", "file", "put", "r2@"+fid+":/b.txt", "-"); !strings.Contains(got, "commit "+fid) {
		t.Fatalf("put by commit id %q: want the same commit id %s (no new commit)", got, fid)
	}
	mustCLI(t, "", "commit", "finish", fid)
	fc := mustCLI(t, "", "commit", "inspect", "r2@"+fid)
	if !strings.Contains(strings.Join(strings.Fields(fc), " "), "finished : true") {
		t.Fatalf("commit inspect %s: want finished", fc)
	}
	if got := mustCLI(t, "", "file", "get", "r2@"+fid+":/b.txt"); got != "B\n" {
		t.Fatalf("get by commit id = %q, want B", got)
	}
	bl := mustCLI(t, "", "branch", "list", "r2")
	for _, line := range strings.Split(bl, "\n")[1:] {
		f := strings.Fields(line)
		if len(f) > 0 && f[0] == fid {
			t.Fatalf("branch list %q: phantom branch %s materialized", bl, fid)
		}
	}
	// the put's finish is explicit: a second put into the now-finished
	// commit errors instead of silently starting a phantom
	errs := failCLI(t, "file", "put", "r2@"+fid+":/c.txt", "-")
	if !strings.Contains(errs, "finished") {
		t.Fatalf("put into a finished commit: stderr %q, want a finished error", errs)
	}

	// 6d. file put into a missing repo errors (no silent auto-create)
	errs = failCLI(t, "file", "put", "no-such-repo@master:x", "-")
	if !strings.Contains(errs, "not found") {
		t.Fatalf("file put into a missing repo: stderr %q, want not-found", errs)
	}

	// 6d2. unknown spec fields are rejected, not silently ignored — a
	// ported spec carrying top-level resource_limits must fail
	// loudly (the pre-fix decode dropped the declaration and ran the
	// container with no limits, no error)
	badSpec := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(badSpec, []byte(`{
	  "resourceLimits": {"memory": "64M", "cpu": 0.5},
	  "name": "capbad",
	  "transform": {"image": "alpine"},
	  "input": {"repo": "r2", "glob": "/*"}
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	errs3 := failCLI(t, "pipeline", "create", "-f", badSpec)
	if !strings.Contains(errs3, "resourceLimits") {
		t.Fatalf("pipeline create with unknown field: stderr %q, want the field named", errs3)
	}

	// 6e. tag delete drops the ref
	mustCLI(t, "payload", "tag", "put", "smoketag", "-")
	mustCLI(t, "", "tag", "delete", "smoketag")
	errs2 := failCLI(t, "tag", "get", "smoketag")
	if !strings.Contains(errs2, "not found") {
		t.Fatalf("tag get after delete: stderr %q, want not-found", errs2)
	}

	// 7. repo delete removes it from the listing
	mustCLI(t, "", "repo", "delete", "r2")
	rl := mustCLI(t, "", "repo", "list")
	if strings.Contains(rl, "r2") {
		t.Fatalf("repo list %q after delete: r2 still present", rl)
	}
}

// pollCLI repeatedly runs a CLI verb until pred holds on its stdout or the
// deadline passes — the CLI sequence's own polling helper (separate
// scaffolding from the conformance matrix).
func pollCLI(t *testing.T, pred func(string) bool, timeout time.Duration, args ...string) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		out, errs, code := runCLI(t, "", args...)
		if code != 0 {
			t.Fatalf("sandman %v: exit %d, stderr: %s", args, code, errs)
		}
		if pred(out) {
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("sandman %v: condition not met within %v (last output %q)", args, timeout, out)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// commitIDFromPut parses the commit id out of `file put`'s
// "wrote ... (N bytes, commit <id>)" line.
func commitIDFromPut(t *testing.T, out string) string {
	t.Helper()
	idx := strings.Index(out, "commit ")
	if idx < 0 {
		t.Fatalf("put output %q: no commit id", out)
	}
	id := strings.TrimSuffix(out[idx+len("commit "):], ")\n")
	if id == "" {
		t.Fatalf("put output %q: empty commit id", out)
	}
	return id
}

// jobRow parses one line of `job list` output (id pipeline state).
type jobRow struct {
	id, pipeline, state string
}

func jobRows(out string) []jobRow {
	var rows []jobRow
	for i, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if i == 0 {
			continue // the table header row
		}
		f := strings.Fields(line)
		if len(f) == 3 {
			rows = append(rows, jobRow{f[0], f[1], f[2]})
		}
	}
	return rows
}

// TestCLI_WrapperCoverage walks the wrapper verbs one at a time: secret,
// tag, branch, commit start/finish/delete, file copy/delete, check, job
// stop/delete, datum list/inspect/restart, pipeline update/stop/start/
// run/run-cron, transaction, logs, and pipeline-create-in-transaction —
// each a thin CLI over a conformance-verified client method (CLI_SURFACE).
func TestCLI_WrapperCoverage(t *testing.T) {
	// 1. check — the fsck analog
	if out := mustCLI(t, "", "check"); !strings.Contains(out, "ok") {
		t.Fatalf("check output %q: want ok", out)
	}

	// 2. secret create/inspect/list/delete
	mustCLI(t, `{"key":"v1","k2":"v2"}`, "secret", "create", "s1", "-")
	si := mustCLI(t, "", "secret", "inspect", "s1")
	if !strings.Contains(si, "s1") {
		t.Fatalf("secret inspect %q: want the secret name", si)
	}
	if sl := mustCLI(t, "", "secret", "list"); !strings.Contains(sl, "s1") {
		t.Fatalf("secret list %q: missing s1", sl)
	}
	mustCLI(t, "", "secret", "delete", "s1")
	if sl := mustCLI(t, "", "secret", "list"); strings.Contains(sl, "s1") {
		t.Fatalf("secret list %q after delete: s1 still present", sl)
	}

	// 3. tag put/get/list
	mustCLI(t, "tagbytes", "tag", "put", "t1", "-")
	if got := mustCLI(t, "", "tag", "get", "t1"); got != "tagbytes" {
		t.Fatalf("tag get = %q, want %q", got, "tagbytes")
	}
	if tl := mustCLI(t, "", "tag", "list"); !strings.Contains(tl, "t1") {
		t.Fatalf("tag list %q: missing t1", tl)
	}

	// 4. repo + three files; remember b.txt's commit for the delete test
	mustCLI(t, "", "repo", "create", "rw")
	mustCLI(t, "A", "file", "put", "rw@master:/a.txt", "-")
	bput := mustCLI(t, "B", "file", "put", "rw@master:/b.txt", "-")
	bid := commitIDFromPut(t, bput)
	mustCLI(t, "C", "file", "put", "rw@master:/c.txt", "-")

	// 5. branch create (defaults the head to master) + branch list
	mustCLI(t, "", "branch", "create", "rw", "side")
	if bl := mustCLI(t, "", "branch", "list"); !strings.Contains(bl, "side") {
		t.Fatalf("branch list %q: missing side", bl)
	}

	// 6. direct commit start/finish on the side branch
	cid := strings.TrimSpace(mustCLI(t, "", "commit", "start", "rw@side"))
	if cid == "" {
		t.Fatal("commit start: empty id")
	}
	mustCLI(t, "", "commit", "finish", cid)
	if cl := mustCLI(t, "", "commit", "list", "rw@side"); !strings.Contains(cl, "FINISHED") {
		t.Fatalf("commit list rw@side %q: want a finished commit", cl)
	}

	// 7. file copy (into a fresh commit on master)
	mustCLI(t, "", "file", "copy", "rw@master:/b.txt", "rw@master:/copied.txt")
	if got := mustCLI(t, "", "file", "get", "rw@master:/copied.txt"); got != "B" {
		t.Fatalf("after copy file get = %q, want %q", got, "B")
	}

	// 8. file delete (tombstone in a fresh commit)
	mustCLI(t, "", "file", "delete", "rw@master:/c.txt")
	if errs := failCLI(t, "file", "get", "rw@master:/c.txt"); !strings.Contains(errs, "not found") {
		t.Fatalf("deleted file stderr %q: want a not-found error", errs)
	}

	// 9. pipeline create (schedules the head job) + flush + output
	spec := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(spec, []byte(`{
	  "name": "capw",
	  "transform": {"image": "alpine", "cmd": ["sh", "-c", "cat ${rw}/* > ${OUT}/all"]},
	  "input": {"repo": "rw", "glob": "/*"},
	  "enableStats": true
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	mustCLI(t, "", "pipeline", "create", "-f", spec)
	f := mustCLI(t, "", "flush", "commit", "rw@master", "--timeout", "5m")
	if !strings.Contains(f, "capw") || !strings.Contains(f, "success") {
		t.Fatalf("flush output %q: want a capw success job", f)
	}
	if got := mustCLI(t, "", "file", "get", "capw@master:/all"); got != "ABB" {
		t.Fatalf("pipeline output = %q, want %q", got, "ABB")
	}
	jl := mustCLI(t, "", "job", "list", "capw")
	rows := jobRows(jl)
	if len(rows) != 1 {
		t.Fatalf("job list capw: %d jobs, want 1", len(rows))
	}
	v1 := rows[0].id

	// 10. datum list/inspect on the settled job (3 files -> 3 datums)
	dl := mustCLI(t, "", "datum", "list", v1)
	if n := len(strings.Split(strings.TrimSpace(dl), "\n")); n != 4 {
		t.Fatalf("datum list: %d lines (header + 3 datums), want 4 (%q)", n, dl)
	}
	did := strings.Fields(strings.Split(strings.TrimSpace(dl), "\n")[1])[0]
	if di := mustCLI(t, "", "datum", "inspect", v1, did); !strings.Contains(di, "state") || !strings.Contains(di, "success") {
		t.Fatalf("datum inspect %q: want state: success", di)
	}

	// 11. pipeline update (v2) — a transform that fails with a marker:
	// the container runner journals a failed datum's output into the log
	// store (success output is not captured), so the logs verbs get
	// deterministic content
	spec2 := filepath.Join(t.TempDir(), "spec2.json")
	if err := os.WriteFile(spec2, []byte(`{
	  "name": "capw",
	  "transform": {"image": "alpine", "cmd": ["sh", "-c", "echo PROCMARK; exit 1"]},
	  "input": {"repo": "rw", "glob": "/*"},
	  "enableStats": true
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	mustCLI(t, "", "pipeline", "update", "-f", spec2)
	if pi := mustCLI(t, "", "pipeline", "inspect", "capw"); !strings.Contains(pi, "version") || !strings.Contains(pi, "2") {
		t.Fatalf("pipeline inspect after update %q: want version: 2", pi)
	}
	// a new input file is what makes the v2 transform actually run: an
	// unchanged datum is dedup-skipped, so the update's own head
	// job would complete without touching the container
	mustCLI(t, "D", "file", "put", "rw@master:/d.txt", "-")
	// the v2 job (triggered by d.txt) fails: wait for it by polling
	ok := pollCLI(t, func(out string) bool {
		for _, r := range jobRows(out) {
			if r.state == "failure" {
				return true
			}
		}
		return false
	}, 60*time.Second, "job", "list", "capw")
	v2 := ""
	for _, r := range jobRows(ok) {
		if r.state == "failure" {
			v2 = r.id
			break
		}
	}

	// 12. logs: pipeline-scoped and job-scoped both carry the marker
	pollCLI(t, func(out string) bool {
		return strings.Contains(out, "PROCMARK")
	}, 30*time.Second, "logs", "-p", "capw")
	pollCLI(t, func(out string) bool {
		return strings.Contains(out, "PROCMARK")
	}, 30*time.Second, "logs", "-j", v2)

	// 13. update to a sleeping transform — its own scheduled head job is
	// the running job (d.txt's datum is not dedup-skipped after the v2
	// failure, so it actually runs) — then restart a datum mid-flight,
	// stop the job, delete it
	spec3 := filepath.Join(t.TempDir(), "spec3.json")
	if err := os.WriteFile(spec3, []byte(`{
	  "name": "capw",
	  "transform": {"image": "alpine", "cmd": ["sh", "-c", "sleep 30"]},
	  "input": {"repo": "rw", "glob": "/*"},
	  "enableStats": true
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	mustCLI(t, "", "pipeline", "update", "-f", spec3)
	run := pollCLI(t, func(out string) bool {
		for _, r := range jobRows(out) {
			if r.state == "running" {
				return true
			}
		}
		return false
	}, 60*time.Second, "job", "list", "capw")
	rjid := ""
	for _, r := range jobRows(run) {
		if r.state == "running" {
			rjid = r.id
		}
	}
	// the datums of the running job appear as it processes (4 files:
	// a.txt, b.txt, copied.txt, d.txt — only c.txt was deleted); restart
	// the datum that is actually running (skipped datums are not
	// restartable)
	rdl := pollCLI(t, func(out string) bool {
		lines := strings.Split(strings.TrimSpace(out), "\n")
		if len(lines) != 5 { // header + 4 datums
			return false
		}
		for _, line := range lines[1:] {
			if len(strings.Fields(line)) == 2 && strings.Fields(line)[1] == "running" {
				return true
			}
		}
		return false
	}, 60*time.Second, "datum", "list", rjid)
	rdid := ""
	for _, line := range strings.Split(strings.TrimSpace(rdl), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[1] == "running" {
			rdid = f[0]
		}
	}
	if rdid == "" {
		t.Fatalf("datum list %q: no running datum", rdl)
	}
	mustCLI(t, "", "datum", "restart", rjid, rdid)
	mustCLI(t, "", "job", "restart-datum", rjid, rdid)
	// cancelJob settles the job before returning: the state is killed
	mustCLI(t, "", "job", "stop", rjid)
	ji := mustCLI(t, "", "job", "inspect", rjid)
	if !strings.Contains(ji, "state") || !strings.Contains(ji, "killed") {
		t.Fatalf("job inspect after stop %q: want state: killed", ji)
	}
	mustCLI(t, "", "job", "delete", rjid)
	if jl := mustCLI(t, "", "job", "list", "capw"); strings.Contains(jl, rjid) {
		t.Fatalf("job list after delete: %s still present", rjid)
	}

	// 14. pipeline stop/start round-trip
	mustCLI(t, "", "pipeline", "stop", "capw")
	if pi := mustCLI(t, "", "pipeline", "inspect", "capw"); !strings.Contains(pi, "state") || !strings.Contains(pi, "paused") {
		t.Fatalf("pipeline inspect after stop %q: want state: paused", pi)
	}
	mustCLI(t, "", "pipeline", "start", "capw")
	if pi := mustCLI(t, "", "pipeline", "inspect", "capw"); !strings.Contains(pi, "state") || !strings.Contains(pi, "running") {
		t.Fatalf("pipeline inspect after start %q: want state: running", pi)
	}

	// 15. transaction start/delete (an empty transaction cannot finish:
	// finishing requires at least one staged operation)
	tx2 := strings.TrimSpace(mustCLI(t, "", "transaction", "start"))
	if tx2 == "" {
		t.Fatal("transaction start: empty id")
	}
	mustCLI(t, "", "transaction", "delete", tx2)

	// 16. commit delete by id: b.txt's commit vanishes, a.txt survives
	mustCLI(t, "", "commit", "delete", bid)
	if errs := failCLI(t, "file", "get", "rw@master:/b.txt"); !strings.Contains(errs, "not found") {
		t.Fatalf("deleted-commit file stderr %q: want a not-found error", errs)
	}
	if got := mustCLI(t, "", "file", "get", "rw@master:/a.txt"); got != "A" {
		t.Fatalf("after commit delete a.txt = %q, want %q", got, "A")
	}

	// 17. cron pipeline + manual tick (run-cron), settled via flush
	cronSpec := filepath.Join(t.TempDir(), "cron.json")
	if err := os.WriteFile(cronSpec, []byte(`{
	  "name": "cronp",
	  "transform": {"image": "alpine"},
	  "input": {"name": "cron", "cron": "@every 5m"}
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	mustCLI(t, "", "pipeline", "create", "-f", cronSpec)
	mustCLI(t, "", "pipeline", "run-cron", "cronp")
	fc := mustCLI(t, "", "flush", "commit", "cronp-cron@master", "--timeout", "5m")
	if !strings.Contains(fc, "cronp") || !strings.Contains(fc, "success") {
		t.Fatalf("cron flush output %q: want a cronp success job", fc)
	}

	// 18. pipeline create staged in a transaction, then finishing the
	// transaction applies it
	tx3 := strings.TrimSpace(mustCLI(t, "", "transaction", "start"))
	txpSpec := filepath.Join(t.TempDir(), "txp.json")
	if err := os.WriteFile(txpSpec, []byte(`{
	  "name": "txp",
	  "transform": {"image": "alpine", "cmd": ["sh", "-c", "cp -r ${rw}/* ${OUT}/"]},
	  "input": {"repo": "rw", "glob": "/*"}
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	mustCLI(t, "", "pipeline", "create", "-f", txpSpec, "--tx", tx3)
	mustCLI(t, "", "transaction", "finish", tx3)
	if pl := mustCLI(t, "", "pipeline", "list"); !strings.Contains(pl, "txp") {
		t.Fatalf("pipeline list %q after tx finish: txp missing", pl)
	}

	// 19. --version reports both the binary and the daemon version (the
	// daemon runs the same binary, so they agree here)
	v := mustCLI(t, "", "--version")
	if !strings.Contains(v, "sandman 0.0.1") || !strings.Contains(v, "daemon: 0.0.1") {
		t.Fatalf("--version = %q, want binary and daemon version lines", v)
	}
}
