package cli

// Command-level tests that drive the REAL sandman binary as a subprocess
// against an in-memory fake of the control plane's HTTP API. Subprocess
// execution gives the true exit codes and true stdout/stderr — no fds
// swapping, no exit interception. TestMain builds the binary once; each
// test starts an httptest fake daemon and passes its address via the
// global -addr flag.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"sandman/client"
)

// binPath is the test-built sandman binary (built in TestMain).
var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "sandman-cli-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "TestMain:", err)
		os.Exit(1)
	}
	binPath = filepath.Join(dir, "sandman")
	root := moduleRoot()
	cmd := exec.Command("go", "build", "-o", binPath, ".")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building sandman for CLI tests: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// moduleRoot walks up from the package dir to the module root (go.mod).
func moduleRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "."
		}
		dir = parent
	}
}

// fakeDaemon is the minimal control-plane stub the CLI tests drive.
type fakeDaemon struct {
	mu        sync.Mutex
	repos     map[string]*fakeRepo
	commits   map[string]*fakeCommit // by id
	heads     map[string]string      // repo + "/" + branch -> commit id
	pipelines []client.Pipeline
	jobs      []client.Job
	hosts     []client.HostInfo
	next      int
}

type fakeRepo struct{ name string }

type fakeCommit struct {
	id       string
	repo     string
	branch   string
	finished bool
	files    map[string][]byte
}

func newFakeDaemon() *fakeDaemon {
	return &fakeDaemon{repos: map[string]*fakeRepo{}, commits: map[string]*fakeCommit{}, heads: map[string]string{}}
}

func (f *fakeDaemon) err(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func (f *fakeDaemon) newID() string {
	f.next++
	return fmt.Sprintf("%016x", f.next)
}

func (f *fakeDaemon) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := r.URL.Path
	switch {
	case r.Method == "POST" && p == "/api/v1/repos":
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		name := body["name"]
		if _, ok := f.repos[name]; !ok {
			f.repos[name] = &fakeRepo{name: name}
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"ok": "true"})
	case r.Method == "GET" && p == "/api/v1/repos":
		var out []client.Repo
		for _, rp := range f.repos {
			var branches []string
			for k := range f.heads {
				if strings.HasPrefix(k, rp.name+"/") && !strings.Contains(strings.TrimPrefix(k, rp.name+"/"), "/") {
					branches = append(branches, strings.TrimPrefix(k, rp.name+"/"))
				}
			}
			out = append(out, client.Repo{Name: rp.name, Branches: branches})
		}
		_ = json.NewEncoder(w).Encode(out)
	case r.Method == "GET" && strings.HasPrefix(p, "/api/v1/repos/"):
		rest := strings.TrimPrefix(p, "/api/v1/repos/")
		parts := strings.Split(rest, "/")
		if len(parts) == 1 { // repo inspect
			name := parts[0]
			rp, ok := f.repos[name]
			if !ok {
				f.err(w, http.StatusNotFound, fmt.Sprintf("repo %q not found", name))
				return
			}
			var branches []string
			for k := range f.heads {
				if strings.HasPrefix(k, rp.name+"/") {
					branches = append(branches, strings.TrimPrefix(k, rp.name+"/"))
				}
			}
			_ = json.NewEncoder(w).Encode(client.Repo{Name: rp.name, Branches: branches})
			return
		}
		if len(parts) == 4 && parts[1] == "branches" && parts[3] == "head" { // branch head
			head, ok := f.heads[parts[0]+"/"+parts[2]]
			if !ok {
				f.err(w, http.StatusNotFound, fmt.Sprintf("branch %s@%s has no head", parts[0], parts[2]))
				return
			}
			cm := f.commits[head]
			_ = json.NewEncoder(w).Encode(client.Commit{ID: cm.id, Repo: cm.repo, Branch: cm.branch, Started: true, Finished: cm.finished})
			return
		}
		f.err(w, http.StatusNotFound, "no such repo route")
	case r.Method == "POST" && strings.HasPrefix(p, "/api/v1/repos/") && strings.HasSuffix(p, "/commits"):
		rest := strings.TrimPrefix(p, "/api/v1/repos/")
		repo := strings.TrimSuffix(rest, "/commits")
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if _, ok := f.repos[repo]; !ok {
			f.err(w, http.StatusNotFound, fmt.Sprintf("repo %q not found", repo))
			return
		}
		cm := &fakeCommit{id: f.newID(), repo: repo, branch: body["branch"], files: map[string][]byte{}}
		f.commits[cm.id] = cm
		_ = json.NewEncoder(w).Encode(client.Commit{ID: cm.id, Repo: repo, Branch: cm.branch, Started: true})
	case r.Method == "POST" && strings.HasPrefix(p, "/api/v1/commits/") && strings.HasSuffix(p, "/finish"):
		id := strings.TrimSuffix(strings.TrimPrefix(p, "/api/v1/commits/"), "/finish")
		cm, ok := f.commits[id]
		if !ok {
			f.err(w, http.StatusNotFound, "no such commit")
			return
		}
		cm.finished = true
		if cm.branch != "" {
			f.heads[cm.repo+"/"+cm.branch] = cm.id
		}
		_ = json.NewEncoder(w).Encode(client.Commit{ID: cm.id, Repo: cm.repo, Branch: cm.branch, Started: true, Finished: true})
	case r.Method == "DELETE" && strings.HasPrefix(p, "/api/v1/commits/"):
		id := strings.TrimPrefix(p, "/api/v1/commits/")
		delete(f.commits, id)
		_ = json.NewEncoder(w).Encode(map[string]string{"ok": "true"})
	case r.Method == "GET" && strings.HasPrefix(p, "/api/v1/commits/") && !strings.Contains(p, "/files"):
		id := strings.TrimPrefix(p, "/api/v1/commits/")
		cm, ok := f.commits[id]
		if !ok {
			f.err(w, http.StatusNotFound, "no such commit")
			return
		}
		_ = json.NewEncoder(w).Encode(client.Commit{ID: cm.id, Repo: cm.repo, Branch: cm.branch, Started: true, Finished: cm.finished})
	case strings.HasPrefix(p, "/api/v1/commits/") && strings.Contains(p, "/files"):
		rest := strings.TrimPrefix(p, "/api/v1/commits/")
		segs := strings.SplitN(rest, "/files", 2)
		cm, ok := f.commits[segs[0]]
		if !ok {
			f.err(w, http.StatusNotFound, "no such commit")
			return
		}
		if r.Method == "PUT" {
			path := strings.TrimPrefix(segs[1], "/")
			data, err := io.ReadAll(r.Body)
			if err != nil {
				f.err(w, http.StatusBadRequest, err.Error())
				return
			}
			cm.files[path] = data
			_ = json.NewEncoder(w).Encode(map[string]string{"ok": "true"})
			return
		}
		if r.Method == "GET" {
			fp := strings.TrimPrefix(segs[1], "/")
			if fp == "" { // list
				glob := r.URL.Query().Get("glob")
				// the server's listing globs are "prefix*" patterns: the
				// text before the first * is a recursive prefix
				prefix := glob
				if i := strings.Index(glob, "*"); i >= 0 {
					prefix = glob[:i]
				}
				var out []client.FileInfo
				for fpath, data := range cm.files {
					if glob == "" || strings.HasPrefix(fpath, prefix) {
						out = append(out, client.FileInfo{Path: fpath, Size: uint64(len(data))})
					}
				}
				_ = json.NewEncoder(w).Encode(out)
				return
			}
			data, ok := cm.files[fp]
			if !ok {
				f.err(w, http.StatusNotFound, fmt.Sprintf("file %q not found", fp))
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(data)
			return
		}
		f.err(w, http.StatusMethodNotAllowed, "method not allowed")
	case r.Method == "GET" && p == "/api/v1/jobs":
		q := r.URL.Query()
		var out []client.Job
		for _, j := range f.jobs {
			if q.Get("pipeline") != "" && j.Pipeline != q.Get("pipeline") {
				continue
			}
			if states, ok := q["state"]; ok {
				found := false
				for _, s := range states {
					if j.State == s {
						found = true
					}
				}
				if !found {
					continue
				}
			}
			if commits, ok := q["inputCommit"]; ok {
				all := true
				for _, want := range commits {
					found := false
					for _, have := range j.InputCommits {
						if have == want {
							found = true
						}
					}
					if !found {
						all = false
						break
					}
				}
				if !all {
					continue
				}
			}
			out = append(out, j)
		}
		_ = json.NewEncoder(w).Encode(out)
	case r.Method == "POST" && p == "/api/v1/pipelines":
		var pl client.Pipeline
		_ = json.NewDecoder(r.Body).Decode(&pl)
		f.pipelines = append(f.pipelines, pl)
		_ = json.NewEncoder(w).Encode(map[string]string{"ok": "true"})
	case r.Method == "POST" && p == "/api/v1/git/delta":
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		res := client.GitDeltaResult{Applied: true, Head: "feedface0000111122223333"}
		if body["base"] == "stale" {
			res = client.GitDeltaResult{Applied: false, Reason: "base mismatch"}
		}
		_ = json.NewEncoder(w).Encode(res)
	case r.Method == "GET" && p == "/api/v1/pipelines":
		var out []client.PipelineInfo
		for _, pl := range f.pipelines {
			out = append(out, client.PipelineInfo{Name: pl.Name})
		}
		_ = json.NewEncoder(w).Encode(out)
	case r.Method == "GET" && p == "/api/v1/logs":
		_ = json.NewEncoder(w).Encode(map[string]any{"lines": []string{"line one", "line two"}})
	case r.Method == "GET" && p == "/api/v1/version":
		_ = json.NewEncoder(w).Encode(map[string]string{"version": "v0.2.35-test"})
	case r.Method == "GET" && p == "/api/v1/hosts":
		_ = json.NewEncoder(w).Encode(f.hosts)
	default:
		f.err(w, http.StatusNotFound, "no such route: "+r.Method+" "+p)
	}
}

// runCLI runs the real binary against the fake daemon and returns its
// stdout, stderr, and exit code.
func runCLI(t *testing.T, f *fakeDaemon, stdin io.Reader, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	ts := httptest.NewServer(f)
	defer ts.Close()
	full := append([]string{"-addr", strings.TrimPrefix(ts.URL, "http://")}, args...)
	cmd := exec.Command(binPath, full...)
	if stdin != nil {
		cmd.Stdin = stdin
	}
	var outB, errB bytes.Buffer
	cmd.Stdout = &outB
	cmd.Stderr = &errB
	err := cmd.Run()
	code = 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else {
			t.Fatalf("running %s: %v", binPath, err)
		}
	}
	return outB.String(), errB.String(), code
}

func testRepo(t *testing.T, f *fakeDaemon) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.repos["in"] = &fakeRepo{name: "in"}
	return "in"
}

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// head returns the branch-head commit of the fake daemon.
func (f *fakeDaemon) head(t *testing.T, repo, branch string) *fakeCommit {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.heads[repo+"/"+branch]
	if !ok {
		t.Fatalf("no head for %s@%s", repo, branch)
	}
	return f.commits[id]
}

// ---- put ----

func TestPutSingleFile(t *testing.T) {
	f := newFakeDaemon()
	testRepo(t, f)
	dir := t.TempDir()
	src := writeFile(t, dir, "a.txt", "hello put")

	out, _, code := runCLI(t, f, nil, "put", src, "in@master:a.txt")
	if code != 0 {
		t.Fatalf("put exit %d, stdout %q", code, out)
	}
	if !strings.Contains(out, "wrote in@master:a.txt") {
		t.Fatalf("stdout = %q, want wrote line", out)
	}
	if got := string(f.head(t, "in", "master").files["a.txt"]); got != "hello put" {
		t.Fatalf("stored content = %q", got)
	}
}

func TestPutFileIntoTrailingSlashDest(t *testing.T) {
	f := newFakeDaemon()
	testRepo(t, f)
	dir := t.TempDir()
	src := writeFile(t, dir, "a.txt", "x")

	_, _, code := runCLI(t, f, nil, "put", src, "in@master:data/")
	if code != 0 {
		t.Fatalf("put exit %d", code)
	}
	if _, ok := f.head(t, "in", "master").files["data/a.txt"]; !ok {
		t.Fatalf("expected data/a.txt, got %v", f.head(t, "in", "master").files)
	}
}

func TestPutDirectoryTree(t *testing.T) {
	f := newFakeDaemon()
	testRepo(t, f)
	dir := t.TempDir()
	writeFile(t, dir, "a.txt", "top")
	writeFile(t, dir, "sub/b.txt", "nested")

	out, _, code := runCLI(t, f, nil, "put", dir, "in@master:data")
	if code != 0 {
		t.Fatalf("put exit %d, stdout %q", code, out)
	}
	cm := f.head(t, "in", "master")
	if got := string(cm.files["data/a.txt"]); got != "top" {
		t.Fatalf("data/a.txt = %q", got)
	}
	if got := string(cm.files["data/sub/b.txt"]); got != "nested" {
		t.Fatalf("data/sub/b.txt = %q", got)
	}
	// one transfer, one commit: the per-file lines share a commit id
	ids := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		ids[line[strings.LastIndex(line, "commit ")+len("commit "):]] = true
	}
	if len(ids) != 1 {
		t.Fatalf("expected one commit for the tree upload, got %v (%q)", ids, out)
	}
}

func TestPutStdin(t *testing.T) {
	f := newFakeDaemon()
	testRepo(t, f)
	out, _, code := runCLI(t, f, strings.NewReader("stdin data"), "put", "-", "in@master:s.txt")
	if code != 0 {
		t.Fatalf("put exit %d, stdout %q", code, out)
	}
	if got := string(f.head(t, "in", "master").files["s.txt"]); got != "stdin data" {
		t.Fatalf("stored = %q", got)
	}
}

func TestPutMissingRepoHints(t *testing.T) {
	f := newFakeDaemon()
	dir := t.TempDir()
	src := writeFile(t, dir, "a.txt", "x")
	_, stderr, code := runCLI(t, f, nil, "put", src, "nosuch@master:a.txt")
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr, "create it with: sandman repo create nosuch") {
		t.Fatalf("stderr = %q, want create hint", stderr)
	}
}

func TestPutExplicitCommitFlow(t *testing.T) {
	f := newFakeDaemon()
	testRepo(t, f)
	out, _, code := runCLI(t, f, nil, "commit", "start", "in@master")
	if code != 0 {
		t.Fatalf("commit start exit %d", code)
	}
	id := strings.TrimSpace(out)
	dir := t.TempDir()
	src := writeFile(t, dir, "a.txt", "explicit")
	_, _, code = runCLI(t, f, nil, "put", src, id+":a.txt")
	if code != 0 {
		t.Fatalf("put into commit exit %d", code)
	}
	if f.commits[id].finished {
		t.Fatalf("explicit-flow put must not finish the commit")
	}
	if got := string(f.commits[id].files["a.txt"]); got != "explicit" {
		t.Fatalf("stored = %q", got)
	}
}

// ---- get / ls / cat ----

func TestGetToStdoutAndFile(t *testing.T) {
	f := newFakeDaemon()
	testRepo(t, f)
	dir := t.TempDir()
	src := writeFile(t, dir, "a.txt", "fetch me")
	if _, _, code := runCLI(t, f, nil, "put", src, "in@master:a.txt"); code != 0 {
		t.Fatal("setup put failed")
	}
	out, _, code := runCLI(t, f, nil, "get", "in@master:a.txt")
	if code != 0 || out != "fetch me" {
		t.Fatalf("get stdout = %q (code %d)", out, code)
	}
	dest := filepath.Join(t.TempDir(), "out.txt")
	_, _, code = runCLI(t, f, nil, "get", "in@master:a.txt", "-o", dest)
	if code != 0 {
		t.Fatalf("get -o exit %d", code)
	}
	b, err := os.ReadFile(dest)
	if err != nil || string(b) != "fetch me" {
		t.Fatalf("downloaded = %q (err %v)", b, err)
	}
}

func TestGetTreeDownload(t *testing.T) {
	f := newFakeDaemon()
	testRepo(t, f)
	dir := t.TempDir()
	writeFile(t, dir, "a.txt", "top")
	writeFile(t, dir, "sub/b.txt", "nested")
	var code int
	if _, _, code = runCLI(t, f, nil, "put", dir, "in@master:data"); code != 0 {
		t.Fatal("setup put failed")
	}
	outDir := t.TempDir()
	if _, _, code = runCLI(t, f, nil, "get", "in@master:data", "-o", outDir); code != 0 {
		t.Fatalf("tree get exit %d", code)
	}
	if b, _ := os.ReadFile(filepath.Join(outDir, "a.txt")); string(b) != "top" {
		t.Fatalf("a.txt = %q", b)
	}
	if b, _ := os.ReadFile(filepath.Join(outDir, "sub", "b.txt")); string(b) != "nested" {
		t.Fatalf("sub/b.txt = %q", b)
	}
}

func TestGetWholeRepo(t *testing.T) {
	f := newFakeDaemon()
	testRepo(t, f)
	var code int
	dir := t.TempDir()
	writeFile(t, dir, "a.txt", "top")
	if _, _, code = runCLI(t, f, nil, "put", dir, "in@master:"); code != 0 {
		t.Fatal("setup put failed")
	}
	outDir := t.TempDir()
	if _, _, code = runCLI(t, f, nil, "get", "in@master", "-o", outDir); code != 0 {
		t.Fatalf("repo get exit %d", code)
	}
	if b, _ := os.ReadFile(filepath.Join(outDir, "a.txt")); string(b) != "top" {
		t.Fatalf("a.txt = %q", b)
	}
}

func TestGetGlobWithoutOutputFails(t *testing.T) {
	f := newFakeDaemon()
	testRepo(t, f)
	dir := t.TempDir()
	writeFile(t, dir, "a.txt", "1")
	writeFile(t, dir, "b.txt", "2")
	var code int
	if _, _, code = runCLI(t, f, nil, "put", dir, "in@master:data"); code != 0 {
		t.Fatal("setup put failed")
	}
	var stderr string
	_, stderr, code = runCLI(t, f, nil, "get", "in@master:data/*.txt")
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr, "use -o DIR") {
		t.Fatalf("stderr = %q, want -o hint", stderr)
	}
}

func TestGetMissingFile(t *testing.T) {
	f := newFakeDaemon()
	testRepo(t, f)
	dir := t.TempDir()
	writeFile(t, dir, "a.txt", "x")
	if _, _, code := runCLI(t, f, nil, "put", dir, "in@master:"); code != 0 {
		t.Fatal("setup put failed")
	}
	_, stderr, code := runCLI(t, f, nil, "get", "in@master:nope.txt")
	if code != 1 || !strings.Contains(stderr, "not found") {
		t.Fatalf("exit %d stderr %q", code, stderr)
	}
}

func TestLsAndCat(t *testing.T) {
	f := newFakeDaemon()
	testRepo(t, f)
	dir := t.TempDir()
	writeFile(t, dir, "a.txt", "cat me")
	if _, _, code := runCLI(t, f, nil, "put", dir, "in@master:"); code != 0 {
		t.Fatal("setup put failed")
	}
	out, _, code := runCLI(t, f, nil, "ls", "in@master")
	if code != 0 || !strings.Contains(out, "a.txt") {
		t.Fatalf("ls = %q (code %d)", out, code)
	}
	out, _, code = runCLI(t, f, nil, "cat", "in@master:a.txt")
	if code != 0 || out != "cat me" {
		t.Fatalf("cat = %q (code %d)", out, code)
	}
}

// ---- lists: --json, state filter ----

func TestJobListStateFilter(t *testing.T) {
	f := newFakeDaemon()
	f.jobs = []client.Job{
		{ID: "aaaaaaaaaaaaaaaa", Pipeline: "p1", State: "success"},
		{ID: "bbbbbbbbbbbbbbbb", Pipeline: "p1", State: "failure"},
		{ID: "cccccccccccccccc", Pipeline: "p2", State: "running"},
	}
	out, _, code := runCLI(t, f, nil, "ps", "--state", "running")
	if code != 0 {
		t.Fatalf("ps exit %d", code)
	}
	if !strings.Contains(out, "cccccccccccccccc") || strings.Contains(out, "bbbbbbbbbbbbbbbb") {
		t.Fatalf("ps filter = %q", out)
	}
	out, _, code = runCLI(t, f, nil, "job", "list", "--state", "success", "--state", "failure", "--json")
	if code != 0 {
		t.Fatalf("job list --json exit %d", code)
	}
	var js []client.Job
	if err := json.Unmarshal([]byte(out), &js); err != nil {
		t.Fatalf("json: %v (%q)", err, out)
	}
	if len(js) != 2 {
		t.Fatalf("json jobs = %d, want 2", len(js))
	}
}

func TestJobListInputCommitFilter(t *testing.T) {
	f := newFakeDaemon()
	f.jobs = []client.Job{
		{ID: "aaaaaaaaaaaaaaaa", Pipeline: "p1", InputCommits: []string{"1111111111111111"}},
		{ID: "bbbbbbbbbbbbbbbb", Pipeline: "p1", InputCommits: []string{"2222222222222222"}},
		{ID: "cccccccccccccccc", Pipeline: "p1", InputCommits: []string{"1111111111111111", "3333333333333333"}},
	}
	out, _, code := runCLI(t, f, nil, "job", "list", "p1", "--input-commit", "1111111111111111", "--json")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	var js []client.Job
	if err := json.Unmarshal([]byte(out), &js); err != nil {
		t.Fatalf("json: %v (%q)", err, out)
	}
	if len(js) != 2 {
		t.Fatalf("jobs = %d, want 2 (both including 1111...): %q", len(js), out)
	}
	for _, j := range js {
		if j.ID == "bbbbbbbbbbbbbbbb" {
			t.Fatalf("input-commit filter leaked job with only 2222...")
		}
	}
}

func TestRepoListJSON(t *testing.T) {
	f := newFakeDaemon()
	testRepo(t, f)
	f.repos["second"] = &fakeRepo{name: "second"}
	out, _, code := runCLI(t, f, nil, "repo", "list", "--json")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	var repos []client.Repo
	if err := json.Unmarshal([]byte(out), &repos); err != nil {
		t.Fatalf("json: %v", err)
	}
	if len(repos) != 2 {
		t.Fatalf("repos = %d, want 2", len(repos))
	}
}

// ---- pipeline builder ----

func TestPipelineCreateFlags(t *testing.T) {
	f := newFakeDaemon()
	testRepo(t, f)
	_, _, code := runCLI(t, f, nil, "pipeline", "create", "mypipe",
		"--image", "alpine", "--cmd", "sh -c hi", "--input", "in@master",
		"--gpu", "1", "--parallelism", "2", "--env", "A=B", "--secret", "gh-token")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	if len(f.pipelines) != 1 {
		t.Fatalf("pipelines = %d, want 1", len(f.pipelines))
	}
	p := f.pipelines[0]
	if p.Name != "mypipe" || p.Transform.Image != "alpine" {
		t.Fatalf("pipeline = %+v", p)
	}
	if len(p.Transform.Cmd) != 3 || p.Transform.Cmd[0] != "sh" || p.Transform.Cmd[2] != "hi" {
		t.Fatalf("cmd = %v", p.Transform.Cmd)
	}
	if p.Input.Repo != "in" || p.Input.Branch != "" {
		t.Fatalf("input = %+v (branch must default to master)", p.Input)
	}
	if p.Transform.ResourceRequests == nil || p.Transform.ResourceRequests.GPU != 1 {
		t.Fatalf("resourceRequests = %+v", p.Transform.ResourceRequests)
	}
	if p.Parallelism == nil || p.Parallelism.Constant != 2 {
		t.Fatalf("parallelism = %+v", p.Parallelism)
	}
	if p.Transform.Env["A"] != "B" {
		t.Fatalf("env = %v", p.Transform.Env)
	}
	if len(p.Transform.Secrets) != 1 || p.Transform.Secrets[0].Name != "gh-token" {
		t.Fatalf("secrets = %v", p.Transform.Secrets)
	}
}

func TestPipelineCreateFlagValidation(t *testing.T) {
	f := newFakeDaemon()
	_, stderr, code := runCLI(t, f, nil, "pipeline", "create", "nope")
	if code != 1 || !strings.Contains(stderr, "--image") {
		t.Fatalf("exit %d stderr %q, want --image hint", code, stderr)
	}
	_, stderr, code = runCLI(t, f, nil, "pipeline", "create", "nope", "--image", "alpine")
	if code != 1 || !strings.Contains(stderr, "--input") {
		t.Fatalf("exit %d stderr %q, want --input hint", code, stderr)
	}
}

// TestPipelineCreateShFlag: --sh keeps the whole script as one argv
// element (sh -c '<script>') — the form that survives $in/$OUT and
// redirects intact.
func TestPipelineCreateShFlag(t *testing.T) {
	f := newFakeDaemon()
	testRepo(t, f)
	_, _, code := runCLI(t, f, nil, "pipeline", "create", "s", "--image", "alpine",
		"--sh", "cp $in/* $OUT/ > /tmp/x", "--input", "in@master")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	p := f.pipelines[0]
	want := []string{"sh", "-c", "cp $in/* $OUT/ > /tmp/x"}
	if len(p.Transform.Cmd) != 3 || p.Transform.Cmd[0] != want[0] || p.Transform.Cmd[1] != want[1] || p.Transform.Cmd[2] != want[2] {
		t.Fatalf("cmd = %v, want %v", p.Transform.Cmd, want)
	}
	if p.Input.Glob != "/*" {
		t.Fatalf("glob = %q, want default /*", p.Input.Glob)
	}
}

// ---- status / version / reachability ----

func TestStatus(t *testing.T) {
	f := newFakeDaemon()
	f.hosts = []client.HostInfo{{Name: "h1", Addr: "1.2.3.4:4343", Gpus: []client.GpuInfo{{Index: 0, Name: "RTX 3090"}}}}
	f.jobs = []client.Job{{ID: "aaaaaaaaaaaaaaaa", Pipeline: "p", State: "running"}}
	out, _, code := runCLI(t, f, nil, "status")
	if code != 0 {
		t.Fatalf("status exit %d", code)
	}
	for _, want := range []string{"daemon", "v0.2.35-test", "1 registered", "1 with GPUs", "1 running"} {
		if !strings.Contains(out, want) {
			t.Fatalf("status = %q, missing %q", out, want)
		}
	}
}

func TestConnRefusedHint(t *testing.T) {
	// a server that closed its port: connects refuse
	ts := httptest.NewServer(http.NotFoundHandler())
	dead := strings.TrimPrefix(ts.URL, "http://")
	ts.Close()

	cmd := exec.Command(binPath, "-addr", dead, "repo", "list")
	var errB bytes.Buffer
	cmd.Stderr = &errB
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected a non-zero exit")
	}
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 1 {
		t.Fatalf("exit = %v, want 1", err)
	}
	if !strings.Contains(errB.String(), "daemon not reachable at") {
		t.Fatalf("stderr = %q, want reachability hint", errB.String())
	}
}

// TestListingGlob pins the rule that the [:path] argument is a path, but the
// listing API accepts only prefix patterns — a bare
// path was passed through and rejected ("unsupported listing glob").
// A path without its own wildcard becomes a prefix of the listing; one
// that already carries a * is passed through unchanged.
func TestListingGlob(t *testing.T) {
	cases := []struct{ in, want string }{
		{"subdir", "subdir*"},
		{"subdir/", "subdir/*"},
		{"a/b/c.txt", "a/b/c.txt*"},
		{"subdir/*.md", "subdir/*.md"},
		{"*.txt", "*.txt"},
		{"a*b", "a*b"},
	}
	for _, c := range cases {
		if got := listingGlob(c.in); got != c.want {
			t.Errorf("listingGlob(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
