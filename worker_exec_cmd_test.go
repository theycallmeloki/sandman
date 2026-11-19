package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sandman/client"
	"sandman/internal/store"
)

// TestAdhocExecRequestZeroSidesAndArgv pins the ad-hoc wire shape: the
// given image and argv, zero input sides, the caller's env, and a streamed
// reply. A stray side would make the worker mount /sandman/in and set an
// input env var that an ad-hoc transform never asked for.
func TestAdhocExecRequestZeroSidesAndArgv(t *testing.T) {
	req := adhocExecRequest("gpu-box", "python:3.12-slim",
		[]string{"FOO=bar", "HF_TOKEN=secret"},
		[]string{"python", "-c", "print(1)"})

	if req.Image != "python:3.12-slim" {
		t.Errorf("image = %q", req.Image)
	}
	if got := strings.Join(req.Cmd, "\x00"); got != "python\x00-c\x00print(1)" {
		t.Errorf("cmd = %q", req.Cmd)
	}
	if len(req.Sides) != 0 {
		t.Errorf("sides = %d, want zero", len(req.Sides))
	}
	if got := strings.Join(req.Env, ","); got != "FOO=bar,HF_TOKEN=secret" {
		t.Errorf("env = %q", got)
	}
	if req.JobID != "" {
		t.Errorf("jobID = %q, want empty (an ad-hoc run has no job record)", req.JobID)
	}
	if !req.Stream {
		t.Error("stream = false, want live output")
	}
	if !strings.HasPrefix(req.Cname, "sandman-exec-gpu_box-") {
		t.Errorf("cname = %q, want a unique sandman-exec-<worker>-* name", req.Cname)
	}
}

// TestDeriveGitRepoSideEnvName pins Change 2: a hyphenated git URL keeps
// its repo spelling but exposes a shell-legal environment name. Before the
// fix the side carried `vendor-specs`, which the container received as
// `vendor-specs=/sandman/in/vendor-specs` — not a legal identifier.
func TestDeriveGitRepoSideEnvName(t *testing.T) {
	dir := t.TempDir()
	d := &daemon{state: dir}
	d.store = store.New(filepath.Join(dir, "store"))

	p := client.Pipeline{Name: "p-git", Input: &client.Input{
		Cross: []client.Input{{Git: &client.GitInput{URL: "https://github.com/acme/vendor-specs.git"}}},
	}}
	if err := validateInputSides(p.Input, p.Name); err != nil {
		t.Fatalf("validation rejected the hyphenated git side: %v", err)
	}
	d.deriveGitRepos(&p)

	side := p.Input.Cross[0]
	if side.Repo != "vendor-specs" {
		t.Errorf("repo = %q, want the stored spelling vendor-specs", side.Repo)
	}
	if side.Name != "vendor_specs" {
		t.Errorf("env name = %q, want vendor_specs", side.Name)
	}
	if !shIdent.MatchString(side.Name) {
		t.Errorf("env name %q is not a shell identifier", side.Name)
	}
	if _, err := os.Stat(d.store.RepoDir(side.Repo)); err != nil {
		t.Errorf("mapped repo %q not created: %v", side.Repo, err)
	}
}

// TestGitCrossSanitizedNameUnique pins that the sanitised name is what the
// cross's duplicate check sees: `vendor-specs` and a plain repo named
// `vendor_specs` collide instead of slipping past validation and both
// claiming the same container env var.
func TestGitCrossSanitizedNameUnique(t *testing.T) {
	in := &client.Input{Cross: []client.Input{
		{Git: &client.GitInput{URL: "https://github.com/acme/vendor-specs.git"}},
		{Repo: "vendor_specs", Glob: "/*"},
	}}
	err := validateInputSides(in, "p-cross")
	if err == nil {
		t.Fatal("cross with colliding sanitised names validated; want a duplicate-name error")
	}
	if !strings.Contains(err.Error(), "vendor_specs") || !strings.Contains(err.Error(), "more than one input") {
		t.Fatalf("error = %v, want the duplicate input-name error for vendor_specs", err)
	}
}

// TestResolveWorkerHostUnknownWorker pins the actionable failure for a
// mistyped or absent worker name.
func TestResolveWorkerHostUnknownWorker(t *testing.T) {
	if _, err := resolveWorkerHost(nil, "nope", "127.0.0.1:4243"); err == nil ||
		!strings.Contains(err.Error(), "no registered workers") {
		t.Fatalf("empty registry: err = %v, want 'no registered workers'", err)
	}
	hosts := []client.HostInfo{{Name: "a", Addr: "127.0.0.1:1"}, {Name: "b", Addr: "127.0.0.1:2"}}
	_, err := resolveWorkerHost(hosts, "c", "127.0.0.1:4243")
	if err == nil || !strings.Contains(err.Error(), "lists: a, b") {
		t.Fatalf("unknown name: err = %v, want the registry names listed", err)
	}
	h, err := resolveWorkerHost(hosts, "b", "")
	if err != nil || h.Addr != "127.0.0.1:2" {
		t.Fatalf("known name: h=%+v err=%v", h, err)
	}
}

// TestClientWorkerStreamsLiveOutput drives the CLI path against a fake
// worker: the daemon registry names it, /exec answers NDJSON, output
// arrives live, the container's exit code is returned, and a secret-looking
// env value never reaches the operator-facing log.
func TestClientWorkerStreamsLiveOutput(t *testing.T) {
	var hostAddr string
	var got execRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/hosts":
			_ = json.NewEncoder(w).Encode([]client.HostInfo{{Name: "w1", Addr: hostAddr}})
		case "/exec":
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Errorf("decode request: %v", err)
				http.Error(w, "bad request", 400)
				return
			}
			w.Header().Set("Content-Type", execStreamType)
			enc := json.NewEncoder(w)
			_ = enc.Encode(execStreamEvent{Type: "out", Data: []byte("hello from the worker\n")})
			_ = enc.Encode(execStreamEvent{Type: "result", Result: &execResult{
				PrimaryCode: 7,
				Log:         "hello from the worker\n",
			}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	hostAddr = strings.TrimPrefix(srv.URL, "http://")

	var out, errOut bytes.Buffer
	code := clientWorkerRun(context.Background(), "w1", hostAddr,
		"python:3.12-slim",
		[]string{"PLAIN=ok", "HF_TOKEN=supersecret"},
		[]string{"python", "-c", "print(1)"},
		&out, &errOut)

	if code != 7 {
		t.Errorf("exit code = %d, want the container's 7", code)
	}
	if out.String() != "hello from the worker\n" {
		t.Errorf("live output = %q", out.String())
	}
	if got.Image != "python:3.12-slim" || len(got.Sides) != 0 || len(got.Cmd) != 3 {
		t.Errorf("worker got image=%q sides=%d cmd=%v", got.Image, len(got.Sides), got.Cmd)
	}
	if strings.Contains(errOut.String(), "supersecret") {
		t.Errorf("secret value leaked into the log: %q", errOut.String())
	}
	if !strings.Contains(errOut.String(), "HF_TOKEN=***") || !strings.Contains(errOut.String(), "PLAIN=ok") {
		t.Errorf("env log = %q, want HF_TOKEN masked and PLAIN visible", errOut.String())
	}
}

// TestExecOnWorkerAcceptsLegacyJSON pins compatibility with a worker that
// predates the stream field: it ignores Stream, answers one JSON result,
// and the client still prints its captured log and returns its exit code.
func TestExecOnWorkerAcceptsLegacyJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(execResult{PrimaryCode: 3, Log: "legacy worker output\n"})
	}))
	defer srv.Close()

	var out bytes.Buffer
	res, err := execOnWorker(context.Background(), srv.URL,
		adhocExecRequest("old-worker", "alpine", nil, []string{"true"}), &out)
	if err != nil {
		t.Fatalf("execOnWorker: %v", err)
	}
	if res.PrimaryCode != 3 || out.String() != "legacy worker output\n" {
		t.Fatalf("code=%d output=%q", res.PrimaryCode, out.String())
	}
}
