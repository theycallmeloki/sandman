package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// TestProcessRunnerEnvIsolation proves the process backend does not hand
// the daemon's environment to user code: a daemon-only variable (including
// a credential-shaped one) is absent in the job, while the allowlisted
// runtime variables and the job's own explicit env are present.
func TestProcessRunnerEnvIsolation(t *testing.T) {
	t.Setenv("SANDMAN_TEST_DAEMON_SECRET", "daemon-env-secret")
	t.Setenv("SANDMAN_AUTH_PASSWORD", "daemon-auth-secret")
	t.Setenv("LC_TEST_LOCALE", "C.UTF-8")

	dir := t.TempDir()
	script := strings.Join([]string{
		`printf 'daemon=%s\n' "${SANDMAN_TEST_DAEMON_SECRET-<unset>}"`,
		`printf 'auth=%s\n' "${SANDMAN_AUTH_PASSWORD-<unset>}"`,
		`printf 'job=%s\n' "${JOB_EXPLICIT-<unset>}"`,
		`printf 'locale=%s\n' "${LC_TEST_LOCALE-<unset>}"`,
		`printf 'path=%s\n' "$([ -n "${PATH-}" ] && echo present || echo absent)"`,
	}, "\n")
	var out bytes.Buffer
	res := processRunner{}.Run(JobSpec{
		Name:    "env-isolation",
		Cmd:     []string{"sh", "-c", script},
		Env:     []string{"JOB_EXPLICIT=job-value"},
		Workdir: dir,
		Capture: &out,
	})
	if res.Code != 0 {
		t.Fatalf("run code = %d tail = %q", res.Code, res.Tail)
	}
	got := out.String()
	for _, want := range []string{
		"daemon=<unset>",
		"auth=<unset>",
		"job=job-value",
		"locale=C.UTF-8",
		"path=present",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("job env output %q missing %q", got, want)
		}
	}
	if strings.Contains(got, "daemon-env-secret") || strings.Contains(got, "daemon-auth-secret") {
		t.Fatalf("daemon secret leaked into the job env: %q", got)
	}
}

// TestProcessRunnerEnvAllowlist pins the explicit allowlist and the
// job-env-wins rule: ordinary runtime variables cross the boundary, an
// unrelated daemon variable does not, and an explicit job value overrides
// an allowlisted daemon value (never duplicate keys, where getenv's pick
// would be runtime-dependent).
func TestProcessRunnerEnvAllowlist(t *testing.T) {
	t.Setenv("SANDMAN_TEST_UNRELATED_SECRET", "nope")
	t.Setenv("HOME", "/daemon/home")
	t.Setenv("LANG", "en_US.UTF-8")

	env := processRunnerEnv([]string{"JOB=1", "HOME=/job/home"})
	joined := strings.Join(env, "\n")
	for _, bad := range []string{"SANDMAN_TEST_UNRELATED_SECRET", "nope", "HOME=/daemon/home"} {
		if strings.Contains(joined, bad) {
			t.Fatalf("processRunnerEnv leaked %q: %v", bad, env)
		}
	}
	for _, want := range []string{"JOB=1", "HOME=/job/home", "LANG=en_US.UTF-8", "PATH="} {
		if !strings.Contains(joined, want) {
			t.Fatalf("processRunnerEnv missing %q: %v", want, env)
		}
	}
	seen := map[string]bool{}
	for _, kv := range env {
		k, _, _ := strings.Cut(kv, "=")
		if seen[k] {
			t.Fatalf("duplicate key %q in %v", k, env)
		}
		seen[k] = true
	}
}

// TestProcessRunnerEnvNoDaemonCreds is a belt-and-braces check that the
// allowlist cannot be widened by a stray prefix match: SANDMAN_* names
// never pass, and LC_ only matches the locale family (not LCX_).
func TestProcessRunnerEnvNoDaemonCreds(t *testing.T) {
	t.Setenv("SANDMAN_ANYTHING", "x")
	t.Setenv("LCX_NOT_A_LOCALE", "x")
	env := processRunnerEnv(nil)
	for _, kv := range env {
		if strings.HasPrefix(kv, "SANDMAN_") || strings.HasPrefix(kv, "LCX_") {
			t.Fatalf("env leaked %q", kv)
		}
	}
	// a sanity guard: PATH from the daemon is inherited (needed to exec sh)
	if len(os.Environ()) == 0 {
		t.Fatal("test environment is empty")
	}
}
