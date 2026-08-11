// Secrets: named typed metadata blobs with create/inspect/list/delete,
// and a credential requirement on every secret-management operation
// (SB-153, SB-154).
package conformance

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

func TestSB153_SecretsCRUD(t *testing.T) {
	// baseline listing
	base, err := c.ListSecrets()
	if err != nil {
		t.Fatalf("list secrets: %v", err)
	}
	if len(base) != 0 {
		t.Fatalf("baseline has %d secrets, want 0", len(base))
	}

	if err := c.CreateSecret("test-secret", map[string]string{"mykey": "bXktdmFsdWU="}); err != nil {
		t.Fatalf("create secret: %v", err)
	}

	info, err := c.InspectSecret("test-secret")
	if err != nil {
		t.Fatalf("inspect secret: %v", err)
	}
	if info.Name != "test-secret" {
		t.Fatalf("secret name = %q", info.Name)
	}
	if info.Type != "Opaque" {
		t.Fatalf("secret type = %q, want Opaque", info.Type)
	}
	if info.Created == "" {
		t.Fatalf("secret has no creation timestamp")
	}

	listed, err := c.ListSecrets()
	if err != nil {
		t.Fatalf("list secrets: %v", err)
	}
	if len(listed) != len(base)+1 {
		t.Fatalf("listed %d secrets, want %d", len(listed), len(base)+1)
	}

	if err := c.DeleteSecret("test-secret"); err != nil {
		t.Fatalf("delete secret: %v", err)
	}
	listed, err = c.ListSecrets()
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}
	if len(listed) != len(base) {
		t.Fatalf("listed %d secrets after delete, want back to baseline %d", len(listed), len(base))
	}
	// a second delete is a no-op and never resurrects the secret
	if err := c.DeleteSecret("test-secret"); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if _, err := c.InspectSecret("test-secret"); err == nil {
		t.Fatalf("inspect after delete: expected an error")
	} else if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("inspect-after-delete error = %q", err.Error())
	}
}

func TestSB154_SecretsRequireCredential(t *testing.T) {
	anon := client.New(fmt.Sprintf("127.0.0.1:%d", daemonPort))
	// no token: every secret-management operation is rejected uniformly
	if err := anon.CreateSecret("s", map[string]string{"k": "v"}); err == nil {
		t.Fatalf("create without token: expected error")
	} else if !strings.Contains(err.Error(), "no authentication token") {
		t.Fatalf("create error = %q, want it to name the missing token", err.Error())
	}
	if _, err := anon.InspectSecret("s"); err == nil {
		t.Fatalf("inspect without token: expected error")
	} else if !strings.Contains(err.Error(), "no authentication token") {
		t.Fatalf("inspect error = %q", err.Error())
	}
	if _, err := anon.ListSecrets(); err == nil {
		t.Fatalf("list without token: expected error")
	} else if !strings.Contains(err.Error(), "no authentication token") {
		t.Fatalf("list error = %q", err.Error())
	}
	if err := anon.DeleteSecret("s"); err == nil {
		t.Fatalf("delete without token: expected error")
	} else if !strings.Contains(err.Error(), "no authentication token") {
		t.Fatalf("delete error = %q", err.Error())
	}
	// a wrong token is also rejected
	wrong := client.New(fmt.Sprintf("127.0.0.1:%d", daemonPort))
	wrong.SetToken("wrong-token")
	if err := wrong.CreateSecret("s", map[string]string{"k": "v"}); err == nil {
		t.Fatalf("create with a wrong token: expected error")
	}
	// the credentialed client still works
	if err := c.CreateSecret("s", map[string]string{"k": "v"}); err != nil {
		t.Fatalf("create with credential: %v", err)
	}
	if err := c.DeleteSecret("s"); err != nil {
		t.Fatalf("delete with credential: %v", err)
	}
}

// TestSB154_AuthDisabledOpenPosture pins the other mode (D-04): with no
// credential configured the system is open — a token-less mutating
// request succeeds, and the disabled branch short-circuits BEFORE the
// header check, so even a wrong token is accepted (no credential is
// configured to be right or wrong against).
func TestSB154_AuthDisabledOpenPosture(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	port := freePort()
	cmd := exec.Command(binPath, "daemon", "-name", "noauth", "-port", strconv.Itoa(port), "-state", state)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start no-auth daemon: %v", err)
	}
	defer func() { _ = cmd.Process.Kill() }()

	open := client.New(fmt.Sprintf("127.0.0.1:%d", port))
	// token-less repo creation succeeds once the daemon is up (probe =
	// first success); repo creation is a gated mutating endpoint, so this
	// proves the open posture, not a read path
	pollFor(t, "no-auth daemon accepts token-less create", 30*time.Second, func() bool {
		return open.CreateRepo(uniq(t)) == nil
	})
	// a wrong token is also accepted: with auth disabled there is no
	// configured credential to match against (the disabled branch
	// short-circuits before the header check)
	open.SetToken("wrong-token")
	if err := open.CreateRepo(uniq(t)); err != nil {
		t.Fatalf("wrong-token create under no auth: %v", err)
	}
}

// TestSB051_SecretBinding — a pipeline's explicit secret references
// (SB-051 clause 2, D-05) reach the job: the key's value as a mounted
// file at the configured path and as an environment variable.
func TestSB051_SecretBinding(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	commitFiles(t, repo, "master", map[string]string{"file": "x\n"})

	secretName := uniq(t)
	if err := c.CreateSecret(secretName, map[string]string{"foo": "secretfoo"}); err != nil {
		t.Fatalf("create secret: %v", err)
	}

	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine",
			Cmd: []string{"sh", "-c",
				"cat /sandman/secrets/foo > ${OUT}/mounted; echo ${SECRET_FOO} > ${OUT}/env"},
			Secrets: []client.SecretMount{
				{Name: secretName, Key: "foo", MountPath: "/sandman/secrets", EnvVar: "SECRET_FOO"},
			},
		},
		Input: &client.Input{Repo: repo, Glob: "/"},
	}
	mustPipeline(t, p)
	cm := commitFiles(t, repo, "master", map[string]string{"file2": "y\n"})
	jobs := flushOK(t, cm.ID)

	b, err := c.GetFile(jobs[0].OutputCommit, "mounted")
	if err != nil {
		t.Fatalf("read mounted secret: %v", err)
	}
	if string(b) != "secretfoo" {
		t.Fatalf("mounted secret = %q, want %q", string(b), "secretfoo")
	}
	b, err = c.GetFile(jobs[0].OutputCommit, "env")
	if err != nil {
		t.Fatalf("read secret env: %v", err)
	}
	if got := strings.TrimRight(string(b), "\n"); got != "secretfoo" {
		t.Fatalf("secret env var = %q, want %q", got, "secretfoo")
	}
}

// TestSB051_SecretBindingRejectsMissing — a pipeline may reference a
// secret only through an explicit binding to an existing secret (D-05).
func TestSB051_SecretBindingRejectsMissing(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", "true"},
			Secrets: []client.SecretMount{
				{Name: "does-not-exist", Key: "k", EnvVar: "K"},
			},
		},
		Input: &client.Input{Repo: repo, Glob: "/"},
	}
	err := c.CreatePipeline(p)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("create err = %v, want missing-secret error", err)
	}
}

// TestSB051_SecretBindingSurvivesRestart — the binding is durable state,
// not memory (D-05): after the daemon restarts, the pipeline still
// carries its secret mount and a new commit is processed with the secret
// available exactly as before.
func TestSB051_SecretBindingSurvivesRestart(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	commitFiles(t, repo, "master", map[string]string{"file": "x\n"})

	secretName := uniq(t)
	if err := c.CreateSecret(secretName, map[string]string{"foo": "restartfoo"}); err != nil {
		t.Fatalf("create secret: %v", err)
	}
	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", "cat /sandman/secrets/foo > ${OUT}/mounted; echo ${SECRET_FOO} > ${OUT}/env"},
			Secrets: []client.SecretMount{
				{Name: secretName, Key: "foo", MountPath: "/sandman/secrets", EnvVar: "SECRET_FOO"},
			},
		},
		Input: &client.Input{Repo: repo, Glob: "/"},
	}
	mustPipeline(t, p)

	restartDaemon(t)

	// the pipeline survives with its binding intact…
	pi, err := c.InspectPipeline(p.Name)
	if err != nil {
		t.Fatalf("inspect after restart: %v", err)
	}
	if pi.Transform == nil || len(pi.Transform.Secrets) != 1 ||
		pi.Transform.Secrets[0].Name != secretName {
		t.Fatalf("binding after restart = %+v, want the secret mount", pi.Transform)
	}
	// …and a fresh commit is processed with the secret mounted
	cm := commitFiles(t, repo, "master", map[string]string{"file2": "y\n"})
	jobs := flushOK(t, cm.ID)
	for _, f := range []string{"mounted", "env"} {
		b, err := c.GetFile(jobs[0].OutputCommit, f)
		if err != nil {
			t.Fatalf("read %s after restart: %v", f, err)
		}
		if got := strings.TrimRight(string(b), "\n"); got != "restartfoo" {
			t.Fatalf("%s after restart = %q, want %q", f, got, "restartfoo")
		}
	}
}
