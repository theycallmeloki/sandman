// Secrets: named typed metadata blobs with create/inspect/list/delete,
// and binding into job environments.
package conformance

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// TestSecrets_NameValidation — a secret name must be a plain name: names
// with path separators or traversal components are rejected before any
// filesystem touch, so a crafted name cannot read, write, or delete
// arbitrary files on the daemon host through the secrets handlers.
func TestSecrets_NameValidation(t *testing.T) {
	if err := c.CreateSecret("../escape", map[string]string{"k": "v"}); err == nil {
		t.Fatalf("create with traversal name: expected error")
	}
	if err := c.CreateSecret("a/b", map[string]string{"k": "v"}); err == nil {
		t.Fatalf("create with slash name: expected error")
	}
	// a delete with a traversal name must not remove anything: plant a
	// victim file beside the secrets dir and confirm it survives
	victim := filepath.Join(daemonStateDir, "victim.json")
	if err := os.WriteFile(victim, []byte("{}"), 0o644); err != nil {
		t.Fatalf("plant victim: %v", err)
	}
	defer os.Remove(victim)
	if err := c.DeleteSecret("../victim"); err == nil {
		t.Fatalf("delete with traversal name: expected error")
	}
	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("victim file removed by traversal delete: %v", err)
	}
	// a valid name still round-trips
	name := uniq(t)
	if err := c.CreateSecret(name, map[string]string{"k": "v"}); err != nil {
		t.Fatalf("create valid secret: %v", err)
	}
	if err := c.DeleteSecret(name); err != nil {
		t.Fatalf("delete valid secret: %v", err)
	}
}

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
			Image: "alpine:3.21",
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
// secret only through an explicit binding to an existing secret.
func TestSB051_SecretBindingRejectsMissing(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine:3.21",
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
// not memory: after the daemon restarts, the pipeline still
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
			Image: "alpine:3.21",
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

// TestSB051_SameMountPathMerges — references to several keys at one
// MountPath merge into a single bind mount: the pachyderm-style
// {name, mountPath} pattern declares multiple keys at one path, and
// docker rejects duplicate mount points for the same container path
// (the pre-fix behavior failed the job with exit 125).
func TestSB051_SameMountPathMerges(t *testing.T) {
	withContainerDaemon(t)
	repo := uniq(t)
	mustRepo(t, repo)
	commitFiles(t, repo, "master", map[string]string{"file": "x\n"})
	secretName := uniq(t)
	if err := c.CreateSecret(secretName, map[string]string{"foo": "secretfoo", "bar": "secretbar"}); err != nil {
		t.Fatalf("create secret: %v", err)
	}

	t.Run("two keys share one mount path", func(t *testing.T) {
		p := client.Pipeline{
			Name: uniq(t),
			Transform: &client.Transform{
				Image: "alpine:3.21",
				Cmd: []string{"sh", "-c",
					"cat /sandman/secrets/foo > ${OUT}/a; cat /sandman/secrets/bar > ${OUT}/b"},
				Secrets: []client.SecretMount{
					{Name: secretName, Key: "foo", MountPath: "/sandman/secrets"},
					{Name: secretName, Key: "bar", MountPath: "/sandman/secrets"},
				},
			},
			Input: &client.Input{Repo: repo, Glob: "/"},
		}
		mustPipeline(t, p)
		cm := commitFiles(t, repo, "master", map[string]string{"file2": "y\n"})
		jobs := flushOK(t, cm.ID)
		for _, tc := range []struct{ path, want string }{
			{"a", "secretfoo"},
			{"b", "secretbar"},
		} {
			b, err := c.GetFile(jobs[0].OutputCommit, tc.path)
			if err != nil {
				t.Fatalf("read %s: %v", tc.path, err)
			}
			if string(b) != tc.want {
				t.Fatalf("%s = %q, want %q", tc.path, string(b), tc.want)
			}
		}
	})

	t.Run("duplicate key on one path is rejected", func(t *testing.T) {
		p := client.Pipeline{
			Name: uniq(t),
			Transform: &client.Transform{
				Image: "alpine:3.21",
				Cmd:   []string{"sh", "-c", "true"},
				Secrets: []client.SecretMount{
					{Name: secretName, Key: "foo", MountPath: "/sandman/secrets"},
					{Name: secretName, Key: "foo", MountPath: "/sandman/secrets"},
				},
			},
			Input: &client.Input{Repo: repo, Glob: "/"},
		}
		mustPipeline(t, p)
		cm := commitFiles(t, repo, "master", map[string]string{"file3": "z\n"})
		jobs, err := c.Flush(cm.ID, 60*time.Second)
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
		var failed *client.Job
		for i := range jobs {
			if jobs[i].State == "failure" {
				failed = &jobs[i]
			}
		}
		if failed == nil {
			t.Fatalf("duplicate-mount jobs = %+v, want the pipeline's failure", jobs)
		}
		if !strings.Contains(failed.Reason, "already mounted") {
			t.Fatalf("job reason %q, want the duplicate-key message", failed.Reason)
		}
	})
}
