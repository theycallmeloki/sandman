package main

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sandman/client"
	"sandman/internal/store"
)

func permOf(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Mode().Perm()
}

// TestSecretStoreModes pins the store's on-disk confidentiality: the
// secrets/ dir is 0700 and each secret record 0600, both on create and
// after the startup migration hardens state written by an earlier
// release.
func TestSecretStoreModes(t *testing.T) {
	state := t.TempDir()
	d := &daemon{state: state, store: store.New(state)}
	h := d.apiHandler()

	req := httptest.NewRequest("POST", "/api/v1/secrets", strings.NewReader(`{"name":"modey","data":{"k":"v"}}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("create secret: %d %q", rec.Code, rec.Body.String())
	}
	dir := filepath.Join(state, "secrets")
	file := filepath.Join(dir, "modey.json")
	if got := permOf(t, dir); got != 0o700 {
		t.Fatalf("secrets dir mode = %#o, want 0700", got)
	}
	if got := permOf(t, file); got != 0o600 {
		t.Fatalf("secret file mode = %#o, want 0600", got)
	}

	// simulate state written by an earlier release, then restart
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(file, 0o644); err != nil {
		t.Fatal(err)
	}
	hardenSecretStore(state)
	if got := permOf(t, dir); got != 0o700 {
		t.Fatalf("migrated secrets dir mode = %#o, want 0700", got)
	}
	if got := permOf(t, file); got != 0o600 {
		t.Fatalf("migrated secret file mode = %#o, want 0600", got)
	}
}

// TestHardenSecretStoreAbsent asserts startup never fails for a missing
// store: a fresh state dir simply has no secrets yet.
func TestHardenSecretStoreAbsent(t *testing.T) {
	state := t.TempDir()
	hardenSecretStore(state) // must not panic or create anything
	if _, err := os.Stat(filepath.Join(state, "secrets")); !os.IsNotExist(err) {
		t.Fatalf("hardenSecretStore touched a missing store: %v", err)
	}
}

// TestServiceSecretMountModes pins the per-job bind dir: 0700 with 0600
// files, so a mounted value is never world-readable on the daemon host.
func TestServiceSecretMountModes(t *testing.T) {
	state := t.TempDir()
	if err := ensureSecretDir(filepath.Join(state, "secrets")); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(map[string]any{"name": "s", "type": "Opaque", "data": map[string]string{"k": "v"}})
	if err := writeSecretFile(filepath.Join(state, "secrets"), "s.json", string(b)); err != nil {
		t.Fatal(err)
	}
	d := &daemon{state: state}
	pl := pipelineRec{Pipeline: client.Pipeline{Transform: &client.Transform{Secrets: []client.SecretMount{
		{Name: "s", Key: "k", MountPath: "/run/secrets"},
	}}}}
	_, mounts, err := d.serviceSecretEnv("job-modes", pl, nil, nil)
	if err != nil {
		t.Fatalf("serviceSecretEnv: %v", err)
	}
	hostDir := ""
	for _, m := range mounts {
		if strings.HasSuffix(m, ":/run/secrets") {
			hostDir = strings.TrimSuffix(m, ":/run/secrets")
		}
	}
	if hostDir == "" {
		t.Fatalf("no mount in %v", mounts)
	}
	if got := permOf(t, hostDir); got != 0o700 {
		t.Fatalf("secret mount dir mode = %#o, want 0700", got)
	}
	if got := permOf(t, filepath.Join(hostDir, "k")); got != 0o600 {
		t.Fatalf("secret mount file mode = %#o, want 0600", got)
	}
}
