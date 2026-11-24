package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sandman/internal/store"
)

// TestBackupExcludesSecrets pins the confidentiality contract: a backup
// never carries plaintext secret material — not the secret store
// (secrets/) and not the per-job bind dirs (jobs/<id>/secrets/) that
// hold mounted copies — while it does carry the rest of the job state.
func TestBackupExcludesSecrets(t *testing.T) {
	state := t.TempDir()
	d := &daemon{state: state, store: store.New(state)}

	const secretValue = "s3cr3t-value"
	if err := ensureSecretDir(filepath.Join(state, "secrets")); err != nil {
		t.Fatal(err)
	}
	if err := writeSecretFile(filepath.Join(state, "secrets"), "webhook.json", `{"data":{"k":"`+secretValue+`"}}`); err != nil {
		t.Fatal(err)
	}
	// a job's mounted secret copy, plus ordinary job state
	jobDir := filepath.Join(state, "jobs", "job1")
	mountDir := filepath.Join(jobDir, "secrets", "0")
	if err := ensureSecretDir(mountDir); err != nil {
		t.Fatal(err)
	}
	if err := writeSecretFile(mountDir, "k", secretValue); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(jobDir, "job.json"), []byte(`{"id":"job1"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(state, "logs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "logs", "job1.jsonl"), []byte(`{"line":"hello"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	if err := d.backupH(rec, httptest.NewRequest("GET", "/api/v1/backup", nil)); err != nil {
		t.Fatalf("backup: %v", err)
	}

	gz, err := gzip.NewReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("gzip: %v", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	sawJob, sawLog := false, false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar: %v", err)
		}
		name := hdr.Name
		if name == "secrets/" || strings.HasPrefix(name, "secrets/") ||
			strings.Contains(name, "/secrets/") || strings.HasSuffix(name, "/secrets") {
			t.Fatalf("backup contains secret path %q", name)
		}
		if name == "jobs/job1/job.json" {
			sawJob = true
		}
		if strings.HasPrefix(name, "logs/") {
			sawLog = true
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if strings.Contains(string(body), secretValue) {
			t.Fatalf("backup entry %q contains the secret value", name)
		}
	}
	if !sawJob {
		t.Fatal("backup dropped the job record (only the secret subtree should be pruned)")
	}
	if !sawLog {
		t.Fatal("backup dropped the logs")
	}
}

// TestSkipJobSecretMounts pins the prune predicate: exactly the bind
// subtree under a job is skipped, never a same-named directory elsewhere.
func TestSkipJobSecretMounts(t *testing.T) {
	skip := map[string]bool{
		"job1/secrets":        true,
		"job1/secrets/0":      true,
		"job2/secrets/a/b":    true,
		"job1/out/secrets":    false,
		"job1/job.json":       false,
		"job1/secretsfoo":     false,
		"secrets":             false, // not under a job
		"job1/volumes/secret": false,
	}
	for rel, want := range skip {
		if got := skipJobSecretMounts(rel, true); got != want {
			t.Errorf("skipJobSecretMounts(%q) = %v, want %v", rel, got, want)
		}
	}
}
