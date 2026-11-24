package main

// Secret material on disk stays private to the daemon's user: the secret
// store directory and the per-job bind directories are 0700, and every
// file that carries a secret value is 0600. These helpers are the single
// place that policy is enforced — the store handlers (api.go) and the
// job/service secret bindings (execute.go, service.go) all go through
// them.

import (
	"log"
	"os"
	"path/filepath"
)

const (
	secretDirMode  os.FileMode = 0o700
	secretFileMode os.FileMode = 0o600
)

// ensureSecretDir creates dir (and any missing parents) and enforces
// secretDirMode even when the directory already existed with a wider
// mode: MkdirAll alone would leave a pre-existing 0755 dir untouched.
func ensureSecretDir(dir string) error {
	if err := os.MkdirAll(dir, secretDirMode); err != nil {
		return err
	}
	return os.Chmod(dir, secretDirMode)
}

// writeSecretFile writes value to dir/key with secretFileMode, tightening
// a pre-existing file's mode as well. key must be a single path component
// (the callers validate or derive it from a validated name).
func writeSecretFile(dir, key, value string) error {
	p := filepath.Join(dir, key)
	if err := os.WriteFile(p, []byte(value), secretFileMode); err != nil {
		return err
	}
	return os.Chmod(p, secretFileMode)
}

// hardenSecretStore migrates an existing secret store to the private
// modes on daemon startup: <state>/secrets 0700 and each secret file
// 0600, repairing state written by an earlier release that created them
// 0755/0644. Best-effort by design — a missing store (fresh install, or
// secrets never used) is normal, and a permissions failure on one entry
// must never keep the daemon from starting; it is logged instead.
func hardenSecretStore(state string) {
	dir := filepath.Join(state, "secrets")
	info, err := os.Stat(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("secret store: %v", err)
		}
		return
	}
	if !info.IsDir() {
		return
	}
	if err := os.Chmod(dir, secretDirMode); err != nil {
		log.Printf("secret store: chmod %s: %v", dir, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("secret store: read %s: %v", dir, err)
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue // one secret is one file; nothing nested
		}
		p := filepath.Join(dir, e.Name())
		if err := os.Chmod(p, secretFileMode); err != nil {
			log.Printf("secret store: chmod %s: %v", p, err)
		}
	}
}
