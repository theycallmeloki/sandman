package main

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
)

// backupH streams the entire control-plane state as a tar.gz: the store's
// own dirs (repos, tags) under the store write lock — the single-writer's
// buffer, pending repo writes queue on the mutex and land after the
// snapshot — then the daemon-owned dirs, whose writes are individually
// atomic (tmp+rename). Restore: stop the daemon, extract the archive into
// the state dir, start the daemon.
//
// The archive deliberately carries no secret material. A backup is a
// state snapshot, not a credential dump: the secret store (secrets/) is
// omitted, and the per-job secret bind directories (jobs/<id>/secrets/),
// which hold plaintext copies of mounted values, are pruned from the jobs
// tree. Everything else under those dirs — job records, logs, outputs —
// is preserved. Secret bindings in pipeline specs and job records name
// secrets only; the values must be recreated (or restored from an
// operator-controlled store) after a restore.
func (d *daemon) backupH(w http.ResponseWriter, r *http.Request) error {
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", `attachment; filename="sandman-backup.tar.gz"`)
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	if err := d.store.BackupTar(tw); err != nil {
		return err
	}
	// jobs first, with the secret mount dirs pruned; then every other
	// daemon-owned dir. "secrets" is absent on purpose (see above).
	if err := d.store.TarDirFiltered(tw, filepath.Join(d.state, "jobs"), "jobs", skipJobSecretMounts); err != nil {
		return err
	}
	for _, dir := range []string{"pipelines", "dedup", "logs", "spout", "triggers", "transactions"} {
		if err := d.store.TarDir(tw, filepath.Join(d.state, dir), dir); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("backup tar close: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("backup gzip close: %w", err)
	}
	return nil
}

// skipJobSecretMounts reports whether a path under the jobs tree is a
// per-job secret bind dir (jobs/<id>/secrets[/...]) or anything beneath
// it. The bind dir is named "secrets" only by the secret-binding code
// (execute.go, service.go), so pruning that subtree cannot drop job
// artifacts.
func skipJobSecretMounts(rel string, _ bool) bool {
	parts := strings.Split(rel, "/")
	return len(parts) >= 2 && parts[1] == "secrets"
}
