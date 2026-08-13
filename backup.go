package main

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"net/http"
	"path/filepath"
)

// backupH streams the entire control-plane state as a tar.gz: the store's
// own dirs (repos, tags) under the store write lock — the single-writer's
// buffer, pending repo writes queue on the mutex and land after the
// snapshot — then the daemon-owned dirs, whose writes are individually
// atomic (tmp+rename). Restore: stop the daemon, extract the archive into
// the state dir, start the daemon.
func (d *daemon) backupH(w http.ResponseWriter, r *http.Request) error {
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", `attachment; filename="sandman-backup.tar.gz"`)
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	if err := d.store.BackupTar(tw); err != nil {
		return err
	}
	for _, dir := range []string{"jobs", "pipelines", "dedup", "logs", "spout", "triggers", "secrets", "transactions"} {
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
