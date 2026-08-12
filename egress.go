// Egress (SB-013, D-17): a configured external output destination the
// job's finished output is copied to after the output commit succeeds.
// A failed egress write fails the job with an egress-related reason even
// though the output commit itself succeeded — output success alone never
// makes the job successful when the destination could not be written.
package main

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"sandman/client"
)

// runEgress copies the job's finished output commit to the pipeline's
// egress destination. Supported destinations: file://<dir> (the output
// files are materialized into the directory, replacing its previous
// contents). Any other scheme is refused — the job then fails with an
// egress reason, never a silent success.
func (d *daemon) runEgress(pl pipelineRec, outCommit client.Commit) error {
	u, err := url.Parse(pl.Pipeline.Egress.URL)
	if err != nil {
		return fmt.Errorf("invalid egress URL: %w", err)
	}
	switch u.Scheme {
	case "file":
		dir := u.Path
		if dir == "" {
			return fmt.Errorf("file egress destination is empty")
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create egress destination: %w", err)
		}
		// replace the destination's previous contents with the commit's
		// files, then materialize the output view
		entries, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("read egress destination: %w", err)
		}
		for _, e := range entries {
			if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
				return fmt.Errorf("clear egress destination: %w", err)
			}
		}
		if err := d.store.MaterializeInput(outCommit.ID, dir); err != nil {
			return fmt.Errorf("copy output to egress destination: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported egress destination scheme %q", u.Scheme)
	}
}
