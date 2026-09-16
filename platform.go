package main

import (
	"os"
	"path/filepath"
	"runtime"
)

// linuxStateDir is the state directory on Linux: the FHS location for
// variable application state, and where every sandman install has kept
// its repos, jobs, and registry until now.
const linuxStateDir = "/var/lib/sandman"

// defaultState is the state directory a verb uses when -state is not
// given: $SANDMAN_STATE when set, else the per-OS default.
//
// Linux keeps /var/lib/sandman. macOS has no /var/lib, and the state
// directory is not merely a dotfile: every job's input/output/staging
// directory is bind-mounted into a container from under it, so it must
// live in a path the container VM can actually share. The per-user
// Application Support directory satisfies both: it is user-owned (no
// root, so the daemon runs as the logged-in user next to Docker Desktop)
// and it is inside a Docker Desktop shared path, without which the
// mounts would silently come up empty.
func defaultState() string {
	if v := os.Getenv("SANDMAN_STATE"); v != "" {
		return v
	}
	if runtime.GOOS == "darwin" {
		if dir, err := os.UserConfigDir(); err == nil && dir != "" {
			return filepath.Join(dir, "sandman")
		}
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			return filepath.Join(home, "Library", "Application Support", "sandman")
		}
	}
	return linuxStateDir
}

// stateFlagHelp is the -state flag's usage text: the default is resolved
// at runtime, so it cannot be spelled out as one literal.
const stateFlagHelp = "state directory (default $SANDMAN_STATE, else /var/lib/sandman; macOS: ~/Library/Application Support/sandman)"
