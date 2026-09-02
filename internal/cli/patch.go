// patchCmd — deliver a git checkout's edits to a git-input mapped
// repository as a delta. The verb computes the edit by diffing the
// checkout's worktree against a base revision (default: HEAD — the
// uncommitted-edits mode an agent loop wants), then POSTs
// /api/v1/git/delta to the control plane, where the pipeline whose git
// input binds the repo URL and tracked branch commits the edit as one
// new revision and re-triggers — the sandman-native "keep editing the
// codebase with patches" loop, no git server or credentials anywhere in
// the control plane. The checkout stays untouched: nothing is staged,
// committed, or pushed by this verb.
package cli

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

// gitOut runs git in dir and returns trimmed stdout; the error carries
// git's stderr when the command fails.
func gitOut(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %v%s", strings.Join(args, " "), err, trimStderr(stderr.String()))
	}
	return strings.TrimSpace(string(out)), nil
}

func trimStderr(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	return ": " + s
}

// httpsRepoURL rewrites a checkout's remote into the https clone URL the
// sandman git-input vocabulary accepts (https scheme, host, .git
// suffix — see validateGitURL). git@host:path and ssh://git@host/path
// forms become https://host/path, git:// becomes https://; an already
// https URL passes through; anything else is refused.
func httpsRepoURL(u string) (string, error) {
	switch {
	case strings.HasPrefix(u, "https://"):
		return u, nil
	case strings.HasPrefix(u, "http://"):
		return "", fmt.Errorf("remote %q is http — sandman git inputs take https clone URLs (pass --url https://…)", u)
	case strings.HasPrefix(u, "git@"):
		host, path, ok := strings.Cut(u[len("git@"):], ":")
		if !ok || host == "" || path == "" {
			return "", fmt.Errorf("cannot parse scp-style remote %q", u)
		}
		return "https://" + host + "/" + path, nil
	case strings.HasPrefix(u, "ssh://"):
		rest := u[len("ssh://"):]
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			host, path := rest[:i], rest[i+1:]
			if j := strings.IndexByte(host, '@'); j >= 0 {
				host = host[j+1:]
			}
			return "https://" + host + "/" + path, nil
		}
		return "", fmt.Errorf("cannot parse ssh remote %q", u)
	case strings.HasPrefix(u, "git://"):
		return "https://" + u[len("git://"):], nil
	}
	return "", fmt.Errorf("unsupported remote %q — pass --url with an https clone URL", u)
}

// buildPatchDelta computes the edit the checkout's worktree holds against
// base: files maps every added or changed path (tracked and untracked) to
// its worktree content, deleted lists every path the edit removes, and
// url is the https clone URL the edit binds (--url, else the checkout's
// origin remote rewritten to https). base defaults to the checkout's HEAD
// — the uncommitted-edits mode: revision is the current HEAD either way,
// so an agent loop that never commits keeps a constant base and every
// delta applies cleanly. With edits committed locally, pass --base <the
// fork revision> (e.g. HEAD^).
func buildPatchDelta(dir, repoURL, baseArg string) (files map[string]string, deleted []string, revision, base, url string, err error) {
	base = baseArg
	if base == "" {
		if base, err = gitOut(dir, "rev-parse", "HEAD"); err != nil {
			return nil, nil, "", "", "", fmt.Errorf("resolving HEAD: %v", err)
		}
	}
	if revision, err = gitOut(dir, "rev-parse", "HEAD"); err != nil {
		return nil, nil, "", "", "", fmt.Errorf("resolving HEAD: %v", err)
	}
	if repoURL == "" {
		var origin string
		if origin, err = gitOut(dir, "remote", "get-url", "origin"); err != nil {
			return nil, nil, "", "", "", fmt.Errorf("no --url and no origin remote: %v", err)
		}
		repoURL = origin
	}
	if url, err = httpsRepoURL(repoURL); err != nil {
		return nil, nil, "", "", "", err
	}

	files = map[string]string{}
	removed := map[string]bool{}
	// tracked changes vs base (--no-renames keeps one path per record: a
	// rename is a delete plus an add). With -z the records are
	// code NUL path NUL …
	statuses, err := gitOut(dir, "diff", "--name-status", "-z", "--no-renames", base)
	if err != nil {
		return nil, nil, "", "", "", fmt.Errorf("diffing vs %s: %v", base, err)
	}
	segs := strings.Split(statuses, "\x00")
	for i := 0; i+1 < len(segs); i += 2 {
		code, p := segs[i], segs[i+1]
		if code == "" {
			continue
		}
		if code[0] == 'D' {
			removed[p] = true
			continue
		}
		content, rerr := worktreeContent(dir, p)
		if rerr != nil {
			if os.IsNotExist(rerr) {
				removed[p] = true
				continue
			}
			return nil, nil, "", "", "", rerr
		}
		files[p] = content
	}
	// untracked (new) files are additions: git diff never lists them
	untracked, err := gitOut(dir, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, nil, "", "", "", fmt.Errorf("listing untracked files: %v", err)
	}
	for _, p := range strings.Split(untracked, "\x00") {
		if p == "" || removed[p] {
			continue
		}
		content, rerr := worktreeContent(dir, p)
		if rerr != nil {
			if os.IsNotExist(rerr) {
				continue
			}
			return nil, nil, "", "", "", rerr
		}
		files[p] = content
	}
	deleted = make([]string, 0, len(removed))
	for p := range removed {
		deleted = append(deleted, p)
	}
	sort.Strings(deleted)
	return files, deleted, revision, base, url, nil
}

// worktreeContent reads a path's worktree bytes. A symlink contributes
// its link target (what git would store as the blob). The git-input wire
// format transports content as UTF-8 text; binary edits are out of scope
// for the delta receiver (full-tree pushes share the same text contract).
func worktreeContent(dir, p string) (string, error) {
	full := filepath.Join(dir, filepath.FromSlash(p))
	info, err := os.Lstat(full)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(full)
		if err != nil {
			return "", err
		}
		return target, nil
	}
	b, err := os.ReadFile(full)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// patchCmd is `sandman patch`: deliver a checkout's edits as a delta.
func patchCmd() *cobra.Command {
	var repoURL, branch, baseArg string
	var private bool
	cmd := &cobra.Command{
		Use:   "patch [flags] [dir]",
		Short: "deliver a git checkout's edits as a delta to a git-input repo",
		Long: `Deliver the edits a git checkout holds as a delta: the changed and
added paths' content plus the deleted paths are POSTed to the control
plane's git delta receiver, where the pipeline whose git input binds the
checkout's remote URL and the tracked branch commits the edit as one new
revision and re-triggers. The checkout itself is never modified — nothing
is staged, committed, or pushed.

The base defaults to the checkout's HEAD (uncommitted-edits mode, the
agent loop: constant base, every delta applies); edits committed locally
need --base <the revision they fork from> (e.g. HEAD^) so the delta
matches what the mapped repository already holds. Content travels as
UTF-8 text (the git-input wire format; binary file edits are out of
scope for deltas).

  sandman patch                      # deliver worktree edits vs HEAD
  sandman patch ~/w/sandman --base HEAD~1
  sandman patch --url https://github.com/you/repo.git
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			dir := "."
			if len(args) == 1 {
				dir = args[0]
			}
			if _, err := os.Stat(filepath.Join(dir, ".git")); err != nil {
				dieErr("patch", fmt.Errorf("%s is not a git checkout", dir), "run inside a repository (or pass its path)")
			}
			files, deleted, revision, base, url, err := buildPatchDelta(dir, repoURL, baseArg)
			if err != nil {
				dieErr("patch", err, "")
			}
			if len(files) == 0 && len(deleted) == 0 {
				fmt.Printf("no changes vs %s; nothing delivered\n", base)
				return
			}
			if branch == "" {
				branch = "master"
			}
			if err := cliClient().PushGitDelta(url, branch, revision, base, files, deleted, private); err != nil {
				dieErr("patch", err, "the delta's base must match the revision last delivered for the mapped repo")
			}
			fmt.Printf("delivered %d changed / %d deleted vs %s to %s@%s\n",
				len(files), len(deleted), base, url, branch)
		},
	}
	cmd.Flags().StringVar(&repoURL, "url", "", "external repo clone URL (default: the checkout's origin remote, ssh rewritten to https)")
	cmd.Flags().StringVar(&branch, "branch", "", "tracked branch (default master)")
	cmd.Flags().StringVar(&baseArg, "base", "", "revision the edits are against (default: the checkout's HEAD)")
	cmd.Flags().BoolVar(&private, "private", false, "mark the repository inaccessible (no commit; fails bound pipelines)")
	return cmd
}
