package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"sandman/client"
	"sandman/internal/store"
)

// The data-plane CLI on spf13/cobra (D-19: the CLI is a second consumer
// of the same client package the conformance suite drives — semantic and
// command-level compatibility with the reference, no wire compatibility).
// The reference surface is tracked verb by verb in
// sandman-behaviour-notes/implementation-review/CLI_SURFACE.md.
//
// addr selects the control plane for every verb; the sandman binary sets
// it from its global -addr flag (parsed by the std flag package before
// cobra sees the args) via SetAddr.

var addr string

// SetAddr selects the control-plane address for the data-plane verbs.
func SetAddr(a string) { addr = a }

// binVersion is the baked build version, injected by main (which owns
// the -ldflags -X main.Version wiring and the daemon's /api/v1/version).
var binVersion = "0.0.1"

// SetVersion installs the binary's baked version for PrintVersion.
func SetVersion(v string) { binVersion = v }

// die is the CLI's fatal-error exit: print the message to stderr and
// exit with the given code.
func die(msg string, code int) {
	fmt.Fprintln(os.Stderr, "sandman:", msg)
	os.Exit(code)
}

// listingGlob turns a file-list [:path] argument into the server's
// prefix glob: the listing API accepts only prefix patterns ("prefix*",
// SB-047 clause 4), so a path without its own wildcard is made a prefix
// of the listing — `file list r@master:subdir` lists everything under
// subdir, exactly as the [:path] help advertises. A path that already
// carries a * is passed through unchanged.
func listingGlob(path string) string {
	if !strings.Contains(path, "*") {
		return path + "*"
	}
	return path
}

// PrintVersion reports the binary's baked version and, when a control
// plane answers, the daemon's version — a stale daemon is visible at a
// glance (the two come from the same binary, so a mismatch means the
// daemon predates this build).
func PrintVersion() {
	fmt.Printf("sandman %s\n", binVersion)
	c := client.New(addr)
	ver, err := c.Version()
	if err != nil {
		fmt.Printf("daemon: not reachable at %s\n", addr)
		return
	}
	fmt.Printf("daemon: %s (%s)\n", ver, addr)
}

func cliClient() *client.Client {
	return client.New(addr)
}

// Commands returns the data-plane subtree (the cobra verbs over the
// client package) plus the top-level get and version verbs. The sandman
// binary mounts these under its root command alongside the fleet/runtime
// verbs (daemon, worker, nodes, ...).
func Commands() []*cobra.Command {
	cmds := dataPlaneCommands()
	cmds = append(cmds, getCmd())
	cmds = append(cmds, &cobra.Command{
		Use: "version",
		Run: func(*cobra.Command, []string) { PrintVersion() },
	})
	return cmds
}

// table prints an aligned table with an uppercase header row via
// tabwriter (empty row sets print nothing — the caller prints the
// empty-state message instead).
func table(header []string, rows [][]string) {
	if len(rows) == 0 {
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 4, 8, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, strings.Join(header, "\t")); err != nil {
		return
	}
	for _, r := range rows {
		if _, err := fmt.Fprintln(w, strings.Join(r, "\t")); err != nil {
			return
		}
	}
	_ = w.Flush()
}

// humanSize renders a byte count for humans (42 B, 1.5 KB, 3.2 MB).
func HumanSize(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}

// parseRef splits repo[@branch][:path]; branch defaults to "master".
func parseRef(s string) (repo, branch, path string, err error) {
	if s == "" {
		return "", "", "", fmt.Errorf("empty repository name in %q", s)
	}
	at := strings.Index(s, "@")
	if at < 0 {
		// plain repo name: master branch, no path
		return s, "master", "", nil
	}
	repo = s[:at]
	rest := s[at+1:]
	if colon := strings.Index(rest, ":"); colon >= 0 {
		branch, path = rest[:colon], rest[colon+1:]
	} else {
		branch = rest
	}
	// store paths are repo-relative; accept pachctl-style /path and
	// normalize (a leading slash is pure convention, not a filesystem root)
	path = strings.TrimPrefix(path, "/")
	if repo == "" {
		return "", "", "", fmt.Errorf("empty repository name in %q", s)
	}
	return repo, branch, path, nil
}

// resolveCommitRef resolves a repo@ref segment to a commit id: a 16-hex
// ref is a commit id — the canonical explicit-commit flow (start, put by
// id, finish) — and must address that commit, never materialize a
// phantom branch of the same name (F14). Anything else is a branch whose
// head is taken.
func resolveCommitRef(c *client.Client, repo, ref string) (string, error) {
	if store.IsCommitID(ref) {
		if cm, err := c.InspectCommit(ref); err == nil && cm.Repo == repo {
			return ref, nil
		}
	}
	head, err := c.HeadCommit(repo, ref)
	if err != nil {
		return "", err
	}
	return head.ID, nil
}

// dataPlaneCommands returns the data-plane subtree of the root command
// (repo, commit, branch, file, check, job, datum, pipeline, flush, secret,
// tag, logs, transaction).
func dataPlaneCommands() []*cobra.Command {
	return []*cobra.Command{
		newRepoCmd(),
		newCommitCmd(),
		newBranchCmd(),
		newFileCmd(),
		newCheckCmd(),
		newJobCmd(),
		newDatumCmd(),
		newPipelineCmd(),
		newFlushCmd(),
		newSecretCmd(),
		newTagCmd(),
		newLogsCmd(),
		newTransactionCmd(),
		newBackupCmd(),
		newResetCmd(),
	}
}

// newBackupCmd snapshots the full control-plane state to a tar.gz: repos,
// tags, pipelines, jobs, dedup, logs, spout markers, secrets, transactions,
// triggers. The store part is captured under the store's write lock, so
// the archive is a consistent point-in-time state. Restore is manual by
// design: stop the daemon, extract into the state dir, start the daemon.
func newBackupCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "backup [dest]",
		Short: "snapshot the full control-plane state to a tar.gz",
		Args:  cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			dest := "sandman-backup-" + time.Now().UTC().Format("20060102-150405") + ".tar.gz"
			if len(args) == 1 {
				dest = args[0]
			}
			f, err := os.Create(dest)
			if err != nil {
				die("backup: "+err.Error(), 1)
			}
			if err := cliClient().Backup(f); err != nil {
				f.Close()
				// die() is os.Exit — no deferred cleanup runs; a
				// truncated archive must not stay on disk looking valid
				os.Remove(dest)
				die("backup: "+err.Error(), 1)
			}
			if err := f.Close(); err != nil {
				os.Remove(dest)
				die("backup: "+err.Error(), 1)
			}
			fmt.Printf("backed up control-plane state to %s\n", dest)
			fmt.Println("restore: stop the daemon, extract the archive into the state dir, start the daemon")
		},
	}
}

// newResetCmd destroys every pipeline and repository, returning the state
// dir to zero (the internal spec repo survives; the fleet keeps running).
// Requires --yes: this is unrecoverable without a backup.
func newResetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset",
		Short: "destroy every repo and pipeline (state to zero)",
		Run: func(cmd *cobra.Command, _ []string) {
			yes, _ := cmd.Flags().GetBool("yes")
			if !yes {
				die("reset: this destroys every repo and pipeline; pass --yes", 1)
			}
			if err := cliClient().Reset(); err != nil {
				die("reset: "+err.Error(), 1)
			}
			fmt.Println("reset complete — repos, pipelines, jobs, secrets, tags cleared (spec repo recreated)")
		},
	}
	cmd.Flags().Bool("yes", false, "confirm the destructive reset")
	return cmd
}

func newRepoCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "repo", Short: "manage repositories"}
	cmd.AddCommand(
		&cobra.Command{
			Use:  "create <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().CreateRepo(args[0]); err != nil {
					die("repo create: "+err.Error(), 1)
				}
				fmt.Printf("created repo %s\n", args[0])
			},
		},
		&cobra.Command{
			Use: "list",
			Run: func(_ *cobra.Command, _ []string) {
				repos, err := cliClient().ListRepos()
				if err != nil {
					die("repo list: "+err.Error(), 1)
				}
				rows := make([][]string, 0, len(repos))
				for _, r := range repos {
					rows = append(rows, []string{r.Name, HumanSize(r.SizeBytes), strings.Join(r.Branches, ",")})
				}
				table([]string{"NAME", "SIZE", "BRANCHES"}, rows)
				if len(rows) == 0 {
					fmt.Println("no repos")
				}
			},
		},
		&cobra.Command{
			Use:  "inspect <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				r, err := cliClient().InspectRepo(args[0])
				if err != nil {
					die("repo inspect: "+err.Error(), 1)
				}
				fmt.Printf("%-10s : %s\n", "name", r.Name)
				fmt.Printf("%-10s : %s\n", "size", HumanSize(r.SizeBytes))
				fmt.Printf("%-10s : %s\n", "branches", strings.Join(r.Branches, ", "))
			},
		},
	)
	del := &cobra.Command{
		Use:  "delete <name>",
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			if err := cliClient().DeleteRepo(args[0], forceDelete); err != nil {
				die("repo delete: "+err.Error(), 1)
			}
			fmt.Printf("deleted repo %s\n", args[0])
		},
	}
	del.Flags().BoolVar(&forceDelete, "force", false, "force delete even with history")
	cmd.AddCommand(del)
	return cmd
}

var forceDelete bool
var forcePipelineDelete bool

func newCommitCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "commit", Short: "manage commits"}
	cmd.AddCommand(
		&cobra.Command{
			Use:  "start <repo>[@branch]",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				repo, branch, _, err := parseRef(args[0])
				if err != nil {
					die(err.Error(), 2)
				}
				cm, err := cliClient().StartCommit(repo, branch, "")
				if err != nil {
					die("commit start: "+err.Error(), 1)
				}
				fmt.Println(cm.ID)
			},
		},
		&cobra.Command{
			Use:  "finish <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if _, err := cliClient().FinishCommit(args[0], "", false); err != nil {
					die("commit finish: "+err.Error(), 1)
				}
				fmt.Printf("finished commit %s\n", args[0])
			},
		},
		&cobra.Command{
			Use:  "list <repo>[@branch]",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				repo, branch, _, err := parseRef(args[0])
				if err != nil {
					die(err.Error(), 2)
				}
				hist, err := cliClient().CommitHistory(repo, branch)
				if err != nil {
					die("commit list: "+err.Error(), 1)
				}
				rows := make([][]string, 0, len(hist))
				for _, cm := range hist {
					rows = append(rows, []string{cm.ID, cm.Branch, fmt.Sprintf("%t", cm.Finished)})
				}
				table([]string{"ID", "BRANCH", "FINISHED"}, rows)
				if len(rows) == 0 {
					fmt.Println("no commits")
				}
			},
		},
		&cobra.Command{
			Use:  "inspect <id|repo@branch>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				id := args[0]
				if strings.Contains(args[0], "@") {
					repo, branch, _, err := parseRef(args[0])
					if err != nil || repo == "" {
						die("commit inspect: invalid ref "+args[0], 2)
					}
					resolved, err := resolveCommitRef(cliClient(), repo, branch)
					if err != nil {
						die("commit inspect: "+err.Error(), 1)
					}
					id = resolved
				}
				cm, err := cliClient().InspectCommit(id)
				if err != nil {
					die("commit inspect: "+err.Error(), 1)
				}
				fmt.Printf("%-10s : %s\n", "id", cm.ID)
				fmt.Printf("%-10s : %s\n", "repo", cm.Repo)
				fmt.Printf("%-10s : %s\n", "branch", cm.Branch)
				fmt.Printf("%-10s : %t\n", "started", cm.Started)
				fmt.Printf("%-10s : %t\n", "finished", cm.Finished)
			},
		},
		&cobra.Command{
			Use:  "delete <id|repo@branch>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().DeleteCommit(args[0]); err != nil {
					die("commit delete: "+err.Error(), 1)
				}
				fmt.Printf("deleted commit %s\n", args[0])
			},
		},
	)
	return cmd
}

func newBranchCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "branch", Short: "manage branches"}
	cmd.AddCommand(
		&cobra.Command{
			// head defaults to the repo's master head (the reference's
			// `branch create <repo> <branch> [head]`)
			Use:  "create <repo> <branch> [head]",
			Args: cobra.RangeArgs(2, 3),
			Run: func(_ *cobra.Command, args []string) {
				head := ""
				if len(args) == 3 {
					head = args[2]
				} else {
					h, err := cliClient().HeadCommit(args[0], "master")
					if err != nil {
						die("branch create: "+err.Error(), 1)
					}
					head = h.ID
				}
				if err := cliClient().CreateBranch(args[0], args[1], head); err != nil {
					die("branch create: "+err.Error(), 1)
				}
				fmt.Printf("created branch %s@%s\n", args[0], args[1])
			},
		},
		&cobra.Command{
			Use:  "inspect <repo> <branch>",
			Args: cobra.ExactArgs(2),
			Run: func(_ *cobra.Command, args []string) {
				b, err := cliClient().InspectBranch(args[0], args[1])
				if err != nil {
					die("branch inspect: "+err.Error(), 1)
				}
				fmt.Printf("%-10s : %s\n", "repo", b.Repo)
				fmt.Printf("%-10s : %s\n", "branch", b.Branch)
				fmt.Printf("%-10s : %s\n", "head", b.Head)
			},
		},
		&cobra.Command{
			Use:  "list [repo]",
			Args: cobra.MaximumNArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if len(args) == 1 {
					bs, err := cliClient().ListBranches(args[0])
					if err != nil {
						die("branch list: "+err.Error(), 1)
					}
					rows := make([][]string, 0, len(bs))
					for _, b := range bs {
						rows = append(rows, []string{b.Branch, b.Head})
					}
					table([]string{"BRANCH", "HEAD"}, rows)
					if len(rows) == 0 {
						fmt.Println("no branches")
					}
					return
				}
				repos, err := cliClient().ListRepos()
				if err != nil {
					die("branch list: "+err.Error(), 1)
				}
				var rows [][]string
				for _, r := range repos {
					for _, b := range r.Branches {
						rows = append(rows, []string{r.Name, b})
					}
				}
				table([]string{"REPO", "BRANCH"}, rows)
				if len(rows) == 0 {
					fmt.Println("no branches")
				}
			},
		},
		&cobra.Command{
			Use:  "delete <repo> <branch>",
			Args: cobra.ExactArgs(2),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().DeleteBranch(args[0], args[1]); err != nil {
					die("branch delete: "+err.Error(), 1)
				}
				fmt.Printf("deleted branch %s@%s\n", args[0], args[1])
			},
		},
	)
	return cmd
}

// getCmd is the reference's top-level `get file` verb: fetch a file
// from a commit by ref, the canonical recovery path. Equivalent to
// `file get` (same resolution), kept as its own command for parity.
func getCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "get", Short: "fetch files from commits"}
	cmd.AddCommand(&cobra.Command{
		Use:  "file <repo@branch:path>",
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			repo, branch, path, err := parseRef(args[0])
			if err != nil {
				die(err.Error(), 2)
			}
			if path == "" {
				die("get file: path required (repo@branch:path)", 2)
			}
			head, err := resolveCommitRef(cliClient(), repo, branch)
			if err != nil {
				die("get file: "+err.Error(), 1)
			}
			data, err := cliClient().GetFile(head, path)
			if err != nil {
				die("get file: "+err.Error(), 1)
			}
			_, _ = os.Stdout.Write(data)
		},
	})
	return cmd
}

func newFileCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "file", Short: "manage files"}
	put := &cobra.Command{
		Use:  "put <repo@branch:path> <src|->",
		Args: cobra.ExactArgs(2),
		Run: func(_ *cobra.Command, args []string) {
			repo, branch, path, err := parseRef(args[0])
			if err != nil {
				die(err.Error(), 2)
			}
			if path == "" {
				die("file put: path required (repo@branch:path)", 2)
			}
			var data []byte
			if args[1] == "-" {
				data, err = io.ReadAll(os.Stdin)
			} else {
				data, err = os.ReadFile(args[1])
			}
			if err != nil {
				die("file put: "+err.Error(), 1)
			}
			// pachctl errors on puts into repos that do not exist — no
			// silent auto-create (StartCommit would create the repo)
			if _, err := cliClient().InspectRepo(repo); err != nil {
				die("file put: repo "+repo+" not found", 1)
			}
			if store.IsCommitID(branch) {
				// the canonical explicit-commit flow (F14): a commit-id
				// ref writes into that OPEN commit — the put neither
				// starts a new commit nor finishes it (the explicit
				// `commit finish <id>` completes the flow). A branch of
				// the commit's name can only be a phantom.
				cm, err := cliClient().InspectCommit(branch)
				if err != nil {
					die("file put: "+err.Error(), 1)
				}
				if cm.Repo != repo {
					die(fmt.Sprintf("file put: commit %s is in repo %s, not %s", branch, cm.Repo, repo), 1)
				}
				if cm.Finished {
					die(fmt.Sprintf("file put: commit %s is finished; start a new commit to write", branch), 1)
				}
				if fileOverwrite {
					err = cliClient().PutFileOverwrite(branch, path, data)
				} else {
					err = cliClient().PutFile(branch, path, data)
				}
				if err != nil {
					die("file put: "+err.Error(), 1)
				}
				fmt.Printf("wrote %s@%s:%s (%d bytes, commit %s)\n", repo, branch, path, len(data), branch)
				return
			}
			cm, err := cliClient().StartCommit(repo, branch, "")
			if err != nil {
				die("file put: "+err.Error(), 1)
			}
			if fileOverwrite {
				err = cliClient().PutFileOverwrite(cm.ID, path, data)
			} else {
				err = cliClient().PutFile(cm.ID, path, data)
			}
			if err != nil {
				die("file put: "+err.Error(), 1)
			}
			if _, err := cliClient().FinishCommit(cm.ID, "", false); err != nil {
				die("file put: "+err.Error(), 1)
			}
			fmt.Printf("wrote %s@%s:%s (%d bytes, commit %s)\n", repo, branch, path, len(data), cm.ID)
		},
	}
	put.Flags().BoolVarP(&fileOverwrite, "overwrite", "o", false, "overwrite accumulated content at the path (FS-3)")
	cmd.AddCommand(put)

	cmd.AddCommand(
		&cobra.Command{
			Use:  "get <repo@branch:path>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				repo, branch, path, err := parseRef(args[0])
				if err != nil {
					die(err.Error(), 2)
				}
				if path == "" {
					die("file get: path required (repo@branch:path)", 2)
				}
				head, err := resolveCommitRef(cliClient(), repo, branch)
				if err != nil {
					die("file get: "+err.Error(), 1)
				}
				data, err := cliClient().GetFile(head, path)
				if err != nil {
					die("file get: "+err.Error(), 1)
				}
				_, _ = os.Stdout.Write(data)
			},
		},
		&cobra.Command{
			Use:  "inspect <repo@branch:path>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				repo, branch, path, err := parseRef(args[0])
				if err != nil {
					die(err.Error(), 2)
				}
				if path == "" {
					die("file inspect: path required (repo@branch:path)", 2)
				}
				head, err := resolveCommitRef(cliClient(), repo, branch)
				if err != nil {
					die("file inspect: "+err.Error(), 1)
				}
				data, err := cliClient().GetFile(head, path)
				if err != nil {
					die("file inspect: "+err.Error(), 1)
				}
				fmt.Printf("%-10s : %s\n", "path", path)
				fmt.Printf("%-10s : %s\n", "size", HumanSize(uint64(len(data))))
			},
		},
		&cobra.Command{
			Use:  "list <repo@branch>[:path]",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				repo, branch, path, err := parseRef(args[0])
				if err != nil {
					die(err.Error(), 2)
				}
				head, err := resolveCommitRef(cliClient(), repo, branch)
				if err != nil {
					die("file list: "+err.Error(), 1)
				}
				var files []client.FileInfo
				if path != "" {
					files, err = cliClient().ListFilesGlob(head, listingGlob(path))
				} else {
					files, err = cliClient().ListFiles(head)
				}
				if err != nil {
					die("file list: "+err.Error(), 1)
				}
				rows := make([][]string, 0, len(files))
				for _, f := range files {
					rows = append(rows, []string{f.Path, HumanSize(f.Size)})
				}
				table([]string{"PATH", "SIZE"}, rows)
				if len(rows) == 0 {
					fmt.Println("no files")
				}
			},
		},
	)
	copyCmd := &cobra.Command{
		Use:  "copy <src@branch:path> <dst@branch:path>",
		Args: cobra.ExactArgs(2),
		Run: func(_ *cobra.Command, args []string) {
			srcRepo, srcBranch, srcPath, err := parseRef(args[0])
			if err != nil {
				die(err.Error(), 2)
			}
			dstRepo, dstBranch, dstPath, err := parseRef(args[1])
			if err != nil {
				die(err.Error(), 2)
			}
			if srcPath == "" || dstPath == "" {
				die("file copy: paths required (repo@branch:path)", 2)
			}
			srcHead, err := cliClient().HeadCommit(srcRepo, srcBranch)
			if err != nil {
				die("file copy: "+err.Error(), 1)
			}
			dst, err := cliClient().StartCommit(dstRepo, dstBranch, "")
			if err != nil {
				die("file copy: "+err.Error(), 1)
			}
			if err := cliClient().CopyFile(dst.ID, dstPath, srcHead.ID, srcPath, fileOverwrite); err != nil {
				die("file copy: "+err.Error(), 1)
			}
			if _, err := cliClient().FinishCommit(dst.ID, "", false); err != nil {
				die("file copy: "+err.Error(), 1)
			}
			fmt.Printf("copied %s to %s@%s:%s (commit %s)\n", srcPath, dstRepo, dstBranch, dstPath, dst.ID)
		},
	}
	copyCmd.Flags().BoolVarP(&fileOverwrite, "overwrite", "o", false, "overwrite accumulated content at the destination path (FS-3)")
	cmd.AddCommand(copyCmd)

	cmd.AddCommand(
		&cobra.Command{
			Use:  "delete <repo@branch:path>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				repo, branch, path, err := parseRef(args[0])
				if err != nil {
					die(err.Error(), 2)
				}
				if path == "" {
					die("file delete: path required (repo@branch:path)", 2)
				}
				cm, err := cliClient().StartCommit(repo, branch, "")
				if err != nil {
					die("file delete: "+err.Error(), 1)
				}
				if err := cliClient().DeleteFile(cm.ID, path); err != nil {
					die("file delete: "+err.Error(), 1)
				}
				if _, err := cliClient().FinishCommit(cm.ID, "", false); err != nil {
					die("file delete: "+err.Error(), 1)
				}
				fmt.Printf("deleted %s@%s:%s (commit %s)\n", repo, branch, path, cm.ID)
			},
		},
	)
	return cmd
}

var fileOverwrite bool

func newCheckCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "check",
		Short: "consistency check (fsck analog)",
		Run: func(_ *cobra.Command, _ []string) {
			if err := cliClient().Check(); err != nil {
				die("check: "+err.Error(), 1)
			}
			fmt.Println("ok")
		},
	}
}

func newJobCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "job", Short: "manage jobs"}
	cmd.AddCommand(
		&cobra.Command{
			Use:  "list [pipeline]",
			Args: cobra.MaximumNArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				filter := client.JobFilter{}
				if len(args) == 1 {
					filter.Pipeline = args[0]
				}
				js, err := cliClient().ListJobsFiltered(filter)
				if err != nil {
					die("job list: "+err.Error(), 1)
				}
				rows := make([][]string, 0, len(js))
				for _, j := range js {
					rows = append(rows, []string{j.ID, j.Pipeline, j.State})
				}
				table([]string{"ID", "PIPELINE", "STATE"}, rows)
				if len(rows) == 0 {
					fmt.Println("no jobs")
				}
			},
		},
		&cobra.Command{
			Use:  "inspect <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				j, err := cliClient().InspectJob(args[0])
				if err != nil {
					die("job inspect: "+err.Error(), 1)
				}
				fmt.Printf("%-13s : %s\n", "id", j.ID)
				fmt.Printf("%-13s : %s\n", "pipeline", j.Pipeline)
				fmt.Printf("%-13s : %s\n", "state", j.State)
				fmt.Printf("%-13s : %s\n", "reason", j.Reason)
				fmt.Printf("%-13s : %s\n", "outputCommit", j.OutputCommit)
				fmt.Printf("%-13s : %d\n", "processed", j.Processed)
				fmt.Printf("%-13s : %d\n", "recovered", j.Recovered)
				fmt.Printf("%-13s : %d\n", "failed", j.Failed)
				fmt.Printf("%-13s : %d\n", "skipped", j.Skipped)
				if j.StatsCommit != "" {
					fmt.Printf("%-13s : %s\n", "statsCommit", j.StatsCommit)
				}
			},
		},
		&cobra.Command{
			Use:  "delete <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().DeleteJob(args[0]); err != nil {
					die("job delete: "+err.Error(), 1)
				}
				fmt.Printf("deleted job %s\n", args[0])
			},
		},
		&cobra.Command{
			Use:  "stop <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().StopJob(args[0]); err != nil {
					die("job stop: "+err.Error(), 1)
				}
				fmt.Printf("stopped job %s\n", args[0])
			},
		},
		&cobra.Command{
			Use:  "restart-datum <job> <datum>",
			Args: cobra.ExactArgs(2),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().RestartDatum(args[0], args[1]); err != nil {
					die("job restart-datum: "+err.Error(), 1)
				}
				fmt.Printf("restarted datum %s\n", args[1])
			},
		},
	)
	return cmd
}

func newDatumCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "datum", Short: "manage datums"}
	cmd.AddCommand(
		&cobra.Command{
			Use:  "list <job>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				page, err := cliClient().ListDatums(args[0], 0, 0)
				if err != nil {
					die("datum list: "+err.Error(), 1)
				}
				rows := make([][]string, 0, len(page.Datums))
				for _, d := range page.Datums {
					rows = append(rows, []string{d.ID, d.State})
				}
				table([]string{"ID", "STATE"}, rows)
				if len(rows) == 0 {
					fmt.Println("no datums")
				}
			},
		},
		&cobra.Command{
			Use:  "inspect <job> <datum>",
			Args: cobra.ExactArgs(2),
			Run: func(_ *cobra.Command, args []string) {
				d, err := cliClient().InspectDatum(args[0], args[1])
				if err != nil {
					die("datum inspect: "+err.Error(), 1)
				}
				fmt.Printf("%-10s : %s\n", "id", d.ID)
				fmt.Printf("%-10s : %s\n", "state", d.State)
				fmt.Printf("%-10s : %s\n", "reason", d.Reason)
				fmt.Printf("%-10s : %s\n", "started", d.Started)
				fmt.Printf("%-10s : %s\n", "finished", d.Finished)
			},
		},
		&cobra.Command{
			Use:  "restart <job> <datum>",
			Args: cobra.ExactArgs(2),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().RestartDatum(args[0], args[1]); err != nil {
					die("datum restart: "+err.Error(), 1)
				}
				fmt.Printf("restarted datum %s\n", args[1])
			},
		},
	)
	return cmd
}

func newPipelineCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "pipeline", Short: "manage pipelines"}
	create := &cobra.Command{
		Use:  "create -f <spec.json>",
		Args: cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			p, err := readPipelineSpec(specFile)
			if err != nil {
				die("pipeline create: "+err.Error(), 1)
			}
			tx := txID
			if tx == "" {
				tx = activeTx() // transaction resume sets this
			}
			if tx != "" {
				if err := cliClient().CreatePipelineTx(p, tx); err != nil {
					die("pipeline create: "+err.Error(), 1)
				}
			} else if err := cliClient().CreatePipeline(p); err != nil {
				die("pipeline create: "+err.Error(), 1)
			}
			fmt.Printf("created pipeline %s\n", p.Name)
		},
	}
	create.Flags().StringVarP(&specFile, "spec", "f", "-", "pipeline spec JSON file ('-' = stdin)")
	create.Flags().StringVar(&txID, "tx", "", "stage the create in this transaction")
	cmd.AddCommand(create)

	update := &cobra.Command{
		Use:  "update -f <spec.json>",
		Args: cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			p, err := readPipelineSpec(specFile)
			if err != nil {
				die("pipeline update: "+err.Error(), 1)
			}
			p.Update = true
			tx := txID
			if tx == "" {
				tx = activeTx() // transaction resume sets this
			}
			if tx != "" {
				if err := cliClient().CreatePipelineTx(p, tx); err != nil {
					die("pipeline update: "+err.Error(), 1)
				}
			} else if err := cliClient().CreatePipeline(p); err != nil {
				die("pipeline update: "+err.Error(), 1)
			}
			fmt.Printf("updated pipeline %s\n", p.Name)
		},
	}
	update.Flags().StringVarP(&specFile, "spec", "f", "-", "pipeline spec JSON file ('-' = stdin)")
	update.Flags().StringVar(&txID, "tx", "", "stage the update in this transaction")
	cmd.AddCommand(update)

	pdel := &cobra.Command{
		Use:  "delete <name>",
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			if err := cliClient().DeletePipeline(args[0], forcePipelineDelete, false); err != nil {
				die("pipeline delete: "+err.Error(), 1)
			}
			fmt.Printf("deleted pipeline %s\n", args[0])
		},
	}
	pdel.Flags().BoolVar(&forcePipelineDelete, "force", false, "delete a pipeline even if it has downstream consumers")
	cmd.AddCommand(
		&cobra.Command{
			Use: "list",
			Run: func(_ *cobra.Command, _ []string) {
				ps, err := cliClient().ListPipelines()
				if err != nil {
					die("pipeline list: "+err.Error(), 1)
				}
				rows := make([][]string, 0, len(ps))
				for _, p := range ps {
					rows = append(rows, []string{p.Name, p.State, fmt.Sprintf("%d", p.Version)})
				}
				table([]string{"NAME", "STATE", "VERSION"}, rows)
				if len(rows) == 0 {
					fmt.Println("no pipelines")
				}
			},
		},
		&cobra.Command{
			Use:  "inspect <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				p, err := cliClient().InspectPipeline(args[0])
				if err != nil {
					die("pipeline inspect: "+err.Error(), 1)
				}
				fmt.Printf("%-10s : %s\n", "name", p.Name)
				fmt.Printf("%-10s : %s\n", "state", p.State)
				fmt.Printf("%-10s : %d\n", "version", p.Version)
				fmt.Printf("%-10s : %s\n", "reason", p.Reason)
			},
		},
		pdel,
		&cobra.Command{
			Use:  "start <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().StartPipeline(args[0]); err != nil {
					die("pipeline start: "+err.Error(), 1)
				}
				fmt.Printf("started pipeline %s\n", args[0])
			},
		},
		&cobra.Command{
			Use:  "stop <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().StopPipeline(args[0]); err != nil {
					die("pipeline stop: "+err.Error(), 1)
				}
				fmt.Printf("stopped pipeline %s\n", args[0])
			},
		},
		&cobra.Command{
			Use:  "run <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				j, err := cliClient().RunPipeline(args[0], nil, "")
				if err != nil {
					die("pipeline run: "+err.Error(), 1)
				}
				fmt.Println(j.ID)
			},
		},
		&cobra.Command{
			Use:  "run-cron <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().TriggerCron(args[0]); err != nil {
					die("pipeline run-cron: "+err.Error(), 1)
				}
				fmt.Printf("triggered %s\n", args[0])
			},
		},
		&cobra.Command{
			Use:  "extract <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				p, err := cliClient().InspectPipeline(args[0])
				if err != nil {
					die("pipeline extract: "+err.Error(), 1)
				}
				b, err := json.Marshal(p)
				if err != nil {
					die("pipeline extract: "+err.Error(), 1)
				}
				out, err := normalizeSpec(b)
				if err != nil {
					die("pipeline extract: "+err.Error(), 1)
				}
				fmt.Println(string(out))
			},
		},
		&cobra.Command{
			Use:  "edit <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				p, err := cliClient().InspectPipeline(args[0])
				if err != nil {
					die("pipeline edit: "+err.Error(), 1)
				}
				raw, err := json.Marshal(p)
				if err != nil {
					die("pipeline edit: "+err.Error(), 1)
				}
				// the editor file is the normalized spec, not the raw
				// inspection: the strict spec decoder rejects the
				// inspection's state/version/jobCounts fields
				b, err := normalizeSpec(raw)
				if err != nil {
					die("pipeline edit: "+err.Error(), 1)
				}
				f, err := os.CreateTemp("", "sandman-pipeline-*.json")
				if err != nil {
					die("pipeline edit: "+err.Error(), 1)
				}
				name := f.Name()
				defer os.Remove(name)
				if _, err := f.Write(b); err != nil {
					die("pipeline edit: "+err.Error(), 1)
				}
				f.Close()
				editor := os.Getenv("EDITOR")
				if editor == "" {
					editor = "vi"
				}
				cmd := exec.Command("sh", "-c", editor+" "+strconv.Quote(name))
				cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
				if err := cmd.Run(); err != nil {
					die("pipeline edit: "+err.Error(), 1)
				}
				spec, err := readPipelineSpec(name)
				if err != nil {
					die("pipeline edit: "+err.Error(), 1)
				}
				spec.Update = true
				if err := cliClient().CreatePipeline(spec); err != nil {
					die("pipeline edit: "+err.Error(), 1)
				}
				fmt.Printf("updated pipeline %s\n", spec.Name)
			},
		},
	)
	return cmd
}

var (
	specFile string
	txID     string
)

// normalizeSpec strips a pipeline inspection down to its spec shape so
// the result round-trips through `pipeline create -f`: the inspection
// carries state/version/jobCounts that are not spec fields, and the
// strict spec decoder rejects unknown fields.
func normalizeSpec(b []byte) ([]byte, error) {
	var spec client.Pipeline
	if err := json.Unmarshal(b, &spec); err != nil {
		return nil, err
	}
	return json.MarshalIndent(spec, "", "  ")
}

// readPipelineSpec decodes a pipeline spec from the -f source (stdin
// default), the same shape the conformance suite passes to CreatePipeline.
func readPipelineSpec(src string) (client.Pipeline, error) {
	var r io.Reader = os.Stdin
	if src != "-" {
		f, err := os.Open(src)
		if err != nil {
			return client.Pipeline{}, err
		}
		defer f.Close()
		r = f
	}
	var p client.Pipeline
	dec := json.NewDecoder(r)
	// strict: a spec field the decoder does not recognize — a typo, or
	// a pachyderm-shaped port like top-level resource_limits — fails
	// loudly instead of being silently ignored (a quiet resource-policy
	// loss: the ported spec runs with no limits, no error)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return client.Pipeline{}, fmt.Errorf("spec: %w", err)
	}
	return p, nil
}

func newFlushCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "flush", Short: "wait for a commit's downstream jobs"}
	commit := &cobra.Command{
		Use:  "commit <repo@branch>",
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			repo, branch, _, err := parseRef(args[0])
			if err != nil {
				die(err.Error(), 2)
			}
			head, err := cliClient().HeadCommit(repo, branch)
			if err != nil {
				die("flush: "+err.Error(), 1)
			}
			js, err := cliClient().Flush(head.ID, flushTimeout)
			if err != nil {
				die("flush: "+err.Error(), 1)
			}
			rows := make([][]string, 0, len(js))
			for _, j := range js {
				rows = append(rows, []string{j.Pipeline, j.State, j.ID})
			}
			table([]string{"PIPELINE", "STATE", "ID"}, rows)
			if len(rows) == 0 {
				fmt.Println("no downstream jobs")
			}
		},
	}
	commit.Flags().DurationVar(&flushTimeout, "timeout", 10*time.Minute, "maximum wait")
	cmd.AddCommand(commit)
	return cmd
}

var flushTimeout time.Duration

func newSecretCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "secret", Short: "manage secrets"}
	create := &cobra.Command{
		Use:  "create <name> [<json|->]",
		Args: cobra.RangeArgs(1, 2),
		Run: func(_ *cobra.Command, args []string) {
			var r io.Reader = os.Stdin
			if len(args) == 2 && args[1] != "-" {
				f, err := os.Open(args[1])
				if err != nil {
					die("secret create: "+err.Error(), 1)
				}
				defer f.Close()
				r = f
			}
			var data map[string]string
			if err := json.NewDecoder(r).Decode(&data); err != nil {
				die("secret create: data: "+err.Error(), 1)
			}
			if err := cliClient().CreateSecret(args[0], data); err != nil {
				die("secret create: "+err.Error(), 1)
			}
			fmt.Printf("created secret %s\n", args[0])
		},
	}
	cmd.AddCommand(create)
	cmd.AddCommand(
		&cobra.Command{
			Use:  "inspect <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				s, err := cliClient().InspectSecret(args[0])
				if err != nil {
					die("secret inspect: "+err.Error(), 1)
				}
				fmt.Printf("%-10s : %s\n", "name", s.Name)
				fmt.Printf("%-10s : %s\n", "type", s.Type)
				fmt.Printf("%-10s : %s\n", "created", s.Created)
			},
		},
		&cobra.Command{
			Use: "list",
			Run: func(_ *cobra.Command, _ []string) {
				ss, err := cliClient().ListSecrets()
				if err != nil {
					die("secret list: "+err.Error(), 1)
				}
				rows := make([][]string, 0, len(ss))
				for _, s := range ss {
					rows = append(rows, []string{s.Name, s.Type})
				}
				table([]string{"NAME", "TYPE"}, rows)
				if len(rows) == 0 {
					fmt.Println("no secrets")
				}
			},
		},
		&cobra.Command{
			Use:  "delete <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().DeleteSecret(args[0]); err != nil {
					die("secret delete: "+err.Error(), 1)
				}
				fmt.Printf("deleted secret %s\n", args[0])
			},
		},
	)
	return cmd
}

func newTagCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "tag", Short: "manage tags"}
	cmd.AddCommand(
		&cobra.Command{
			Use:  "put <name> <src|->",
			Args: cobra.ExactArgs(2),
			Run: func(_ *cobra.Command, args []string) {
				var data []byte
				var err error
				if args[1] == "-" {
					data, err = io.ReadAll(os.Stdin)
				} else {
					data, err = os.ReadFile(args[1])
				}
				if err != nil {
					die("tag put: "+err.Error(), 1)
				}
				if err := cliClient().PutTag(args[0], data); err != nil {
					die("tag put: "+err.Error(), 1)
				}
				fmt.Printf("put tag %s (%d bytes)\n", args[0], len(data))
			},
		},
		&cobra.Command{
			Use:  "get <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				data, err := cliClient().GetTag(args[0])
				if err != nil {
					die("tag get: "+err.Error(), 1)
				}
				_, _ = os.Stdout.Write(data)
			},
		},
		&cobra.Command{
			Use: "list",
			Run: func(_ *cobra.Command, _ []string) {
				ts, err := cliClient().ListTags()
				if err != nil {
					die("tag list: "+err.Error(), 1)
				}
				rows := make([][]string, 0, len(ts))
				for _, t := range ts {
					rows = append(rows, []string{t.Name, t.Ref})
				}
				table([]string{"NAME", "REF"}, rows)
				if len(rows) == 0 {
					fmt.Println("no tags")
				}
			},
		},
		&cobra.Command{
			Use:  "delete <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().DeleteTag(args[0]); err != nil {
					die("tag delete: "+err.Error(), 1)
				}
				fmt.Printf("deleted tag %s\n", args[0])
			},
		},
	)
	return cmd
}

func newLogsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "job and pipeline logs",
		Run: func(_ *cobra.Command, _ []string) {
			params := client.LogParams{Pipeline: logPipeline, Job: logJob, Since: logSince}
			if logFollow {
				rc, err := cliClient().FollowLogs(params)
				if err != nil {
					die("logs: "+err.Error(), 1)
				}
				defer rc.Close()
				// the follow stream is NDJSON {"line": ...}; decode and
				// print plain lines. Follow never terminates on its own
				// (like tail -f); it ends when the stream does.
				dec := json.NewDecoder(rc)
				for {
					var rec struct {
						Line string `json:"line"`
					}
					if err := dec.Decode(&rec); err != nil {
						return
					}
					fmt.Println(rec.Line)
				}
			}
			lines, err := cliClient().Logs(params)
			if err != nil {
				die("logs: "+err.Error(), 1)
			}
			for _, l := range lines {
				fmt.Println(l)
			}
		},
	}
	cmd.Flags().StringVarP(&logPipeline, "pipeline", "p", "", "pipeline name")
	cmd.Flags().StringVarP(&logJob, "job", "j", "", "job id")
	cmd.Flags().DurationVar(&logSince, "since", 0, "only lines logged after this duration ago")
	cmd.Flags().BoolVar(&logFollow, "follow", false, "stream live log lines (never terminates on its own)")
	return cmd
}

var (
	logPipeline string
	logJob      string
	logSince    time.Duration
	logFollow   bool
)

func newTransactionCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "transaction", Short: "manage transactions"}
	cmd.AddCommand(
		&cobra.Command{
			Use: "start",
			Run: func(_ *cobra.Command, _ []string) {
				id, err := cliClient().StartTransaction()
				if err != nil {
					die("transaction start: "+err.Error(), 1)
				}
				fmt.Println(id)
			},
		},
		&cobra.Command{
			Use:  "finish <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().FinishTransaction(args[0]); err != nil {
					die("transaction finish: "+err.Error(), 1)
				}
				fmt.Printf("finished transaction %s\n", args[0])
			},
		},
		&cobra.Command{
			Use:  "delete <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().DeleteTransaction(args[0]); err != nil {
					die("transaction delete: "+err.Error(), 1)
				}
				fmt.Printf("deleted transaction %s\n", args[0])
			},
		},
		&cobra.Command{
			Use: "list",
			Run: func(_ *cobra.Command, _ []string) {
				ts, err := cliClient().ListTransactions()
				if err != nil {
					die("transaction list: "+err.Error(), 1)
				}
				rows := make([][]string, 0, len(ts))
				for _, tx := range ts {
					rows = append(rows, []string{tx.ID, fmt.Sprintf("%d ops", len(tx.Ops))})
				}
				table([]string{"ID", "OPS"}, rows)
				if len(rows) == 0 {
					fmt.Println("no transactions")
				}
			},
		},
		&cobra.Command{
			Use:  "inspect <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				tx, err := cliClient().InspectTransaction(args[0])
				if err != nil {
					die("transaction inspect: "+err.Error(), 1)
				}
				fmt.Printf("transaction: %s\n", tx.ID)
				if len(tx.Ops) == 0 {
					fmt.Println("  (no staged operations)")
				}
				for _, op := range tx.Ops {
					fmt.Printf("  %s %s\n", op.Kind, op.Pipeline)
				}
			},
		},
		&cobra.Command{
			Use:  "resume <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := setActiveTx(args[0]); err != nil {
					die("transaction resume: "+err.Error(), 1)
				}
				fmt.Printf("resumed transaction %s (pipeline create/update will stage into it)\n", args[0])
			},
		},
		&cobra.Command{
			Use: "stop",
			Run: func(_ *cobra.Command, _ []string) {
				if err := setActiveTx(""); err != nil {
					die("transaction stop: "+err.Error(), 1)
				}
				fmt.Println("no active transaction")
			},
		},
	)
	return cmd
}

// activeTxFile is the CLI's per-user active-transaction marker (the
// reference's "resume" persists the session's transaction in local state).
func activeTxFile() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "sandman", "active-tx")
}

// setActiveTx persists (or clears, with "") the CLI's active transaction.
func setActiveTx(id string) error {
	p := activeTxFile()
	if p == "" {
		return fmt.Errorf("no user config directory")
	}
	if id == "" {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, []byte(id+"\n"), 0o600)
}

// activeTx returns the resumed transaction id, or "" when none is set.
func activeTx() string {
	p := activeTxFile()
	if p == "" {
		return ""
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
