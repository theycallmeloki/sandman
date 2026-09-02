package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mattn/go-runewidth"
	"github.com/spf13/cobra"
	"golang.org/x/term"
	"sandman/client"
	"sandman/internal/store"
)

// The data-plane CLI on spf13/cobra (the CLI is a second consumer
// of the same client package the conformance suite drives — semantic and
// command-level compatibility with the reference, no wire compatibility).
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

// exitFunc is swappable for tests: the real CLI exits; tests capture the
// code instead of killing the test process.
var exitFunc = os.Exit

// die is the CLI's fatal-error exit: print the message to stderr and
// exit with the given code.
func die(msg string, code int) {
	fmt.Fprintln(os.Stderr, "sandman:", msg)
	exitFunc(code)
}

// isConnErr reports whether an error string smells like a control-plane
// reachability problem — the case where the fix is "start the daemon",
// not "change the arguments".
func isConnErr(msg string) bool {
	l := strings.ToLower(msg)
	return strings.Contains(l, "connection refused") ||
		strings.Contains(l, "no such host") ||
		strings.Contains(l, "connection reset") ||
		strings.Contains(l, "dial tcp") ||
		strings.Contains(l, "i/o timeout") ||
		strings.Contains(l, "eof")
}

// dieErr is die for verb failures: it appends a daemon-reachability hint
// for connection-shaped errors, and a caller-supplied hint for the cases
// where the fix is a specific next command.
func dieErr(verb string, err error, hint string) {
	msg := err.Error()
	switch {
	case isConnErr(msg):
		die(fmt.Sprintf("%s: %v (daemon not reachable at %s — is it running? start it with `sandman daemon`)", verb, err, addr), 1)
	case hint != "":
		die(fmt.Sprintf("%s: %v — %s", verb, err, hint), 1)
	default:
		die(fmt.Sprintf("%s: %v", verb, err), 1)
	}
}

// jsonOut is the shared --json flag of the list verbs: a command binds it
// to its own flag, and emitJSON prints the raw objects instead of a
// table. One command executes per run, so the shared package variable is
// safe (the same pattern as the shared specFile/forceDelete flags).
var jsonOut bool

// emitJSON prints v as indented JSON — the --json mode of the list verbs.
func emitJSON(v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		die("json: "+err.Error(), 1)
	}
	fmt.Println(string(b))
}

// listingGlob turns a file-list [:path] argument into the server's
// prefix glob: the listing API accepts only prefix patterns ("prefix*"),
// so a path without its own wildcard is made a prefix
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
	cmds = append(cmds, getCmd())    // get [file] <ref> [-o dest]
	cmds = append(cmds, putCmd())    // cp-like upload
	cmds = append(cmds, patchCmd())  // deliver a checkout's edits as a delta
	cmds = append(cmds, lsCmd())     // repos, or files in a repo
	cmds = append(cmds, catCmd())    // files to stdout
	cmds = append(cmds, psCmd())     // jobs, alias for `job list`
	cmds = append(cmds, statusCmd()) // one-glance fleet/pipeline/job view
	cmds = append(cmds, &cobra.Command{
		Use: "version",
		Run: func(*cobra.Command, []string) { PrintVersion() },
	})
	return cmds
}

// table prints an aligned table with an uppercase header row (empty row sets
// print nothing — the caller prints the empty-state message instead).
func table(header []string, rows [][]string) {
	RenderTable(header, rows)
}

// RenderTable prints list output with terminal-aware column sizing. Redirected
// output stays complete for scripts; interactive terminals get columns fitted
// to the available width.
func RenderTable(header []string, rows [][]string) {
	if len(rows) == 0 {
		return
	}
	widths := tableWidths(header, rows)
	tty := term.IsTerminal(int(os.Stdout.Fd()))
	if tty {
		if width, _, err := term.GetSize(int(os.Stdout.Fd())); err == nil && width > 0 {
			fitWidths(widths, width)
		}
	}
	printTableRow(header, widths, tty)
	for _, r := range rows {
		printTableRow(r, widths, tty)
	}
}

func tableWidths(header []string, rows [][]string) []int {
	widths := make([]int, len(header))
	for i, h := range header {
		widths[i] = runewidth.StringWidth(h)
	}
	for _, row := range rows {
		for i := range widths {
			if i < len(row) {
				widths[i] = max(widths[i], runewidth.StringWidth(row[i]))
			}
		}
	}
	return widths
}

func fitWidths(widths []int, termWidth int) {
	if len(widths) == 0 {
		return
	}
	const gap = 2
	minWidth := 4
	if termWidth < 64 {
		minWidth = 3
	}
	for tableWidth(widths, gap) > termWidth {
		widest := -1
		for i, w := range widths {
			if w > minWidth && (widest < 0 || w > widths[widest]) {
				widest = i
			}
		}
		if widest < 0 {
			return
		}
		widths[widest]--
	}
}

func tableWidth(widths []int, gap int) int {
	total := 0
	for i, w := range widths {
		if i > 0 {
			total += gap
		}
		total += w
	}
	return total
}

func printTableRow(row []string, widths []int, truncate bool) {
	for i, width := range widths {
		if i > 0 {
			fmt.Print("  ")
		}
		value := ""
		if i < len(row) {
			value = row[i]
		}
		if truncate {
			value = fitString(value, width)
		}
		fmt.Print(value)
		if i < len(widths)-1 {
			fmt.Print(strings.Repeat(" ", max(0, width-runewidth.StringWidth(value))))
		}
	}
	fmt.Println()
}

func fitString(s string, width int) string {
	if runewidth.StringWidth(s) <= width {
		return s
	}
	if width <= 3 {
		return strings.Repeat(".", max(0, width))
	}
	return runewidth.Truncate(s, width, "...")
}

func detail(rows ...[2]string) {
	labelWidth := 0
	for _, row := range rows {
		labelWidth = max(labelWidth, runewidth.StringWidth(row[0]))
	}
	for _, row := range rows {
		fmt.Printf("%s%s : %s\n", row[0], strings.Repeat(" ", max(0, labelWidth-runewidth.StringWidth(row[0]))), row[1])
	}
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
	// store paths are repo-relative; accept a leading-slash /path and
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
				dieErr("backup", err, "")
			}
			if err := cliClient().Backup(f); err != nil {
				f.Close()
				// die() is os.Exit — no deferred cleanup runs; a
				// truncated archive must not stay on disk looking valid
				os.Remove(dest)
				dieErr("backup", err, "")
			}
			if err := f.Close(); err != nil {
				os.Remove(dest)
				dieErr("backup", err, "")
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
				dieErr("reset", err, "")
			}
			fmt.Println("reset complete — repos, pipelines, jobs, secrets, tags cleared (spec repo recreated)")
		},
	}
	cmd.Flags().Bool("yes", false, "confirm the destructive reset")
	return cmd
}

func newRepoCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "repo", Short: "manage repositories"}
	list := &cobra.Command{
		Use:  "list",
		Args: cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			repoList()
		},
	}
	list.Flags().BoolVar(&jsonOut, "json", false, "emit JSON instead of a table")
	cmd.AddCommand(
		&cobra.Command{
			Use:  "create <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().CreateRepo(args[0]); err != nil {
					dieErr("repo create", err, "")
				}
				fmt.Printf("created repo %s\n", args[0])
			},
		},
		list,
		&cobra.Command{
			Use:  "inspect <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				r, err := cliClient().InspectRepo(args[0])
				if err != nil {
					dieErr("repo inspect", err, "")
				}
				detail(
					[2]string{"name", r.Name},
					[2]string{"size", HumanSize(r.SizeBytes)},
					[2]string{"branches", strings.Join(r.Branches, ", ")},
				)
			},
		},
	)
	del := &cobra.Command{
		Use:  "delete <name>",
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			if err := cliClient().DeleteRepo(args[0], forceDelete); err != nil {
				dieErr("repo delete", err, "")
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
	list := &cobra.Command{
		Use:  "list <repo>[@branch]",
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			repo, branch, _, err := parseRef(args[0])
			if err != nil {
				die(err.Error(), 2)
			}
			hist, err := cliClient().CommitHistory(repo, branch)
			if err != nil {
				dieErr("commit list", err, "")
			}
			if jsonOut {
				emitJSON(hist)
				return
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
	}
	list.Flags().BoolVar(&jsonOut, "json", false, "emit JSON instead of a table")
	cmd.AddCommand(
		list,
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
					dieErr("commit start", err, "")
				}
				fmt.Println(cm.ID)
			},
		},
		&cobra.Command{
			Use:  "finish <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if _, err := cliClient().FinishCommit(args[0], "", false); err != nil {
					dieErr("commit finish", err, "")
				}
				fmt.Printf("finished commit %s\n", args[0])
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
						dieErr("commit inspect", err, "")
					}
					id = resolved
				}
				cm, err := cliClient().InspectCommit(id)
				if err != nil {
					dieErr("commit inspect", err, "")
				}
				detail(
					[2]string{"id", cm.ID},
					[2]string{"repo", cm.Repo},
					[2]string{"branch", cm.Branch},
					[2]string{"started", fmt.Sprintf("%t", cm.Started)},
					[2]string{"finished", fmt.Sprintf("%t", cm.Finished)},
				)
			},
		},
		&cobra.Command{
			Use:  "delete <id|repo@branch>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().DeleteCommit(args[0]); err != nil {
					dieErr("commit delete", err, "")
				}
				fmt.Printf("deleted commit %s\n", args[0])
			},
		},
	)
	return cmd
}

func newBranchCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "branch", Short: "manage branches"}
	list := &cobra.Command{
		Use:  "list [repo]",
		Args: cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			if len(args) == 1 {
				bs, err := cliClient().ListBranches(args[0])
				if err != nil {
					dieErr("branch list", err, "")
				}
				if jsonOut {
					emitJSON(bs)
					return
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
				dieErr("branch list", err, "")
			}
			if jsonOut {
				var bs []client.Branch
				for _, r := range repos {
					for _, b := range r.Branches {
						bs = append(bs, client.Branch{Repo: r.Name, Branch: b})
					}
				}
				emitJSON(bs)
				return
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
	}
	list.Flags().BoolVar(&jsonOut, "json", false, "emit JSON instead of a table")
	cmd.AddCommand(
		list,
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
						dieErr("branch create", err, "")
					}
					head = h.ID
				}
				if err := cliClient().CreateBranch(args[0], args[1], head); err != nil {
					dieErr("branch create", err, "")
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
					dieErr("branch inspect", err, "")
				}
				detail(
					[2]string{"repo", b.Repo},
					[2]string{"branch", b.Branch},
					[2]string{"head", b.Head},
				)
			},
		},
		&cobra.Command{
			Use:  "delete <repo> <branch>",
			Args: cobra.ExactArgs(2),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().DeleteBranch(args[0], args[1]); err != nil {
					dieErr("branch delete", err, "")
				}
				fmt.Printf("deleted branch %s@%s\n", args[0], args[1])
			},
		},
	)
	return cmd
}

// getCmd is defined in transfer.go: the reference's `get file <ref>`
// survives as a subcommand; `get <ref> [-o dest]` is the ergonomic form.

func newFileCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "file", Short: "manage files"}
	put := &cobra.Command{
		Use:  "put <repo@branch:path> <src|->",
		Args: cobra.ExactArgs(2),
		Run: func(_ *cobra.Command, args []string) {
			_, _, path, err := parseRef(args[0])
			if err != nil {
				die(err.Error(), 2)
			}
			if path == "" {
				die("file put: path required (repo@branch:path)", 2)
			}
			// shared upload: `put` uses the cp-like order, `file put`
			// keeps the reference's <ref> <src> order — same machinery
			putRun([]string{args[1]}, args[0], fileOverwrite, true, "file put")
		},
	}
	put.Flags().BoolVarP(&fileOverwrite, "overwrite", "o", false, "overwrite accumulated content at the path")
	cmd.AddCommand(put)

	get := &cobra.Command{
		Use:  "get <repo@branch:path>",
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			getRun(cliClient(), args[0], fileGetOut, false, "file get")
		},
	}
	get.Flags().StringVarP(&fileGetOut, "output", "o", "", "write to this file or directory (default: stdout)")
	cmd.AddCommand(get)

	list := &cobra.Command{
		Use:  "list <repo@branch>[:path]",
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			listFilesRef(args[0])
		},
	}
	list.Flags().BoolVar(&jsonOut, "json", false, "emit JSON instead of a table")
	cmd.AddCommand(list)

	cmd.AddCommand(
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
					dieErr("file inspect", err, "")
				}
				data, err := cliClient().GetFile(head, path)
				if err != nil {
					dieErr("file inspect", err, "")
				}
				detail(
					[2]string{"path", path},
					[2]string{"size", HumanSize(uint64(len(data)))},
				)
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
				dieErr("file copy", err, "")
			}
			dst, err := cliClient().StartCommit(dstRepo, dstBranch, "")
			if err != nil {
				dieErr("file copy", err, "")
			}
			if err := cliClient().CopyFile(dst.ID, dstPath, srcHead.ID, srcPath, fileOverwrite); err != nil {
				dieErr("file copy", err, "")
			}
			if _, err := cliClient().FinishCommit(dst.ID, "", false); err != nil {
				dieErr("file copy", err, "")
			}
			fmt.Printf("copied %s to %s@%s:%s (commit %s)\n", srcPath, dstRepo, dstBranch, dstPath, dst.ID)
		},
	}
	copyCmd.Flags().BoolVarP(&fileOverwrite, "overwrite", "o", false, "overwrite accumulated content at the destination path")
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
					dieErr("file delete", err, "")
				}
				if err := cliClient().DeleteFile(cm.ID, path); err != nil {
					dieErr("file delete", err, "")
				}
				if _, err := cliClient().FinishCommit(cm.ID, "", false); err != nil {
					dieErr("file delete", err, "")
				}
				fmt.Printf("deleted %s@%s:%s (commit %s)\n", repo, branch, path, cm.ID)
			},
		},
	)
	return cmd
}

var (
	fileOverwrite bool
	fileGetOut    string
)

func newCheckCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "check",
		Short: "consistency check (fsck analog)",
		Run: func(_ *cobra.Command, _ []string) {
			if err := cliClient().Check(); err != nil {
				dieErr("check", err, "")
			}
			fmt.Println("ok")
		},
	}
}

func newJobCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "job", Short: "manage jobs"}
	list := &cobra.Command{
		Use:  "list [pipeline]",
		Args: cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			runJobList(args, jobStates)
		},
	}
	list.Flags().StringSliceVarP(&jobStates, "state", "s", nil, "only jobs in these states (repeatable)")
	list.Flags().BoolVar(&jsonOut, "json", false, "emit JSON instead of a table")
	cmd.AddCommand(list)
	cmd.AddCommand(
		&cobra.Command{
			Use:  "inspect <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				j, err := cliClient().InspectJob(args[0])
				if err != nil {
					dieErr("job inspect", err, "")
				}
				rows := [][2]string{
					{"id", j.ID},
					{"pipeline", j.Pipeline},
					{"state", j.State},
					{"reason", j.Reason},
					{"outputCommit", j.OutputCommit},
					{"processed", fmt.Sprintf("%d", j.Processed)},
					{"recovered", fmt.Sprintf("%d", j.Recovered)},
					{"failed", fmt.Sprintf("%d", j.Failed)},
					{"skipped", fmt.Sprintf("%d", j.Skipped)},
				}
				if j.StatsCommit != "" {
					rows = append(rows, [2]string{"statsCommit", j.StatsCommit})
				}
				detail(rows...)
			},
		},
		&cobra.Command{
			Use:  "delete <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().DeleteJob(args[0]); err != nil {
					dieErr("job delete", err, "")
				}
				fmt.Printf("deleted job %s\n", args[0])
			},
		},
		&cobra.Command{
			Use:  "stop <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().StopJob(args[0]); err != nil {
					dieErr("job stop", err, "")
				}
				fmt.Printf("stopped job %s\n", args[0])
			},
		},
		&cobra.Command{
			Use:  "restart-datum <job> <datum>",
			Args: cobra.ExactArgs(2),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().RestartDatum(args[0], args[1]); err != nil {
					dieErr("job restart-datum", err, "")
				}
				fmt.Printf("restarted datum %s\n", args[1])
			},
		},
	)
	return cmd
}

// jobStates is the shared --state filter of `job list` and `ps`.
var jobStates []string

// runJobList is the shared listing of `job list` and `ps`: pipeline
// filter, repeatable state filter, table or JSON.
func runJobList(args []string, states []string) {
	filter := client.JobFilter{}
	if len(args) == 1 {
		filter.Pipeline = args[0]
	}
	filter.States = states
	js, err := cliClient().ListJobsFiltered(filter)
	if err != nil {
		dieErr("job list", err, "")
	}
	if jsonOut {
		emitJSON(js)
		return
	}
	rows := make([][]string, 0, len(js))
	for _, j := range js {
		rows = append(rows, []string{j.ID, j.Pipeline, j.State})
	}
	table([]string{"ID", "PIPELINE", "STATE"}, rows)
	if len(rows) == 0 {
		fmt.Println("no jobs")
	}
}

// psCmd is the ps-style face of `job list`: `sandman ps [pipeline]`.
func psCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ps [pipeline]",
		Short: "list jobs (alias for `job list`)",
		Args:  cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			runJobList(args, jobStates)
		},
	}
	cmd.Flags().StringSliceVarP(&jobStates, "state", "s", nil, "only jobs in these states (repeatable)")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "emit JSON instead of a table")
	return cmd
}

// statusCmd is the one-glance view: daemon version, registered hosts,
// pipelines by state, jobs by state — the first thing to type when
// something feels off.
func statusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "one-glance view: daemon, hosts, pipelines, jobs",
		Args:  cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			c := cliClient()
			ver, err := c.Version()
			if err != nil {
				dieErr("status", err, "")
			}
			fmt.Printf("daemon    %s @ %s\n", ver, addr)
			hosts, err := c.Hosts()
			if err != nil {
				dieErr("status", err, "")
			}
			gpuHosts := 0
			for _, h := range hosts {
				if len(h.Gpus) > 0 {
					gpuHosts++
				}
			}
			fmt.Printf("hosts     %d registered", len(hosts))
			if gpuHosts > 0 {
				fmt.Printf(" (%d with GPUs)", gpuHosts)
			}
			fmt.Println()
			ps, err := c.ListPipelines()
			if err != nil {
				dieErr("status", err, "")
			}
			fmt.Printf("pipelines %d (%s)\n", len(ps), countStates(ps, func(p client.PipelineInfo) string { return p.State }))
			js, err := c.ListJobsFiltered(client.JobFilter{})
			if err != nil {
				dieErr("status", err, "")
			}
			fmt.Printf("jobs      %d (%s)\n", len(js), countStates(js, func(j client.Job) string { return j.State }))
		},
	}
	return cmd
}

// countStates renders a state histogram of State-carrying records:
// "2 running, 1 queued, 9 success"; an empty list prints as "none".
func countStates[T any](items []T, state func(T) string) string {
	if len(items) == 0 {
		return "none"
	}
	counts := map[string]int{}
	var order []string
	for _, it := range items {
		s := state(it)
		if _, seen := counts[s]; !seen {
			order = append(order, s)
		}
		counts[s]++
	}
	sort.Strings(order)
	parts := make([]string, 0, len(order))
	for _, s := range order {
		parts = append(parts, fmt.Sprintf("%d %s", counts[s], s))
	}
	return strings.Join(parts, ", ")
}

func newDatumCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "datum", Short: "manage datums"}
	list := &cobra.Command{
		Use:  "list <job>",
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			page, err := cliClient().ListDatums(args[0], 0, 0)
			if err != nil {
				dieErr("datum list", err, "")
			}
			if jsonOut {
				emitJSON(page.Datums)
				return
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
	}
	list.Flags().BoolVar(&jsonOut, "json", false, "emit JSON instead of a table")
	cmd.AddCommand(list)
	cmd.AddCommand(
		&cobra.Command{
			Use:  "inspect <job> <datum>",
			Args: cobra.ExactArgs(2),
			Run: func(_ *cobra.Command, args []string) {
				d, err := cliClient().InspectDatum(args[0], args[1])
				if err != nil {
					dieErr("datum inspect", err, "")
				}
				detail(
					[2]string{"id", d.ID},
					[2]string{"state", d.State},
					[2]string{"reason", d.Reason},
					[2]string{"started", d.Started},
					[2]string{"finished", d.Finished},
				)
			},
		},
		&cobra.Command{
			Use:  "restart <job> <datum>",
			Args: cobra.ExactArgs(2),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().RestartDatum(args[0], args[1]); err != nil {
					dieErr("datum restart", err, "")
				}
				fmt.Printf("restarted datum %s\n", args[1])
			},
		},
	)
	return cmd
}

func newPipelineCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "pipeline", Short: "manage pipelines"}
	list := &cobra.Command{
		Use: "list",
		Run: func(_ *cobra.Command, _ []string) {
			ps, err := cliClient().ListPipelines()
			if err != nil {
				dieErr("pipeline list", err, "")
			}
			if jsonOut {
				emitJSON(ps)
				return
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
	}
	list.Flags().BoolVar(&jsonOut, "json", false, "emit JSON instead of a table")
	run := &cobra.Command{
		Use:  "run <name>",
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			j, err := cliClient().RunPipeline(args[0], nil, "")
			if err != nil {
				dieErr("pipeline run", err, "")
			}
			if !pipelineWait {
				fmt.Println(j.ID)
				return
			}
			c := cliClient()
			fmt.Printf("job %s started, waiting for it to settle...\n", j.ID)
			state := j.State
			for !isTerminalJob(state) {
				time.Sleep(time.Second)
				cur, err := c.InspectJob(j.ID)
				if err != nil {
					dieErr("pipeline run", err, "")
				}
				state = cur.State
			}
			fmt.Printf("job %s: %s\n", j.ID, state)
			if state != "success" {
				os.Exit(1)
			}
		},
	}
	run.Flags().BoolVarP(&pipelineWait, "wait", "w", false, "wait for the job to settle (exit 1 unless it succeeds)")
	create := &cobra.Command{
		Use:   "create [name]",
		Short: "create a pipeline from a spec file (-f) or builder flags",
		Args:  cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			p, fromFlags, err := pipelineFromArgs(args)
			if err != nil {
				dieErr("pipeline create", err, "")
			}
			if !fromFlags {
				p, err = readPipelineSpec(specFile)
				if err != nil {
					dieErr("pipeline create", err, "")
				}
			}
			tx := txID
			if tx == "" {
				tx = activeTx() // transaction resume sets this
			}
			if tx != "" {
				if err := cliClient().CreatePipelineTx(p, tx); err != nil {
					dieErr("pipeline create", err, "")
				}
			} else if err := cliClient().CreatePipeline(p); err != nil {
				dieErr("pipeline create", err, "")
			}
			fmt.Printf("created pipeline %s\n", p.Name)
		},
	}
	create.Flags().StringVarP(&specFile, "spec", "f", "-", "pipeline spec JSON file ('-' = stdin)")
	create.Flags().StringVar(&txID, "tx", "", "stage the create in this transaction")
	addPipelineBuilderFlags(create)
	cmd.AddCommand(create)

	update := &cobra.Command{
		Use:   "update [name]",
		Short: "update a pipeline from a spec file (-f) or builder flags",
		Args:  cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			p, fromFlags, err := pipelineFromArgs(args)
			if err != nil {
				dieErr("pipeline update", err, "")
			}
			if !fromFlags {
				p, err = readPipelineSpec(specFile)
				if err != nil {
					dieErr("pipeline update", err, "")
				}
			}
			p.Update = true
			tx := txID
			if tx == "" {
				tx = activeTx() // transaction resume sets this
			}
			if tx != "" {
				if err := cliClient().CreatePipelineTx(p, tx); err != nil {
					dieErr("pipeline update", err, "")
				}
			} else if err := cliClient().CreatePipeline(p); err != nil {
				dieErr("pipeline update", err, "")
			}
			fmt.Printf("updated pipeline %s\n", p.Name)
		},
	}
	update.Flags().StringVarP(&specFile, "spec", "f", "-", "pipeline spec JSON file ('-' = stdin)")
	update.Flags().StringVar(&txID, "tx", "", "stage the update in this transaction")
	addPipelineBuilderFlags(update)
	cmd.AddCommand(update)

	pdel := &cobra.Command{
		Use:  "delete <name>",
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			if err := cliClient().DeletePipeline(args[0], forcePipelineDelete, false); err != nil {
				dieErr("pipeline delete", err, "")
			}
			fmt.Printf("deleted pipeline %s\n", args[0])
		},
	}
	pdel.Flags().BoolVar(&forcePipelineDelete, "force", false, "delete a pipeline even if it has downstream consumers")
	cmd.AddCommand(
		list,
		&cobra.Command{
			Use:  "inspect <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				p, err := cliClient().InspectPipeline(args[0])
				if err != nil {
					dieErr("pipeline inspect", err, "")
				}
				detail(
					[2]string{"name", p.Name},
					[2]string{"state", p.State},
					[2]string{"version", fmt.Sprintf("%d", p.Version)},
					[2]string{"reason", p.Reason},
				)
			},
		},
		pdel,
		&cobra.Command{
			Use:  "start <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().StartPipeline(args[0]); err != nil {
					dieErr("pipeline start", err, "")
				}
				fmt.Printf("started pipeline %s\n", args[0])
			},
		},
		&cobra.Command{
			Use:  "stop <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().StopPipeline(args[0]); err != nil {
					dieErr("pipeline stop", err, "")
				}
				fmt.Printf("stopped pipeline %s\n", args[0])
			},
		},
		run,
		&cobra.Command{
			Use:  "run-cron <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().TriggerCron(args[0]); err != nil {
					dieErr("pipeline run-cron", err, "")
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
					dieErr("pipeline extract", err, "")
				}
				b, err := json.Marshal(p)
				if err != nil {
					dieErr("pipeline extract", err, "")
				}
				out, err := normalizeSpec(b)
				if err != nil {
					dieErr("pipeline extract", err, "")
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
					dieErr("pipeline edit", err, "")
				}
				raw, err := json.Marshal(p)
				if err != nil {
					dieErr("pipeline edit", err, "")
				}
				// the editor file is the normalized spec, not the raw
				// inspection: the strict spec decoder rejects the
				// inspection's state/version/jobCounts fields
				b, err := normalizeSpec(raw)
				if err != nil {
					dieErr("pipeline edit", err, "")
				}
				f, err := os.CreateTemp("", "sandman-pipeline-*.json")
				if err != nil {
					dieErr("pipeline edit", err, "")
				}
				name := f.Name()
				defer os.Remove(name)
				if _, err := f.Write(b); err != nil {
					dieErr("pipeline edit", err, "")
				}
				f.Close()
				editor := os.Getenv("EDITOR")
				if editor == "" {
					editor = "vi"
				}
				cmd := exec.Command("sh", "-c", editor+" "+strconv.Quote(name))
				cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
				if err := cmd.Run(); err != nil {
					dieErr("pipeline edit", err, "")
				}
				spec, err := readPipelineSpec(name)
				if err != nil {
					dieErr("pipeline edit", err, "")
				}
				spec.Update = true
				if err := cliClient().CreatePipeline(spec); err != nil {
					dieErr("pipeline edit", err, "")
				}
				fmt.Printf("updated pipeline %s\n", spec.Name)
			},
		},
	)
	return cmd
}

var (
	specFile     string
	txID         string
	pipelineWait bool
)

// isTerminalJob reports whether a job state is settled: the --wait
// polling loop stops there.
func isTerminalJob(state string) bool {
	switch state {
	case "success", "failure", "killed", "skipped":
		return true
	}
	return false
}

// ---- pipeline builder flags ----
//
// `pipeline create mypipe --image x --cmd 'sh -c hi' --input in@master
// --gpu 1` — the ad-hoc path; the full spec file (-f) remains the
// complete-control path. The two are mutually exclusive: a name means
// "build from flags", -f (or stdin) means "use this spec".

var (
	pipelineImage       string
	pipelineCmd         string
	pipelineSh          string
	pipelineInput       string
	pipelineGlob        string
	pipelineCron        string
	pipelinePlacement   string
	pipelineDescription string
	pipelineParallelism int
	pipelineGPU         int
	pipelineCPU         float64
	pipelineMemory      string
	pipelineStandby     bool
	pipelineAutoscaling bool
	pipelineReprocess   bool
	pipelineEnableStats bool
	pipelineEnv         []string
	pipelineSecrets     []string
)

func addPipelineBuilderFlags(cmd *cobra.Command) {
	f := cmd.Flags()
	f.StringVar(&pipelineImage, "image", "", "transform image (e.g. nvidia/cuda:12.4.1-base-ubuntu22.04)")
	f.StringVar(&pipelineCmd, "cmd", "", "transform command, whitespace-split into argv (exec form)")
	f.StringVar(&pipelineSh, "sh", "", "transform shell script — runs as sh -c '<script>' (use for $in/$OUT, redirects)")
	f.StringVar(&pipelineInput, "input", "", "input repo[@branch] (e.g. in@master)")
	f.StringVar(&pipelineGlob, "glob", "", "input file glob (default: all files)")
	f.StringVar(&pipelineCron, "cron", "", "cron input schedule (e.g. '@every 5m') — replaces --input")
	f.StringVar(&pipelinePlacement, "placement", "", "placement label a host must bear")
	f.StringVar(&pipelineDescription, "description", "", "human description")
	f.IntVar(&pipelineParallelism, "parallelism", 0, "constant parallelism (default: 1)")
	f.IntVar(&pipelineGPU, "gpu", 0, "GPUs per datum worker (0 = none)")
	f.Float64Var(&pipelineCPU, "cpu", 0, "CPU cores per datum worker (fractional allowed)")
	f.StringVar(&pipelineMemory, "memory", "", "memory request per datum worker (e.g. 100M, 2G)")
	f.BoolVar(&pipelineStandby, "standby", false, "idle until work arrives")
	f.BoolVar(&pipelineAutoscaling, "autoscaling", false, "scale workers to the datum count")
	f.BoolVar(&pipelineReprocess, "reprocess", false, "re-execute all datums on the next job")
	f.BoolVar(&pipelineEnableStats, "enable-stats", false, "persist per-datum statistics")
	f.StringSliceVar(&pipelineEnv, "env", nil, "environment variable K=V (repeatable)")
	f.StringSliceVar(&pipelineSecrets, "secret", nil, "bind a secret's keys into the environment (repeatable)")
}

// pipelineFromArgs resolves the create/update input: a positional name
// builds a spec from the builder flags; no positional uses -f. Returns
// the pipeline, whether flags built it, and a validation error.
func pipelineFromArgs(args []string) (client.Pipeline, bool, error) {
	if len(args) == 0 {
		return client.Pipeline{}, false, nil
	}
	if specFile != "-" {
		return client.Pipeline{}, false, fmt.Errorf("give either a pipeline name (with flags) or -f, not both")
	}
	p, err := buildPipelineFromFlags(args[0])
	return p, true, err
}

func buildPipelineFromFlags(name string) (client.Pipeline, error) {
	var p client.Pipeline
	p.Name = name
	p.Description = pipelineDescription
	if pipelineCron != "" && pipelineInput != "" {
		return p, fmt.Errorf("--cron and --input are mutually exclusive")
	}
	if pipelineCmd != "" && pipelineSh != "" {
		return p, fmt.Errorf("--cmd and --sh are mutually exclusive")
	}
	if pipelineImage == "" && pipelineCmd == "" && pipelineSh == "" {
		return p, fmt.Errorf("a transform needs --image (or use -f spec.json)")
	}
	p.Transform = &client.Transform{Image: pipelineImage}
	switch {
	case pipelineSh != "":
		// the whole script is one argv element: sh -c '<script>'
		p.Transform.Cmd = []string{"sh", "-c", pipelineSh}
	case pipelineCmd != "":
		p.Transform.Cmd = strings.Fields(pipelineCmd)
	}
	if len(pipelineEnv) > 0 {
		env := map[string]string{}
		for _, kv := range pipelineEnv {
			k, v, ok := strings.Cut(kv, "=")
			if !ok || k == "" {
				return p, fmt.Errorf("env %q: want K=V", kv)
			}
			env[k] = v
		}
		p.Transform.Env = env
	}
	for _, s := range pipelineSecrets {
		p.Transform.Secrets = append(p.Transform.Secrets, client.SecretMount{Name: s, EnvVar: s})
	}
	if pipelineMemory != "" || pipelineCPU > 0 || pipelineGPU > 0 {
		p.Transform.ResourceRequests = &client.ResourceRequests{
			Memory: pipelineMemory,
			CPU:    pipelineCPU,
			GPU:    pipelineGPU,
		}
	}
	switch {
	case pipelineCron != "":
		p.Input = &client.Input{Cron: pipelineCron}
	case pipelineInput != "":
		repo, branch, _, err := parseRef(pipelineInput)
		if err != nil {
			return p, err
		}
		glob := pipelineGlob
		if glob == "" {
			glob = "/*" // the server requires a glob on repo inputs
		}
		in := &client.Input{Repo: repo, Glob: glob}
		if branch != "" && branch != "master" {
			in.Branch = branch
		}
		p.Input = in
	default:
		return p, fmt.Errorf("a pipeline needs --input repo[@branch] (or --cron, or use -f spec.json)")
	}
	if pipelineParallelism > 0 {
		p.Parallelism = &client.Parallelism{Constant: pipelineParallelism}
	}
	p.Placement = pipelinePlacement
	p.Standby = pipelineStandby
	p.Autoscaling = pipelineAutoscaling
	p.Reprocess = pipelineReprocess
	p.EnableStats = pipelineEnableStats
	return p, nil
}

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
	// a ported spec carrying top-level resource_limits — fails
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
				dieErr("flush", err, "")
			}
			js, err := cliClient().Flush(head.ID, flushTimeout)
			if err != nil {
				dieErr("flush", err, "")
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
	list := &cobra.Command{
		Use: "list",
		Run: func(_ *cobra.Command, _ []string) {
			ss, err := cliClient().ListSecrets()
			if err != nil {
				dieErr("secret list", err, "")
			}
			if jsonOut {
				emitJSON(ss)
				return
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
	}
	list.Flags().BoolVar(&jsonOut, "json", false, "emit JSON instead of a table")
	create := &cobra.Command{
		Use:  "create <name> [<json|->]",
		Args: cobra.RangeArgs(1, 2),
		Run: func(_ *cobra.Command, args []string) {
			var r io.Reader = os.Stdin
			if len(args) == 2 && args[1] != "-" {
				f, err := os.Open(args[1])
				if err != nil {
					dieErr("secret create", err, "")
				}
				defer f.Close()
				r = f
			}
			var data map[string]string
			if err := json.NewDecoder(r).Decode(&data); err != nil {
				dieErr("secret create", err, "")
			}
			if err := cliClient().CreateSecret(args[0], data); err != nil {
				dieErr("secret create", err, "")
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
					dieErr("secret inspect", err, "")
				}
				detail(
					[2]string{"name", s.Name},
					[2]string{"type", s.Type},
					[2]string{"created", s.Created},
				)
			},
		},
		list,
		&cobra.Command{
			Use:  "delete <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().DeleteSecret(args[0]); err != nil {
					dieErr("secret delete", err, "")
				}
				fmt.Printf("deleted secret %s\n", args[0])
			},
		},
	)
	return cmd
}

func newTagCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "tag", Short: "manage tags"}
	list := &cobra.Command{
		Use: "list",
		Run: func(_ *cobra.Command, _ []string) {
			ts, err := cliClient().ListTags()
			if err != nil {
				dieErr("tag list", err, "")
			}
			if jsonOut {
				emitJSON(ts)
				return
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
	}
	list.Flags().BoolVar(&jsonOut, "json", false, "emit JSON instead of a table")
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
					dieErr("tag put", err, "")
				}
				if err := cliClient().PutTag(args[0], data); err != nil {
					dieErr("tag put", err, "")
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
					dieErr("tag get", err, "")
				}
				_, _ = os.Stdout.Write(data)
			},
		},
		list,
		&cobra.Command{
			Use:  "delete <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().DeleteTag(args[0]); err != nil {
					dieErr("tag delete", err, "")
				}
				fmt.Printf("deleted tag %s\n", args[0])
			},
		},
	)
	return cmd
}

func newLogsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs [pipeline-or-job]",
		Short: "job and pipeline logs",
		Args:  cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			if len(args) == 1 {
				if store.IsCommitID(args[0]) {
					logJob = args[0]
				} else {
					logPipeline = args[0]
				}
			}
			params := client.LogParams{Pipeline: logPipeline, Job: logJob, Since: logSince}
			if logFollow {
				rc, err := cliClient().FollowLogs(params)
				if err != nil {
					dieErr("logs", err, "")
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
				dieErr("logs", err, "")
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
	list := &cobra.Command{
		Use: "list",
		Run: func(_ *cobra.Command, _ []string) {
			ts, err := cliClient().ListTransactions()
			if err != nil {
				dieErr("transaction list", err, "")
			}
			if jsonOut {
				emitJSON(ts)
				return
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
	}
	list.Flags().BoolVar(&jsonOut, "json", false, "emit JSON instead of a table")
	cmd.AddCommand(
		&cobra.Command{
			Use: "start",
			Run: func(_ *cobra.Command, _ []string) {
				id, err := cliClient().StartTransaction()
				if err != nil {
					dieErr("transaction start", err, "")
				}
				fmt.Println(id)
			},
		},
		&cobra.Command{
			Use:  "finish <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().FinishTransaction(args[0]); err != nil {
					dieErr("transaction finish", err, "")
				}
				fmt.Printf("finished transaction %s\n", args[0])
			},
		},
		&cobra.Command{
			Use:  "delete <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().DeleteTransaction(args[0]); err != nil {
					dieErr("transaction delete", err, "")
				}
				fmt.Printf("deleted transaction %s\n", args[0])
			},
		},
		list,
		&cobra.Command{
			Use:  "inspect <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				tx, err := cliClient().InspectTransaction(args[0])
				if err != nil {
					dieErr("transaction inspect", err, "")
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
					dieErr("transaction resume", err, "")
				}
				fmt.Printf("resumed transaction %s (pipeline create/update will stage into it)\n", args[0])
			},
		},
		&cobra.Command{
			Use: "stop",
			Run: func(_ *cobra.Command, _ []string) {
				if err := setActiveTx(""); err != nil {
					dieErr("transaction stop", err, "")
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
