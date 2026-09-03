package cli

// Transfer verbs: put / get / ls / cat — the cp/less-style face of the
// data plane. `file` keeps its explicit subcommand surface (file
// put/get/list/... ) for parity with the reference; these verbs are the
// ergonomic entry points: cp-like argument order, whole-directory
// transfers, download-to-file, progress on payloads over a megabyte.
// Progress goes to stderr and only when stderr is a terminal, so stdout
// stays clean for scripts and data piping.

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"sandman/client"
	"sandman/internal/store"
)

// progressMin is the payload size below which no transfer progress is
// shown (a megabyte finishes too fast to matter).
const progressMin = 1 << 20

// progressOn reports whether a transfer of size bytes should display
// progress: stderr must be a terminal and the payload must exceed the
// progress floor.
func progressOn(size int64, noProgress bool) (*os.File, bool) {
	if noProgress || size < progressMin || !term.IsTerminal(int(os.Stderr.Fd())) {
		return nil, false
	}
	return os.Stderr, true
}

// countingReader wraps an upload body, printing a progress line to stderr
// at most every 100ms.
type countingReader struct {
	r     io.Reader
	n     int64
	total int64
	out   *os.File
	last  time.Time
}

func (c *countingReader) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	c.n += int64(n)
	if c.out != nil && c.total > 0 && time.Since(c.last) > 100*time.Millisecond {
		c.last = time.Now()
		fmt.Fprintf(c.out, "\r  up %s / %s", HumanSize(uint64(c.n)), HumanSize(uint64(c.total)))
	}
	return n, err
}

// countingWriter wraps a download destination the same way.
type countingWriter struct {
	w     io.Writer
	n     int64
	total int64
	out   *os.File
	last  time.Time
}

func (c *countingWriter) Write(b []byte) (int, error) {
	n, err := c.w.Write(b)
	c.n += int64(n)
	if c.out != nil && c.total > 0 && time.Since(c.last) > 100*time.Millisecond {
		c.last = time.Now()
		fmt.Fprintf(c.out, "\r  down %s / %s", HumanSize(uint64(c.n)), HumanSize(uint64(c.total)))
	}
	return n, err
}

// progressDone clears a progress line; a no-op when none was shown.
func progressDone(out *os.File) {
	if out != nil {
		_, _ = fmt.Fprintln(out)
	}
}

// joinPath joins a repo-path prefix with a relative path: the destination
// for one walked/listed file. path.Join cleans "."/".." and redundant
// separators; the result is repo-relative with no leading "./".
func joinPath(prefix, rel string) string {
	return strings.TrimPrefix(path.Join(prefix, rel), "./")
}

// ---- put ----

func putCmd() *cobra.Command {
	var append_, noProgress bool
	cmd := &cobra.Command{
		Use:   "put [flags] <src>... <repo[@branch][:path]>",
		Short: "upload local files to a repo (cp-like; directories upload recursively)",
		Long: `Upload local files into a repository, cp-style: sources first,
destination last.

  sandman put data.csv in@master:data.csv         single file
  sandman put - data.json < data.json            stdin
  sandman put dataset/ in@master:dataset         a whole tree
  sandman put a.csv b.csv in@master:data/        files into data/
  sandman put f.csv <commit-id>:f.csv            write into an open commit

A branch destination starts one commit holding every file of the
transfer and finishes it (the branch head advances); a commit-id
destination writes into that open commit without finishing — the
explicit commit flow. A plain put replaces content at each destination
path (cp semantics); --append grows accumulated content instead.`,
		Args: cobra.MinimumNArgs(2),
		Run: func(_ *cobra.Command, args []string) {
			srcs, dst := args[:len(args)-1], args[len(args)-1]
			putRun(srcs, dst, append_, noProgress, "put")
		},
	}
	cmd.Flags().BoolVarP(&append_, "append", "a", false, "append to content accumulated at each destination path")
	cmd.Flags().BoolVar(&noProgress, "no-progress", false, "disable the transfer progress display")
	return cmd
}

// putRun is the shared upload: `file put <ref> <src>` (reference order)
// and `put <src>... <ref>` (cp order) both land here.
func putRun(srcs []string, dst string, append_, noProgress bool, verb string) {
	c := cliClient()
	repo, branch, dstPath := "", "", ""
	// a bare commit-id destination (<commit-id>:path) writes into that
	// open commit; the repo is derived from the commit itself
	if !strings.Contains(dst, "@") {
		if cid, p, ok := strings.Cut(dst, ":"); ok && store.IsCommitID(cid) {
			cm, err := c.InspectCommit(cid)
			if err != nil {
				dieErr(verb, err, "")
			}
			repo, branch, dstPath = cm.Repo, cid, p
		}
	}
	if repo == "" {
		var err error
		repo, branch, dstPath, err = parseRef(dst)
		if err != nil {
			die(err.Error(), 2)
		}
	}
	if _, err := c.InspectRepo(repo); err != nil {
		dieErr(verb, err, fmt.Sprintf("create it with: sandman repo create %s", repo))
	}

	// enumerate the local sources before any commit exists, so a broken
	// source fails the transfer without leaving an open commit behind
	type upload struct{ local, target string }
	var ups []upload
	for _, src := range srcs {
		if src == "-" {
			if dstPath == "" {
				die(fmt.Sprintf("%s: stdin needs a destination path (repo@branch:path)", verb), 2)
			}
			ups = append(ups, upload{local: "-", target: dstPath})
			continue
		}
		fi, err := os.Stat(src)
		if err != nil {
			dieErr(verb, err, "")
		}
		if fi.IsDir() {
			err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if d.IsDir() {
					return nil
				}
				rel, err := filepath.Rel(src, p)
				if err != nil {
					return err
				}
				ups = append(ups, upload{local: p, target: joinPath(dstPath, filepath.ToSlash(rel))})
				return nil
			})
			if err != nil {
				dieErr(verb, err, "")
			}
			continue
		}
		if dstPath == "" {
			die(fmt.Sprintf("%s: destination path required for a file (repo@branch:path)", verb), 2)
		}
		target := dstPath
		if strings.HasSuffix(dstPath, "/") {
			target = joinPath(dstPath, filepath.Base(src))
		}
		ups = append(ups, upload{local: src, target: target})
	}
	if len(ups) == 0 {
		die(fmt.Sprintf("%s: no files to upload", verb), 1)
	}

	// resolve the write target: a commit id writes into that open commit
	// (the explicit flow); a branch starts one commit for the transfer
	explicit := store.IsCommitID(branch)
	cmID := branch
	cleanup := func() {}
	if explicit {
		cm, err := c.InspectCommit(branch)
		if err != nil {
			dieErr(verb, err, "")
		}
		if cm.Repo != repo {
			die(fmt.Sprintf("%s: commit %s is in repo %s, not %s", verb, branch, cm.Repo, repo), 1)
		}
		if cm.Finished {
			die(fmt.Sprintf("%s: commit %s is finished; start a new commit to write", verb, branch), 1)
		}
	} else {
		cm, err := c.StartCommit(repo, branch, "")
		if err != nil {
			dieErr(verb, err, "")
		}
		cmID = cm.ID
		// a failed transfer must not leave an open commit on the branch
		cleanup = func() {
			if err := c.DeleteCommit(cmID); err == nil {
				fmt.Fprintf(os.Stderr, "%s: upload failed, commit %s discarded\n", verb, cmID)
			}
		}
	}

	fail := func(err error) {
		cleanup()
		dieErr(verb, err, "")
	}
	for _, u := range ups {
		var r io.Reader
		var size int64
		var closeSrc func()
		if u.local == "-" {
			r, closeSrc = os.Stdin, func() {}
		} else {
			f, err := os.Open(u.local)
			if err != nil {
				fail(err)
			}
			fi, err := f.Stat()
			if err != nil {
				f.Close()
				fail(err)
			}
			size, r, closeSrc = fi.Size(), f, func() { f.Close() }
		}
		out, on := progressOn(size, noProgress)
		var cr *countingReader
		n := size
		if on {
			cr = &countingReader{r: r, total: size, out: out}
			r = cr
		} else if u.local == "-" {
			// stdin: count the bytes for the summary line (no progress
			// bar — the total is unknown)
			cr = &countingReader{r: r}
			r = cr
		}
		var err error
		if append_ {
			err = c.PutFileAppendStream(cmID, u.target, r)
		} else {
			err = c.PutFileStream(cmID, u.target, r)
		}
		progressDone(out)
		closeSrc()
		if err != nil {
			fail(err)
		}
		if cr != nil {
			n = cr.n
		}
		fmt.Printf("wrote %s@%s:%s (%s, commit %s)\n", repo, branch, u.target, HumanSize(uint64(n)), cmID)
	}
	if !explicit {
		if _, err := c.FinishCommit(cmID, "", false); err != nil {
			cleanup()
			dieErr(verb, err, "")
		}
	}
}

// ---- get ----

func getCmd() *cobra.Command {
	var output string
	var noProgress bool
	parent := &cobra.Command{
		Use:   "get [flags] <repo[@branch][:path]>",
		Short: "fetch files from a repo (stdout, or -o to a file or directory)",
		Long: `Fetch a file from a commit, cp/less-style. Without -o the content
goes to stdout (pipe it anywhere); with -o it lands in a file or — for
a directory or a glob — a whole tree:

  sandman get in@master:data.csv                    stdout
  sandman get in@master:data.csv -o data.csv        to a file
  sandman get in@master:dataset -o out/             a whole directory
  sandman get in@master:data/*.csv -o out/          a glob of files
  sandman get in@master -o snap/                    the whole repo

A directory destination (trailing slash) keeps the file's base name;
a tree download strips the requested prefix from each path.`,
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			getRun(cliClient(), args[0], output, noProgress, "get")
		},
	}
	parent.Flags().StringVarP(&output, "output", "o", "", "write to this file or directory (default: stdout)")
	parent.Flags().BoolVar(&noProgress, "no-progress", false, "disable the transfer progress display")
	parent.AddCommand(fileGetCmd())
	return parent
}

// fileGetCmd is the reference's `get file <ref>` / `file get <ref>`:
// identical resolution, kept as subcommands for parity.
func fileGetCmd() *cobra.Command {
	var output string
	var noProgress bool
	cmd := &cobra.Command{
		Use:   "file <repo@branch:path>",
		Short: "fetch a file from a commit (same as `get` without -o)",
		Args:  cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			getRun(cliClient(), args[0], output, noProgress, "get file")
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "", "write to this file or directory (default: stdout)")
	cmd.Flags().BoolVar(&noProgress, "no-progress", false, "disable the transfer progress display")
	return cmd
}

// getRun resolves ref and downloads: stdout for a single file, or -o a
// file/dir/tree per the rules in get's help.
func getRun(c *client.Client, ref, output string, noProgress bool, verb string) {
	repo, branch, p, err := parseRef(ref)
	if err != nil {
		die(err.Error(), 2)
	}
	if repo == "" {
		die(fmt.Sprintf("%s: invalid ref %q", verb, ref), 2)
	}
	head, err := resolveCommitRef(c, repo, branch)
	if err != nil {
		dieErr(verb, err, "")
	}

	if p == "" {
		// whole-repo download: every file at the head into -o DIR
		if output == "" {
			die(fmt.Sprintf("%s: downloading a whole repo needs -o DIR", verb), 2)
		}
		files, err := c.ListFiles(head)
		if err != nil {
			dieErr(verb, err, "")
		}
		for _, f := range files {
			downloadOne(c, head, f, filepath.Join(output, f.Path), noProgress, verb)
		}
		return
	}

	// classify the target: a directory (files under p/), a glob, or a
	// single file. The directory probe is exact: p/* only matches files
	// actually under p (the listing API accepts prefix patterns only).
	// A glob argument is never a directory.
	under, err := c.ListFilesGlob(head, listingGlob(p+"/"))
	if err != nil {
		dieErr(verb, err, "")
	}
	if !strings.Contains(p, "*") && (strings.HasSuffix(p, "/") || len(under) > 0) {
		if output == "" {
			die(fmt.Sprintf("%s: downloading a directory needs -o DIR", verb), 2)
		}
		prefix := strings.TrimSuffix(p, "/")
		for _, f := range under {
			rel := strings.TrimPrefix(strings.TrimPrefix(f.Path, prefix), "/")
			downloadOne(c, head, f, filepath.Join(output, rel), noProgress, verb)
		}
		return
	}

	matches, err := c.ListFilesGlob(head, listingGlob(p))
	if err != nil {
		dieErr(verb, err, "")
	}
	switch {
	case len(matches) == 0:
		die(fmt.Sprintf("%s: %s not found", verb, ref), 1)
	case len(matches) == 1:
		dest := output
		if dest == "" {
			if _, err := c.FetchFileTo(os.Stdout, head, matches[0].Path, false, 0); err != nil {
				dieErr(verb, err, "")
			}
			return
		}
		if strings.HasSuffix(dest, "/") || dirExists(dest) {
			dest = filepath.Join(dest, filepath.Base(matches[0].Path))
		}
		downloadOne(c, head, matches[0], dest, noProgress, verb)
	default:
		if output == "" {
			die(fmt.Sprintf("%s: %s matches %d files — use -o DIR to download them", verb, ref, len(matches)), 1)
		}
		// glob: strip the literal prefix before the first "*"
		prefix := p[:strings.Index(p, "*")]
		prefix = strings.TrimSuffix(prefix, "/")
		for _, f := range matches {
			rel := strings.TrimPrefix(strings.TrimPrefix(f.Path, prefix), "/")
			downloadOne(c, head, f, filepath.Join(output, rel), noProgress, verb)
		}
	}
}

// downloadOne streams one file from a commit to dest, showing progress on
// stderr for payloads over the floor. A failed download removes the
// partial file so it never looks complete.
func downloadOne(c *client.Client, head string, f client.FileInfo, dest string, noProgress bool, verb string) {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		dieErr(verb, err, "")
	}
	of, err := os.Create(dest)
	if err != nil {
		dieErr(verb, err, "")
	}
	out, on := progressOn(int64(f.Size), noProgress)
	var w io.Writer = of
	if on {
		w = &countingWriter{w: of, total: int64(f.Size), out: out}
	}
	_, err = c.FetchFileTo(w, head, f.Path, false, 0)
	progressDone(out)
	cerr := of.Close()
	if err != nil || cerr != nil {
		os.Remove(dest)
		if err != nil {
			dieErr(verb, err, "")
		}
		dieErr(verb, cerr, "")
	}
	fmt.Printf("wrote %s (%s)\n", dest, HumanSize(f.Size))
}

func dirExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

// ---- ls / cat ----

func lsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ls [repo[@branch][:path]]",
		Short: "list repos, or files in a repo (default: all repos)",
		Args:  cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			if len(args) == 0 {
				repoList()
				return
			}
			listFilesRef(args[0])
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "emit JSON instead of a table")
	return cmd
}

func catCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cat <repo@branch:path>...",
		Short: "fetch files to stdout (get without -o)",
		Args:  cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			c := cliClient()
			for _, ref := range args {
				getRun(c, ref, "", true, "cat")
			}
		},
	}
	return cmd
}

// repoList prints every repo, table or JSON (shared by `repo list` and
// `ls`).
func repoList() {
	repos, err := cliClient().ListRepos()
	if err != nil {
		dieErr("repo list", err, "")
	}
	if jsonOut {
		emitJSON(repos)
		return
	}
	rows := make([][]string, 0, len(repos))
	for _, r := range repos {
		rows = append(rows, []string{r.Name, HumanSize(r.SizeBytes), strings.Join(r.Branches, ",")})
	}
	table([]string{"NAME", "SIZE", "BRANCHES"}, rows)
	if len(rows) == 0 {
		fmt.Println("no repos")
	}
}

// listFilesRef prints files at a repo@branch[:path] ref, table or JSON
// (shared by `file list` and `ls`).
func listFilesRef(ref string) {
	repo, branch, p, err := parseRef(ref)
	if err != nil {
		die(err.Error(), 2)
	}
	head, err := resolveCommitRef(cliClient(), repo, branch)
	if err != nil {
		dieErr("file list", err, "")
	}
	var files []client.FileInfo
	if p != "" {
		files, err = cliClient().ListFilesGlob(head, listingGlob(p))
	} else {
		files, err = cliClient().ListFiles(head)
	}
	if err != nil {
		dieErr("file list", err, "")
	}
	if jsonOut {
		emitJSON(files)
		return
	}
	rows := make([][]string, 0, len(files))
	for _, f := range files {
		rows = append(rows, []string{f.Path, HumanSize(f.Size)})
	}
	table([]string{"PATH", "SIZE"}, rows)
	if len(rows) == 0 {
		fmt.Println("no files")
	}
}
