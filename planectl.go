package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"sandman/client"
)

// planectl implements the data-plane CLI on spf13/cobra (D-19: the CLI is
// a second consumer of the same client package the conformance suite
// drives — semantic and command-level compatibility with the reference,
// no wire compatibility). The reference surface is tracked verb by verb
// in sandman-behaviour-notes/implementation-review/CLI_SURFACE.md.
//
// The global flags -addr and -token (declared in main.go, parsed by the
// std flag package before cobra sees the args) select the control plane
// and credential for every verb.

func cliClient() *client.Client {
	c := client.New(*addrFlag)
	if *tokenFlag != "" {
		c.SetToken(*tokenFlag)
	}
	return c
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

// newDataPlaneCommands returns the data-plane subtree of the root command
// (repo, commit, branch, file, check, job, datum, pipeline, flush, secret,
// tag, logs, transaction).
func newDataPlaneCommands() []*cobra.Command {
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
	}
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
				for _, r := range repos {
					fmt.Printf("%s\t%d\t%s\n", r.Name, r.SizeBytes, strings.Join(r.Branches, ","))
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
				fmt.Printf("name: %s\nsize: %d\nbranches: %s\n", r.Name, r.SizeBytes, strings.Join(r.Branches, ", "))
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
				for _, cm := range hist {
					fmt.Printf("%s\t%s\tfinished=%v\n", cm.ID, cm.Branch, cm.Finished)
				}
			},
		},
		&cobra.Command{
			Use:  "inspect <id>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				cm, err := cliClient().InspectCommit(args[0])
				if err != nil {
					die("commit inspect: "+err.Error(), 1)
				}
				fmt.Printf("id: %s\nrepo: %s\nbranch: %s\nstarted: %v\nfinished: %v\n", cm.ID, cm.Repo, cm.Branch, cm.Started, cm.Finished)
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
				fmt.Printf("repo: %s\nbranch: %s\nhead: %s\n", b.Repo, b.Branch, b.Head)
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
					for _, b := range bs {
						fmt.Printf("%s\t%s\t%s\n", b.Repo, b.Branch, b.Head)
					}
					return
				}
				repos, err := cliClient().ListRepos()
				if err != nil {
					die("branch list: "+err.Error(), 1)
				}
				for _, r := range repos {
					for _, b := range r.Branches {
						fmt.Printf("%s\t%s\n", r.Name, b)
					}
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
				head, err := cliClient().HeadCommit(repo, branch)
				if err != nil {
					die("file get: "+err.Error(), 1)
				}
				data, err := cliClient().GetFile(head.ID, path)
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
				head, err := cliClient().HeadCommit(repo, branch)
				if err != nil {
					die("file inspect: "+err.Error(), 1)
				}
				desc, err := cliClient().DescribeFile(head.ID, path)
				if err != nil {
					die("file inspect: "+err.Error(), 1)
				}
				fmt.Println(desc)
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
				head, err := cliClient().HeadCommit(repo, branch)
				if err != nil {
					die("file list: "+err.Error(), 1)
				}
				var files []client.FileInfo
				if path != "" {
					files, err = cliClient().ListFilesGlob(head.ID, path)
				} else {
					files, err = cliClient().ListFiles(head.ID)
				}
				if err != nil {
					die("file list: "+err.Error(), 1)
				}
				for _, f := range files {
					fmt.Printf("%s\t%d\n", f.Path, f.Size)
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
				for _, j := range js {
					fmt.Printf("%s\t%s\t%s\n", j.ID, j.Pipeline, j.State)
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
				fmt.Printf("id: %s\npipeline: %s\nstate: %s\nreason: %s\noutputCommit: %s\n", j.ID, j.Pipeline, j.State, j.Reason, j.OutputCommit)
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
				for _, d := range page.Datums {
					fmt.Printf("%s\t%s\n", d.ID, d.State)
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
				fmt.Printf("id: %s\nstate: %s\nreason: %s\nstarted: %s\nfinished: %s\n", d.ID, d.State, d.Reason, d.Started, d.Finished)
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
	cmd.AddCommand(update)

	cmd.AddCommand(
		&cobra.Command{
			Use: "list",
			Run: func(_ *cobra.Command, _ []string) {
				ps, err := cliClient().ListPipelines()
				if err != nil {
					die("pipeline list: "+err.Error(), 1)
				}
				for _, p := range ps {
					fmt.Printf("%s\t%s\tv%d\n", p.Name, p.State, p.Version)
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
				fmt.Printf("name: %s\nstate: %s\nversion: %d\nreason: %s\n", p.Name, p.State, p.Version, p.Reason)
			},
		},
		&cobra.Command{
			Use:  "delete <name>",
			Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, args []string) {
				if err := cliClient().DeletePipeline(args[0], false, false); err != nil {
					die("pipeline delete: "+err.Error(), 1)
				}
				fmt.Printf("deleted pipeline %s\n", args[0])
			},
		},
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
				// normalize to the spec shape so the output round-trips
				// through `pipeline create -f` (the inspection carries
				// state/version/jobCounts the spec decoder ignores)
				b, err := json.Marshal(p)
				if err != nil {
					die("pipeline extract: "+err.Error(), 1)
				}
				var spec client.Pipeline
				if err := json.Unmarshal(b, &spec); err != nil {
					die("pipeline extract: "+err.Error(), 1)
				}
				out, err := json.MarshalIndent(spec, "", "  ")
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
				b, err := json.Marshal(p)
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
	if err := json.NewDecoder(r).Decode(&p); err != nil {
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
			for _, j := range js {
				fmt.Printf("%s\t%s\n", j.Pipeline, j.State)
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
				fmt.Printf("name: %s\ntype: %s\ncreated: %s\n", s.Name, s.Type, s.Created)
			},
		},
		&cobra.Command{
			Use: "list",
			Run: func(_ *cobra.Command, _ []string) {
				ss, err := cliClient().ListSecrets()
				if err != nil {
					die("secret list: "+err.Error(), 1)
				}
				for _, s := range ss {
					fmt.Printf("%s\t%s\n", s.Name, s.Type)
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
				for _, t := range ts {
					fmt.Printf("%s\t%s\n", t.Name, t.Ref)
				}
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
				for _, tx := range ts {
					fmt.Printf("%s\t%d ops\n", tx.ID, len(tx.Ops))
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
