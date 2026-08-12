package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"sandman/client"
)

// planectl implements the data-plane CLI: repo, commit, file, pipeline,
// job and flush verbs, thin wrappers over the same HTTP client the
// conformance suite drives. The CLI is the second consumer of the client
// contract (conformance is the first); the cli/ package exercises these
// verbs end-to-end through the binary.
//
// The global flags -addr and -token (declared in main.go) select the
// control plane and credential for every verb.

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

func cmdRepo(args []string) {
	fs := flag.NewFlagSet("repo", flag.ExitOnError)
	force := fs.Bool("force", false, "force delete even with history")
	if len(args) == 0 {
		die("usage: sandman repo <create|list|inspect|delete> ...", 2)
	}
	fs.Parse(args[1:])
	c := cliClient()
	rest := fs.Args()
	switch args[0] {
	case "create":
		if len(rest) != 1 {
			die("usage: sandman repo create <name>", 2)
		}
		if err := c.CreateRepo(rest[0]); err != nil {
			die("repo create: "+err.Error(), 1)
		}
		fmt.Printf("created repo %s\n", rest[0])
	case "list":
		repos, err := c.ListRepos()
		if err != nil {
			die("repo list: "+err.Error(), 1)
		}
		for _, r := range repos {
			fmt.Printf("%s\t%d\t%s\n", r.Name, r.SizeBytes, strings.Join(r.Branches, ","))
		}
	case "inspect":
		if len(rest) != 1 {
			die("usage: sandman repo inspect <name>", 2)
		}
		r, err := c.InspectRepo(rest[0])
		if err != nil {
			die("repo inspect: "+err.Error(), 1)
		}
		fmt.Printf("name: %s\nsize: %d\nbranches: %s\n", r.Name, r.SizeBytes, strings.Join(r.Branches, ", "))
	case "delete":
		if len(rest) != 1 {
			die("usage: sandman repo delete <name>", 2)
		}
		if err := c.DeleteRepo(rest[0], *force); err != nil {
			die("repo delete: "+err.Error(), 1)
		}
		fmt.Printf("deleted repo %s\n", rest[0])
	default:
		die(fmt.Sprintf("sandman: unknown repo verb %q", rest[0]), 2)
	}
}

func cmdCommit(args []string) {
	fs := flag.NewFlagSet("commit", flag.ExitOnError)
	if len(args) == 0 {
		die("usage: sandman commit <list|inspect> ...", 2)
	}
	fs.Parse(args[1:])
	c := cliClient()
	rest := fs.Args()
	switch args[0] {
	case "list":
		if len(rest) != 1 {
			die("usage: sandman commit list <repo>[@branch]", 2)
		}
		repo, branch, _, err := parseRef(rest[0])
		if err != nil {
			die(err.Error(), 2)
		}
		hist, err := c.CommitHistory(repo, branch)
		if err != nil {
			die("commit list: "+err.Error(), 1)
		}
		for _, cm := range hist {
			fmt.Printf("%s\t%s\tfinished=%v\n", cm.ID, cm.Branch, cm.Finished)
		}
	case "inspect":
		if len(rest) != 1 {
			die("usage: sandman commit inspect <id>", 2)
		}
		cm, err := c.InspectCommit(rest[0])
		if err != nil {
			die("commit inspect: "+err.Error(), 1)
		}
		fmt.Printf("id: %s\nrepo: %s\nbranch: %s\nstarted: %v\nfinished: %v\n", cm.ID, cm.Repo, cm.Branch, cm.Started, cm.Finished)
	default:
		die(fmt.Sprintf("sandman: unknown commit verb %q", rest[0]), 2)
	}
}

func cmdFile(args []string) {
	fs := flag.NewFlagSet("file", flag.ExitOnError)
	overwrite := fs.Bool("o", false, "overwrite accumulated content at the path (FS-3)")
	if len(args) == 0 {
		die("usage: sandman file <put|get|list> ...", 2)
	}
	fs.Parse(args[1:])
	c := cliClient()
	rest := fs.Args()
	switch args[0] {
	case "put":
		if len(rest) != 2 {
			die("usage: sandman file put <repo@branch:path> <src|->", 2)
		}
		repo, branch, path, err := parseRef(rest[0])
		if err != nil {
			die(err.Error(), 2)
		}
		if path == "" {
			die("file put: path required (repo@branch:path)", 2)
		}
		var data []byte
		if rest[1] == "-" {
			data, err = io.ReadAll(os.Stdin)
		} else {
			data, err = os.ReadFile(rest[1])
		}
		if err != nil {
			die("file put: "+err.Error(), 1)
		}
		cm, err := c.StartCommit(repo, branch, "")
		if err != nil {
			die("file put: "+err.Error(), 1)
		}
		if *overwrite {
			err = c.PutFileOverwrite(cm.ID, path, data)
		} else {
			err = c.PutFile(cm.ID, path, data)
		}
		if err != nil {
			die("file put: "+err.Error(), 1)
		}
		if _, err := c.FinishCommit(cm.ID, "", false); err != nil {
			die("file put: "+err.Error(), 1)
		}
		fmt.Printf("wrote %s@%s:%s (%d bytes, commit %s)\n", repo, branch, path, len(data), cm.ID)
	case "get":
		if len(rest) != 1 {
			die("usage: sandman file get <repo@branch:path>", 2)
		}
		repo, branch, path, err := parseRef(rest[0])
		if err != nil {
			die(err.Error(), 2)
		}
		if path == "" {
			die("file get: path required (repo@branch:path)", 2)
		}
		head, err := c.HeadCommit(repo, branch)
		if err != nil {
			die("file get: "+err.Error(), 1)
		}
		data, err := c.GetFile(head.ID, path)
		if err != nil {
			die("file get: "+err.Error(), 1)
		}
		_, _ = os.Stdout.Write(data)
	case "list":
		if len(rest) != 1 {
			die("usage: sandman file list <repo@branch>[:path]", 2)
		}
		repo, branch, path, err := parseRef(rest[0])
		if err != nil {
			die(err.Error(), 2)
		}
		head, err := c.HeadCommit(repo, branch)
		if err != nil {
			die("file list: "+err.Error(), 1)
		}
		var files []client.FileInfo
		if path != "" {
			files, err = c.ListFilesGlob(head.ID, path)
		} else {
			files, err = c.ListFiles(head.ID)
		}
		if err != nil {
			die("file list: "+err.Error(), 1)
		}
		for _, f := range files {
			fmt.Printf("%s\t%d\n", f.Path, f.Size)
		}
	default:
		die(fmt.Sprintf("sandman: unknown file verb %q", rest[0]), 2)
	}
}

func cmdPipeline(args []string) {
	fs := flag.NewFlagSet("pipeline", flag.ExitOnError)
	spec := fs.String("f", "-", "pipeline spec JSON file ('-' = stdin)")
	if len(args) == 0 {
		die("usage: sandman pipeline <create|list|inspect|delete> ...", 2)
	}
	fs.Parse(args[1:])
	c := cliClient()
	rest := fs.Args()
	switch args[0] {
	case "create":
		var r io.Reader = os.Stdin
		if *spec != "-" {
			f, err := os.Open(*spec)
			if err != nil {
				die("pipeline create: "+err.Error(), 1)
			}
			defer f.Close()
			r = f
		}
		var p client.Pipeline
		if err := json.NewDecoder(r).Decode(&p); err != nil {
			die("pipeline create: spec: "+err.Error(), 1)
		}
		if err := c.CreatePipeline(p); err != nil {
			die("pipeline create: "+err.Error(), 1)
		}
		fmt.Printf("created pipeline %s\n", p.Name)
	case "list":
		ps, err := c.ListPipelines()
		if err != nil {
			die("pipeline list: "+err.Error(), 1)
		}
		for _, p := range ps {
			fmt.Printf("%s\t%s\tv%d\n", p.Name, p.State, p.Version)
		}
	case "inspect":
		if len(rest) != 1 {
			die("usage: sandman pipeline inspect <name>", 2)
		}
		p, err := c.InspectPipeline(rest[0])
		if err != nil {
			die("pipeline inspect: "+err.Error(), 1)
		}
		fmt.Printf("name: %s\nstate: %s\nversion: %d\nreason: %s\n", p.Name, p.State, p.Version, p.Reason)
	case "delete":
		if len(rest) != 1 {
			die("usage: sandman pipeline delete <name>", 2)
		}
		if err := c.DeletePipeline(rest[0], false, false); err != nil {
			die("pipeline delete: "+err.Error(), 1)
		}
		fmt.Printf("deleted pipeline %s\n", rest[0])
	default:
		die(fmt.Sprintf("sandman: unknown pipeline verb %q", rest[0]), 2)
	}
}

func cmdJob(args []string) {
	fs := flag.NewFlagSet("job", flag.ExitOnError)
	if len(args) == 0 {
		die("usage: sandman job <list|inspect> ...", 2)
	}
	fs.Parse(args[1:])
	c := cliClient()
	rest := fs.Args()
	switch args[0] {
	case "list":
		if len(rest) > 1 {
			die("usage: sandman job list [pipeline]", 2)
		}
		filter := client.JobFilter{}
		if len(rest) == 1 {
			filter.Pipeline = rest[0]
		}
		js, err := c.ListJobsFiltered(filter)
		if err != nil {
			die("job list: "+err.Error(), 1)
		}
		for _, j := range js {
			fmt.Printf("%s\t%s\t%s\n", j.ID, j.Pipeline, j.State)
		}
	case "inspect":
		if len(rest) != 1 {
			die("usage: sandman job inspect <id>", 2)
		}
		j, err := c.InspectJob(rest[0])
		if err != nil {
			die("job inspect: "+err.Error(), 1)
		}
		fmt.Printf("id: %s\npipeline: %s\nstate: %s\nreason: %s\noutputCommit: %s\n", j.ID, j.Pipeline, j.State, j.Reason, j.OutputCommit)
	default:
		die(fmt.Sprintf("sandman: unknown job verb %q", rest[0]), 2)
	}
}

func cmdFlush(args []string) {
	if len(args) == 0 || args[0] != "commit" {
		die("usage: sandman flush commit <repo@branch> [-timeout d]", 2)
	}
	// the timeout flag may follow the ref, so parse by hand instead of a
	// FlagSet (which stops at the first positional)
	timeout := 10 * time.Minute
	var ref string
	rest := args[1:]
	for i := 0; i < len(rest); i++ {
		a := rest[i]
		switch {
		case a == "-timeout":
			if i+1 >= len(rest) {
				die("usage: sandman flush commit <repo@branch> [-timeout d]", 2)
			}
			i++
			d, err := time.ParseDuration(rest[i])
			if err != nil {
				die("flush: bad -timeout: "+err.Error(), 2)
			}
			timeout = d
		case strings.HasPrefix(a, "-timeout="):
			d, err := time.ParseDuration(strings.TrimPrefix(a, "-timeout="))
			if err != nil {
				die("flush: bad -timeout: "+err.Error(), 2)
			}
			timeout = d
		case strings.HasPrefix(a, "-"):
			die("flush: unknown flag "+a, 2)
		default:
			if ref != "" {
				die("usage: sandman flush commit <repo@branch> [-timeout d]", 2)
			}
			ref = a
		}
	}
	if ref == "" {
		die("usage: sandman flush commit <repo@branch> [-timeout d]", 2)
	}
	c := cliClient()
	repo, branch, _, err := parseRef(ref)
	if err != nil {
		die(err.Error(), 2)
	}
	head, err := c.HeadCommit(repo, branch)
	if err != nil {
		die("flush: "+err.Error(), 1)
	}
	js, err := c.Flush(head.ID, timeout)
	if err != nil {
		die("flush: "+err.Error(), 1)
	}
	for _, j := range js {
		fmt.Printf("%s\t%s\n", j.Pipeline, j.State)
	}
}
