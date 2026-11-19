package main

// Read-only fabric DAG: every pipeline and every repo it consumes or
// produces, plus repo refs and each pipeline's last job. The handler reads
// pipeline records, repo refs, and lightweight job listings — it never
// resolves a head view, so a dashboard poll does not walk commit ancestry.

import (
	"fmt"
	"net/http"
	"strings"

	dagpkg "sandman/internal/dag"
)

func (d *daemon) dagH(w http.ResponseWriter, r *http.Request) error {
	format := strings.ToLower(r.URL.Query().Get("format"))
	if format == "" {
		format = "json"
	}
	if format != "json" && format != "dot" {
		return fmt.Errorf("unsupported dag format %q (use json or dot)", format)
	}

	pipelines, err := d.listPipelinesFiltered(nil, "", true)
	if err != nil {
		return err
	}
	names, err := d.store.ListRepoNames()
	if err != nil {
		return err
	}
	repos := make([]dagpkg.RepoState, 0, len(names))
	for _, name := range names {
		branches, err := d.store.BranchRefs(name)
		if err != nil {
			// a repo can disappear between the directory scan and the
			// ref read; its absence is not a reason to fail the graph
			branches = nil
		}
		repos = append(repos, dagpkg.RepoState{Name: name, Branches: branches})
	}
	jobs, err := d.listJobsFiltered("", "", nil, false, nil, nil)
	if err != nil {
		return err
	}

	g := dagpkg.Build(pipelines, repos, jobs)
	if format == "dot" {
		w.Header().Set("Content-Type", "text/vnd.graphviz; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write([]byte(dagpkg.DOT(g)))
		return nil
	}
	writeJSON(w, g)
	return nil
}
