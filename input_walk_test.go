package main

// Input-tree derivation consistency: every side derivation shares the
// single walkInputs walker, so a cron, size-trigger, or git side nested
// under any combiner (cross/union/join/group) is derived exactly like a
// top-level side. The cron and trigger derivations used to keep their
// own walkers that only descended cross and union — a tick or
// size-trigger side nested under a join or group member was invisible
// there (no repo, no ticker, no accumulation branch) while its
// enumeration twin (cronSideRepos) listed it, so derivation and
// enumeration drifted apart.

import (
	"path/filepath"
	"testing"

	"sandman/client"
	"sandman/internal/store"
)

// TestNestedSidesDeriveUnderGroup — a group whose members are a cron
// tick, a size-triggered repo side, and a git side: the create path's
// three derivations must reach the nested members (the cron repo and the
// git repo get created, the nested trigger gets its accumulation
// branch), not just the top level.
func TestNestedSidesDeriveUnderGroup(t *testing.T) {
	dir := t.TempDir()
	d := &daemon{state: dir}
	d.store = store.New(filepath.Join(dir, "store"))

	const pipelineName = "p-walk"
	gitURL := "https://github.com/theycallmeloki/example.git"
	const gitRepo = "example"

	group := []client.Input{
		{Name: "tick", Cron: "@every 1m"},
		{Name: "data", Repo: "data", Glob: "/*", Trigger: &client.Trigger{SizeBytes: 1000}},
		{Git: &client.GitInput{URL: gitURL}},
	}
	p := client.Pipeline{Name: pipelineName, Input: &client.Input{Group: group}}

	// the create/update path runs all three derivations on the spec
	// (pipeline.go: applyCreate/applyUpdate)
	d.deriveCronRepos(&p)
	d.deriveTriggerBranches(&p)
	d.deriveGitRepos(&p)

	if p.Input == nil || len(p.Input.Group) != 3 {
		t.Fatalf("group members = %v, want 3", p.Input)
	}
	tick := p.Input.Group[0]
	data := p.Input.Group[1]
	git := p.Input.Group[2]

	if tick.Cron == "" {
		t.Fatal("member 0 is not the cron side")
	}
	if want := cronRepo(pipelineName, "tick"); tick.Repo != want {
		t.Errorf("cron member repo = %q, want %q (cron nested under group must derive)", tick.Repo, want)
	}
	if tick.Glob != "/*" {
		t.Errorf("cron member glob = %q, want /*", tick.Glob)
	}

	if data.Trigger == nil {
		t.Fatal("member 1 lost its size trigger")
	}
	if data.Trigger.Branch != "master" {
		t.Errorf("nested trigger watched branch = %q, want master", data.Trigger.Branch)
	}
	if want := triggerBranch(pipelineName, 1); data.Branch != want {
		t.Errorf("nested trigger accumulation branch = %q, want %q", data.Branch, want)
	}

	if git.Git == nil {
		t.Fatal("member 2 is not the git side")
	}
	if git.Repo != gitRepo {
		t.Errorf("git member repo = %q, want %q", git.Repo, gitRepo)
	}
	if git.Glob != "/" {
		t.Errorf("git member glob = %q, want /", git.Glob)
	}

	// the derived repos were actually created: the cron tick repo (only
	// created when the walk reaches the nested cron member) and the
	// mapped git repo
	repos, err := d.store.ListRepos()
	if err != nil {
		t.Fatalf("list repos: %v", err)
	}
	got := map[string]bool{}
	for _, r := range repos {
		got[r.Name] = true
	}
	for _, want := range []string{cronRepo(pipelineName, "tick"), gitRepo} {
		if !got[want] {
			t.Errorf("derived repo %q was not created; repos = %v", want, repos)
		}
	}
}
