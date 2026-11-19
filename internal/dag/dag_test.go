package dag

import (
	"strings"
	"testing"

	"sandman/client"
)

func nodeByID(g Graph, id string) (Node, bool) {
	for _, n := range g.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return Node{}, false
}

func hasEdge(g Graph, from, to, typ string) bool {
	for _, e := range g.Edges {
		if e.From == from && e.To == to && e.Type == typ {
			return true
		}
	}
	return false
}

// TestBuildFabricGraph covers one pipeline of each input shape the graph
// must expose: a plain repo, a cross, a git input with no stored repo, a
// cron input with no stored repo, and an orphan repo.
func TestBuildFabricGraph(t *testing.T) {
	par := &client.Parallelism{Constant: 3}
	pipelines := []client.PipelineInfo{
		{
			Name:         "extract",
			State:        "running",
			Version:      4,
			OutputBranch: "main",
			Parallelism:  par,
			Input:        &client.Input{Repo: "src", Glob: "/*"},
		},
		{
			Name:  "combine",
			State: "paused",
			Input: &client.Input{Cross: []client.Input{
				{Name: "extract", Repo: "extract", Glob: "/*"},
				{Name: "other", Repo: "other", Glob: "/*"},
			}},
		},
		{
			Name:  "gitsync",
			State: "running",
			Input: &client.Input{Git: &client.GitInput{URL: "https://example.com/org/vendor.git"}},
		},
		{
			Name:  "ticker",
			State: "standby",
			Input: &client.Input{Name: "ticks", Cron: "@every 30s"},
		},
	}
	repos := []RepoState{
		{Name: "src", Branches: []client.Branch{{Repo: "src", Branch: "master", Head: "0123456789abcdef"}}},
		{Name: "other"},
		{Name: "scratch", Branches: []client.Branch{{Repo: "scratch", Branch: "master", Head: "deadbeef"}}},
	}
	jobs := []client.Job{
		{ID: "old", Pipeline: "extract", State: "failure", Started: "2026-01-01T00:00:00Z"},
		{ID: "new", Pipeline: "extract", State: "success", Started: "2026-01-02T00:00:00Z", Finished: "2026-01-02T00:01:00Z"},
		{ID: "other-job", Pipeline: "combine", State: "running", Started: "2026-01-03T00:00:00Z"},
	}

	g := Build(pipelines, repos, jobs)

	// Every pipeline and every produced/consumed repo is a node, plus the
	// orphan scratch repo.
	for _, id := range []string{
		"pipeline:extract", "pipeline:combine", "pipeline:gitsync", "pipeline:ticker",
		"repo:src", "repo:extract", "repo:other", "repo:combine",
		"repo:vendor", "repo:gitsync", "repo:ticker-ticks", "repo:ticker", "repo:scratch",
	} {
		if _, ok := nodeByID(g, id); !ok {
			t.Fatalf("missing node %q in %+v", id, g.Nodes)
		}
	}
	if len(g.Nodes) != 13 {
		t.Fatalf("node count = %d, want 13: %+v", len(g.Nodes), g.Nodes)
	}
	if len(g.Edges) != 9 {
		t.Fatalf("edge count = %d, want 9: %+v", len(g.Edges), g.Edges)
	}
	if len(g.Orphans) != 1 || g.Orphans[0] != "scratch" {
		t.Fatalf("orphans = %v, want [scratch]", g.Orphans)
	}
	scratch, _ := nodeByID(g, "repo:scratch")
	if !scratch.Orphan {
		t.Fatal("scratch repo must be marked orphan")
	}

	if !hasEdge(g, "repo:src", "pipeline:extract", "input") {
		t.Fatal("missing plain repo input edge")
	}
	if !hasEdge(g, "repo:extract", "pipeline:combine", "input") {
		t.Fatal("missing cross member input edge")
	}
	if !hasEdge(g, "repo:other", "pipeline:combine", "input") {
		t.Fatal("missing second cross member input edge")
	}
	if !hasEdge(g, "repo:vendor", "pipeline:gitsync", "input") {
		t.Fatal("missing URL-derived git input edge")
	}
	if !hasEdge(g, "repo:ticker-ticks", "pipeline:ticker", "input") {
		t.Fatal("missing cron-derived input edge")
	}
	for _, name := range []string{"extract", "combine", "gitsync", "ticker"} {
		if !hasEdge(g, "pipeline:"+name, "repo:"+name, "output") {
			t.Fatalf("missing output edge for %s", name)
		}
	}

	extract, _ := nodeByID(g, "pipeline:extract")
	if extract.State != "running" || extract.Version != 4 || extract.OutputBranch != "main" {
		t.Fatalf("extract node = %+v", extract)
	}
	if extract.Parallelism == nil || extract.Parallelism.Constant != 3 {
		t.Fatalf("extract parallelism = %+v", extract.Parallelism)
	}
	if extract.LastJob == nil || extract.LastJob.ID != "new" || extract.LastJob.State != "success" {
		t.Fatalf("extract last job = %+v, want newest success", extract.LastJob)
	}
	if extract.LastJob.Time != "2026-01-02T00:01:00Z" {
		t.Fatalf("last job time = %q, want finished time", extract.LastJob.Time)
	}
	ticker, _ := nodeByID(g, "pipeline:ticker")
	if ticker.Cron != "@every 30s" {
		t.Fatalf("ticker cron = %q", ticker.Cron)
	}
	combine, _ := nodeByID(g, "pipeline:combine")
	if combine.OutputBranch != "master" {
		t.Fatalf("default output branch = %q, want master", combine.OutputBranch)
	}

	src, _ := nodeByID(g, "repo:src")
	if len(src.Branches) != 1 || src.Branches[0].Head != "0123456789abcdef" {
		t.Fatalf("src branches = %+v", src.Branches)
	}
}

// TestBuildCombinerMembers covers union, join, and group members: every
// nested side contributes a repo -> pipeline edge.
func TestBuildCombinerMembers(t *testing.T) {
	pipelines := []client.PipelineInfo{
		{Name: "u", Input: &client.Input{Union: []client.Input{
			{Name: "left", Repo: "ra"},
			{Name: "right", Repo: "rb"},
		}}},
		{Name: "j", Input: &client.Input{JoinOn: "$1", Join: []client.Input{
			{Repo: "rc"}, {Repo: "rd"},
		}}},
		{Name: "g", Input: &client.Input{GroupBy: "$1", Group: []client.Input{
			{Repo: "re"}, {Repo: "rf"},
		}}},
	}
	g := Build(pipelines, nil, nil)
	for _, c := range []struct{ repo, pipeline string }{
		{"ra", "u"}, {"rb", "u"},
		{"rc", "j"}, {"rd", "j"},
		{"re", "g"}, {"rf", "g"},
	} {
		if !hasEdge(g, "repo:"+c.repo, "pipeline:"+c.pipeline, "input") {
			t.Fatalf("missing %s -> %s", c.repo, c.pipeline)
		}
	}
}

// TestSelectReachable: selecting a middle pipeline keeps its whole upstream
// and downstream chain and excludes an unrelated pipeline.
func TestSelectReachable(t *testing.T) {
	pipelines := []client.PipelineInfo{
		{Name: "a", Input: &client.Input{Repo: "src"}},
		{Name: "b", Input: &client.Input{Repo: "a"}},
		{Name: "c", Input: &client.Input{Repo: "b"}},
		{Name: "unrelated", Input: &client.Input{Repo: "elsewhere"}},
	}
	g := Build(pipelines, nil, nil)

	sub, err := Select(g, "b")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{
		"pipeline:a", "pipeline:b", "pipeline:c",
		"repo:src", "repo:a", "repo:b", "repo:c",
	} {
		if _, ok := nodeByID(sub, id); !ok {
			t.Fatalf("subgraph missing %q: %+v", id, sub.Nodes)
		}
	}
	for _, id := range []string{"pipeline:unrelated", "repo:elsewhere"} {
		if _, ok := nodeByID(sub, id); ok {
			t.Fatalf("subgraph unexpectedly contains %q", id)
		}
	}
	if !hasEdge(sub, "repo:src", "pipeline:a", "input") ||
		!hasEdge(sub, "repo:b", "pipeline:c", "input") {
		t.Fatalf("subgraph lost chain edges: %+v", sub.Edges)
	}

	if _, err := Select(g, "no-such-pipeline"); err == nil {
		t.Fatal("selecting a missing pipeline must error")
	}
}

// TestDOTEscaping pins quoted-string escaping for names carrying the
// characters that would otherwise corrupt a DOT document.
func TestDOTEscaping(t *testing.T) {
	cases := []struct{ in, want string }{
		{`plain`, `"plain"`},
		{`a"b`, `"a\"b"`},
		{`a\b`, `"a\\b"`},
		{"a\nb", `"a\nb"`},
		{"a\rb\tc", `"a\rb\tc"`},
	}
	for _, c := range cases {
		if got := dotQuote(c.in); got != c.want {
			t.Fatalf("dotQuote(%q) = %s, want %s", c.in, got, c.want)
		}
	}

	g := Build([]client.PipelineInfo{{
		Name:  `a"b\c`,
		State: "failure",
		Input: &client.Input{Repo: `r"x\y`},
	}}, nil, nil)
	dot := DOT(g)
	for _, want := range []string{
		`"repo:r\"x\\y"`,
		`"pipeline:a\"b\\c"`,
		`-> "pipeline:a\"b\\c"`,
		`[style=dashed]`,
	} {
		if !strings.Contains(dot, want) {
			t.Fatalf("DOT lacks %q:\n%s", want, dot)
		}
	}
}
