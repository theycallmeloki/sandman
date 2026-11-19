// Package dag builds, selects, and renders the fabric's pipeline graph.
//
// The graph is deliberately static: one node per pipeline, one node per
// repository the fabric knows about, and edges from a consumed repo into
// its pipeline and from a pipeline into its output repo. It is built from
// pipeline specs, repo refs, and job records only — never by walking
// commit ancestry, so it stays cheap enough for a live dashboard poll.
package dag

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"sandman/client"
)

// Node is one graph vertex. Kind is "pipeline" or "repo"; the remaining
// fields are meaningful for the matching kind and omitted otherwise.
type Node struct {
	ID   string `json:"id"` // "pipeline:<name>" or "repo:<name>"
	Kind string `json:"kind"`
	Name string `json:"name"`

	// pipeline nodes
	State        string              `json:"state,omitempty"`
	Version      int                 `json:"version,omitempty"`
	OutputBranch string              `json:"outputBranch,omitempty"`
	Parallelism  *client.Parallelism `json:"parallelism,omitempty"`
	Cron         string              `json:"cron,omitempty"`
	LastJob      *LastJob            `json:"lastJob,omitempty"`

	// repo nodes
	Branches []BranchHead `json:"branches,omitempty"`
	Orphan   bool         `json:"orphan,omitempty"`
}

// LastJob is a pipeline's most recent job: the operator's first answer to
// "is it running, and did the last run work?".
type LastJob struct {
	ID    string `json:"id"`
	State string `json:"state"`
	Time  string `json:"time,omitempty"`
}

// BranchHead is one branch ref in a repo node.
type BranchHead struct {
	Branch string `json:"branch"`
	Head   string `json:"head"`
}

// Edge is a directed graph edge. Type is "input" for repo -> pipeline and
// "output" for pipeline -> repo.
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Type string `json:"type"`
}

// Graph is the whole fabric graph.
type Graph struct {
	Nodes   []Node   `json:"nodes"`
	Edges   []Edge   `json:"edges"`
	Orphans []string `json:"orphans"`
}

// RepoState is the cheap repo state the graph carries: the repo's branch
// heads. File counts are intentionally absent because resolving a head
// view walks commit ancestry.
type RepoState struct {
	Name     string
	Branches []client.Branch
}

// Build constructs the graph from pipeline specs, repo refs, and jobs.
//
// Input sides are walked through every combiner (cross, union, join,
// group). A side's repo is its declared repo when the stored spec carries
// one; otherwise it is derived from a git input's custom name or URL, or
// from a cron input's pipeline-qualified name. Repos not consumed or
// produced by any pipeline are marked orphans.
func Build(pipelines []client.PipelineInfo, repos []RepoState, jobs []client.Job) Graph {
	g := Graph{Nodes: []Node{}, Edges: []Edge{}, Orphans: []string{}}
	nodes := make(map[string]*Node, len(pipelines)+len(repos))
	repoStates := make(map[string]RepoState, len(repos))

	ensureRepo := func(name string) *Node {
		id := "repo:" + name
		if n, ok := nodes[id]; ok {
			return n
		}
		n := &Node{ID: id, Kind: "repo", Name: name}
		if rs, ok := repoStates[name]; ok {
			n.Branches = make([]BranchHead, 0, len(rs.Branches))
			for _, b := range rs.Branches {
				n.Branches = append(n.Branches, BranchHead{Branch: b.Branch, Head: b.Head})
			}
			sort.Slice(n.Branches, func(i, j int) bool { return n.Branches[i].Branch < n.Branches[j].Branch })
		}
		nodes[id] = n
		return n
	}

	// every known repo is a vertex, even when no pipeline references it:
	// seeing an orphan repo in the fabric is the point of "whole graph"
	for _, rs := range repos {
		repoStates[rs.Name] = rs
		ensureRepo(rs.Name)
	}

	last := make(map[string]client.Job, len(jobs))
	for _, j := range jobs {
		cur, ok := last[j.Pipeline]
		if !ok || jobNewer(j, cur) {
			last[j.Pipeline] = j
		}
	}

	consumed := make(map[string]bool, len(repos))
	produced := make(map[string]bool, len(pipelines))
	edges := make(map[string]Edge, len(pipelines)*2+len(repos))
	addEdge := func(from, to, typ string) {
		key := from + "\x00" + to
		if _, ok := edges[key]; ok {
			return
		}
		edges[key] = Edge{From: from, To: to, Type: typ}
	}

	for i := range pipelines {
		p := pipelines[i]
		pipelineID := "pipeline:" + p.Name
		pn, ok := nodes[pipelineID]
		if !ok {
			pn = &Node{ID: pipelineID, Kind: "pipeline", Name: p.Name}
			nodes[pipelineID] = pn
		}
		pn.State = p.State
		pn.Version = p.Version
		pn.OutputBranch = p.OutputBranch
		if pn.OutputBranch == "" {
			pn.OutputBranch = "master"
		}
		pn.Parallelism = p.Parallelism
		if j, ok := last[p.Name]; ok {
			pn.LastJob = &LastJob{ID: j.ID, State: j.State, Time: jobTime(j)}
		}

		crons := map[string]bool{}
		walkInputs(p.Input, func(in *client.Input) {
			if in.Cron != "" {
				crons[in.Cron] = true
			}
			repo := sideRepo(p.Name, in)
			if repo == "" {
				return
			}
			ensureRepo(repo)
			consumed[repo] = true
			addEdge("repo:"+repo, pipelineID, "input")
		})
		if len(crons) > 0 {
			schedules := make([]string, 0, len(crons))
			for s := range crons {
				schedules = append(schedules, s)
			}
			sort.Strings(schedules)
			pn.Cron = strings.Join(schedules, ", ")
		}

		// The output repo is named after the pipeline and exists from
		// creation, whether or not a job has written a commit yet.
		ensureRepo(p.Name)
		produced[p.Name] = true
		addEdge(pipelineID, "repo:"+p.Name, "output")
	}

	// Every repo node is a candidate orphan; production and consumption
	// both connect it to the pipeline fabric.
	for _, n := range nodes {
		if n.Kind != "repo" {
			continue
		}
		if !consumed[n.Name] && !produced[n.Name] {
			n.Orphan = true
			g.Orphans = append(g.Orphans, n.Name)
		}
	}
	sort.Strings(g.Orphans)

	g.Nodes = make([]Node, 0, len(nodes))
	for _, n := range nodes {
		g.Nodes = append(g.Nodes, *n)
	}
	sort.Slice(g.Nodes, func(i, j int) bool { return g.Nodes[i].ID < g.Nodes[j].ID })

	g.Edges = make([]Edge, 0, len(edges))
	for _, e := range edges {
		g.Edges = append(g.Edges, e)
	}
	sort.Slice(g.Edges, func(i, j int) bool {
		if g.Edges[i].From != g.Edges[j].From {
			return g.Edges[i].From < g.Edges[j].From
		}
		return g.Edges[i].To < g.Edges[j].To
	})
	return g
}

// walkInputs visits every input node depth-first, descending through each
// combiner before visiting the node itself (the same shape as the daemon's
// git/cron derivations).
func walkInputs(in *client.Input, visit func(*client.Input)) {
	if in == nil {
		return
	}
	for i := range in.Cross {
		walkInputs(&in.Cross[i], visit)
	}
	for i := range in.Union {
		walkInputs(&in.Union[i], visit)
	}
	for i := range in.Join {
		walkInputs(&in.Join[i], visit)
	}
	for i := range in.Group {
		walkInputs(&in.Group[i], visit)
	}
	visit(in)
}

// sideRepo resolves the repo an input node consumes. Stored pipeline specs
// already carry derived repos; the fallbacks keep Build correct for raw
// specs (tests, API callers) with git or cron inputs.
func sideRepo(pipeline string, in *client.Input) string {
	if in.Repo != "" {
		return in.Repo
	}
	if in.Git != nil {
		if in.Name != "" {
			return in.Name
		}
		return gitRepoName(in.Git.URL)
	}
	if in.Cron != "" && in.Name != "" {
		return pipeline + "-" + in.Name
	}
	return ""
}

// gitRepoName mirrors the daemon's URL-derived mapped repo name: the last
// path segment without ".git".
func gitRepoName(u string) string {
	trimmed := strings.TrimSuffix(u, ".git")
	if i := strings.LastIndexByte(trimmed, '/'); i >= 0 {
		return trimmed[i+1:]
	}
	return trimmed
}

// jobNewer orders jobs by start time, then id, so the chosen "last job" is
// deterministic even for records written in the same instant.
func jobNewer(a, b client.Job) bool {
	if a.Started != b.Started {
		return a.Started > b.Started
	}
	return a.ID > b.ID
}

func jobTime(j client.Job) string {
	if j.Finished != "" {
		return j.Finished
	}
	return j.Started
}

// Select returns the subgraph reachable upstream and downstream from the
// named pipeline. Upstream follows incoming edges (input repos and their
// producers), downstream follows outgoing edges (output repos and their
// consumers). The result keeps the original node and edge order.
func Select(g Graph, name string) (Graph, error) {
	root := "pipeline:" + name
	found := false
	for _, n := range g.Nodes {
		if n.ID == root && n.Kind == "pipeline" {
			found = true
			break
		}
	}
	if !found {
		return Graph{}, fmt.Errorf("pipeline %q not found in the dag", name)
	}

	selected := reach(g, root, true)
	for id := range reach(g, root, false) {
		selected[id] = true
	}

	out := Graph{Nodes: []Node{}, Edges: []Edge{}, Orphans: []string{}}
	for _, n := range g.Nodes {
		if selected[n.ID] {
			out.Nodes = append(out.Nodes, n)
		}
	}
	for _, e := range g.Edges {
		if selected[e.From] && selected[e.To] {
			out.Edges = append(out.Edges, e)
		}
	}
	for _, o := range g.Orphans {
		if selected["repo:"+o] {
			out.Orphans = append(out.Orphans, o)
		}
	}
	return out, nil
}

// reach returns the node ids reachable from root following outgoing edges,
// or incoming edges when reverse is set.
func reach(g Graph, root string, reverse bool) map[string]bool {
	adj := make(map[string][]string, len(g.Edges))
	for _, e := range g.Edges {
		from, to := e.From, e.To
		if reverse {
			from, to = to, from
		}
		adj[from] = append(adj[from], to)
	}
	seen := map[string]bool{root: true}
	queue := []string{root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range adj[cur] {
			if seen[next] {
				continue
			}
			seen[next] = true
			queue = append(queue, next)
		}
	}
	return seen
}

// DOT renders the graph as Graphviz DOT. Labels are quoted and escaped, so
// names may contain quotes, backslashes, or newlines without corrupting
// the document.
func DOT(g Graph) string {
	var b strings.Builder
	b.WriteString("digraph sandman {\n")
	b.WriteString("  rankdir=LR;\n")
	b.WriteString("  graph [fontname=\"Helvetica\"];\n")
	b.WriteString("  node [shape=box, style=\"rounded,filled\", fontname=\"Helvetica\", fontsize=11];\n")
	b.WriteString("  edge [fontname=\"Helvetica\", fontsize=9, color=\"#6b7280\"];\n")
	for _, n := range g.Nodes {
		attrs := []string{"label=" + dotQuote(nodeLabel(n))}
		if n.Kind == "repo" {
			attrs = append(attrs,
				"shape=cylinder",
				"fillcolor=\"#f3f4f6\"",
				"color=\"#9ca3af\"",
			)
		} else {
			attrs = append(attrs,
				"fillcolor="+dotQuote(stateFill(n.State)),
				"color="+dotQuote(stateStroke(n.State)),
				"penwidth=1.5",
			)
		}
		if n.Orphan {
			attrs = append(attrs, "style=\"rounded,filled,dashed\"")
		}
		b.WriteString("  ")
		b.WriteString(dotQuote(n.ID))
		b.WriteString(" [")
		b.WriteString(strings.Join(attrs, ", "))
		b.WriteString("];\n")
	}
	for _, e := range g.Edges {
		b.WriteString("  ")
		b.WriteString(dotQuote(e.From))
		b.WriteString(" -> ")
		b.WriteString(dotQuote(e.To))
		if e.Type == "output" {
			b.WriteString(" [style=dashed]")
		}
		b.WriteString(";\n")
	}
	b.WriteString("}\n")
	return b.String()
}

// nodeLabel is the multi-line label shown inside a DOT node.
func nodeLabel(n Node) string {
	if n.Kind == "repo" {
		lines := []string{n.Name}
		if n.Orphan {
			lines = append(lines, "orphan")
		}
		for _, b := range n.Branches {
			lines = append(lines, b.Branch+"@"+shortID(b.Head))
		}
		return strings.Join(lines, "\n")
	}
	lines := []string{n.Name}
	state := n.State
	if state == "" {
		state = "unknown"
	}
	if n.Version > 0 {
		state += " · v" + strconv.Itoa(n.Version)
	}
	if n.Parallelism != nil {
		state += " · par " + parallelismString(n.Parallelism)
	}
	lines = append(lines, state)
	if n.Cron != "" {
		lines = append(lines, "cron "+n.Cron)
	}
	if n.LastJob != nil {
		lines = append(lines, "last "+n.LastJob.State+" "+shortID(n.LastJob.ID))
	}
	return strings.Join(lines, "\n")
}

func parallelismString(p *client.Parallelism) string {
	switch {
	case p == nil:
		return ""
	case p.Constant != 0:
		return strconv.Itoa(p.Constant)
	case p.Coefficient != 0:
		return strconv.FormatFloat(p.Coefficient, 'g', -1, 64)
	default:
		return "1"
	}
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12] + "…"
	}
	return id
}

func stateFill(s string) string {
	switch s {
	case "running":
		return "#dcfce7"
	case "failure", "crashed":
		return "#fee2e2"
	case "paused", "standby", "queued":
		return "#fef3c7"
	default:
		return "#e0f2fe"
	}
}

func stateStroke(s string) string {
	switch s {
	case "running":
		return "#16a34a"
	case "failure", "crashed":
		return "#dc2626"
	case "paused", "standby", "queued":
		return "#d97706"
	default:
		return "#0284c7"
	}
}

// dotQuote quotes a DOT string and escapes the characters that can end or
// corrupt it. Newlines become the DOT line-break escape \n.
func dotQuote(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
