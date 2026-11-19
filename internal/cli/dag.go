package cli

// `sandman dag`: the fabric's pipeline topology as a compact summary,
// JSON, or Graphviz DOT. The daemon computes the graph once; --pipeline
// selects the subgraph reachable upstream and downstream from a named
// pipeline, which is the common "what feeds this and what breaks if it
// fails?" question.

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"sandman/internal/dag"
)

func newDagCmd() *cobra.Command {
	var (
		format   string
		pipeline string
		asJSON   bool
	)
	cmd := &cobra.Command{
		Use:   "dag",
		Short: "show the pipeline/repo DAG (summary, JSON, or DOT)",
		Args:  cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			f := strings.ToLower(format)
			if asJSON {
				if f != "" && f != "json" {
					dieErr("dag", fmt.Errorf("--json conflicts with --format %s", format), "")
				}
				f = "json"
			}
			if f != "" && f != "json" && f != "dot" {
				dieErr("dag", fmt.Errorf("unsupported format %q (use json or dot)", format), "")
			}

			raw, err := cliClient().DAG()
			if err != nil {
				dieErr("dag", err, "")
			}
			var g dag.Graph
			if err := json.Unmarshal(raw, &g); err != nil {
				dieErr("dag", err, "")
			}
			if pipeline != "" {
				g, err = dag.Select(g, pipeline)
				if err != nil {
					dieErr("dag", err, "")
				}
			}

			switch f {
			case "json":
				emitJSON(g)
			case "dot":
				fmt.Print(dag.DOT(g))
			default:
				printDagSummary(g, pipeline)
			}
		},
	}
	cmd.Flags().StringVar(&format, "format", "", "output format: json or dot (default human summary)")
	cmd.Flags().StringVar(&pipeline, "pipeline", "", "show the subgraph reachable upstream and downstream from this pipeline")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit JSON (same as --format json)")
	return cmd
}

// printDagSummary prints the compact human view: counts, one row per
// pipeline, and orphan repos. A selected subgraph also lists its edges,
// because the reachable set is the point of --pipeline.
func printDagSummary(g dag.Graph, root string) {
	pipelines := make([]dag.Node, 0, len(g.Nodes))
	repos := 0
	for _, n := range g.Nodes {
		switch n.Kind {
		case "pipeline":
			pipelines = append(pipelines, n)
		case "repo":
			repos++
		}
	}
	if root != "" {
		fmt.Printf("subgraph %s: %d pipelines, %d repos, %d edges\n", root, len(pipelines), repos, len(g.Edges))
	} else {
		orphanWord := "orphans"
		if len(g.Orphans) == 1 {
			orphanWord = "orphan"
		}
		fmt.Printf("fabric: %d pipelines, %d repos, %d edges, %d %s\n",
			len(pipelines), repos, len(g.Edges), len(g.Orphans), orphanWord)
	}

	rows := make([][]string, 0, len(pipelines))
	for _, n := range pipelines {
		rows = append(rows, []string{
			n.Name,
			n.State,
			fmt.Sprintf("%d", n.Version),
			n.OutputBranch,
			parallelismCell(n),
			n.Cron,
			lastJobCell(n),
		})
	}
	table([]string{"PIPELINE", "STATE", "VER", "BRANCH", "PAR", "CRON", "LAST JOB"}, rows)
	if len(rows) == 0 {
		fmt.Println("no pipelines")
	}
	if len(g.Orphans) > 0 {
		fmt.Println("orphans: " + strings.Join(g.Orphans, ", "))
	}
	if root != "" {
		for _, e := range g.Edges {
			fmt.Printf("  %s -> %s\n", dagNodeName(e.From), dagNodeName(e.To))
		}
	}
}

func parallelismCell(n dag.Node) string {
	p := n.Parallelism
	switch {
	case p == nil:
		return "—"
	case p.Constant != 0:
		return fmt.Sprintf("%d", p.Constant)
	case p.Coefficient != 0:
		return fmt.Sprintf("%g", p.Coefficient)
	default:
		return "1"
	}
}

func lastJobCell(n dag.Node) string {
	if n.LastJob == nil {
		return "—"
	}
	id := n.LastJob.ID
	if len(id) > 8 {
		id = id[:8]
	}
	cell := n.LastJob.State + " " + id
	if n.LastJob.Time != "" {
		cell += " " + n.LastJob.Time
	}
	return cell
}

// dagNodeName strips the id's kind prefix for the human edge list.
func dagNodeName(id string) string {
	if _, name, ok := strings.Cut(id, ":"); ok {
		return name
	}
	return id
}
