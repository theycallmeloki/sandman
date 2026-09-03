// deltaCmd — `sandman delta`: submit a git delta to a git-input mapped
// repo and print the receiver's delivery report. This is the raw wire
// contract for programmatic callers (e.g. the symphony RepoDelta client)
// that already hold the complete edit as a files map — as opposed to
// `patch`, which derives a delta from a git checkout's worktree.
//
// The payload is one JSON object on stdin ("-") or in a file, matching
// the git-input delta receiver: {url, branch, revision, base, files,
// deleted, private}. Output is the GitDeltaResult JSON {applied, reason,
// head}. applied=false means the edit bound no pipeline or failed the
// base check (reason names the refusal); the caller decides whether that
// is an error — the verb exits 0 because the delivery report is data.
package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

// gitDeltaPayload is the wire body `sandman delta` forwards to the
// control plane's git-input delta receiver.
type gitDeltaPayload struct {
	URL      string            `json:"url"`
	Branch   string            `json:"branch"`
	Revision string            `json:"revision"`
	Base     string            `json:"base"`
	Files    map[string]string `json:"files"`
	Deleted  []string          `json:"deleted"`
	Private  bool              `json:"private"`
}

func deltaCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delta [payload.json|-]",
		Short: "submit a git delta and print its delivery report",
		Long: `Submit one edit to a git-input mapped repository and print the
receiver's report as JSON ({applied, reason, head}). The payload is the
delta wire contract — {"url","branch","revision","base","files",
"deleted","private"} — read from the named file, or from stdin when the
argument is "-" (the shape programmatic callers like RepoDelta already
build). Unlike "patch", this does not diff a checkout: it forwards the
exact files map you supply. Exit is 0 either way: applied=false is
reported in JSON (reason names the refusal), not as a process failure.
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			src := "-"
			if len(args) == 1 {
				src = args[0]
			}
			var raw []byte
			var err error
			if src == "-" {
				raw, err = io.ReadAll(os.Stdin)
				if err != nil {
					die("delta: reading stdin: "+err.Error(), 1)
				}
			} else {
				raw, err = os.ReadFile(src)
				if err != nil {
					die("delta: reading "+src+": "+err.Error(), 1)
				}
			}
			var p gitDeltaPayload
			if err := json.Unmarshal(raw, &p); err != nil {
				die(fmt.Sprintf("delta: invalid payload %s: %v", src, err), 1)
			}
			if p.URL == "" {
				die("delta: payload needs a non-empty \"url\"", 1)
			}
			if p.Files == nil {
				p.Files = map[string]string{}
			}
			res, err := cliClient().PushGitDeltaReport(
				p.URL, p.Branch, p.Revision, p.Base, p.Files, p.Deleted, p.Private)
			if err != nil {
				dieErr("delta", err, "")
			}
			emitJSON(res)
		},
	}
}
