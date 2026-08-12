// Git inputs (SB-104..112): an input that maps an external git repository
// (identified by a clone URL) onto an auto-created local repository. The
// pipeline's declaration is validated at creation (URL form SB-104/159-12,
// duplicate names/URLs SB-106); each push event for the tracked branch
// commits the pushed revision into the mapped repository and triggers the
// pipeline (SB-107/108). The push receiver is the sandbox's interface
// choice (D-16: "push-receiver mechanics are an interface choice"): a
// POST /api/v1/git/push carries the pushed refs AND the revision's
// working tree — the webhook delivers the clone result, so no git server
// or credentials machinery lives in the control plane. A push marked
// uncloneable (private, no credentials) produces no commit and fails the
// bound pipelines with a reason naming the URL (SB-105).
package main

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"sandman/client"
)

// validateGitURL enforces the SB-104/159-12 URL vocabulary: a plain HTTPS
// clone URL — https scheme, a host, a path ending in ".git", and no
// embedded port or userinfo. git://, scp syntax, and port-bearing URLs
// are rejected at creation with the pinned error signals.
func validateGitURL(u string) error {
	if u == "" {
		return fmt.Errorf("clone URL is missing (")
	}
	if !strings.HasSuffix(u, ".git") {
		return fmt.Errorf("clone URL is missing .git suffix")
	}
	if !strings.HasPrefix(u, "https://") {
		return fmt.Errorf("clone URL must use https protocol")
	}
	parsed, err := url.Parse(u)
	if err != nil || parsed.Hostname() == "" || parsed.Path == "" || parsed.User != nil {
		return fmt.Errorf("clone URL must use https protocol")
	}
	if strings.Contains(parsed.Host, ":") {
		return fmt.Errorf("clone URL must not include a port")
	}
	return nil
}

// gitRepoName derives a mapped repository's name from a clone URL: the
// last path segment without the ".git" suffix (SB-107 — "named after the
// external repository").
func gitRepoName(u string) string {
	trimmed := strings.TrimSuffix(u, ".git")
	if i := strings.LastIndexByte(trimmed, '/'); i >= 0 {
		return trimmed[i+1:]
	}
	return trimmed
}

// gitTrackedBranch is the branch a git input watches: the declared branch
// or the external default.
func gitTrackedBranch(g *client.GitInput) string {
	if g.Branch != "" {
		return g.Branch
	}
	return "master"
}

// deriveGitRepos resolves a pipeline spec's git inputs: each side's
// repository is the mapped name (custom or URL-derived), the side gets
// the default glob, the tracked branch, and its environment name — and
// the mapped repository is created, empty, exactly as the record demands
// ("a repository is auto-created ... and it starts with zero commits").
// The stored spec's sides then carry real repos for triggering, pairing,
// and enumeration (mirroring deriveCronRepos).
func (d *daemon) deriveGitRepos(p *client.Pipeline) {
	var walk func(in *client.Input)
	walk = func(in *client.Input) {
		for i := range in.Cross {
			walk(&in.Cross[i])
		}
		for i := range in.Union {
			walk(&in.Union[i])
		}
		for i := range in.Join {
			walk(&in.Join[i])
		}
		for i := range in.Group {
			walk(&in.Group[i])
		}
		if in.Git != nil {
			// the mapped repository is named by the input's own name
			// (SB-109) or derived from the URL when unnamed (SB-107)
			name := in.Name
			if name == "" {
				name = gitRepoName(in.Git.URL)
			}
			in.Name = name
			in.Repo = name
			if in.Glob == "" {
				// the whole revision is one datum: the commit's tree
				// (including its .git metadata) is the input (SB-107)
				in.Glob = "/"
			}
			in.Branch = gitTrackedBranch(in.Git)
			d.store.CreateRepo(name)
		}
	}
	if p.Input != nil {
		walk(p.Input)
	}
}

// gitPushEvent is the push-receiver payload (the sandbox's git-input
// interface): the pushed refs plus the revision's working tree. private
// marks a repository the control plane cannot clone (no credentials).
type gitPushEvent struct {
	URL      string            `json:"url"`
	Branch   string            `json:"branch"`
	Revision string            `json:"revision"`
	Files    map[string]string `json:"files,omitempty"`
	Private  bool              `json:"private,omitempty"`
}

// gitPushH is the push receiver (POST /api/v1/git/push). Delivery never
// errors on the repository's behalf: an uncloneable repository fails the
// bound pipelines asynchronously (SB-105), it does not reject the event.
func (d *daemon) gitPushH(w http.ResponseWriter, r *http.Request) error {
	var ev gitPushEvent
	if err := decodeBody(r, &ev); err != nil {
		return fmt.Errorf("invalid request body")
	}
	d.gitPush(ev)
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// gitPush processes one push event: every pipeline whose git input binds
// this URL and tracks this branch receives the revision — one commit per
// mapped repository (pipelines sharing a mapped repository — SB-111 —
// share the commit; custom names split it, SB-110), each replacing the
// previous revision's tree and carrying the revision identifier in
// .git/HEAD (SB-107). A push to an untracked branch is a complete no-op
// (SB-112); an uncloneable repository produces no commit and fails the
// bound pipelines (SB-105).
func (d *daemon) gitPush(ev gitPushEvent) {
	// bind: pipeline name -> mapped repo + tracked branch
	type binding struct{ pipeline, repo string }
	var bound []binding
	seen := map[string]bool{}
	pipes, err := d.listPipelinesFiltered(nil, "", false)
	if err != nil {
		return
	}
	for _, p := range pipes {
		rec, err := d.loadPipeline(p.Name)
		if err != nil {
			continue
		}
		var walk func(in *client.Input)
		walk = func(in *client.Input) {
			for i := range in.Cross {
				walk(&in.Cross[i])
			}
			for i := range in.Union {
				walk(&in.Union[i])
			}
			for i := range in.Join {
				walk(&in.Join[i])
			}
			for i := range in.Group {
				walk(&in.Group[i])
			}
			if in.Git != nil && in.Git.URL == ev.URL && gitTrackedBranch(in.Git) == ev.Branch {
				repo := in.Repo
				if repo == "" {
					repo = gitRepoName(ev.URL)
				}
				bound = append(bound, binding{pipeline: rec.Pipeline.Name, repo: repo})
				seen[repo] = true
			}
		}
		if rec.Pipeline.Input != nil {
			walk(rec.Pipeline.Input)
		}
	}
	if len(bound) == 0 {
		return
	}
	if ev.Private {
		// the repository cannot be cloned: no commit, and every bound
		// pipeline fails with a reason naming the cause and the URL
		// (SB-105)
		for _, b := range bound {
			d.markPipelineFailed(b.pipeline, fmt.Sprintf("unable to clone private repository (%s)", ev.URL))
		}
		return
	}
	// one commit per mapped repository; the revision's tree replaces the
	// previous revision's content at every path
	repos := make([]string, 0, len(seen))
	for repo := range seen {
		repos = append(repos, repo)
	}
	sort.Strings(repos)
	for _, repo := range repos {
		d.commitRevision(repo, ev.Branch, func(commitID string) bool {
			paths := make([]string, 0, len(ev.Files)+1)
			for p := range ev.Files {
				paths = append(paths, p)
			}
			paths = append(paths, ".git/HEAD")
			sort.Strings(paths)
			for _, p := range paths {
				content := ev.Files[p]
				if p == ".git/HEAD" {
					content = ev.Revision
				}
				if err := d.store.OverwriteFile(commitID, p, []byte(content)); err != nil {
					// a failed write abandons the revision rather than
					// publishing a partial tree
					return false
				}
			}
			return true
		}, nil)
	}
}
