// Delta receiver: an edit delivered to a git-input mapped repository
// instead of a full working-tree replacement. The delta applies onto the
// repository's existing tree — files added/changed, paths deleted,
// unchanged paths untouched — as one new commit on the tracked branch
// that re-triggers the pipeline exactly like a push. The base guard: a
// delta made against a stale base (its base does not equal the revision
// recorded at the mapped head) produces no commit and fails the bound
// pipelines with a reason naming both revisions; a later delta with the
// matching base is the recovery signal. Delta onto a head-less
// repository bootstraps a partial revision when no base is set and fails
// like a stale base when one is.
package conformance

import (
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// TestGitDeltaAppliesEditAndTriggers — a delta after a full push edits
// one file, deletes another, and adds a third; the new head carries the
// edited tree and the new revision marker, the prior commit keeps its own
// tree, and the pipeline is re-triggered for the delta commit.
func TestGitDeltaAppliesEditAndTriggers(t *testing.T) {
	name := uniq(t)
	url := gitURL(t)
	gitPipeline(t, name, "", &client.GitInput{URL: url})
	repo := gitSideName(url)

	r1 := revHex("base")
	if err := c.PushGitEvent(url, "master", r1,
		map[string]string{"README.md": "v1\n", "app/main.go": "package main\n"}, false); err != nil {
		t.Fatalf("push base: %v", err)
	}
	head1, err := c.HeadCommit(repo, "master")
	if err != nil {
		t.Fatalf("head after push: %v", err)
	}

	r2 := revHex("edit")
	if err := c.PushGitDelta(url, "master", r2, r1,
		map[string]string{"app/main.go": "package main\n\n// v2\n", "NEW.md": "added\n"},
		[]string{"README.md"}, false); err != nil {
		t.Fatalf("push delta: %v", err)
	}

	head2, err := c.HeadCommit(repo, "master")
	if err != nil {
		t.Fatalf("head after delta: %v", err)
	}
	if head2.ID == head1.ID {
		t.Fatalf("delta produced no new commit (head still %s)", head2.ID)
	}
	if got := headContentOf(t, head2.ID, ".git/HEAD"); got != r2 {
		t.Fatalf(".git/HEAD after delta = %q, want the delta revision %q", got, r2)
	}
	if got := headContentOf(t, head2.ID, "app/main.go"); !strings.Contains(got, "v2") {
		t.Fatalf("edited file at head = %q, want the edited content", got)
	}
	if got := headContentOf(t, head2.ID, "NEW.md"); got != "added\n" {
		t.Fatalf("added file at head = %q, want the added content", got)
	}
	if _, err := c.GetFile(head2.ID, "README.md"); err == nil {
		t.Fatalf("deleted path still readable at the delta head")
	}
	// unchanged paths resolve through the parent: the delta commit carried
	// no other paths, but the head view must still hold the pushed tree's
	// untouched files — here none remain, so assert the prior commit kept
	// its own content untouched
	if got := headContentOf(t, head1.ID, "README.md"); got != "v1\n" {
		t.Fatalf("base commit README = %q, want the pushed v1 (immutability)", got)
	}

	// the delta commit re-triggers the pipeline exactly like a push: one
	// job per commit on the tracked branch
	pollFor(t, "delta re-triggers the pipeline", 30*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
		return err == nil && len(js) >= 2
	})
}

// TestGitDeltaStaleBaseFailsThenRecovers — a delta whose base does not
// match the revision recorded at the mapped head is refused: no commit,
// and the bound pipeline fails with a reason naming the mismatch. A later
// delta with the current base commits, clears the failure, and triggers.
func TestGitDeltaStaleBaseFailsThenRecovers(t *testing.T) {
	name := uniq(t)
	url := gitURL(t)
	gitPipeline(t, name, "", &client.GitInput{URL: url})
	repo := gitSideName(url)

	r1 := revHex("base")
	if err := c.PushGitEvent(url, "master", r1, map[string]string{"f": "1"}, false); err != nil {
		t.Fatalf("push base: %v", err)
	}
	if n := commitCount(t, repo, "master"); n != 1 {
		t.Fatalf("repo has %d commits after the base push, want 1", n)
	}

	// stale base: the edit claims to be against a revision the head no
	// longer records
	stale := revHex("stale")
	if err := c.PushGitDelta(url, "master", revHex("edit"), stale,
		map[string]string{"f": "2"}, nil, false); err != nil {
		t.Fatalf("stale-base delta: %v", err)
	}
	pollFor(t, "pipeline failed with delta base reason", 30*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State == "failure" &&
			strings.Contains(info.Reason, "delta base") && strings.Contains(info.Reason, stale)
	})
	if n := commitCount(t, repo, "master"); n != 1 {
		t.Fatalf("stale-base delta committed: %d commits, want 1", n)
	}
	if got := headContentOf(t, mustHead(t, repo), "f"); got != "1" {
		t.Fatalf("tree changed by a refused delta: f = %q, want the base content", got)
	}

	// recovery: a delta against the current base commits and clears the
	// failure (the stale-base failure was per-event, not structural)
	r2 := revHex("edit2")
	if err := c.PushGitDelta(url, "master", r2, r1, map[string]string{"f": "2"}, nil, false); err != nil {
		t.Fatalf("recovery delta: %v", err)
	}
	pollFor(t, "matching-base delta clears the failure", 30*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State != "failure" && info.Reason == ""
	})
	if n := commitCount(t, repo, "master"); n != 2 {
		t.Fatalf("repo has %d commits after recovery delta, want 2", n)
	}
	if got := headContentOf(t, mustHead(t, repo), ".git/HEAD"); got != r2 {
		t.Fatalf(".git/HEAD after recovery = %q, want %q", got, r2)
	}
	if got := headContentOf(t, mustHead(t, repo), "f"); got != "2" {
		t.Fatalf("recovery delta did not apply: f = %q, want 2", got)
	}
}

// TestGitDeltaPrivateFailsNoCommit — a delta marked private is a repo the
// control plane cannot access: no commit, bound pipelines fail with the
// clone reason, mirroring the full-push private path.
func TestGitDeltaPrivateFailsNoCommit(t *testing.T) {
	name := uniq(t)
	url := gitURL(t)
	gitPipeline(t, name, "", &client.GitInput{URL: url})
	repo := gitSideName(url)

	if err := c.PushGitDelta(url, "master", revHex("edit"), "",
		map[string]string{"f": "1"}, nil, true); err != nil {
		t.Fatalf("private delta: %v", err)
	}
	pollFor(t, "pipeline failed with clone reason", 30*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State == "failure" &&
			strings.Contains(info.Reason, "clone") && strings.Contains(info.Reason, url)
	})
	if n := commitCount(t, repo, "master"); n != 0 {
		t.Fatalf("private delta committed: %d commits, want 0", n)
	}
}

// TestGitDeltaOntoEmptyRepo — no head yet: a blind delta bootstraps a
// partial revision onto the empty tree; a delta with a base has nothing
// to match and fails like any stale base.
func TestGitDeltaOntoEmptyRepo(t *testing.T) {
	name := uniq(t)
	url := gitURL(t)
	gitPipeline(t, name, "", &client.GitInput{URL: url})
	repo := gitSideName(url)

	// base on a repo with no head: nothing to match
	if err := c.PushGitDelta(url, "master", revHex("e1"), revHex("somewhere"),
		map[string]string{"f": "1"}, nil, false); err != nil {
		t.Fatalf("base delta onto empty repo: %v", err)
	}
	pollFor(t, "pipeline failed with delta base reason", 30*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State == "failure" && strings.Contains(info.Reason, "delta base")
	})
	if n := commitCount(t, repo, "master"); n != 0 {
		t.Fatalf("base delta onto empty repo committed: %d commits, want 0", n)
	}

	// blind delta: bootstraps the partial revision
	r := revHex("boot")
	if err := c.PushGitDelta(url, "master", r, "",
		map[string]string{"a": "1"}, []string{"missing"}, false); err != nil {
		t.Fatalf("blind delta onto empty repo: %v", err)
	}
	pollFor(t, "blind delta commits and recovers", 30*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State != "failure"
	})
	head := mustHead(t, repo)
	if got := headContentOf(t, head, ".git/HEAD"); got != r {
		t.Fatalf(".git/HEAD after bootstrap = %q, want %q", got, r)
	}
	if got := headContentOf(t, head, "a"); got != "1" {
		t.Fatalf("bootstrapped file = %q, want 1", got)
	}
}

// mustHead returns the current head commit id of a repo, failing the test.
func mustHead(t *testing.T, repo string) string {
	t.Helper()
	cm, err := c.HeadCommit(repo, "master")
	if err != nil {
		t.Fatalf("head of %s: %v", repo, err)
	}
	return cm.ID
}
