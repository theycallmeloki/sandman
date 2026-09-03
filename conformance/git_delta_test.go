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
	// the recovery commit re-triggers the pipeline: the failure was
	// cleared BEFORE the commit landed, so triggerForCommit sees a
	// running pipeline and spawns a job for the commit that resolved the
	// failure (a clear-after-commit ordering leaves it untriggered — the
	// fix for the recovery-delta commit that built never)
	pollFor(t, "recovery delta re-triggers the pipeline", 30*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
		return err == nil && len(js) >= 2
	})
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
	// the blind recovery commit re-triggers the pipeline too (the
	// failure cleared before the commit, so the bootstrap spawns a job)
	pollFor(t, "blind recovery delta re-triggers the pipeline", 30*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
		return err == nil && len(js) >= 1
	})
}

// TestGitDeltaMarkerlessHeadRecordsItself — a head seeded through the
// per-file commit API (StartCommit/PutFile/FinishCommit) carries no
// .git/HEAD marker; the recorded revision is the head commit id itself,
// which is exactly the base a bootstrap of that head falls back to. A
// delta with base == head id applies (and clears any earlier base
// failure); a delta with any other base is still refused.
func TestGitDeltaMarkerlessHeadRecordsItself(t *testing.T) {
	name := uniq(t)
	url := gitURL(t)
	gitPipeline(t, name, "", &client.GitInput{URL: url})
	repo := gitSideName(url)

	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start seed commit: %v", err)
	}
	if err := c.PutFile(cm.ID, "f", []byte("1")); err != nil {
		t.Fatalf("put seed file: %v", err)
	}
	if _, err := c.FinishCommit(cm.ID, "", false); err != nil {
		t.Fatalf("finish seed commit: %v", err)
	}
	head := mustHead(t, repo)
	if n := commitCount(t, repo, "master"); n != 1 {
		t.Fatalf("repo has %d commits after seed, want 1", n)
	}

	// a base that is not the head id is refused even without a marker
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

	// base == head id matches the markerless head's recorded revision
	r := revHex("edit2")
	if err := c.PushGitDelta(url, "master", r, head,
		map[string]string{"f": "2"}, nil, false); err != nil {
		t.Fatalf("head-base delta: %v", err)
	}
	pollFor(t, "head-id-base delta commits and clears failure", 30*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State != "failure" && info.Reason == ""
	})
	if n := commitCount(t, repo, "master"); n != 2 {
		t.Fatalf("repo has %d commits after head-base delta, want 2", n)
	}
	got := mustHead(t, repo)
	if hc := headContentOf(t, got, "f"); hc != "2" {
		t.Fatalf("delta did not apply: f = %q, want 2", hc)
	}
	if hc := headContentOf(t, got, ".git/HEAD"); hc != r {
		t.Fatalf(".git/HEAD after delta = %q, want %q", hc, r)
	}
}

// TestGitDeltaSpellingDriftStillBinds — a git input wired with one URL
// spelling (a hyphenated repo key) and a mapped repo named by the env-safe
// mirror spelling still receives deltas addressed to the mirror URL: the
// binding falls back to the mapped repository name, so a harness workspace
// (which records the mirror URL it bootstrapped from) is never silently
// dropped because the stored input URL drifted from the emitter's.
func TestGitDeltaSpellingDriftStillBinds(t *testing.T) {
	name := uniq(t)
	// the wiring-time URL carries the raw upstream name; the mirror and
	// the emitters use the env-safe spelling (bus-wire rewires names
	// hyphen -> underscore; the two URLs name the same repository)
	stem := uniq(t)
	envSafe := strings.ReplaceAll(stem, "-", "_")
	wiredURL := "https://github.com/example/" + stem + ".git"
	mirrorURL := "https://github.com/example/" + envSafe + ".git"
	gitPipeline(t, name, envSafe, &client.GitInput{URL: wiredURL})
	repo := envSafe

	// seed the mapped repo through the per-file commit API (no push)
	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start seed: %v", err)
	}
	if err := c.PutFile(cm.ID, "f", []byte("1")); err != nil {
		t.Fatalf("put seed file: %v", err)
	}
	if _, err := c.FinishCommit(cm.ID, "", false); err != nil {
		t.Fatalf("finish seed: %v", err)
	}
	head := mustHead(t, repo)

	// a delta addressed to the MIRROR url (what every workspace emits):
	// the stored input URL differs, but the mapped repo is the repo the
	// event URL names, so the edit binds, commits, and triggers
	r := revHex("edit")
	if err := c.PushGitDelta(mirrorURL, "master", r, head,
		map[string]string{"f": "2"}, nil, false); err != nil {
		t.Fatalf("mirror-url delta: %v", err)
	}
	if got := mustHead(t, repo); got == head {
		t.Fatalf("delta produced no commit (head still %s)", got)
	}
	if got := headContentOf(t, mustHead(t, repo), "f"); got != "2" {
		t.Fatalf("delta did not apply: f = %q, want 2", got)
	}
	if got := headContentOf(t, mustHead(t, repo), ".git/HEAD"); got != r {
		t.Fatalf(".git/HEAD = %q, want the delta revision %q", got, r)
	}
	pollFor(t, "spelling-drift delta triggers the pipeline", 30*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
		return err == nil && len(js) >= 1
	})
}

// TestGitDeltaUnboundURLReportsNotApplied — a delta addressed to a URL no
// pipeline binds is no longer a silent 200 no-op: the response reports
// applied=false with a reason naming the URL, so the emitter learns the
// edit never landed (this is the harness deploy that vanished: the watch
// keyed on a different URL spelling and the workspace delta dropped).
func TestGitDeltaUnboundURLReportsNotApplied(t *testing.T) {
	name := uniq(t)
	url := gitURL(t)
	gitPipeline(t, name, "", &client.GitInput{URL: url})
	repo := gitSideName(url)
	if err := c.PushGitEvent(url, "master", revHex("base"),
		map[string]string{"f": "1"}, false); err != nil {
		t.Fatalf("push base: %v", err)
	}
	_ = repo

	// a delta to a URL nothing watches: applied=false, reason names it,
	// and no pipeline is failed (nothing was bound to fail)
	ghost := "https://github.com/example/" + uniq(t) + ".git"
	res, err := c.PushGitDeltaReport(ghost, "master", revHex("edit"), "",
		map[string]string{"f": "2"}, nil, false)
	if err != nil {
		t.Fatalf("unbound delta: %v", err)
	}
	if res.Applied {
		t.Fatalf("unbound delta reported applied=true")
	}
	if !strings.Contains(res.Reason, "no pipeline is bound") || !strings.Contains(res.Reason, ghost) {
		t.Fatalf("reason = %q, want it to name the unbound url", res.Reason)
	}
	info, err := c.InspectPipeline(name)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if info.State == "failure" {
		t.Fatalf("unbound delta failed a pipeline that never bound it: %q", info.Reason)
	}
}

// TestGitDeltaAuthoredHeadResetsMarker — a commit authored over a pushed
// head (per-file commit API, no explicit revision) records its OWN id as
// the head's revision, not the inherited push marker. A delta based on
// the pre-authoring revision — a workspace bootstrapped before the
// authoring — is then refused instead of silently overwriting the
// authored work; a delta based on the authored head applies.
func TestGitDeltaAuthoredHeadResetsMarker(t *testing.T) {
	name := uniq(t)
	url := gitURL(t)
	gitPipeline(t, name, "", &client.GitInput{URL: url})
	repo := gitSideName(url)

	r1 := revHex("base")
	if err := c.PushGitEvent(url, "master", r1, map[string]string{"f": "1"}, false); err != nil {
		t.Fatalf("push base: %v", err)
	}
	pushed := mustHead(t, repo)

	// author a seed commit on top of the pushed head (no .git/HEAD op)
	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start seed: %v", err)
	}
	if err := c.PutFile(cm.ID, "g", []byte("authored")); err != nil {
		t.Fatalf("put seed file: %v", err)
	}
	seed, err := c.FinishCommit(cm.ID, "", false)
	if err != nil {
		t.Fatalf("finish seed: %v", err)
	}
	if seed.ID == pushed {
		t.Fatalf("seed did not advance the head")
	}
	// the authored head records ITSELF, not the inherited push revision
	if got := headContentOf(t, mustHead(t, repo), ".git/HEAD"); got != seed.ID {
		t.Fatalf(".git/HEAD after authored seed = %q, want the seed head id %q (inherited marker %q must not survive the authoring)", got, seed.ID, r1)
	}

	// a delta based on the pre-authoring push revision is stale: without
	// the reset the inherited marker made that base look current and the
	// edit would have overwritten the authored work
	if err := c.PushGitDelta(url, "master", revHex("edit"), r1,
		map[string]string{"f": "2"}, nil, false); err != nil {
		t.Fatalf("stale-base delta: %v", err)
	}
	pollFor(t, "pipeline failed with delta base reason", 30*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State == "failure" &&
			strings.Contains(info.Reason, "delta base") && strings.Contains(info.Reason, r1)
	})
	if got := headContentOf(t, mustHead(t, repo), "g"); got != "authored" {
		t.Fatalf("authored file lost to a stale delta: g = %q", got)
	}

	// a delta based on the authored head applies
	r3 := revHex("edit2")
	if err := c.PushGitDelta(url, "master", r3, seed.ID,
		map[string]string{"f": "2"}, nil, false); err != nil {
		t.Fatalf("authored-head-base delta: %v", err)
	}
	pollFor(t, "authored-head-base delta clears the failure", 30*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State != "failure"
	})
	if got := headContentOf(t, mustHead(t, repo), "f"); got != "2" {
		t.Fatalf("delta did not apply: f = %q, want 2", got)
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
