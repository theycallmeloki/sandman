package conformance

// Git inputs: URL-form validation at creation, duplicate
// name/URL rejection, auto-created empty repositories, one commit per
// push event on the tracked branch (tree + .git/HEAD revision marker),
// custom-name and shared-repo fan-out, branch filtering, and the
// clone-failure path. The push receiver is the sandman's interface choice:
// a POST /api/v1/git/push carries the pushed refs and the
// revision's working tree.

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// gitTransform reads the mapped repository's revision identifier (the
// commit's .git/HEAD) into the output — the load-bearing assertion.
func gitTransform(side string) *client.Transform {
	return &client.Transform{
		Image: "alpine:3.21",
		Cmd:   []string{"sh", "-c", fmt.Sprintf("cat ${%s}/.git/HEAD > ${OUT}/rev", side)},
	}
}

// gitPipeline creates a pipeline with one git input; the transform reads
// the input's side name (inputName, or the URL-derived name when empty).
func gitPipeline(t *testing.T, name, inputName string, g *client.GitInput) {
	t.Helper()
	side := inputName
	if side == "" {
		side = gitSideName(g.URL)
	}
	mustPipeline(t, client.Pipeline{Name: name, Transform: gitTransform(side), Input: &client.Input{Name: inputName, Git: g}})
}

// gitSideName derives a mapped repository's name from a clone URL: the
// last path segment minus the ".git" suffix.
func gitSideName(u string) string {
	trimmed := strings.TrimSuffix(u, ".git")
	if i := strings.LastIndexByte(trimmed, '/'); i >= 0 {
		return trimmed[i+1:]
	}
	return trimmed
}

// gitURL builds a fresh valid clone URL whose derived repository name is
// unique to the test — the URL-derived name is a shared namespace, so
// tests must not collide on it.
func gitURL(t *testing.T) string {
	return "https://github.com/example/" + uniq(t) + ".git"
}

// headContentOf reads a commit's path.
func headContentOf(t *testing.T, commitID, p string) string {
	t.Helper()
	b, err := c.GetFile(commitID, p)
	if err != nil {
		t.Fatalf("read %s at %s: %v", p, commitID, err)
	}
	return string(b)
}

func TestGitURLValidation(t *testing.T) {
	// each unsupported URL form is rejected at creation, and no pipeline
	// is created
	cases := []struct {
		url, want string
	}{
		{"git://host/path.git", "clone URL must use https protocol"},
		{"git@host:path.git", "clone URL must use https protocol"},
		{"https://host:1234/path.git", "clone URL must not include a port"},
		{"https://host/path", "clone URL is missing .git suffix"},
		{"", "clone URL is missing ("},
	}
	for _, tc := range cases {
		err := c.CreatePipeline(client.Pipeline{Name: uniq(t), Transform: gitTransform("x"), Input: &client.Input{Git: &client.GitInput{URL: tc.url}}})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("URL %q: error = %v, want it to contain %q", tc.url, err, tc.want)
		}
	}
	// the supported plain https form is accepted
	ok := uniq(t)
	gitPipeline(t, ok, "", &client.GitInput{URL: gitURL(t)})
	if _, err := c.InspectPipeline(ok); err != nil {
		t.Fatalf("created pipeline not inspectable: %v", err)
	}
}

func TestGitCloneFailure(t *testing.T) {
	name := uniq(t)
	url := gitURL(t)
	gitPipeline(t, name, "", &client.GitInput{URL: url})
	// the mapped repository is auto-created, empty
	repo := gitSideName(url)
	if n := commitCount(t, repo, "master"); n != 0 {
		t.Fatalf("auto-created repo has %d commits, want 0", n)
	}
	// a push for an uncloneable (private) repository: the event is
	// accepted, produces no commit, and the pipeline fails with a reason
	// naming the cause and the URL
	if err := c.PushGitEvent(url, "master", revHex("a"), nil, true); err != nil {
		t.Fatalf("push delivery errored: %v", err)
	}
	pollFor(t, "pipeline failed with clone reason", 30*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State == "failure" &&
			strings.Contains(info.Reason, "clone") && strings.Contains(info.Reason, url)
	})
	if n := commitCount(t, repo, "master"); n != 0 {
		t.Fatalf("repo has %d commits after the uncloneable push, want 0", n)
	}
	// recovery: a subsequent normal push to the same URL is the
	// repository-becoming-cloneable signal — the failure clears and the
	// push triggers a job (the review finding: a failed pipeline was
	// permanently silenced — commits kept landing with zero processing)
	if err := c.PushGitEvent(url, "master", revHex("b"), map[string]string{"README.md": "v2"}, false); err != nil {
		t.Fatalf("recovery push errored: %v", err)
	}
	pollFor(t, "pipeline recovered and job succeeded", 60*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		if err != nil || info.State != "running" || info.Reason != "" {
			return false
		}
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
		if err != nil || len(js) == 0 || js[0].State != "success" {
			return false
		}
		return true
	})
}

func TestGitDuplicateNamesAndURLs(t *testing.T) {
	url := gitURL(t)
	// (a) two git inputs sharing a custom name are rejected
	err := c.CreatePipeline(client.Pipeline{
		Name:      uniq(t),
		Transform: gitTransform("foo"),
		Input: &client.Input{Cross: []client.Input{
			{Name: "foo", Git: &client.GitInput{URL: url}},
			{Name: "foo", Git: &client.GitInput{URL: url}},
		}},
	})
	if err == nil {
		t.Fatal("duplicate git names accepted, want rejection")
	}
	// (b) two git inputs with the same URL and no custom names collide on
	// the derived name
	err = c.CreatePipeline(client.Pipeline{
		Name:      uniq(t),
		Transform: gitTransform("x"),
		Input: &client.Input{Cross: []client.Input{
			{Git: &client.GitInput{URL: url}},
			{Git: &client.GitInput{URL: url}},
		}},
	})
	if err == nil {
		t.Fatal("duplicate git URLs accepted, want rejection")
	}
	// (c) the same URL under distinct custom names is accepted
	ok := uniq(t)
	if err := c.CreatePipeline(client.Pipeline{
		Name:      ok,
		Transform: gitTransform("one"),
		Input: &client.Input{Cross: []client.Input{
			{Name: "one", Git: &client.GitInput{URL: url}},
			{Name: "two", Git: &client.GitInput{URL: url}},
		}},
	}); err != nil {
		t.Fatalf("same URL under distinct names rejected: %v", err)
	}
}

func TestGitPushCommitsAndTriggers(t *testing.T) {
	name := uniq(t)
	url := gitURL(t)
	gitPipeline(t, name, "", &client.GitInput{URL: url})
	repo := gitSideName(url)
	if n := commitCount(t, repo, "master"); n != 0 {
		t.Fatalf("repo starts with %d commits, want 0", n)
	}
	rev := revHex("r1")
	if err := c.PushGitEvent(url, "master", rev, map[string]string{"README": "hi"}, false); err != nil {
		t.Fatalf("push: %v", err)
	}
	// exactly one branch (master) with one commit whose tree carries the
	// revision identifier in .git/HEAD
	head, err := c.HeadCommit(repo, "master")
	if err != nil {
		t.Fatalf("mapped branch head: %v", err)
	}
	if got := headContentOf(t, head.ID, ".git/HEAD"); got != rev {
		t.Fatalf(".git/HEAD = %q, want the pushed revision %q", got, rev)
	}
	if got := headContentOf(t, head.ID, "README"); got != "hi" {
		t.Fatalf("README = %q, want the pushed tree content", got)
	}
	ri, err := c.InspectRepo(repo)
	if err != nil {
		t.Fatalf("inspect repo: %v", err)
	}
	if len(ri.Branches) != 1 || ri.Branches[0] != "master" {
		t.Fatalf("branches = %v, want exactly [master]", ri.Branches)
	}
	// the commit triggers the pipeline; the output carries the revision
	jobs := flushOK(t, head.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush = %d jobs, want 1", len(jobs))
	}
	if got := headContentOf(t, jobs[0].OutputCommit, "rev"); got != rev {
		t.Fatalf("output = %q, want the full pushed revision %q", got, rev)
	}
}

func TestSequentialPushes(t *testing.T) {
	name := uniq(t)
	url := gitURL(t)
	gitPipeline(t, name, "", &client.GitInput{URL: url})
	repo := gitSideName(url)
	r1, r2 := revHex("r1"), revHex("r2")
	if err := c.PushGitEvent(url, "master", r1, map[string]string{"f": "1"}, false); err != nil {
		t.Fatalf("push 1: %v", err)
	}
	head1, err := c.HeadCommit(repo, "master")
	if err != nil {
		t.Fatalf("head 1: %v", err)
	}
	jobs := flushOK(t, head1.ID)
	if got := headContentOf(t, jobs[0].OutputCommit, "rev"); got != r1 {
		t.Fatalf("output after push 1 = %q, want %q", got, r1)
	}
	// a second push advances the same branch: one new commit, still one
	// branch, and the latest output reflects the newest revision only
	if err := c.PushGitEvent(url, "master", r2, map[string]string{"f": "2"}, false); err != nil {
		t.Fatalf("push 2: %v", err)
	}
	pollFor(t, "second push commits", 30*time.Second, func() bool {
		ch, err := c.CommitHistory(repo, "master")
		return err == nil && len(ch) == 2
	})
	head2, err := c.HeadCommit(repo, "master")
	if err != nil {
		t.Fatalf("head 2: %v", err)
	}
	jobs = flushOK(t, head2.ID)
	if got := headContentOf(t, jobs[0].OutputCommit, "rev"); got != r2 {
		t.Fatalf("output after push 2 = %q, want the newest revision %q", got, r2)
	}
	ri, _ := c.InspectRepo(repo)
	if len(ri.Branches) != 1 {
		t.Fatalf("branches after two pushes = %v, want exactly [master]", ri.Branches)
	}
}

func TestGitCustomName(t *testing.T) {
	name := uniq(t)
	url := gitURL(t)
	derived := gitSideName(url)
	custom := uniq(t) + "c" // a custom name unique to this test (repo names are a shared namespace)
	gitPipeline(t, name, custom, &client.GitInput{URL: url})
	// the repository exists under the custom name, empty; the URL-derived
	// name is not created
	if n := commitCount(t, custom, "master"); n != 0 {
		t.Fatalf("custom repo has %d commits, want 0", n)
	}
	if _, err := c.HeadCommit(derived, "master"); err == nil {
		t.Fatalf("URL-derived repo exists under a custom-name input, want only the custom one")
	}
	rev := revHex("r1")
	if err := c.PushGitEvent(url, "master", rev, map[string]string{"f": "1"}, false); err != nil {
		t.Fatalf("push: %v", err)
	}
	head, err := c.HeadCommit(custom, "master")
	if err != nil {
		t.Fatalf("custom repo head: %v", err)
	}
	jobs := flushOK(t, head.ID)
	if got := headContentOf(t, jobs[0].OutputCommit, "rev"); got != rev {
		t.Fatalf("output = %q, want the revision %q", got, rev)
	}
}

func TestFanOutDistinctNames(t *testing.T) {
	// two pipelines, same URL, distinct custom names: each gets its own
	// repository, and one push triggers both
	n1, n2 := uniq(t)+"a", uniq(t)+"b"
	url := gitURL(t)
	// two distinct custom names for one URL: each pipeline gets its own
	// repository
	cn1, cn2 := uniq(t)+"x", uniq(t)+"y"
	gitPipeline(t, n1, cn1, &client.GitInput{URL: url})
	gitPipeline(t, n2, cn2, &client.GitInput{URL: url})
	rev := revHex("r1")
	if err := c.PushGitEvent(url, "master", rev, map[string]string{"f": "1"}, false); err != nil {
		t.Fatalf("push: %v", err)
	}
	for _, repo := range []string{cn1, cn2} {
		head, err := c.HeadCommit(repo, "master")
		if err != nil {
			t.Fatalf("%s head: %v", repo, err)
		}
		jobs := flushOK(t, head.ID)
		if len(jobs) != 1 {
			t.Fatalf("%s flush = %d jobs, want 1", repo, len(jobs))
		}
		if got := headContentOf(t, jobs[0].OutputCommit, "rev"); got != rev {
			t.Fatalf("%s output = %q, want %q", repo, got, rev)
		}
	}
}

func TestFanOutSharedRepo(t *testing.T) {
	// two pipelines, same URL, no custom names: they share the single
	// URL-derived repository, and one push creates one commit that
	// triggers both
	n1, n2 := uniq(t)+"a", uniq(t)+"b"
	url := gitURL(t)
	gitPipeline(t, n1, "", &client.GitInput{URL: url})
	gitPipeline(t, n2, "", &client.GitInput{URL: url})
	repo := gitSideName(url)
	rev := revHex("r1")
	if err := c.PushGitEvent(url, "master", rev, map[string]string{"f": "1"}, false); err != nil {
		t.Fatalf("push: %v", err)
	}
	head, err := c.HeadCommit(repo, "master")
	if err != nil {
		t.Fatalf("shared head: %v", err)
	}
	if ch, _ := c.CommitHistory(repo, "master"); len(ch) != 1 {
		t.Fatalf("shared repo has %d commits, want exactly 1", len(ch))
	}
	// flushing the single commit yields one output commit per pipeline
	jobs := flushOK(t, head.ID)
	if len(jobs) != 2 {
		t.Fatalf("flush = %d jobs, want 2 (one per consuming pipeline)", len(jobs))
	}
	for _, j := range jobs {
		if got := headContentOf(t, j.OutputCommit, "rev"); got != rev {
			t.Fatalf("output of %s = %q, want %q", j.Pipeline, got, rev)
		}
	}
}

func TestGitBranchFilter(t *testing.T) {
	name := uniq(t)
	url := gitURL(t)
	gitPipeline(t, name, "", &client.GitInput{URL: url, Branch: "foo"})
	repo := gitSideName(url)
	// a push to an untracked branch is a complete no-op: no branch, no
	// commit, no job
	if err := c.PushGitEvent(url, "master", revHex("m"), map[string]string{"f": "1"}, false); err != nil {
		t.Fatalf("push master: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if _, err := c.HeadCommit(repo, "master"); err == nil {
		t.Fatalf("untracked push created a master branch")
	}
	if n := commitCount(t, repo, "foo"); n != 0 {
		t.Fatalf("untracked push created %d commits, want 0", n)
	}
	if js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: name}); len(js) != 0 {
		t.Fatalf("untracked push triggered %d jobs, want 0", len(js))
	}
	// a push to the declared branch works, and the mapped repository's
	// only branch is the declared one
	rev := revHex("foo-rev")
	if err := c.PushGitEvent(url, "foo", rev, map[string]string{"f": "2"}, false); err != nil {
		t.Fatalf("push foo: %v", err)
	}
	head, err := c.HeadCommit(repo, "foo")
	if err != nil {
		t.Fatalf("foo branch head: %v", err)
	}
	ri, _ := c.InspectRepo(repo)
	if len(ri.Branches) != 1 || ri.Branches[0] != "foo" {
		t.Fatalf("branches = %v, want exactly [foo]", ri.Branches)
	}
	jobs := flushOK(t, head.ID)
	if got := headContentOf(t, jobs[0].OutputCommit, "rev"); got != rev {
		t.Fatalf("output = %q, want the foo revision %q", got, rev)
	}
}

// revHex returns a 40-hex revision identifier (a full git SHA shape),
// distinct per seed.
func revHex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:40]
}
