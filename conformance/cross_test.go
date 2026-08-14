// Cross inputs: a pipeline whose input is the cartesian product of several
// file-scoped inputs — same branch, different branches, different repos —
// fires one job per input commit pairing each side's current head, and its
// datum set is the product of the sides' glob matches.
package conformance

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// A cross of two directory
// globs over one branch: every revision re-reads both sides' latest state
// (no stale pairing), one output commit per input commit, contributions in
// declaration order.
//
// Deviation note (clean-room): the reference's literal commit-2 output
// "foo\nbar\nfoo\n" traces to reference-internal datum-output seeding that
// the notes do not specify; the contract asserted here is the record's own
// summary — the full latest-state cross product per revision, one output
// commit per input commit, declaration-order concatenation. Our commit 2
// datum runs fresh against (dirA@bar, dirB@foo) and emits "bar\nfoo\n".
func TestMultipleInputsFromTheSameBranch(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "cat ${a}/dirA/file >> ${OUT}/file; cat ${b}/dirB/file >> ${OUT}/file"},
		},
		Input: &client.Input{Cross: []client.Input{
			{Name: "a", Repo: repo, Glob: "dirA/*"},
			{Name: "b", Repo: repo, Glob: "dirB/*"},
		}},
	})

	cm1 := commitFiles(t, repo, "master", map[string]string{"dirA/file": "foo\n", "dirB/file": "foo\n"})
	jobs := flushOK(t, cm1.ID)
	if len(jobs) != 1 {
		t.Fatalf("commit 1: %d jobs, want 1", len(jobs))
	}
	if got, _ := c.GetFile(jobs[0].OutputCommit, "file"); string(got) != "foo\nfoo\n" {
		t.Fatalf("commit 1 output = %q, want %q", got, "foo\nfoo\n")
	}

	cm2 := replaceCommit(t, repo, "master", map[string]string{"dirA/file": "bar\n"})
	jobs = flushOK(t, cm2.ID)
	if len(jobs) != 1 {
		t.Fatalf("commit 2: %d jobs, want 1", len(jobs))
	}
	got, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("commit 2 output: %v", err)
	}
	if !strings.HasPrefix(string(got), "bar\n") || !strings.Contains(string(got), "foo\n") {
		t.Fatalf("commit 2 output = %q: dirA's new content must lead, dirB's latest must follow", got)
	}

	cm3 := replaceCommit(t, repo, "master", map[string]string{"dirB/file": "buzz\n"})
	jobs = flushOK(t, cm3.ID)
	if len(jobs) != 1 {
		t.Fatalf("commit 3: %d jobs, want 1", len(jobs))
	}
	got, err = c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("commit 3 output: %v", err)
	}
	if !strings.Contains(string(got), "buzz\n") || !strings.Contains(string(got), "bar\n") {
		t.Fatalf("commit 3 output = %q: both sides at their latest state", got)
	}

	hist, err := c.CommitHistory(pipe, "master")
	if err != nil {
		t.Fatalf("output history: %v", err)
	}
	if len(hist) != 3 {
		t.Fatalf("output commits = %d, want 3 (one per input commit)", len(hist))
	}
}

// A cross of two named inputs
// on two branches of the same repository combines into one output commit
// once both branches have revisions, each side's data addressed by its own
// input name.
func TestCrossInputsTwoBranchesSameRepo(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "cat ${branchA}/file >> ${OUT}/file; cat ${branchB}/file >> ${OUT}/file"},
		},
		Input: &client.Input{Cross: []client.Input{
			{Name: "branchA", Repo: repo, Branch: "branchA", Glob: "/*"},
			{Name: "branchB", Repo: repo, Branch: "branchB", Glob: "/*"},
		}},
	})

	cmA := commitFiles(t, repo, "branchA", map[string]string{"file": "data A\n"})
	cmB := commitFiles(t, repo, "branchB", map[string]string{"file": "data B\n"})

	// flushing the pair yields exactly one output commit — the cross
	// pairing — not the lone-side job
	jobs, err := c.FlushSet([]string{cmA.ID, cmB.ID}, 60*time.Second)
	if err != nil {
		t.Fatalf("flush set: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want exactly 1 output commit", len(jobs))
	}
	got, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != "data A\ndata B\n" {
		t.Fatalf("output = %q, want %q (both branches, declaration order)", got, "data A\ndata B\n")
	}
}

// A cross over two
// repositories processes every combination of per-side datums: 2 files × 2
// files = 4 datums, each emitting both sides' files, concatenated in the
// single output file (8 lines).
func TestCrossInputProcessesEveryCombination(t *testing.T) {
	r1, r2 := uniq(t)+"1", uniq(t)+"2"
	mustRepo(t, r1)
	mustRepo(t, r2)
	cm1 := commitFiles(t, r1, "master", map[string]string{"file1": "foo\n", "file2": "foo\n"})
	cm2 := commitFiles(t, r2, "master", map[string]string{"file1": "foo\n", "file2": "foo\n"})

	pipe := uniq(t)
	cross := &client.Input{Cross: []client.Input{
		{Name: "r1", Repo: r1, Glob: "/*"},
		{Name: "r2", Repo: r2, Glob: "/*"},
	}}
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "cat ${r1}/* ${r2}/* >> ${OUT}/file"},
		},
		Input: cross,
	})

	jobs, err := c.FlushSet([]string{cm1.ID, cm2.ID}, 60*time.Second)
	if err != nil {
		t.Fatalf("flush set: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want 1", len(jobs))
	}
	got, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != strings.Repeat("foo\n", 8) {
		t.Fatalf("output = %q, want %d foo lines (4 datums x 2 sides)", got, 8)
	}

	// the datum count is the cross product, confirmed by enumeration
	datums, err := c.EnumerateDatums(*cross)
	if err != nil {
		t.Fatalf("enumerate: %v", err)
	}
	if len(datums) != 4 {
		t.Fatalf("enumerated %d datums, want 4 (2 x 2)", len(datums))
	}
	for _, dt := range datums {
		if len(dt.Files) != 2 {
			t.Fatalf("datum %s has %d input files, want 2 (one per side)", dt.ID, len(dt.Files))
		}
	}
}

// A cross pipeline creates a job on every
// input commit, pairing it with the other sides' current heads; job
// listing filters by the exact input commits a job consumed.
func TestListJobInputCommits(t *testing.T) {
	repoA, repoB := uniq(t)+"a", uniq(t)+"b"
	mustRepo(t, repoA)
	mustRepo(t, repoB)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "cat ${a}/* ${b}/* > ${OUT}/file"},
		},
		Input: &client.Input{Cross: []client.Input{
			{Name: "a", Repo: repoA, Glob: "/*"},
			{Name: "b", Repo: repoB, Glob: "/*"},
		}},
	})

	// commits in order: A1, B1, A2, B2, flushing each head pairing
	a1 := commitFiles(t, repoA, "master", map[string]string{"f1": "a1"})
	b1 := commitFiles(t, repoB, "master", map[string]string{"f1": "b1"})
	flushSetOK(t, []string{a1.ID, b1.ID})
	a2 := commitFiles(t, repoA, "master", map[string]string{"f2": "a2"})
	flushSetOK(t, []string{a2.ID, b1.ID})
	b2 := commitFiles(t, repoB, "master", map[string]string{"f2": "b2"})
	flushSetOK(t, []string{a2.ID, b2.ID})

	// all four jobs exist and are terminal before filtering
	pollFor(t, "all 4 cross jobs terminal", 60*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		if err != nil || len(js) != 4 {
			return false
		}
		for _, j := range js {
			if j.State == "running" {
				return false
			}
		}
		return true
	})

	count := func(commits ...string) int {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe, InputCommits: commits})
		if err != nil {
			t.Fatalf("filter %v: %v", commits, err)
		}
		return len(js)
	}
	// single-commit filters: every job whose input set includes the commit
	if n := count(a1.ID); n != 2 {
		t.Fatalf("[A1] -> %d jobs, want 2 (A1-only and A1+B1)", n)
	}
	if n := count(b1.ID); n != 2 {
		t.Fatalf("[B1] -> %d jobs, want 2 (A1+B1 and A2+B1)", n)
	}
	if n := count(a2.ID); n != 2 {
		t.Fatalf("[A2] -> %d jobs, want 2 (A2+B1 and A2+B2)", n)
	}
	if n := count(b2.ID); n != 1 {
		t.Fatalf("[B2] -> %d jobs, want 1 (A2+B2)", n)
	}
	// pair filters: jobs whose input set includes every listed commit
	if n := count(a1.ID, b1.ID); n != 1 {
		t.Fatalf("[A1,B1] -> %d jobs, want 1", n)
	}
	if n := count(a2.ID, b1.ID); n != 1 {
		t.Fatalf("[A2,B1] -> %d jobs, want 1", n)
	}
	if n := count(a2.ID, b2.ID); n != 1 {
		t.Fatalf("[A2,B2] -> %d jobs, want 1", n)
	}
	// branch-head references resolve to the current heads
	headA, err := c.HeadCommit(repoA, "master")
	if err != nil {
		t.Fatalf("head A: %v", err)
	}
	headB, err := c.HeadCommit(repoB, "master")
	if err != nil {
		t.Fatalf("head B: %v", err)
	}
	if n := count(headA.ID, headB.ID); n != 1 {
		t.Fatalf("[master,master] -> %d jobs, want 1 (A2+B2)", n)
	}
}

// Datum enumeration of a cross input yields the full
// cartesian product, standalone (no pipeline).
func TestListDatum(t *testing.T) {
	r1, r2 := uniq(t)+"1", uniq(t)+"2"
	mustRepo(t, r1)
	mustRepo(t, r2)
	files := map[string]string{}
	for i := 0; i < 5; i++ {
		files[fmt.Sprintf("file-%d", i)] = fmt.Sprintf("%d", i)
	}
	commitFiles(t, r1, "master", files)
	commitFiles(t, r2, "master", files)

	cross := &client.Input{Cross: []client.Input{
		{Name: "r1", Repo: r1, Glob: "/*"},
		{Name: "r2", Repo: r2, Glob: "/*"},
	}}
	datums, err := c.EnumerateDatums(*cross)
	if err != nil {
		t.Fatalf("enumerate: %v", err)
	}
	if len(datums) != 25 {
		t.Fatalf("enumerated %d datums, want 25 (5 x 5 cross product)", len(datums))
	}
}

// flushSetOK flushes a set of commits and requires the pairing job(s) to
// succeed.
func flushSetOK(t *testing.T, ids []string) []client.Job {
	t.Helper()
	jobs, err := c.FlushSet(ids, 60*time.Second)
	if err != nil {
		t.Fatalf("flush set %v: %v", ids, err)
	}
	for _, j := range jobs {
		if j.State != "success" {
			t.Fatalf("job %s (%s) state = %s, want success (reason %q)", j.ID, j.Pipeline, j.State, j.Reason)
		}
	}
	return jobs
}
