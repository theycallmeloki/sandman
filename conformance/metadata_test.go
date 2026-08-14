package conformance

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// SB-119 — a pipeline carries an optional user description that inspection
// returns unchanged.
func TestSB119_PipelineDescriptionRoundTrips(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	p := client.Pipeline{
		Name:        name,
		Description: "pipeline description",
		Transform:   copyTransform(repo),
		Input:       &client.Input{Repo: repo, Glob: "/*"},
	}
	mustPipeline(t, p)

	info, err := c.InspectPipeline(name)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if info.Description != "pipeline description" {
		t.Fatalf("description = %q, want %q", info.Description, "pipeline description")
	}
}

// SB-029 — pipeline metadata counts jobs per terminal state: one commit,
// one successful job, success count exactly 1.
func TestSB029_PipelineJobCounts(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{Name: pipe, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})
	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush: %d jobs, want 1", len(jobs))
	}

	info, err := c.InspectPipeline(pipe)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if info.JobCounts["success"] != 1 {
		t.Fatalf("jobCounts = %v, want success:1", info.JobCounts)
	}
	for _, st := range []string{"running", "failure", "killed"} {
		if info.JobCounts[st] != 0 {
			t.Fatalf("jobCounts[%s] = %d, want 0", st, info.JobCounts[st])
		}
	}

	// the reference's InspectJob(BlockState): WaitJob blocks on the
	// server's state broadcast until the job settles — a running job's
	// wait returns its terminal state and genuinely blocks for the
	// remaining run time (the long-poll wait)
	repo2 := uniq(t)
	mustRepo(t, repo2)
	slow := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: slow,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "sleep 3; cp ${" + repo2 + "}/f ${OUT}/f"},
		},
		Input: &client.Input{Repo: repo2, Glob: "/*"},
	})
	_ = commitFiles(t, repo2, "master", map[string]string{"f": "x"})
	waitJobFor(t, slow, 30*time.Second)
	j := latestJob(t, slow)
	start := time.Now()
	settled, err := c.WaitJob(j.ID, 60*time.Second)
	if err != nil {
		t.Fatalf("wait job: %v", err)
	}
	elapsed := time.Since(start)
	if settled.State != "success" {
		t.Fatalf("waited job state = %s (reason %q), want success", settled.State, settled.Reason)
	}
	if elapsed < 2*time.Second {
		t.Fatalf("WaitJob returned after %v; it did not block for the run", elapsed)
	}
}

// SB-036 — metadata for repos, commits, files, pipelines, and jobs renders
// in detailed human-readable form without error after a real end-to-end run.
func TestSB036_DetailedRendering(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{Name: pipe, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})
	jobs := flushOK(t, cm.ID)
	job := jobs[0]

	if s, err := c.DescribeRepo(repo); err != nil || s == "" {
		t.Fatalf("DescribeRepo: %v (%q)", err, s)
	}
	if s, err := c.DescribeCommit(job.OutputCommit); err != nil || s == "" {
		t.Fatalf("DescribeCommit: %v (%q)", err, s)
	}
	if s, err := c.DescribeFile(job.OutputCommit, "file"); err != nil || s == "" {
		t.Fatalf("DescribeFile: %v (%q)", err, s)
	}
	if s, err := c.DescribePipeline(pipe); err != nil || s == "" {
		t.Fatalf("DescribePipeline: %v (%q)", err, s)
	}
	if s, err := c.DescribeJob(job.ID); err != nil || s == "" {
		t.Fatalf("DescribeJob: %v (%q)", err, s)
	}
	_ = cm
}

// SB-047 — a single job produces 20,000 output files in one commit, with
// correct prefix-glob counts.
func TestSB047_TwentyThousandOutputFiles(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", fmt.Sprintf("while read n; do echo data > ${OUT}/$n; done < ${%s}/input", repo)},
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	})

	// input: one file with 20,000 lines, one line per output file name
	var sb strings.Builder
	for i := range 20000 {
		fmt.Fprintf(&sb, "%d\n", i)
	}
	cm := commitFiles(t, repo, "master", map[string]string{"input": sb.String()})

	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush: %d jobs, want 1", len(jobs))
	}
	out := jobs[0].OutputCommit
	files, err := c.ListFiles(out)
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(files) != 20000 {
		t.Fatalf("output has %d files, want 20000", len(files))
	}
	count := func(glob string) int {
		// the server applies the prefix-glob filter
		got, err := c.ListFilesGlob(out, glob)
		if err != nil {
			t.Fatalf("list %q: %v", glob, err)
		}
		return len(got)
	}
	if got := count("1*"); got != 11111 {
		t.Fatalf("prefix 1*: %d files, want 11111", got)
	}
	if got := count("5*"); got != 1111 {
		t.Fatalf("prefix 5*: %d files, want 1111", got)
	}
	if got := count("9*"); got != 1111 {
		t.Fatalf("prefix 9*: %d files, want 1111", got)
	}
}
