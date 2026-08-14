package conformance

import (
	"fmt"
	"sandman/client"
	"strings"
	"testing"
)

// Each input repository is exposed to the pipeline command as an
// environment variable named after the repository; the command can copy all
// 10 input files through that variable, byte-for-byte.
func TestInputRepoEnvVar(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	files := map[string]string{}
	for i := 0; i < 10; i++ {
		files[fmt.Sprintf("file-%d", i)] = fmt.Sprintf("%d", i)
	}
	p := client.Pipeline{
		Name:      uniq(t),
		Transform: copyTransform(repo), // uses $<repo> to reach the input
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	}
	mustPipeline(t, p)

	cm := commitFiles(t, repo, "master", files)

	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want 1", len(jobs))
	}
	out := jobs[0].OutputCommit
	for i := 0; i < 10; i++ {
		got, err := c.GetFile(out, fmt.Sprintf("file-%d", i))
		if err != nil {
			t.Fatalf("read file-%d: %v", i, err)
		}
		if string(got) != fmt.Sprintf("%d", i) {
			t.Fatalf("file-%d content = %q, want %q", i, got, fmt.Sprintf("%d", i))
		}
	}
}

// Custom environment variables declared in a pipeline's execution
// configuration are visible inside the execution environment, unmodified.
func TestCustomEnvVarsVisible(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)

	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "echo ${CUSTOM_ENV_VAR} > ${OUT}/value"},
			Env:   map[string]string{"CUSTOM_ENV_VAR": "custom-value"},
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	}
	mustPipeline(t, p)

	cm := commitFiles(t, repo, "master", map[string]string{"file1": "x"})

	jobs := flushOK(t, cm.ID)
	got, err := c.GetFile(jobs[0].OutputCommit, "value")
	if err != nil {
		t.Fatalf("read value: %v", err)
	}
	if strings.TrimSpace(string(got)) != "custom-value" {
		t.Fatalf("value = %q, want %q", got, "custom-value")
	}
}

// Jobs receive metadata through the environment: job id, output
// commit id, per-input commit id, and the input path, alongside custom
// variables. (Secret store values are deferred to the secrets batch.)
func TestJobMetadataEnvVars(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)

	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd: []string{"sh", "-c",
				fmt.Sprintf(`printf '%%s' "${CUSTOM}" > ${OUT}/custom; printf '%%s' "${JOB_ID}" > ${OUT}/jobid; printf '%%s' "${OUTPUT_COMMIT}" > ${OUT}/outcommit; printf '%%s' "${%s_COMMIT}" > ${OUT}/incommit; printf '%%s' "${%s}/file" > ${OUT}/inpath`, repo, repo)},
			Env: map[string]string{"CUSTOM": "bar"},
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	}
	mustPipeline(t, p)

	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})

	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want 1", len(jobs))
	}
	j := jobs[0]
	out := j.OutputCommit

	read := func(path string) string {
		t.Helper()
		b, err := c.GetFile(out, path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return string(b)
	}
	if got := read("custom"); got != "bar" {
		t.Fatalf("custom = %q, want bar", got)
	}
	if got := read("jobid"); got != j.ID {
		t.Fatalf("jobid = %q, want %q", got, j.ID)
	}
	if got := read("outcommit"); got != out {
		t.Fatalf("outcommit = %q, want %q", got, out)
	}
	if got := read("incommit"); got != cm.ID {
		t.Fatalf("incommit = %q, want input commit %q", got, cm.ID)
	}
	if got := read("inpath"); !strings.HasSuffix(got, "/file") {
		t.Fatalf("inpath = %q, want a path ending in /file", got)
	}
}

// Execution participants run user code under a configured user
// identity and working directory, observable through whoami and pwd.
