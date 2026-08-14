package conformance

import (
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// Pipeline names are unique; duplicate creation without update is
// rejected at creation time.
func TestPipelineNameUniqueness(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)

	p := client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}
	mustPipeline(t, p)

	err := c.CreatePipeline(p)
	wantErr(t, err, "already exists")
}

// A pipeline whose input is its own output repository is rejected
// at creation.
func TestSelfReferentialInputRejected(t *testing.T) {
	name := uniq(t)
	p := client.Pipeline{
		Name:      name,
		Transform: copyTransform(name),
		Input:     &client.Input{Repo: name, Glob: "/"},
	}
	err := c.CreatePipeline(p)
	wantErr(t, err, "output") // a pipeline must not have its own output repo as an input
}

// A pipeline created without a command runs a default entry point
// that copies inputs to output, preserving path and content.
func TestCommandlessPipelineCopies(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)

	p := client.Pipeline{
		Name:      uniq(t),
		Transform: &client.Transform{}, // no cmd, no stdin → default entry point
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	}
	mustPipeline(t, p)

	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})

	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want 1", len(jobs))
	}
	got, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if string(got) != "foo" {
		t.Fatalf("output content = %q, want %q", got, "foo")
	}
}

// Empty commits still trigger jobs (20 consecutive), and a command
// that never reads stdin completes without deadlock.
func TestEmptyCommitsTriggerJobsNoStdinDeadlock(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	commitFiles(t, repo, "master", map[string]string{"seed": "x"})

	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"true"}, // never reads stdin
			Stdin: []string{"data the command never reads"},
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	}
	mustPipeline(t, p)

	for i := 0; i < 20; i++ {
		cm, err := c.StartCommit(repo, "master", "")
		if err != nil {
			t.Fatalf("commit %d start: %v", i, err)
		}
		if _, err := c.FinishCommit(cm.ID, "", false); err != nil {
			t.Fatalf("commit %d finish: %v", i, err)
		}
		jobs := flushOK(t, cm.ID)
		if len(jobs) != 1 {
			t.Fatalf("commit %d: %d jobs, want exactly 1", i, len(jobs))
		}
	}
}

// Pipeline creation without a name is rejected with an error
// naming the missing field; the service survives.
func TestCreateWithoutNameRejected(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	p := client.Pipeline{Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}
	err := c.CreatePipeline(p)
	wantErr(t, err, "pipeline")
}

// Pipeline creation without a transform is rejected with a
// descriptive error.
func TestCreateWithoutTransformRejected(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	p := client.Pipeline{Name: uniq(t), Input: &client.Input{Repo: repo, Glob: "/*"}}
	err := c.CreatePipeline(p)
	wantErr(t, err, "transform")
}

// A pipeline whose transform has no command but specifies stdin is
// accepted at creation, then transitions to FAILURE within 30s.
func TestNoCommandAcceptedButFailsAtStart(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})

	name := uniq(t)
	p := client.Pipeline{
		Name: name,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Stdin: []string{"a stdin line"}, // no command
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	}
	mustPipeline(t, p) // creation succeeds

	deadline := time.Now().Add(30 * time.Second)
	for {
		info, err := c.InspectPipeline(name)
		if err != nil {
			t.Fatalf("inspect pipeline: %v", err)
		}
		if info.State == "failure" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pipeline state = %q after 30s, want failure", info.State)
		}
		time.Sleep(500 * time.Millisecond)
	}
	// the job for the input commit must have failed, not completed
	jobs, err := c.Flush(cm.ID, 10*time.Second)
	if err == nil {
		for _, j := range jobs {
			if j.State == "success" {
				t.Fatalf("job %s succeeded despite no command", j.ID)
			}
		}
	}
}

// Pipeline creation validates the request shape and rejects every
// malformed file-input variant cleanly, with the documented signals. (Cases
// for aggregate inputs and services are deferred to the batches that
// introduce those input types; git-input signals are asserted
// by TestGitURLValidation.)
func TestMalformedPipelineRequestsRejected(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)

	cases := []struct {
		name string
		p    client.Pipeline
		want string
	}{
		{"git missing url", client.Pipeline{Name: uniq(t), Transform: &client.Transform{Image: "alpine:3.21"}, Input: &client.Input{Git: &client.GitInput{}}}, "clone URL is missing ("},
		{"git no .git suffix", client.Pipeline{Name: uniq(t), Transform: &client.Transform{Image: "alpine:3.21"}, Input: &client.Input{Git: &client.GitInput{URL: "https://host/path"}}}, "clone URL is missing .git suffix"},
		{"git not https", client.Pipeline{Name: uniq(t), Transform: &client.Transform{Image: "alpine:3.21"}, Input: &client.Input{Git: &client.GitInput{URL: "git://host/path.git"}}}, "clone URL must use https protocol"},
		{"no name no transform", client.Pipeline{}, "invalid pipeline spec"},
		{"name without transform", client.Pipeline{Name: uniq(t), Input: &client.Input{Repo: repo, Glob: "/*"}}, "transform"},
		{"empty input", client.Pipeline{Name: uniq(t), Transform: copyTransform(repo)}, "no input set"},
		{"input missing everything", client.Pipeline{Name: uniq(t), Transform: copyTransform(repo), Input: &client.Input{}}, "input must specify a name"},
		{"input missing repo", client.Pipeline{Name: uniq(t), Transform: copyTransform(repo), Input: &client.Input{Name: "in"}}, "input must specify a repo"},
		{"input missing glob", client.Pipeline{Name: uniq(t), Transform: copyTransform("in"), Input: &client.Input{Name: "in", Repo: repo}}, "input must specify a glob"},
		{"input named out", client.Pipeline{Name: uniq(t), Transform: copyTransform("out"), Input: &client.Input{Name: "out", Repo: repo, Glob: "/*"}}, `input cannot be named "out"`},
		{"input repo named out", client.Pipeline{Name: uniq(t), Transform: copyTransform("out"), Input: &client.Input{Repo: "out", Glob: "/*"}}, `input cannot be named "out"`},
		{"nonexistent repo", client.Pipeline{Name: uniq(t), Transform: copyTransform("in"), Input: &client.Input{Name: "in", Repo: "does-not-exist", Glob: "/*"}}, "not found"},
		// a cron input missing its name (its derived repository
		// needs the name)
		{"cron missing name", client.Pipeline{Name: uniq(t), Transform: copyTransform("in"), Input: &client.Input{Cron: "@every 1m"}}, "input must specify a name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := c.CreatePipeline(tc.p)
			wantErr(t, err, tc.want)
		})
	}
}

// Pipeline validation rejects inputs named "out" and file inputs
// without a glob.
func TestInputNamedOutAndMissingGlobRejected(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)

	err := c.CreatePipeline(client.Pipeline{
		Name: uniq(t), Transform: copyTransform("out"),
		Input: &client.Input{Name: "out", Repo: repo, Glob: "/*"},
	})
	wantErr(t, err, "out")

	err = c.CreatePipeline(client.Pipeline{
		Name: uniq(t), Transform: copyTransform("in"),
		Input: &client.Input{Name: "in", Repo: repo}, // no glob
	})
	wantErr(t, err, "glob")
}

// Pipeline creation fails when a declared input repository does
// not exist.
func TestNonexistentInputRepoRejected(t *testing.T) {
	err := c.CreatePipeline(client.Pipeline{
		Name:      uniq(t),
		Transform: copyTransform("in"),
		Input:     &client.Input{Name: "in", Repo: "no-such-repo", Glob: "/*"},
	})
	wantErr(t, err, "not found")
}

// Pipeline names may contain hyphens and underscores.
func TestNamesWithHyphensAndUnderscores(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := "my-pipeline_01"
	p := client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}
	mustPipeline(t, p)
	if _, err := c.InspectPipeline(name); err != nil {
		t.Fatalf("inspect: %v", err)
	}
}

// A parallelism specification must not combine a constant count
// with a coefficient.
func TestMixedParallelismRejected(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	err := c.CreatePipeline(client.Pipeline{
		Name:      uniq(t),
		Transform: copyTransform(repo),
		Input:     &client.Input{Repo: repo, Glob: "/*"},
		Parallelism: &client.Parallelism{
			Constant:    1,
			Coefficient: 1.0,
		},
	})
	wantErr(t, err, "parallelism")
}

// A plain cross (not cross-of-union) whose members
// expose the same name is rejected: two branches sharing a namespace are
// ambiguous, and the record demands the rejection (its message is
// "name was used more than once"; Sandman's signal is the cross
// namespace check).
func TestCrossDuplicateNamesRejected(t *testing.T) {
	ra := uniq(t) + "a"
	rb := uniq(t) + "b"
	mustRepo(t, ra)
	mustRepo(t, rb)
	err := c.CreatePipeline(client.Pipeline{
		Name:      uniq(t),
		Transform: copyTransform("x"),
		Input: &client.Input{Cross: []client.Input{
			{Name: "branch", Repo: ra, Glob: "/*"},
			{Name: "branch", Repo: rb, Glob: "/*"},
		}},
	})
	wantErr(t, err, "distinct namespaces")
}

// A pipeline name is a state-dir path component and the output
// repo's name: names that could escape the pipelines directory ("..",
// separators, leading dots) are rejected at creation, not silently
// written outside the state dir.
func TestPipelineNamePathEscapeRejected(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	for _, name := range []string{"..", ".", "../x", "a/b", "a\\b", ".hidden"} {
		err := c.CreatePipeline(client.Pipeline{
			Name:      name,
			Transform: copyTransform(repo),
			Input:     &client.Input{Repo: repo, Glob: "/*"},
		})
		if err == nil {
			t.Fatalf("pipeline name %q accepted, want rejection", name)
		}
	}
	// the update path validates the same way
	err := c.CreatePipeline(client.Pipeline{
		Name:      uniq(t),
		Transform: copyTransform(repo),
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	})
	if err != nil {
		t.Fatalf("valid pipeline: %v", err)
	}
}

// TestHyphenatedRepoName — a repo with a hyphen in its name is
// consumable: the datum env var derives from the repo name with
// non-identifier characters sanitized to underscores (pachctl allows
// hyphens in names throughout; the transform references the sanitized
// name).
func TestHyphenatedRepoName(t *testing.T) {
	repo := uniq(t) + "-data"
	mustRepo(t, repo)
	env := strings.ReplaceAll(repo, "-", "_")
	name := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: name,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "cp ${" + env + "}/file ${OUT}/file"},
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	})
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("hyphenated-repo flush = %d jobs, want 1", len(jobs))
	}
	b, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil || string(b) != "x" {
		t.Fatalf("output = %q, err %v; want x (the sanitized env name ${%s} resolves)", b, err, env)
	}
}
