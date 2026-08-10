package conformance

import (
	"testing"
	"time"

	"sandman/client"
)

// SB-039 — pipeline names are unique; duplicate creation without update is
// rejected at creation time.
func TestSB039_PipelineNameUniqueness(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)

	p := client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}
	mustPipeline(t, p)

	err := c.CreatePipeline(p)
	wantErr(t, err, "already exists")
}

// SB-090 — a pipeline whose input is its own output repository is rejected
// at creation.
func TestSB090_SelfReferentialInputRejected(t *testing.T) {
	name := uniq(t)
	p := client.Pipeline{
		Name:      name,
		Transform: copyTransform(name),
		Input:     &client.Input{Repo: name, Glob: "/"},
	}
	err := c.CreatePipeline(p)
	wantErr(t, err, "output") // a pipeline must not have its own output repo as an input
}

// SB-126 — a pipeline created without a command runs a default entry point
// that copies inputs to output, preserving path and content.
func TestSB126_CommandlessPipelineCopies(t *testing.T) {
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

// SB-129 — empty commits still trigger jobs (20 consecutive), and a command
// that never reads stdin completes without deadlock.
func TestSB129_EmptyCommitsTriggerJobsNoStdinDeadlock(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	commitFiles(t, repo, "master", map[string]string{"seed": "x"})

	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine",
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

// SB-147 — pipeline creation without a name is rejected with an error
// naming the missing field; the service survives.
func TestSB147_CreateWithoutNameRejected(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	p := client.Pipeline{Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}
	err := c.CreatePipeline(p)
	wantErr(t, err, "pipeline")
}

// SB-148 — pipeline creation without a transform is rejected with a
// descriptive error.
func TestSB148_CreateWithoutTransformRejected(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	p := client.Pipeline{Name: uniq(t), Input: &client.Input{Repo: repo, Glob: "/*"}}
	err := c.CreatePipeline(p)
	wantErr(t, err, "transform")
}

// SB-149 — a pipeline whose transform has no command but specifies stdin is
// accepted at creation, then transitions to FAILURE within 30s.
func TestSB149_NoCommandAcceptedButFailsAtStart(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})

	name := uniq(t)
	p := client.Pipeline{
		Name: name,
		Transform: &client.Transform{
			Image: "alpine",
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

// SB-159 — pipeline creation validates the request shape and rejects every
// malformed file-input variant cleanly, with the documented signals. (Cases
// for aggregate/cron/git inputs and services are deferred to the batches
// that introduce those input types.)
func TestSB159_MalformedPipelineRequestsRejected(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)

	cases := []struct {
		name string
		p    client.Pipeline
		want string
	}{
		{"no name no transform", client.Pipeline{}, "invalid pipeline spec"},
		{"name without transform", client.Pipeline{Name: uniq(t), Input: &client.Input{Repo: repo, Glob: "/*"}}, "transform"},
		{"empty input", client.Pipeline{Name: uniq(t), Transform: copyTransform(repo)}, "no input set"},
		{"input missing everything", client.Pipeline{Name: uniq(t), Transform: copyTransform(repo), Input: &client.Input{}}, "input must specify a name"},
		{"input missing repo", client.Pipeline{Name: uniq(t), Transform: copyTransform(repo), Input: &client.Input{Name: "in"}}, "input must specify a repo"},
		{"input missing glob", client.Pipeline{Name: uniq(t), Transform: copyTransform("in"), Input: &client.Input{Name: "in", Repo: repo}}, "input must specify a glob"},
		{"input named out", client.Pipeline{Name: uniq(t), Transform: copyTransform("out"), Input: &client.Input{Name: "out", Repo: repo, Glob: "/*"}}, `input cannot be named "out"`},
		{"input repo named out", client.Pipeline{Name: uniq(t), Transform: copyTransform("out"), Input: &client.Input{Repo: "out", Glob: "/*"}}, `input cannot be named "out"`},
		{"nonexistent repo", client.Pipeline{Name: uniq(t), Transform: copyTransform("in"), Input: &client.Input{Name: "in", Repo: "does-not-exist", Glob: "/*"}}, "not found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := c.CreatePipeline(tc.p)
			wantErr(t, err, tc.want)
		})
	}
}

// SB-170 — pipeline validation rejects inputs named "out" and file inputs
// without a glob.
func TestSB170_InputNamedOutAndMissingGlobRejected(t *testing.T) {
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

// SB-171 — pipeline creation fails when a declared input repository does
// not exist.
func TestSB171_NonexistentInputRepoRejected(t *testing.T) {
	err := c.CreatePipeline(client.Pipeline{
		Name:      uniq(t),
		Transform: copyTransform("in"),
		Input:     &client.Input{Name: "in", Repo: "no-such-repo", Glob: "/*"},
	})
	wantErr(t, err, "not found")
}

// SB-172 — pipeline names may contain hyphens and underscores.
func TestSB172_NamesWithHyphensAndUnderscores(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	name := "my-pipeline_01"
	p := client.Pipeline{Name: name, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}
	mustPipeline(t, p)
	if _, err := c.InspectPipeline(name); err != nil {
		t.Fatalf("inspect: %v", err)
	}
}

// SB-173 — a parallelism specification must not combine a constant count
// with a coefficient.
func TestSB173_MixedParallelismRejected(t *testing.T) {
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
