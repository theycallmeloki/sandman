// Egress: a configured external output destination is a
// supported pipeline output; a failed egress write fails the job with an
// egress-related reason even when the output commit succeeded.
package conformance

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// TestSB013_EgressFailure — a pipeline with an invalid egress URL runs its
// execution (the output commit succeeds), then the egress write fails and
// the job settles as failure with an egress-related reason. Exactly one
// job is created (the reference's TestEgressFailure shape).
func TestSB013_EgressFailure(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo\n"})

	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "cp ${" + repo + "}/file ${OUT}/file"},
		},
		Input:  &client.Input{Repo: repo, Glob: "/"},
		Egress: &client.Egress{URL: "invalid://blahblah"},
	}
	mustPipeline(t, p)

	// exactly one job, then block until it reaches a terminal state
	pollFor(t, "egress job appears", 30*time.Second, func() bool {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: p.Name})
		return err == nil && len(js) == 1
	})
	js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: p.Name})
	j, err := c.WaitJob(js[0].ID, 60*time.Second)
	if err != nil {
		t.Fatalf("wait job: %v", err)
	}
	if j.State != "failure" {
		t.Fatalf("egress job state = %s, want failure (reason %q)", j.State, j.Reason)
	}
	if !strings.Contains(j.Reason, "egress") {
		t.Fatalf("egress job reason = %q, want it to mention egress", j.Reason)
	}
	// the output commit still exists — output success alone did not make
	// the job successful
	if j.OutputCommit == "" {
		t.Fatalf("egress job has no output commit; the execution phase succeeded")
	}
	if _, err := c.GetFile(j.OutputCommit, "file"); err != nil {
		t.Fatalf("output commit content: %v", err)
	}
	_ = cm
}

// TestEgressFileDestination — file:// is the supported egress destination
// ("a configured external output destination is a supported pipeline
// output"): the job's output files land in the destination directory and
// the job succeeds.
func TestEgressFileDestination(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	commitFiles(t, repo, "master", map[string]string{"file": "foo\n"})

	dst := t.TempDir()
	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "cp ${" + repo + "}/file ${OUT}/file"},
		},
		Input:  &client.Input{Repo: repo, Glob: "/"},
		Egress: &client.Egress{URL: "file://" + dst},
	}
	mustPipeline(t, p)

	pollFor(t, "egress file destination written", 60*time.Second, func() bool {
		b, err := os.ReadFile(filepath.Join(dst, "file"))
		return err == nil && string(b) == "foo\n"
	})
	js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: p.Name})
	j, err := c.WaitJob(js[0].ID, 60*time.Second)
	if err != nil {
		t.Fatalf("wait job: %v", err)
	}
	if j.State != "success" {
		t.Fatalf("file-egress job state = %s, want success (reason %q)", j.State, j.Reason)
	}
}
