package conformance

import (
	"testing"

	"sandman/client"
)

// TestRepoSize — a repository's reported size is the total bytes of
// its primary branch's head revision, for data repos and pipeline output
// repos alike, and stays in sync across commit deletion (the
// upstream #3330 regression: output repos reporting 0B).
func TestRepoSize(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)

	// the pipeline exists before any input: it watches the primary branch
	// only, so the second branch's commit never triggers it
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      pipe,
		Transform: copyTransform(repo),
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	})

	// file2 (3 bytes) on the primary branch, through its own commit — the
	// record's implicit-commit path; the head job must settle so the
	// output repo keeps a surviving ancestor after the deletion below
	c2 := commitFiles(t, repo, "master", map[string]string{"file2": "abc"})
	flushOK(t, c2.ID)

	// file3 (3 bytes) on a second branch: excluded from the primary
	// branch's size, and no job runs (the pipeline watches master)
	commitFiles(t, repo, "dev", map[string]string{"file3": "def"})

	// file1 (3 bytes) in an explicitly opened and finished commit on the
	// primary branch; the pipeline accumulates and outputs both files
	c1 := commitFiles(t, repo, "master", map[string]string{"file1": "ghi"})
	flushOK(t, c1.ID)

	// the data repo reports 6 bytes: the master head revision (file1 +
	// file2); dev's file3 does not count
	r, err := c.InspectRepo(repo)
	if err != nil {
		t.Fatalf("inspect data repo: %v", err)
	}
	if r.SizeBytes != 6 {
		t.Fatalf("data repo size = %d, want 6 (master head only)", r.SizeBytes)
	}

	// the output repo reports 6 bytes too — never 0 after processing
	o, err := c.InspectRepo(pipe)
	if err != nil {
		t.Fatalf("inspect output repo: %v", err)
	}
	if o.SizeBytes != 6 {
		t.Fatalf("output repo size = %d, want 6 (regression: output repos reporting 0B)", o.SizeBytes)
	}

	// deleting the commit holding file1 shrinks both repos to the
	// remaining master file: the data repo's head falls back to file2's
	// commit, and the output repo's derived commit is removed with its
	// provenance, leaving the file2-derived output as the head
	if err := c.DeleteCommit(c1.ID); err != nil {
		t.Fatalf("delete commit: %v", err)
	}
	r, err = c.InspectRepo(repo)
	if err != nil {
		t.Fatalf("inspect data repo after delete: %v", err)
	}
	if r.SizeBytes != 3 {
		t.Fatalf("data repo size after delete = %d, want 3", r.SizeBytes)
	}
	o, err = c.InspectRepo(pipe)
	if err != nil {
		t.Fatalf("inspect output repo after delete: %v", err)
	}
	if o.SizeBytes != 3 {
		t.Fatalf("output repo size after delete = %d, want 3 (output follows the input deletion)", o.SizeBytes)
	}
}
