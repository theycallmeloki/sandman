// A cross input whose member is a union of two branches of one
// repository resolves the other cross leg to the current branch head at
// job-creation time and creates exactly one job (reference issue #5172).
package conformance

import (
	"testing"

	"sandman/client"
)

func TestCrossUnionHeadResolution(t *testing.T) {
	ds := uniq(t)
	mustRepo(t, ds)
	cm := commitFiles(t, ds, "master", map[string]string{"a": "1", "b": "2", "c": "3"})

	// downstream consumes the dataset into its own master
	down := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      down,
		Transform: copyTransform(ds),
		Input:     &client.Input{Repo: ds, Glob: "/*"},
	})
	downJobs := flushSetOK(t, []string{cm.ID})
	downHead := downJobs[0].OutputCommit

	// a second branch at a previous commit of the downstream repo
	if err := c.CreateBranch(down, "other", downHead); err != nil {
		t.Fatalf("create branch: %v", err)
	}

	// imputation: cross of (union of the two downstream branches) with the
	// whole dataset repo
	impute := uniq(t)
	j := flushPipeline(t, client.Pipeline{
		Name: impute,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "cp -r ${ds}/* ${OUT}/"},
		},
		Input: &client.Input{Cross: []client.Input{
			{Name: "u", Union: []client.Input{
				{Repo: down, Branch: "master", Glob: "/*"},
				{Repo: down, Branch: "other", Glob: "/*"},
			}},
			{Name: "ds", Repo: ds, Branch: "master", Glob: "/"},
		}},
	})

	jobs, err := c.ListJobsFiltered(client.JobFilter{Pipeline: impute})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("imputation has %d jobs, want exactly 1", len(jobs))
	}
	// the dataset leg of the job's input equals the dataset master head
	found := false
	for _, ic := range j.InputCommits {
		if ic == cm.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("job input %v does not contain the dataset head %s", j.InputCommits, cm.ID)
	}
}
