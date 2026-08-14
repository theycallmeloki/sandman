// SB-151 — extracting a pipeline's configuration returns a creation
// request deep-equal to the one used to create it: every user-settable
// field round-trips, the input's name/branch defaults are materialized,
// and an unsupported execution framework is rejected at creation with an
// error naming it.
package conformance

import (
	"reflect"
	"strings"
	"testing"

	"sandman/client"
)

func TestSB151_ConfigExtractionRoundTrip(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)

	want := client.Pipeline{
		Name:        pipe,
		Description: "round-trip me",
		Transform: &client.Transform{
			Image:            "alpine:3.21",
			Cmd:              []string{"sh", "-c", "cp -r ${" + repo + "}/* ${OUT}/"},
			Stdin:            []string{"line1"},
			Env:              map[string]string{"K": "V"},
			User:             "1000",
			Workdir:          "/sandman/out",
			AcceptReturnCode: 3,
			DatumTries:       2,
			DatumTimeout:     "30s",
			JobTimeout:       "120s",
		},
		Input: &client.Input{Name: repo, Repo: repo, Branch: "master", Glob: "/*"},
		Parallelism: &client.Parallelism{
			Constant: 2,
		},
		ChunkSpec: &client.ChunkSpec{
			Number: 4,
		},
		MaxQueueSize: 3,
		Autoscaling:  true,
		Standby:      true,
		OutputBranch: "out",
		Reprocess:    true,
		EnableStats:  true,
	}
	mustPipeline(t, want)

	info, err := c.InspectPipeline(pipe)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	got := client.Pipeline{
		Name:         info.Name,
		Description:  info.Description,
		Transform:    info.Transform,
		Input:        info.Input,
		Parallelism:  info.Parallelism,
		ChunkSpec:    info.ChunkSpec,
		MaxQueueSize: info.MaxQueueSize,
		Autoscaling:  info.Autoscaling,
		Standby:      info.Standby,
		OutputBranch: info.OutputBranch,
		Reprocess:    info.Reprocess,
		EnableStats:  info.EnableStats,
		Spout:        info.Spout,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("extraction differs from creation:\n got %+v\nwant %+v", got, want)
	}

	// a creation request that selects an unsupported execution framework
	// is rejected with an error naming the framework; the retry succeeds
	pipe2 := uniq(t)
	bad := want
	bad.Name = pipe2
	bad.Framework = "TFJob"
	if err := c.CreatePipeline(bad); err == nil || !strings.Contains(err.Error(), "TFJob") {
		t.Fatalf("framework rejection: err=%v, want error naming TFJob", err)
	}
	bad.Framework = ""
	if err := c.CreatePipeline(bad); err != nil {
		t.Fatalf("retry without framework: %v", err)
	}
}

// the input's implicit name and branch defaults are materialized into the
// stored spec and echoed by extraction
func TestSB151_InputDefaultsMaterialized(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      pipe,
		Transform: copyTransform(repo),
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	})
	info, err := c.InspectPipeline(pipe)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if info.Input.Name != repo {
		t.Fatalf("input name = %q, want materialized %q", info.Input.Name, repo)
	}
	if info.Input.Branch != "master" {
		t.Fatalf("input branch = %q, want materialized \"master\"", info.Input.Branch)
	}
}
