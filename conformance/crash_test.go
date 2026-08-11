// SB-091 — a pipeline whose execution environment cannot be provisioned
// enters a crashing state with a recorded reason, for both an obviously
// invalid image name and a plausible-but-absent qualified reference.
package conformance

import (
	"testing"
	"time"

	"sandman/client"
)

func TestSB091_UnprovisionableEnvironmentCrashes(t *testing.T) {
	images := []string{
		"sandman-no-such-image-xyz",
		"docker.io/library/sandman-no-such-image-xyz:latest",
	}
	for _, img := range images {
		repo := uniq(t)
		mustRepo(t, repo)
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name: pipe,
			Transform: &client.Transform{
				Image: img,
				Cmd:   []string{"sh", "-c", "true"},
			},
			Input: &client.Input{Repo: repo, Glob: "/*"},
		})
		cm := commitFiles(t, repo, "master", map[string]string{"f": "x"})
		if _, err := c.FlushSet([]string{cm.ID}, 60*time.Second); err != nil {
			t.Fatalf("flush of the unprovisionable pipeline: %v", err)
		}

		// the provisioning failure converges on the crashing state with a
		// recorded reason, not a hang in running
		var pi client.PipelineInfo
		pollFor(t, "pipeline "+pipe+" to crash", 120*time.Second, func() bool {
			got, err := c.InspectPipeline(pipe)
			if err != nil {
				return false
			}
			pi = got
			return got.State == "crashed"
		})
		if pi.Reason == "" {
			t.Fatalf("pipeline %s crashed with no reason", pipe)
		}
	}
}
