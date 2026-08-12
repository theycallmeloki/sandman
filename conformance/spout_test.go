// Spout pipelines (SB-139): a pipeline with no input whose transform runs
// in the background, with the daemon committing each data-bearing cycle to
// the output branch, a marker directory to a separate marker branch, and
// spout/input and marker-name validation.
package conformance

import (
	"sandman/client"
	"strconv"
	"testing"
	"time"
)

// spoutCmd runs n cycles, writing the output file with size*i bytes.
func spoutCmd(size, n int, marker bool) []string {
	script := "for i in $(seq 1 " + itoa(n) + "); do head -c $((i*" + itoa(size) + ")) /dev/zero | tr '\\0' 'x' > ${OUT}/file; "
	if marker {
		script += "echo m$i > ${MARKER}/marker; "
	}
	script += "sleep 1; done"
	return []string{"sh", "-c", script}
}

func itoa(n int) string {
	return strconv.Itoa(n)
}

// waitSpoutCommits waits until the spout's output branch holds at least n
// commits and returns them.
func waitSpoutCommits(t *testing.T, pipe string, n int) []client.Commit {
	t.Helper()
	var ch []client.Commit
	pollFor(t, "spout commits", 90*time.Second, func() bool {
		got, err := c.CommitHistory(pipe, "master")
		if err != nil {
			return false
		}
		ch = got
		return len(got) >= n
	})
	return ch
}

// pollSpoutJobSettled waits until the spout pipeline's job leaves the
// running state (the container exited and the daemon settled it).
func pollSpoutJobSettled(t *testing.T, pipe string) {
	t.Helper()
	pollFor(t, "spout job settled", 60*time.Second, func() bool {
		js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		if len(js) == 0 {
			return false
		}
		j, err := c.InspectJob(js[0].ID)
		return err == nil && j.State != "running"
	})
	j, err := c.InspectJob(mustJobID(t, pipe))
	if err != nil || j.State != "success" {
		t.Fatalf("spout job state = %s (reason %q), want success", j.State, j.Reason)
	}
}

// mustJobID returns the pipeline's first job id (there is exactly one per
// spout run).
func mustJobID(t *testing.T, pipe string) string {
	t.Helper()
	js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
	if err != nil || len(js) == 0 {
		t.Fatalf("jobs for %s: %v", pipe, err)
	}
	return js[0].ID
}
