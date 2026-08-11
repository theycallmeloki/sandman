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
