package conformance

// SB-140's spout behaviors that the pipe suite (SB-139) does not cover:
// provenance epochs anchored on the specification commit, marker state
// across plain and reprocess updates, and spec-commit subvenance with a
// downstream consumer. The spout is driven through the pipe mechanism —
// the same container environment the daemon already provides — so the
// suite stays docker-predictable.

import (
	"sandman/client"
	"testing"
	"time"
)

// updateSpout applies a new version of a spout pipeline, preserving the
// spout declaration (mustUpdate does not carry Spout).
func updateSpout(t *testing.T, name string, tr *client.Transform, spout *client.Spout, reprocess bool) {
	t.Helper()
	p := client.Pipeline{Name: name, Transform: tr, Spout: spout, Update: true, Reprocess: reprocess}
	if err := c.CreatePipeline(p); err != nil {
		t.Fatalf("update spout %s: %v", name, err)
	}
}

// markerCmd is a spout transform that appends one JOB_ID-tagged line to
// the marker file per cycle: the marker directory persists across spout
// restarts, so the accumulated content distinguishes epochs.
func markerCmd(n int) []string {
	return []string{"sh", "-c", "for i in $(seq 1 " + itoa(n) + "); do echo \"$JOB_ID-$i\" >> ${MARKER}/marker; sleep 1; done"}
}

// markerHeadContent reads the markers branch head's marker file once the
// branch holds at least n commits.
func markerHeadContent(t *testing.T, pipe string, n int) string {
	t.Helper()
	pollFor(t, "marker commits", 90*time.Second, func() bool {
		ch, err := c.CommitHistory(pipe, "markers")
		return err == nil && len(ch) >= n
	})
	mh, err := c.HeadCommit(pipe, "markers")
	if err != nil {
		t.Fatalf("marker head: %v", err)
	}
	b, err := c.GetFile(mh.ID, "marker")
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	return string(b)
}

// TestSB140_SpoutEpochsAndMarker covers the spout contracts that the pipe
// suite does not: each spout commit carries its pipeline's specification
// commit as provenance, an update starts a new epoch shared by every
// commit after it (SB-139 clause 7, SB-140 clause 3); the marker state
// persists across a plain update and resets on a reprocess update
// (SB-139 clause 10, SB-140 clause 4); and a spec commit's subvenants are
// its spout's output plus the downstream output (SB-139 clause 6).

// containsStrList reports whether a string slice holds the needle.
func containsStrList(hay []string, needle string) bool {
	for _, s := range hay {
		if s == needle {
			return true
		}
	}
	return false
}
