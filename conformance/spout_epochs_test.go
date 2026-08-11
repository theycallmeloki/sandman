package conformance

// SB-140's spout behaviors that the pipe suite (SB-139) does not cover:
// provenance epochs anchored on the specification commit, marker state
// across plain and reprocess updates, and spec-commit subvenance with a
// downstream consumer. The spout is driven through the pipe mechanism —
// the same container environment the daemon already provides — so the
// suite stays docker-predictable.

import (
	"strings"
	"testing"
	"time"

	"sandman/client"
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
func TestSB140_SpoutEpochsAndMarker(t *testing.T) {
	t.Run("provenance epochs across updates", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: spoutCmd(50, 6, false)},
			Spout:     &client.Spout{},
		})
		ch := waitSpoutCommits(t, pipe, 3)
		s1 := ch[0].Provenance
		if len(s1) != 1 {
			t.Fatalf("epoch provenance = %v, want one spec commit", s1)
		}
		for _, cm := range ch {
			if len(cm.Provenance) != 1 || cm.Provenance[0] != s1[0] {
				t.Fatalf("commit %s provenance = %v, want %v (one epoch)", cm.ID, cm.Provenance, s1)
			}
		}
		// a plain update starts a new epoch: the new spec commit anchors
		// the new commits' provenance
		updateSpout(t, pipe, &client.Transform{Image: "alpine", Cmd: spoutCmd(50, 6, false)}, &client.Spout{}, false)
		ch2 := waitSpoutCommits(t, pipe, 6)
		next := ch2[3:] // the commits after the update
		if len(next) != 3 {
			t.Fatalf("expected 3 post-update commits, got %d", len(next))
		}
		s2 := next[0].Provenance
		if len(s2) != 1 || s2[0] == s1[0] {
			t.Fatalf("new epoch provenance = %v, want a fresh spec commit != %v", s2, s1[0])
		}
		for _, cm := range next {
			if len(cm.Provenance) != 1 || cm.Provenance[0] != s2[0] {
				t.Fatalf("post-update commit %s provenance = %v, want %v", cm.ID, cm.Provenance, s2)
			}
		}
	})

	t.Run("marker persists across plain update, resets on reprocess", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: markerCmd(2)},
			Spout:     &client.Spout{Marker: "markers"},
		})
		epoch1 := markerHeadContent(t, pipe, 2)
		if strings.Count(epoch1, "\n") != 2 {
			t.Fatalf("first epoch marker = %q, want two lines", epoch1)
		}
		// a plain update restarts the spout but keeps the marker state:
		// the marker continues accumulating from its previous content
		updateSpout(t, pipe, &client.Transform{Image: "alpine", Cmd: markerCmd(2)}, &client.Spout{Marker: "markers"}, false)
		acc := markerHeadContent(t, pipe, 4)
		if strings.Count(acc, "\n") != 4 || !strings.HasPrefix(acc, epoch1) {
			t.Fatalf("marker after plain update = %q, want it to continue from %q", acc, epoch1)
		}
		// a reprocess update resets the marker state: the new epoch's
		// marker no longer reflects the previous content
		updateSpout(t, pipe, &client.Transform{Image: "alpine", Cmd: markerCmd(2)}, &client.Spout{Marker: "markers"}, true)
		ep2 := markerHeadContent(t, pipe, 6)
		if strings.Count(ep2, "\n") != 2 || strings.Contains(ep2, epoch1) {
			t.Fatalf("marker after reprocess update = %q, want fresh state without %q", ep2, epoch1)
		}
	})

	t.Run("downstream subvenance of the spec commit", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: spoutCmd(100, 5, false)},
			Spout:     &client.Spout{},
		})
		ch := waitSpoutCommits(t, pipe, 5)
		var spec string
		for _, cm := range ch {
			if len(cm.Provenance) != 1 {
				t.Fatalf("spout commit %s provenance = %v, want the spec commit", cm.ID, cm.Provenance)
			}
			spec = cm.Provenance[0]
		}
		down := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      down,
			Transform: &client.Transform{Image: "alpine"},
			Input:     &client.Input{Repo: pipe, Glob: "/*"},
		})
		head, err := c.HeadCommit(pipe, "master")
		if err != nil {
			t.Fatalf("spout head: %v", err)
		}
		jobs := flushOK(t, head.ID)
		var downJob client.Job
		for _, j := range jobs {
			if j.Pipeline == down {
				downJob = j
			}
		}
		if downJob.ID == "" {
			t.Fatalf("no downstream job for the spout's head")
		}
		downOut, err := c.InspectCommit(downJob.OutputCommit)
		if err != nil {
			t.Fatalf("inspect downstream commit: %v", err)
		}
		if !containsStrList(downOut.Provenance, head.ID) || !containsStrList(downOut.Provenance, spec) {
			t.Fatalf("downstream provenance = %v, want the spout commit %s and the spec %s", downOut.Provenance, head.ID, spec)
		}
		// the spec commit's subvenants are exactly the spout's output and
		// the downstream output
		sc, err := c.InspectCommit(spec)
		if err != nil {
			t.Fatalf("inspect spec commit: %v", err)
		}
		if !containsStrList(sc.Subvenants, head.ID) || !containsStrList(sc.Subvenants, downJob.OutputCommit) {
			t.Fatalf("spec commit subvenants = %v, want the spout output %s and the downstream output %s", sc.Subvenants, head.ID, downJob.OutputCommit)
		}
	})
}

// containsStrList reports whether a string slice holds the needle.
func containsStrList(hay []string, needle string) bool {
	for _, s := range hay {
		if s == needle {
			return true
		}
	}
	return false
}
