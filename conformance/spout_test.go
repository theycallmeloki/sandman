// Spout pipelines (SB-139): a pipeline with no input whose transform runs
// in the background, with the daemon committing each data-bearing cycle to
// the output branch, a marker directory to a separate marker branch, and
// spout/input and marker-name validation.
package conformance

import (
	"strconv"
	"testing"
	"time"

	"sandman/client"
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

func TestSB139_SpoutPipelines(t *testing.T) {
	t.Run("spout accumulates commits", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: spoutCmd(100, 5, false)},
			Spout:     &client.Spout{},
		})
		ch := waitSpoutCommits(t, pipe, 5)
		// the latest commit holds the file at its final, grown size
		last := ch[len(ch)-1]
		b, err := c.GetFile(last.ID, "file")
		if err != nil {
			t.Fatalf("read spout file: %v", err)
		}
		if len(b) != 500 {
			t.Fatalf("final file = %d bytes, want 500 (grown across cycles)", len(b))
		}
		// the job settles success when the container's loop ends
		js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		pollFor(t, "spout job settled", 60*time.Second, func() bool {
			if len(js) == 0 {
				return false
			}
			j, err := c.InspectJob(js[0].ID)
			return err == nil && j.State != "running"
		})
		j, _ := c.InspectJob(js[0].ID)
		if j.State != "success" {
			t.Fatalf("spout job state = %s (reason %q), want success", j.State, j.Reason)
		}
	})

	t.Run("overwrite keeps size constant", func(t *testing.T) {
		pipe := uniq(t)
		// every cycle writes the same 100 bytes
		script := "for i in $(seq 1 5); do yes $i | head -c 100 > ${OUT}/file; sleep 1; done"
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", script}},
			Spout:     &client.Spout{Overwrite: true},
		})
		ch := waitSpoutCommits(t, pipe, 5)
		b, err := c.GetFile(ch[len(ch)-1].ID, "file")
		if err != nil {
			t.Fatalf("read spout file: %v", err)
		}
		if len(b) != 100 {
			t.Fatalf("overwrite file = %d bytes, want a constant 100", len(b))
		}
	})

	t.Run("deleting the head does not stop the spout", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: spoutCmd(100, 8, false)},
			Spout:     &client.Spout{},
		})
		ch := waitSpoutCommits(t, pipe, 3)
		// delete the head commit of the spout's branch
		if err := c.DeleteCommit(ch[len(ch)-1].ID); err != nil {
			t.Fatalf("delete spout head: %v", err)
		}
		// the spout keeps producing: more commits appear after the delete
		pollFor(t, "more spout commits after deletion", 120*time.Second, func() bool {
			got, err := c.CommitHistory(pipe, "master")
			return err == nil && len(got) > 2
		})
		// once the spout's cycles finish, the branch holds 8 minus the
		// deleted head
		js0, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		if len(js0) > 0 {
			pollFor(t, "spout job settled", 120*time.Second, func() bool {
				j, err := c.InspectJob(js0[0].ID)
				return err == nil && j.State != "running"
			})
		}
		after, _ := c.CommitHistory(pipe, "master")
		if len(after) != 7 {
			t.Fatalf("spout history has %d commits after deletion, want 7 (8 cycles minus the deleted head)", len(after))
		}
	})

	t.Run("downstream consumes the spout", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: spoutCmd(100, 5, false)},
			Spout:     &client.Spout{},
		})
		waitSpoutCommits(t, pipe, 5)
		js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		if len(js) == 0 {
			t.Fatalf("no spout job")
		}
		pollFor(t, "spout job settled", 60*time.Second, func() bool {
			j, err := c.InspectJob(js[0].ID)
			return err == nil && j.State != "running"
		})
		// attach a downstream pipeline: exactly one job against the head
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
		b, err := c.GetFile(downJob.OutputCommit, "file")
		if err != nil || len(b) != 500 {
			t.Fatalf("downstream file = %d bytes (%v), want the spout's 500", len(b), err)
		}
	})

	t.Run("marker branch", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: spoutCmd(100, 5, true)},
			Spout:     &client.Spout{Marker: "markers"},
		})
		waitSpoutCommits(t, pipe, 5)
		pollFor(t, "marker commits", 60*time.Second, func() bool {
			ch, err := c.CommitHistory(pipe, "markers")
			return err == nil && len(ch) >= 5
		})
		mh, err := c.HeadCommit(pipe, "markers")
		if err != nil {
			t.Fatalf("marker head: %v", err)
		}
		b, err := c.GetFile(mh.ID, "marker")
		if err != nil {
			t.Fatalf("read marker: %v", err)
		}
		if string(b) != "m5\n" {
			t.Fatalf("marker = %q, want the latest marker content", string(b))
		}
	})

	t.Run("input and marker validation", func(t *testing.T) {
		repo := uniq(t)
		mustRepo(t, repo)
		bad := client.Pipeline{
			Name:      uniq(t),
			Transform: &client.Transform{Image: "alpine"},
			Input:     &client.Input{Repo: repo, Glob: "/*"},
			Spout:     &client.Spout{},
		}
		if err := c.CreatePipeline(bad); err == nil {
			t.Fatalf("a spout with an input must be rejected")
		} else if !containsStr(err.Error(), "cannot have inputs") {
			t.Fatalf("spout-with-input error = %q", err.Error())
		}
		badMarker := client.Pipeline{
			Name:      uniq(t),
			Transform: &client.Transform{Image: "alpine"},
			Spout:     &client.Spout{Marker: "bad*name"},
		}
		if err := c.CreatePipeline(badMarker); err == nil {
			t.Fatalf("a spout with a glob-metacharacter marker must be rejected")
		} else if !containsStr(err.Error(), "marker") {
			t.Fatalf("marker validation error = %q", err.Error())
		}
	})
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
