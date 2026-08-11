// Cron inputs: scheduled ticks commit time-stamped files that trigger the
// pipeline, overwrite mode replaces instead of accumulating, crosses with
// regular inputs work, and manual triggers create ticks immediately
// (SB-089, SB-133).
package conformance

import (
	"fmt"
	"testing"
	"time"

	"sandman/client"
)

func TestSB089_CronInputs(t *testing.T) {
	t.Run("scheduled ticks accumulate", func(t *testing.T) {
		pipe, down := uniq(t), uniq(t)
		mustPipeline(t, client.Pipeline{
			Name: pipe,
			Transform: &client.Transform{
				Image: "alpine",
				Cmd:   []string{"sh", "-c", "cp -r ${cron}/* ${OUT}/"},
			},
			Input: &client.Input{Name: "cron", Cron: "@every 2s"},
		})
		mustPipeline(t, client.Pipeline{
			Name:      down,
			Transform: &client.Transform{Image: "alpine"},
			Input:     &client.Input{Repo: pipe, Glob: "/*"},
		})
		cronRepo := pipe + "-cron"
		// wait for two ticks, then flush each through both stages
		first := waitCronTicks(t, cronRepo, 2, 60*time.Second)
		jobs1 := flushOK(t, first.ID)
		if len(jobs1) != 2 {
			t.Fatalf("after 2 ticks: %d jobs, want 2 (pipeline + downstream)", len(jobs1))
		}
		outFiles := func(j client.Job) int {
			fs, err := c.ListFiles(j.OutputCommit)
			if err != nil {
				t.Fatalf("list files: %v", err)
			}
			return len(fs)
		}
		var pipeJob client.Job
		for _, j := range jobs1 {
			if j.Pipeline == pipe {
				pipeJob = j
			}
		}
		if n := outFiles(pipeJob); n != 2 {
			t.Fatalf("pipeline output has %d files after 2 ticks, want 2 (cumulative)", n)
		}
	})

	t.Run("overwrite keeps one file", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name: pipe,
			Transform: &client.Transform{
				Image: "alpine",
				Cmd:   []string{"sh", "-c", "cp -r ${cron}/* ${OUT}/"},
			},
			Input: &client.Input{Name: "cron", Cron: "@every 2s", Overwrite: true},
		})
		first := waitCronTicks(t, pipe+"-cron", 3, 90*time.Second)
		jobs := flushOK(t, first.ID)
		var pipeJob client.Job
		for _, j := range jobs {
			if j.Pipeline == pipe {
				pipeJob = j
			}
		}
		fs, err := c.ListFiles(pipeJob.OutputCommit)
		if err != nil {
			t.Fatalf("list files: %v", err)
		}
		if len(fs) != 1 {
			t.Fatalf("overwrite output has %d files, want exactly 1", len(fs))
		}
	})

	t.Run("cross of cron and regular input", func(t *testing.T) {
		repo := uniq(t)
		mustRepo(t, repo)
		cm := commitFiles(t, repo, "master", map[string]string{"data": "x"})
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name: pipe,
			Transform: &client.Transform{
				Image: "alpine",
				Cmd:   []string{"sh", "-c", "cat ${cron}/* ${data}/* > ${OUT}/out"},
			},
			Input: &client.Input{Cross: []client.Input{
				{Name: "cron", Cron: "@every 2s"},
				{Name: "data", Repo: repo, Glob: "/*"},
			}},
		})
		first := waitCronTicks(t, pipe+"-cron", 1, 60*time.Second)
		jobs := flushSetOK(t, []string{first.ID, cm.ID})
		if len(jobs) != 1 {
			t.Fatalf("cron x regular: %d jobs, want exactly 1 output commit", len(jobs))
		}
		if b, err := c.GetFile(jobs[0].OutputCommit, "out"); err != nil {
			t.Fatalf("read output: %v", err)
		} else if len(b) == 0 {
			t.Fatalf("cron x regular output is empty")
		}
	})

	t.Run("manual triggers create ticks", func(t *testing.T) {
		pipe, down := uniq(t), uniq(t)
		mustPipeline(t, client.Pipeline{
			Name: pipe,
			Transform: &client.Transform{
				Image: "alpine",
				Cmd:   []string{"sh", "-c", "cp -r ${cron}/* ${OUT}/"},
			},
			Input: &client.Input{Name: "cron", Cron: "@every 1h"},
		})
		mustPipeline(t, client.Pipeline{
			Name:      down,
			Transform: &client.Transform{Image: "alpine"},
			Input:     &client.Input{Repo: pipe, Glob: "/*"},
		})
		for i := 0; i < 3; i++ {
			if err := c.TriggerCron(pipe); err != nil {
				t.Fatalf("trigger %d: %v", i, err)
			}
			time.Sleep(2 * time.Second) // distinct tick seconds (SB-089 clause 5)
		}
		first := waitCronTicks(t, pipe+"-cron", 3, 60*time.Second)
		jobs := flushOK(t, first.ID)
		if len(jobs) != 2 {
			t.Fatalf("after 3 manual triggers: %d jobs, want 2 (pipeline + downstream)", len(jobs))
		}
	})

	t.Run("manual trigger on two cron inputs", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name: pipe,
			Transform: &client.Transform{
				Image: "alpine",
				Cmd:   []string{"sh", "-c", "cat ${c1}/* ${c2}/* > ${OUT}/out"},
			},
			Input: &client.Input{Cross: []client.Input{
				{Name: "c1", Cron: "@every 1h"},
				{Name: "c2", Cron: "@every 1h"},
			}},
		})
		for i := 0; i < 3; i++ {
			if err := c.TriggerCron(pipe); err != nil {
				t.Fatalf("trigger %d: %v", i, err)
			}
			time.Sleep(2 * time.Second)
		}
		// one trigger ticked BOTH cron inputs (their repos both advance)
		waitCronTicks(t, pipe+"-c1", 3, 60*time.Second)
		waitCronTicks(t, pipe+"-c2", 3, 60*time.Second)
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		if err != nil {
			t.Fatalf("list jobs: %v", err)
		}
		if len(js) < 3 {
			t.Fatalf("two-cron pipeline has %d jobs, want at least 3", len(js))
		}
	})
}

// waitCronTicks waits until the cron repository's branch holds at least n
// commits and returns the newest one.
func waitCronTicks(t *testing.T, cronRepo string, n int, timeout time.Duration) client.Commit {
	t.Helper()
	var newest client.Commit
	pollFor(t, fmt.Sprintf("%d cron ticks on %s", n, cronRepo), timeout, func() bool {
		ch, err := c.CommitHistory(cronRepo, "master")
		if err != nil {
			return false
		}
		if len(ch) >= n {
			newest = ch[len(ch)-1]
			return true
		}
		return false
	})
	return newest
}

// TestSB133_CronCadenceSurvivesUpdates — a cron pipeline's cadence
// survives rapid spec updates: the ticks keep their interval, with no
// bursts and no stalls. Scaled from the reference's 5-minute window to
// three ticks before and three after a burst of 20 updates.
func TestSB133_CronCadenceSurvivesUpdates(t *testing.T) {
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", "cp -r ${cron}/* ${OUT}/"},
		},
		Input: &client.Input{Name: "cron", Cron: "@every 30s", Overwrite: true},
	})

	cronTicks := func() []time.Time {
		ch, err := c.CommitHistory(pipe+"-cron", "master")
		if err != nil {
			return nil
		}
		var out []time.Time
		for _, cm := range ch {
			if st, err := time.Parse(time.RFC3339Nano, cm.CreatedAt); err == nil {
				out = append(out, st)
			}
		}
		return out
	}
	checkGaps := func(label string, ticks []time.Time) {
		t.Helper()
		if len(ticks) < 3 {
			t.Fatalf("%s: only %d ticks observed, want at least 3", label, len(ticks))
		}
		for i := 1; i < len(ticks); i++ {
			gap := ticks[i].Sub(ticks[i-1])
			if gap < 15*time.Second || gap > 45*time.Second {
				t.Fatalf("%s: consecutive ticks %v apart (want 15-45s): %v -> %v", label, gap, ticks[i-1], ticks[i])
			}
		}
	}
	cronJobs := func() int {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		if err != nil {
			t.Fatalf("list jobs: %v", err)
		}
		return len(js)
	}

	// three ticks at 30s cadence, each triggering a job
	waitCronTicks(t, pipe+"-cron", 3, 150*time.Second)
	pollFor(t, "three cron jobs settled", 90*time.Second, func() bool {
		return cronJobs() >= 3
	})
	checkGaps("before updates", cronTicks())

	// a burst of 20 updates must not restart the clock, stall it, or
	// double-schedule a tick
	update := client.Pipeline{
		Name:      pipe,
		Transform: &client.Transform{Image: "alpine"},
		Input:     &client.Input{Name: "cron", Cron: "@every 30s", Overwrite: true},
		Update:    true,
		Reprocess: true,
	}
	for i := 0; i < 20; i++ {
		if err := c.CreatePipeline(update); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}
	before := len(cronTicks())
	waitCronTicks(t, pipe+"-cron", before+3, 150*time.Second)
	checkGaps("after updates", cronTicks())
	// the post-update ticks still trigger jobs (the cadence carries on)
	pollFor(t, "jobs after the update burst", 90*time.Second, func() bool {
		return cronJobs() >= before+3
	})
}
