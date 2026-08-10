// Scheduling knobs and worker visibility: chunking changes only internal
// granularity (SB-102), the per-worker queue bound holds under load
// (SB-097), autoscaling ramps to the datum count capped at the parallelism
// (SB-165), and worker status / datum restart make execution observable
// and steerable (SB-064/065).
package conformance

import (
	"fmt"
	"testing"
	"time"

	"sandman/client"
)

// TestSB102_ChunkSpecOutputComplete — chunking configures only scheduling
// granularity: whatever the chunk target, the output commit contains every
// input file with identical content (SB-102).
func TestSB102_ChunkSpecOutputComplete(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	files := map[string]string{}
	for i := 0; i < 101; i++ {
		files[fmt.Sprintf("file-%d", i)] = "foo"
	}
	cm := commitFiles(t, repo, "master", files)

	for _, variant := range []struct {
		name string
		spec *client.ChunkSpec
	}{
		{"one-datum", &client.ChunkSpec{Number: 1}},
		{"five-bytes", &client.ChunkSpec{SizeBytes: 5}},
	} {
		t.Run(variant.name, func(t *testing.T) {
			pipe := uniq(t)
			mustPipeline(t, client.Pipeline{
				Name:      pipe,
				Transform: copyTransform(repo),
				Input:     &client.Input{Repo: repo, Glob: "/*"},
				ChunkSpec: variant.spec,
			})
			jobs, err := c.Flush(cm.ID, 180*time.Second)
			if err != nil {
				t.Fatalf("flush: %v", err)
			}
			for _, j := range jobs {
				if j.State != "success" {
					t.Fatalf("job %s (%s) state = %s, want success", j.ID, j.Pipeline, j.State)
				}
			}
			// both variants share the input commit; the flush reports every
			// pipeline consuming it, so pick this variant's job
			var myJob *client.Job
			for i := range jobs {
				if jobs[i].Pipeline == pipe {
					myJob = &jobs[i]
					break
				}
			}
			if myJob == nil {
				t.Fatalf("flush jobs %+v did not include the variant pipeline", jobs)
			}
			for i := 0; i < 101; i++ {
				got, err := c.GetFile(myJob.OutputCommit, fmt.Sprintf("file-%d", i))
				if err != nil || string(got) != "foo" {
					t.Fatalf("file-%d = %q (err %v), want foo (no file split or loss)", i, got, err)
				}
			}
		})
	}
}

// TestSB097_MaxQueueSize — no worker's pending-datum queue exceeds the
// configured bound, even while workers are busy with slow datums; the job
// still completes (SB-097).
func TestSB097_MaxQueueSize(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	files := map[string]string{}
	for i := 0; i < 10; i++ {
		files[fmt.Sprintf("file-%d", i)] = fmt.Sprintf("%d", i)
	}
	cm := commitFiles(t, repo, "master", files)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", fmt.Sprintf("sleep 5; cp -r ${%s}/* ${OUT}/", repo)},
		},
		Input:        &client.Input{Repo: repo, Glob: "/*"},
		Parallelism:  &client.Parallelism{Constant: 2},
		MaxQueueSize: 1,
	})

	job := waitJobFor(t, pipe, 30*time.Second)
	pollFor(t, "job running", 30*time.Second, func() bool {
		j, err := c.InspectJob(job.ID)
		return err == nil && j.State == "running"
	})
	// spot-check the worker queues while the job is in flight
	for i := 0; i < 10; i++ {
		j, err := c.InspectJob(job.ID)
		if err != nil {
			t.Fatalf("inspect job: %v", err)
		}
		if len(j.Workers) != 2 {
			t.Fatalf("sample %d: %d worker status entries, want exactly 2", i, len(j.Workers))
		}
		for _, w := range j.Workers {
			if w.Queue > 1 {
				t.Fatalf("sample %d: worker %d queue = %d, want at most 1", i, w.Worker, w.Queue)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	flushOK(t, cm.ID)
}

// TestSB165_AutoscalingRampsToDatumCount — the execution scale ramps to
// the number of datums, capped at the configured parallelism, and never
// exceeds the datum count (SB-165; the ramp is instant — workers are
// sized upfront — so a short transform suffices to observe it).
func TestSB165_AutoscalingRampsToDatumCount(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", fmt.Sprintf("sleep 5; cp -r ${%s}/* ${OUT}/", repo)},
		},
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		Parallelism: &client.Parallelism{Constant: 4}, // the autoscaling cap
		Autoscaling: true,
	})

	for _, tc := range []struct {
		files int
		want  int
	}{
		{1, 1}, // one datum: one worker, never more
		{3, 3},
		{8, 4}, // capped at the configured parallelism
	} {
		files := map[string]string{}
		for i := 0; i < tc.files; i++ {
			files[fmt.Sprintf("f-%d-%d", tc.files, i)] = "x"
		}
		cm := commitFiles(t, repo, "master", files)
		var job client.Job
		pollFor(t, "job for the commit", 30*time.Second, func() bool {
			js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe, InputCommits: []string{cm.ID}})
			if err != nil || len(js) == 0 {
				return false
			}
			job = js[0]
			return true
		})
		pollFor(t, "job running with the scaled worker count", 30*time.Second, func() bool {
			j, err := c.InspectJob(job.ID)
			return err == nil && j.State == "running" && len(j.Workers) == tc.want
		})
		j, err := c.InspectJob(job.ID)
		if err != nil {
			t.Fatalf("inspect job: %v", err)
		}
		if len(j.Workers) != tc.want {
			t.Fatalf("%d datums: %d workers, want %d (min of datum count and cap)", tc.files, len(j.Workers), tc.want)
		}
		flushOK(t, cm.ID)
	}
}

// TestSB064_DatumStatusRestart — restarting a datum aborts its current
// processing and starts it over with a fresh, later start time; the job
// still completes with exactly one output commit (SB-064).
func TestSB064_DatumStatusRestart(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file1": "a", "file2": "b"})
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", "sleep 20"},
		},
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		Parallelism: &client.Parallelism{Constant: 2},
		EnableStats: true,
	})

	job := waitJobFor(t, pipe, 30*time.Second)
	pollFor(t, "both datums running", 90*time.Second, func() bool {
		j, err := c.InspectJob(job.ID)
		if err != nil || j.State != "running" {
			return false
		}
		pg, err := c.ListDatums(job.ID, 0, 0)
		if err != nil {
			return false
		}
		return len(pg.Datums) == 2 && pg.Datums[0].State == "running" && pg.Datums[1].State == "running"
	})
	pg, err := c.ListDatums(job.ID, 0, 0)
	if err != nil {
		t.Fatalf("list datums: %v", err)
	}
	first := pg.Datums[0]
	second := pg.Datums[1]
	if first.State != "running" || second.State != "running" {
		t.Fatalf("datums not both running: %s / %s", first.State, second.State)
	}
	// (the two concurrent pick-ups are unordered — the contractual
	// progress signal is the restart's later start time, asserted next)

	// restart the later datum: it must re-enter processing with a newer
	// start time
	restarted := second.ID
	if err := c.RestartDatum(job.ID, restarted); err != nil {
		t.Fatalf("restart datum: %v", err)
	}
	pollFor(t, "restarted datum running with a later start", 90*time.Second, func() bool {
		pg, err := c.ListDatums(job.ID, 0, 0)
		if err != nil {
			return false
		}
		for _, dt := range pg.Datums {
			if dt.ID == restarted && dt.State == "running" && dt.Started > second.Started {
				return true
			}
		}
		return false
	})

	// the job still completes with exactly one output commit
	jobs, err := c.Flush(cm.ID, 120*time.Second)
	if err != nil {
		t.Fatalf("flush after restart: %v", err)
	}
	if len(jobs) != 1 || jobs[0].State != "success" {
		t.Fatalf("job after restart = %+v, want one success", jobs)
	}
}

// TestSB065_UseMultipleWorkers — a parallelism-2 job reports exactly two
// worker status entries while it runs (SB-065).
func TestSB065_UseMultipleWorkers(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	files := map[string]string{}
	for i := 0; i < 4; i++ {
		files[fmt.Sprintf("file-%d", i)] = fmt.Sprintf("%d", i)
	}
	cm := commitFiles(t, repo, "master", files)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", fmt.Sprintf("sleep 3; cp -r ${%s}/* ${OUT}/", repo)},
		},
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		Parallelism: &client.Parallelism{Constant: 2},
	})

	job := waitJobFor(t, pipe, 30*time.Second)
	pollFor(t, "job running with 2 workers", 30*time.Second, func() bool {
		j, err := c.InspectJob(job.ID)
		return err == nil && j.State == "running" && len(j.Workers) == 2
	})
	flushOK(t, cm.ID)
}
