// Size-based commit triggers: bytes committed to a watched
// branch accumulate durably; every completed threshold unit runs the
// pipeline on the accumulated data, and the accumulation branch is stable
// across pipeline updates.
package conformance

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

func TestSB160_SizeTriggers(t *testing.T) {
	data := uniq(t)
	mustRepo(t, data)
	p1 := uniq(t) // 1K trigger watching data@master
	mustPipeline(t, client.Pipeline{
		Name:      p1,
		Transform: &client.Transform{Image: "alpine:3.21"},
		Input:     &client.Input{Repo: data, Glob: "/*", Trigger: &client.Trigger{SizeBytes: 1000}},
	})
	p2 := uniq(t) // 2K trigger watching p1's output
	mustPipeline(t, client.Pipeline{
		Name:      p2,
		Transform: &client.Transform{Image: "alpine:3.21"},
		Input:     &client.Input{Repo: p1, Glob: "/*", Trigger: &client.Trigger{SizeBytes: 2000}},
	})

	writeBatch := func(prefix string, n int) client.Commit {
		t.Helper()
		cm, err := c.StartCommit(data, "master", "")
		if err != nil {
			t.Fatalf("start commit: %v", err)
		}
		for i := 0; i < n; i++ {
			if err := c.PutFile(cm.ID, fmt.Sprintf("%s-%02d", prefix, i), []byte(strings.Repeat("x", 100))); err != nil {
				t.Fatalf("put file: %v", err)
			}
		}
		fin, err := c.FinishCommit(cm.ID, "", false)
		if err != nil {
			t.Fatalf("finish commit: %v", err)
		}
		return fin
	}
	// the trigger's latest fire commit on its accumulation branch
	triggerHead := func(repo, pipeline string) client.Commit {
		t.Helper()
		var head client.Commit
		pollFor(t, fmt.Sprintf("trigger commit on %s@%s-0", repo, pipeline), 60*time.Second, func() bool {
			h, err := c.HeadCommit(repo, pipeline+"-0")
			if err != nil {
				return false
			}
			head = h
			return true
		})
		return head
	}
	jobCount := func(pipeline string) int {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipeline})
		if err != nil {
			t.Fatalf("list %s jobs: %v", pipeline, err)
		}
		return len(js)
	}
	outFileCount := func(j client.Job) int {
		fs, err := c.ListFiles(j.OutputCommit)
		if err != nil {
			t.Fatalf("list files: %v", err)
		}
		return len(fs)
	}

	// batch 1: 10 x 100 bytes = 1K — pipeline1 fires once, pipeline2 stays
	// idle (sub-threshold)
	writeBatch("b1", 10)
	tr := triggerHead(data, p1)
	jobs1 := flushOK(t, tr.ID)
	if len(jobs1) != 1 {
		t.Fatalf("batch 1: %d jobs, want 1 (pipeline1 only)", len(jobs1))
	}
	if jobCount(p1) != 1 || jobCount(p2) != 0 {
		t.Fatalf("batch 1: p1=%d p2=%d jobs, want 1 and 0", jobCount(p1), jobCount(p2))
	}
	if n := outFileCount(jobs1[0]); n != 10 {
		t.Fatalf("batch 1: pipeline1 output has %d files, want all 10", n)
	}

	// batch 2: another 1K — pipeline1 fires again; pipeline2's cumulative
	// 2K fires it with all 20 files
	writeBatch("b2", 10)
	tr2 := triggerHead(data, p1)
	jobs2 := flushOK(t, tr2.ID)
	if jobCount(p1) != 2 || jobCount(p2) != 1 {
		t.Fatalf("batch 2: p1=%d p2=%d jobs, want 2 and 1", jobCount(p1), jobCount(p2))
	}
	var p2job client.Job
	for _, j := range jobs2 {
		if j.Pipeline == p2 {
			p2job = j
		}
	}
	if p2job.ID == "" {
		// the p2 job may be a later trigger; find it directly
		js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: p2})
		if len(js) == 0 {
			t.Fatalf("no pipeline2 job after batch 2")
		}
		p2job = js[len(js)-1]
	}
	if n := outFileCount(p2job); n != 20 {
		t.Fatalf("batch 2: pipeline2 output has %d files, want all 20 accumulated", n)
	}

	// update pipeline2's trigger to 3K: the update alone produces a new
	// output commit, and the accumulation branch is reused (the data
	// repo keeps exactly 2 branches)
	update := client.Pipeline{
		Name:      p2,
		Transform: &client.Transform{Image: "alpine:3.21"},
		Input:     &client.Input{Repo: p1, Glob: "/*", Trigger: &client.Trigger{SizeBytes: 3000}},
		Update:    true,
	}
	if err := c.CreatePipeline(update); err != nil {
		t.Fatalf("update p2: %v", err)
	}
	pollFor(t, "p2 update reprocessing job", 60*time.Second, func() bool {
		return jobCount(p2) >= 2
	})
	if jobCount(p2) != 2 {
		t.Fatalf("after update: p2 has %d jobs, want 2 (reprocessing included)", jobCount(p2))
	}
	refsDir := filepath.Join(daemonStateDir, "repos", data, "refs")
	entries, err := os.ReadDir(refsDir)
	if err != nil {
		t.Fatalf("read refs: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("data repo has %d branches, want 2 (master + the stable trigger branch)", len(entries))
	}

	// batch 3: 30 x 100 bytes = 3K — pipeline1 fires three times (one per
	// threshold unit); pipeline2's cumulative 3K fires once more
	writeBatch("b3", 30)
	tr3 := triggerHead(data, p1)
	flushOK(t, tr3.ID)
	pollFor(t, "all trigger jobs settled", 90*time.Second, func() bool {
		return jobCount(p1) >= 5 && jobCount(p2) >= 3
	})
	if jobCount(p1) != 5 {
		t.Fatalf("batch 3: p1 has %d jobs, want 5 (1 + 1 + 3 firings)", jobCount(p1))
	}
	if jobCount(p2) != 3 {
		t.Fatalf("batch 3: p2 has %d jobs, want 3", jobCount(p2))
	}
	// the flush of the final trigger commit reports the last of pipeline1;
	// pipeline2's last run is reached through its own trigger commit (the
	// trigger commits are the intermediaries, so a single flush cannot
	// walk through them)
	last := flushOK(t, tr3.ID)
	if len(last) != 1 {
		t.Fatalf("final p1 flush = %d jobs, want 1 (the last firing)", len(last))
	}
	p2tr := triggerHead(p1, p2)
	last2 := flushOK(t, p2tr.ID)
	if len(last2) != 1 {
		t.Fatalf("final p2 flush = %d jobs, want 1 (the last firing)", len(last2))
	}
}

// SB-160 — the trigger accumulation ledger is durable across a control-
// plane restart: 500B before the restart plus 500B after crosses the 1K
// threshold exactly once. A lost ledger would leave the second batch at
// 500B and never fire.
func TestSB160_SizeTriggerLedgerSurvivesRestart(t *testing.T) {
	data := uniq(t)
	mustRepo(t, data)
	p1 := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      p1,
		Transform: &client.Transform{Image: "alpine:3.21"},
		Input:     &client.Input{Repo: data, Glob: "/*", Trigger: &client.Trigger{SizeBytes: 1000}},
	})

	writeBatch := func(prefix string, n int) {
		t.Helper()
		cm, err := c.StartCommit(data, "master", "")
		if err != nil {
			t.Fatalf("start commit: %v", err)
		}
		for i := 0; i < n; i++ {
			if err := c.PutFile(cm.ID, fmt.Sprintf("%s-%02d", prefix, i), []byte(strings.Repeat("x", 100))); err != nil {
				t.Fatalf("put file: %v", err)
			}
		}
		if _, err := c.FinishCommit(cm.ID, "", false); err != nil {
			t.Fatalf("finish commit: %v", err)
		}
	}
	jobCount := func() int {
		js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: p1})
		if err != nil {
			t.Fatalf("list %s jobs: %v", p1, err)
		}
		return len(js)
	}

	// 500B: below the 1K threshold, no fire — but the ledger is written
	writeBatch("a", 5)
	if n := jobCount(); n != 0 {
		t.Fatalf("jobs after 500B = %d, want 0 (threshold not reached)", n)
	}

	restartDaemon(t)

	// another 500B after the restart: cumulative 1K fires exactly once
	writeBatch("b", 5)
	pollFor(t, "trigger fired after restart", 60*time.Second, func() bool {
		return jobCount() == 1
	})
	if n := jobCount(); n != 1 {
		t.Fatalf("jobs after 500B+restart+500B = %d, want exactly 1 (ledger survived)", n)
	}

	// the fired job completes and its output is the accumulated view
	var head client.Commit
	pollFor(t, "trigger branch head", 30*time.Second, func() bool {
		h, err := c.HeadCommit(data, p1+"-0")
		if err != nil {
			return false
		}
		head = h
		return true
	})
	jobs := flushOK(t, head.ID)
	if len(jobs) != 1 || jobs[0].State != "success" {
		t.Fatalf("triggered job = %+v, want one success", jobs)
	}
	fs, err := c.ListFiles(jobs[0].OutputCommit)
	if err != nil || len(fs) != 10 {
		t.Fatalf("triggered output has %d files (err %v), want 10 (both batches)", len(fs), err)
	}
}
