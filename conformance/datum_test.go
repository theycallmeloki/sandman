// The datum engine's contract: jobs execute their input as per-datum units
// of work — parallelism, dedup, retries, reprocessing — while the output
// stays one commit per job (SB-004/006/007/011/073/103/134/166).
package conformance

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// TestSB004_ParallelPipelineProducesAllPerDatumOutputs — a pipeline with
// parallelism 4 processes 1000 per-datum inputs concurrently and produces
// exactly one output commit containing every file with correct content.
// The default entry point (no command) keeps this a pure scheduler test.
func TestSB004_ParallelPipelineProducesAllPerDatumOutputs(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	files := map[string]string{}
	for i := 0; i < 1000; i++ {
		files[fmt.Sprintf("file-%d", i)] = fmt.Sprintf("%d", i)
	}
	cm := commitFiles(t, repo, "master", files)

	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:        pipe,
		Transform:   &client.Transform{}, // default entry point: copy inputs to OUT
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		Parallelism: &client.Parallelism{Constant: 4},
	})

	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want exactly 1 output commit", len(jobs))
	}
	for i := 0; i < 1000; i++ {
		got, err := c.GetFile(jobs[0].OutputCommit, fmt.Sprintf("file-%d", i))
		if err != nil {
			t.Fatalf("read file-%d: %v", i, err)
		}
		if string(got) != fmt.Sprintf("%d", i) {
			t.Fatalf("file-%d content = %q, want %q (lost/corrupted across workers)", i, got, fmt.Sprintf("%d", i))
		}
	}
}

// TestSB006_UnchangedDatumsNotReprocessed — a datum processed once is not
// re-executed for an unchanged input revision: an empty commit flushes far
// faster than the 10s per-datum sleep, and the job records the datum as
// skipped (SB-006).
func TestSB006_UnchangedDatumsNotReprocessed(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm1 := commitFiles(t, repo, "master", map[string]string{"file": "foo"})

	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", fmt.Sprintf("sleep 10; cp -r ${%s}/* ${OUT}/", repo)},
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	})
	flushOK(t, cm1.ID) // the datum was processed once (takes >= 10s)

	cm2, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start empty commit: %v", err)
	}
	if _, err := c.FinishCommit(cm2.ID, "", false); err != nil {
		t.Fatalf("finish empty commit: %v", err)
	}

	start := time.Now()
	jobs, err := c.Flush(cm2.ID, 5*time.Second) // far shorter than the 10s sleep
	if err != nil {
		t.Fatalf("dedup flush did not complete within 5s (datum re-executed?): %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("dedup flush took %s, want < 5s", elapsed)
	}
	if len(jobs) != 1 || jobs[0].State != "success" {
		t.Fatalf("empty-commit job = %+v, want one success", jobs)
	}
	if jobs[0].Skipped != 1 || jobs[0].Processed != 0 {
		t.Fatalf("empty-commit job counters = processed %d skipped %d, want 0/1 (dedup)",
			jobs[0].Processed, jobs[0].Skipped)
	}
}

// TestSB007_InputDataModifications — replace, delete, and add propagate to
// the pipeline output: one output commit per input commit, a deleted file
// genuinely absent, a replacement carried (SB-007).
func TestSB007_InputDataModifications(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      pipe,
		Transform: copyTransform(repo),
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	})

	cm1 := commitFiles(t, repo, "master", map[string]string{"file": "foo"})
	jobs1 := flushOK(t, cm1.ID)
	if len(jobs1) != 1 {
		t.Fatalf("commit 1: %d jobs, want 1", len(jobs1))
	}
	if got, _ := c.GetFile(jobs1[0].OutputCommit, "file"); string(got) != "foo" {
		t.Fatalf("commit 1 output = %q, want foo", got)
	}

	// replacement: delete then re-add the same path in one commit counts as
	// a content replacement, not an append (FS-4 — a plain put would
	// append "bar" to "foo")
	cm2 := replaceCommit(t, repo, "master", map[string]string{"file": "bar"})
	jobs2 := flushOK(t, cm2.ID)
	if len(jobs2) != 1 {
		t.Fatalf("commit 2: %d jobs, want 1", len(jobs2))
	}
	if got, _ := c.GetFile(jobs2[0].OutputCommit, "file"); string(got) != "bar" {
		t.Fatalf("commit 2 output = %q, want bar", got)
	}

	// deletion + addition: file vanishes from the output, file2 appears
	cm3, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start commit 3: %v", err)
	}
	if err := c.DeleteFile(cm3.ID, "file"); err != nil {
		t.Fatalf("delete file: %v", err)
	}
	if err := c.PutFile(cm3.ID, "file2", []byte("foo")); err != nil {
		t.Fatalf("put file2: %v", err)
	}
	if _, err := c.FinishCommit(cm3.ID, "", false); err != nil {
		t.Fatalf("finish commit 3: %v", err)
	}
	jobs3 := flushOK(t, cm3.ID)
	if len(jobs3) != 1 {
		t.Fatalf("commit 3: %d jobs, want 1", len(jobs3))
	}
	out3 := jobs3[0].OutputCommit
	if _, err := c.GetFile(out3, "file"); err == nil {
		t.Fatalf("deleted file still readable from the output commit (absence is a tombstone, not empty content)")
	}
	if got, err := c.GetFile(out3, "file2"); err != nil || string(got) != "foo" {
		t.Fatalf("file2 = %q (err %v), want foo", got, err)
	}

	// one output commit per input commit, deletions included
	hist, err := c.CommitHistory(pipe, "master")
	if err != nil {
		t.Fatalf("output history: %v", err)
	}
	if len(hist) != 3 {
		t.Fatalf("output commits = %d, want 3 (one per input commit)", len(hist))
	}
}

// TestSB011_PipelineFailureMentionsDatum — a failing execution surfaces as
// a failed job whose reason names the datum that failed (SB-011).
func TestSB011_PipelineFailureMentionsDatum(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      pipe,
		Transform: &client.Transform{Image: "alpine:3.21", Cmd: []string{"sh", "-c", "exit 1"}},
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	})

	jobs, err := c.Flush(cm.ID, 60*time.Second)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected exactly 1 job, got %d", len(jobs))
	}
	j := jobs[0]
	if j.State != "failure" {
		t.Fatalf("job state = %s, want failure", j.State)
	}
	if !strings.Contains(j.Reason, "datum") {
		t.Fatalf("failure reason %q does not mention the datum", j.Reason)
	}
	if j.Failed != 1 {
		t.Fatalf("job failed datum count = %d, want 1", j.Failed)
	}
}

// TestSB073_LargeOutputAcrossParallelWorkers — 100 datums each producing
// 100 output files (10,000 total) at parallelism 4 complete in one output
// commit without loss (SB-073).
func TestSB073_LargeOutputAcrossParallelWorkers(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	files := map[string]string{}
	for i := 0; i < 100; i++ {
		files[fmt.Sprintf("file-%d", i)] = ""
	}
	cm := commitFiles(t, repo, "master", files)

	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", fmt.Sprintf("for i in $(seq 1 100); do touch ${OUT}/$(basename ${%s}/*)-$i; done", repo)},
		},
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		Parallelism: &client.Parallelism{Constant: 4},
	})

	jobs, err := c.Flush(cm.ID, 180*time.Second) // 100 datums x 100 files, 4 workers — slow under full-suite load
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want exactly 1 output commit", len(jobs))
	}
	infos, err := c.ListFiles(jobs[0].OutputCommit)
	if err != nil {
		t.Fatalf("list output files: %v", err)
	}
	if len(infos) != 10000 {
		t.Fatalf("output has %d files, want 10000", len(infos))
	}
}

// TestSB103_SlowDatumsParallelCompleteness — slow datums at parallelism 4
// all complete; every input file appears in the single output commit
// (SB-103).
func TestSB103_SlowDatumsParallelCompleteness(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	files := map[string]string{}
	for i := 0; i < 8; i++ {
		files[fmt.Sprintf("file-%d", i)] = "foo"
	}
	cm := commitFiles(t, repo, "master", files)

	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", fmt.Sprintf("sleep 2; cp -r ${%s}/* ${OUT}/", repo)},
		},
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		Parallelism: &client.Parallelism{Constant: 4},
	})

	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want 1", len(jobs))
	}
	for i := 0; i < 8; i++ {
		got, err := c.GetFile(jobs[0].OutputCommit, fmt.Sprintf("file-%d", i))
		if err != nil || string(got) != "foo" {
			t.Fatalf("file-%d = %q (err %v), want foo", i, got, err)
		}
	}
}

// TestSB134_DatumTries — a failing datum is retried exactly the configured
// number of times, one log entry per attempt (SB-134).
func TestSB134_DatumTries(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image:      "alpine:3.21",
			Cmd:        []string{"sh", "-c", "definitely-not-a-command-xyz"},
			DatumTries: 5,
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	})

	jobs, err := c.Flush(cm.ID, 60*time.Second)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected exactly 1 job, got %d", len(jobs))
	}
	if jobs[0].State != "failure" {
		t.Fatalf("job state = %s, want failure after retries exhausted", jobs[0].State)
	}
	lines, err := c.Logs(client.LogParams{Job: jobs[0].ID})
	if err != nil {
		t.Fatalf("job logs: %v", err)
	}
	markers := 0
	for _, l := range lines {
		if strings.Contains(l, "errored running user code") {
			markers++
		}
	}
	if markers != 5 {
		t.Fatalf("job log has %d failure markers, want exactly 5 (DatumTries 5)", markers)
	}
}

// TestSB166_ReprocessEveryJob — a pipeline configured to reprocess all of
// its datums re-executes every datum on every job: unchanged declared
// inputs do not skip, and each job's output reflects the data current at
// processing time (SB-166).
func TestSB166_ReprocessEveryJob(t *testing.T) {
	trigger := uniq(t) + "t"
	mustRepo(t, trigger)
	// The declared input's glob selects only the trigger-N datum paths; the
	// value file is data outside the datum set. The full input view is
	// mounted at /sandman/view/<input>, so a datum can read it (SB-166
	// clause 5: execution — not input-diffing — determines output).
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", fmt.Sprintf("cat /sandman/view/%s/value >> ${OUT}/file", trigger)},
		},
		Input:     &client.Input{Repo: trigger, Glob: "/trigger-*"},
		Reprocess: true,
	})

	// non-append-only: one datum, its declared input byte-identical across
	// jobs; only the data changes. Reprocess makes each job re-execute it,
	// so the output tracks the current value.
	for i := 0; i < 5; i++ {
		val := fmt.Sprintf("v%d\n", i)
		cm := overwriteCommit(t, trigger, "master", map[string]string{"value": val, "trigger-1": "x"})
		jobs := flushOK(t, cm.ID)
		if len(jobs) != 1 {
			t.Fatalf("iteration %d: %d jobs, want 1", i, len(jobs))
		}
		got, err := c.GetFile(jobs[0].OutputCommit, "file")
		if err != nil || string(got) != val {
			t.Fatalf("iteration %d output = %q (err %v), want %q (stale datum output?)", i, got, err, val)
		}
	}

	// append-only: the datum set grows one file per iteration; every datum
	// (old and new) is re-executed against the current data, so the output
	// holds the current value once per datum.
	for i := 1; i <= 5; i++ {
		val := fmt.Sprintf("v%d\n", i)
		files := map[string]string{"value": val}
		for j := 1; j <= i; j++ {
			files[fmt.Sprintf("trigger-%d", j)] = "x"
		}
		cm := overwriteCommit(t, trigger, "master", files)
		jobs := flushOK(t, cm.ID)
		if len(jobs) != 1 {
			t.Fatalf("append iteration %d: %d jobs, want 1", i, len(jobs))
		}
		got, err := c.GetFile(jobs[0].OutputCommit, "file")
		if err != nil {
			t.Fatalf("append iteration %d: read output: %v", i, err)
		}
		want := strings.Repeat(val, i)
		if string(got) != want {
			t.Fatalf("append iteration %d output = %q, want %q (all %d datums re-executed)", i, got, want, i)
		}
	}
}

// TestD13_ChangedTransformDoesNotReprocessUnchangedDatums — the dedup
// cache is keyed per pipeline by the datum's content hash, independent of
// the transform (D-13 isolating clause): changing the transform does NOT
// reprocess unchanged datums. A same-content commit after an update is
// skipped — the wave settles with no output commit. Reprocess is the
// explicit override (SB-166).
func TestD13_ChangedTransformDoesNotReprocessUnchangedDatums(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	tr := &client.Transform{Image: "alpine:3.21", Cmd: []string{"sh", "-c", "cp ${" + repo + "}/file ${OUT}/file"}}
	in := &client.Input{Repo: repo, Glob: "/*"}
	mustPipeline(t, client.Pipeline{Name: pipe, Transform: tr, Input: in})

	cm1 := commitFiles(t, repo, "master", map[string]string{"file": "same\n"})
	if jobs := flushOK(t, cm1.ID); len(jobs) != 1 {
		t.Fatalf("first wave = %d jobs, want 1", len(jobs))
	}

	// change the transform (same pipeline, update flag); the update must
	// not invalidate the dedup memory for unchanged content
	mustPipeline(t, client.Pipeline{
		Name: pipe, Transform: &client.Transform{Image: "alpine:3.21", Cmd: []string{"sh", "-c", "cp -r ${" + repo + "}/* ${OUT}/"}}, Input: in, Update: true,
	})

	// a new commit that does NOT touch "file": that datum's content is
	// byte-identical (append semantics: accumulated from cm1), so it is
	// skipped even under the new transform; the new datum runs
	cm2 := commitFiles(t, repo, "master", map[string]string{"file2": "x\n"})
	jobs := flushOK(t, cm2.ID)
	if len(jobs) != 1 {
		t.Fatalf("second wave = %d jobs, want 1", len(jobs))
	}
	if jobs[0].Skipped != 1 || jobs[0].Processed != 1 {
		t.Fatalf("wave after transform change: skipped=%d processed=%d, want 1/1 — the unchanged datum must be skipped (dedup is content-based, transform-independent), the new datum processed", jobs[0].Skipped, jobs[0].Processed)
	}
}
