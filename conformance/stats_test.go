// Per-datum statistics: the enable flag (one-way), live and paginated
// datum listings with state ordering, records across jobs, the stats
// branch, and the timeout records that depend on stats (SB-080..086,
// SB-113/114).
package conformance

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// statsCopy is a copy transform for a repo, with an optional per-datum
// sleep to make the job observable mid-flight.
func statsCopy(repo string, sleep string) *client.Transform {
	if sleep == "" {
		return copyTransform(repo)
	}
	return &client.Transform{
		Image: "alpine",
		Cmd:   []string{"sh", "-c", fmt.Sprintf("sleep %s; cp -r ${%s}/* ${OUT}/", sleep, repo)},
	}
}

// TestSB080_PerDatumStatistics — a stats-enabled pipeline records one
// datum per glob match; datums are listable during execution and after,
// with pagination metadata (SB-080).
func TestSB080_PerDatumStatistics(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	files := map[string]string{}
	for i := 0; i < 10; i++ {
		files[fmt.Sprintf("file-%d", i)] = fmt.Sprintf("%d", i)
	}
	cm := commitFiles(t, repo, "master", files)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:        pipe,
		Transform:   statsCopy(repo, "0.5"),
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		Parallelism: &client.Parallelism{Constant: 4},
		EnableStats: true,
	})

	// wait for the job to be running and all 10 datums to appear with
	// their input files (the listing is complete during execution —
	// queued datums included — once the placeholder records are durable)
	job := waitJobFor(t, pipe, 30*time.Second)
	pollFor(t, "10 datums with input files mid-flight", 30*time.Second, func() bool {
		pg, err := c.ListDatums(job.ID, 0, 0)
		if err != nil || len(pg.Datums) != 10 {
			return false
		}
		for _, dt := range pg.Datums {
			if len(dt.InputFiles) != 1 {
				return false
			}
		}
		return true
	})
	pg, err := c.ListDatums(job.ID, 0, 0)
	if err != nil {
		t.Fatalf("list datums during execution: %v", err)
	}
	if len(pg.Datums) != 10 {
		t.Fatalf("mid-flight listing has %d datums, want 10", len(pg.Datums))
	}
	for _, dt := range pg.Datums {
		if len(dt.InputFiles) != 1 {
			t.Fatalf("datum %s carries %d input files, want 1", dt.ID, len(dt.InputFiles))
		}
	}

	// pagination: page size 5 -> 5 datums, 2 total pages, page index 0
	pg, err = c.ListDatums(job.ID, 5, 0)
	if err != nil {
		t.Fatalf("paginated listing: %v", err)
	}
	if len(pg.Datums) != 5 || pg.TotalPages != 2 || pg.Page != 0 {
		t.Fatalf("page = %d datums, %d pages, index %d; want 5/2/0", len(pg.Datums), pg.TotalPages, pg.Page)
	}

	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want 1", len(jobs))
	}
	pg, err = c.ListDatums(job.ID, 0, 0)
	if err != nil {
		t.Fatalf("list datums after completion: %v", err)
	}
	if len(pg.Datums) != 10 {
		t.Fatalf("post-completion listing has %d datums, want 10", len(pg.Datums))
	}
	for _, dt := range pg.Datums {
		if dt.State != "success" {
			t.Fatalf("datum %s state = %s, want success", dt.ID, dt.State)
		}
	}
	insp, err := c.InspectDatum(job.ID, pg.Datums[0].ID)
	if err != nil || insp.State != "success" {
		t.Fatalf("inspect datum: state %q err %v, want success", insp.State, err)
	}
}

// TestSB081_StatsCanBeEnabledNotDisabled — statistics can be toggled on by
// an update, never off; a no-stats job's datums list by identity but do
// not inspect (SB-081).
func TestSB081_StatsCanBeEnabledNotDisabled(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	in := &client.Input{Repo: repo, Glob: "/*"}
	mustPipeline(t, client.Pipeline{Name: pipe, Transform: copyTransform(repo), Input: in})

	first := map[string]string{}
	for i := 0; i < 5; i++ {
		first[fmt.Sprintf("old-%d", i)] = "x"
	}
	cm1 := commitFiles(t, repo, "master", first)
	jobs1 := flushOK(t, cm1.ID)
	job1 := jobs1[0]

	// no statistics: the datums are listable by count and identity, but
	// per-datum inspection errors
	pg, err := c.ListDatums(job1.ID, 0, 0)
	if err != nil {
		t.Fatalf("list datums without stats: %v", err)
	}
	if len(pg.Datums) != 5 {
		t.Fatalf("no-stats listing has %d datums, want 5", len(pg.Datums))
	}
	if _, err := c.InspectDatum(job1.ID, pg.Datums[0].ID); err == nil {
		t.Fatalf("inspect datum without stats: expected an error (no per-datum records)")
	}

	// enable statistics; a new commit's job records them
	if err := c.CreatePipeline(client.Pipeline{Name: pipe, Transform: copyTransform(repo), Input: in, Update: true, EnableStats: true}); err != nil {
		t.Fatalf("enable stats: %v", err)
	}
	second := map[string]string{}
	for i := 0; i < 5; i++ {
		second[fmt.Sprintf("new-%d", i)] = "y"
	}
	cm2 := commitFiles(t, repo, "master", second)
	jobs2 := flushOK(t, cm2.ID)
	job2 := jobs2[0]

	// the new job lists its full datum set: 5 newly processed + 5 carried
	// (SB-084's across-jobs semantics corroborate the listing includes
	// skipped datums; the record's literal "5 datums" line contradicts its
	// own summary and SB-084, so the listing is asserted at 10)
	pg, err = c.ListDatums(job2.ID, 0, 0)
	if err != nil {
		t.Fatalf("list datums with stats: %v", err)
	}
	if len(pg.Datums) != 10 {
		t.Fatalf("stats listing has %d datums, want 10 (5 new + 5 carried)", len(pg.Datums))
	}
	successes, skipped := 0, 0
	for _, dt := range pg.Datums {
		switch dt.State {
		case "success":
			successes++
		case "skipped":
			skipped++
		}
	}
	if successes != 5 || skipped != 5 {
		t.Fatalf("stats listing: %d success, %d skipped; want 5/5", successes, skipped)
	}
	insp, err := c.InspectDatum(job2.ID, pg.Datums[0].ID)
	if err != nil {
		t.Fatalf("inspect datum with stats: %v", err)
	}
	if insp.State != "success" {
		t.Fatalf("first datum state = %s, want success (new files process first)", insp.State)
	}

	// disabling is rejected
	if err := c.CreatePipeline(client.Pipeline{Name: pipe, Transform: copyTransform(repo), Input: in, Update: true, EnableStats: false}); err == nil {
		t.Fatalf("disabling stats: expected an error")
	}
}

// TestSB082_FailedDatumsListedFirst — a failed datum is recorded FAILED and
// leads the state-ordered listing (SB-082).
func TestSB082_FailedDatumsListedFirst(t *testing.T) {
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
			Cmd:   []string{"sh", "-c", fmt.Sprintf("if [ -f ${%s}/file-5 ]; then exit 1; fi; cp -r ${%s}/* ${OUT}/", repo, repo)},
		},
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		Parallelism: &client.Parallelism{Constant: 4},
		EnableStats: true,
	})

	jobs, err := c.Flush(cm.ID, 60*time.Second)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want 1", len(jobs))
	}
	pg, err := c.ListDatums(jobs[0].ID, 0, 0)
	if err != nil {
		t.Fatalf("list datums: %v", err)
	}
	if len(pg.Datums) != 10 {
		t.Fatalf("listing has %d datums, want 10", len(pg.Datums))
	}
	if pg.Datums[0].State != "failed" {
		t.Fatalf("first datum state = %s, want failed (state-ordered, not input order)", pg.Datums[0].State)
	}
	if pg.Datums[len(pg.Datums)-1].State != "success" {
		t.Fatalf("last datum state = %s, want success", pg.Datums[len(pg.Datums)-1].State)
	}
	insp, err := c.InspectDatum(jobs[0].ID, pg.Datums[0].ID)
	if err != nil || insp.State != "failed" {
		t.Fatalf("inspect failed datum: state %q err %v, want failed", insp.State, err)
	}
}

// TestSB083_PaginatedDatumListing — page boundaries, page metadata, and
// out-of-range rejection (SB-083).
func TestSB083_PaginatedDatumListing(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	files := map[string]string{}
	for i := 0; i < 20; i++ {
		files[fmt.Sprintf("file-%d", i)] = fmt.Sprintf("%d", i)
	}
	cm := commitFiles(t, repo, "master", files)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", fmt.Sprintf("if [ -f ${%s}/file-5 ]; then exit 1; fi; cp -r ${%s}/* ${OUT}/", repo, repo)},
		},
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		Parallelism: &client.Parallelism{Constant: 4},
		EnableStats: true,
	})
	jobs, err := c.Flush(cm.ID, 60*time.Second)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	job := jobs[0]

	pg, err := c.ListDatums(job.ID, 10, 0)
	if err != nil {
		t.Fatalf("page 0: %v", err)
	}
	if len(pg.Datums) != 10 || pg.TotalPages != 2 || pg.Page != 0 {
		t.Fatalf("page 0 = %d datums, %d pages, index %d; want 10/2/0", len(pg.Datums), pg.TotalPages, pg.Page)
	}
	if pg.Datums[0].State != "failed" {
		t.Fatalf("page 0 first datum = %s, want failed (state order spans pages consistently)", pg.Datums[0].State)
	}

	pg, err = c.ListDatums(job.ID, 10, 1)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(pg.Datums) != 10 || pg.Page != 1 {
		t.Fatalf("page 1 = %d datums, index %d; want 10/1", len(pg.Datums), pg.Page)
	}
	if pg.Datums[len(pg.Datums)-1].State != "success" {
		t.Fatalf("page 1 last datum = %s, want success", pg.Datums[len(pg.Datums)-1].State)
	}

	// an index equal to the page count is out of range
	if _, err := c.ListDatums(job.ID, 10, 2); err == nil {
		t.Fatalf("page 2 (== total pages): expected an out-of-range error")
	}
}

// TestSB084_DatumStatsAcrossJobs — a job's listing includes the datums
// carried from the previous job as skipped, ordered processed before
// skipped (SB-084).
func TestSB084_DatumStatsAcrossJobs(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:        pipe,
		Transform:   copyTransform(repo),
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		EnableStats: true,
	})

	first := map[string]string{}
	for i := 0; i < 10; i++ {
		first[fmt.Sprintf("file-%d", i)] = fmt.Sprintf("%d", i)
	}
	cm1 := commitFiles(t, repo, "master", first)
	jobs1 := flushOK(t, cm1.ID)
	pg, err := c.ListDatums(jobs1[0].ID, 0, 0)
	if err != nil {
		t.Fatalf("job 1 list: %v", err)
	}
	if len(pg.Datums) != 10 {
		t.Fatalf("job 1 has %d datums, want 10", len(pg.Datums))
	}
	for _, dt := range pg.Datums {
		if dt.State != "success" {
			t.Fatalf("job 1 datum %s state = %s, want success", dt.ID, dt.State)
		}
	}

	second := map[string]string{}
	for i := 10; i < 20; i++ {
		second[fmt.Sprintf("file-%d", i)] = fmt.Sprintf("%d", i)
	}
	cm2 := commitFiles(t, repo, "master", second)
	jobs2 := flushOK(t, cm2.ID)
	all, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
	if err != nil || len(all) != 2 {
		t.Fatalf("job count = %d (err %v), want 2", len(all), err)
	}
	pg, err = c.ListDatums(jobs2[0].ID, 0, 0)
	if err != nil {
		t.Fatalf("job 2 list: %v", err)
	}
	if len(pg.Datums) != 20 {
		t.Fatalf("job 2 has %d datums, want 20 (10 new + 10 carried)", len(pg.Datums))
	}
	if pg.Datums[0].State != "success" {
		t.Fatalf("job 2 datum 0 state = %s, want success (processed before skipped)", pg.Datums[0].State)
	}
	if pg.Datums[10].State != "skipped" {
		t.Fatalf("job 2 datum 10 state = %s, want skipped", pg.Datums[10].State)
	}
	insp0, err := c.InspectDatum(jobs2[0].ID, pg.Datums[0].ID)
	if err != nil || insp0.State != "success" {
		t.Fatalf("inspect datum 0: %q %v, want success", insp0.State, err)
	}
	insp10, err := c.InspectDatum(jobs2[0].ID, pg.Datums[10].ID)
	if err != nil || insp10.State != "skipped" {
		t.Fatalf("inspect datum 10: %q %v, want skipped", insp10.State, err)
	}
}

// TestSB085_SkippedEdgeCase — a file deleted and re-added with identical
// content is skipped, not reprocessed: skip detection compares the final
// content against the last successful processing (SB-085).
func TestSB085_SkippedEdgeCase(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:        pipe,
		Transform:   copyTransform(repo),
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		EnableStats: true,
	})

	files := map[string]string{}
	for i := 0; i < 10; i++ {
		files[fmt.Sprintf("file-%d", i)] = fmt.Sprintf("%d", i)
	}
	cm1 := commitFiles(t, repo, "master", files)
	jobs1 := flushOK(t, cm1.ID)
	pg, err := c.ListDatums(jobs1[0].ID, 0, 0)
	if err != nil || len(pg.Datums) != 10 {
		t.Fatalf("job 1: %d datums (err %v), want 10", len(pg.Datums), err)
	}
	for _, dt := range pg.Datums {
		if dt.State != "success" {
			t.Fatalf("job 1 datum %s = %s, want success", dt.ID, dt.State)
		}
	}

	// delete file-0, then re-add it byte-identically
	del, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start delete commit: %v", err)
	}
	if err := c.DeleteFile(del.ID, "file-0"); err != nil {
		t.Fatalf("delete file-0: %v", err)
	}
	if _, err := c.FinishCommit(del.ID, "", false); err != nil {
		t.Fatalf("finish delete commit: %v", err)
	}
	flushOK(t, del.ID)

	cm3 := commitFiles(t, repo, "master", files) // re-adds file-0 with identical content
	jobs3 := flushOK(t, cm3.ID)
	all, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
	if err != nil || len(all) != 3 {
		t.Fatalf("job count = %d (err %v), want 3", len(all), err)
	}
	pg, err = c.ListDatums(jobs3[0].ID, 0, 0)
	if err != nil {
		t.Fatalf("job 3 list: %v", err)
	}
	if len(pg.Datums) != 10 {
		t.Fatalf("job 3 has %d datums, want 10", len(pg.Datums))
	}
	for _, dt := range pg.Datums {
		if dt.State != "skipped" {
			t.Fatalf("job 3 datum %s state = %s, want skipped (re-added file-0 included)", dt.ID, dt.State)
		}
	}
}

// TestSB086_PipelineOnStatsBranch — a stats-enabled pipeline maintains a
// "stats" branch on its output repo that downstream pipelines can consume;
// the change propagates through both stages (SB-086).
func TestSB086_PipelineOnStatsBranch(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})

	p1 := uniq(t) + "p1"
	mustPipeline(t, client.Pipeline{
		Name:        p1,
		Transform:   copyTransform(repo),
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		EnableStats: true,
	})
	p2 := uniq(t) + "p2"
	mustPipeline(t, client.Pipeline{
		Name:      p2,
		Transform: copyTransform(p1),
		Input:     &client.Input{Repo: p1, Branch: "stats", Glob: "/*"},
	})

	// the change propagates through both stages: p1's job and the
	// downstream job consuming the stats branch
	jobs := flushOK(t, cm.ID)
	if len(jobs) != 2 {
		t.Fatalf("flush returned %d jobs, want 2 (both pipeline stages)", len(jobs))
	}
	seen := map[string]bool{}
	for _, j := range jobs {
		if j.State != "success" {
			t.Fatalf("job %s (%s) state = %s, want success", j.ID, j.Pipeline, j.State)
		}
		seen[j.Pipeline] = true
	}
	if !seen[p1] || !seen[p2] {
		t.Fatalf("flush jobs %+v did not cover both pipelines", jobs)
	}
	// the stats branch is a real branch with its own commit
	hist, err := c.CommitHistory(p1, "stats")
	if err != nil || len(hist) != 1 {
		t.Fatalf("stats branch history = %d (err %v), want 1 commit", len(hist), err)
	}
}

// TestSB113_DatumTimeoutRecorded — a datum exceeding its per-datum timeout
// is terminated at the boundary: failed datum, failed job, process time
// equal to the timeout, and output plus statistics commits (SB-113).
func TestSB113_DatumTimeoutRecorded(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image:        "alpine",
			Cmd:          []string{"sh", "-c", "sleep 1000"},
			DatumTimeout: "20s",
		},
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		EnableStats: true,
	})

	jobs, err := c.Flush(cm.ID, 90*time.Second)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(jobs) != 1 || jobs[0].State != "failure" {
		t.Fatalf("job = %+v, want one failure", jobs)
	}
	// the job produced both its output commit and a statistics commit
	master, err := c.CommitHistory(pipe, "master")
	if err != nil || len(master) != 1 {
		t.Fatalf("output branch = %d commits (err %v), want 1", len(master), err)
	}
	stats, err := c.CommitHistory(pipe, "stats")
	if err != nil || len(stats) != 1 {
		t.Fatalf("stats branch = %d commits (err %v), want 1", len(stats), err)
	}
	pg, err := c.ListDatums(jobs[0].ID, 0, 0)
	if err != nil {
		t.Fatalf("list datums: %v", err)
	}
	if len(pg.Datums) != 1 {
		t.Fatalf("listing has %d datums, want 1", len(pg.Datums))
	}
	dt := pg.Datums[0]
	if dt.State != "failed" {
		t.Fatalf("datum state = %s, want failed", dt.State)
	}
	if dt.ProcessTime < 19.5 || dt.ProcessTime > 23 {
		t.Fatalf("datum process time = %.1fs, want the full 20s timeout", dt.ProcessTime)
	}
	insp, err := c.InspectDatum(jobs[0].ID, dt.ID)
	if err != nil || insp.State != "failed" {
		t.Fatalf("inspect datum: %q %v, want failed", insp.State, err)
	}
}

// TestSB114_ListDatumDuringJob — datums are listable while the job is
// still running; the datum in progress appears before completion (SB-114).
func TestSB114_ListDatumDuringJob(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image:        "alpine",
			Cmd:          []string{"sh", "-c", "sleep 1000"},
			DatumTimeout: "20s",
		},
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		EnableStats: true,
	})
	_ = cm

	job := waitJobFor(t, pipe, 30*time.Second)
	pollFor(t, "job running", 30*time.Second, func() bool {
		j, err := c.InspectJob(job.ID)
		return err == nil && j.State == "running"
	})
	pg, err := c.ListDatums(job.ID, 0, 0)
	if err != nil {
		t.Fatalf("list datums mid-flight: %v", err)
	}
	if len(pg.Datums) != 1 {
		t.Fatalf("mid-flight listing has %d datums, want exactly 1 (the one in progress)", len(pg.Datums))
	}
	if pg.Datums[0].State != "running" {
		t.Fatalf("in-progress datum state = %s, want running", pg.Datums[0].State)
	}
	// the job stays alive long enough to be observed (the 20s timeout)
	if !strings.Contains(job.ID, "-") {
		t.Fatalf("unexpected job id %q", job.ID)
	}
}
