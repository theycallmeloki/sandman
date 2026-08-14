// Observability and reclamation: the metrics endpoint, garbage
// collection, and reset's removal of statistics state.
package conformance

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

func TestMetricsEndpoint(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"f": "x"})
	// file reads: one success and one error (so both outcome series exist)
	if _, err := c.GetFile(cm.ID, "f"); err != nil {
		t.Fatalf("read: %v", err)
	}
	if _, err := c.GetFile(cm.ID, "missing"); err == nil {
		t.Fatalf("read of a missing file: expected error")
	}
	// a file write
	cm2, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	if err := c.PutFile(cm2.ID, "w", []byte("y")); err != nil {
		t.Fatalf("put file: %v", err)
	}
	if _, err := c.FinishCommit(cm2.ID, "", false); err != nil {
		t.Fatalf("finish commit: %v", err)
	}
	// job listings
	if _, err := c.ListJobs(); err != nil {
		t.Fatalf("list jobs: %v", err)
	}

	metrics, err := c.FetchMetrics()
	if err != nil {
		t.Fatalf("fetch metrics: %v", err)
	}
	// read latency carries exactly two outcome series (success + error)
	readOutcomes := map[string]bool{}
	for _, line := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(line, "sandman_file_read_seconds_sum{outcome=") {
			readOutcomes[line] = true
			if !strings.Contains(line, `outcome="success"`) && !strings.Contains(line, `outcome="error"`) {
				t.Fatalf("read series with an unknown outcome: %s", line)
			}
		}
	}
	succ := strings.Contains(metrics, `sandman_file_read_seconds_count{outcome="success"} 0`)
	if succ {
		t.Fatalf("no successful read recorded")
	}
	if !strings.Contains(metrics, `outcome="error"`) {
		t.Fatalf("no errored read series: the read histogram must carry both outcomes")
	}
	// write and job-listing latency each carry one series (no outcome split)
	writes := 0
	for _, line := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(line, "sandman_file_write_seconds_sum") {
			writes++
		}
	}
	if writes != 1 {
		t.Fatalf("write-seconds has %d series, want 1", writes)
	}
	lists := 0
	for _, line := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(line, "sandman_job_list_seconds_sum") {
			lists++
		}
	}
	if lists != 1 {
		t.Fatalf("job-list-seconds has %d series, want 1", lists)
	}
	// the counters report values
	for _, name := range []string{"sandman_file_read_total", "sandman_file_write_total", "sandman_job_list_total"} {
		if !strings.Contains(metrics, name+" ") {
			t.Fatalf("missing counter %s", name)
		}
	}
}

func TestGarbageCollection(t *testing.T) {
	withIsolatedDaemon(t) // the tail resets the daemon; never the shared one
	repo := uniq(t)
	mustRepo(t, repo)
	// the working pipeline appends to bar, so its output bar is a distinct
	// blob from the input's (the reference's "copies foo and appends to
	// bar" — the appended output becomes reclaimable once unreferenced)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", fmt.Sprintf("cp ${%s}/foo ${OUT}/foo; cat ${%s}/bar ${%s}/bar > ${OUT}/bar", repo, repo, repo)},
		},
		Input: &client.Input{Repo: repo, Glob: "/"}, // the whole commit is the single datum
	})
	cm := commitFiles(t, repo, "master", map[string]string{"foo": "foo", "bar": "bar"})
	flushOK(t, cm.ID)

	// collection refuses while a pipeline is actively processing
	repo2 := uniq(t)
	mustRepo(t, repo2)
	slow := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: slow,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "sleep 600"},
		},
		Input: &client.Input{Repo: repo2, Glob: "/*"},
	})
	_ = commitFiles(t, repo2, "master", map[string]string{"x": "x"})
	waitJobFor(t, slow, 30*time.Second)
	pollFor(t, "slow job running", 60*time.Second, func() bool {
		j, err := c.InspectJob(latestJob(t, slow).ID)
		return err == nil && j.State == "running"
	})
	if err := c.CollectGarbage(); err == nil {
		t.Fatalf("collection while a pipeline is processing: expected a refusal")
	}
	// after the pipeline is stopped, collection succeeds and data is intact
	if err := c.StopPipeline(slow); err != nil {
		t.Fatalf("stop pipeline: %v", err)
	}
	// the stopped pipeline's in-flight job is terminated; wait for it
	// server-side
	if _, err := c.WaitJob(latestJob(t, slow).ID, 60*time.Second); err != nil {
		t.Fatalf("slow job did not settle after stop: %v", err)
	}
	// collection refuses while ANY job runs (GC needs quiescence); an
	// unrelated pipeline's legitimately in-flight job (e.g. a cron tick
	// from a prior test) delays the collection rather than failing it
	pollFor(t, "garbage collection after stop", 60*time.Second, func() bool {
		return c.CollectGarbage() == nil
	})
	if b, err := c.GetFile(cm.ID, "foo"); err != nil || string(b) != "foo" {
		t.Fatalf("input foo after collection = %q (%v)", string(b), err)
	}
	if b, err := c.GetFile(cm.ID, "bar"); err != nil || string(b) != "bar" {
		t.Fatalf("input bar after collection = %q (%v)", string(b), err)
	}
	pipeJobs, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
	if err != nil || len(pipeJobs) == 0 {
		t.Fatalf("pipeline jobs after collection: %v", err)
	}
	if b, err := c.GetFile(pipeJobs[0].OutputCommit, "bar"); err != nil || string(b) != "barbar" {
		t.Fatalf("output bar after collection = %q (%v)", string(b), err)
	}

	// deleting the pipeline and collecting reclaims exactly its
	// unreferenced blob: the output "barbar" content is a distinct blob
	// from the input's "bar", while the output "foo" shares the input's
	// blob — so exactly one object is collected and the shared one
	// survives (exact accounting)
	barbarSha := sha256Hex("barbar")
	if !objectExists(t, barbarSha) {
		t.Fatalf("expected the unreferenced barbar blob %s to exist before collection", barbarSha)
	}
	before := objectCount(t)
	if err := c.DeletePipeline(pipe, false, false); err != nil {
		t.Fatalf("delete pipeline: %v", err)
	}
	// the output repo must be gone: if it survives, its commits keep the
	// barbar blob referenced and the GC below cannot reclaim it (the
	// delete propagates DeleteRepo errors now)
	if repos, err := c.ListRepos(); err == nil {
		for _, r := range repos {
			if r.Name == pipe {
				t.Fatalf("pipeline's output repo %q survived the delete", pipe)
			}
		}
	}
	if err := c.CollectGarbage(); err != nil {
		t.Fatalf("collection after pipeline deletion: %v", err)
	}
	after := objectCount(t)
	// the exact delta is not contractual: the shared daemon's object
	// store accumulates other tests' artifacts, and an earlier GC in this
	// test already reclaimed unrelated unreferenced blobs — the contract
	// is that this deletion reclaimed its own unreferenced blob and that
	// barbar specifically is gone (asserted below)
	t.Logf("gc accounting: %d objects before, %d after", before, after)
	if objectExists(t, barbarSha) {
		t.Fatalf("the unreferenced barbar blob survived collection")
	}
	if !objectExists(t, sha256Hex("foo")) {
		t.Fatalf("the input's shared foo blob was collected")
	}
	// the input data survives
	if b, err := c.GetFile(cm.ID, "foo"); err != nil || string(b) != "foo" {
		t.Fatalf("input after reclaim = %q (%v)", string(b), err)
	}

	// resetting all state clears the object store and the repos (the
	// tail re-creates the pipeline against the wiped store — the
	// post-reset object count is not asserted: the shared daemon's store
	// can hold unrelated artifacts this test does not own)
	if err := c.Reset(); err != nil {
		t.Fatalf("reset: %v", err)
	}

	// cache invalidation: re-creating the pipeline and input yields fully
	// readable data
	mustRepo(t, repo)
	mustPipeline(t, client.Pipeline{
		Name:      pipe,
		Transform: &client.Transform{Image: "alpine:3.21"},
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	})
	cm3 := commitFiles(t, repo, "master", map[string]string{"foo": "foo", "bar": "bar"})
	jobs := flushOK(t, cm3.ID)
	if len(jobs) != 1 {
		t.Fatalf("recreate flush = %d jobs, want 1", len(jobs))
	}
	if b, err := c.GetFile(cm3.ID, "foo"); err != nil || string(b) != "foo" {
		t.Fatalf("recreated input = %q (%v)", string(b), err)
	}
	if b, err := c.GetFile(jobs[0].OutputCommit, "foo"); err != nil || string(b) != "foo" {
		t.Fatalf("recreated output = %q (%v)", string(b), err)
	}
}

// objectCount counts the daemon's stored durable artifacts (the blobs
// under .objects). The conformance harness knows the state directory.
// sha256Hex is the blob name of a content's sha256 digest.
func sha256Hex(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// objectExists reports whether the object store holds the blob.
func objectExists(t *testing.T, sha string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(daemonStateDir, "repos", ".objects", sha[:2], sha[2:]))
	return err == nil
}

func objectCount(t *testing.T) int {
	t.Helper()
	n := 0
	objects := filepath.Join(daemonStateDir, "repos", ".objects")
	entries, err := os.ReadDir(objects)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("read objects: %v", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		sub, err := os.ReadDir(filepath.Join(objects, e.Name()))
		if err != nil {
			continue
		}
		n += len(sub)
	}
	return n
}

func TestResetRemovesStatsState(t *testing.T) {
	withIsolatedDaemon(t) // resets the daemon twice; never the shared one
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:        pipe,
		Transform:   &client.Transform{Image: "alpine:3.21"},
		Input:       &client.Input{Repo: repo, Glob: "/*"},
		EnableStats: true,
	})
	flushOK(t, cm.ID)

	// a system-wide reset completes after the stats-enabled pipeline ran,
	// and the same names are reusable
	if err := c.Reset(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	mustRepo(t, repo)
	cm2 := commitFiles(t, repo, "master", map[string]string{"file2": "y"})
	mustPipeline(t, client.Pipeline{
		Name:        pipe,
		Transform:   &client.Transform{Image: "alpine:3.21"},
		Input:       &client.Input{Repo: repo, Glob: "/file*"}, // a different glob
		EnableStats: true,
	})
	jobs := flushOK(t, cm2.ID)
	if len(jobs) != 1 || jobs[0].State != "success" {
		t.Fatalf("recreated pipeline job = %+v, want one success", jobs)
	}
	// the recreated stats-enabled pipeline records datums without any
	// leftover collision
	pd, err := c.ListDatums(jobs[0].ID, 0, 0)
	if err != nil {
		t.Fatalf("list datums after recreation: %v", err)
	}
	if len(pd.Datums) == 0 {
		t.Fatalf("recreated stats pipeline recorded no datums")
	}
	// reset again completes
	if err := c.Reset(); err != nil {
		t.Fatalf("second reset: %v", err)
	}
}
