// Delimited uploads replicate the header into every record, and URL
// ingestion feeds JSON-spec pipelines.
package conformance

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"sandman/client"
)

func TestDelimitedUploadsReplicateHeader(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	// the transform verifies the header appears once in every datum's
	// file, then copies the records onward
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", fmt.Sprintf("for f in ${%s}/d/*; do head -1 $f | grep -qx 'HDR' || exit 1; done; cp -r ${%s}/d ${OUT}/d", repo, repo)},
		},
		Input: &client.Input{Repo: repo, Glob: "/d/*"},
	})

	// first upload: 5 records under the header
	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	if err := c.PutFileSplit(cm.ID, "d", []byte("HDR\nr1\nr2\nr3\nr4\nr5"), "\n", true); err != nil {
		t.Fatalf("split upload: %v", err)
	}
	fin, err := c.FinishCommit(cm.ID, "", false)
	if err != nil {
		t.Fatalf("finish commit: %v", err)
	}

	jobs := flushOK(t, fin.ID)
	if len(jobs) != 1 {
		t.Fatalf("first flush = %d jobs, want 1", len(jobs))
	}
	if jobs[0].Processed != 5 || jobs[0].Skipped != 0 {
		t.Fatalf("first job: processed=%d skipped=%d, want 5 and 0", jobs[0].Processed, jobs[0].Skipped)
	}
	// each record file carries the header exactly once
	for i := 0; i < 5; i++ {
		b, err := c.GetFile(fin.ID, fmt.Sprintf("d/%d", i))
		if err != nil {
			t.Fatalf("read d/%d: %v", i, err)
		}
		if strings.Count(string(b), "HDR") != 1 {
			t.Fatalf("record d/%d carries the header %d times, want exactly 1", i, strings.Count(string(b), "HDR"))
		}
	}

	// append 3 records under the same header: the earlier records keep
	// their identity and are skipped; only the new ones process
	cm2, _ := c.StartCommit(repo, "master", "")
	if err := c.PutFileSplit(cm2.ID, "d", []byte("HDR\nr6\nr7\nr8"), "\n", true); err != nil {
		t.Fatalf("append split upload: %v", err)
	}
	fin2, _ := c.FinishCommit(cm2.ID, "", false)
	jobs2 := flushOK(t, fin2.ID)
	if len(jobs2) != 1 {
		t.Fatalf("second flush = %d jobs, want 1", len(jobs2))
	}
	if jobs2[0].Processed != 3 || jobs2[0].Skipped != 5 {
		t.Fatalf("second job: processed=%d skipped=%d, want 3 and 5", jobs2[0].Processed, jobs2[0].Skipped)
	}
	// the branch now holds all 8 records
	if b, err := c.GetFile(fin2.ID, "d/7"); err != nil || string(b) != "HDR\nr8" {
		t.Fatalf("d/7 = %q (%v), want the appended record", string(b), err)
	}
}

func TestChangedHeaderReprocessesAll(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      pipe,
		Transform: &client.Transform{Image: "alpine:3.21"},
		Input:     &client.Input{Repo: repo, Glob: "/d/*"},
	})

	upload := func(header string) client.Commit {
		cm, err := c.StartCommit(repo, "master", "")
		if err != nil {
			t.Fatalf("start commit: %v", err)
		}
		data := header + "\nr1\nr2\nr3"
		if err := c.PutFileSplit(cm.ID, "d", []byte(data), "\n", true); err != nil {
			t.Fatalf("split upload: %v", err)
		}
		fin, err := c.FinishCommit(cm.ID, "", false)
		if err != nil {
			t.Fatalf("finish commit: %v", err)
		}
		return fin
	}
	jobs := flushOK(t, upload("HDR").ID)
	if len(jobs) != 1 || jobs[0].Processed != 3 {
		t.Fatalf("first job: processed=%d, want 3", jobs[0].Processed)
	}
	// a changed header re-identifies every record: all are reprocessed
	jobs2 := flushOK(t, upload("HDR2").ID)
	if len(jobs2) != 1 || jobs2[0].Processed != 3 || jobs2[0].Skipped != 0 {
		t.Fatalf("changed-header job: processed=%d skipped=%d, want 3 and 0", jobs2[0].Processed, jobs2[0].Skipped)
	}
}

func TestUrlIngestionJsonSpecPipelines(t *testing.T) {
	// a local HTTP server serves the "remote" image
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("image-bytes"))
	}))
	defer srv.Close()

	repo := uniq(t)
	mustRepo(t, repo)
	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	if err := c.PutFileURL(cm.ID, "img.jpg", srv.URL); err != nil {
		t.Fatalf("url ingest: %v", err)
	}
	fin, _ := c.FinishCommit(cm.ID, "", false)
	if b, err := c.GetFile(fin.ID, "img.jpg"); err != nil || string(b) != "image-bytes" {
		t.Fatalf("ingested file = %q (%v), want the URL's bytes", string(b), err)
	}

	// pipelines defined from JSON specs form a two-stage DAG
	spec1 := fmt.Sprintf(`{"name": %q, "transform": {"image": "alpine:3.21"}, "input": {"repo": %q, "glob": "/*"}}`, uniq(t), repo)
	spec2 := fmt.Sprintf(`{"name": %q, "transform": {"image": "alpine:3.21"}, "input": {"repo": %q, "glob": "/*"}}`, uniq(t), uniq(t))
	var p1, p2 client.Pipeline
	if err := json.Unmarshal([]byte(spec1), &p1); err != nil {
		t.Fatalf("spec1: %v", err)
	}
	if err := json.Unmarshal([]byte(spec2), &p2); err != nil {
		t.Fatalf("spec2: %v", err)
	}
	// the second pipeline consumes the first's output
	p2.Input.Repo = p1.Name
	if err := c.CreatePipeline(p1); err != nil {
		t.Fatalf("create p1: %v", err)
	}
	if err := c.CreatePipeline(p2); err != nil {
		t.Fatalf("create p2: %v", err)
	}

	// flushing the ingested commit propagates through both stages
	jobs := flushOK(t, fin.ID)
	if len(jobs) != 2 {
		t.Fatalf("flush = %d jobs, want 2 (one per pipeline stage)", len(jobs))
	}
	seen := map[string]bool{}
	for _, j := range jobs {
		seen[j.Pipeline] = true
		if j.State != "success" {
			t.Fatalf("stage %s state = %s", j.Pipeline, j.State)
		}
	}
	if !seen[p1.Name] || !seen[p2.Name] {
		t.Fatalf("flush missing a stage: %v", seen)
	}
}
