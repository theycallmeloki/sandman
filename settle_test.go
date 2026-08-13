package main

// Unit tests for settlePanicJob (jobs.go): a panic in a job goroutine
// must settle the job record failed — the guard alone abandons it with a
// forever-"running" record, which wedges every later flush and garbage
// collection (the stuck-job class).

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"sandman/internal/store"
)

func TestSettlePanicJob(t *testing.T) {
	dir := t.TempDir()
	d := &daemon{state: dir}
	d.store = store.New(filepath.Join(dir, "store"))

	// a running record is settled failed with a reason
	rec := jobRec{ID: "j1", Pipeline: "p1", State: stateRunning}
	if err := d.saveJob(&rec); err != nil {
		t.Fatalf("saveJob: %v", err)
	}
	d.settlePanicJob("j1")
	b, err := os.ReadFile(filepath.Join(dir, "jobs", "j1", "job.json"))
	if err != nil {
		t.Fatalf("read settled record: %v", err)
	}
	var got jobRec
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("decode settled record: %v", err)
	}
	if got.State != stateFailure {
		t.Fatalf("settled state = %q, want failure", got.State)
	}
	if got.Reason == "" || got.Finished == "" {
		t.Fatalf("settled record = %+v, want reason and finish time", got)
	}

	// a terminal record is untouched
	ok := jobRec{ID: "j2", Pipeline: "p2", State: stateSuccess, Reason: "fine", Finished: "now"}
	if err := d.saveJob(&ok); err != nil {
		t.Fatalf("saveJob j2: %v", err)
	}
	d.settlePanicJob("j2")
	b, _ = os.ReadFile(filepath.Join(dir, "jobs", "j2", "job.json"))
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("decode j2: %v", err)
	}
	if got.State != stateSuccess || got.Reason != "fine" {
		t.Fatalf("terminal record disturbed: %+v", got)
	}

	// a missing record is a no-op (the panic struck before saveJob)
	d.settlePanicJob("j3")

	// a running record with an output commit: the empty finish is
	// attempted best-effort (the commit does not exist here; the error
	// is ignored) — the record must still settle failed
	rec4 := jobRec{ID: "j4", Pipeline: "p4", State: stateRunning, OutputCommit: "nope"}
	if err := d.saveJob(&rec4); err != nil {
		t.Fatalf("saveJob j4: %v", err)
	}
	d.settlePanicJob("j4")
	b, _ = os.ReadFile(filepath.Join(dir, "jobs", "j4", "job.json"))
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("decode j4: %v", err)
	}
	if got.State != stateFailure {
		t.Fatalf("j4 settled state = %q, want failure", got.State)
	}
}
