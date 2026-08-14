package main

// Batch B regression tests: union-trigger rejection, sub-second cron tick filenames,
// and the brief commit inspect.

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"sandman/client"
	"sandman/internal/store"
)

// TestUnionRejectsSizeTrigger pins the creation-time rejection: a size
// trigger nested in a union (or on the union node itself) can never fire
// — accumulateTriggers accumulates per watched branch, a notion a union's
// merged view does not have — so it is rejected instead of silently
// never firing (the pre-fix behavior). A trigger on a cross member stays
// allowed.
func TestUnionRejectsSizeTrigger(t *testing.T) {
	base := client.Pipeline{Name: "p", Transform: &client.Transform{}}
	cases := []struct {
		name string
		in   *client.Input
	}{
		{"nested in a union member", &client.Input{Name: "u", Glob: "/*", Union: []client.Input{{Repo: "a", Glob: "/*", Trigger: &client.Trigger{SizeBytes: 1024}}}}},
		{"on the union node", &client.Input{Name: "u", Glob: "/*", Union: []client.Input{{Repo: "a", Glob: "/*"}}, Trigger: &client.Trigger{SizeBytes: 1024}}},
		{"deep inside a union member", &client.Input{Name: "u", Glob: "/*", Union: []client.Input{{Name: "m", Glob: "/*", Union: []client.Input{{Repo: "a", Glob: "/*", Trigger: &client.Trigger{SizeBytes: 1024}}}}}}},
	}
	for _, tc := range cases {
		p := base
		p.Input = tc.in
		if err := validatePipelineSpec(p); err == nil {
			t.Fatalf("%s: expected rejection, got nil", tc.name)
		}
	}
	// a plain union and a cross-member trigger still validate
	p := base
	p.Input = &client.Input{Name: "u", Glob: "/*", Union: []client.Input{{Repo: "a", Glob: "/*"}, {Repo: "b", Glob: "/*"}}}
	if err := validatePipelineSpec(p); err != nil {
		t.Fatalf("plain union: %v", err)
	}
	p = base
	p.Input = &client.Input{Cross: []client.Input{{Repo: "a", Glob: "/*", Trigger: &client.Trigger{SizeBytes: 1024}}}}
	if err := validatePipelineSpec(p); err != nil {
		t.Fatalf("trigger in a cross member: %v", err)
	}
}

// TestCronTickUniqueFilenames pins the sub-second fix: a tick file is
// named by the tick time with fractional seconds, so two ticks in the
// same second land in two files instead of one (whose content the
// append-mode view would have doubled). The filenames stay RFC3339
// (parseable) and free of glob metacharacters.
func TestCronTickUniqueFilenames(t *testing.T) {
	dir := t.TempDir()
	st := store.New(dir)
	if err := st.CreateRepo("r-cron"); err != nil {
		t.Fatal(err)
	}
	d := &daemon{state: dir, store: st}
	d.cronTick("r-cron", false)
	d.cronTick("r-cron", false)
	head := st.HeadCommit("r-cron", defaultBranch)
	if head == "" {
		t.Fatalf("no tick commit was written")
	}
	view, err := st.ResolveViewByID(head)
	if err != nil {
		t.Fatal(err)
	}
	if len(view) != 2 {
		t.Fatalf("tick commit holds %d files after two ticks, want 2 (distinct names)", len(view))
	}
	for p := range view {
		if _, err := time.Parse(time.RFC3339, p); err != nil {
			t.Fatalf("tick file %q is not RFC3339: %v", p, err)
		}
	}
}

// TestInspectCommitBriefSkipsSubvenants pins ?brief=1: the commit chain
// walkers (CommitHistory) never read Subvenants, and the unconditional
// subvenant scan is O(all commits) per inspection — the brief form
// skips it while the default form still reports subvenants.
func TestInspectCommitBriefSkipsSubvenants(t *testing.T) {
	dir := t.TempDir()
	st := store.New(dir)
	if err := st.CreateRepo("r"); err != nil {
		t.Fatal(err)
	}
	cm1, err := st.StartCommit("r", "master", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.FinishCommit(cm1.ID, "", false); err != nil {
		t.Fatal(err)
	}
	cm2, err := st.StartCommit("r", "master", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.FinishCommit(cm2.ID, "", false); err != nil {
		t.Fatal(err)
	}
	rec, err := st.LoadCommitByID(cm2.ID)
	if err != nil {
		t.Fatal(err)
	}
	rec.Provenance = []string{cm1.ID}
	if err := st.SaveCommit(rec); err != nil {
		t.Fatal(err)
	}
	d := &daemon{state: dir, store: st}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/commits/"+cm1.ID+"?brief=1", nil)
	req.SetPathValue("id", cm1.ID)
	if err := d.inspectCommitH(rr, req); err != nil {
		t.Fatalf("brief inspect: %v", err)
	}
	var brief client.Commit
	if err := json.Unmarshal(rr.Body.Bytes(), &brief); err != nil {
		t.Fatalf("decode brief: %v", err)
	}
	if len(brief.Subvenants) != 0 {
		t.Fatalf("brief inspect reported subvenants %v, want none", brief.Subvenants)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/api/v1/commits/"+cm1.ID, nil)
	req.SetPathValue("id", cm1.ID)
	if err := d.inspectCommitH(rr, req); err != nil {
		t.Fatalf("full inspect: %v", err)
	}
	var full client.Commit
	if err := json.Unmarshal(rr.Body.Bytes(), &full); err != nil {
		t.Fatalf("decode full: %v", err)
	}
	if len(full.Subvenants) != 1 || full.Subvenants[0] != cm2.ID {
		t.Fatalf("full inspect subvenants %v, want [%s]", full.Subvenants, cm2.ID)
	}
}
