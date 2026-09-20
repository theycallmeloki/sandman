package main

import (
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"sandman/internal/store"
)

// A commit's provenance is part of the revision, not an annotation applied
// to it afterwards: a consumer that can see a finished commit — the branch
// head moved, the commit listed by the API — must already see the epoch
// anchor its writer supplied, because a spout commit's provenance is what
// makes it belong to an epoch. The stamp used to be applied after
// FinishCommit (and its error swallowed), so a reader racing the publish
// observed a finished commit with provenance = [] (CI:
// TestSpoutEpochsAndMarker/provenance_epochs_across_updates saw one spout
// commit of an epoch report empty provenance while its siblings carried
// the epoch's spec commit).
//
// The reader here polls the branch head, which FinishCommit moves — the
// exact thing a consumer reads — while the writer publishes revisions
// back to back.
func TestProvenanceVisibleWhenCommitFinishes(t *testing.T) {
	d := &daemon{state: t.TempDir(), running: map[string]*runningJob{}}
	d.store = store.New(filepath.Join(d.state, "store"))
	repo := "provrace"
	if err := d.store.CreateRepo(repo); err != nil {
		t.Fatal(err)
	}

	var reads, unanchored int64
	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-stop:
				return
			default:
			}
			h, err := d.store.HeadCommitRec(repo, defaultBranch)
			if err != nil {
				continue
			}
			atomic.AddInt64(&reads, 1)
			if h.Finished && len(h.Provenance) == 0 {
				atomic.AddInt64(&unanchored, 1)
			}
		}
	}()

	const revisions = 150
	for i := 0; i < revisions; i++ {
		body := strconv.Itoa(i)
		if !d.commitRevision(repo, defaultBranch, func(id string) bool {
			return d.store.OverwriteFile(id, "f", []byte(body)) == nil
		}, []string{"spec-" + body}) {
			t.Fatalf("revision %d was abandoned", i)
		}
	}
	close(stop)
	<-stopped

	if u := atomic.LoadInt64(&unanchored); u > 0 {
		t.Fatalf("read a finished commit with no provenance %d times (%d head reads)", u, atomic.LoadInt64(&reads))
	}
}
