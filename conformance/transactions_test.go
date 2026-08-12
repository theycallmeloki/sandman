package conformance

// Transactions (SB-162/163): pipeline creations and updates stage into an
// open transaction and apply atomically on finish — all or nothing. A
// pipeline staged in a transaction may consume another pipeline staged in
// the same transaction (its output repo does not exist yet); after finish
// the whole chain runs. Updating two pipelines in one transaction yields
// exactly one new commit and one new job per pipeline. A pipeline modified
// outside its open transaction invalidates the transaction at finish.

import (
	"testing"

	"sandman/client"
)

func TestSB162_TransactionAtomicApply(t *testing.T) {
	repo := uniq(t) + "r"
	pa := uniq(t) + "a"
	pb := uniq(t) + "b"
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "", map[string]string{"file": "foo\n"})
	// the update op changes nothing observable beyond the version, so both
	// transactions use the same copy transform
	spec := func(name, input string, update bool) client.Pipeline {
		return client.Pipeline{Name: name, Update: update,
			Transform: copyTransform(input), Input: &client.Input{Repo: input, Glob: "/*"}}
	}

	// creation transaction: A consumes the input, B consumes A — the
	// cross-reference resolves inside the transaction even though A's
	// output repo does not exist yet (SB-162)
	tx, err := c.StartTransaction()
	if err != nil {
		t.Fatalf("start transaction: %v", err)
	}
	if err := c.CreatePipelineTx(spec(pa, repo, false), tx); err != nil {
		t.Fatalf("stage A: %v", err)
	}
	if err := c.CreatePipelineTx(spec(pb, pa, false), tx); err != nil {
		t.Fatalf("stage B: %v", err)
	}
	if err := c.FinishTransaction(tx); err != nil {
		t.Fatalf("finish creation transaction: %v", err)
	}
	flushOK(t, cm.ID)
	aHead, err := c.HeadCommit(pa, "master")
	if err != nil {
		t.Fatalf("A head: %v", err)
	}
	bHead, err := c.HeadCommit(pb, "master")
	if err != nil {
		t.Fatalf("B head: %v", err)
	}
	if data, err := c.GetFile(bHead.ID, "file"); err != nil || string(data) != "foo\n" {
		t.Fatalf("B output = %q (err %v), want %q — chain did not propagate", data, err, "foo\n")
	}
	_ = aHead

	// atomicity: a transaction with one invalid op applies nothing — the
	// valid pipeline staged alongside it must not exist afterwards
	badTx, err := c.StartTransaction()
	if err != nil {
		t.Fatalf("start transaction: %v", err)
	}
	pax := uniq(t) + "ax"
	pbx := uniq(t) + "bx"
	if err := c.CreatePipelineTx(spec(pax, repo, false), badTx); err != nil {
		t.Fatalf("stage valid op: %v", err)
	}
	if err := c.CreatePipelineTx(spec(pbx, uniq(t)+"missingrepo", false), badTx); err != nil {
		t.Fatalf("stage invalid op: %v", err)
	}
	wantErr(t, c.FinishTransaction(badTx), "not found")
	pipelineGone(t, pax)
	pipelineGone(t, pbx)
	if err := c.DeleteTransaction(badTx); err != nil {
		t.Fatalf("abort transaction: %v", err)
	}

	// update transaction: both pipelines in one transaction; exactly one
	// new commit and one new job per pipeline (B: 2 commits, 2 jobs —
	// initial version's plus the updated version's)
	tx2, err := c.StartTransaction()
	if err != nil {
		t.Fatalf("start transaction: %v", err)
	}
	if err := c.CreatePipelineTx(spec(pa, repo, true), tx2); err != nil {
		t.Fatalf("stage A update: %v", err)
	}
	if err := c.CreatePipelineTx(spec(pb, pa, true), tx2); err != nil {
		t.Fatalf("stage B update: %v", err)
	}
	if err := c.FinishTransaction(tx2); err != nil {
		t.Fatalf("finish update transaction: %v", err)
	}
	flushOK(t, cm.ID)

	hist, err := c.CommitHistory(pb, "master")
	if err != nil {
		t.Fatalf("B commit history: %v", err)
	}
	if len(hist) != 2 {
		t.Fatalf("B has %d commits after the update transaction, want exactly 2", len(hist))
	}
	jobs, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pb})
	if err != nil {
		t.Fatalf("B job listing: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("B has %d jobs, want exactly 2 (one per pipeline version)", len(jobs))
	}
	// and the update still propagated end to end
	bHead2, err := c.HeadCommit(pb, "master")
	if err != nil {
		t.Fatalf("B head after update: %v", err)
	}
	if data, err := c.GetFile(bHead2.ID, "file"); err != nil || string(data) != "foo\n" {
		t.Fatalf("B output after update = %q (err %v), want %q", data, err, "foo\n")
	}
}

func TestSB163_TransactionInvalidatedByExternalUpdate(t *testing.T) {
	ra := uniq(t) + "a"
	rb := uniq(t) + "b"
	rc := uniq(t) + "c"
	pipe := uniq(t) + "p"
	for _, r := range []string{ra, rb, rc} {
		mustRepo(t, r)
	}
	cm := commitFiles(t, ra, "", map[string]string{"file": "foo\n"})
	mustPipeline(t, client.Pipeline{Name: pipe, Transform: copyTransform(ra), Input: &client.Input{Repo: ra, Glob: "/*"}})
	flushOK(t, cm.ID)

	// open a transaction and stage an update of the pipeline (to consume rb)
	tx, err := c.StartTransaction()
	if err != nil {
		t.Fatalf("start transaction: %v", err)
	}
	staged := client.Pipeline{Name: pipe, Update: true,
		Transform: copyTransform(rb), Input: &client.Input{Repo: rb, Glob: "/*"}}
	if err := c.CreatePipelineTx(staged, tx); err != nil {
		t.Fatalf("stage update: %v", err)
	}

	// the same pipeline is updated outside the transaction (to consume rc)
	// before the transaction finishes
	outside := client.Pipeline{Name: pipe, Update: true,
		Transform: copyTransform(rc), Input: &client.Input{Repo: rc, Glob: "/*"}}
	mustPipeline(t, outside)

	// finishing the transaction detects the external modification and
	// refuses to commit, applying nothing
	wantErr(t, c.FinishTransaction(tx), "outside of transaction")
	p, err := c.InspectPipeline(pipe)
	if err != nil {
		t.Fatalf("inspect pipeline: %v", err)
	}
	if p.Input == nil || p.Input.Repo != rc {
		t.Fatalf("pipeline input = %+v, want repo %s — the staged update must not apply", p.Input, rc)
	}
	if err := c.DeleteTransaction(tx); err != nil {
		t.Fatalf("abort transaction: %v", err)
	}
}

// TestSB164_TxAbortLeavesNoSpecCommits — an aborted transaction cleans up
// the spec commits its applied operations wrote: no orphaned entries on
// the failure path (SB-164's literal 0-spec-commits-on-abort clause). The
// update's statistics one-way check fails only at apply time, forcing a
// real mid-apply rollback after the create already wrote its spec commit.
func TestSB164_TxAbortLeavesNoSpecCommits(t *testing.T) {
	repo := uniq(t) + "r"
	mustRepo(t, repo)
	// the spec repository is shared across the daemon's lifetime (and may
	// not exist yet), so the assertion is the delta: the aborted
	// transaction writes no spec commits (SB-164's 0-on-abort clause)
	specCount := func() int {
		ch, err := c.CommitHistory("spec", "master")
		if err != nil {
			return 0 // no head yet: zero commits
		}
		return len(ch)
	}
	specBefore := specCount()
	tx, err := c.StartTransaction()
	if err != nil {
		t.Fatalf("start transaction: %v", err)
	}
	pa := uniq(t) + "a"
	on := client.Pipeline{Name: pa, Transform: copyTransform(repo),
		Input: &client.Input{Repo: repo, Glob: "/*"}, EnableStats: true}
	if err := c.CreatePipelineTx(on, tx); err != nil {
		t.Fatalf("stage create: %v", err)
	}
	off := on
	off.Update = true
	off.EnableStats = false
	if err := c.CreatePipelineTx(off, tx); err != nil {
		t.Fatalf("stage update: %v", err)
	}

	// the update's stats one-way check fails at apply time, after the
	// create's spec commit was written: the transaction aborts
	wantErr(t, c.FinishTransaction(tx), "statistics cannot be disabled")

	// the aborted pipeline does not exist…
	if _, err := c.InspectPipeline(pa); err == nil {
		t.Fatalf("aborted pipeline %q still exists", pa)
	}
	// …and the spec repository gained no commits (the rollback deleted
	// the create's spec commit — no orphans on the failure path)
	specAfter := specCount()
	if specAfter != specBefore {
		t.Fatalf("spec commits after abort = %d, want %d (unchanged)", specAfter, specBefore)
	}
}

// TestTxInspectList — the transaction inspect/list surface (CLI_SURFACE
// row closure): open transactions are enumerable and inspectable with
// their staged operations; a finished or deleted transaction disappears
// from the list and inspects as an error.
func TestTxInspectList(t *testing.T) {
	// fresh slate: the harness may carry a transaction from an aborted
	// earlier test (each leaves none, but be safe)
	tx0, err := c.ListTransactions()
	if err != nil {
		t.Fatalf("list transactions: %v", err)
	}
	repo := uniq(t) + "r"
	mustRepo(t, repo)

	tx, err := c.StartTransaction()
	if err != nil {
		t.Fatalf("start transaction: %v", err)
	}
	pa := uniq(t) + "a"
	if err := c.CreatePipelineTx(client.Pipeline{Name: pa, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}, tx); err != nil {
		t.Fatalf("stage create: %v", err)
	}
	pb := uniq(t) + "b"
	up := client.Pipeline{Name: pb, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}}
	if err := c.CreatePipelineTx(up, tx); err != nil {
		t.Fatalf("stage create b: %v", err)
	}
	if err := c.CreatePipelineTx(client.Pipeline{Name: pb, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}, Update: true}, tx); err != nil {
		t.Fatalf("stage update b: %v", err)
	}

	// inspect shows the staged operations in order
	ti, err := c.InspectTransaction(tx)
	if err != nil {
		t.Fatalf("inspect transaction: %v", err)
	}
	if ti.ID != tx {
		t.Fatalf("inspect id = %s, want %s", ti.ID, tx)
	}
	if len(ti.Ops) != 3 || ti.Ops[0].Kind != "create" || ti.Ops[0].Pipeline != pa ||
		ti.Ops[1].Kind != "create" || ti.Ops[1].Pipeline != pb || ti.Ops[2].Kind != "update" {
		t.Fatalf("ops = %+v, want create %s, create %s, update %s", ti.Ops, pa, pb, pb)
	}

	// the list carries the transaction with its op count
	tl, err := c.ListTransactions()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	found := false
	for _, txn := range tl {
		if txn.ID == tx {
			found = true
			if len(txn.Ops) != 3 {
				t.Fatalf("listed tx op count = %d, want 3", len(txn.Ops))
			}
		}
	}
	if !found {
		t.Fatalf("transaction %s not listed (list = %+v)", tx, tl)
	}

	// finish: the transaction is gone from the list and inspects as an
	// error; the staged pipelines took effect
	if err := c.FinishTransaction(tx); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if _, err := c.InspectTransaction(tx); err == nil {
		t.Fatalf("inspect finished transaction: want an error")
	}
	tl, err = c.ListTransactions()
	if err != nil {
		t.Fatalf("list after finish: %v", err)
	}
	for _, txn := range tl {
		if txn.ID == tx {
			t.Fatalf("finished transaction %s still listed", tx)
		}
	}
	if len(tl) != len(tx0) {
		t.Fatalf("transaction count after finish = %d, want %d (back to the baseline)", len(tl), len(tx0))
	}
	if _, err := c.InspectPipeline(pa); err != nil {
		t.Fatalf("staged pipeline %s did not take effect: %v", pa, err)
	}

	// delete: gone the same way
	tx2, err := c.StartTransaction()
	if err != nil {
		t.Fatalf("start second transaction: %v", err)
	}
	if err := c.DeleteTransaction(tx2); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := c.InspectTransaction(tx2); err == nil {
		t.Fatalf("inspect deleted transaction: want an error")
	}
}
