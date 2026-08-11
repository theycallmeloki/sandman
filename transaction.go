package main

// Meta plane, transactions (SB-162/163): pipeline creations and updates
// stage into an open transaction and apply atomically on finish — either
// every staged operation takes effect or none does. Ops are plain JSON
// files under <state>/transactions/<id>/; there is no hidden state.
//
// A pipeline staged in the transaction may consume another pipeline staged
// in the same transaction (its output repo does not exist yet): repo
// existence resolves against the transaction's own pipelines. Head jobs
// for the applied versions are scheduled topologically — a pipeline waits
// for its in-transaction upstreams to settle, and event-driven triggers
// are suppressed until then — so each updated pipeline produces exactly
// one new job and one new output commit (SB-162).
//
// Conflict detection (SB-163): an update staged while the transaction is
// open records the pipeline's version; if the pipeline is modified outside
// the transaction before finish, the version differs and finish fails with
// "outside of transaction", applying nothing.

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"sandman/client"
)

// ---- storage ----

func (d *daemon) txDir(id string) string {
	return filepath.Join(d.state, "transactions", id)
}

func (d *daemon) txExists(id string) error {
	if st, err := os.Stat(d.txDir(id)); err != nil || !st.IsDir() {
		return fmt.Errorf("transaction %q not found", id)
	}
	return nil
}

func newTxID(node string) string {
	b := make([]byte, 6)
	rand.Read(b)
	return node + "-tx-" + hex.EncodeToString(b)
}

// txOp is one staged operation. Baseline is the pipeline's version when
// the op was staged: finish refuses if the live pipeline has since been
// modified outside the transaction (SB-163).
type txOp struct {
	Kind     string          `json:"kind"` // create | update
	Spec     client.Pipeline `json:"spec"`
	Baseline int             `json:"baseline,omitempty"`
	// SpecCommit is the spec-repository commit the op wrote when it
	// applied; the rollback deletes it so an aborted transaction leaves
	// no orphaned spec commits (SB-164).
	SpecCommit string `json:"specCommit,omitempty"`
}

func (d *daemon) loadTxOps(id string) ([]txOp, error) {
	entries, err := os.ReadDir(d.txDir(id))
	if err != nil {
		return nil, fmt.Errorf("transaction %q not found", id)
	}
	var ops []txOp
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(d.txDir(id), e.Name()))
		if err != nil {
			continue
		}
		var op txOp
		if json.Unmarshal(b, &op) != nil {
			return nil, fmt.Errorf("transaction %q has a corrupted operation record", id)
		}
		ops = append(ops, op)
	}
	return ops, nil
}

// ---- staging ----

// stageTxOp records a create or update into an open transaction. Only the
// structural rules are checked here; repo existence and name conflicts are
// validated at finish, where cross-transaction references can resolve.
func (d *daemon) stageTxOp(id string, p client.Pipeline) error {
	if err := d.txExists(id); err != nil {
		return err
	}
	if err := validatePipelineSpec(p); err != nil {
		return err
	}
	op := txOp{Spec: p}
	if p.Update {
		op.Kind = "update"
		if rec, err := d.loadPipeline(p.Name); err == nil {
			op.Baseline = rec.Version
		}
	} else {
		op.Kind = "create"
	}
	entries, _ := os.ReadDir(d.txDir(id))
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			n++
		}
	}
	b, err := json.Marshal(op)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(d.txDir(id), fmt.Sprintf("%06d.json", n)), b, 0o644)
}

func (d *daemon) startTransaction() (string, error) {
	id := newTxID(d.name)
	if err := os.MkdirAll(d.txDir(id), 0o755); err != nil {
		return "", err
	}
	return id, nil
}

func (d *daemon) deleteTransaction(id string) error {
	if err := d.txExists(id); err != nil {
		return err
	}
	return os.RemoveAll(d.txDir(id))
}

// ---- finish: validate, then apply ----

func (d *daemon) finishTransaction(id string) error {
	if err := d.txExists(id); err != nil {
		return err
	}
	ops, err := d.loadTxOps(id)
	if err != nil {
		return err
	}
	if len(ops) == 0 {
		return fmt.Errorf("transaction %q has no operations", id)
	}

	// Phase 1 — validate and conflict-check everything up front so a
	// failing transaction applies nothing (all or nothing). txState tracks
	// each name's version as it will stand after the transaction's own ops.
	txState := map[string]int{}
	for _, op := range ops {
		if err := validatePipelineSpec(op.Spec); err != nil {
			return err
		}
		switch op.Kind {
		case "create":
			if _, touched := txState[op.Spec.Name]; touched {
				return fmt.Errorf("pipeline %q specified more than once in the transaction", op.Spec.Name)
			}
			if _, err := d.loadPipeline(op.Spec.Name); err == nil {
				return fmt.Errorf("pipeline %q already exists", op.Spec.Name)
			}
			txState[op.Spec.Name] = 1
		case "update":
			v, touched := txState[op.Spec.Name]
			if !touched {
				rec, err := d.loadPipeline(op.Spec.Name)
				if err != nil {
					if _, statErr := os.Stat(d.pipelinePath(op.Spec.Name)); statErr == nil {
						return fmt.Errorf("pipeline %q is incomplete and cannot be updated", op.Spec.Name)
					}
					return fmt.Errorf("pipeline %q not found", op.Spec.Name)
				}
				if rec.Version != op.Baseline {
					// modified outside the transaction between staging and
					// finish: refuse to commit (SB-163)
					return fmt.Errorf("pipeline %q was modified outside of transaction", op.Spec.Name)
				}
				v = rec.Version
			}
			txState[op.Spec.Name] = v + 1
		default:
			return fmt.Errorf("transaction %q contains an unknown operation %q", id, op.Kind)
		}
	}

	// Repo existence resolves against the transaction's own pipelines: a
	// pipeline staged here may consume another staged here, whose output
	// repo does not exist yet (SB-162).
	for _, op := range ops {
		for _, s := range inputSides(op.Spec.Input) {
			if _, pending := txState[s.Repo]; pending {
				continue
			}
			if _, err := os.Stat(d.store.repoDir(s.Repo)); err != nil {
				return fmt.Errorf("input repo %q not found", s.Repo)
			}
		}
	}

	// The final spec of each staged name and its dependency graph, for
	// topological head-job scheduling; a cycle would deadlock the
	// coordinator, so it is refused here.
	final := map[string]client.Pipeline{}
	for _, op := range ops {
		final[op.Spec.Name] = op.Spec
	}
	deps := map[string][]string{}
	for name, spec := range final {
		for _, s := range inputSides(spec.Input) {
			if dep, ok := final[s.Repo]; ok && s.Repo != name {
				deps[name] = append(deps[name], dep.Name)
			}
		}
	}
	order, err := topoOrder(final, deps)
	if err != nil {
		return fmt.Errorf("transaction pipelines form a dependency cycle")
	}

	// Phase 2 — hold every staged pipeline's event triggers, then apply the
	// metadata in staged order. The coordinator (spawned below) schedules
	// each pipeline's head job exactly once, in dependency order.
	txHoldMu.Lock()
	for name := range final {
		txHold[name] = true
	}
	txHoldMu.Unlock()

	var applied []txOp
	for _, op := range ops {
		switch op.Kind {
		case "create":
			rec, err := d.applyCreate(op.Spec)
			if err != nil {
				d.txAbortHolds(final)
				d.rollbackTx(applied)
				return fmt.Errorf("apply transaction: %v", err)
			}
			op.SpecCommit = rec.SpecCommit
		case "update":
			rec, err := d.loadPipeline(op.Spec.Name)
			if err != nil {
				d.txAbortHolds(final)
				d.rollbackTx(applied)
				return fmt.Errorf("apply transaction: %v", err)
			}
			// in-flight old-version work must not race the new head job
			d.cancelPipelineJobs(op.Spec.Name)
			nrec, err := d.applyUpdate(rec, op.Spec)
			if err != nil {
				d.txAbortHolds(final)
				d.rollbackTx(applied)
				return fmt.Errorf("apply transaction: %v", err)
			}
			op.SpecCommit = nrec.SpecCommit
		}
		applied = append(applied, op)
	}
	os.RemoveAll(d.txDir(id))

	go d.txCoordinate(final, deps, order)
	return nil
}

// txAbortHolds releases every staged pipeline's trigger hold when the
// transaction fails mid-apply: without a coordinator, the holds would
// otherwise suppress triggers forever.
func (d *daemon) txAbortHolds(final map[string]client.Pipeline) {
	txHoldMu.Lock()
	for name := range final {
		txHold[name] = false
		txPending[name] = false
	}
	txHoldMu.Unlock()
}

// rollbackTx undoes already-applied operations on a mid-apply failure:
// created pipelines are removed, updated pipelines are restored from their
// immutable version archives. Spec commits remain (they are durable
// history, not observable pipeline state).
func (d *daemon) rollbackTx(applied []txOp) {
	for i := len(applied) - 1; i >= 0; i-- {
		op := applied[i]
		switch op.Kind {
		case "create":
			os.Remove(d.pipelinePath(op.Spec.Name))
			os.RemoveAll(filepath.Join(d.state, "pipelines", "versions", op.Spec.Name))
		case "update":
			if b, err := os.ReadFile(d.versionPath(op.Spec.Name, op.Baseline)); err == nil {
				os.WriteFile(d.pipelinePath(op.Spec.Name), b, 0o644)
			}
		}
		// an aborted transaction leaves no spec commits behind (SB-164):
		// the applied op's definition commit is deleted with its pipeline
		// — no orphaned entries on the failure path
		if op.SpecCommit != "" {
			d.deleteCommit(op.SpecCommit)
		}
	}
}

// ---- head-job coordination ----

// A held pipeline ignores event-driven triggers: the transaction's
// coordinator is the sole scheduler for it until its head job is settled.
// txPending records a commit that arrived while held, so the coordinator
// re-schedules rather than missing it.
var (
	txHoldMu  sync.Mutex
	txHold    = map[string]bool{}
	txPending = map[string]bool{}
)

// txNoteTrigger reports whether the pipeline's event-driven trigger must be
// suppressed (held); the arrival is then recorded for the coordinator.
func txNoteTrigger(name string) bool {
	txHoldMu.Lock()
	defer txHoldMu.Unlock()
	if txHold[name] {
		txPending[name] = true
		return true
	}
	return false
}

// topoOrder returns the staged pipeline names in dependency order —
// consumers after the pipelines they consume. A dependency cycle is an
// error: the coordinator would otherwise wait forever.
func topoOrder(final map[string]client.Pipeline, deps map[string][]string) ([]string, error) {
	consumers := map[string][]string{} // dep -> names that consume it
	indeg := map[string]int{}
	for name := range final {
		indeg[name] = len(deps[name])
		for _, dep := range deps[name] {
			consumers[dep] = append(consumers[dep], name)
		}
	}
	var ready []string
	for name, n := range indeg {
		if n == 0 {
			ready = append(ready, name)
		}
	}
	sort.Strings(ready)
	var order []string
	for len(ready) > 0 {
		name := ready[0]
		ready = ready[1:]
		order = append(order, name)
		for _, c := range consumers[name] {
			indeg[c]--
			if indeg[c] == 0 {
				ready = append(ready, c)
			}
		}
	}
	if len(order) != len(final) {
		return nil, fmt.Errorf("cycle")
	}
	return order, nil
}

// txCoordinate schedules each staged pipeline's head job in dependency
// order: a pipeline waits until every in-transaction pipeline it consumes
// has settled — the exact job the coordinator scheduled for it is terminal
// (a goroutine-registration race would otherwise let the downstream
// schedule against a stale head) — then claims the head job, re-scheduling
// if a commit arrived while it was being scheduled, so exactly one job
// processes the coherent post-transaction head (SB-162).
func (d *daemon) txCoordinate(final map[string]client.Pipeline, deps map[string][]string, order []string) {
	scheduled := map[string]string{} // pipeline -> id of its coordinator-scheduled head job
	for _, name := range order {
		for _, dep := range deps[name] {
			d.waitJobSettled(scheduled[dep])
		}
		rec, err := d.loadPipeline(name)
		if err != nil {
			d.txUnhold(name)
			continue
		}
		last := ""
		for {
			txHoldMu.Lock()
			txHold[name] = true
			txPending[name] = false
			txHoldMu.Unlock()
			last = d.scheduleHeadJob(rec)
			txHoldMu.Lock()
			again := txPending[name]
			txHold[name] = false
			txHoldMu.Unlock()
			if !again {
				break
			}
		}
		scheduled[name] = last
		if last == "" {
			d.standbyIdle(rec) // a standby pipeline with nothing to process parks in standby
		}
	}
}

func (d *daemon) txUnhold(name string) {
	txHoldMu.Lock()
	txHold[name] = false
	txPending[name] = false
	txHoldMu.Unlock()
}

// waitJobSettled blocks until the named job's record is terminal. An
// empty id (nothing was scheduled) settles immediately.
func (d *daemon) waitJobSettled(id string) {
	if id == "" {
		return
	}
	for {
		rec, err := d.loadJobRec(id)
		if err == nil && rec.State != "running" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ---- HTTP surface ----

func (d *daemon) startTransactionH(w http.ResponseWriter, r *http.Request) error {
	id, err := d.startTransaction()
	if err != nil {
		return err
	}
	writeJSON(w, map[string]string{"id": id})
	return nil
}

func (d *daemon) finishTransactionH(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("id")
	if err := d.finishTransaction(id); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"id": id, "status": "finished"})
	return nil
}

func (d *daemon) deleteTransactionH(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("id")
	if err := d.deleteTransaction(id); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"id": id, "status": "deleted"})
	return nil
}
