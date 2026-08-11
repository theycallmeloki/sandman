// Size-based commit triggers (SB-160): bytes newly committed to a watched
// branch accumulate durably, and every completed threshold unit commits
// the accumulated data to the input's dedicated accumulation branch and
// runs the pipeline on it. The branch name is deterministic (pipeline +
// input position) and reused across updates, so accumulated state is
// never orphaned.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"sandman/client"
)

// triggerBranch is a trigger input's deterministic accumulation branch:
// the pipeline name plus the input's position, stable across pipeline
// updates (SB-160 clause 7).
func triggerBranch(pipeline string, pos int) string {
	return fmt.Sprintf("%s-%d", pipeline, pos)
}

// deriveTriggerBranches resolves a pipeline spec's trigger inputs: the
// input's branch becomes its accumulation branch, and the trigger keeps
// the watched branch (default master).
func (d *daemon) deriveTriggerBranches(p *client.Pipeline) {
	pos := 0
	var walk func(in *client.Input)
	walk = func(in *client.Input) {
		for i := range in.Cross {
			walk(&in.Cross[i])
		}
		for i := range in.Union {
			walk(&in.Union[i])
		}
		if in.Trigger != nil && in.Trigger.SizeBytes > 0 {
			if in.Trigger.Branch == "" {
				in.Trigger.Branch = inputBranch(*in)
			}
			in.Branch = triggerBranch(p.Name, pos)
		}
		pos++
	}
	if p.Input != nil {
		walk(p.Input)
	}
}

// triggerLedgerPath is a trigger's durable accumulation state: the bytes
// accumulated since the last firing (SB-160: durable across commits so an
// interruption never loses or double-counts bytes).
func (d *daemon) triggerLedgerPath(pipeline string, pos int) string {
	return filepath.Join(d.state, "triggers", triggerBranch(pipeline, pos)+".json")
}

func (d *daemon) loadTriggerAccum(pipeline string, pos int) int64 {
	b, err := os.ReadFile(d.triggerLedgerPath(pipeline, pos))
	if err != nil {
		return 0
	}
	var accum int64
	json.Unmarshal(b, &accum)
	return accum
}

func (d *daemon) saveTriggerAccum(pipeline string, pos int, accum int64) {
	os.MkdirAll(filepath.Join(d.state, "triggers"), 0o755)
	b, _ := json.Marshal(accum)
	tmp := d.triggerLedgerPath(pipeline, pos) + ".tmp"
	os.WriteFile(tmp, b, 0o644)
	os.Rename(tmp, d.triggerLedgerPath(pipeline, pos))
}

// commitDelta is a commit's newly committed bytes: the sizes of files
// that are new or grew versus the parent's view.
func (d *daemon) commitDelta(cm client.Commit) int64 {
	view, err := d.store.resolveViewByID(cm.ID)
	if err != nil {
		return 0
	}
	var parent map[string]viewEntry
	if cm.ParentID != "" {
		parent, _ = d.store.resolveViewByID(cm.ParentID)
	}
	var delta int64
	for p, f := range view {
		pf, ok := parent[p]
		if !ok {
			delta += int64(f.size())
		} else {
			fh, _ := f.hash(d.store)
			ph, _ := pf.hash(d.store)
			if fh != ph && f.size() > pf.size() {
				delta += int64(f.size() - pf.size())
			}
		}
	}
	return delta
}

// accumulateTriggers applies every size trigger watching the commit's
// branch: the commit's new bytes accumulate, and each completed threshold
// unit fires by committing the accumulated view to the trigger's
// accumulation branch — whose finish triggers the pipeline normally.
func (d *daemon) accumulateTriggers(cm client.Commit) {
	pipes, err := d.listPipelinesFiltered(nil, "", false)
	if err != nil {
		return
	}
	for _, p := range pipes {
		rec, err := d.loadPipeline(p.Name)
		if err != nil || rec.Pipeline.Input == nil || rec.Stopped {
			continue
		}
		pos := 0
		var walk func(in *client.Input)
		walk = func(in *client.Input) {
			for i := range in.Cross {
				walk(&in.Cross[i])
			}
			if in.Trigger != nil && in.Trigger.SizeBytes > 0 && in.Repo == cm.Repo && in.Trigger.Branch == cm.Branch {
				d.fireTrigger(rec.Pipeline.Name, pos, *in, cm)
			}
			pos++
		}
		walk(rec.Pipeline.Input)
	}
}

// fireTrigger accumulates the commit's delta and fires the trigger once
// per completed threshold unit: each firing commits the watched branch's
// accumulated view to the accumulation branch (the pipeline runs on it)
// and deducts one threshold from the ledger.
func (d *daemon) fireTrigger(pipeline string, pos int, in client.Input, cm client.Commit) {
	key := triggerBranch(pipeline, pos)
	accum := d.loadTriggerAccum(pipeline, pos) + d.commitDelta(cm)
	for accum >= in.Trigger.SizeBytes {
		d.fireOnce(pipeline, key, in, cm)
		accum -= in.Trigger.SizeBytes
	}
	d.saveTriggerAccum(pipeline, pos, accum)
}

// fireOnce creates one trigger commit: the watched branch's current view
// (all accumulated data, not just the newest delta — SB-160 clause 2) on
// the accumulation branch.
func (d *daemon) fireOnce(pipeline, branch string, in client.Input, cm client.Commit) {
	snapshot, err := d.store.resolveViewByID(cm.ID)
	if err != nil {
		return
	}
	acc, err := d.store.startCommit(in.Repo, branch, "")
	if err != nil {
		return
	}
	for p, f := range snapshot {
		if b, err := f.bytes(d.store); err == nil {
			d.store.overwriteFile(acc.ID, p, b)
		}
	}
	if fin, err := d.store.finishCommit(acc.ID, "", false); err == nil {
		// the trigger commit is a real revision: trigger the pipeline
		d.triggerForCommit(fin)
	}
}

// clearTriggerLedgers removes a pipeline's trigger accumulation state
// (used by deletion and reset).
func (d *daemon) clearTriggerLedgers(pipeline string) {
	prefix := pipeline + "-"
	entries, err := os.ReadDir(filepath.Join(d.state, "triggers"))
	if err != nil {
		return
	}
	for _, e := range entries {
		if len(e.Name()) > len(prefix) && e.Name()[:len(prefix)] == prefix {
			os.Remove(filepath.Join(d.state, "triggers", e.Name()))
		}
	}
}
