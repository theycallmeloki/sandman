// Spout pipelines (SB-139): a pipeline with no input whose transform runs
// in the background; the daemon watches the container's output directory
// and commits each data-bearing cycle to the output branch (accumulating
// or replacing per the overwrite option), and the marker directory's
// files to a separate marker branch. The job settles when the container
// exits; a cancel kills it.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"sandman/client"
)

// markerBranch is the branch a spout's marker files land on.
const markerBranch = "markers"

// validateSpout enforces the spout contracts: no inputs, and a marker
// name without glob metacharacters (SB-139 clauses 11/13).
func validateSpout(p client.Pipeline) error {
	if p.Spout == nil {
		return nil
	}
	if p.Input != nil {
		return fmt.Errorf("a spout pipeline cannot have inputs")
	}
	if m := p.Spout.Marker; m != "" {
		for _, c := range m {
			if c == '*' || c == '?' || c == '[' || c == ']' {
				return fmt.Errorf("invalid marker name %q", m)
			}
		}
	}
	return nil
}

// spawnSpoutJob starts a spout's background job. fresh marks a restart
// after a reprocess update: the marker state is reset (SB-139 clause 10,
// SB-140 clause 4).
func (d *daemon) spawnSpoutJob(rec *pipelineRec, fresh bool) string {
	id := newJobID(d.name)
	if fresh {
		if dir := d.spoutMarkerDir(rec.Pipeline.Name); dir != "" {
			os.RemoveAll(dir)
		}
	}
	go d.runSpoutJob(*rec, id)
	return id
}

// runSpoutJob runs one spout: the transform container runs detached, and
// the daemon polls its output directory, committing each data-bearing
// cycle (a set of new or changed files) to the output branch, and the
// marker directory's changes to the marker branch. The job settles when
// the container exits; a cancel kills the container and settles killed.
func (d *daemon) runSpoutJob(pl pipelineRec, id string) {
	dir := d.jobDir(id)
	outDir := filepath.Join(dir, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return
	}
	rj := d.registerRunning(id, pl.Pipeline.Name)
	defer d.unregisterRunning(id, rj)
	defer d.standbySettle(pl.Pipeline.Name)

	rec := newJobRec(pl, nil, id)
	d.saveJob(rec)
	gate := d.jobGate(pl.Pipeline.Name)
	if !gate.enter(rj) {
		rec.State = stateKilled
		rec.Reason = reasonJobCancelled
		rec.Finished = now()
		if _, err := os.Stat(filepath.Join(dir, "job.json")); err == nil {
			d.saveJob(rec)
		}
		return
	}
	defer gate.release()

	cname := fmt.Sprintf("sandman-%s-spout", id)
	rj.registerContainer(cname)
	defer rj.unregisterContainer(cname)

	// the container's environment is passed as docker -e flags in argv;
	// the mount list is the only other docker argument
	mounts := []string{"-v", outDir + ":/sandman/out"}
	markerDir := d.spoutMarkerDir(pl.Pipeline.Name)
	if pl.Pipeline.Spout.Marker != "" {
		os.MkdirAll(markerDir, 0o755)
		mounts = append(mounts, "-v", markerDir+":/sandman/marker")
	}
	// the container is labelled so a crashed daemon's orphan prune finds
	// it (pruneOrphans filters label=sandman.node=; unlabelled, a spout
	// container survives a daemon crash forever). --rm is deliberately
	// NOT used: the poll loop detects a natural exit via docker inspect
	// of the stopped container, and auto-removal would read as an
	// unexpected-exit failure.
	argv := []string{"run", "-d", "--name", cname, "--label", "sandman.node=" + d.name, "-e", "OUT=/sandman/out", "-e", "JOB_ID=" + id}
	if markerDir != "" {
		argv = append(argv, "-e", "MARKER=/sandman/marker")
	}
	argv = append(argv, mounts...)
	argv = append(argv, pl.Pipeline.Transform.Image, "sh", "-c", joinSh(pl.Pipeline.Transform.Cmd))
	if pl.Pipeline.Transform.Image == "" {
		argv[len(argv)-4] = "alpine"
	}
	// the start is bounded too (an image pull may take a while; a
	// stalled daemon beyond that fails the job instead of wedging it
	// "running" before the poll loop ever starts)
	sctx, scancel := context.WithTimeout(context.Background(), 120*time.Second)
	err := exec.CommandContext(sctx, "docker", argv...).Run()
	scancel()
	if err != nil {
		rec.State = stateFailure
		rec.Reason = "spout container failed to start"
		rec.Finished = now()
		d.saveJob(rec)
		return
	}

	committedOut := map[string]string{}
	committedMarker := map[string]string{}
	settle := func(state, reason string) {
		rec.State = state
		rec.Reason = reason
		rec.Finished = now()
		d.saveJob(rec)
	}
	// commitCycle snapshots and commits any data-bearing change; final
	// runs it once more at the settle so a cycle deferred by the
	// stability verify (mid-write at the last poll) is not lost to the
	// container's exit — the files are stable once it is gone.
	commitCycle := func() {
		if changed := spoutDiff(outDir, committedOut); len(changed) > 0 {
			d.spoutCommit(outDir, changed, outputBranch(pl), pl.Pipeline.Name, rj, pl.SpecCommit)
			committedOut = spoutSnapshot(outDir)
		}
		if markerDir != "" {
			if changed := spoutDiff(markerDir, committedMarker); len(changed) > 0 {
				d.spoutCommit(markerDir, changed, markerBranch, pl.Pipeline.Name, rj, pl.SpecCommit)
				committedMarker = spoutSnapshot(markerDir)
			}
		}
	}
	for {
		if rj.cancelled.Load() {
			kctx, kcancel := context.WithTimeout(context.Background(), 30*time.Second)
			exec.CommandContext(kctx, "docker", "kill", cname).Run()
			kcancel()
			settle(stateKilled, reasonJobCancelled)
			break
		}
		// the container exited? (a natural end settles the job). The
		// inspect is bounded: a stalled docker daemon must not freeze
		// the poll — a missed beat re-inspects on the next tick, while
		// an unbounded inspect was observed freezing the loop forever,
		// orphaning the exited container and leaving the job "running".
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		out, err := exec.CommandContext(ctx, "docker", "inspect", "-f", "{{.State.Running}}", cname).Output()
		cancel()
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				continue // docker is stalled; try again on the next tick
			}
			settle(stateFailure, "spout container exited unexpectedly")
			break
		} else if strings.TrimSpace(string(out)) != "true" {
			// the container is gone: commit any final cycle — a
			// deferred mid-write file must not be lost to the settle
			commitCycle()
			rmCtx, rmCancel := context.WithTimeout(context.Background(), 30*time.Second)
			exec.CommandContext(rmCtx, "docker", "rm", "-f", cname).Run()
			rmCancel()
			settle(stateSuccess, "")
			break
		}
		commitCycle()
		time.Sleep(250 * time.Millisecond)
	}
	rmCtx, rmCancel := context.WithTimeout(context.Background(), 30*time.Second)
	exec.CommandContext(rmCtx, "docker", "rm", "-f", cname).Run()
	rmCancel()
}

// spoutMarkerDir is the spout's per-pipeline marker directory: it lives
// across spout restarts, so a plain update preserves the marker file
// (SB-139 clause 10) — a reprocess update clears it via spawnSpoutJob.
func (d *daemon) spoutMarkerDir(pipeline string) string {
	return filepath.Join(d.state, "spout", pipeline, "marker")
}

// spoutCommit commits a data-bearing cycle: the changed files with their
// current content, one finished commit that triggers the consumers. The
// commit records the pipeline's specification commit as its provenance —
// the epoch anchor (SB-139 clause 7, SB-140 clause 3).
func (d *daemon) spoutCommit(dir string, changed map[string]string, branch, repo string, rj *runningJob, specCommit string) {
	if rj.cancelled.Load() {
		return
	}
	var prov []string
	if specCommit != "" {
		prov = []string{specCommit}
	}
	d.commitRevision(repo, branch, func(commitID string) bool {
		for _, p := range sortedStringKeys(changed) {
			if data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(p))); err == nil {
				d.store.OverwriteFile(commitID, p, data)
			}
		}
		return true
	}, prov)
}

// spoutSnapshot maps a directory's files to their content hashes.
func spoutSnapshot(dir string) map[string]string {
	out := map[string]string{}
	filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return nil
		}
		if data, err := os.ReadFile(p); err == nil {
			sum := sha256.Sum256(data)
			out[filepath.ToSlash(rel)] = hex.EncodeToString(sum[:])
		}
		return nil
	})
	return out
}

// spoutDiff returns the paths whose content differs from the last cycle's
// snapshot (new or changed files — the cycle's data).
func spoutDiff(dir string, committed map[string]string) map[string]string {
	files := spoutSnapshot(dir)
	changed := map[string]string{}
	for p, h := range files {
		if committed[p] != h {
			changed[p] = h
		}
	}
	// A changed file caught mid-write (a non-atomic ">" truncates before
	// writing; the daemon polls on a directory watch with no open/close
	// signal) would commit an empty or partial snapshot — observed as a
	// zero-byte head the downstream then copied. Verify each changed
	// file is stable across a short window and defer mid-write files to
	// the next poll: the writer's final state is stable and commits.
	if len(changed) > 0 {
		time.Sleep(10 * time.Millisecond)
		for p := range changed {
			if b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(p))); err == nil {
				if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != changed[p] {
					delete(changed, p) // still being written: catch it next cycle
				}
			} else {
				delete(changed, p) // vanished mid-write
			}
		}
	}
	return changed
}

func sortedStringKeys(m map[string]string) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
