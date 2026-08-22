// Spout pipelines: a pipeline with no input whose transform runs
// in the background; the daemon watches the container's output directory
// and commits each data-bearing cycle to the output branch (accumulating
// or replacing per the overwrite option), and the marker directory's
// files to a separate marker branch. The job settles when the container
// exits; a cancel kills it.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"sandman/client"
)

// markerBranch is the branch a spout's marker files land on.
const markerBranch = "markers"

// validateSpout enforces the spout contracts: no inputs, and a marker
// name without glob metacharacters.
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
// after a reprocess update: the marker state is reset.
func (d *daemon) spawnSpoutJob(rec *pipelineRec, fresh bool) string {
	id := newJobID(d.name)
	if fresh {
		if dir := d.spoutMarkerDir(rec.Pipeline.Name); dir != "" {
			os.RemoveAll(dir)
		}
	}
	// mirror spawnJob: the running handle is registered before
	// the goroutine starts, so a stop/delete arriving the instant the
	// spout spawns can always find it — a not-yet-registered handle
	// would escape the cancel and keep running (container up, cycles
	// committing) against a stopped or deleted pipeline
	rj := d.registerRunning(id, rec.Pipeline.Name)
	go d.runSpoutJob(*rec, id, rj)
	return id
}

// runSpoutJob runs one spout: the transform container runs detached, and
// the daemon polls its output directory, committing each data-bearing
// cycle (a set of new or changed files) to the output branch, and the
// marker directory's changes to the marker branch — empty-payload cycles
// surface no commit, and the spout keeps producing after its head commit
// is deleted. The job settles when the container exits; a cancel kills
// the container and settles killed.
// rj is the pre-registered running handle (spawnSpoutJob) and is
// unregistered on every exit, including the early MkdirAll failure.
func (d *daemon) runSpoutJob(pl pipelineRec, id string, rj *runningJob) {
	defer d.unregisterRunning(id, rj)
	defer d.standbySettle(pl.Pipeline.Name)
	dir := d.jobDir(id)
	outDir := filepath.Join(dir, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return
	}

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

	// the slot is ours: the queued record becomes running
	rec.State = stateRunning
	d.saveJob(rec)

	cname := fmt.Sprintf("sandman-%s-spout", id)
	rj.registerContainer(cname)
	defer rj.unregisterContainer(cname)

	// the container's environment is passed as env entries; the mount
	// list is the execution backend's mount vocabulary
	mounts := []string{"-v", outDir + ":/sandman/out"}
	markerDir := d.spoutMarkerDir(pl.Pipeline.Name)
	if pl.Pipeline.Spout.Marker != "" {
		os.MkdirAll(markerDir, 0o755)
		mounts = append(mounts, "-v", markerDir+":/sandman/marker")
	}
	// the container is labelled so a crashed daemon's orphan prune finds
	// it (pruneOrphans filters label=sandman.node=; unlabelled, a spout
	// container survives a daemon crash forever). The task runs detached
	// through the execution backend: the poll loop reads the exit
	// channel, so a natural end is distinguishable from an
	// unexpected-exit failure without inspecting a stopped container.
	env := []string{"OUT=/sandman/out", "JOB_ID=" + id}
	if pl.Pipeline.Spout.Marker != "" {
		// the marker dir path is always derivable, so only the declared
		// marker decides: a spout without one must not see a MARKER that
		// points at a mount that was never attached
		env = append(env, "MARKER=/sandman/marker")
	}
	image := pl.Pipeline.Transform.Image
	if image == "" {
		image = "alpine"
	}
	spec := JobSpec{
		Image:    image,
		NodeName: d.name,
		Name:     cname,
		Cmd:      []string{"sh", "-c", joinSh(pl.Pipeline.Transform.Cmd)},
		Env:      env,
		Mounts:   mounts,
		OutDir:   outDir,
	}
	// the start is bounded too (an image pull may take a while; a
	// stalled daemon beyond that fails the job instead of wedging it
	// "running" before the poll loop ever starts) — the backend's
	// provisioning budget covers it, and a start failure lands on the
	// exit channel as a provisioning error
	exited := make(chan RunResult, 1)
	go func() {
		exited <- containerRunner{}.Run(spec)
	}()

	committedOut := map[string]string{}
	committedMarker := map[string]string{}
	settle := func(state, reason string) {
		rec.State = state
		rec.Reason = reason
		rec.Finished = now()
		d.saveJob(rec)
	}
	// commitCycle snapshots and commits any data-bearing change; final
	// runs it once more at the settle with the stability verify
	// disabled — the container is gone, so the visible files are the
	// final state, and a file deferred by an earlier mid-write check
	// must not be lost to the exit.
	commitCycle := func(final bool) {
		if dbg := os.Getenv("SANDMAN_SPOUT_DEBUG"); dbg != "" {
			snap := spoutSnapshot(outDir)
			log.Printf("spout %s: poll snapshot %d files: %v", pl.Pipeline.Name, len(snap), sortedStringKeys(snap))
		}
		changed, deferred := spoutDiffVerify(outDir, committedOut, !final)
		if len(changed) > 0 {
			log.Printf("spout %s: cycle commit (final=%v) %d files: %v", pl.Pipeline.Name, final, len(changed), sortedStringKeys(changed))
			d.spoutCommit(outDir, changed, outputBranch(pl), pl.Pipeline.Name, rj, pl.SpecCommit)
			// The committed set is the previous committed files plus
			// this cycle's changed files, refreshed to their current
			// content. It is NOT the raw post-commit snapshot: a file
			// that appeared during the commit window was never written
			// by this commit, and marking it committed would silently
			// drop its cycle (observed on CI: a rapid writer's file
			// landing mid-commit was never committed nor retried).
			snap := spoutSnapshot(outDir)
			for p, h := range snap {
				if _, was := committedOut[p]; !was {
					if _, just := changed[p]; !just {
						delete(snap, p) // appeared mid-cycle: stays uncommitted, retried next poll
					}
				} else {
					snap[p] = h // refresh a previously committed file's content hash
				}
			}
			for p := range deferred {
				delete(snap, p) // still being written: must stay uncommitted for the next poll
				log.Printf("spout %s: deferred mid-write file %s (left uncommitted)", pl.Pipeline.Name, p)
			}
			committedOut = snap
		}
		if markerDir != "" {
			changed, deferred := spoutDiffVerify(markerDir, committedMarker, !final)
			if len(changed) > 0 {
				log.Printf("spout %s: marker cycle commit (final=%v) %d files: %v", pl.Pipeline.Name, final, len(changed), sortedStringKeys(changed))
				d.spoutCommit(markerDir, changed, markerBranch, pl.Pipeline.Name, rj, pl.SpecCommit)
				snap := spoutSnapshot(markerDir)
				for p, h := range snap {
					if _, was := committedMarker[p]; !was {
						if _, just := changed[p]; !just {
							delete(snap, p)
						}
					} else {
						snap[p] = h
					}
				}
				for p := range deferred {
					delete(snap, p)
					log.Printf("spout %s: deferred marker mid-write file %s (left uncommitted)", pl.Pipeline.Name, p)
				}
				committedMarker = snap
			}
		}
	}
loop:
	for {
		if rj.cancelled.Load() {
			// keep killing until the run settles: the run may still be
			// pulling the image (no container yet, a single kill is
			// lost), and the backend's run goroutine cleans up once the
			// task dies. No cycles are committed while cancelled — the
			// stop must not surface a partial wave.
			containerRunner{}.Kill(cname)
			select {
			case <-exited:
				settle(stateKilled, reasonJobCancelled)
				break loop
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}
		select {
		case res := <-exited:
			if res.ProvisioningErr != nil {
				// the container never started (bad image, runtime down):
				// settle as failure, matching a failed `run -d` start
				settle(stateFailure, "spout container failed to start")
				break loop
			}
			// the container exited naturally: commit any final cycle — a
			// deferred mid-write file must not be lost to the settle (the
			// exit code is not a gate: a spout's natural end settles the
			// job, exactly like the container backend it replaces)
			log.Printf("spout %s: container exited; committing final cycle", pl.Pipeline.Name)
			commitCycle(true)
			settle(stateSuccess, "")
			break loop
		case <-time.After(250 * time.Millisecond):
			commitCycle(false)
		}
	}
}

// spoutMarkerDir is the spout's per-pipeline marker directory: it lives
// across spout restarts, so a plain update preserves the marker file — a
// reprocess update clears it via spawnSpoutJob.
func (d *daemon) spoutMarkerDir(pipeline string) string {
	return filepath.Join(d.state, "spout", pipeline, "marker")
}

// spoutCommit commits a data-bearing cycle: the changed files with their
// current content, one finished commit that triggers the consumers. The
// commit records the pipeline's specification commit as its provenance —
// the epoch anchor: an update that writes a new spec commit starts a new
// provenance epoch shared by all commits after it.
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
// spoutDiffVerify is the cycle diff with the mid-write stability check
// optional: verify=true defers files whose content changed across a
// short window (still being written — commit them next cycle), while
// the natural-exit settle calls with verify=false — the container is
// gone, so whatever is visible is the final state, and a file deferred
// by an earlier verify must not be lost to the settle. It returns the
// changed files and the deferred set (verify deferred them; the caller
// must not record them as committed — the post-commit snapshot would
// otherwise capture their finished content and the next poll would
// never see them as changed again, silently dropping the cycle).
func spoutDiffVerify(dir string, committed map[string]string, verify bool) (changed, deferred map[string]string) {
	files := spoutSnapshot(dir)
	changed = map[string]string{}
	deferred = map[string]string{}
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
	if verify && len(changed) > 0 {
		time.Sleep(10 * time.Millisecond)
		for p := range changed {
			if b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(p))); err == nil {
				if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != changed[p] {
					deferred[p] = changed[p]
					delete(changed, p) // still being written: catch it next cycle
				}
			} else {
				deferred[p] = changed[p]
				delete(changed, p) // vanished mid-write
			}
		}
	}
	return changed, deferred
}

func sortedStringKeys(m map[string]string) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
