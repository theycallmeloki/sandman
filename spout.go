// Spout pipelines (SB-139): a pipeline with no input whose transform runs
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

// spawnSpoutJob starts a spout's background job.
func (d *daemon) spawnSpoutJob(rec *pipelineRec) string {
	id := newJobID(d.name)
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
	rj := registerRunning(id, pl.Pipeline.Name)
	defer unregisterRunning(id, rj)
	defer d.standbySettle(pl.Pipeline.Name)

	rec := newJobRec(pl, nil, id)
	d.saveJob(rec)
	gate := d.jobGate(pl.Pipeline.Name)
	if !gate.enter(rj) {
		rec.State = "killed"
		rec.Reason = "job cancelled"
		rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := os.Stat(filepath.Join(dir, "job.json")); err == nil {
			d.saveJob(rec)
		}
		return
	}
	defer gate.release()

	cname := fmt.Sprintf("sandman-%s-spout", id)
	rj.registerContainer(cname)
	defer rj.unregisterContainer(cname)

	env := []string{"OUT=/sandman/out", "JOB_ID=" + id}
	mounts := []string{"-v", outDir + ":/sandman/out"}
	markerDir := ""
	if pl.Pipeline.Spout.Marker != "" {
		markerDir = filepath.Join(dir, "marker")
		os.MkdirAll(markerDir, 0o755)
		env = append(env, "MARKER=/sandman/marker")
		mounts = append(mounts, "-v", markerDir+":/sandman/marker")
	}
	argv := []string{"run", "-d", "--name", cname, "-e", "OUT=/sandman/out", "-e", "JOB_ID=" + id}
	if markerDir != "" {
		argv = append(argv, "-e", "MARKER=/sandman/marker")
	}
	argv = append(argv, mounts...)
	argv = append(argv, pl.Pipeline.Transform.Image, "sh", "-c", joinSh(pl.Pipeline.Transform.Cmd))
	if pl.Pipeline.Transform.Image == "" {
		argv[len(argv)-4] = "alpine"
	}
	if exec.Command("docker", argv...).Run() != nil {
		rec.State = "failure"
		rec.Reason = "spout container failed to start"
		rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
		d.saveJob(rec)
		return
	}

	committedOut := map[string]string{}
	committedMarker := map[string]string{}
	settle := func(state, reason string) {
		rec.State = state
		rec.Reason = reason
		rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
		d.saveJob(rec)
	}
	for {
		if rj.cancelled.Load() {
			exec.Command("docker", "kill", cname).Run()
			settle("killed", "job cancelled")
			break
		}
		// the container exited? (a natural end settles the job)
		if out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", cname).Output(); err != nil {
			settle("failure", "spout container exited unexpectedly")
			break
		} else if strings.TrimSpace(string(out)) != "true" {
			exec.Command("docker", "rm", "-f", cname).Run()
			settle("success", "")
			break
		}
		if changed := spoutDiff(outDir, committedOut); len(changed) > 0 {
			d.spoutCommit(outDir, changed, outputBranch(pl), pl.Pipeline.Name, rj)
			committedOut = spoutSnapshot(outDir)
		}
		if markerDir != "" {
			if changed := spoutDiff(markerDir, committedMarker); len(changed) > 0 {
				d.spoutCommit(markerDir, changed, markerBranch, pl.Pipeline.Name, rj)
				committedMarker = spoutSnapshot(markerDir)
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	exec.Command("docker", "rm", "-f", cname).Run()
}

// spoutCommit commits a data-bearing cycle: the changed files with their
// current content, one finished commit that triggers the consumers.
func (d *daemon) spoutCommit(dir string, changed map[string]string, branch, repo string, rj *runningJob) {
	if rj.cancelled.Load() {
		return
	}
	cm, err := d.store.startCommit(repo, branch, "")
	if err != nil {
		return
	}
	for _, p := range sortedStringKeys(changed) {
		if data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(p))); err == nil {
			d.store.putFile(cm.ID, p, data)
		}
	}
	if fin, err := d.store.finishCommit(cm.ID, "", false); err == nil {
		d.triggerForCommit(fin)
	}
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
