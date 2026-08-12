package main

// Job execution: runJob and the execution-backend seam (D-23) — the
// containerRunner/processRunner that run one pipeline transform, and the
// datum execution glue. The rest of the control plane lives in
// pipeline.go (spec validation + CRUD) and jobs.go (lifecycle).

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"sandman/client"
)

// runJob coordinates one job: enumerate the input sides' datums, take
// their cartesian product, run the datums with a bounded worker pool, merge
// their outputs into the single output commit, and record the per-datum
// outcomes in the pipeline's dedup table. heads is the input pairing — one
// commit per side, empty where a side has no head (SB-120's lone-input
// job; its cross contributes no datums).
// outputBranch returns the branch a pipeline's output commits land on
// (default "master", SB-142).
func outputBranch(pl pipelineRec) string {
	if pl.Pipeline.OutputBranch == "" {
		return defaultBranch
	}
	return pl.Pipeline.OutputBranch
}

func (d *daemon) runJob(pl pipelineRec, heads []client.Commit, id, propagated string, pre *jobRec, rj *runningJob) {
	sides := inputSides(pl.Pipeline.Input)
	for i := range sides {
		if sides[i].Name == "" {
			sides[i].Name = sides[i].Repo
		}
	}
	dir := d.jobDir(id)
	outDir := filepath.Join(dir, "out")
	// the defers must register before any failure path: spawnJob already
	// put the job in the running map, and an early return without them
	// leaks the running handle (a later cancel waits 30s and errors) and
	// the standby activation count (the pipeline never returns to standby)
	defer unregisterRunning(id, rj)
	// a standby pipeline returns to standby once its work settles; the
	// defer covers every terminal path (success, failure, killed)
	defer d.standbySettle(pl.Pipeline.Name)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return
	}

	rec := pre
	if rec == nil {
		rec = newJobRec(pl, heads, id)
	}
	d.saveJob(rec)

	// Per-pipeline serialization (SB-123): one job at a time, in spawn
	// order — the record is already saved, so a queued job is visible and
	// cancellable. A cancel that arrived while queued settles the job
	// killed without doing any work and passes the slot on.
	gate := d.jobGate(pl.Pipeline.Name)
	if !gate.enter(rj) {
		rec.State = stateKilled
		rec.Reason = reasonJobCancelled
		rec.Finished = now()
		if _, err := os.Stat(filepath.Join(dir, "job.json")); err == nil {
			// the record may have been deleted while the job was queued
			// (deleteJob removes the whole job directory); never resurrect
			d.saveJob(rec)
		}
		return
	}
	defer gate.release()

	if propagated != "" {
		// an upstream stage failed, so this stage fails too — recorded,
		// never executed (SB-022). The empty output commit keeps the DAG's
		// commits continuous, so the failure reaches every downstream stage
		// and the flush can walk the chain.
		rec.State = stateFailure
		rec.Reason = propagated
		rec.Finished = now()
		d.saveJob(rec) // terminal state durable before the commit finishes
		if oc, err := d.store.startCommit(pl.Pipeline.Name, outputBranch(pl), ""); err == nil {
			rec.OutputCommit = oc.ID
			d.saveJob(rec)
			d.finishOutput(pl, oc, "", true)
			d.recordProvenance(oc.ID, rec.InputCommits)
		}
		return
	}

	fail := func(reason string) {
		rec.State = stateFailure
		rec.Reason = reason
		rec.Finished = now()
		d.saveJob(rec)
	}

	// Placement (SB-167/169): a pipeline may require its work to run on a
	// host bearing a placement label. Until such a host has registered,
	// the job waits — its record was saved at trigger time, so the pending
	// work is durable — and the pipeline surfaces the outage as the
	// crashed state instead of hanging silently (SB-169 clause 1). When a
	// host bearing the label registers, the wait re-places automatically
	// and the pipeline recovers (SB-169 clause 2): the same job, the same
	// input revision, exactly one output commit. A cancel while unplaced
	// settles the job killed like any other in-flight cancel (SB-058).
	var placedHost *execHost
	if pl.Pipeline.Placement != "" {
		for {
			if h, ok := d.hosts.pick(pl.Pipeline.Placement); ok {
				placedHost = &h
				break
			}
			d.markPipelineCrashed(pl.Pipeline.Name,
				fmt.Sprintf("no execution host bearing placement label %q", pl.Pipeline.Placement))
			select {
			case <-rj.cancelCh:
				rec.State = stateKilled
				rec.Reason = reasonJobCancelled
				rec.Finished = now()
				d.saveJob(rec)
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
		d.markPipelineRunning(pl.Pipeline.Name)
	}

	// Resolve each side's input revision and enumerate its datums; the
	// job's datum set depends on the input kind: a cross takes the
	// cartesian product (SB-063), a join pairs files by their join key
	// (SB-074/075), a group collects files by their group key (SB-076). A
	// side without a head contributes no datums, so the product is empty.
	views := map[string]map[string]viewEntry{}
	sideLists := make([][]datumSide, len(sides))
	in := pl.Pipeline.Input
	// Resolve every consumed repo's head into the views: union branches
	// nested anywhere — including inside a cross — contribute their own
	// branches' heads, keyed by branch so two branches of one repo stay
	// distinct (SB-141). Sides already covered by the pairing heads are
	// left to the loop below — a manual run pins the job to specific
	// commits, and the current head must not leak into the recorded
	// input set (SB-010). The full resolved input set is recorded on the
	// job so the flush can find it (SB-078).
	seenInput := map[string]bool{}
	covered := map[string]bool{} // repo/branch covered by the pairing heads
	for _, h := range heads {
		if h.ID != "" {
			seenInput[h.ID] = true
			covered[h.Repo+"/"+h.Branch] = true
		}
	}
	resolvedHead := map[string]client.Commit{} // side key → head picked up while queued
	var resolve func(s client.Input, key string)
	resolve = func(s client.Input, key string) {
		if key == "" {
			key = s.Name
			if key == "" {
				key = s.Repo
			}
		}
		switch {
		case s.Repo != "":
			if covered[s.Repo+"/"+inputBranch(s)] {
				return
			}
			if h, err := d.store.headCommitRec(s.Repo, inputBranch(s)); err == nil && h.Finished && !seenInput[h.ID] {
				if v, err := d.store.resolveViewByID(h.ID); err == nil {
					views[key] = v
					seenInput[h.ID] = true
					rec.InputCommits = append(rec.InputCommits, h.ID)
					// a side whose spawn-time pairing was empty can pick
					// up the head that finished while the job was queued:
					// the record grows to the full pairing, and the datum
					// loop below must enumerate it too — otherwise the
					// record claims a pairing the job never executed and
					// the trigger dedup suppresses the real job (SB-019:
					// D's wave-1 commit lost)
					resolvedHead[key] = h
				}
			}
		case len(s.Union) > 0:
			for _, b := range s.Union {
				resolve(b, unionBranchKey(b))
			}
		case len(s.Cross) > 0:
			for i := range s.Cross {
				resolve(s.Cross[i], "")
			}
		}
	}
	resolve(*in, "")
	d.saveJob(rec)
	// A late head picked up while queued must not duplicate a pairing a
	// sibling job already covers: the lone side job and the pairing job
	// race — whichever runs first claims the pairing, the other settles as
	// the lone job with no output (SB-056: the lone C job for E1 must not
	// produce a second wave-1 output after the B1×E1 pairing job spawned;
	// SB-019: when no sibling exists the lone job grows into the pairing
	// itself and the trigger's dedup suppresses the duplicate). The check
	// runs under the trigger mutex, so it cannot interleave with the
	// trigger's own dedup+save.
	if len(resolvedHead) > 0 {
		triggerMu.Lock()
		dup := d.hasJobWithExactInputs(rec.Pipeline, rec.InputCommits, rec.ID)
		triggerMu.Unlock()
		if dup {
			late := map[string]bool{}
			for key, h := range resolvedHead {
				late[h.ID] = true
				delete(views, key)
			}
			var keep []string
			for _, id := range rec.InputCommits {
				if !late[id] {
					keep = append(keep, id)
				}
			}
			rec.InputCommits = keep
			resolvedHead = map[string]client.Commit{}
			d.saveJob(rec)
		}
	}
	d.saveJob(rec)
	for i, s := range sides {
		if len(s.Union) > 0 {
			// a union member of a cross contributes its own merged datums
			// even though it has no single head commit (SB-141)
			var us []datumSide
			for _, dt := range d.unionDatums(views, &s) {
				us = append(us, dt.Sides[0])
			}
			sideLists[i] = us
			continue
		}
		head := client.Commit{}
		if i < len(heads) && heads[i].ID != "" {
			head = heads[i]
		} else if h, ok := resolvedHead[sideKey(s)]; ok {
			head = h // the side's head finished while the job was queued
		}
		if head.ID == "" {
			continue
		}
		view, err := d.store.resolveViewByID(head.ID)
		if err != nil {
			fail("materialize input: " + err.Error())
			return
		}
		views[s.Name] = view
		sd := enumerateDatums(view, s.Glob)
		sd = chunkSideDatums(sd, pl.Pipeline.ChunkSpec, view)
		for j := range sd {
			sd[j].Name = s.Name
		}
		sideLists[i] = sd
	}
	var datums []datum
	switch {
	case len(in.Union) > 0:
		datums = d.unionDatums(views, in)
	case len(in.Join) > 0:
		datums = joinDatums(views, sides)
	case len(in.Group) > 0:
		datums = groupDatums(views, sides)
	default:
		datums = crossDatums(sideLists)
	}
	for i := range datums {
		if len(datums[i].Sides) == 1 && datums[i].Sides[0].Merge != nil {
			continue // a union datum already carries its merged content hash
		}
		datums[i].Hash = datumHash(d.store, views, datums[i])
	}
	for _, dt := range datums {
		rec.DatumIDs = append(rec.DatumIDs, dt.ID)
	}
	d.saveJob(rec)
	// the datum set for log filters is the first side's full input files
	// (SB-060); cross jobs filter by their sides' files.
	var logDatums []datumRef
	for i := range sides {
		head := client.Commit{}
		if i < len(heads) && heads[i].ID != "" {
			head = heads[i]
		} else if h, ok := resolvedHead[sideKey(sides[i])]; ok {
			head = h
		}
		if head.ID == "" {
			continue
		}
		if vd, err := d.store.viewDatums(head.ID); err == nil {
			logDatums = append(logDatums, vd...)
		}
	}
	rec.Datums = logDatums
	d.saveJob(rec)

	// a job whose inputs contribute no datums settles successful with
	// nothing to produce — no output commit, so an empty wave never
	// propagates through the DAG (SB-056: exactly one commit per wave;
	// SB-120's lone cross jobs produce nothing downstream)
	if len(datums) == 0 {
		rec.State = stateSuccess
		rec.Finished = now()
		d.saveJob(rec)
		return
	}

	// The job's container output is captured into the log store as it is
	// produced. A capture failure degrades to no logs, never to a broken
	// job: execution is the control plane's job, logs are the meta plane's.
	outCommit, err := d.store.startCommit(pl.Pipeline.Name, outputBranch(pl), "")
	if err != nil {
		fail("start output commit: " + err.Error())
		if strings.Contains(err.Error(), "not found") {
			// the output repository vanished (D-10): the pipeline fails with
			// a recorded reason and stops scheduling
			d.markPipelineFailed(pl.Pipeline.Name, "output repository missing")
		}
		return
	}
	rec.OutputCommit = outCommit.ID
	d.saveJob(rec)

	// Dedup (D-13): a datum whose content is unchanged from a previous
	// successful run is skipped — the pipeline does not pay for data it
	// already processed — unless the pipeline reprocesses every job
	// (SB-166). The skip is recorded on the job, never on the shared
	// record: the record must keep its last successful outcome so later
	// jobs can still skip on it (SB-085). Every datum gets a placeholder
	// record so the job's full datum set is listable mid-flight with its
	// input files (SB-080).
	dedup := d.loadDedup(pl.Pipeline.Name)
	reprocess := pl.Pipeline.Reprocess
	rec.DatumStates = map[string]string{}
	var todo []datum
	for _, dt := range datums {
		if st, ok := dedup[dt.ID]; ok && !reprocess && st.Outcome == stateSuccess && st.Hash == dt.Hash {
			rec.DatumStates[dt.ID] = stateSkipped
			continue
		}
		if _, ok := dedup[dt.ID]; !ok {
			var inputFiles []fileRef
			for _, sd := range dt.Sides {
				for _, f := range sd.Files {
					e := views[sd.Name][f]
					if h, err := e.hash(d.store); err == nil {
						inputFiles = append(inputFiles, fileRef{Path: f, Hash: h, Size: e.size()})
					}
				}
			}
			dedup[dt.ID] = datumState{Hash: dt.Hash, InputFiles: inputFiles}
		}
		todo = append(todo, dt)
	}

	jx := &jobExec{d: d, pl: pl, id: id, outDir: outDir, views: views,
		viewDirs: map[string]string{}, dedup: dedup, rj: rj, host: placedHost}
	jx.env = d.jobEnv(pl, id, outCommit.ID, sides, heads)
	// apply the pipeline's execution-environment customization
	// (SB-072/152): the document's env vars join the job environment and
	// its volumes become mounts at /sandman/volumes/<name> — an
	// emptyDir volume is a fresh per-job directory
	if custom, err := parseCustomization(pl.Pipeline.Transform); err != nil {
		// validated at creation; reaching here is a provisioning failure
		fail("customization: " + err.Error())
		d.finishOutput(pl, outCommit, "", true)
		return
	} else if custom != nil {
		for k, v := range custom.Env {
			if !reservedEnv[k] {
				jx.extraEnv = append(jx.extraEnv, k+"="+v)
			}
		}
		for name, vol := range custom.Volumes {
			host := vol.HostPath
			if vol.EmptyDir {
				host = filepath.Join(d.jobDir(id), "volumes", name)
				os.MkdirAll(host, 0o755)
			}
			jx.extraMounts = append(jx.extraMounts, "-v", host+":/sandman/volumes/"+name)
		}
	}
	// secret bindings (SB-051 clause 2, D-05): each reference's key is
	// written as a file at MountPath/<key> and/or injected as the env
	// var, so secret values reach the execution environment before the
	// job starts
	for i, m := range pl.Pipeline.Transform.Secrets {
		if !validName(m.Name) {
			fail("invalid secret name: " + m.Name)
			d.finishOutput(pl, outCommit, "", true)
			return
		}
		srec, err := d.loadSecret(m.Name)
		if err != nil {
			fail("secret: " + err.Error())
			d.finishOutput(pl, outCommit, "", true)
			return
		}
		if m.EnvVar != "" {
			v, ok := srec.Data[m.Key]
			if !ok {
				fail(fmt.Sprintf("secret %q has no key %q", m.Name, m.Key))
				d.finishOutput(pl, outCommit, "", true)
				return
			}
			jx.extraEnv = append(jx.extraEnv, m.EnvVar+"="+v)
		}
		if m.MountPath != "" {
			dir := filepath.Join(d.jobDir(id), "secrets", strconv.Itoa(i))
			os.MkdirAll(dir, 0o755)
			if m.Key != "" {
				v, ok := srec.Data[m.Key]
				if !ok {
					fail(fmt.Sprintf("secret %q has no key %q", m.Name, m.Key))
					d.finishOutput(pl, outCommit, "", true)
					return
				}
				os.WriteFile(filepath.Join(dir, m.Key), []byte(v), 0o644)
			} else {
				for k, v := range srec.Data {
					os.WriteFile(filepath.Join(dir, k), []byte(v), 0o644)
				}
			}
			jx.extraMounts = append(jx.extraMounts, "-v", dir+":"+m.MountPath)
		}
	}
	// the live execution context is visible to the datum API (restart,
	// SB-064) while the job runs
	d.liveJobs.Store(id, jx)
	defer d.liveJobs.Delete(id)

	// Whole-job deadline (SB-116): at the boundary the job is cancelled and
	// its active containers killed; it settles as killed, never as a plain
	// failure. A job that already settled is unaffected (its containers are
	// unregistered by then).
	if tr := pl.Pipeline.Transform; tr.JobTimeout != "" {
		if dur, err := time.ParseDuration(tr.JobTimeout); err == nil {
			time.AfterFunc(dur, func() {
				select {
				case <-rj.done:
					return
				default:
				}
				rj.cancelled.Store(true)
				for _, n := range rj.containerNames() {
					exec.Command("docker", "kill", n).Run()
				}
			})
		}
	}
	failedAny := d.runDatums(jx, todo)

	for _, dt := range datums {
		if _, ok := rec.DatumStates[dt.ID]; !ok {
			rec.DatumStates[dt.ID] = dedup[dt.ID].Outcome
		}
		switch rec.DatumStates[dt.ID] {
		case stateSuccess:
			rec.Processed++
		case stateRecovered:
			rec.Recovered++
		case "failed":
			rec.Failed++
		case stateSkipped:
			rec.Skipped++
		}
	}

	if failedAny {
		// All-or-nothing output: finish the commit explicitly empty. A
		// failed datum still leaves the job inspectable and the pipeline
		// schedulable (SB-082).
		killed := rj.cancelled.Load()
		if killed {
			rec.State = stateKilled
			rec.Reason = reasonJobCancelled
		} else {
			rec.State = stateFailure
			rec.Reason = failedDatumReason(dedup, datums)
		}
		rec.Finished = now()
		// the terminal state is durable before the output commit finishes,
		// so the downstream trigger observes the failure (SB-022)
		d.saveJob(rec)
		d.finishOutput(pl, outCommit, "", true)
		d.recordProvenance(outCommit.ID, rec.InputCommits)
		if !killed && !rec.Manual {
			// a failed output is still a revision: every downstream stage
			// is triggered and fails in turn (SB-022). A killed job's empty
			// output is not a processing event — stopping a pipeline must
			// not create spurious downstream commits (SB-020); neither is a
			// manual run's (SB-010).
			if fin, err := d.store.inspectCommit(outCommit.ID); err == nil {
				d.triggerForCommit(fin)
			}
		}
		if pl.Pipeline.EnableStats {
			// the failed job's datum records are still published on the
			// stats branch (SB-113: output + statistics commits)
			if statsID := d.writeStatsCommit(pl, dedup, datums); statsID != "" {
				rec.StatsCommit = statsID
			}
		}
		d.saveDedup(pl.Pipeline.Name, dedup)
		if rec.StatsCommit != "" {
			if sc, err := d.store.inspectCommit(rec.StatsCommit); err == nil {
				d.triggerForCommit(sc)
			}
		}
		return
	}

	// Merge every datum's contribution into the output directory — a
	// processed datum's fresh files, a skipped datum's carried files.
	if err := d.mergeOutputs(jx, datums); err != nil {
		d.finishOutput(pl, outCommit, "", true)
		fail("merge output: " + err.Error())
		d.saveDedup(pl.Pipeline.Name, dedup)
		return
	}

	// Upload OUT into the output commit in one batch, then finish it (which
	// may trigger downstream pipelines). The output repository may have
	// been force-deleted while the job ran (SB-146): that fails the job and
	// the pipeline rather than silently resurrecting the repo.
	if _, err := os.Stat(d.store.repoDir(pl.Pipeline.Name)); err != nil {
		d.finishOutput(pl, outCommit, "", true)
		fail("output repository missing: " + err.Error())
		d.markPipelineFailed(pl.Pipeline.Name, "output repository deleted while running")
		return
	}
	fin, err := d.finishOutput(pl, outCommit, outDir, false)
	if err != nil {
		fail("upload output: " + err.Error())
		return
	}
	d.recordProvenance(fin.ID, rec.InputCommits)
	// statistics-enabled pipelines also produce a per-job statistics
	// commit on the output repo's "stats" branch, consumable downstream
	// (SB-086, SB-113's two-commit count)
	if pl.Pipeline.EnableStats {
		if statsID := d.writeStatsCommit(pl, dedup, datums); statsID != "" {
			rec.StatsCommit = statsID
		}
	}
	// the egress step runs after the output commit succeeds: a failure to
	// write the external destination fails the job with an egress-related
	// reason — output success alone does not make the job successful
	// (SB-013)
	if pl.Pipeline.Egress != nil {
		if err := d.runEgress(pl, fin); err != nil {
			rec.State = stateFailure
			rec.Reason = "egress: " + err.Error()
			rec.Finished = now()
			d.saveJob(rec)
			d.saveDedup(pl.Pipeline.Name, dedup)
			return
		}
	}
	rec.State = stateSuccess
	rec.Finished = now()
	d.saveJob(rec)
	d.saveDedup(pl.Pipeline.Name, dedup)

	// The output commit is a real revision of the output repo: propagate —
	// unless this was a manual run, whose output is not a processing wave
	// (SB-010: runs never propagate downstream).
	if rec.Manual {
		return
	}
	d.triggerForCommit(fin)
	if rec.StatsCommit != "" {
		if sc, err := d.store.inspectCommit(rec.StatsCommit); err == nil {
			d.triggerForCommit(sc)
		}
	}
}

// copyDir copies every file under src into dst, preserving relative paths
// (the default entry point). Returns 0 on success, 1 on any failure.
func copyDir(src, dst string) int {
	ok := true
	filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			ok = false
			return nil
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			ok = false
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			ok = false
			return nil
		}
		target := filepath.Join(dst, rel)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			ok = false
			return nil
		}
		if err := os.WriteFile(target, data, 0o644); err != nil {
			ok = false
		}
		return nil
	})
	if !ok {
		return 1
	}
	return 0
}

// datumSpec describes one command run through the execution-backend seam
// (D-23): the transform's image, argv, env, mounts, resources, identity,
// and stdin, plus the PathMap translating the execution-internal paths
// (/sandman/out, /sandman/in/<side>, /sandman/view/<side>, /tmp) to the
// staging directories the process backend runs against directly.
func datumSpec(tr *client.Transform, nodeName, cname string, env []string, mounts []string, outDir string, capture io.Writer, argv, stdin []string) JobSpec {
	pathMap := map[string]string{"/sandman/out": outDir}
	for i := 0; i+1 < len(mounts); i += 2 {
		if !strings.HasPrefix(mounts[i], "-v") {
			continue
		}
		host, rest, ok := strings.Cut(mounts[i+1], ":")
		if !ok {
			continue
		}
		container, _, _ := strings.Cut(rest, ":") // drop a ":ro"/":rw" mode
		pathMap[container] = host
	}
	return JobSpec{
		Image:            tr.Image,
		NodeName:         nodeName,
		Name:             cname,
		Cmd:              argv,
		Stdin:            stdin,
		Env:              env,
		Mounts:           mounts,
		OutDir:           outDir,
		Capture:          capture,
		Workdir:          tr.Workdir,
		User:             tr.User,
		ResourceLimits:   tr.ResourceLimits,
		ResourceRequests: tr.ResourceRequests,
		PathMap:          pathMap,
	}
}

// runSpec executes one command on the daemon's execution backend (the
// control plane's executor; the worker's executor is runDatumContainer).
func (d *daemon) runSpec(tr *client.Transform, nodeName, cname string, env []string, mounts []string, outDir string, capture io.Writer, argv, stdin []string) (int, string) {
	res := d.runner.Run(datumSpec(tr, nodeName, cname, env, mounts, outDir, capture, argv, stdin))
	return res.Code, res.Tail
}

// runDatumContainer runs one command through the container backend — the
// remote execution host's worker executor (SB-167): nodeName is the host
// identity the container is labelled with.
func runDatumContainer(tr *client.Transform, nodeName, cname string, env []string, mounts []string, outDir string, capture io.Writer, argv, stdin []string) (int, string) {
	res := containerRunner{}.Run(datumSpec(tr, nodeName, cname, env, mounts, outDir, capture, argv, stdin))
	return res.Code, res.Tail
}

// shQuote single-quotes s for /bin/sh (busybox included).
func shQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// joinSh renders argv as a single shell command, each element quoted.
func joinSh(argv []string) string {
	parts := make([]string, len(argv))
	for i, a := range argv {
		parts[i] = shQuote(a)
	}
	return strings.Join(parts, " ")
}
