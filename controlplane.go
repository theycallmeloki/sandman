package main

// Control plane: pipelines and jobs, stored as plain JSON records under
// <state>/pipelines/<name>.json and <state>/jobs/<id>/job.json (Rule of
// Transparency). Finishing a commit triggers one job per pipeline whose
// input repo it belongs to; a job runs the pipeline's transform in a
// throwaway container, uploads the OUT directory into a new commit of the
// pipeline's output repo, and records success/failure.

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sandman/client"
)

// pipelineRec is the persisted form of a pipeline. State (P7) is durable so
// a restarting daemon remembers what it decided. Stopped is a persistent
// flag distinct from the transient state (SB-028); StoppedAt is the input
// branch head when the pipeline was stopped, the watermark for the backlog
// replayed on start (SB-048).
type pipelineRec struct {
	Pipeline  client.Pipeline `json:"pipeline"`
	State     string          `json:"state"` // running | stopped | standby | failure | degraded | crashed
	Reason    string          `json:"reason,omitempty"`
	Stopped   bool            `json:"stopped,omitempty"`
	StoppedAt string          `json:"stoppedAt,omitempty"`
	Version   int             `json:"version"`
}

var shIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// reservedEnv are the job environment variables owned by the system; a
// custom environment variable may not shadow them (package client docs).
var reservedEnv = map[string]bool{
	"OUT": true, "JOB_ID": true, "OUTPUT_COMMIT": true,
}

// createPipeline validates and persists a pipeline. The validation order is
// the SB-159 contract: spec present, name, transform, input, then input
// fields (name → reserved "out" → repo → glob), then cross-references
// (self-reference before repo existence, so a pipeline never mistakes its
// own future output repo for a missing input).
func (d *daemon) createPipeline(p client.Pipeline) error {
	if p.Name == "" && p.Transform == nil {
		return fmt.Errorf("invalid pipeline spec")
	}
	if p.Name == "" {
		return fmt.Errorf("pipeline must specify a name")
	}
	if p.Transform == nil {
		return fmt.Errorf("pipeline must specify a transform")
	}
	if p.Input == nil {
		return fmt.Errorf("no input set")
	}
	in := *p.Input
	if in.Name == "" {
		in.Name = in.Repo // an input's environment variable is named after its repo
	}
	if in.Name == "" {
		return fmt.Errorf("input must specify a name")
	}
	if in.Name == "out" {
		return fmt.Errorf(`input cannot be named "out"`)
	}
	if in.Repo == "" {
		return fmt.Errorf("input must specify a repo")
	}
	if in.Glob == "" {
		return fmt.Errorf("input must specify a glob")
	}
	if !shIdent.MatchString(in.Name) {
		return fmt.Errorf("input name %q is not a valid environment variable name", in.Name)
	}
	if in.Repo == p.Name {
		return fmt.Errorf("pipeline cannot have its output as an input")
	}
	if _, err := os.Stat(d.store.repoDir(in.Repo)); err != nil {
		return fmt.Errorf("input repo %q not found", in.Repo)
	}
	if p.Parallelism != nil && p.Parallelism.Constant != 0 && p.Parallelism.Coefficient != 0 {
		return fmt.Errorf("cannot specify both a constant and a coefficient of parallelism")
	}
	dir := filepath.Join(d.state, "pipelines")
	if _, err := os.Stat(filepath.Join(dir, p.Name+".json")); err == nil {
		return fmt.Errorf("pipeline %q already exists", p.Name)
	}

	rec := pipelineRec{Pipeline: p, State: "running", Version: 1}
	if len(p.Transform.Cmd) == 0 && len(p.Transform.Stdin) > 0 {
		// No command to feed the stdin lines to: accepted, but the pipeline
		// fails as soon as it would start (SB-149).
		rec.State = "failure"
		rec.Reason = "no command specified but stdin lines provided"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, p.Name+".json"), b, 0o644); err != nil {
		return err
	}

	// A pipeline created over existing history processes the current branch
	// head once, in one output commit (SB-023, SB-053). Failure and stopped
	// pipelines do not run.
	if rec.State != "failure" && !rec.Stopped {
		if head, err := d.store.headCommitRec(in.Repo, defaultBranch); err == nil && head.Finished {
			go d.runJob(rec, head)
		}
	}
	return nil
}

// stopPipeline pauses the pipeline: the persistent Stopped flag is set, the
// transient state reports "paused" (SB-028), and the input head at stop
// time becomes the backlog watermark.
func (d *daemon) stopPipeline(name string) error {
	rec, err := d.loadPipeline(name)
	if err != nil {
		return fmt.Errorf("pipeline %q not found", name)
	}
	rec.Stopped = true
	rec.State = "paused"
	if head, err := d.store.headCommitRec(rec.Pipeline.Input.Repo, defaultBranch); err == nil {
		rec.StoppedAt = head.ID
	}
	return d.savePipeline(rec)
}

// startPipeline resumes the pipeline and replays the backlog: every commit
// finished while it was stopped, oldest first, that has no job from this
// pipeline (SB-048).
func (d *daemon) startPipeline(name string) error {
	rec, err := d.loadPipeline(name)
	if err != nil {
		return fmt.Errorf("pipeline %q not found", name)
	}
	if !rec.Stopped {
		return nil // already running
	}
	stopAt := rec.StoppedAt
	rec.Stopped = false
	rec.State = "running"
	rec.StoppedAt = ""
	if err := d.savePipeline(rec); err != nil {
		return err
	}
	for _, cid := range d.store.chainFromHead(rec.Pipeline.Input.Repo, defaultBranch, stopAt) {
		if !d.hasJob(rec.Pipeline.Name, cid) {
			if cm, err := d.store.inspectCommit(cid); err == nil {
				go d.runJob(*rec, cm)
			}
		}
	}
	return nil
}

func (d *daemon) savePipeline(rec *pipelineRec) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return os.WriteFile(d.pipelinePath(rec.Pipeline.Name), b, 0o644)
}

// hasJob reports whether the pipeline already ran a job for the commit.
func (d *daemon) hasJob(pipeline, commitID string) bool {
	for _, j := range d.mustListJobs() {
		if j.Pipeline != pipeline {
			continue
		}
		for _, ic := range j.InputCommits {
			if ic == commitID {
				return true
			}
		}
	}
	return false
}

func (d *daemon) pipelinePath(name string) string {
	return filepath.Join(d.state, "pipelines", name+".json")
}

func (d *daemon) inspectPipeline(name string) (client.PipelineInfo, error) {
	b, err := os.ReadFile(d.pipelinePath(name))
	if err != nil {
		return client.PipelineInfo{}, fmt.Errorf("pipeline %q not found", name)
	}
	var rec pipelineRec
	if err := json.Unmarshal(b, &rec); err != nil {
		return client.PipelineInfo{}, err
	}
	info := rec.info()
	info.JobCounts = map[string]int{}
	for _, j := range d.mustListJobs() {
		if j.Pipeline == name {
			info.JobCounts[j.State]++
		}
	}
	return info, nil
}

func (d *daemon) listPipelines() ([]client.PipelineInfo, error) {
	entries, err := os.ReadDir(filepath.Join(d.state, "pipelines"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]client.PipelineInfo, 0, len(entries))
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			b, err := os.ReadFile(filepath.Join(d.state, "pipelines", e.Name()))
			if err != nil {
				continue
			}
			var rec pipelineRec
			if json.Unmarshal(b, &rec) == nil {
				out = append(out, rec.info())
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (rec *pipelineRec) info() client.PipelineInfo {
	return client.PipelineInfo{
		Name:        rec.Pipeline.Name,
		State:       rec.State,
		Reason:      rec.Reason,
		Description: rec.Pipeline.Description,
		Stopped:     rec.Stopped,
		Version:     rec.Version,
	}
}

func (d *daemon) deletePipeline(name string) error {
	if err := os.Remove(d.pipelinePath(name)); err != nil {
		return fmt.Errorf("pipeline %q not found", name)
	}
	return nil
}

// ---- jobs ----

// jobRec is the persisted form of a job.
type jobRec struct {
	ID           string   `json:"id"`
	Pipeline     string   `json:"pipeline"`
	State        string   `json:"state"` // running | success | failure | killed | skipped
	Reason       string   `json:"reason,omitempty"`
	InputCommits []string `json:"inputCommits,omitempty"`
	OutputCommit string   `json:"outputCommit,omitempty"`
	Started      string   `json:"started,omitempty"`
	Finished     string   `json:"finished,omitempty"`
}

func (d *daemon) jobDir(id string) string {
	return filepath.Join(d.state, "jobs", id)
}

func (rec *jobRec) job() client.Job {
	return client.Job{
		ID:           rec.ID,
		Pipeline:     rec.Pipeline,
		State:        rec.State,
		Reason:       rec.Reason,
		InputCommits: rec.InputCommits,
		OutputCommit: rec.OutputCommit,
		Started:      rec.Started,
		Finished:     rec.Finished,
	}
}

func (d *daemon) saveJob(rec *jobRec) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(d.jobDir(rec.ID), "job.json"), b, 0o644)
}

func (d *daemon) listJobs() ([]client.Job, error) {
	return d.listJobsFiltered("", "", nil, false)
}

// listJobsFiltered lists jobs, applying the pipeline, output-commit, and
// inclusive state-set filters (SB-093, SB-095). With full set, each job
// carries its pipeline's transform and input spec (SB-094).
func (d *daemon) listJobsFiltered(pipeline, outputCommit string, states []string, full bool) ([]client.Job, error) {
	entries, err := os.ReadDir(filepath.Join(d.state, "jobs"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]client.Job, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(d.state, "jobs", e.Name(), "job.json"))
		if err != nil {
			continue
		}
		var rec jobRec
		if json.Unmarshal(b, &rec) != nil {
			continue
		}
		if pipeline != "" && rec.Pipeline != pipeline {
			continue
		}
		if outputCommit != "" && rec.OutputCommit != outputCommit {
			continue
		}
		if len(states) > 0 {
			match := false
			for _, s := range states {
				if s == rec.State {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		j := rec.job()
		if full {
			if p, err := d.loadPipeline(rec.Pipeline); err == nil {
				j.Transform = p.Pipeline.Transform
				j.Input = p.Pipeline.Input
			}
		}
		out = append(out, j)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Started > out[j].Started })
	return out, nil
}

func (d *daemon) inspectJob(id string) (client.Job, error) {
	if b, err := os.ReadFile(filepath.Join(d.jobDir(id), "job.json")); err == nil {
		var rec jobRec
		if json.Unmarshal(b, &rec) == nil {
			return rec.job(), nil
		}
	}
	// a job may also be keyed by the output commit it produced (SB-135)
	for _, j := range d.mustListJobs() {
		if j.OutputCommit == id {
			return j, nil
		}
	}
	return client.Job{}, fmt.Errorf("job %q not found: specify a Job or an OutputCommit", id)
}

// markStaleJobsFailed repairs the state after a daemon restart: jobs that
// were running when the daemon died can never finish here (their containers
// were orphaned and will be pruned), so they are recorded as failed.
func (d *daemon) markStaleJobsFailed() {
	for _, j := range d.mustListJobs() {
		if j.State == "running" {
			rec := jobRec{ID: j.ID, Pipeline: j.Pipeline, State: "failure",
				Reason: "daemon restarted mid-job", InputCommits: j.InputCommits,
				OutputCommit: j.OutputCommit, Started: j.Started, Finished: time.Now().UTC().Format(time.RFC3339)}
			d.saveJob(&rec)
		}
	}
}

func (d *daemon) mustListJobs() []client.Job {
	jobs, _ := d.listJobs()
	return jobs
}

func newJobID(node string) string {
	b := make([]byte, 6)
	rand.Read(b)
	return node + "-" + hex.EncodeToString(b)
}

// triggerForCommit launches one job per running pipeline subscribed to the
// commit's repo. Jobs run in their own goroutines; the trigger never blocks
// the caller (the HTTP handler that finished the commit).
func (d *daemon) triggerForCommit(cm client.Commit) {
	pipes, _ := d.listPipelines()
	for _, p := range pipes {
		if p.State == "failure" {
			continue
		}
		rec, err := d.loadPipeline(p.Name)
		if err != nil || rec.Pipeline.Input == nil {
			continue
		}
		if rec.Stopped {
			continue // stopped pipelines ignore new commits (SB-048)
		}
		if rec.Pipeline.Input.Repo == cm.Repo {
			go d.runJob(*rec, cm)
		}
	}
}

// runningJob is the handle on an in-flight job's container; done signals
// the job goroutine has settled, cancelled distinguishes a deliberate kill
// from a plain failure (SB-122).
type runningJob struct {
	cancelled atomic.Bool
	done      chan struct{}
}

var (
	jobsMu  sync.Mutex
	running = map[string]*runningJob{}
)

func registerRunning(id string) *runningJob {
	rj := &runningJob{done: make(chan struct{})}
	jobsMu.Lock()
	running[id] = rj
	jobsMu.Unlock()
	return rj
}

func unregisterRunning(id string, rj *runningJob) {
	jobsMu.Lock()
	delete(running, id)
	jobsMu.Unlock()
	close(rj.done)
}

// cancelJob kills a running job and waits for it to settle as KILLED. A
// terminal job cancels to a no-op. The kill retries: a job can be cancelled
// the instant it appears, before its container exists (docker run still
// starting), and a single kill would be silently lost.
func (d *daemon) cancelJob(id string) error {
	if _, err := d.inspectJob(id); err != nil {
		return err
	}
	jobsMu.Lock()
	rj, ok := running[id]
	jobsMu.Unlock()
	if !ok {
		return nil // already terminal
	}
	rj.cancelled.Store(true)
	go func() {
		for i := 0; i < 120; i++ { // ~30s of retries, or until the job settles
			select {
			case <-rj.done:
				return
			default:
			}
			if exec.Command("docker", "kill", "sandman-"+id).Run() == nil {
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
	}()
	select {
	case <-rj.done:
		return nil
	case <-time.After(30 * time.Second):
		return fmt.Errorf("job %q did not settle after cancel", id)
	}
}

// deleteJob removes a job's record. A running job is first cancelled, which
// finalizes its output revision (SB-057).
func (d *daemon) deleteJob(id string) error {
	if _, err := d.inspectJob(id); err != nil {
		return err
	}
	if err := d.cancelJob(id); err != nil {
		return err
	}
	return os.RemoveAll(d.jobDir(id))
}

func (d *daemon) loadPipeline(name string) (*pipelineRec, error) {
	b, err := os.ReadFile(d.pipelinePath(name))
	if err != nil {
		return nil, err
	}
	var rec pipelineRec
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// runJob executes one job: materialize the input view, run the transform
// (or the default copy entry point), upload the OUT directory into a new
// commit of the pipeline's output repo, and finish it. All or nothing: a
// failed job's output commit is finished empty, so partial output is never
// observable.
func (d *daemon) runJob(pl pipelineRec, cm client.Commit) {
	in := pl.Pipeline.Input
	inName := in.Name
	if inName == "" {
		inName = in.Repo
	}
	id := newJobID(d.name)
	dir := d.jobDir(id)
	inDir := filepath.Join(dir, "in", inName)
	outDir := filepath.Join(dir, "out")
	for _, p := range []string{inDir, outDir} {
		if err := os.MkdirAll(p, 0o755); err != nil {
			return
		}
	}
	rj := registerRunning(id)
	defer unregisterRunning(id, rj)

	rec := &jobRec{ID: id, Pipeline: pl.Pipeline.Name, State: "running",
		InputCommits: []string{cm.ID}, Started: time.Now().UTC().Format(time.RFC3339)}
	d.saveJob(rec)
	fail := func(reason string) {
		rec.State = "failure"
		rec.Reason = reason
		rec.Finished = time.Now().UTC().Format(time.RFC3339)
		d.saveJob(rec)
	}

	// Materialize the input revision into the job's input directory. A
	// failure here means the input vanished — nothing to run.
	if err := d.store.materializeInput(cm.ID, inDir); err != nil {
		fail("materialize input: " + err.Error())
		return
	}

	outCommit, err := d.store.startCommit(pl.Pipeline.Name, "", "")
	if err != nil {
		fail("start output commit: " + err.Error())
		return
	}
	rec.OutputCommit = outCommit.ID
	d.saveJob(rec)

	env := []string{
		inName + "=" + "/sandman/in/" + inName,
		inName + "_COMMIT=" + cm.ID,
		"OUT=/sandman/out",
		"JOB_ID=" + id,
		"OUTPUT_COMMIT=" + outCommit.ID,
	}
	for k, v := range pl.Pipeline.Transform.Env {
		if !reservedEnv[k] {
			env = append(env, k+"="+v)
		}
	}

	var exit int
	var tail string
	if len(pl.Pipeline.Transform.Cmd) == 0 && len(pl.Pipeline.Transform.Stdin) == 0 {
		// Default entry point (SB-126): copy every input file to OUT.
		exit = copyDir(inDir, outDir)
	} else {
		exit, tail = d.runPipelineContainer(pl, id, inName, env, inDir, outDir)
	}

	if exit != 0 {
		reason := fmt.Sprintf("job exited with status %d", exit)
		if tail != "" {
			reason += "\n" + tail
		}
		if len(reason) > 4000 {
			reason = reason[len(reason)-4000:]
		}
		// All-or-nothing output: finish the commit explicitly empty.
		d.store.finishCommit(outCommit.ID, "", true)
		if rj.cancelled.Load() {
			rec.State = "killed"
			rec.Reason = "job cancelled"
		} else {
			rec.State = "failure"
			rec.Reason = reason
		}
		rec.Finished = time.Now().UTC().Format(time.RFC3339)
		d.saveJob(rec)
		return
	}

	// Upload OUT into the output commit in one batch, then finish it (which
	// may trigger downstream pipelines).
	if err := d.store.addFilesFromDir(outCommit.ID, outDir); err != nil {
		fail("upload output: " + err.Error())
		return
	}
	fin, err := d.store.finishCommit(outCommit.ID, "", false)
	if err != nil {
		fail("finish output commit: " + err.Error())
		return
	}
	rec.State = "success"
	rec.Finished = time.Now().UTC().Format(time.RFC3339)
	d.saveJob(rec)

	// The output commit is a real revision of the output repo: propagate.
	d.triggerForCommit(fin)
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

// runPipelineContainer runs the transform in a throwaway container and
// returns its exit code plus a tail of combined stderr/stdout for failure
// reporting. inName is the input's environment variable name, which also
// names the in-container mount point.
func (d *daemon) runPipelineContainer(pl pipelineRec, jobID, inName string, env []string, inDir, outDir string) (int, string) {
	tr := pl.Pipeline.Transform
	image := tr.Image
	if image == "" {
		image = "alpine"
	}
	args := []string{"run", "--rm", "--name", "sandman-" + jobID,
		"--label", "sandman.node=" + d.name,
		"-v", inDir + ":/sandman/in/" + inName + ":ro",
		"-v", outDir + ":/sandman/out",
	}
	for _, e := range env {
		args = append(args, "-e", e)
	}
	if len(tr.Stdin) > 0 {
		args = append(args, "-i")
	}
	workdir := tr.Workdir
	if workdir == "" {
		workdir = "/sandman/out"
	}
	args = append(args, "-w", workdir)

	argv := tr.Cmd
	if tr.User != "" {
		// Run user code as the configured identity: create the user and
		// working directory in-container, then su to it. Needs a
		// busybox-style image (alpine): adduser + su are provided.
		inner := "cd " + shQuote(workdir) + " && exec " + joinSh(argv)
		script := fmt.Sprintf("adduser -D %s 2>/dev/null; mkdir -p %s; chown -R %s %s 2>/dev/null; chown %s /sandman/out 2>/dev/null; su %s -c %s",
			shQuote(tr.User), shQuote(workdir), shQuote(tr.User), shQuote(workdir), shQuote(tr.User), shQuote(tr.User), shQuote(inner))
		argv = []string{"sh", "-c", script}
	}

	cmd := exec.Command("docker", append(append(args, image), argv...)...)
	if len(tr.Stdin) > 0 {
		cmd.Stdin = strings.NewReader(strings.Join(tr.Stdin, "\n") + "\n")
	}
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	runErr := cmd.Run()
	code := 0
	if runErr != nil {
		if ee, ok := runErr.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else {
			code = 1
		}
	}
	tail := buf.String()
	if len(tail) > 2000 {
		tail = tail[len(tail)-2000:]
	}
	return code, tail
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
