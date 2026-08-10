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
	"time"

	"sandman/client"
)

// pipelineRec is the persisted form of a pipeline. State (P7) is durable so
// a restarting daemon remembers what it decided.
type pipelineRec struct {
	Pipeline client.Pipeline `json:"pipeline"`
	State    string          `json:"state"` // running | stopped | standby | failure | degraded | crashed
	Reason   string          `json:"reason,omitempty"`
	Version  int             `json:"version"`
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
	return os.WriteFile(filepath.Join(dir, p.Name+".json"), b, 0o644)
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
	return rec.info(), nil
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
		Name:    rec.Pipeline.Name,
		State:   rec.State,
		Reason:  rec.Reason,
		Version: rec.Version,
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
	entries, err := os.ReadDir(filepath.Join(d.state, "jobs"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]client.Job, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			if b, err := os.ReadFile(filepath.Join(d.state, "jobs", e.Name(), "job.json")); err == nil {
				var rec jobRec
				if json.Unmarshal(b, &rec) == nil {
					out = append(out, rec.job())
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Started > out[j].Started })
	return out, nil
}

func (d *daemon) inspectJob(id string) (client.Job, error) {
	b, err := os.ReadFile(filepath.Join(d.jobDir(id), "job.json"))
	if err != nil {
		return client.Job{}, fmt.Errorf("job %q not found", id)
	}
	var rec jobRec
	if err := json.Unmarshal(b, &rec); err != nil {
		return client.Job{}, err
	}
	return rec.job(), nil
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
		if rec.Pipeline.Input.Repo == cm.Repo {
			go d.runJob(*rec, cm)
		}
	}
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
	if in.Name == "" {
		in.Name = in.Repo
	}
	id := newJobID(d.name)
	dir := d.jobDir(id)
	inDir := filepath.Join(dir, "in", in.Name)
	outDir := filepath.Join(dir, "out")
	for _, p := range []string{inDir, outDir} {
		if err := os.MkdirAll(p, 0o755); err != nil {
			return
		}
	}

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
		in.Name + "=" + "/sandman/in/" + in.Name,
		in.Name + "_COMMIT=" + cm.ID,
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
		exit, tail = d.runPipelineContainer(pl, id, env, inDir, outDir)
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
		fail(reason)
		return
	}

	// Upload OUT into the output commit, then finish it (which may trigger
	// downstream pipelines).
	filepath.Walk(outDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(outDir, p)
		if err != nil {
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return d.store.putFile(outCommit.ID, filepath.ToSlash(rel), data)
	})
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
// reporting.
func (d *daemon) runPipelineContainer(pl pipelineRec, jobID string, env []string, inDir, outDir string) (int, string) {
	tr := pl.Pipeline.Transform
	image := tr.Image
	if image == "" {
		image = "alpine"
	}
	args := []string{"run", "--rm", "--name", "sandman-" + jobID,
		"--label", "sandman.node=" + d.name,
		"-v", inDir + ":/sandman/in/" + pl.Pipeline.Input.Name + ":ro",
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
