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
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
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

// createPipeline validates and persists a pipeline, or updates an existing
// one when the update flag is set (SB-040). The validation order is the
// SB-159 contract: spec present, name, transform, input, then input fields
// (name → reserved "out" → repo → glob), then cross-references
// validatePipelineSpec checks the structural SB-159 rules that do not
// depend on surrounding store state: spec, name, transform, input name
// (valid shell identifier, not "out"), repo, glob, self-reference, and
// parallelism. Repo existence and name uniqueness are checked by the
// caller (they resolve differently inside a transaction, SB-162).
// validateInputSides checks an input's structure and every side in the
// SB-159 order: name → reserved "out" → repo → glob → identifier → unique
// names → self-reference. Cross members must have distinct names (same-repo
// sides are addressable separately).
func validateInputSides(in *client.Input, pipelineName string) error {
	if in == nil {
		return fmt.Errorf("no input set")
	}
	if in.Repo != "" && len(in.Cross) > 0 {
		return fmt.Errorf("input cannot specify both a repo and a cross")
	}
	names := map[string]bool{}
	for _, s := range inputSides(in) {
		if s.Name == "" {
			s.Name = s.Repo // an input's environment variable is named after its repo
		}
		if s.Name == "" {
			return fmt.Errorf("input must specify a name")
		}
		if s.Name == "out" {
			return fmt.Errorf(`input cannot be named "out"`)
		}
		if s.Repo == "" {
			return fmt.Errorf("input must specify a repo")
		}
		if s.Glob == "" {
			return fmt.Errorf("input must specify a glob")
		}
		if !shIdent.MatchString(s.Name) {
			return fmt.Errorf("input name %q is not a valid environment variable name", s.Name)
		}
		if names[s.Name] {
			return fmt.Errorf("input name %q is used by more than one input", s.Name)
		}
		names[s.Name] = true
		if s.Repo == pipelineName {
			return fmt.Errorf("pipeline cannot have its output as an input")
		}
	}
	return nil
}

func validatePipelineSpec(p client.Pipeline) error {
	if p.Name == "" && p.Transform == nil {
		return fmt.Errorf("invalid pipeline spec")
	}
	if p.Name == "" {
		return fmt.Errorf("pipeline must specify a name")
	}
	if p.Transform == nil {
		return fmt.Errorf("pipeline must specify a transform")
	}
	if err := validateInputSides(p.Input, p.Name); err != nil {
		return err
	}
	if p.Parallelism != nil && p.Parallelism.Constant != 0 && p.Parallelism.Coefficient != 0 {
		return fmt.Errorf("cannot specify both a constant and a coefficient of parallelism")
	}
	return nil
}

// (self-reference before repo existence, so a pipeline never mistakes its
// own future output repo for a missing input).
// inputSides / inputBranch are the server's aliases for the client's
// input normalization helpers.
func inputSides(in *client.Input) []client.Input { return client.InputSides(in) }
func inputBranch(s client.Input) string          { return client.InputBranch(s) }

func (d *daemon) createPipeline(p client.Pipeline) error {
	if err := validatePipelineSpec(p); err != nil {
		return err
	}
	if _, err := os.Stat(d.store.repoDir(p.Input.Repo)); err != nil {
		return fmt.Errorf("input repo %q not found", p.Input.Repo)
	}

	// update (or create) branching. A corrupt record is an incomplete
	// pipeline: not updatable, not silently recreated (SB-144).
	existing, loadErr := d.loadPipeline(p.Name)
	if loadErr == nil {
		if !p.Update {
			return fmt.Errorf("pipeline %q already exists", p.Name)
		}
		return d.updatePipeline(existing, p)
	}
	if p.Update {
		if _, err := os.Stat(d.pipelinePath(p.Name)); err == nil {
			return fmt.Errorf("pipeline %q is incomplete and cannot be updated", p.Name)
		}
	}

	rec, err := d.applyCreate(p)
	if err != nil {
		return err
	}
	d.scheduleHeadJob(rec)
	d.standbyIdle(rec) // a standby pipeline with no input head parks in standby
	return nil
}

// applyCreate persists a pipeline's version-1 metadata: the spec commit
// (SB-164) and the head record with its immutable version archive. It
// does not schedule any job; the caller decides when the pipeline runs.
func (d *daemon) applyCreate(p client.Pipeline) (*pipelineRec, error) {
	p.Update = false
	rec := pipelineRec{Pipeline: p, State: "running", Version: 1}
	if len(p.Transform.Cmd) == 0 && len(p.Transform.Stdin) > 0 {
		// No command to feed the stdin lines to: accepted, but the pipeline
		// fails as soon as it would start (SB-149).
		rec.State = "failure"
		rec.Reason = "no command specified but stdin lines provided"
	}
	// The spec commit is durable before the pipeline is considered created
	// (SB-164): a failed create leaves no spec commit behind because the
	// validation above ran first.
	d.writeSpecCommit(p.Name, p, 1)
	d.archiveVersion(&rec)
	if err := d.savePipeline(&rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// scheduleHeadJob processes the input heads once under the pipeline's
// current version — each side at its current head — when any side has a
// finished head and the pipeline is able to run (SB-023, SB-053,
// SB-042/092/143 for updates). Failure and stopped pipelines never run. It
// returns the spawned job's id, or "" when nothing was scheduled — the
// caller can wait for exactly that job to settle.
func (d *daemon) scheduleHeadJob(rec *pipelineRec) string {
	if rec.State == "failure" || rec.Stopped {
		return ""
	}
	heads := d.pairHeads(rec.Pipeline.Input)
	for _, h := range heads {
		if h.ID != "" {
			return d.spawnJob(rec, heads)
		}
	}
	return ""
}

// A standby pipeline's activation is counted so its settle hook never
// races an incoming job: spawnJob increments before the job can run, and
// the job's settle decrements and returns the pipeline to standby when the
// count reaches zero (SB-049/050: idle in standby, wake on input, rest
// again once the work is done).
var (
	standbyMu     sync.Mutex
	standbyActive = map[string]int{}
)

// spawnJob launches a job, activating a standby pipeline synchronously:
// the activation count is incremented and the state moves to "running"
// before the goroutine can start, so a settling predecessor can never
// observe quiescence while a new job is on its way. heads is the job's
// input pairing — one commit per input side, empty when a side has no
// head (its cross contributes no datums).
func (d *daemon) spawnJob(rec *pipelineRec, heads []client.Commit) string {
	if rec.Pipeline.Standby {
		standbyMu.Lock()
		standbyActive[rec.Pipeline.Name]++
		if rec.State != "running" {
			rec.State = "running"
			d.savePipeline(rec)
		}
		standbyMu.Unlock()
	}
	id := newJobID(d.name)
	go d.runJob(*rec, heads, id)
	return id
}

// standbySettle is runJob's defer: it decrements the pipeline's activation
// count and returns a standby-enabled pipeline to the standby state when
// the count reaches zero and no further work is pending (the pipeline is
// not stopped, and it did not degrade into failure or crashed). The whole
// decision runs under standbyMu, mutually exclusive with spawnJob's
// activation: a trigger that increments between the decrement and the
// state save would otherwise leave a running job under a "standby" label.
func (d *daemon) standbySettle(name string) {
	standbyMu.Lock()
	defer standbyMu.Unlock()
	standbyActive[name]--
	if standbyActive[name] < 0 {
		standbyActive[name] = 0 // non-standby job settling: nothing tracked
	}
	if standbyActive[name] > 0 {
		return
	}
	rec, err := d.loadPipeline(name)
	if err != nil || !rec.Pipeline.Standby || rec.Stopped {
		return
	}
	if rec.State == "running" {
		rec.State = "standby"
		d.savePipeline(rec)
	}
}

// standbyIdle parks a just-created or just-updated standby pipeline in the
// standby state when it has no work to do: with no finished input head on
// any side, nothing will be scheduled until a commit arrives (SB-049).
func (d *daemon) standbyIdle(rec *pipelineRec) {
	if !rec.Pipeline.Standby || rec.Stopped || rec.State == "failure" || rec.State == "crashed" {
		return
	}
	any := false
	for _, h := range d.pairHeads(rec.Pipeline.Input) {
		if h.ID != "" {
			any = true
			break
		}
	}
	if !any {
		rec.State = "standby"
		d.savePipeline(rec)
	}
}

// updatePipeline applies a new version of an existing pipeline (SB-040).
// In-flight jobs of the previous version are terminated and recorded as
// killed (SB-045); every update then processes the current input head under
// the new transform — the version transition is itself a processing event
// (SB-042, SB-092, SB-143). A stopped pipeline stays stopped (SB-044).
func (d *daemon) updatePipeline(existing *pipelineRec, p client.Pipeline) error {
	d.cancelPipelineJobs(existing.Pipeline.Name) // SB-045: no old-version work may race the new head job
	rec, err := d.applyUpdate(existing, p)
	if err != nil {
		return err
	}
	d.scheduleHeadJob(rec)
	d.standbyIdle(rec)
	return nil
}

// applyUpdate persists a new version of an existing pipeline — the spec
// commit (SB-164), the version archive, and the head record — without
// scheduling any job. In-flight work cancellation is the caller's job so
// a transaction can coordinate it.
func (d *daemon) applyUpdate(existing *pipelineRec, p client.Pipeline) (*pipelineRec, error) {
	name := existing.Pipeline.Name
	p.Update = false
	v := existing.Version + 1
	rec := pipelineRec{
		Pipeline:  p,
		State:     "running",
		Stopped:   existing.Stopped,
		StoppedAt: existing.StoppedAt,
		Version:   v,
	}
	// The dedup table is keyed by datum identity within the pipeline. An
	// update that changes the input (repo, branch, glob) makes the old
	// records meaningless: drop them so nothing is wrongly skipped.
	if !reflect.DeepEqual(existing.Pipeline.Input, p.Input) {
		os.Remove(d.dedupPath(name))
	}
	if len(p.Transform.Cmd) == 0 && len(p.Transform.Stdin) > 0 {
		rec.State = "failure"
		rec.Reason = "no command specified but stdin lines provided"
	} else if existing.Stopped {
		rec.State = "paused" // an update must not restart a paused pipeline (SB-044)
	}
	d.writeSpecCommit(name, p, v)
	d.archiveVersion(&rec)
	if err := d.savePipeline(&rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// writeSpecCommit records one pipeline definition as a commit in the spec
// repository (SB-127, SB-164): one commit per definition, written only
// after validation passed.
func (d *daemon) writeSpecCommit(name string, spec client.Pipeline, version int) {
	b, err := json.Marshal(spec)
	if err != nil {
		return
	}
	cm, err := d.store.startCommit("spec", defaultBranch, fmt.Sprintf("pipeline %s v%d", name, version))
	if err != nil {
		return
	}
	if err := d.store.putFile(cm.ID, "spec.json", b); err != nil {
		return
	}
	d.store.finishCommit(cm.ID, "", false)
}

func (d *daemon) versionPath(name string, version int) string {
	return filepath.Join(d.state, "pipelines", "versions", name, fmt.Sprintf("%d.json", version))
}

// archiveVersion persists an immutable copy of a pipeline version, keeping
// the history addressable by ancestry (SB-136).
func (d *daemon) archiveVersion(rec *pipelineRec) {
	b, err := json.Marshal(rec)
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(d.versionPath(rec.Pipeline.Name, rec.Version)), 0o755); err != nil {
		return
	}
	os.WriteFile(d.versionPath(rec.Pipeline.Name, rec.Version), b, 0o644)
}

// loadAllPipelineRecs reads every pipeline record, skipping unreadable ones
// (used by the deletion guard, which must not be wedged by an incomplete
// pipeline).
func (d *daemon) loadAllPipelineRecs() []*pipelineRec {
	entries, err := os.ReadDir(filepath.Join(d.state, "pipelines"))
	if err != nil {
		return nil
	}
	var out []*pipelineRec
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(d.state, "pipelines", e.Name()))
		if err != nil {
			continue
		}
		var rec pipelineRec
		if json.Unmarshal(b, &rec) != nil {
			continue
		}
		out = append(out, &rec)
	}
	return out
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

// startPipeline resumes the pipeline and processes the backlog: the
// commits finished while it was stopped are consumed together as one job
// over the current branch head — the accumulated view — matching SB-023's
// process-the-head-once semantics and SB-050's "commits created while
// paused are consumed together" (a job already run for the head commit is
// not re-run).
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
	chain := d.store.chainFromHead(rec.Pipeline.Input.Repo, defaultBranch, stopAt)
	if len(chain) == 0 {
		return nil
	}
	headID := chain[len(chain)-1]
	if d.hasJob(rec.Pipeline.Name, headID) {
		return nil
	}
	d.spawnJob(rec, d.pairHeads(rec.Pipeline.Input))
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

func (d *daemon) inspectPipeline(name string, ancestry int) (client.PipelineInfo, error) {
	rec, err := d.loadPipeline(name)
	if err != nil {
		if _, statErr := os.Stat(d.pipelinePath(name)); statErr == nil {
			return client.PipelineInfo{}, fmt.Errorf("pipeline %q is incomplete", name)
		}
		return client.PipelineInfo{}, fmt.Errorf("pipeline %q not found", name)
	}
	if ancestry == 0 {
		info := rec.info()
		info.JobCounts = map[string]int{}
		for _, j := range d.mustListJobs() {
			if j.Pipeline == name {
				info.JobCounts[j.State]++
			}
		}
		return info, nil
	}
	// ancestry k addresses version current-k (SB-136)
	b, err := os.ReadFile(d.versionPath(name, rec.Version-ancestry))
	if err != nil {
		return client.PipelineInfo{}, fmt.Errorf("pipeline %q has no version at ancestry %d", name, ancestry)
	}
	var old pipelineRec
	if json.Unmarshal(b, &old) != nil {
		return client.PipelineInfo{}, fmt.Errorf("pipeline %q is incomplete", name)
	}
	return old.info(), nil
}

// listPipelinesFiltered lists pipelines. history < 0 returns every
// historical version of every pipeline; otherwise one entry per pipeline
// (the current version). name restricts to one pipeline. A pipeline whose
// definition is lost makes the ordinary listing error; with allowIncomplete
// it is listed by name only (SB-144).
func (d *daemon) listPipelinesFiltered(history *int, name string, allowIncomplete bool) ([]client.PipelineInfo, error) {
	entries, err := os.ReadDir(filepath.Join(d.state, "pipelines"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			names = append(names, strings.TrimSuffix(e.Name(), ".json"))
		}
	}
	sort.Strings(names)

	var out []client.PipelineInfo
	if history != nil && *history < 0 {
		// every historical version of every (or the named) pipeline
		for _, n := range names {
			if name != "" && n != name {
				continue
			}
			vs, err := os.ReadDir(filepath.Join(d.state, "pipelines", "versions", n))
			if err != nil {
				// no archive (shouldn't happen): fall back to the head
				if rec, err := d.loadPipeline(n); err == nil {
					out = append(out, rec.info())
				}
				continue
			}
			var nums []int
			for _, v := range vs {
				if i, err := strconv.Atoi(strings.TrimSuffix(v.Name(), ".json")); err == nil {
					nums = append(nums, i)
				}
			}
			sort.Ints(nums)
			for _, v := range nums {
				if b, err := os.ReadFile(d.versionPath(n, v)); err == nil {
					var rec pipelineRec
					if json.Unmarshal(b, &rec) == nil {
						out = append(out, rec.info())
					}
				}
			}
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].Name != out[j].Name {
				return out[i].Name < out[j].Name
			}
			return out[i].Version < out[j].Version
		})
		return out, nil
	}

	for _, n := range names {
		if name != "" && n != name {
			continue
		}
		rec, err := d.loadPipeline(n)
		if err != nil {
			if allowIncomplete {
				out = append(out, client.PipelineInfo{Name: n})
				continue
			}
			return nil, fmt.Errorf("pipeline %q is incomplete", n)
		}
		out = append(out, rec.info())
	}
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
		Transform:   rec.Pipeline.Transform,
		Input:       rec.Pipeline.Input,
	}
}

// deletePipeline removes a pipeline. A pipeline whose output feeds a
// downstream pipeline is refused unless force is set (SB-026/027); in-flight
// jobs are cancelled and their records removed; the output repository is
// removed unless keepRepo is set (SB-157). An incomplete pipeline is
// deletable by name only (SB-144).
func (d *daemon) deletePipeline(name string, force, keepRepo bool) error {
	rec, loadErr := d.loadPipeline(name)
	if loadErr != nil {
		if _, err := os.Stat(d.pipelinePath(name)); err != nil {
			return fmt.Errorf("pipeline %q not found", name)
		}
		rec = nil // incomplete pipeline: name-only delete
	} else if !force {
		for _, other := range d.loadAllPipelineRecs() {
			if other.Pipeline.Name == name || other.Pipeline.Input == nil {
				continue
			}
			if inputConsumesRepo(other.Pipeline.Input, name) {
				return fmt.Errorf("pipeline %q has downstream consumers; force required", name)
			}
		}
	}
	// cancel in-flight work and wait for it to settle, then remove the job
	// records (SB-026/027: no orphaned job listings)
	d.cancelPipelineJobs(name)
	for _, j := range d.mustListJobs() {
		if j.Pipeline == name {
			os.RemoveAll(d.jobDir(j.ID))
			os.Remove(d.logPath(j.ID))
		}
	}
	os.Remove(d.pipelinePath(name))
	os.Remove(d.dedupPath(name))
	os.RemoveAll(filepath.Join(d.state, "pipelines", "versions", name))
	if !keepRepo {
		if _, err := os.Stat(d.store.repoDir(name)); err == nil {
			d.store.deleteRepo(name, true) // internal: the pipeline is gone
		}
	}
	_ = rec
	return nil
}

// ---- jobs ----

// datumRef is one input file of a job — its path and content hash — the
// per-datum handle for log filters (SB-060). A job's datum set is the
// input revision's full view.
type datumRef struct {
	Path string `json:"path"`
	Hash string `json:"hash"`
}

// jobRec is the persisted form of a job. Version, Transform, and Input are
// snapshots of the pipeline version that created the job: historical jobs
// keep their original transform across pipeline updates (SB-040, SB-143).
type jobRec struct {
	ID           string            `json:"id"`
	Pipeline     string            `json:"pipeline"`
	State        string            `json:"state"` // running | success | failure | killed | skipped
	Reason       string            `json:"reason,omitempty"`
	InputCommits []string          `json:"inputCommits,omitempty"`
	OutputCommit string            `json:"outputCommit,omitempty"`
	Started      string            `json:"started,omitempty"`
	Finished     string            `json:"finished,omitempty"`
	Version      int               `json:"version,omitempty"`
	Transform    *client.Transform `json:"transform,omitempty"`
	Input        *client.Input     `json:"input,omitempty"`
	Datums       []datumRef        `json:"datums,omitempty"`
	Processed    int               `json:"processed,omitempty"`
	Recovered    int               `json:"recovered,omitempty"`
	Failed       int               `json:"failed,omitempty"`
	Skipped      int               `json:"skipped,omitempty"`
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
		Version:      rec.Version,
		Processed:    rec.Processed,
		Recovered:    rec.Recovered,
		Failed:       rec.Failed,
		Skipped:      rec.Skipped,
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
	return d.listJobsFiltered("", "", nil, false, nil, nil)
}

// requirePipeline fails when the named pipeline does not exist or its
// definition record is corrupted (the listing of jobs or logs of a missing
// pipeline is an error, not an empty result; SB-026/027).
func (d *daemon) requirePipeline(pipeline string) error {
	if _, err := d.loadPipeline(pipeline); err != nil {
		if _, statErr := os.Stat(d.pipelinePath(pipeline)); statErr == nil {
			return fmt.Errorf("pipeline %q is incomplete", pipeline)
		}
		return fmt.Errorf("pipeline %q not found", pipeline)
	}
	return nil
}

// listJobsFiltered lists jobs, applying the pipeline, output-commit, and
// inclusive state-set filters (SB-093, SB-095), plus a history depth
// (0 = current version only, N = N most recent versions, -1 = every
// version, SB-143). Full listings carry each job's own version's transform
// and input snapshots (SB-094, SB-040). Listing jobs for a pipeline that
// does not exist is an error, not an empty list (SB-026/027).
func (d *daemon) listJobsFiltered(pipeline, outputCommit string, states []string, full bool, history *int, inputCommits []string) ([]client.Job, error) {
	if pipeline != "" {
		if err := d.requirePipeline(pipeline); err != nil {
			return nil, err
		}
	}
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
		if len(inputCommits) > 0 {
			want := map[string]bool{}
			for _, ic := range inputCommits {
				want[ic] = true
			}
			got := map[string]bool{}
			for _, jic := range rec.InputCommits {
				if want[jic] {
					got[jic] = true
				}
			}
			if len(got) != len(want) {
				continue
			}
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
		if history != nil {
			h := *history
			keep := h < 0
			if !keep {
				if cur, err := d.loadPipeline(rec.Pipeline); err == nil {
					// depth 0 = current version only; depth N = the current
					// version plus N older versions (the record's exact
					// counts: depth 1 lists the two most recent versions)
					keep = rec.Version >= cur.Version-h
				}
			}
			if !keep {
				continue
			}
		}
		if full {
			j := rec.job()
			j.Transform = rec.Transform
			j.Input = rec.Input
			out = append(out, j)
		} else {
			out = append(out, rec.job())
		}
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
// were orphaned and will be pruned), so they are recorded as failed. A
// standby pipeline whose in-flight work was lost that way has no pending
// work left and returns to standby.
func (d *daemon) markStaleJobsFailed() {
	for _, j := range d.mustListJobs() {
		if j.State == "running" {
			rec := jobRec{ID: j.ID, Pipeline: j.Pipeline, State: "failure",
				Reason: "daemon restarted mid-job", InputCommits: j.InputCommits,
				OutputCommit: j.OutputCommit, Started: j.Started, Finished: time.Now().UTC().Format(time.RFC3339Nano)}
			d.saveJob(&rec)
			if p, err := d.loadPipeline(j.Pipeline); err == nil && p.Pipeline.Standby && p.State == "running" {
				p.State = "standby"
				d.savePipeline(p)
			}
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
// pairHeads resolves the current finished head of every input side, in
// declaration order; a side with no finished head yields an empty commit —
// its contribution to the cross is no datums.
func (d *daemon) pairHeads(in *client.Input) []client.Commit {
	sides := inputSides(in)
	heads := make([]client.Commit, len(sides))
	for i, s := range sides {
		if h, err := d.store.headCommitRec(s.Repo, inputBranch(s)); err == nil && h.Finished {
			heads[i] = h
		}
	}
	return heads
}

// pipelineConsumes reports whether any input side subscribes to the
// (repo, branch) pair — the trigger condition for a commit.
func pipelineConsumes(in *client.Input, repo, branch string) bool {
	for _, s := range inputSides(in) {
		if s.Repo == repo && inputBranch(s) == branch {
			return true
		}
	}
	return false
}

// inputConsumesRepo reports whether any input side reads from repo.
func inputConsumesRepo(in *client.Input, repo string) bool {
	for _, s := range inputSides(in) {
		if s.Repo == repo {
			return true
		}
	}
	return false
}

// the caller (the HTTP handler that finished the commit).
func (d *daemon) triggerForCommit(cm client.Commit) {
	pipes, _ := d.listPipelinesFiltered(nil, "", false)
	for _, p := range pipes {
		if p.State == "failure" || p.State == "crashed" {
			continue
		}
		// A pipeline whose transaction head job has not been scheduled yet
		// must not be triggered twice: the transaction coordinator is the
		// sole scheduler for it (SB-162: exactly one job per update).
		if txNoteTrigger(p.Name) {
			continue
		}
		rec, err := d.loadPipeline(p.Name)
		if err != nil || rec.Pipeline.Input == nil {
			continue
		}
		if rec.Stopped {
			continue // stopped pipelines ignore new commits (SB-048)
		}
		if !pipelineConsumes(rec.Pipeline.Input, cm.Repo, cm.Branch) {
			continue
		}
		// the pairing at trigger time: the new commit on its side, each
		// other side at its current head (SB-120)
		heads := d.pairHeads(rec.Pipeline.Input)
		d.spawnJob(rec, heads)
	}
}

// runningJob is the handle on an in-flight job's execution; pipeline is the
// owning pipeline (for update/delete cancellation, SB-045/026), done signals
// the job goroutine has settled, cancelled distinguishes a deliberate kill
// from a plain failure (SB-122). containers tracks the live datum
// container names so a cancel can kill every one of them.
type runningJob struct {
	pipeline  string
	cancelled atomic.Bool
	done      chan struct{}

	containersMu sync.Mutex
	containers   map[string]struct{}
}

func (rj *runningJob) registerContainer(name string) {
	rj.containersMu.Lock()
	defer rj.containersMu.Unlock()
	if rj.containers == nil {
		rj.containers = map[string]struct{}{}
	}
	rj.containers[name] = struct{}{}
}

func (rj *runningJob) unregisterContainer(name string) {
	rj.containersMu.Lock()
	defer rj.containersMu.Unlock()
	delete(rj.containers, name)
}

func (rj *runningJob) containerNames() []string {
	rj.containersMu.Lock()
	defer rj.containersMu.Unlock()
	names := make([]string, 0, len(rj.containers))
	for n := range rj.containers {
		names = append(names, n)
	}
	return names
}

var (
	jobsMu  sync.Mutex
	running = map[string]*runningJob{}
)

func registerRunning(id, pipeline string) *runningJob {
	rj := &runningJob{pipeline: pipeline, done: make(chan struct{})}
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

// cancelPipelineJobs cancels every in-flight job of the pipeline and waits
// for each to settle (used by update and delete).
func (d *daemon) cancelPipelineJobs(pipeline string) {
	jobsMu.Lock()
	var ids []string
	for id, rj := range running {
		if rj.pipeline == pipeline {
			ids = append(ids, id)
		}
	}
	jobsMu.Unlock()
	for _, id := range ids {
		d.cancelJob(id)
	}
}

// markPipelineFailed records a pipeline-level failure with a reason; the
// pipeline stops scheduling until repaired (D-10).
func (d *daemon) markPipelineFailed(name, reason string) {
	if rec, err := d.loadPipeline(name); err == nil && !rec.Stopped {
		rec.State = "failure"
		rec.Reason = reason
		d.savePipeline(rec)
	}
}

// markPipelineCrashed records that a pipeline's execution environment could
// not be provisioned (SB-043, SB-091).
func (d *daemon) markPipelineCrashed(name, reason string) {
	if rec, err := d.loadPipeline(name); err == nil && !rec.Stopped {
		rec.State = "crashed"
		rec.Reason = reason
		d.savePipeline(rec)
	}
}

// outputMu serializes each pipeline's output-commit write phase. Output
// commits are opened at job start (their id goes into the job's
// environment); concurrent jobs of one pipeline would otherwise parent
// against the same stale head and the last finisher would orphan the other
// off the branch. Under the lock the open commit is re-parented to the
// current head, so the branch stays linear whatever order the jobs finish.
var (
	outputMuGuard sync.Mutex
	outputMu      = map[string]*sync.Mutex{}
)

func (d *daemon) repoLock(repo string) *sync.Mutex {
	outputMuGuard.Lock()
	defer outputMuGuard.Unlock()
	m, ok := outputMu[repo]
	if !ok {
		m = &sync.Mutex{}
		outputMu[repo] = m
	}
	return m
}

// finishOutput finalizes a job's output commit under the pipeline's commit
// lock: re-parent to the current head, upload OUT unless the commit is an
// explicit empty finish, and close it.
func (d *daemon) finishOutput(pl pipelineRec, outCommit client.Commit, outDir string, empty bool) (client.Commit, error) {
	m := d.repoLock(pl.Pipeline.Name)
	m.Lock()
	defer m.Unlock()
	if head := d.store.headCommit(pl.Pipeline.Name, defaultBranch); head != "" && head != outCommit.ParentID {
		if err := d.store.reparent(outCommit.ID, head); err != nil {
			return client.Commit{}, err
		}
	}
	if !empty {
		if err := d.store.addFilesFromDir(outCommit.ID, outDir); err != nil {
			// all-or-nothing: a failed upload closes the commit empty
			d.store.finishCommit(outCommit.ID, "", true)
			return client.Commit{}, err
		}
		// deletions propagate to the output revision: paths that were in
		// the parent's view and are gone from this output are tombstoned
		// (SB-007 — a deleted input file is absent, not stale)
		d.store.tombstoneRemoved(outCommit.ID, outDir)
	}
	return d.store.finishCommit(outCommit.ID, "", empty)
}

// isProvisioningError reports whether a failed docker run never started the
// container — an environment problem, not a user-code failure.
func isProvisioningError(tail string) bool {
	for _, marker := range []string{
		"invalid reference format",
		"Unable to find image",
		"pull access denied",
		"No such image",
		"failed to resolve reference",
		"manifest unknown",
		"image not found",
	} {
		if strings.Contains(tail, marker) {
			return true
		}
	}
	return false
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
			// kill every container the job has registered (per-datum
			// execution runs several concurrently); a single kill can be
			// lost the instant the job appears, before its containers exist
			names := rj.containerNames()
			if len(names) == 0 {
				time.Sleep(250 * time.Millisecond)
				continue
			}
			all := true
			for _, n := range names {
				if exec.Command("docker", "kill", n).Run() != nil {
					all = false
				}
			}
			if all {
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
	os.Remove(d.logPath(id))
	return os.RemoveAll(d.jobDir(id))
}

// checkMetadata verifies the control-plane metadata parses, tolerating
// missing records but not corrupted ones.
func (d *daemon) checkMetadata() error {
	pdir := filepath.Join(d.state, "pipelines")
	if entries, err := os.ReadDir(pdir); err == nil {
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), ".json") {
				continue
			}
			if b, err := os.ReadFile(filepath.Join(pdir, e.Name())); err == nil && !json.Valid(b) {
				return fmt.Errorf("pipeline record %s is not valid JSON", e.Name())
			}
		}
	}
	jdir := filepath.Join(d.state, "jobs")
	if entries, err := os.ReadDir(jdir); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			p := filepath.Join(jdir, e.Name(), "job.json")
			if b, err := os.ReadFile(p); err == nil && !json.Valid(b) {
				return fmt.Errorf("job record %s is not valid JSON", e.Name())
			}
		}
	}
	return nil
}

// reset removes every repository, pipeline, and job, and is idempotent
// (SB-037). Corrupted metadata aborts the reset instead of being wiped
// around (product decision D-08).
func (d *daemon) reset() error {
	if err := d.checkMetadata(); err != nil {
		return fmt.Errorf("reset aborted: corrupted metadata (%v)", err)
	}
	// cancel in-flight work so no goroutine writes into removed state
	jobsMu.Lock()
	var ids []string
	for id := range running {
		ids = append(ids, id)
	}
	jobsMu.Unlock()
	for _, id := range ids {
		d.cancelJob(id)
	}
	os.RemoveAll(filepath.Join(d.state, "repos"))
	os.RemoveAll(filepath.Join(d.state, "pipelines"))
	os.RemoveAll(filepath.Join(d.state, "jobs"))
	os.RemoveAll(filepath.Join(d.state, "logs"))
	os.RemoveAll(filepath.Join(d.state, "transactions"))
	os.RemoveAll(filepath.Join(d.state, "dedup"))
	if err := os.MkdirAll(filepath.Join(d.state, "repos"), 0o755); err != nil {
		return err
	}
	// the spec repository is internal and recreated empty (SB-127)
	return d.store.createRepo("spec")
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
// observable. The job id is supplied by the caller so schedulers can track
// the job they spawned.
// jobEnv builds the job-scoped execution environment: each input side's
// commit, the output directory, and job identity (SB-051, SB-101,
// SB-128). The input directory variables are per datum (each datum's own
// staging mount) and are appended by the datum executor.
func (d *daemon) jobEnv(pl pipelineRec, id, outCommit string, sides []client.Input, heads []client.Commit) []string {
	env := []string{
		"OUT=/sandman/out",
		"JOB_ID=" + id,
		"OUTPUT_COMMIT=" + outCommit,
	}
	for i, s := range sides {
		if i < len(heads) && heads[i].ID != "" {
			env = append(env, s.Name+"_COMMIT="+heads[i].ID)
		}
	}
	for k, v := range pl.Pipeline.Transform.Env {
		if !reservedEnv[k] {
			env = append(env, k+"="+v)
		}
	}
	return env
}

// failedDatumReason summarizes a job's failed datums for the job reason —
// the failure is attributed to the datum that failed (SB-011).
func failedDatumReason(dedup map[string]datumState, datums []datum) string {
	var parts []string
	for _, dt := range datums {
		st := dedup[dt.ID]
		if st.Outcome != "failed" {
			continue
		}
		r := "datum " + dt.ID + " failed"
		if st.Reason != "" {
			r += ": " + st.Reason
		}
		parts = append(parts, r)
	}
	reason := strings.Join(parts, "; ")
	if len(reason) > 4000 {
		reason = reason[len(reason)-4000:]
	}
	return reason
}

// runJob coordinates one job: enumerate the input sides' datums, take
// their cartesian product, run the datums with a bounded worker pool, merge
// their outputs into the single output commit, and record the per-datum
// outcomes in the pipeline's dedup table. heads is the input pairing — one
// commit per side, empty where a side has no head (SB-120's lone-input
// job; its cross contributes no datums).
func (d *daemon) runJob(pl pipelineRec, heads []client.Commit, id string) {
	sides := inputSides(pl.Pipeline.Input)
	for i := range sides {
		if sides[i].Name == "" {
			sides[i].Name = sides[i].Repo
		}
	}
	dir := d.jobDir(id)
	outDir := filepath.Join(dir, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return
	}
	rj := registerRunning(id, pl.Pipeline.Name)
	defer unregisterRunning(id, rj)
	// a standby pipeline returns to standby once its work settles; the
	// defer covers every terminal path (success, failure, killed)
	defer d.standbySettle(pl.Pipeline.Name)

	rec := &jobRec{ID: id, Pipeline: pl.Pipeline.Name, State: "running",
		Started: time.Now().UTC().Format(time.RFC3339Nano),
		Version: pl.Version, Transform: pl.Pipeline.Transform, Input: pl.Pipeline.Input}
	seen := map[string]bool{}
	for _, h := range heads {
		if h.ID != "" && !seen[h.ID] {
			seen[h.ID] = true
			rec.InputCommits = append(rec.InputCommits, h.ID)
		}
	}
	d.saveJob(rec)
	fail := func(reason string) {
		rec.State = "failure"
		rec.Reason = reason
		rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
		d.saveJob(rec)
	}

	// Resolve each side's input revision and enumerate its datums; the
	// job's datum set is the cartesian product (SB-063). A side without a
	// head contributes no datums, so the product is empty.
	views := map[string]map[string]fileEntry{}
	sideLists := make([][]datumSide, len(sides))
	for i, s := range sides {
		if i >= len(heads) || heads[i].ID == "" {
			continue
		}
		view, err := d.store.resolveViewByID(heads[i].ID)
		if err != nil {
			fail("materialize input: " + err.Error())
			return
		}
		views[s.Name] = view
		sd := enumerateDatums(view, s.Glob)
		for j := range sd {
			sd[j].Name = s.Name
		}
		sideLists[i] = sd
	}
	datums := crossDatums(sideLists)
	for i := range datums {
		datums[i].Hash = datumHash(views, datums[i])
	}
	// the datum set for log filters is the first side's full input files
	// (SB-060); cross jobs filter by their sides' files.
	var logDatums []datumRef
	for i := range sides {
		if i >= len(heads) || heads[i].ID == "" {
			continue
		}
		if vd, err := d.store.viewDatums(heads[i].ID); err == nil {
			logDatums = append(logDatums, vd...)
		}
	}
	rec.Datums = logDatums
	d.saveJob(rec)

	// The job's container output is captured into the log store as it is
	// produced. A capture failure degrades to no logs, never to a broken
	// job: execution is the control plane's job, logs are the meta plane's.
	outCommit, err := d.store.startCommit(pl.Pipeline.Name, "", "")
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
	// (SB-166). Skipped datums carry their previous output forward.
	dedup := d.loadDedup(pl.Pipeline.Name)
	reprocess := pl.Pipeline.Reprocess
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var todo []datum
	for _, dt := range datums {
		if st, ok := dedup[dt.ID]; ok && !reprocess && st.Outcome == "success" && st.Hash == dt.Hash {
			st.Outcome = "skipped"
			st.Started = now
			st.Finished = now
			st.ProcessTime = 0
			dedup[dt.ID] = st
			continue
		}
		todo = append(todo, dt)
	}

	jx := &jobExec{d: d, pl: pl, id: id, outDir: outDir, views: views,
		viewDirs: map[string]string{}, dedup: dedup, rj: rj}
	jx.env = d.jobEnv(pl, id, outCommit.ID, sides, heads)
	failedAny := d.runDatums(jx, todo)

	for _, dt := range datums {
		switch dedup[dt.ID].Outcome {
		case "success":
			rec.Processed++
		case "recovered":
			rec.Recovered++
		case "failed":
			rec.Failed++
		case "skipped":
			rec.Skipped++
		}
	}

	if failedAny {
		// All-or-nothing output: finish the commit explicitly empty. A
		// failed datum still leaves the job inspectable and the pipeline
		// schedulable (SB-082).
		d.finishOutput(pl, outCommit, "", true)
		if rj.cancelled.Load() {
			rec.State = "killed"
			rec.Reason = "job cancelled"
		} else {
			rec.State = "failure"
			rec.Reason = failedDatumReason(dedup, datums)
		}
		rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
		d.saveJob(rec)
		d.saveDedup(pl.Pipeline.Name, dedup)
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
	rec.State = "success"
	rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
	d.saveJob(rec)
	d.saveDedup(pl.Pipeline.Name, dedup)

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
// reporting. capture, when non-nil, also receives the full combined output
// for the log store (timestamped, line-split). inName is the input's
// environment variable name, which also names the in-container mount point.
func (d *daemon) runPipelineContainer(pl pipelineRec, jobID, inName string, env []string, inDir, outDir string, capture io.Writer) (int, string) {
	mounts := []string{"-v", inDir + ":/sandman/in/" + inName + ":ro"}
	return d.runPipelineContainerNamed(pl, jobID, "sandman-"+jobID, env, mounts, outDir, capture)
}

// runPipelineContainerNamed runs the transform's container under an
// explicit container name (per-datum containers are named after the datum
// so a cancel can kill exactly the running ones). mounts carries the
// per-input-side read-only mounts (each side's datum files at
// /sandman/in/<name> and, when materialized, the side's full view at
// /sandman/view/<name>).
func (d *daemon) runPipelineContainerNamed(pl pipelineRec, jobID, cname string, env []string, mounts []string, outDir string, capture io.Writer) (int, string) {
	tr := pl.Pipeline.Transform
	image := tr.Image
	if image == "" {
		image = "alpine"
	}
	args := []string{"run", "--rm", "--name", cname,
		"--label", "sandman.node=" + d.name,
		"-v", outDir + ":/sandman/out",
	}
	args = append(args, mounts...)
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
	w := io.Writer(&buf)
	if capture != nil {
		w = io.MultiWriter(&buf, capture)
	}
	cmd.Stdout = w
	cmd.Stderr = w
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
