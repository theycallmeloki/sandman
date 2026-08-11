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
	// SpecCommit is the pipeline's current specification commit (the
	// "spec" repository, SB-164): the provenance anchor for the pipeline's
	// spout commits. An update writes a new spec commit, so spout commits
	// before and after the update carry distinct provenance epochs
	// (SB-139 clause 7, SB-140 clause 3).
	SpecCommit string `json:"specCommit,omitempty"`
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
	if len(in.Join) > 0 && len(in.Group) > 0 {
		return fmt.Errorf("input cannot specify both a join and a group")
	}
	if len(in.Union) > 0 {
		if in.Repo != "" || len(in.Cross) > 0 || len(in.Join) > 0 || len(in.Group) > 0 {
			return fmt.Errorf("input cannot combine a union with other input kinds")
		}
		if in.Name == "" {
			return fmt.Errorf("a union input must specify a name")
		}
		if !shIdent.MatchString(in.Name) || in.Name == "out" {
			return fmt.Errorf("invalid union name %q", in.Name)
		}
		for i := range in.Union {
			if err := validateInputSides(&in.Union[i], pipelineName); err != nil {
				return err
			}
		}
		return nil
	}
	if len(in.Cross) > 0 {
		// a cross's immediate members expose distinct namespaces — a
		// member's namespace is its own name (a union member's name is
		// its alias), so two branches sharing an alias are rejected
		// (SB-078 clauses 5/6)
		ns := map[string]bool{}
		for _, m := range in.Cross {
			n := m.Name
			if n == "" {
				n = m.Repo
			}
			if n == "" {
				n = "input"
			}
			if ns[n] {
				return fmt.Errorf("cross branches must expose distinct namespaces")
			}
			ns[n] = true
		}
	}
	if len(in.Join) > 0 && in.Repo != "" {
		return fmt.Errorf("input cannot specify both a repo and a join")
	}
	if len(in.Group) > 0 && in.Repo != "" {
		return fmt.Errorf("input cannot specify both a repo and a group")
	}
	if len(in.Cross) > 0 && (in.JoinOn != "" || in.GroupBy != "") {
		return fmt.Errorf("input cannot specify a cross with a join-on or group-by")
	}
	if in.JoinOn != "" && !validCaptureSelector(in.JoinOn) {
		return fmt.Errorf("invalid join-on selector %q", in.JoinOn)
	}
	if in.GroupBy != "" && !validCaptureSelector(in.GroupBy) {
		return fmt.Errorf("invalid group-by selector %q", in.GroupBy)
	}
	names := map[string]bool{}
	for _, s := range inputSides(in) {
		if len(s.Union) > 0 {
			// a union embedded in a cross: its name is the exposed
			// namespace; validate it (and its branches) recursively
			if err := validateInputSides(&s, pipelineName); err != nil {
				return err
			}
			continue
		}
		if s.Name == "" {
			s.Name = s.Repo // an input's environment variable is named after its repo
		}
		if s.Name == "" {
			return fmt.Errorf("input must specify a name")
		}
		if s.Name == "out" {
			return fmt.Errorf(`input cannot be named "out"`)
		}
		if s.Cron != "" {
			// a cron input needs no repo or glob; its repository is
			// derived from the pipeline and the input's name (SB-089)
			if !shIdent.MatchString(s.Name) {
				return fmt.Errorf("input name %q is not a valid environment variable name", s.Name)
			}
			if names[s.Name] {
				return fmt.Errorf("input name %q is used by more than one input", s.Name)
			}
			names[s.Name] = true
			continue
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
		if (len(s.Join) > 0 || len(s.Group) > 0) && (s.JoinOn != "" || s.GroupBy != "") {
			return fmt.Errorf("nested joins and groups are not supported")
		}
	}
	return nil
}

// validCaptureSelector checks a join-on/group-by selector: one or more
// "$N" group references, e.g. "$1" or "$1$3".
func validCaptureSelector(sel string) bool {
	if sel == "" {
		return false
	}
	for _, tok := range strings.Split(sel, "$") {
		if tok == "" {
			continue
		}
		n, err := strconv.Atoi(tok)
		if err != nil || n <= 0 {
			return false
		}
	}
	return true
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
	if p.Spout == nil {
		// a spout declares no input (SB-139 clause 13: it is rejected when
		// one is given, by validateSpout)
		if err := validateInputSides(p.Input, p.Name); err != nil {
			return err
		}
	}
	if p.Parallelism != nil && p.Parallelism.Constant != 0 && p.Parallelism.Coefficient != 0 {
		return fmt.Errorf("cannot specify both a constant and a coefficient of parallelism")
	}
	if p.Framework != "" {
		return fmt.Errorf("pipeline framework %q is not supported", p.Framework)
	}
	return nil
}

// (self-reference before repo existence, so a pipeline never mistakes its
// own future output repo for a missing input).
// materializeInputDefaults fills an input's implicit defaults into the
// stored spec so extraction echoes them (SB-151): every side's name
// defaults to its repo and its branch to "master".
func materializeInputDefaults(in *client.Input) {
	if in == nil {
		return
	}
	if in.Name == "" {
		in.Name = in.Repo
	}
	if in.Branch == "" {
		in.Branch = "master"
	}
	for i := range in.Cross {
		materializeInputDefaults(&in.Cross[i])
	}
	for i := range in.Join {
		materializeInputDefaults(&in.Join[i])
	}
	for i := range in.Group {
		materializeInputDefaults(&in.Group[i])
	}
	for i := range in.Union {
		materializeInputDefaults(&in.Union[i])
	}
}

// inputSides / inputBranch are the server's aliases for the client's
// input normalization helpers.
func inputSides(in *client.Input) []client.Input { return client.InputSides(in) }
func inputBranch(s client.Input) string          { return client.InputBranch(s) }

func (d *daemon) createPipeline(p client.Pipeline) error {
	if err := validatePipelineSpec(p); err != nil {
		return err
	}
	if err := validateSpout(p); err != nil {
		return err
	}
	if p.Input == nil {
		// a spout has no input repo to check; anything else with no input
		// is rejected by the spec validation's "no input set"
		if p.Spout == nil {
			return fmt.Errorf("no input set")
		}
	} else if _, err := os.Stat(d.store.repoDir(p.Input.Repo)); err != nil {
		return fmt.Errorf("input repo %q not found", p.Input.Repo)
	}
	// materialize the input's implicit defaults into the stored spec so
	// extraction echoes them (SB-151): every side's name defaults to its
	// repo and its branch to master
	materializeInputDefaults(p.Input)

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
	if p.Spout != nil {
		// a spout's job is its own: a background run committing each
		// data-bearing cycle (SB-139)
		d.spawnSpoutJob(rec, false)
		return nil
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
	// cron inputs get their derived repositories and their schedules
	// started (SB-089); size triggers get their accumulation branches
	// (SB-160) — the derivations mutate the stored spec
	d.deriveCronRepos(&p)
	d.deriveTriggerBranches(&p)
	// the output repo exists from creation: downstream pipelines can be
	// defined against it before it has any commits (SB-086's stats branch).
	// An existing repo (a keepRepo delete followed by a recreate, SB-157)
	// is reused as-is.
	if _, err := os.Stat(d.store.repoDir(p.Name)); err != nil {
		if err := d.store.createRepo(p.Name); err != nil {
			return nil, err
		}
	}
	rec := pipelineRec{Pipeline: p, State: "running", Version: 1}
	if len(p.Transform.Cmd) == 0 && len(p.Transform.Stdin) > 0 {
		// No command to feed the stdin lines to: accepted, but the pipeline
		// fails as soon as it would start (SB-149).
		rec.State = "failure"
		rec.Reason = "no command specified but stdin lines provided"
	}
	for _, s := range inputSides(p.Input) {
		if s.Cron != "" {
			d.startCronTicker(p.Name, s.Name, s.Cron, s.Overwrite)
		}
	}
	// The spec commit is durable before the pipeline is considered created
	// (SB-164): a failed create leaves no spec commit behind because the
	// validation above ran first.
	rec.SpecCommit = d.writeSpecCommit(p.Name, p, 1)
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
			return d.spawnJob(rec, heads, "", "", nil)
		}
	}
	// a union whose branches have no direct heads may still consume repos
	// through its cross or nested-union branches (SB-078 clauses 2/3)
	if len(rec.Pipeline.Input.Union) > 0 {
		var any func(in *client.Input) bool
		any = func(in *client.Input) bool {
			for _, s := range inputSides(in) {
				if s.Repo != "" {
					if _, err := d.store.headCommitRec(s.Repo, inputBranch(s)); err == nil {
						return true
					}
				} else if len(s.Cross) > 0 || len(s.Union) > 0 {
					if any(&s) {
						return true
					}
				}
			}
			return false
		}
		if any(rec.Pipeline.Input) {
			return d.spawnJob(rec, heads, "", "", nil)
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
func (d *daemon) spawnJob(rec *pipelineRec, heads []client.Commit, propagated, id string, pre *jobRec) string {
	if rec.Pipeline.Standby {
		standbyMu.Lock()
		standbyActive[rec.Pipeline.Name]++
		if rec.State != "running" {
			rec.State = "running"
			d.savePipeline(rec)
		}
		standbyMu.Unlock()
	}
	if id == "" {
		id = newJobID(d.name)
	}
	// the running handle is registered before the goroutine starts, so a
	// cancel arriving the instant the job spawns can always find it — a
	// not-yet-scheduled goroutine would otherwise escape the cancel and
	// run the old version indefinitely (SB-045)
	rj := registerRunning(id, rec.Pipeline.Name)
	go d.runJob(*rec, heads, id, propagated, pre, rj)
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

// runPipeline manually triggers a pipeline run (SB-010). Provenance, when
// non-empty, fixes the exact input revisions the job processes — one per
// side, matched by the side's repo and branch; two commits of the same
// branch are rejected, and a commit outside the pipeline's input lineage
// is rejected. JobID re-executes an existing job's input pairing. With
// neither, the current branch heads are used. The run's output never
// propagates downstream (a manual run is not a processing wave).
func (d *daemon) runPipeline(name string, provenance []string, jobID string) (client.Job, error) {
	rec, err := d.loadPipeline(name)
	if err != nil {
		return client.Job{}, err
	}
	if rec.Pipeline.Input == nil {
		return client.Job{}, fmt.Errorf("pipeline %q has no inputs and cannot be run", name)
	}
	provenance = append([]string{}, provenance...)
	if jobID != "" {
		j, err := d.inspectJob(jobID)
		if err != nil {
			return client.Job{}, err
		}
		provenance = append(provenance, j.InputCommits...)
	}
	sides := inputSides(rec.Pipeline.Input)
	heads := make([]client.Commit, len(sides))
	seenBranch := map[string]bool{}
	for _, pid := range provenance {
		cm, err := d.store.loadCommitByID(pid)
		if err != nil {
			return client.Job{}, fmt.Errorf("provenance commit %s: not found", pid)
		}
		if seenBranch[cm.Repo+"/"+cm.Branch] {
			return client.Job{}, fmt.Errorf("provenance contains two commits of branch %s/%s", cm.Repo, cm.Branch)
		}
		seenBranch[cm.Repo+"/"+cm.Branch] = true
		found := false
		for i, s := range sides {
			if s.Repo == cm.Repo && inputBranch(s) == cm.Branch {
				heads[i] = client.Commit{ID: cm.ID, Repo: cm.Repo, Branch: cm.Branch}
				found = true
			}
		}
		if !found {
			return client.Job{}, fmt.Errorf("provenance commit %s is not part of the pipeline's input", pid)
		}
	}
	if len(provenance) == 0 {
		heads = d.pairHeads(rec.Pipeline.Input)
		any := false
		for _, h := range heads {
			if h.ID != "" {
				any = true
				break
			}
		}
		if !any {
			// nothing to run against: no provenance, and no input commits
			// exist (SB-010 clause 6: an unrunnable pipeline errors)
			return client.Job{}, fmt.Errorf("pipeline %q has no input commits to run", name)
		}
	}
	id := newJobID(d.name)
	jr := newJobRec(*rec, heads, id)
	jr.Manual = true
	os.MkdirAll(d.jobDir(id), 0o755)
	d.saveJob(jr)
	d.spawnJob(rec, heads, "", id, jr)
	return jr.job(), nil
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
	if p.Spout != nil {
		// the update killed the old spout job; the new epoch starts fresh
		// (SB-139 clause 7/10); a reprocess update resets the marker state
		d.spawnSpoutJob(rec, p.Reprocess)
		return nil
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
	if existing.Pipeline.EnableStats && !p.EnableStats {
		// per-datum statistics are one-way: an update cannot disable them
		// (SB-081)
		return nil, fmt.Errorf("statistics cannot be disabled once enabled")
	}
	// cron inputs keep their derived repositories; the existing tickers
	// are keyed by those repositories and are left running — an update
	// must not restart the cron clock (SB-133). Trigger branches are
	// reused across updates (SB-160 clause 7).
	d.deriveCronRepos(&p)
	d.deriveTriggerBranches(&p)
	for _, s := range inputSides(p.Input) {
		if s.Cron != "" {
			d.startCronTicker(p.Name, s.Name, s.Cron, s.Overwrite)
		}
	}
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
	rec.SpecCommit = d.writeSpecCommit(name, p, v)
	d.archiveVersion(&rec)
	if err := d.savePipeline(&rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// writeSpecCommit records one pipeline definition as a commit in the spec
// repository (SB-127, SB-164): one commit per definition, written only
// after validation passed. It returns the commit id — the pipeline's
// provenance anchor for spout epochs (SB-139 clause 7).
func (d *daemon) writeSpecCommit(name string, spec client.Pipeline, version int) string {
	b, err := json.Marshal(spec)
	if err != nil {
		return ""
	}
	cm, err := d.store.startCommit("spec", defaultBranch, fmt.Sprintf("pipeline %s v%d", name, version))
	if err != nil {
		return ""
	}
	if err := d.store.overwriteFile(cm.ID, "spec.json", b); err != nil {
		return ""
	}
	if _, err := d.store.finishCommit(cm.ID, "", false); err != nil {
		return ""
	}
	return cm.ID
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
	// a paused pipeline's in-flight work stops: stopping ends active
	// processing, so garbage collection can proceed (SB-079) and the
	// paused pipeline holds no containers
	d.cancelPipelineJobs(name)
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
	d.spawnJob(rec, d.pairHeads(rec.Pipeline.Input), "", "", nil)
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
		Name:         rec.Pipeline.Name,
		State:        rec.State,
		Reason:       rec.Reason,
		Description:  rec.Pipeline.Description,
		Stopped:      rec.Stopped,
		Version:      rec.Version,
		Transform:    rec.Pipeline.Transform,
		Input:        rec.Pipeline.Input,
		Parallelism:  rec.Pipeline.Parallelism,
		ChunkSpec:    rec.Pipeline.ChunkSpec,
		MaxQueueSize: rec.Pipeline.MaxQueueSize,
		Autoscaling:  rec.Pipeline.Autoscaling,
		Standby:      rec.Pipeline.Standby,
		OutputBranch: rec.Pipeline.OutputBranch,
		Reprocess:    rec.Pipeline.Reprocess,
		EnableStats:  rec.Pipeline.EnableStats,
		Spout:        rec.Pipeline.Spout,
		Placement:    rec.Pipeline.Placement,
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
			return nil // deleting an already-deleted pipeline is a no-op (SB-010)
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
	d.stopCronTickers(name)     // a deleted pipeline's schedule stops (SB-089)
	d.clearTriggerLedgers(name) // its trigger accumulation goes too (SB-160)
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
	DatumIDs     []string          `json:"datumIds,omitempty"`
	DatumStates  map[string]string `json:"datumStates,omitempty"`
	StatsCommit  string            `json:"statsCommit,omitempty"`
	// Manual marks a job spawned by an explicit pipeline run: its output
	// never propagates downstream (SB-010).
	Manual    bool `json:"manual,omitempty"`
	Processed int  `json:"processed,omitempty"`
	Recovered int  `json:"recovered,omitempty"`
	Failed    int  `json:"failed,omitempty"`
	Skipped   int  `json:"skipped,omitempty"`
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
		StatsCommit:  rec.StatsCommit,
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
	var info client.Job
	if b, err := os.ReadFile(filepath.Join(d.jobDir(id), "job.json")); err == nil {
		var rec jobRec
		if json.Unmarshal(b, &rec) == nil {
			info = rec.job()
		}
	}
	// a job may also be keyed by the output commit it produced (SB-135)
	if info.ID == "" {
		for _, j := range d.mustListJobs() {
			if j.OutputCommit == id {
				info = j
				break
			}
		}
	}
	if info.ID == "" {
		return client.Job{}, fmt.Errorf("job %q not found: specify a Job or an OutputCommit", id)
	}
	// the live per-worker status (SB-065/097)
	if b, err := os.ReadFile(filepath.Join(d.jobDir(info.ID), "workers.json")); err == nil {
		var ws []workerStatus
		if json.Unmarshal(b, &ws) == nil {
			for _, w := range ws {
				info.Workers = append(info.Workers, client.WorkerStatus{
					Worker: w.Worker, Datum: w.Datum, Started: w.Started, Queue: w.Queue,
				})
			}
		}
	}
	return info, nil
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

// jobByOutput finds the job that produced a commit, if any. Commits with
// no producing job — user commits and spec commits — are not failures.
func (d *daemon) jobByOutput(commitID string) (client.Job, bool) {
	for _, j := range d.mustListJobs() {
		if j.OutputCommit == commitID {
			return j, true
		}
	}
	return client.Job{}, false
}

func newJobID(node string) string {
	b := make([]byte, 6)
	rand.Read(b)
	return node + "-" + hex.EncodeToString(b)
}

// newJobRec builds a job's initial record: the running state, the input
// pairing it consumed, and the pipeline-version snapshots (SB-040/143).
func newJobRec(pl pipelineRec, heads []client.Commit, id string) *jobRec {
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
	return rec
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
// (repo, branch) pair — the trigger condition for a commit. Union and
// cross branches are walked recursively, so a union of crosses still
// triggers on its members' repos (SB-078).
func pipelineConsumes(in *client.Input, repo, branch string) bool {
	for _, s := range inputSides(in) {
		if s.Repo != "" {
			if s.Repo == repo && inputBranch(s) == branch {
				return true
			}
			continue
		}
		if len(s.Cross) > 0 || len(s.Union) > 0 {
			if pipelineConsumes(&s, repo, branch) {
				return true
			}
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
		if s.Repo == "" && (len(s.Cross) > 0 || len(s.Union) > 0) && inputConsumesRepo(&s, repo) {
			return true
		}
	}
	return false
}

// the caller (the HTTP handler that finished the commit).
func (d *daemon) triggerForCommit(cm client.Commit) {
	// size triggers watching the commit's branch accumulate its bytes and
	// may fire (SB-160); the trigger commits they create re-enter here
	d.accumulateTriggers(cm)
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
		// a commit produced by a failed job propagates the failure: the
		// consuming pipeline's job is recorded failed without executing
		// (SB-022)
		propagated := ""
		if j, ok := d.jobByOutput(cm.ID); ok && j.State == "failure" {
			propagated = "upstream job " + j.ID + " failed: " + j.Reason
		}
		// exactly one job per input pairing: two sides' commits landing
		// near-simultaneously can each pair with the other's fresh head,
		// and the second trigger must not spawn a duplicate job for the
		// same input set (SB-056: one job per wave, never extra). The
		// duplicate check and the job record's creation are atomic, so a
		// racing trigger sees the record instead of double-spawning.
		triggerMu.Lock()
		if d.hasRunningJobWithInputs(rec.Pipeline.Name, heads) {
			triggerMu.Unlock()
			continue
		}
		// provenance consistency: two sides that derive from the same
		// source branch must pair at the same source revision. A trigger
		// that pairs a fresh commit with the other side's still-stale head
		// (the other side has not caught up yet) is deferred instead of
		// spawning a mismatched job — the catch-up trigger will form the
		// coherent pairing (SB-018/019 diamonds: one commit per source
		// revision, never one per dependency path).
		if !d.crossPairingConsistent(heads) {
			triggerMu.Unlock()
			continue
		}
		id := newJobID(d.name)
		jr := newJobRec(*rec, heads, id)
		os.MkdirAll(d.jobDir(id), 0o755) // saveJob does not create the dir
		d.saveJob(jr)
		triggerMu.Unlock()
		d.spawnJob(rec, heads, propagated, id, jr)
	}
}

// triggerMu serializes the duplicate check and the job record's creation
// in triggerForCommit: a racing trigger for the same input pairing must
// observe the record the first trigger just saved (SB-056).
var triggerMu sync.Mutex

// hasRunningJobWithInputs reports whether the pipeline already has a
// non-terminal job consuming exactly this input set — the guard against
// duplicate pairing jobs when two input sides' commits land together.
func (d *daemon) hasRunningJobWithInputs(pipeline string, heads []client.Commit) bool {
	set := map[string]bool{}
	for _, h := range heads {
		if h.ID != "" {
			set[h.ID] = true
		}
	}
	for _, j := range d.mustListJobs() {
		if j.Pipeline != pipeline || j.State != "running" {
			continue
		}
		if len(j.InputCommits) != len(set) {
			continue
		}
		all := true
		for _, ic := range j.InputCommits {
			if !set[ic] {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// crossPairingConsistent reports whether a cross pairing's sides reference
// each source branch at exactly one revision. Two sides deriving from the
// same source must pair at the same source revision — pairing a fresh
// commit with the other side's stale head would process a mismatched
// revision pair (SB-018/019). Disjoint sources are always consistent.
func (d *daemon) crossPairingConsistent(heads []client.Commit) bool {
	sources := map[string]string{} // repo+branch → source commit
	for _, h := range heads {
		if h.ID == "" {
			continue // an empty side contributes no source
		}
		for _, leaf := range d.provenanceOf(h.ID, map[string]bool{}) {
			rec, err := d.store.loadCommitByID(leaf)
			if err != nil {
				continue
			}
			key := rec.Repo + "/" + rec.Branch
			if prev, ok := sources[key]; ok && prev != leaf {
				return false
			}
			sources[key] = leaf
		}
	}
	return true
}

// provenanceOf expands a commit to the leaf source commits it derives
// from: the commit itself when no job produced it, otherwise its producing
// job's inputs, recursively.
func (d *daemon) provenanceOf(commitID string, seen map[string]bool) []string {
	if seen[commitID] {
		return nil
	}
	seen[commitID] = true
	if j, ok := d.jobByOutput(commitID); ok {
		var out []string
		for _, ic := range j.InputCommits {
			out = append(out, d.provenanceOf(ic, seen)...)
		}
		return out
	}
	return []string{commitID}
}

// runningJob is the handle on an in-flight job's execution; pipeline is the
// owning pipeline (for update/delete cancellation, SB-045/026), done signals
// the job goroutine has settled, cancelled distinguishes a deliberate kill
// from a plain failure (SB-122). containers tracks the live datum
// container names so a cancel can kill every one of them.
type runningJob struct {
	pipeline   string
	cancelled  atomic.Bool
	cancelCh   chan struct{}
	cancelOnce sync.Once
	started    atomic.Bool // the job passed the pipeline's gate
	done       chan struct{}

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
	rj := &runningJob{pipeline: pipeline, cancelCh: make(chan struct{}), done: make(chan struct{})}
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

// markPipelineRunning clears the placement-outage crash once a host bearing
// the pipeline's label has registered: the crashed state was only the
// unplaced outage, and placement has become possible again (SB-169).
func (d *daemon) markPipelineRunning(name string) {
	if rec, err := d.loadPipeline(name); err == nil && rec.State == "crashed" {
		rec.State = "running"
		rec.Reason = ""
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

// jobGate serializes a pipeline's jobs in spawn order: strictly one job at
// a time, the slot handed to the oldest waiter (SB-123). With parallelism
// 1 the jobs of successive commits therefore come up in commit order and
// only one runs at a time; cancelling the running job lets the next queued
// job start, and cancelling one job never touches the others. Serializing
// every job of a pipeline also keeps its output commits strictly ordered,
// so a later commit's content is never stranded under an earlier commit's
// output.
type jobGate struct {
	mu   sync.Mutex
	head *jobGateNode // the next waiter to receive the slot
	tail *jobGateNode
	held bool // the slot is taken by a running job
}

type jobGateNode struct {
	ch   chan struct{}
	next *jobGateNode
}

var (
	jobGateGuard sync.Mutex
	jobGates     = map[string]*jobGate{}
)

func (d *daemon) jobGate(pipeline string) *jobGate {
	jobGateGuard.Lock()
	defer jobGateGuard.Unlock()
	g, ok := jobGates[pipeline]
	if !ok {
		g = &jobGate{}
		jobGates[pipeline] = g
	}
	return g
}

// enter queues the job and blocks until it is handed the slot. It returns
// false when a cancel arrived while the job was queued (or raced the slot
// hand-off): the job never ran, and the slot was released or passed on.
func (g *jobGate) enter(rj *runningJob) bool {
	n := &jobGateNode{ch: make(chan struct{})}
	g.mu.Lock()
	if g.tail != nil {
		g.tail.next = n
	} else {
		g.head = n
	}
	g.tail = n
	start := false
	if !g.held {
		// the gate is free: hand the slot to this job immediately
		g.held = true
		g.head = n.next
		if g.head == nil {
			g.tail = nil
		}
		start = true
	}
	g.mu.Unlock()
	if !start {
		select {
		case <-n.ch:
		case <-rj.cancelCh:
			// cancelled while queued: unlink so the slot reaches the next
			// waiter when the current holder releases it
			g.remove(n)
			return false
		}
	}
	rj.started.Store(true)
	if rj.cancelled.Load() {
		// a cancel raced the hand-off: decline the slot
		g.release()
		return false
	}
	return true
}

// remove unlinks a still-queued node; the jobs behind it move up.
func (g *jobGate) remove(n *jobGateNode) {
	g.mu.Lock()
	defer g.mu.Unlock()
	var prev *jobGateNode
	for cur := g.head; cur != nil; cur = cur.next {
		if cur == n {
			if prev != nil {
				prev.next = cur.next
			} else {
				g.head = cur.next
			}
			if g.tail == cur {
				g.tail = prev
			}
			return
		}
		prev = cur
	}
}

// release hands the slot to the next queued job, or frees it when the
// queue is empty.
func (g *jobGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.head != nil {
		n := g.head
		g.head = n.next
		if g.head == nil {
			g.tail = nil
		}
		close(n.ch)
		return
	}
	g.held = false
}

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
	outBranch := outputBranch(pl)
	if head := d.store.headCommit(pl.Pipeline.Name, outBranch); head != "" && head != outCommit.ParentID {
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

// recordProvenance stamps a finished output commit with its derivation:
// its input commits and their own provenance, transitively. The recorded
// provenance makes spout epochs and spec-commit subvenance observable
// (SB-139 clause 7, SB-140 clauses 1/3): a downstream commit's recorded
// provenance includes its upstream spout commit AND that spout's
// specification commit, so the spec commit's subvenants are exactly the
// spout output and the downstream output.
func (d *daemon) recordProvenance(commitID string, inputs []string) {
	if commitID == "" {
		return
	}
	seen := map[string]bool{}
	var prov []string
	var expand func(id string)
	expand = func(id string) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		prov = append(prov, id)
		if cm, err := d.store.loadCommitByID(id); err == nil {
			for _, p := range cm.Provenance {
				expand(p)
			}
		}
	}
	for _, in := range inputs {
		expand(in)
	}
	if len(prov) == 0 {
		return
	}
	if cm, err := d.store.loadCommitByID(commitID); err == nil {
		cm.Provenance = prov
		d.store.saveCommit(cm)
	}
}

// isProvisioningError reports whether a failed docker run never started the// container — an environment problem, not a user-code failure.
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
	// the live running handle is the authority — a cancel must not abort
	// on a transiently unreadable job record (a concurrent save can race
	// the read; the job then escapes the cancel and runs the old version
	// indefinitely, SB-045)
	jobsMu.Lock()
	rj, ok := running[id]
	jobsMu.Unlock()
	if !ok {
		if _, err := d.inspectJob(id); err != nil {
			return err
		}
		return nil // already terminal
	}
	rj.cancelled.Store(true)
	rj.cancelOnce.Do(func() { close(rj.cancelCh) })
	if !rj.started.Load() {
		// the job is queued behind the pipeline's gate: it has no work to
		// kill, and it will settle as killed when it reaches the slot's
		// front (SB-123). Returning now lets a later job start as soon as
		// the running one settles — a queued cancel never blocks on it.
		return nil
	}
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

// resolveCommitRef resolves a commit reference: a commit id, or
// repo@branch meaning that branch's head.
func (d *daemon) resolveCommitRef(ref string) (*commitRec, error) {
	if repo, branch, ok := strings.Cut(ref, "@"); ok {
		head, err := d.store.headCommitRec(repo, branch)
		if err != nil {
			return nil, err
		}
		return d.store.loadCommitByID(head.ID)
	}
	return d.store.loadCommitByID(ref)
}

// allCommitRecs enumerates every commit record in every repository,
// including the internal spec repository (its commits reference spec
// blobs that garbage collection must keep — SB-079).
func (d *daemon) allCommitRecs() []*commitRec {
	var out []*commitRec
	repos, _ := d.store.listRepos()
	for _, r := range repos {
		out = append(out, d.repoCommitRecs(r.Name)...)
	}
	// the spec repository is internal and not listed as a user repo
	// (SB-127), but its commits are still durable references
	out = append(out, d.repoCommitRecs("spec")...)
	return out
}

func (d *daemon) repoCommitRecs(repo string) []*commitRec {
	var out []*commitRec
	dir := filepath.Join(d.store.repoDir(repo), "commits")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		if b, err := os.ReadFile(filepath.Join(dir, e.Name())); err == nil {
			var rec commitRec
			if json.Unmarshal(b, &rec) == nil {
				out = append(out, &rec)
			}
		}
	}
	return out
}

// deleteCommit removes a commit and everything derived from it (SB-124):
// the commit's own record, every commit in any repository whose
// provenance includes it (transitively through the DAG), and the jobs
// that consumed any of them. Surviving commits whose parent was removed
// get their parent link cleared — the survivor becomes the first commit
// of its branch — and branch heads that pointed at a removed commit move
// to the nearest surviving ancestor or disappear. Deleting a branch head
// supersedes an in-flight job processing it (SB-125). Deletion never
// triggers pipelines: the surviving revisions were already processed.
func (d *daemon) deleteCommit(ref string) error {
	rec, err := d.resolveCommitRef(ref)
	if err != nil {
		return err
	}
	// the deletion set: the commit and every commit derived from it
	deleted := map[string]bool{rec.ID: true}
	for {
		grown := false
		for _, cm := range d.allCommitRecs() {
			if deleted[cm.ID] {
				continue
			}
			for _, leaf := range d.provenanceOf(cm.ID, map[string]bool{}) {
				if deleted[leaf] {
					deleted[cm.ID] = true
					grown = true
					break
				}
			}
		}
		if !grown {
			break
		}
	}
	// cancel in-flight jobs that consumed a deleted commit, then remove
	// every affected job record (SB-124: job history reflects the removal;
	// SB-125: the in-flight job is superseded, not left running)
	for _, j := range d.mustListJobs() {
		hit := false
		for _, ic := range j.InputCommits {
			if deleted[ic] {
				hit = true
				break
			}
		}
		if !hit {
			continue
		}
		d.cancelJob(j.ID) // a no-op for terminal jobs
		os.RemoveAll(d.jobDir(j.ID))
		os.Remove(d.logPath(j.ID))
	}
	// branch heads that point at a removed commit move to the nearest
	// surviving ancestor, or the ref goes away. The repair is computed
	// before the records are removed — a deleted head's parent chain is
	// unrecoverable afterwards.
	type headFix struct {
		repo, branch, newHead string
	}
	var fixes []headFix
	repos, _ := d.store.listRepos()
	for _, r := range repos {
		refsDir := filepath.Join(d.store.repoDir(r.Name), "refs")
		entries, err := os.ReadDir(refsDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			head := d.store.headCommit(r.Name, e.Name())
			if head == "" || !deleted[head] {
				continue
			}
			newHead := ""
			for cur := head; ; {
				cm, err := d.store.loadCommit(r.Name, cur)
				if err != nil || cm.ParentID == "" {
					break
				}
				cur = cm.ParentID
				if !deleted[cur] {
					newHead = cur
					break
				}
			}
			fixes = append(fixes, headFix{repo: r.Name, branch: e.Name(), newHead: newHead})
		}
	}
	// repair surviving commits: a removed parent is relinked to the
	// nearest surviving ancestor (or cleared when none exists), so the
	// branch chain stays connected. The walk needs the removed commits'
	// records, so the fixes are captured before they are removed.
	type parentFix struct {
		cm     *commitRec
		newPar string
	}
	var pfixes []parentFix
	for _, cm := range d.allCommitRecs() {
		if deleted[cm.ID] || cm.ParentID == "" {
			continue
		}
		if deleted[cm.ParentID] {
			newPar := ""
			for cur := cm.ParentID; cur != ""; {
				if !deleted[cur] {
					newPar = cur
					break
				}
				prec, err := d.store.loadCommit(cm.Repo, cur)
				if err != nil || prec.ParentID == "" {
					break
				}
				cur = prec.ParentID
			}
			pfixes = append(pfixes, parentFix{cm: cm, newPar: newPar})
		}
	}
	// remove the commit records, then apply the parent repairs
	for _, cm := range d.allCommitRecs() {
		if deleted[cm.ID] {
			os.Remove(d.store.commitPath(cm.Repo, cm.ID))
		}
	}
	for _, pf := range pfixes {
		pf.cm.ParentID = pf.newPar
		d.store.saveCommit(pf.cm)
	}
	// apply the captured head repairs
	for _, fx := range fixes {
		if fx.newHead == "" {
			os.Remove(filepath.Join(d.store.repoDir(fx.repo), "refs", fx.branch))
		} else {
			d.store.setHead(fx.repo, fx.branch, fx.newHead)
		}
	}
	return nil
}

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
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return
	}
	defer unregisterRunning(id, rj)
	// a standby pipeline returns to standby once its work settles; the
	// defer covers every terminal path (success, failure, killed)
	defer d.standbySettle(pl.Pipeline.Name)

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
		rec.State = "killed"
		rec.Reason = "job cancelled"
		rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
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
		rec.State = "failure"
		rec.Reason = propagated
		rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
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
		rec.State = "failure"
		rec.Reason = reason
		rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
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
				rec.State = "killed"
				rec.Reason = "job cancelled"
				rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
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
		if i >= len(heads) || heads[i].ID == "" {
			continue
		}
		if vd, err := d.store.viewDatums(heads[i].ID); err == nil {
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
		rec.State = "success"
		rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
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
		if st, ok := dedup[dt.ID]; ok && !reprocess && st.Outcome == "success" && st.Hash == dt.Hash {
			rec.DatumStates[dt.ID] = "skipped"
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
		killed := rj.cancelled.Load()
		if killed {
			rec.State = "killed"
			rec.Reason = "job cancelled"
		} else {
			rec.State = "failure"
			rec.Reason = failedDatumReason(dedup, datums)
		}
		rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
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
	rec.State = "success"
	rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
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

// runPipelineContainer runs the transform in a throwaway container and
// returns its exit code plus a tail of combined stderr/stdout for failure
// reporting. capture, when non-nil, also receives the full combined output
// for the log store (timestamped, line-split). inName is the input's
// runDatumContainer runs one command (the primary or the error handler)
// in a throwaway container under an explicit name, with the datum's
// per-side mounts and output directory. Returns the exit code and the
// output tail. Shared by the control plane's own executor and a remote
// execution host's worker (SB-167): nodeName is the host identity the
// container is labelled with (the daemon's name, or the worker's host
// name when the datum runs remotely).
func runDatumContainer(tr *client.Transform, nodeName, cname string, env []string, mounts []string, outDir string, capture io.Writer, argv, stdin []string) (int, string) {
	image := tr.Image
	if image == "" {
		image = "alpine"
	}
	args := []string{"run", "--rm", "--name", cname,
		"--label", "sandman.node=" + nodeName,
		"-v", outDir + ":/sandman/out",
	}
	// resource requests and limits are applied to the execution
	// environment (SB-067/068/069/070). Sandbox deviation: docker
	// expresses a CPU request only as an allocation, so a CPU request
	// without a limit sets the container's CPU allocation; an
	// ephemeral-storage (disk) request is recorded but not enforceable
	// on docker's default driver.
	if tr.ResourceLimits != nil {
		if tr.ResourceLimits.Memory != "" {
			args = append(args, "--memory", tr.ResourceLimits.Memory)
		}
		if tr.ResourceLimits.CPU > 0 {
			args = append(args, "--cpus", fmt.Sprintf("%g", tr.ResourceLimits.CPU))
		}
	}
	if tr.ResourceRequests != nil {
		if tr.ResourceRequests.Memory != "" {
			args = append(args, "--memory-reservation", tr.ResourceRequests.Memory)
		}
		if tr.ResourceRequests.CPU > 0 && (tr.ResourceLimits == nil || tr.ResourceLimits.CPU == 0) {
			args = append(args, "--cpus", fmt.Sprintf("%g", tr.ResourceRequests.CPU))
		}
	}
	args = append(args, mounts...)
	for _, e := range env {
		args = append(args, "-e", e)
	}
	if len(stdin) > 0 {
		args = append(args, "-i")
	}
	workdir := tr.Workdir
	if workdir == "" {
		workdir = "/sandman/out"
	}
	args = append(args, "-w", workdir)

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
	if len(stdin) > 0 {
		cmd.Stdin = strings.NewReader(strings.Join(stdin, "\n") + "\n")
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
