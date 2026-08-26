package main

// Control plane: pipelines and jobs, stored as plain JSON records under
// <state>/pipelines/<name>.json and <state>/jobs/<id>/job.json (Rule of
// Transparency). Finishing a commit triggers one job per pipeline whose
// input repo it belongs to; a job runs the pipeline's transform in a
// throwaway container, uploads the OUT directory into a new commit of the
// pipeline's output repo, and records success/failure.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"sandman/client"
	"sandman/internal/store"
)

// pipelineRec is the persisted form of a pipeline. State is durable so
// a restarting daemon remembers what it decided. Stopped is a persistent
// flag distinct from the transient state; StoppedAt is the input
// branch head when the pipeline was stopped, the watermark for the backlog
// replayed on start.
type pipelineRec struct {
	Pipeline  client.Pipeline `json:"pipeline"`
	State     string          `json:"state"` // running | paused | standby | failure | crashed
	Reason    string          `json:"reason,omitempty"`
	Stopped   bool            `json:"stopped,omitempty"`
	StoppedAt string          `json:"stoppedAt,omitempty"`
	Version   int             `json:"version"`
	// SpecCommit is the pipeline's current specification commit (the
	// "spec" repository): the provenance anchor for the pipeline's
	// spout commits. An update writes a new spec commit, so spout commits
	// before and after the update carry distinct provenance epochs.
	SpecCommit string `json:"specCommit,omitempty"`
}

var shIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// envName renders a shell-identifier from an arbitrary string: characters
// that cannot appear in an environment variable name (e.g. hyphens in
// repo names) become underscores. The datum env var is named after its
// repo; names may carry hyphens throughout, so the derived name
// is the sanitized form while the repo keeps its own spelling.
func envName(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

// reservedEnv are the job environment variables owned by the system; a
// custom environment variable may not shadow them (package client docs).
var reservedEnv = map[string]bool{
	"OUT": true, "JOB_ID": true, "OUTPUT_COMMIT": true,
}

// createPipeline validates and persists a pipeline, or updates an existing
// one when the update flag is set. The validation order is fixed: spec
// present, name, transform, input, then input fields (name → reserved
// "out" → repo → glob), then cross-references. validatePipelineSpec checks
// the structural rules that do not depend on surrounding store state:
// spec, name, transform, input name (valid shell identifier, not "out"),
// repo, glob, self-reference, and parallelism. Repo existence and name
// uniqueness are checked by the caller (they resolve differently inside a
// transaction). validateInputSides checks an input's structure and every
// side in the same fixed order — name → reserved "out" → repo → glob →
// identifier → unique names → self-reference — rejecting every malformed
// variant cleanly with a descriptive error and never panicking the
// service. An input side may not consume the pipeline's own output (a side
// whose repo equals the pipeline's name is rejected), a file input's alias
// must not be the reserved output directory name "out" (explicit or
// defaulted from the repo name), and every file input must declare a glob
// selecting which files become datums. A cross's immediate members must
// expose distinct namespaces — a member's namespace is its own name (a
// union member's name is its alias) — so two branches sharing an alias,
// or two immediate cross branches whose nested unions expose the same
// alias set, are rejected. Git inputs share a single namespace: two git
// inputs must not resolve to the same derived name or URL, while the same
// URL under distinct custom names is allowed.
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
		if in.Trigger != nil && in.Trigger.SizeBytes > 0 {
			return fmt.Errorf("size triggers are not supported on union inputs")
		}
		for i := range in.Union {
			if hasSizeTrigger(&in.Union[i]) {
				// a union member is accumulated and fired as one datum; a
				// size trigger's accumulation branch is keyed by input
				// position and only fires on a watched branch, semantics
				// that do not extend to a union's merged view
				return fmt.Errorf("size triggers are not supported inside union inputs")
			}
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
		if s.Git != nil {
			// a git input needs no repo or glob: the mapped repository is
			// derived from the URL or the custom name. The URL form is
			// validated at creation, and the derived names participate in
			// the duplicate-name check, so two git inputs with the same
			// URL and no custom names collide while distinct names
			// disambiguate.
			if err := validateGitURL(s.Git.URL); err != nil {
				return err
			}
			name := s.Name
			if name == "" {
				name = envName(gitRepoName(s.Git.URL))
			}
			if !shIdent.MatchString(name) {
				return fmt.Errorf("input name %q is not a valid environment variable name", name)
			}
			if names[name] {
				return fmt.Errorf("input name %q is used by more than one input", name)
			}
			names[name] = true
			continue
		}
		if len(s.Union) > 0 {
			// a union embedded in a cross: its name is the exposed
			// namespace; validate it (and its branches) recursively
			if err := validateInputSides(&s, pipelineName); err != nil {
				return err
			}
			continue
		}
		if s.Name == "" {
			s.Name = envName(s.Repo) // an input's environment variable is named after its repo
		}
		if s.Name == "" {
			return fmt.Errorf("input must specify a name")
		}
		if s.Name == "out" {
			return fmt.Errorf(`input cannot be named "out"`)
		}
		if s.Cron != "" {
			// a cron input needs no repo or glob; its repository is
			// derived from the pipeline and the input's name
			if s.Name == "" {
				return fmt.Errorf("input must specify a name")
			}
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

// validatePipelineSpec checks a pipeline declaration's shape before any
// creation-side effects. A name is mandatory: a request with none is
// rejected with an error naming the missing 'pipeline' field, returned as
// a normal error response that neither panics nor wedges the service. A
// transform is mandatory: its absence is rejected with an error naming the
// missing 'transform' field, rather than crashing. A parallelism
// specification must select exactly one mechanism: setting both a constant
// worker count and a coefficient is rejected with an error referencing
// 'parallelism', even though each field alone is legal. Resource
// declarations may be partially or entirely unspecified — only memory,
// memory+CPU, or none at all — and are accepted as-is: declarations are
// accept-and-record (enforcement is the worker runtime's), so partial or
// empty resource specs never gate creation or block the running state.
func validatePipelineSpec(p client.Pipeline) error {
	if p.Name == "" && p.Transform == nil {
		return fmt.Errorf("invalid pipeline spec")
	}
	if p.Name == "" {
		return fmt.Errorf("pipeline must specify a name")
	}
	if !store.ValidName(p.Name) {
		// a pipeline name is a state-dir path component (pipelines/<name>,
		// versions/, spout markers) and the output repo's name: a name with
		// a separator or ".." would escape the pipelines directory
		return fmt.Errorf("invalid pipeline name %q", p.Name)
	}
	if p.Transform == nil {
		return fmt.Errorf("pipeline must specify a transform")
	}
	if p.Spout == nil {
		// a spout declares no input (it is rejected when one is given, by
		// validateSpout)
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
	if p.Transform != nil {
		// malformed execution-environment customization fails pipeline
		// creation, before any execution
		if _, err := parseCustomization(p.Transform); err != nil {
			return err
		}
	}
	return nil
}

// validateService checks a service pipeline's declaration: a service is
// one long-lived process (parallelism 1), always on (no standby),
// mutually exclusive with spouts, and serves a real input.
func validateService(p client.Pipeline) error {
	if p.Service == nil {
		return nil
	}
	if p.Spout != nil {
		return fmt.Errorf("a pipeline cannot be both a service and a spout")
	}
	if p.Standby {
		return fmt.Errorf("a service pipeline cannot be standby")
	}
	if p.Service.InternalPort <= 0 || p.Service.ExternalPort <= 0 {
		return fmt.Errorf("a service pipeline must declare internal and external ports")
	}
	if p.Parallelism != nil && p.Parallelism.Constant > 1 {
		return fmt.Errorf("a service pipeline runs one long-lived process (parallelism 1)")
	}
	return nil
}

// validateSecrets checks a pipeline's secret bindings: each reference
// names a secret, mounts under the /sandman/ execution namespace, and env
// injections use valid shell identifiers that are not reserved. Secret
// existence is checked at creation (the daemon-level check in
// createPipeline).
func validateSecrets(p client.Pipeline) error {
	for i := range p.Transform.Secrets {
		m := p.Transform.Secrets[i]
		if m.Name == "" {
			return fmt.Errorf("secret reference %d is missing its name", i)
		}
		if m.MountPath == "" && m.EnvVar == "" {
			return fmt.Errorf("secret %q must set a mount path or an env var", m.Name)
		}
		if m.MountPath != "" && !strings.HasPrefix(m.MountPath, "/sandman/") {
			return fmt.Errorf("secret %q mount path %q must be under /sandman/ (the execution namespace)", m.Name, m.MountPath)
		}
		if m.EnvVar != "" {
			if m.Key == "" {
				return fmt.Errorf("secret %q env var injection needs a key", m.Name)
			}
			if !shIdent.MatchString(m.EnvVar) {
				return fmt.Errorf("secret env var %q is not a valid shell identifier", m.EnvVar)
			}
			if reservedEnv[m.EnvVar] {
				return fmt.Errorf("secret env var %q is reserved", m.EnvVar)
			}
		}
	}
	return nil
}

// hasSizeTrigger reports whether the input subtree declares a size
// trigger: unions reject them at creation (validateInputSides), so the
// subtree scan only ever fires in the union-rejection path.
func hasSizeTrigger(in *client.Input) bool {
	if in == nil {
		return false
	}
	if in.Trigger != nil && in.Trigger.SizeBytes > 0 {
		return true
	}
	for i := range in.Cross {
		if hasSizeTrigger(&in.Cross[i]) {
			return true
		}
	}
	for i := range in.Union {
		if hasSizeTrigger(&in.Union[i]) {
			return true
		}
	}
	return false
}

// externalPortTaken reports whether another live pipeline already declares
// the external port — two services cannot share the control-plane host's
// bound port.
func (d *daemon) externalPortTaken(port int, except string) bool {
	pipes, err := d.listPipelinesFiltered(nil, "", false)
	if err != nil {
		return false
	}
	for _, p := range pipes {
		if p.Name == except || p.Service == nil {
			continue
		}
		if p.Service.ExternalPort == port {
			return true
		}
	}
	return false
}

// (self-reference before repo existence, so a pipeline never mistakes its
// own future output repo for a missing input).
// materializeInputDefaults fills an input's implicit defaults into the
// stored spec so extraction echoes them: every side's name defaults to its
// repo and its branch to "master". Extraction returns a creation request
// deep-equal to the one used to create it — every user-settable field
// round-trips, and the input's implicit name and branch defaults are
// materialized into the stored spec so they echo back; non-configuration
// fields (spec commit, update/reprocess flags) are excluded, and an
// unsupported execution framework is rejected at creation naming it.
func materializeInputDefaults(in *client.Input) {
	if in == nil {
		return
	}
	if in.Name == "" {
		in.Name = envName(in.Repo) // the datum env var is named after its repo (hyphens sanitized)
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

// sideKey is the resolve-loop key for a repo side: its declared name, else
// its repo (matching the resolve func's key derivation).
func sideKey(s client.Input) string {
	if s.Name != "" {
		return s.Name
	}
	return s.Repo
}

// createPipeline validates and persists a pipeline. Pipeline names are
// unique: a create whose name matches an existing pipeline is rejected
// with an 'already exists' error unless the update flag is set — identical
// configuration does not exempt a duplicate, and the duplicate is surfaced
// at create time, not at first job scheduling. Every declared input
// repository must exist at creation: a file input referencing a missing
// repo is rejected synchronously with a 'not found' error, rather than
// deferred to job scheduling or datum processing.
func (d *daemon) createPipeline(p client.Pipeline) error {
	if err := validatePipelineSpec(p); err != nil {
		return err
	}
	if err := validateService(p); err != nil {
		return err
	}
	if err := validateSpout(p); err != nil {
		return err
	}
	if err := validateSecrets(p); err != nil {
		return err
	}
	if p.Input == nil {
		// a spout has no input repo to check; anything else with no input
		// is rejected by the spec validation's "no input set"
		if p.Spout == nil {
			return fmt.Errorf("no input set")
		}
	} else if _, err := os.Stat(d.store.RepoDir(p.Input.Repo)); err != nil {
		return notFound("input repo %q not found", p.Input.Repo)
	}
	if p.Service != nil && d.externalPortTaken(p.Service.ExternalPort, p.Name) {
		return fmt.Errorf("external port %d is already declared by another service pipeline", p.Service.ExternalPort)
	}
	for _, m := range p.Transform.Secrets {
		// a pipeline consumes a secret only through an explicit reference
		// to an existing secret; the reference must be a valid name (it
		// becomes a path component at provisioning time)
		if !store.ValidName(m.Name) {
			return fmt.Errorf("invalid secret name %q", m.Name)
		}
		if _, err := d.loadSecret(m.Name); err != nil {
			return notFound("secret %q not found", m.Name)
		}
	}
	// materialize the input's implicit defaults into the stored spec so
	// extraction echoes them: every side's name defaults to its
	// repo and its branch to master
	materializeInputDefaults(p.Input)

	// update (or create) branching. A corrupt record is an incomplete
	// pipeline: not updatable, not silently recreated.
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
		// data-bearing cycle
		d.spawnSpoutJob(rec, false)
		return nil
	}
	if p.Service != nil {
		// a service's job is its own: one long-lived process serving the
		// input, never a datum run
		d.spawnServiceJob(rec)
		return nil
	}
	d.scheduleHeadJob(rec)
	d.standbyIdle(rec) // a standby pipeline with no input head parks in standby
	return nil
}

// applyCreate persists a pipeline's version-1 metadata: the spec commit
// and the head record with its immutable version archive. It does not
// schedule any job; the caller decides when the pipeline runs. A transform
// with stdin lines but no executable command is accepted at creation —
// the command's absence is a start-time failure, not a creation-time
// validation error — and the pipeline is immediately recorded in the
// failure state with reason 'no command specified but stdin lines
// provided', so it can never run rather than hanging.
func (d *daemon) applyCreate(p client.Pipeline) (*pipelineRec, error) {
	p.Update = false
	// cron inputs get their derived repositories and their schedules
	// started; size triggers get their accumulation branches — the
	// derivations mutate the stored spec
	d.deriveCronRepos(&p)
	d.deriveGitRepos(&p)
	d.deriveTriggerBranches(&p)
	// the output repo exists from creation: downstream pipelines can be
	// defined against it before it has any commits — including a
	// stats-enabled pipeline's "stats" branch. An existing repo (a
	// keepRepo delete followed by a recreate) is reused as-is.
	if _, err := os.Stat(d.store.RepoDir(p.Name)); err != nil {
		if err := d.store.CreateRepo(p.Name); err != nil {
			return nil, err
		}
	}
	rec := pipelineRec{Pipeline: p, State: stateRunning, Version: 1}
	if len(p.Transform.Cmd) == 0 && len(p.Transform.Stdin) > 0 {
		// No command to feed the stdin lines to: accepted, but the pipeline
		// fails as soon as it would start.
		rec.State = stateFailure
		rec.Reason = reasonNoCommandStdin
	}
	for _, s := range inputSides(p.Input) {
		if s.Cron != "" {
			d.startCronTicker(p.Name, s.Name, s.Cron, s.Overwrite)
		}
	}
	// The spec commit is durable before the pipeline is considered created:
	// a failed create leaves no spec commit behind because the validation
	// above ran first.
	rec.SpecCommit = d.writeSpecCommit(p.Name, p, 1)
	d.archiveVersion(&rec)
	if err := d.savePipeline(&rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// scheduleHeadJob processes the input heads once under the pipeline's
// current version — each side at its current head — when any side has a
// finished head and the pipeline is able to run. Failure and stopped
// pipelines never run. The model is exactly one job per triggering input
// commit, and each job writes exactly one output commit: a single-input
// pipeline therefore produces one output commit per input commit (no
// extra, no missing — the count is part of the copy-pipeline contract).
// A pipeline created over existing history processes only the current
// head, in one output commit: each input side is paired at its finished
// head and a single job spawns over the full accumulated head content —
// older history is not replayed, and historical commits never each
// trigger a job at creation. An update re-runs the head under the new
// version the same way. It returns the spawned job's id, or "" when
// nothing was scheduled — the caller can wait for exactly that job to
// settle.
func (d *daemon) scheduleHeadJob(rec *pipelineRec) string {
	if rec.State == stateFailure || rec.Stopped {
		return ""
	}
	heads := d.pairHeads(rec.Pipeline.Input)
	for _, h := range heads {
		if h.ID != "" {
			return d.spawnJob(rec, heads, "", "", nil)
		}
	}
	// a nested union/cross may still consume repos whose heads exist even
	// when no side has a direct head (union of crosses, cross of unions):
	// schedule the head job; the resolve loop picks up each nested branch's
	// head
	var nestedAny func(in *client.Input) bool
	nestedAny = func(in *client.Input) bool {
		for _, s := range inputSides(in) {
			if s.Repo != "" {
				if _, err := d.store.HeadCommitRec(s.Repo, inputBranch(s)); err == nil {
					return true
				}
			} else if len(s.Cross) > 0 || len(s.Union) > 0 {
				if nestedAny(&s) {
					return true
				}
			}
		}
		return false
	}
	if nestedAny(rec.Pipeline.Input) {
		return d.spawnJob(rec, heads, "", "", nil)
	}
	return ""
}

// A standby pipeline's activation is counted so its settle hook never
// races an incoming job: spawnJob increments before the job can run, and
// the job's settle decrements and returns the pipeline to standby when the
// count reaches zero.
var (
	standbyMu     sync.Mutex
	standbyActive = map[string]int{}
)

// spawnJob launches a job, activating a standby pipeline synchronously:
// the activation count is incremented and the state moves to "running"
// before the goroutine can start, so a settling predecessor can never
// observe quiescence while a new job is on its way. A standby pipeline
// idles in the standby state, wakes to the running state when input
// arrives, and returns to standby once the work settles; there is no
// distinct partially-scheduled standby state — a partial-capacity or
// provisioning condition surfaces as the crashed/failed state, so a
// standby pipeline that fails to provision crashes rather than resting in
// standby. heads is the job's input pairing — one commit per input side,
// empty when a side has no head (its cross contributes no datums).
func (d *daemon) spawnJob(rec *pipelineRec, heads []client.Commit, propagated, id string, pre *jobRec) string {
	if rec.Pipeline.Standby {
		standbyMu.Lock()
		standbyActive[rec.Pipeline.Name]++
		if rec.State != stateRunning {
			rec.State = stateRunning
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
	// run the old version indefinitely
	rj := d.registerRunning(id, rec.Pipeline.Name)
	go guard(func() {
		// a panic must not abandon the job with a forever-"running"
		// record: settle it failed first (the wedge-breaker), then let
		// the guard log the stack
		defer func() {
			if r := recover(); r != nil {
				d.settlePanicJob(id)
				panic(r)
			}
		}()
		d.runJob(*rec, heads, id, propagated, pre, rj)
	})
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
	pipelineRecMu.Lock()
	defer pipelineRecMu.Unlock()
	rec, err := d.loadPipeline(name)
	if err != nil || !rec.Pipeline.Standby || rec.Stopped {
		return
	}
	if rec.State == stateRunning {
		rec.State = stateStandby
		d.savePipeline(rec)
	}
}

// runPipeline manually triggers a pipeline run. Provenance, when
// non-empty, fixes the exact input revisions the job processes — one per
// side, matched by the side's repo and branch; two commits of the same
// branch are rejected, and a commit outside the pipeline's input lineage
// is rejected. With no provenance the current branch heads are used, and a
// pipeline with no input commits and no provenance errors as unrunnable.
// JobID re-executes an existing job's input pairing, adding a job rather
// than replacing it. The run's job carries the Manual flag and its output
// never propagates downstream (a manual run is not a processing wave).
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
		cm, err := d.store.LoadCommitByID(pid)
		if err != nil {
			return client.Job{}, notFound("provenance commit %s: not found", pid)
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
			// exist (an unrunnable pipeline errors)
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
// standby state whenever it has no finished input head to process and is
// not stopped, failed, or crashed: with no finished input head on any
// side, nothing will be scheduled until a commit arrives. The pipeline
// wakes to the running state only when input arrives and returns to
// standby once its work settles; wake-up need not activate an entire chain
// at once, and consecutive jobs reuse the standby pipeline's execution
// participant without per-job reconfiguration.
func (d *daemon) standbyIdle(rec *pipelineRec) {
	if !rec.Pipeline.Standby || rec.Stopped || rec.State == stateFailure || rec.State == stateCrashed {
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
		rec.State = stateStandby
		d.savePipeline(rec)
	}
}

// updatePipeline applies a new version of an existing pipeline: the new
// transform governs all jobs created after the update while historical
// jobs keep their original transform in metadata, and each update
// provisions a fresh set of execution participants under the new version,
// retiring the previous version's so none of any older version linger.
// In-flight jobs of the previous version are terminated and recorded as
// killed — no old-version work may race the new head job. Every update is
// itself a processing event: it processes the current input head under the
// new transform, producing a new output commit and at least one new job,
// whether or not reprocessing is requested (with Reprocess it re-runs the
// head and makes the new output visible at the pipeline's branch). A
// failing pipeline is a valid update target — updating it to a working
// command creates a new job for the same input, leaving the prior failed
// job in history and the pipeline identity unchanged — and a pipeline
// crashed because its execution environment could not be provisioned never
// wedges the update path either. An update issued for a nonexistent
// pipeline is a create, not an error. A stopped pipeline stays stopped,
// and an unfinished stats commit left by a previous version must not
// deadlock later jobs: statistics are one-way and cannot be dropped on
// update.
func (d *daemon) updatePipeline(existing *pipelineRec, p client.Pipeline) error {
	d.cancelPipelineJobs(existing.Pipeline.Name) // no old-version work may race the new head job
	rec, err := d.applyUpdate(existing, p)
	if err != nil {
		return err
	}
	if p.Spout != nil {
		// the update killed the old spout job; the new epoch starts fresh
		// (a reprocess update resets the marker state)
		d.spawnSpoutJob(rec, p.Reprocess)
		return nil
	}
	if p.Service != nil {
		// the update killed the old service process; the new declaration
		// serves the current input head
		d.spawnServiceJob(rec)
		return nil
	}
	d.scheduleHeadJob(rec)
	d.standbyIdle(rec)
	return nil
}

// applyUpdate persists a new version of an existing pipeline — the spec
// commit, the version archive, and the head record — without scheduling
// any job. In-flight work cancellation is the caller's job so a
// transaction can coordinate it. An update must not implicitly restart a
// paused pipeline: updating a stopped pipeline increments its version and
// applies the new configuration but leaves the state paused, producing no
// new output commit — input written while paused accumulates in the head
// and is processed only after the pipeline is started again. Per-datum
// statistics are a one-way flag: an update may enable them, but an update
// attempting to disable them is rejected with an error rather than
// silently ignored.
func (d *daemon) applyUpdate(existing *pipelineRec, p client.Pipeline) (*pipelineRec, error) {
	name := existing.Pipeline.Name
	p.Update = false
	if existing.Pipeline.EnableStats && !p.EnableStats {
		// per-datum statistics are one-way: an update cannot disable them
		return nil, fmt.Errorf("statistics cannot be disabled once enabled")
	}
	// cron inputs keep their derived repositories; the existing tickers
	// are keyed by those repositories and are left running — an update
	// must not restart the cron clock. Trigger branches are reused across
	// updates.
	d.deriveCronRepos(&p)
	d.deriveGitRepos(&p)
	d.deriveTriggerBranches(&p)
	for _, s := range inputSides(p.Input) {
		if s.Cron != "" {
			d.startCronTicker(p.Name, s.Name, s.Cron, s.Overwrite)
		}
	}
	v := existing.Version + 1
	rec := pipelineRec{
		Pipeline:  p,
		State:     stateRunning,
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
		rec.State = stateFailure
		rec.Reason = reasonNoCommandStdin
	} else if existing.Stopped {
		rec.State = statePaused // an update must not restart a paused pipeline
	}
	rec.SpecCommit = d.writeSpecCommit(name, p, v)
	d.archiveVersion(&rec)
	if err := d.savePipeline(&rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// writeSpecCommit records one pipeline definition as a commit in the spec
// repository: one commit per definition, written only after validation
// passed. It returns the commit id — the pipeline's provenance anchor for
// spout epochs.
func (d *daemon) writeSpecCommit(name string, spec client.Pipeline, version int) string {
	b, err := json.Marshal(spec)
	if err != nil {
		return ""
	}
	cm, err := d.store.StartCommit("spec", defaultBranch, fmt.Sprintf("pipeline %s v%d", name, version))
	if err != nil {
		return ""
	}
	if err := d.store.OverwriteFile(cm.ID, "spec.json", b); err != nil {
		return ""
	}
	if _, err := d.store.FinishCommit(cm.ID, "", false); err != nil {
		return ""
	}
	return cm.ID
}

func (d *daemon) versionPath(name string, version int) string {
	return filepath.Join(d.state, "pipelines", "versions", name, fmt.Sprintf("%d.json", version))
}

// archiveVersion persists an immutable copy of a pipeline version, keeping
// the history addressable by ancestry.
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

// stopPipeline pauses the pipeline: the persistent Stopped flag is set,
// the transient state reports statePaused, and the input head at stop time
// becomes the backlog watermark. Stopping writes no output commit, so
// downstream pipelines watching this one are never triggered by the stop —
// a stop must never look like new input data. The stopped condition is
// durable and distinct from the transient state, so a restarting daemon
// remembers the pipeline is stopped. A spout declares no input (it is
// rejected with one), so it has no watermark — stopping it just ends the
// background job.
func (d *daemon) stopPipeline(name string) error {
	pipelineRecMu.Lock()
	defer pipelineRecMu.Unlock()
	rec, err := d.loadPipeline(name)
	if err != nil {
		return notFound("pipeline %q not found", name)
	}
	rec.Stopped = true
	rec.State = statePaused
	if rec.Pipeline.Input != nil {
		if head, err := d.store.HeadCommitRec(rec.Pipeline.Input.Repo, defaultBranch); err == nil {
			rec.StoppedAt = head.ID
		}
	}
	// a paused pipeline's in-flight work stops: stopping ends active
	// processing, so garbage collection can proceed and the
	// paused pipeline holds no containers
	d.cancelPipelineJobs(name)
	return d.savePipeline(rec)
}

// startPipeline resumes the pipeline and processes the backlog: commits
// finished while it was stopped — which produced no jobs and no output
// commits, yet were retained — are consumed together as one job over the
// current branch head, the accumulated view of everything finished while
// stopped, stopping short of the stop-time watermark (a job already run
// for the head commit is not re-run). A standby pipeline paused while
// stopped wakes the same way: the commits accumulated during the pause are
// consumed together as one job producing exactly one additional output
// commit, and the pipeline returns to standby afterward.
func (d *daemon) startPipeline(name string) error {
	pipelineRecMu.Lock()
	defer pipelineRecMu.Unlock()
	rec, err := d.loadPipeline(name)
	if err != nil {
		return notFound("pipeline %q not found", name)
	}
	if !rec.Stopped {
		return nil // already running
	}
	stopAt := rec.StoppedAt
	rec.Stopped = false
	rec.State = stateRunning
	rec.StoppedAt = ""
	err = d.savePipeline(rec)
	if err != nil {
		return err
	}
	if rec.Pipeline.Service != nil {
		// a stopped service was cancelled; starting it brings the long-
		// lived process back up serving the current input head
		d.spawnServiceJob(rec)
		return nil
	}
	if rec.Pipeline.Spout != nil {
		// a stopped spout was cancelled; starting it resumes the spout
		// from its preserved marker state (a plain restart does not reset
		// the marker) — there is no input head to process, so no backlog
		// path exists
		d.spawnSpoutJob(rec, false)
		return nil
	}
	chain := d.store.ChainFromHead(rec.Pipeline.Input.Repo, defaultBranch, stopAt)
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

// pipelineRecMu serializes load-modify-save sequences on pipeline records:
// state transitions arrive from concurrent goroutines (standby settle,
// placement crash marking, stop/start), and an unserialized sequence loses
// updates. savePipeline's tmp+rename makes each write atomic against
// concurrent readers. Lock order: standbyMu → pipelineRecMu (standbySettle);
// no path takes pipelineRecMu before standbyMu.
var pipelineRecMu sync.Mutex

func (d *daemon) savePipeline(rec *pipelineRec) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	p := d.pipelinePath(rec.Pipeline.Name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, p); err != nil {
		return err
	}
	// a pipeline state change can settle an empty flush (consumers
	// settled): wake the blocking waits
	d.stateChanged.signal()
	return nil
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

// inspectPipeline inspects a pipeline's metadata. Ancestry 0 is the
// current version, and ancestry k addresses version current-k, returning
// that version's original spec: every update archives an immutable
// version, so historical versions are retrievable and strictly ordered by
// ancestry even for a pipeline that has never run. The inspection reports
// a per-state job count — JobCounts maps every job state to the number of
// that pipeline's jobs currently in that state, derived live from the job
// records and keyed strictly by state, so a successful job increments only
// the success bucket — and returns the pipeline's optional free-form
// description byte-for-byte, with no transformation (no trimming,
// defaulting, or truncation) applied to it.
func (d *daemon) inspectPipeline(name string, ancestry int) (client.PipelineInfo, error) {
	rec, err := d.loadPipeline(name)
	if err != nil {
		if _, statErr := os.Stat(d.pipelinePath(name)); statErr == nil {
			return client.PipelineInfo{}, fmt.Errorf("pipeline %q is incomplete", name)
		}
		return client.PipelineInfo{}, notFound("pipeline %q not found", name)
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
	// ancestry k addresses version current-k
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
// definition content is lost becomes incomplete and only its name is
// recoverable: the ordinary listing errors rather than returning a partial
// result, while listing with allowIncomplete returns the name-only entry.
// Such a pipeline is not silently recreated or repaired by an update, but
// deletion succeeds by name without the missing definition.
func (d *daemon) listPipelinesFiltered(history *int, name string, allowIncomplete bool) ([]client.PipelineInfo, error) {
	entries, err := os.ReadDir(filepath.Join(d.state, "pipelines"))
	if err != nil {
		if os.IsNotExist(err) {
			// an empty cluster lists as [] — a JSON null would break the
			// dashboard's list consumers, which expect an array
			return []client.PipelineInfo{}, nil
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

	// an empty pipelines directory must list as [] — never JSON null,
	// which breaks the dashboard's array consumers
	out := []client.PipelineInfo{}
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
		Service:      rec.Pipeline.Service,
		Egress:       rec.Pipeline.Egress,
		Secrets:      rec.Pipeline.Transform.Secrets,
		Placement:    rec.Pipeline.Placement,
	}
}

// deletePipeline removes a pipeline. A pipeline whose output feeds a
// downstream pipeline is refused unless force is set — the non-forced
// delete errors naming the downstream consumer, and a forced delete
// overrides the mid-DAG guard; the same contract holds whether the delete
// runs as one atomic transaction or is decomposed into a split
// transaction. Deleting a standby pipeline fully terminates its background
// monitoring and cancels its in-flight jobs, so no leaked goroutine or
// in-flight job wedges the controller, and a replacement pipeline over the
// same input still works end to end. The delete fully removes the
// pipeline's incarnation — its job records, dedup table, and version
// archive — so a later create under the same name is a fresh incarnation
// that reprocesses the input head into a new output commit; job listing
// for a deleted pipeline is an error, not an empty list. The output
// repository is removed unless keepRepo is set, in which case it and all
// committed data are preserved and a re-created pipeline of the same name
// attaches to the existing repository rather than resetting it. An
// incomplete pipeline is deletable by name only.
func (d *daemon) deletePipeline(name string, force, keepRepo bool) error {
	rec, loadErr := d.loadPipeline(name)
	if loadErr != nil {
		if _, err := os.Stat(d.pipelinePath(name)); err != nil {
			return nil // deleting an already-deleted pipeline is a no-op
		}
		// incomplete pipeline: name-only delete
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
	// records (no orphaned job listings). The tickers stop first: a cron
	// tick landing between the cancel scan and the ticker stop would create
	// a job that escapes the cancel and keeps the registry busy — garbage
	// collection must not see a ghost running job after the delete returns.
	d.stopCronTickers(name)     // a deleted pipeline's schedule stops
	d.clearTriggerLedgers(name) // its trigger accumulation goes too
	d.cancelPipelineJobs(name)
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		d.jobsMu.Lock()
		late := 0
		for _, rj := range d.running {
			if rj.pipeline == name {
				late++
			}
		}
		d.jobsMu.Unlock()
		if late == 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
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
		if _, err := os.Stat(d.store.RepoDir(name)); err == nil {
			// a repo that survives its pipeline's deletion keeps the
			// pipeline's blobs referenced forever (its commits are the
			// only references) — a silent failure here leaks the whole
			// tree and wedges garbage collection
			if err := d.store.DeleteRepo(name, true); err != nil {
				return fmt.Errorf("delete pipeline %q: output repo: %w", name, err)
			}
		}
		// The pipeline's side repos go with it unless another pipeline
		// still references them: the git-derived mapped repos (shared
		// by pipelines bound to the same URL or consumed as a plain
		// input) and the cron tick repos. A surviving side repo
		// keeps its blobs referenced forever — the pushed tree or the
		// tick files — leaking and wedging garbage collection.
		if rec != nil && rec.Pipeline.Input != nil {
			for _, repo := range gitSideRepos(rec.Pipeline.Input) {
				if repo == name || d.repoReferencedByOther(repo, name) {
					continue
				}
				if _, err := os.Stat(d.store.RepoDir(repo)); err == nil {
					if err := d.store.DeleteRepo(repo, true); err != nil {
						return fmt.Errorf("delete pipeline %q: git repo %q: %w", name, repo, err)
					}
				}
			}
			for _, repo := range cronSideRepos(name, rec.Pipeline.Input) {
				if repo == name || d.repoReferencedByOther(repo, name) {
					continue
				}
				if _, err := os.Stat(d.store.RepoDir(repo)); err == nil {
					if err := d.store.DeleteRepo(repo, true); err != nil {
						return fmt.Errorf("delete pipeline %q: cron repo %q: %w", name, repo, err)
					}
				}
			}
		}
	}
	return nil
}
