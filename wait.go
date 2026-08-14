// Server-side blocking waits: the
// control plane broadcasts state changes and exposes long-polling wait
// endpoints, so clients and the harness block on server-signaled
// conditions instead of deadline-polling. The notifier is a broadcast
// condition variable: signal() wakes every waiter; a waiter re-checks
// its condition and waits again.
package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"sandman/client"
)

// notifier is a broadcast condition: every waiter holds a channel that
// signal() closes; waiters re-check their condition and re-wait. The
// grab-then-check discipline (register the channel BEFORE reading state)
// makes the check immune to signals that land mid-read.
type notifier struct {
	mu sync.Mutex
	ch chan struct{}
}

// signal wakes every current waiter. Safe to call from any state-change
// path (job records, commit finishes, pipeline states).
func (n *notifier) signal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ch == nil {
		n.ch = make(chan struct{})
	}
	close(n.ch)
	n.ch = make(chan struct{})
}

// changed returns the current wait channel: closed the next time signal()
// fires after this call.
func (n *notifier) changed() chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ch == nil {
		n.ch = make(chan struct{})
	}
	return n.ch
}

// stabilityWindow is how long a terminal flush snapshot must hold quiet
// before it is returned — the server-side analogue of the client's
// double-read: short enough not to tax the flush, long enough to absorb a
// trailing trigger.
const stabilityWindow = 250 * time.Millisecond

// waitFor polls cond on every state-change broadcast until it holds,
// timing out at deadline. The single long-poll loop: machine slowness
// delays the response, it never fails the test (R-5).
func (d *daemon) waitFor(deadline time.Time, cond func() bool) bool {
	for {
		// grab-then-check: register the broadcast channel BEFORE reading
		// state, so a signal landing mid-read is not lost
		ch := d.stateChanged.changed()
		if cond() {
			return true
		}
		wait := time.Until(deadline)
		if wait <= 0 {
			return cond()
		}
		// a broadcast can still land between changed() and this select
		// (the channel snapshot predates the signal): bound each wait
		// with a short poll tick so settle latency is jitter, never the
		// full remaining deadline
		if wait > 100*time.Millisecond {
			wait = 100 * time.Millisecond
		}
		t := time.NewTimer(wait)
		select {
		case <-ch:
			t.Stop()
		case <-t.C:
			t.Stop()
		}
	}
}

// jobWaitH is the job-terminal long poll: GET /api/v1/jobs/{id}/wait
// blocks until the job reaches a terminal state (or the timeout elapses,
// returning the current state with a timeout error).
func (d *daemon) jobWaitH(w http.ResponseWriter, r *http.Request) error {
	timeout := 30 * time.Second
	if t := r.URL.Query().Get("timeout"); t != "" {
		if n, err := time.ParseDuration(t); err == nil {
			timeout = n
		}
	}
	deadline := time.Now().Add(timeout)
	id := r.PathValue("id")
	terminal := false
	ok := d.waitFor(deadline, func() bool {
		j, err := d.inspectJob(id)
		if err != nil {
			return false
		}
		terminal = j.State != stateRunning
		return terminal
	})
	j, err := d.inspectJob(id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("job %q did not settle within %s (state %s)", id, timeout, j.State)
	}
	writeJSON(w, j)
	return nil
}

// flushH is the blocking flush: POST /api/v1/flush with
// {"commits": [...], "timeout": "60s"} runs the flush loop server-side —
// the downstream closure of the commits, terminal and stable — and
// returns the jobs. A timeout returns the jobs seen so far with a
// timedOut flag (the client surfaces it as the flush error).
func (d *daemon) flushH(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Commits []string `json:"commits"`
		Timeout string   `json:"timeout,omitempty"`
	}
	if err := decodeBody(r, &body); err != nil {
		return fmt.Errorf("invalid request body")
	}
	timeout := 60 * time.Second
	if body.Timeout != "" {
		if dur, err := time.ParseDuration(body.Timeout); err == nil {
			timeout = dur
		}
	}
	jobs, timedOut, err := d.flushSet(body.Commits, timeout)
	if err != nil {
		return err
	}
	writeJSON(w, map[string]any{"jobs": jobs, "timedOut": timedOut})
	return nil
}

// flushSet waits until every job triggered by the commits — including
// jobs of downstream stages — is terminal, and returns them, deduplicated
// per pipeline keeping the latest (the flush contract: exactly one job
// per pipeline stage per input wave; the algorithm mirrors the client's,
// but the waits are server-signaled state broadcasts, not deadline
// polls). A terminal
// snapshot is only final once no state change arrives to contradict it:
// the job graph can still be growing (head backfill, downstream
// triggers). When no job exists, the flush terminates empty once every
// pipeline that could schedule work for the commits' repositories is
// settled (stopped, failed, or crashed).
func (d *daemon) flushSet(commitIDs []string, timeout time.Duration) ([]client.Job, bool, error) {
	deadline := time.Now().Add(timeout)
	var repos []string
	branches := map[string]string{}
	var relevant []client.Job
	for {
		// every path is deadline-bounded: a signal arriving faster than
		// the stability window must not starve the long-poll
		if time.Now().After(deadline) {
			return relevant, true, nil
		}
		// register the wait channel BEFORE reading state: a change that
		// lands during the read closes this channel, and the select below
		// wakes to re-evaluate (a change before the register is seen by
		// the read itself)
		ch := d.stateChanged.changed()
		jobs := d.mustListJobs()
		relevant = client.LatestPerPipeline(client.DownstreamJobsSet(jobs, commitIDs))
		if client.AllTerminal(relevant) {
			// stable: the terminal snapshot holds through a quiet window —
			// any state change re-evaluates (the graph may still be
			// growing: head backfill, downstream triggers)
			t := time.NewTimer(stabilityWindow)
			select {
			case <-ch:
				t.Stop()
				continue // the graph changed: re-evaluate
			case <-t.C:
				t.Stop()
			}
			// the trigger watcher is asynchronous: a terminal job's
			// finished output commit may not have spawned its downstream
			// job yet (the watcher goroutine can lag under load). A flush
			// returning now would miss that growth (4 of 5
			// stages returned). Wait while a trigger is still owed.
			if d.triggerPending(relevant) {
				continue
			}
			// the closure can grow between the loop-top computation and
			// this point: a downstream spawn landing in the window's tail
			// is visible to triggerPending's raw job list but not to the
			// closure below. Re-read the closure with a fresh wait
			// channel registered first (grab-then-check) — any change,
			// landed or landing, forces a re-evaluation instead of
			// returning a stale snapshot (4 of 5 stages).
			ch2 := d.stateChanged.changed()
			fresh := client.LatestPerPipeline(client.DownstreamJobsSet(d.mustListJobs(), commitIDs))
			if len(fresh) != len(relevant) {
				continue
			}
			select {
			case <-ch2:
				continue // a change landed during the recompute
			default:
			}
			return relevant, false, nil
		}
		if len(relevant) == 0 {
			if len(repos) == 0 {
				for _, id := range commitIDs {
					if cm, err := d.store.InspectCommit(id); err == nil {
						repos = append(repos, cm.Repo)
						branches[cm.Repo] = cm.Branch
					}
				}
			}
			settled := len(repos) > 0
			for _, repo := range repos {
				if !d.consumersSettled(repo, branches[repo]) {
					settled = false
					break
				}
			}
			if settled {
				// hold the settled verdict through a quiet window: a
				// trigger can still fire a job right after the check
				t := time.NewTimer(stabilityWindow)
				select {
				case <-ch:
					t.Stop()
					continue
				case <-t.C:
					t.Stop()
					return relevant, false, nil
				}
			}
		}
		wait := time.Until(deadline)
		if wait <= 0 {
			return relevant, true, nil
		}
		t := time.NewTimer(wait)
		select {
		case <-ch:
			t.Stop()
		case <-t.C:
			t.Stop()
			return relevant, true, nil
		}
	}
}

// consumersSettled reports whether every pipeline consuming the branch is
// settled (stopped, failed, or crashed) — nothing can schedule new work
// for the branch, so a flush with no jobs is complete (mirrors the
// client-side check).
func (d *daemon) consumersSettled(repo, branch string) bool {
	pipes, err := d.listPipelinesFiltered(nil, "", false)
	if err != nil {
		return false
	}
	consumers := 0
	for _, p := range pipes {
		if p.Input != nil && p.Input.Repo == repo {
			if client.InputBranch(*p.Input) != branch {
				continue
			}
			consumers++
			if p.State == stateRunning {
				return false
			}
		}
	}
	return true
}

// triggerPending reports whether the trigger watcher still owes the
// flush's closure a job: some relevant job's output commit is unfinished
// (its finish will fire the downstream trigger), or an active (running or
// standby) consumer of a relevant finished commit has no job for that
// pairing yet. The watcher processes commit finishes asynchronously, so
// the flush must not return while a spawn is pending.
func (d *daemon) triggerPending(jobs []client.Job) bool {
	for _, j := range jobs {
		if j.OutputCommit == "" {
			continue // no output commit, no downstream trigger
		}
		cm, err := d.store.InspectCommit(j.OutputCommit)
		if err != nil || !cm.Finished {
			// a terminal failure job's unfinished commit is orphaned (its
			// output repo was deleted mid-job): no trigger comes
			// from it, and waiting would stall the flush to its deadline.
			// A success job's unfinished commit is the finish still in
			// flight — wait for it.
			if j.State == stateFailure {
				continue
			}
			return true
		}
		pipes, err := d.listPipelinesFiltered(nil, "", false)
		if err != nil {
			return true
		}
		for _, p := range pipes {
			if p.Input == nil || (p.State != stateRunning && p.State != stateStandby) {
				continue // settled consumers spawn nothing
			}
			rec, err := d.loadPipeline(p.Name)
			if err != nil || rec.Pipeline.Service != nil {
				continue // services never spawn per commit
			}
			if !pipelineConsumes(rec.Pipeline.Input, cm.Repo, cm.Branch) {
				continue
			}
			if d.hasJobForPairing(p.Name, j.OutputCommit) {
				continue // the pairing job already exists
			}
			return true // the watcher will spawn this pairing
		}
	}
	return false
}

// hasJobForPairing reports whether any job of the pipeline lists the
// commit among its input commits — the trigger's dedup view.
func (d *daemon) hasJobForPairing(pipeline, commitID string) bool {
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
