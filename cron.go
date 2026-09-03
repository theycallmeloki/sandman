// Cron inputs: a scheduled input whose ticks commit a
// time-stamped file into an auto-created repository, triggering the
// pipeline. A manual trigger creates the tick immediately.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sandman/client"
)

// cronRepo is a cron input's derived repository name: named after the
// pipeline and the input.
func cronRepo(pipeline, name string) string {
	return pipeline + "-" + name
}

// cronDuration parses an "@every <duration>" schedule; empty or malformed
// schedules never tick.
func cronDuration(schedule string) (time.Duration, error) {
	s := strings.TrimSpace(schedule)
	s = strings.TrimPrefix(s, "@every ")
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid cron schedule %q", schedule)
	}
	return d, nil
}

// deriveCronRepos resolves a pipeline spec's cron inputs: each cron
// side's repository is derived from the pipeline and the input's name and
// created, and the side gets the default glob — the stored spec's sides
// then carry real repos for triggering, pairing, and enumeration.
func (d *daemon) deriveCronRepos(p *client.Pipeline) {
	walkInputs(p.Input, func(in *client.Input) {
		if in.Cron != "" {
			in.Repo = cronRepo(p.Name, in.Name)
			if in.Glob == "" {
				in.Glob = "/*"
			}
			d.store.CreateRepo(in.Repo)
		}
	})
}

// cronTicker is one pipeline's cron schedule: the owning pipeline and
// the ticker's cancel. The owner enables exact-name cleanup: cron repos
// are named <pipeline>-<input>, so a prefix match would let a "foo"
// cleanup stop "foo-bar"'s schedule.
type cronTicker struct {
	owner  string
	cancel context.CancelFunc
}

// startCronTicker begins a cron input's schedule: every interval a tick
// commit lands in the cron repository, triggering the pipeline. The
// ticker is keyed by the cron repository, not the pipeline version, so
// rapid spec updates — even with reprocessing — never restart the clock,
// stall it, or double-schedule a tick; pipeline deletion stops it.
func (d *daemon) startCronTicker(pipeline, name, schedule string, overwrite bool) {
	dur, err := cronDuration(schedule)
	if err != nil {
		return
	}
	repo := cronRepo(pipeline, name)
	d.cronMu.Lock()
	if d.cronTickers == nil {
		d.cronTickers = map[string]cronTicker{}
	}
	if _, ok := d.cronTickers[repo]; ok {
		d.cronMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.cronTickers[repo] = cronTicker{owner: pipeline, cancel: cancel}
	d.cronMu.Unlock()
	go guard(func() {
		t := time.NewTicker(dur)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				d.cronTick(repo, overwrite)
			case <-ctx.Done():
				return
			}
		}
	})
}

// stopCronTickers stops every ticker owned by the pipeline (used by
// deletion and reset). Ownership is exact: cron repos are named
// <pipeline>-<input>, so a prefix match would also stop another
// pipeline's schedule — a "foo" cleanup must not kill "foo-bar"'s.
func (d *daemon) stopCronTickers(pipeline string) {
	d.cronMu.Lock()
	for repo, t := range d.cronTickers {
		if t.owner == pipeline {
			t.cancel()
			delete(d.cronTickers, repo)
		}
	}
	d.cronMu.Unlock()
}

// stopAllCronTickers cancels every cron ticker (reset): a reset removes
// the pipelines whose schedules own the tickers, and a surviving ticker
// would write into the freshly recreated store.
func (d *daemon) stopAllCronTickers() {
	d.cronMu.Lock()
	defer d.cronMu.Unlock()
	for repo, t := range d.cronTickers {
		t.cancel()
		delete(d.cronTickers, repo)
	}
}

// restartCronTickers re-arms every persisted pipeline's cron schedule at
// daemon boot. Tickers are in-memory goroutines created by apply/update,
// so a control-plane restart leaves every schedule dead until the next
// apply — the cadence silently stops (harvest starves; a restart during a
// roll was exactly that). Boot walks the persisted pipeline records and
// restarts each cron input's ticker; startCronTicker is idempotent per
// cron repo, so a racing apply cannot double-arm a schedule.
func (d *daemon) restartCronTickers() {
	entries, err := os.ReadDir(filepath.Join(d.state, "pipelines"))
	if err != nil {
		return
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".json")
		rec, err := d.loadPipeline(name)
		if err != nil || rec.Pipeline.Input == nil {
			continue
		}
		for _, s := range inputSides(rec.Pipeline.Input) {
			if s.Cron != "" {
				d.startCronTicker(rec.Pipeline.Name, s.Name, s.Cron, s.Overwrite)
			}
		}
	}
}

// cronTick creates one tick commit: a file named by the tick time (UTC
// RFC3339 with fractional seconds — a legal path with no glob
// metacharacters; the fractional part keeps a sub-second schedule from
// writing two ticks into one filename) lands in the cron input's
// auto-created repository. With overwrite the previous tick's file is
// tombstoned so the branch holds exactly one tick file; otherwise ticks
// accumulate.
func (d *daemon) cronTick(repo string, overwrite bool) {
	name := time.Now().UTC().Format(time.RFC3339Nano)
	d.commitRevision(repo, defaultBranch, func(commitID string) bool {
		if overwrite {
			if view, err := d.store.ResolveViewByID(commitID); err == nil {
				for p := range view {
					d.store.DeleteFile(commitID, p)
				}
			}
		}
		// an unreadable tick never publishes a partial revision
		return d.store.PutFile(commitID, name, []byte(name)) == nil
	}, nil)
}

// triggerCron creates an immediate tick on every cron input of the
// pipeline: scheduled ticks keep flowing, and a
// pipeline with no cron input errors.
func (d *daemon) triggerCron(pipeline string) error {
	rec, err := d.loadPipeline(pipeline)
	if err != nil {
		return err
	}
	any := false
	for _, s := range inputSides(rec.Pipeline.Input) {
		if s.Cron != "" {
			d.cronTick(cronRepo(pipeline, s.Name), s.Overwrite)
			any = true
		}
	}
	if !any {
		return fmt.Errorf("pipeline %q has no cron inputs", pipeline)
	}
	return nil
}
