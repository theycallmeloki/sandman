// Cron inputs (SB-089, SB-133): a scheduled input whose ticks commit a
// time-stamped file into an auto-created repository, triggering the
// pipeline. A manual trigger creates the tick immediately.
package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"sandman/client"
)

// cronRepo is a cron input's derived repository name: named after the
// pipeline and the input (SB-089).
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
// then carry real repos for triggering, pairing, and enumeration (SB-089).
func (d *daemon) deriveCronRepos(p *client.Pipeline) {
	var walk func(in *client.Input)
	walk = func(in *client.Input) {
		for i := range in.Cross {
			walk(&in.Cross[i])
		}
		for i := range in.Union {
			walk(&in.Union[i])
		}
		if in.Cron != "" {
			in.Repo = cronRepo(p.Name, in.Name)
			if in.Glob == "" {
				in.Glob = "/*"
			}
			d.store.CreateRepo(in.Repo)
		}
	}
	if p.Input != nil {
		walk(p.Input)
	}
}

// cronTicker is one pipeline's cron schedule: the owning pipeline and
// the ticker's cancel. The owner enables exact-name cleanup: cron repos
// are named <pipeline>-<input>, so a prefix match would let a "foo"
// cleanup stop "foo-bar"'s schedule (M2).
type cronTicker struct {
	owner  string
	cancel context.CancelFunc
}

// startCronTicker begins a cron input's schedule: every interval a tick
// commit lands in the cron repository, triggering the pipeline. The
// ticker is keyed by the cron repository, so pipeline updates never
// restart the clock (SB-133); pipeline deletion stops it.
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
// pipeline's schedule — a "foo" cleanup must not kill "foo-bar"'s (M2).
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

// cronTick creates one tick commit: a file named by the tick time (UTC
// RFC3339 with fractional seconds — a legal path with no glob
// metacharacters, SB-089; the fractional part keeps a sub-second schedule
// from writing two ticks into one filename). With overwrite the previous
// tick's file is tombstoned so the branch holds exactly one tick file.
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
// pipeline (SB-089 clauses 4-6): scheduled ticks keep flowing, and a
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
