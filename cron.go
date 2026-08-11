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
			d.store.createRepo(in.Repo)
		}
	}
	if p.Input != nil {
		walk(p.Input)
	}
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
		d.cronTickers = map[string]context.CancelFunc{}
	}
	if _, ok := d.cronTickers[repo]; ok {
		d.cronMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.cronTickers[repo] = cancel
	d.cronMu.Unlock()
	go func() {
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
	}()
}

// stopCronTickers stops every ticker whose cron repository belongs to the
// pipeline (used by deletion and reset).
func (d *daemon) stopCronTickers(pipeline string) {
	prefix := pipeline + "-"
	d.cronMu.Lock()
	for repo, cancel := range d.cronTickers {
		if strings.HasPrefix(repo, prefix) {
			cancel()
			delete(d.cronTickers, repo)
		}
	}
	d.cronMu.Unlock()
}

// cronTick creates one tick commit: a file named by the tick time (UTC
// RFC3339 — a legal path with no glob metacharacters, SB-089) in the cron
// repository. With overwrite the previous tick's file is tombstoned so
// the branch holds exactly one tick file.
func (d *daemon) cronTick(repo string, overwrite bool) {
	cm, err := d.store.startCommit(repo, defaultBranch, "")
	if err != nil {
		return
	}
	if overwrite {
		if view, err := d.store.resolveViewByID(cm.ID); err == nil {
			for p := range view {
				d.store.deleteFile(cm.ID, p)
			}
		}
	}
	name := time.Now().UTC().Format(time.RFC3339)
	if err := d.store.putFile(cm.ID, name, []byte(name)); err != nil {
		return
	}
	if fin, err := d.store.finishCommit(cm.ID, "", false); err == nil {
		// the tick is a real revision: trigger the consuming pipelines
		// (the store's finish does not fire the trigger — that is the
		// API layer's job)
		d.triggerForCommit(fin)
	}
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
