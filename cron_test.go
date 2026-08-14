package main

import "testing"

// TestStopCronTickersExactOwner pins the rule that cron repos are named
// <pipeline>-<input>, so a prefix match would let a "foo" cleanup stop
// "foo-bar"'s schedule. The cleanup must stop exactly the pipelines that
// own the tickers — never a name-colliding neighbor.
func TestStopCronTickersExactOwner(t *testing.T) {
	d := &daemon{}
	d.startCronTicker("foo", "a", "@every 1h", false)     // repo foo-a
	d.startCronTicker("foo-bar", "a", "@every 1h", false) // repo foo-bar-a
	defer d.stopCronTickers("foo-bar")                    // stop the survivor
	d.stopCronTickers("foo")

	d.cronMu.Lock()
	_, fooStill := d.cronTickers["foo-a"]
	_, barKept := d.cronTickers["foo-bar-a"]
	d.cronMu.Unlock()
	if fooStill {
		t.Fatal("foo's own ticker was not stopped by foo's cleanup")
	}
	if !barKept {
		t.Fatal("foo-bar's ticker was stopped by foo's cleanup (prefix collision)")
	}
}
