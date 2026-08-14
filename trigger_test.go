package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestClearTriggerLedgersExactOwner pins M2: ledger names are
// <pipeline>-<pos>.json with an integer pos, so a prefix match would let
// a "foo" cleanup remove "foo-bar"'s ledgers. The cleanup must remove
// exactly the ledger names whose suffix parses as an integer — both
// directions, plus a crash-leftover tmp file of the pipeline's own.
func TestClearTriggerLedgersExactOwner(t *testing.T) {
	dir := t.TempDir()
	d := &daemon{state: dir}
	trig := filepath.Join(dir, "triggers")
	if err := os.MkdirAll(trig, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(names ...string) {
		for _, n := range names {
			if err := os.WriteFile(filepath.Join(trig, n), []byte("{}"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	list := func() []string {
		entries, err := os.ReadDir(trig)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, e := range entries {
			out = append(out, e.Name())
		}
		return out
	}

	write("foo-0.json", "foo-1.json", "foo-bar-0.json", "foo-0.json.tmp")
	d.clearTriggerLedgers("foo")
	if got := list(); len(got) != 1 || got[0] != "foo-bar-0.json" {
		t.Fatalf("after foo cleanup: %v, want [foo-bar-0.json]", got)
	}

	// the reverse direction: cleaning foo-bar leaves foo's ledgers alone
	write("foo-0.json", "foo-bar-0.json", "foo-bar-1.json")
	d.clearTriggerLedgers("foo-bar")
	if got := list(); len(got) != 1 || got[0] != "foo-0.json" {
		t.Fatalf("after foo-bar cleanup: %v, want [foo-0.json]", got)
	}
}
