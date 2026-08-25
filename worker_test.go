package main

import (
	"testing"
	"time"
)

// TestRunExecTimeoutTimerStopsOnCompletion pins the rule on the worker side:
// runExec's per-attempt timer must be stopped when the attempt completes
// (a restart re-runs with the same container name; a stale timer would
// kill the restarted attempt's container). The timer's kill goes through
// the killExecution hook (the containerd backend's Kill); recording the
// hook proves the timer never fired — no container runtime required.
func TestRunExecTimeoutTimerStopsOnCompletion(t *testing.T) {
	var killed []string
	old := killExecution
	killExecution = func(name string) { killed = append(killed, name) }
	defer func() { killExecution = old }()

	res := runExec("test-node", execRequest{
		Cname:        "sandman-j1-0-1",
		DatumTimeout: "50ms",
	})
	if res.PrimaryCode != 0 || res.Error != "" {
		t.Fatalf("runExec: code=%d err=%q", res.PrimaryCode, res.Error)
	}
	time.Sleep(150 * time.Millisecond)
	if len(killed) != 0 {
		t.Fatalf("stale timeout timer ran: kill %v", killed)
	}
}

// TestParseMemSize pins the memory-size parser used to map declared memory
// onto OCI cgroup resources (docker's own conventions: bare = bytes,
// suffix = binary units).
func TestParseMemSize(t *testing.T) {
	for in, want := range map[string]uint64{
		"100M":           100 * 1024 * 1024,
		"100m":           100 * 1024 * 1024,
		"1g":             1 << 30,
		"2G":             2 << 30,
		"64k":            64 << 10,
		"1000000000000b": 1000000000000,
		"1048576":        1048576,
		"512":            512,
		"1t":             1 << 40,
	} {
		got, err := parseMemSize(in)
		if err != nil {
			t.Errorf("parseMemSize(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("parseMemSize(%q) = %d, want %d", in, got, want)
		}
	}
	for _, bad := range []string{"", "abc", "12x", "-5M"} {
		if _, err := parseMemSize(bad); err == nil {
			t.Errorf("parseMemSize(%q): expected error", bad)
		}
	}
}

// TestRTMounts pins the mount-vocabulary translator: "-v" pairs become OCI
// bind mounts (ro mode honored), "-p" selects host networking.
func TestMountVocabulary(t *testing.T) {
	mounts, hostNet := rtMounts([]string{
		"-v", "/host/out:/sandman/out",
		"-v", "/host/in:/sandman/in/repo:ro",
		"-p", "127.0.0.1:8001:8001",
	})
	if !hostNet {
		t.Fatal("a -p entry must select host networking")
	}
	if len(mounts) != 2 {
		t.Fatalf("mounts = %d, want 2", len(mounts))
	}
	if mounts[0].Source != "/host/out" || mounts[0].Destination != "/sandman/out" {
		t.Errorf("mount[0] = %+v", mounts[0])
	}
	if mounts[1].Destination != "/sandman/in/repo" {
		t.Errorf("mount[1] = %+v", mounts[1])
	}
	ro := false
	for _, o := range mounts[1].Options {
		if o == "ro" {
			ro = true
		}
	}
	if !ro {
		t.Errorf("mount[1] options = %v, want ro", mounts[1].Options)
	}
	if _, hostNet := rtMounts(nil); hostNet {
		t.Fatal("no -p entry must keep bridge networking off (host networking off)")
	}
}
