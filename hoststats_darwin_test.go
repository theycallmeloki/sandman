//go:build darwin

package main

import "testing"

// readMem shells out to /usr/sbin/sysctl and /usr/bin/vm_stat and fails
// soft: a wrong path or an unparsable format yields a zero figure and no
// error, so nothing but a live probe on a real Mac can tell a working
// sampler from a silently dead one. This runs on the macOS CI job; on
// Linux it is compiled away.
func TestDarwinHostMemReadable(t *testing.T) {
	total, used := readMem()
	if total == 0 {
		t.Fatal("readMem reported no total memory — the hw.memsize probe or the vm_stat parse is dead")
	}
	if used == 0 {
		t.Fatal("readMem reported zero used memory — vm_stat did not parse")
	}
	if used > total {
		t.Fatalf("readMem used %d > total %d", used, total)
	}
}

// readCpu is a documented zero on macOS (see hoststats_darwin.go): there
// is no cheap tick source left, so the node must report "no figure"
// rather than a fabricated one. This pins that no call can panic or
// return a sample the delta math would turn into a bogus percentage.
func TestDarwinHostCpuReportsNoFigure(t *testing.T) {
	if got := readCpu(); got != (cpuSample{}) {
		t.Fatalf("readCpu = %+v, want the zero sample (no macOS tick source)", got)
	}
	if got := cpuBusyDelta(cpuSample{}, readCpu()); got != 0 {
		t.Fatalf("cpuBusyDelta = %d, want 0", got)
	}
}
