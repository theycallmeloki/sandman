package main

import "testing"

// The host-stat parsers are the platform-free half of host sampling: one
// per platform format, exercised here on every platform that can build
// the program, because the darwin ones cannot be run from Linux CI.

func TestParseProcStat(t *testing.T) {
	// /proc/stat's aggregate line: user nice system idle iowait irq
	// softirq steal guest guest_nice
	b := []byte("cpu  100 20 300 4000 50 0 10 0 0 0\ncpu0 1 2 3 4 5 6 7 8 9 10\n")
	got := parseProcStat(b)
	if want := uint64(100 + 20 + 300 + 4000 + 50 + 10); got.total != want {
		t.Errorf("total = %d, want %d", got.total, want)
	}
	// idle counts iowait too: waiting on disk is not work.
	if want := uint64(4000 + 50); got.idle != want {
		t.Errorf("idle = %d, want %d (idle + iowait)", got.idle, want)
	}
}

func TestParseProcStatShortLine(t *testing.T) {
	if got := parseProcStat([]byte("cpu  1 2 3\n")); got != (cpuSample{}) {
		t.Errorf("truncated line = %+v, want zero sample", got)
	}
}

func TestParseMeminfo(t *testing.T) {
	b := []byte("MemTotal:       16384000 kB\nMemFree:         1000000 kB\nMemAvailable:    8192000 kB\n")
	total, used := parseMeminfo(b)
	if total != 16384000*1024 {
		t.Errorf("total = %d, want %d", total, 16384000*1024)
	}
	if want := uint64((16384000 - 8192000) * 1024); used != want {
		t.Errorf("used = %d, want %d (total - available)", used, want)
	}
	// a host that cannot be read reports zero, never a fabricated figure
	if total, used := parseMeminfo([]byte("garbage\n")); total != 0 || used != 0 {
		t.Errorf("unparsable meminfo = (%d, %d), want (0, 0)", total, used)
	}
}

func TestParseVmStat(t *testing.T) {
	// vm_stat output with 16 KiB pages (Apple Silicon)
	out := `Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                               23027.
Pages active:                            368949.
Pages inactive:                          365498.
Pages speculative:                         5818.
Pages throttled:                              0.
Pages wired down:                        165169.
Pages purgeable:                           4256.
"Translation faults":                 2305393220.
`
	got, pageSize, ok := parseVmStat(out)
	if !ok {
		t.Fatal("parseVmStat reported failure on real vm_stat output")
	}
	if pageSize != 16384 {
		t.Errorf("page size = %d, want 16384", pageSize)
	}
	if want := uint64(23027 + 365498 + 5818 + 4256); got != want {
		t.Errorf("available pages = %d, want %d (free + inactive + speculative + purgeable)", got, want)
	}
}

func TestParseVmStatIncomplete(t *testing.T) {
	// a missing page state means the count cannot be trusted: reporting
	// the sum of the rest would silently understate "available" and
	// overstate memory pressure.
	out := "Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: 100.\nPages inactive: 200.\n"
	if _, _, ok := parseVmStat(out); ok {
		t.Error("parseVmStat accepted output missing the speculative/purgeable states")
	}
	if _, _, ok := parseVmStat("no header here\n"); ok {
		t.Error("parseVmStat accepted output without a page size header")
	}
}

func TestCpuBusyDelta(t *testing.T) {
	// half the window busy
	if got := cpuBusyDelta(cpuSample{idle: 100, total: 200}, cpuSample{idle: 150, total: 300}); got != 50000 {
		t.Errorf("50%% busy = %d, want 50000", got)
	}
	for _, c := range []struct {
		name     string
		prev, cu cpuSample
	}{
		{"empty window", cpuSample{idle: 10, total: 20}, cpuSample{idle: 10, total: 20}},
		{"counter reset", cpuSample{idle: 10, total: 20}, cpuSample{idle: 5, total: 30}},
		{"idle went backwards", cpuSample{idle: 10, total: 20}, cpuSample{idle: 9, total: 30}},
	} {
		if got := cpuBusyDelta(c.prev, c.cu); got != 0 {
			t.Errorf("%s = %d, want 0", c.name, got)
		}
	}
}
