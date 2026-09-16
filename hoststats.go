package main

import (
	"strconv"
	"strings"
)

// Host resource sampling. The daemon and the worker report the host's cpu
// count, memory totals, and cpu utilization on STATS (stats.go); the
// dashboard renders the same figures.
//
// Memory sampling is per-OS (hoststats_linux.go reads /proc/meminfo,
// hoststats_darwin.go reads hw.memsize + vm_stat) behind readCpu/readMem.
// This file holds the part that does not touch the OS: the sample type,
// the delta arithmetic, and one parser per platform's text format, so the
// parsing and the math stay testable on every platform that can build the
// program.

// cpuSample is one host-wide cpu reading: cumulative idle and total ticks.
type cpuSample struct {
	idle, total uint64
}

// cpuBusyDelta computes host-wide cpu utilization (percent * 1000) between
// two samples: 0 when the window is empty or not moving.
func cpuBusyDelta(prev, cur cpuSample) uint64 {
	if cur.total <= prev.total || cur.idle < prev.idle {
		return 0
	}
	dIdle := cur.idle - prev.idle
	dTotal := cur.total - prev.total
	if dTotal == 0 {
		return 0
	}
	busy := 100 * (1 - float64(dIdle)/float64(dTotal))
	return uint64(busy*1000 + 0.5)
}

// parseProcStat parses the aggregate "cpu" line of Linux /proc/stat
// (user nice system idle iowait irq softirq steal ...) into cumulative
// ticks. iowait counts as idle: a host waiting on disk is not busy.
func parseProcStat(b []byte) cpuSample {
	f := strings.Fields(strings.SplitN(string(b), "\n", 2)[0])
	if len(f) < 8 {
		return cpuSample{}
	}
	var total uint64
	for _, s := range f[1:] {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			total += v
		}
	}
	idle, _ := strconv.ParseUint(f[4], 10, 64) // idle
	ioWait, _ := strconv.ParseUint(f[5], 10, 64)
	return cpuSample{idle: idle + ioWait, total: total}
}

// parseMeminfo parses MemTotal/MemAvailable from Linux /proc/meminfo
// (values are kB) and returns bytes. used = total - available is the
// kernel's honest "in use" figure.
func parseMeminfo(b []byte) (total, used uint64) {
	var memTotal, memAvail uint64
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		kb, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			continue
		}
		switch f[0] {
		case "MemTotal:":
			memTotal = kb
		case "MemAvailable:":
			memAvail = kb
		}
	}
	return memTotal * 1024, (memTotal - memAvail) * 1024
}

// parseVmStat parses macOS `vm_stat` output: the header carries the page
// size, the body the per-state page counts. available is the sum of the
// states a healthy macOS hands back on demand — free, inactive,
// speculative, purgeable — the closest darwin analogue of Linux's
// MemAvailable; macOS parks most of memory in those states, so free alone
// would report a permanently full host. ok is false when the header or
// any counted line is missing, so a caller never reports a fabricated
// figure.
func parseVmStat(s string) (available, pageSize uint64, ok bool) {
	const marker = "page size of "
	i := strings.Index(s, marker)
	if i < 0 {
		return 0, 0, false
	}
	rest := s[i+len(marker):]
	j := strings.Index(rest, " bytes")
	if j < 0 {
		return 0, 0, false
	}
	if pageSize, _ = strconv.ParseUint(strings.TrimSpace(rest[:j]), 10, 64); pageSize == 0 {
		return 0, 0, false
	}
	counts := map[string]uint64{}
	for _, line := range strings.Split(s, "\n") {
		key, val, found := strings.Cut(line, ":")
		// "Pages free:   1234." — the trailing period is vm_stat's own
		// punctuation, not part of the number.
		n, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimSpace(val), "."), 10, 64)
		if !found || err != nil {
			continue
		}
		counts[strings.TrimSpace(key)] = n
	}
	var sum uint64
	for _, k := range []string{"Pages free", "Pages inactive", "Pages speculative", "Pages purgeable"} {
		n, found := counts[k]
		if !found {
			return 0, 0, false
		}
		sum += n
	}
	return sum, pageSize, true
}
