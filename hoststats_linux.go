//go:build linux

package main

import "os"

// Host sampling on Linux: the kernel's own accounting under /proc.

// readCpu samples host-wide cpu ticks from /proc/stat.
func readCpu() cpuSample {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return cpuSample{}
	}
	return parseProcStat(b)
}

// readMem reads host memory totals from /proc/meminfo (kB -> bytes).
// used = MemTotal - MemAvailable, the kernel's honest "in use" figure.
// A host that cannot be read reports (0, 0), never a fabricated figure.
func readMem() (total, used uint64) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	return parseMeminfo(b)
}
