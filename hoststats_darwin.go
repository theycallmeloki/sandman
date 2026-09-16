//go:build darwin

package main

import (
	"os/exec"
	"strconv"
	"strings"
)

// Host sampling on macOS.
//
// Memory comes from the same sources Activity Monitor reads: hw.memsize
// for the total and vm_stat for the page states. The binaries are
// addressed by absolute path on purpose — /usr/sbin is not on the PATH
// launchd hands a LaunchAgent, and the host figures must not disappear
// because the service manager's environment is thin.

// readCpu has no macOS implementation, and this is deliberate: the
// kern.cp_time (and per-core kern.cp_times) sysctl that would carry the
// tick counters is gone on modern macOS — `sysctl kern.cp_time` answers
// "unknown oid" on Apple Silicon — and the Mach host_statistics route
// needs cgo, which the static release build (CGO_ENABLED=0) forbids. The
// alternatives left are two-sample tools (top -l 2, iostat) that block
// for at least a second per call; the daemon samples every 5s and the
// dashboard polls every 2s, so that cost would land on every frame.
//
// The zero sample is the same value a Linux host with an unreadable
// /proc/stat produces: the node reports its cpu count and memory, and
// cpuBusyDelta stays 0 rather than inventing a utilization figure.
func readCpu() cpuSample { return cpuSample{} }

// readMem reads total memory from the hw.memsize sysctl and the
// reclaimable pages from vm_stat. A probe that fails reports used as 0
// (unknown) rather than 100% (a full host), matching the Linux path's
// refusal to invent a figure.
func readMem() (total, used uint64) {
	out, err := exec.Command("/usr/sbin/sysctl", "-n", "hw.memsize").Output()
	if err != nil {
		return 0, 0
	}
	total, err = strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	if err != nil || total == 0 {
		return 0, 0
	}
	vm, err := exec.Command("/usr/bin/vm_stat").Output()
	if err != nil {
		return total, 0
	}
	pages, pageSize, ok := parseVmStat(string(vm))
	if !ok {
		return total, 0
	}
	avail := pages * pageSize
	if avail >= total {
		return total, 0
	}
	return total, total - avail
}
