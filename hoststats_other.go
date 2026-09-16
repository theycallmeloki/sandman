//go:build !linux && !darwin

package main

// Host sampling has an implementation only on Linux and macOS. Elsewhere
// (the code still builds for other targets) the host reports zero rather
// than a guess, exactly as a Linux host with an unreadable /proc does.

func readCpu() cpuSample { return cpuSample{} }

func readMem() (total, used uint64) { return 0, 0 }
