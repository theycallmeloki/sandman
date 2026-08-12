package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grandcat/zeroconf"
)

// The stats layer is the programmatic interface to the fleet: `sandman stats`
// emits JSONL (one node object per line) that any tool can consume —
// `sandman stats | jq '.containers[].cpu'`. The dashboard is just a renderer
// over the same data (Rule of Separation: mechanism vs. presentation).

type containerInfo struct {
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	Image    string  `json:"image"`
	Status   string  `json:"status"`
	CPU      float64 `json:"cpu,omitempty"`      // percent of one core
	MemBytes uint64  `json:"memBytes,omitempty"` // current usage
	MemLimit uint64  `json:"memLimit,omitempty"`
	MemPerc  float64 `json:"memPerc,omitempty"`
	PIDs     int     `json:"pids,omitempty"`
}

type nodeStats struct {
	Node        string          `json:"node"`
	Addr        string          `json:"addr"`
	Docker      string          `json:"docker,omitempty"`
	Role        string          `json:"role,omitempty"` // "daemon" | "worker"
	Error       string          `json:"error,omitempty"`
	Containers  []containerInfo `json:"containers,omitempty"`
	HostCpus    int             `json:"hostCpus,omitempty"`
	HostMemT    uint64          `json:"hostMemTotal,omitempty"`
	HostMemU    uint64          `json:"hostMemUsed,omitempty"`
	HostMemPerc float64         `json:"hostMemPerc,omitempty"` // percent
	HostCpuPerc float64         `json:"hostCpuPerc,omitempty"` // percent, 5s sample
}

// fleetNode is one entry in the fleet view.
type fleetNode struct {
	Name, Addr, Docker, Source string
	Role                       string // "daemon" | "worker" ("" = unknown)
}

// fleet collects the fleet: registry/peers files, plus a live mDNS browse
// when browse is true (one-shot commands want freshness; the dashboard
// refreshes on a ticker and must not block 2.5s per frame on a browse).
func fleet(state string, browseNow bool) []fleetNode {
	seen := map[string]fleetNode{}
	for _, f := range []string{"registry", "peers"} {
		b, err := os.ReadFile(filepath.Join(state, f))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(b), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			docker, src := "-", "static"
			if len(fields) > 2 && fields[2] != "-" {
				docker = fields[2]
			}
			if len(fields) > 3 {
				src = fields[3]
			}
			role := ""
			if len(fields) > 5 {
				role = fields[5] // appended last: old files omit it
			}
			if _, ok := seen[fields[0]]; !ok || src == "mdns" {
				seen[fields[0]] = fleetNode{fields[0], fields[1], docker, src, role}
			}
		}
	}
	if browseNow {
		ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
		defer cancel()
		ch := make(chan *zeroconf.ServiceEntry, 32)
		go browse(ctx, ch)
		for e := range ch {
			addr := textValue(e.Text, "addr")
			if addr == "" {
				if ip := firstAddr(e); ip != "" {
					addr = net.JoinHostPort(ip, strconv.Itoa(e.Port))
				}
			}
			if addr == "" {
				continue
			}
			role := textValue(e.Text, "role")
			if role == "" {
				role = "daemon"
			}
			seen[e.Instance] = fleetNode{e.Instance, addr, textValue(e.Text, "docker"), "mdns", role}
		}
	}
	// A node's own registry excludes itself (a peer list is for peers), so
	// include the local daemon when it answers on the default port — the
	// dashboard and stats must show the node you're sitting on.
	if name, docker := probeLocalDaemon(DefaultPort); name != "" {
		seen[name] = fleetNode{name, net.JoinHostPort("127.0.0.1", strconv.Itoa(DefaultPort)), docker, "local", "daemon"}
	}
	out := make([]fleetNode, 0, len(seen))
	for _, n := range seen {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// probeLocalDaemon asks 127.0.0.1:port who it is; empty name = no local daemon.
func probeLocalDaemon(port int) (name, docker string) {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 500*time.Millisecond)
	if err != nil {
		return "", ""
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	if writeLine(w, "HELLO", ProtoVersion) != nil {
		return "", ""
	}
	ok, err := readLine(r)
	if err != nil || len(ok) == 0 || ok[0] != "OK" {
		return "", ""
	}
	for _, t := range ok {
		if v, found := strings.CutPrefix(t, "node="); found {
			name = v
		}
		if v, found := strings.CutPrefix(t, "docker="); found {
			docker = v
		}
	}
	return name, docker
}

// collectStats polls every node in the fleet in parallel.
func collectStats(state string, timeout time.Duration) []nodeStats {
	nodes := fleet(state, false)
	out := make([]nodeStats, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, n fleetNode) {
			defer wg.Done()
			out[i] = queryNodeStats(n, timeout)
		}(i, n)
	}
	wg.Wait()
	return out
}

func queryNodeStats(n fleetNode, timeout time.Duration) nodeStats {
	ns := nodeStats{Node: n.Name, Addr: n.Addr, Role: n.Role}
	conn, err := net.DialTimeout("tcp", n.Addr, timeout)
	if err != nil {
		ns.Error = err.Error()
		return ns
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	if err := writeLine(w, "HELLO", ProtoVersion); err != nil {
		ns.Error = err.Error()
		return ns
	}
	ok, err := readLine(r)
	if err != nil || len(ok) == 0 || ok[0] != "OK" {
		ns.Error = "handshake failed"
		return ns
	}
	for _, t := range ok {
		if d, found := strings.CutPrefix(t, "docker="); found {
			ns.Docker = d
		}
	}
	if err := writeLine(w, "STATS"); err != nil {
		ns.Error = err.Error()
		return ns
	}
	head, err := readLine(r)
	if err != nil || len(head) < 2 || head[0] != "STATS" {
		ns.Error = "node rejected stats"
		return ns
	}
	// header carries host facts: STATS <count> <hostJSON>
	if len(head) >= 3 {
		var host struct {
			Cpus     int     `json:"cpus"`
			MemTotal uint64  `json:"memTotal"`
			MemUsed  uint64  `json:"memUsed"`
			CPUBusy  float64 `json:"cpuBusy"`
		}
		if json.Unmarshal([]byte(head[2]), &host) == nil {
			ns.HostCpus = host.Cpus
			ns.HostMemT = host.MemTotal
			ns.HostMemU = host.MemUsed
			if host.MemTotal > 0 {
				ns.HostMemPerc = 100 * float64(host.MemUsed) / float64(host.MemTotal)
			}
			ns.HostCpuPerc = host.CPUBusy
		}
	}
	count, _ := strconv.Atoi(head[1])
	for i := 0; i < count; i++ {
		tok, err := readLine(r)
		if err != nil {
			ns.Error = "truncated stats reply"
			return ns
		}
		var c containerInfo
		if json.Unmarshal([]byte(strings.Join(tok, " ")), &c) == nil {
			ns.Containers = append(ns.Containers, c)
		}
	}
	return ns
}

// clientStats is the `stats` verb: JSONL to stdout, one node per line.
// The timeout covers the daemon's docker-stats sampling (~4s on a busy node).
func clientStats(state string) {
	for _, ns := range collectStats(state, 10*time.Second) {
		b, err := json.Marshal(ns)
		if err != nil {
			continue
		}
		fmt.Println(string(b))
	}
}

// parseMemUsage turns docker's "1.2MiB / 15.6GiB" into (used, limit) bytes.
func parseMemUsage(s string) (used, limit uint64) {
	f := strings.Fields(s)
	if len(f) >= 1 {
		used, _ = parseMemUnit(f[0])
	}
	if len(f) >= 3 {
		limit, _ = parseMemUnit(f[2])
	}
	return
}

func parseMemUnit(s string) (uint64, error) {
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.' || s[i] == ',') {
		i++
	}
	num, err := strconv.ParseFloat(strings.ReplaceAll(s[:i], ",", "."), 64)
	if err != nil {
		return 0, err
	}
	mult := map[string]float64{
		"B": 1, "kB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12,
		"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40,
	}[s[i:]]
	if mult == 0 {
		return 0, fmt.Errorf("unknown unit %q", s[i:])
	}
	return uint64(num * mult), nil
}
