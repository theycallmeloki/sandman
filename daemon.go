package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/grandcat/zeroconf"
)

var (
	dockerVerOnce sync.Once
	dockerVer     string
)

func dockerVersion() string {
	dockerVerOnce.Do(func() {
		out, err := exec.Command("docker", "version", "--format", "{{.Server.Version}}").Output()
		if err != nil {
			dockerVer = "?"
		} else {
			dockerVer = strings.TrimSpace(string(out))
		}
	})
	return dockerVer
}

// pruneOrphans removes containers a previous instance of THIS daemon left
// behind (a daemon crash cannot run the kill-on-disconnect path). The
// sandman.node label scopes the prune so a daemon never touches jobs a
// sibling daemon owns on a shared dockerd.
func pruneOrphans(node string) {
	out, err := exec.Command("docker", "ps", "-a", "--filter", "label=sandman.node="+node, "--format", "{{.ID}}").Output()
	if err != nil {
		log.Printf("orphan scan: %v", err)
		return
	}
	for _, id := range strings.Fields(string(out)) {
		if err := exec.Command("docker", "rm", "-f", id).Run(); err != nil {
			log.Printf("orphan prune %s: %v", id, err)
			continue
		}
		log.Printf("pruned orphan job container %s", id)
	}
}

// daemon is the node side of the fabric: it advertises itself, browses for
// peers, serves jobs over one TCP port, and hosts the HTTP API on the same
// port (connections are routed by their first bytes).
type daemon struct {
	reg     *registry
	state   string
	name    string
	store   *apiStore
	syncIdx uint64
	cpuBusy atomic.Uint64 // host cpu busy percent * 1000, sampled each tick

	// authToken is the daemon's configured credential; when non-empty the
	// management endpoints that require authentication check the request
	// header against it (SB-154).
	authToken string

	// metrics accumulates the instrumented operations' runtime
	// observability (SB-132).
	metrics metricsStore

	// cronTickers are the live cron-input schedules, keyed by the cron
	// repository; the cancel functions stop the ticker goroutines
	// (SB-089, SB-133).
	cronMu      sync.Mutex
	cronTickers map[string]context.CancelFunc

	// liveJobs maps running job ids to their execution contexts for the
	// datum API (restart, SB-064).
	liveJobs sync.Map
}

// cpuSample is one /proc/stat reading for host-wide cpu utilization.
type cpuSample struct {
	idle, total uint64
}

func readCpu() cpuSample {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return cpuSample{}
	}
	line := strings.SplitN(string(b), "\n", 2)[0]
	f := strings.Fields(line)
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

// readMem reads host memory totals from /proc/meminfo (kB -> bytes).
// used = MemTotal - MemAvailable, the kernel's honest "in use" figure.
func readMem() (total, used uint64) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
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

func cmdDaemon(args []string) {
	fs := flag.NewFlagSet("daemon", flag.ExitOnError)
	port := fs.Int("port", DefaultPort, "TCP listen port")
	name := fs.String("name", sanitizeName(hostname()), "advertised instance name")
	state := fs.String("state", DefaultState, "state directory")
	authToken := fs.String("authToken", os.Getenv("SANDBOX_TOKEN"), "credential for the authenticated management endpoints (empty = auth disabled)")
	fs.Parse(args)

	if err := os.MkdirAll(filepath.Join(*state, "jobs"), 0o755); err != nil {
		log.Fatalf("state dir: %v", err)
	}
	d := &daemon{reg: newRegistry(*state, *name), state: *state, name: *name, store: newAPIStore(*state), authToken: *authToken}
	if err := d.reg.loadStatic(); err != nil {
		log.Printf("peers file: %v", err)
	}
	// the internal pipeline-specification repository (SB-127) exists for
	// the life of the daemon; a restart finds it already there
	d.store.createRepo("spec")
	pruneOrphans(*name)
	d.markStaleJobsFailed() // jobs running in a previous daemon died with it

	server, err := advertise(*name, *port)
	if err != nil {
		log.Printf("mDNS advertise failed (continuing without): %v", err)
	} else {
		defer server.Shutdown()
	}

	// Discovery maintenance, every 5s: a short one-shot browse with a fresh
	// resolver. Long-lived zeroconf resolvers on a shared 5353 (server +
	// resolver in one process, transient clients churning) go stale after a
	// while and silently stop receiving announcements; a fresh resolver per
	// tick is cheap and always works.
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		var last cpuSample
		haveLast := false
		for range t.C {
			// host-wide cpu utilization, delta since the previous tick
			cur := readCpu()
			if haveLast && cur.total > last.total && cur.idle >= last.idle {
				dIdle := cur.idle - last.idle
				dTotal := cur.total - last.total
				if dTotal > 0 {
					busy := 100 * (1 - float64(dIdle)/float64(dTotal))
					d.cpuBusy.Store(uint64(busy*1000 + 0.5))
				}
			}
			last, haveLast = cur, true

			ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
			ch := make(chan *zeroconf.ServiceEntry, 64)
			go browse(ctx, ch)
			for e := range ch {
				d.reg.mergeMdns(e)
			}
			cancel()
			d.reg.loadStatic()
			d.reg.prune()
			d.reg.writeSnapshot()
			d.syncOnce()
		}
	}()

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("listen :%d: %v", *port, err)
	}
	log.Printf("sandmand %q on :%d docker=%s", *name, *port, dockerVersion())

	// On SIGTERM/SIGINT, announce the mDNS goodbye so peers drop us
	// immediately instead of waiting out the TTL.
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		if server != nil {
			server.Shutdown()
		}
		os.Exit(0)
	}()

	// The HTTP API shares this port with the text protocol. HTTP conns are
	// routed to http.Server (keep-alive friendly) via a channel listener;
	// the text protocol keeps its own per-connection handler.
	apiConns := make(chan net.Conn)
	apiSrv := &http.Server{
		Handler:           d.apiHandler(),
		ReadHeaderTimeout: 15 * time.Second,
	}
	go apiSrv.Serve(&chanListener{ch: apiConns})

	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		go d.serveConn(c, apiConns)
	}
}

// syncOnce pulls one known peer's registry over the wire and merges it,
// round-robin across peers per tick. mDNS bootstrap + TCP gossip: any peer
// learned by any daemon propagates fleet-wide within a few ticks, even when
// the kernel's shared-5353 multicast hashing starves a specific peer pair.
func (d *daemon) syncOnce() {
	peers := d.reg.list()
	if len(peers) == 0 {
		return
	}
	idx := int(atomic.AddUint64(&d.syncIdx, 1)-1) % len(peers)
	p := peers[idx]
	if p.Source == "local" {
		return // ourselves; next tick advances the index
	}
	conn, err := net.DialTimeout("tcp", p.Addr, 2*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	if writeLine(w, "HELLO", ProtoVersion) != nil {
		return
	}
	if ok, err := readLine(r); err != nil || len(ok) == 0 || ok[0] != "OK" {
		return
	}
	if writeLine(w, "NODES") != nil {
		return
	}
	head, err := readLine(r)
	if err != nil || len(head) != 2 || head[0] != "NODES" {
		return
	}
	n, _ := strconv.Atoi(head[1])
	for i := 0; i < n; i++ {
		tok, err := readLine(r)
		if err != nil {
			return
		}
		if len(tok) < 3 || tok[0] != "NODE" {
			continue
		}
		docker := "-"
		if len(tok) > 3 {
			docker = tok[3]
		}
		d.reg.mergeSync(tok[1], tok[2], docker)
	}
}

func (d *daemon) handleConn(c net.Conn, r *bufio.Reader) {
	defer c.Close()
	c.SetDeadline(time.Now().Add(30 * time.Second)) // handshake window
	w := bufio.NewWriter(c)

	tok, err := readLine(r)
	if err != nil || len(tok) == 0 || tok[0] != "HELLO" {
		return
	}
	if err := writeLine(w, "OK", "node="+d.name, "docker="+dockerVersion()); err != nil {
		return
	}

	tok, err = readLine(r)
	if err != nil {
		return
	}
	switch tok[0] {
	case "NODES":
		c.SetDeadline(time.Now().Add(10 * time.Second))
		d.handleNodes(w)
	case "STATS":
		c.SetDeadline(time.Now().Add(15 * time.Second))
		d.handleStats(w)
	case "RUN":
		c.SetDeadline(time.Time{}) // no idle timeout mid-job, like ssh
		d.handleRun(r, w)
	}
}

func (d *daemon) handleNodes(w *bufio.Writer) {
	d.reg.loadStatic()
	d.reg.prune()
	peers := d.reg.list()
	if err := writeLine(w, "NODES", strconv.Itoa(len(peers))); err != nil {
		return
	}
	for _, p := range peers {
		if err := writeLine(w, "NODE", p.Name, p.Addr, p.Docker, p.Source); err != nil {
			return
		}
	}
}

// handleStats answers a STATS request with the node's running containers and
// their live resource usage: "STATS <n>" then n JSON lines, one per
// container. The docker CLI supplies the numbers; this relays them as a
// stable schema the fleet can consume (Rule of Representation).
func (d *daemon) handleStats(w *bufio.Writer) {
	// docker stats samples all running containers and takes a few seconds
	// on a busy node — give each call its own generous deadline.
	psCtx, psCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer psCancel()
	statsCtx, statsCancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer statsCancel()

	type psLine struct {
		ID, Names, Image, State, Status string
	}
	var conts []psLine
	// --no-trunc: docker ps shortens IDs, but docker stats reports full IDs —
	// the join must match on the full 64-char id.
	if out, err := exec.CommandContext(psCtx, "docker", "ps", "--no-trunc", "--format", "{{json .}}").Output(); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if line == "" {
				continue
			}
			var c psLine
			if json.Unmarshal([]byte(line), &c) == nil {
				conts = append(conts, c)
			}
		}
	}

	type statsLine struct {
		Container, CPUPerc, MemUsage, MemPerc, PIDs string
	}
	byID := map[string]containerInfo{}
	if out, err := exec.CommandContext(statsCtx, "docker", "stats", "--no-stream", "--format", "{{json .}}").Output(); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if line == "" {
				continue
			}
			var s statsLine
			if json.Unmarshal([]byte(line), &s) != nil {
				continue
			}
			var c containerInfo
			c.CPU, _ = strconv.ParseFloat(strings.TrimSuffix(s.CPUPerc, "%"), 64)
			c.MemBytes, c.MemLimit = parseMemUsage(s.MemUsage)
			c.MemPerc, _ = strconv.ParseFloat(strings.TrimSuffix(s.MemPerc, "%"), 64)
			c.PIDs, _ = strconv.Atoi(s.PIDs)
			byID[s.Container] = c
		}
	}

	// Host-level facts ride in the STATS header: cpu count, real memory
	// totals, and host-wide cpu utilization (sampled over the 5s tick).
	type hostStats struct {
		Cpus     int     `json:"cpus"`
		MemTotal uint64  `json:"memTotal"`
		MemUsed  uint64  `json:"memUsed"`
		CPUBusy  float64 `json:"cpuBusy"`
	}
	memTotal, memUsed := readMem()
	host, _ := json.Marshal(hostStats{
		Cpus:     runtime.NumCPU(),
		MemTotal: memTotal,
		MemUsed:  memUsed,
		CPUBusy:  float64(d.cpuBusy.Load()) / 1000,
	})
	if err := writeLine(w, "STATS", strconv.Itoa(len(conts)), string(host)); err != nil {
		return
	}
	for _, c := range conts {
		info := byID[c.ID]
		info.ID = c.ID
		info.Name = strings.TrimPrefix(c.Names, "/")
		info.Image = c.Image
		info.Status = c.Status
		line, err := json.Marshal(info)
		if err != nil {
			continue
		}
		if err := writeLine(w, string(line)); err != nil {
			return
		}
	}
}

// handleRun executes one job on this connection. The connection stays bound
// to the job until EXIT — streams flow live, signals travel the same pipe.
func (d *daemon) handleRun(r *bufio.Reader, w *bufio.Writer) {
	idTok, err := readLine(r)
	if err != nil {
		return
	}
	imgTok, err := readLine(r)
	if err != nil {
		return
	}
	jobID := strings.Join(idTok, "")
	image := strings.Join(imgTok, "")
	if jobID == "" || image == "" {
		writeLine(w, "ERR", "missing jobid or image")
		return
	}

	// header: ENV lines, then CMD, then argv lines until a blank line
	var env []string
	for {
		tok, err := readLine(r)
		if err != nil {
			return
		}
		if len(tok) == 0 {
			return // blank line before CMD is a protocol error
		}
		if tok[0] == "ENV" {
			env = append(env, strings.Join(tok[1:], " "))
			continue
		}
		if tok[0] == "CMD" {
			break
		}
		return // unknown header token
	}
	var argv []string
	for {
		tok, err := readLine(r)
		if err != nil {
			return
		}
		if len(tok) == 0 {
			break
		}
		argv = append(argv, strings.Join(tok, " "))
	}

	workdir := filepath.Join(d.state, "jobs", jobID)
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		writeLine(w, "ERR", "workdir: "+err.Error())
		return
	}
	j, err := startJob(jobSpec{ID: jobID, Image: image, Env: env, Workdir: workdir, Argv: argv, Node: d.name})
	if err != nil {
		writeLine(w, "ERR", err.Error())
		return
	}
	if err := writeLine(w, "RUNNING", jobID); err != nil {
		j.Kill()
		return
	}

	var wmu sync.Mutex // serialize writers to w (relay goroutines + EXIT)
	relay := func(fd string, src io.Reader) {
		buf := make([]byte, 32*1024)
		for {
			n, rerr := src.Read(buf)
			if n > 0 {
				wmu.Lock()
				writeOutFrame(w, fd, buf[:n])
				wmu.Unlock()
			}
			if rerr != nil {
				return
			}
		}
	}

	// stdin pump: frames in, container stdin out. A dead connection means a
	// dead client — kill the job, leave no orphans (Rule of Repair).
	go func() {
		for {
			tok, err := readLine(r)
			if err != nil {
				j.Kill()
				j.Stdin.Close()
				return
			}
			if len(tok) == 0 {
				continue
			}
			switch tok[0] {
			case "DATA":
				n, err := strconv.Atoi(tok[1])
				if err != nil || n < 0 {
					j.Kill()
					return
				}
				payload := make([]byte, n)
				if _, err := io.ReadFull(r, payload); err != nil {
					j.Kill()
					return
				}
				j.Stdin.Write(payload)
			case "EOF":
				j.Stdin.Close()
				return
			case "SIGNAL":
				if len(tok) > 1 {
					j.Signal(tok[1])
				}
			}
		}
	}()

	var rg sync.WaitGroup
	rg.Add(2)
	go func() {
		defer rg.Done()
		relay("0", j.Stdout)
	}()
	go func() {
		defer rg.Done()
		relay("1", j.Stderr)
	}()

	code := <-j.done
	rg.Wait() // flush remaining output before declaring the exit code
	wmu.Lock()
	writeLine(w, "EXIT", strconv.Itoa(code))
	wmu.Unlock()
}
