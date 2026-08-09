package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

// daemon is the node side of the fabric: it advertises itself, browses for
// peers, and serves jobs over one TCP port.
type daemon struct {
	reg   *registry
	state string
	name  string
}

func cmdDaemon(args []string) {
	fs := flag.NewFlagSet("daemon", flag.ExitOnError)
	port := fs.Int("port", DefaultPort, "TCP listen port")
	name := fs.String("name", sanitizeName(hostname()), "advertised instance name")
	state := fs.String("state", DefaultState, "state directory")
	fs.Parse(args)

	if err := os.MkdirAll(filepath.Join(*state, "jobs"), 0o755); err != nil {
		log.Fatalf("state dir: %v", err)
	}
	d := &daemon{reg: newRegistry(*state, *name), state: *state, name: *name}
	if err := d.reg.loadStatic(); err != nil {
		log.Printf("peers file: %v", err)
	}

	server, err := advertise(*name, *port)
	if err != nil {
		log.Printf("mDNS advertise failed (continuing without): %v", err)
	} else {
		defer server.Shutdown()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entries := make(chan *zeroconf.ServiceEntry, 32)
	go browse(ctx, entries)
	go func() {
		for e := range entries {
			d.reg.mergeMdns(e)
		}
	}()

	// Periodic maintenance: pick up hand-edited peers, drop lapsed mdns
	// peers, and keep the registry file fresh for `sandman nodes`.
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			d.reg.loadStatic()
			d.reg.prune()
			d.reg.writeSnapshot()
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

	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		go d.handleConn(c)
	}
}

func (d *daemon) handleConn(c net.Conn) {
	defer c.Close()
	c.SetDeadline(time.Now().Add(30 * time.Second)) // handshake window
	r := bufio.NewReader(c)
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
	j, err := startJob(jobSpec{ID: jobID, Image: image, Env: env, Workdir: workdir, Argv: argv})
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
