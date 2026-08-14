package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"sandman/internal/cli"
)

// die is the CLI's fatal-error exit: print the message to stderr and
// exit with the given code. (internal/cli has its own copy for the
// data-plane verbs.)
func die(msg string, code int) {
	fmt.Fprintln(os.Stderr, "sandman:", msg)
	os.Exit(code)
}

// resolveNode turns a node name into host:port. Resolution order: literal
// addr, local registry/peers files, a quick mDNS browse, then hostname:4242.
func resolveNode(node, state string) (string, error) {
	if strings.Contains(node, ":") {
		return node, nil
	}
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
			if len(fields) >= 2 && fields[0] == node {
				return fields[1], nil
			}
		}
	}
	if addr := mdnsLookup(node, 2500*time.Millisecond); addr != "" {
		return addr, nil
	}
	return net.JoinHostPort(node, strconv.Itoa(DefaultPort)), nil
}

// clientRun streams a job from the local process table to a remote node:
// stdin in, stdout/stderr out, signals across, the container's exit code
// back. `sandman run` should feel like ssh: output appears as it happens.
func clientRun(node, state, image string, env, argv []string) {
	addr, err := resolveNode(node, state)
	if err != nil {
		die(err.Error(), 1)
	}
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		die(fmt.Sprintf("%s: %v", node, err), 1)
	}
	// every terminal path below exits the process (die/os.Exit), so a
	// defer would never run; the EXIT path closes the connection itself
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	if err := writeLine(w, "HELLO", ProtoVersion); err != nil {
		die("connect: "+err.Error(), 1)
	}
	ok, err := readLine(r)
	if err != nil || len(ok) == 0 || ok[0] != "OK" {
		die("handshake failed", 1)
	}

	jobID := newID(sanitizeName(hostname()), "")
	if err := writeLine(w, "RUN"); err != nil {
		die("send: "+err.Error(), 1)
	}
	_ = writeLine(w, jobID)
	_ = writeLine(w, image)
	for _, e := range env {
		_ = writeLine(w, "ENV", e)
	}
	_ = writeLine(w, "CMD")
	for _, a := range argv {
		_ = writeLine(w, a)
	}
	_ = writeLine(w, "")

	run, err := readLine(r)
	if err != nil || len(run) == 0 || run[0] != "RUNNING" {
		if len(run) > 1 && run[0] == "ERR" {
			die("node rejected job: "+strings.Join(run[1:], " "), 1)
		}
		die("node rejected job", 1)
	}

	// Forward local signals as remote docker kill signals. The second
	// Ctrl-C escalates to SIGKILL, like any self-respecting terminal.
	signaled := map[string]bool{}
	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for s := range sigCh {
			name := "KILL"
			switch s {
			case syscall.SIGINT:
				name = "INT"
			case syscall.SIGTERM:
				name = "TERM"
			case syscall.SIGQUIT:
				name = "QUIT"
			}
			if signaled[name] {
				name = "KILL"
			}
			signaled[name] = true
			_ = writeLine(w, "SIGNAL", name)
		}
	}()

	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := os.Stdin.Read(buf)
			if n > 0 {
				_ = writeFrame(w, "DATA", buf[:n])
			}
			if err != nil {
				_ = writeLine(w, "EOF")
				return
			}
		}
	}()

	for {
		tag, fd, payload, err := readEvent(r)
		if err != nil {
			die(fmt.Sprintf("%s: connection lost", node), 1)
		}
		switch tag {
		case "OUT":
			if fd == "1" {
				os.Stderr.Write([]byte(payload))
			} else {
				os.Stdout.Write([]byte(payload))
			}
		case "EXIT":
			code, _ := strconv.Atoi(payload)
			conn.Close()
			os.Exit(code)
		case "ERR":
			die(payload, 1)
		}
	}
}

// clientNodes prints the fleet: registry/peers files merged with a live mDNS
// browse, so it works on any machine, node or not.
func clientNodes(state string) {
	nodes := fleet(state, true)
	rows := make([][]string, 0, len(nodes))
	for _, n := range nodes {
		role := n.Role
		if role == "" {
			role = "-"
		}
		ver := n.Version
		if ver == "" {
			ver = "-"
		}
		rows = append(rows, []string{n.Name, n.Addr, n.Docker, role, ver, n.Source})
	}
	cli.RenderTable([]string{"NAME", "ADDR", "DOCKER", "ROLE", "VERSION", "SOURCE"}, rows)
	if len(rows) == 0 {
		fmt.Println("no nodes")
	}
}
