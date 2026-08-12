package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	ProtoVersion = "sandman/0.1"
	ServiceType  = "_sandman._tcp"
	DefaultPort  = 4242
	DefaultState = "/var/lib/sandman"
)

// textBackend is the protocol-specific half of the fabric text protocol:
// what a node answers to NODES and STATS, and — daemons only — RUN. The
// daemon and the worker share one connection handler; the backend carries
// the divergence (a worker's NODES is empty, it has no peer registry, and
// it cannot RUN).
type textBackend struct {
	nodeName    string
	handleNodes func(w *bufio.Writer)
	handleStats func(w *bufio.Writer)
	handleRun   func(r *bufio.Reader, w *bufio.Writer) // nil = not supported
}

// serveTextProtocol serves the fabric text protocol on one connection:
// HELLO/OK handshake, then NODES/STATS/RUN dispatched to the backend.
// The daemon and the worker share it — both answer the fleet's
// nodes/stats/dashboard views on the same port as their HTTP API.
func (be textBackend) serve(c net.Conn, r *bufio.Reader) {
	defer c.Close()
	c.SetDeadline(time.Now().Add(30 * time.Second)) // handshake window
	w := bufio.NewWriter(c)

	tok, err := readLine(r)
	if err != nil || len(tok) == 0 || tok[0] != "HELLO" {
		return
	}
	if err := writeLine(w, "OK", "node="+be.nodeName, "docker="+dockerVersion()); err != nil {
		return
	}

	tok, err = readLine(r)
	if err != nil {
		return
	}
	switch tok[0] {
	case "NODES":
		c.SetDeadline(time.Now().Add(10 * time.Second))
		be.handleNodes(w)
	case "STATS":
		c.SetDeadline(time.Now().Add(15 * time.Second))
		be.handleStats(w)
	case "RUN":
		if be.handleRun == nil {
			return
		}
		c.SetDeadline(time.Time{}) // no idle timeout mid-job, like ssh
		be.handleRun(r, w)
	}
}

// readLine reads one line and splits it into space-separated tokens.
// A blank line yields an empty (len 0) token slice.
func readLine(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return []string{}, nil
	}
	return strings.Split(line, " "), nil
}

// writeLine writes "tok tok ...\n" and flushes.
func writeLine(w *bufio.Writer, parts ...string) error {
	if _, err := w.WriteString(strings.Join(parts, " ")); err != nil {
		return err
	}
	if err := w.WriteByte('\n'); err != nil {
		return err
	}
	return w.Flush()
}

// writeFrame writes "TAG <n>\n" followed by n raw bytes (client->daemon DATA).
func writeFrame(w *bufio.Writer, tag string, p []byte) error {
	if err := writeLine(w, tag, strconv.Itoa(len(p))); err != nil {
		return err
	}
	if _, err := w.Write(p); err != nil {
		return err
	}
	return w.Flush()
}

// writeOutFrame writes "OUT <fd> <n>\n" followed by n raw bytes (daemon->client).
func writeOutFrame(w *bufio.Writer, fd string, p []byte) error {
	if err := writeLine(w, "OUT", fd, strconv.Itoa(len(p))); err != nil {
		return err
	}
	if _, err := w.Write(p); err != nil {
		return err
	}
	return w.Flush()
}

// readEvent reads one event from the daemon: either a control line
// (RUNNING/EXIT/ERR/NODES/...) or an OUT frame with its payload.
func readEvent(r *bufio.Reader) (tag, fd, payload string, err error) {
	tok, err := readLine(r)
	if err != nil {
		return "", "", "", err
	}
	if len(tok) == 0 {
		return "", "", "", fmt.Errorf("empty event line")
	}
	if tok[0] == "OUT" {
		if len(tok) < 3 {
			return "", "", "", fmt.Errorf("short OUT frame")
		}
		n, err := strconv.Atoi(tok[2])
		if err != nil {
			return "", "", "", err
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", "", "", err
		}
		return "OUT", tok[1], string(buf), nil
	}
	return tok[0], "", strings.Join(tok[1:], " "), nil
}
