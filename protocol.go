package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	ProtoVersion = "sandman/0.1"
	ServiceType  = "_sandman._tcp"
	DefaultPort  = 4242
	DefaultState = "/var/lib/sandman"
)

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
