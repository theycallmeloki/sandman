package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestIsProvisioningError pins the classifier: a pull that FAILED is a
// provisioning failure (crashes the pipeline), while a first-run pull
// that SUCCEEDED — docker prints "Unable to find image" as an
// informational notice even on success — followed by a user-code error
// must NOT be classified as provisioning (the pipeline would wrongly
// stop scheduling on a fixable transform).
func TestIsProvisioningError(t *testing.T) {
	provisioning := []string{
		"docker: invalid reference format",
		"Unable to find image 'nope/nope:latest' locally\ndocker: pull access denied for nope/nope",
		"failed to resolve reference \"nope/nope:latest\": not found",
		"manifest unknown: manifest unknown",
		"No such image: nope:latest",
	}
	for _, s := range provisioning {
		if !isProvisioningError(s) {
			t.Errorf("isProvisioningError(%q) = false, want true (pull failed)", s)
		}
	}
	userCode := []string{
		// a slow first pull that succeeded, then the real user-code failure
		"Unable to find image 'alpine:latest' locally\nPulling from library/alpine\n" +
			"55afa1ecc21d: Pull complete\nStatus: Downloaded newer image for alpine:latest\n" +
			"cat: read error: Is a directory",
		"Pulling from library/alpine\nPull complete\nsh: boom: not found",
		"Status: Downloaded newer image for alpine:latest\nexit status 1",
		"plain user-code failure with no image chatter",
	}
	for _, s := range userCode {
		if isProvisioningError(s) {
			t.Errorf("isProvisioningError(%q) = true, want false (pull succeeded / user code)", s)
		}
	}
}

// TestProcessRunnerCapturesBothFDs runs a real job through the
// no-container backend with the capture wired exactly as
// production does — cmd.Stdout and cmd.Stderr are the same
// io.MultiWriter over the logCapture — and verifies every line of
// interleaved stdout/stderr lands exactly once. Under -race this locks
// in that the exec copy path and the capture's partial-line buffer
// never race (the capture serializes writes itself; the exec.Cmd
// single-goroutine funnel is caller wiring and not to be relied on).
func TestProcessRunnerCapturesBothFDs(t *testing.T) {
	dir := t.TempDir()
	lc, err := newLogCapture(filepath.Join(dir, "job.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer lc.Close()
	spec := JobSpec{
		Name: "capture-both-fds",
		Cmd: []string{"sh", "-c",
			`i=0; while [ $i -lt 1000 ]; do echo "o$i"; echo "e$i" >&2; i=$((i+1)); done`},
		Capture: lc,
		Workdir: dir,
	}
	res := processRunner{}.Run(spec)
	if res.Code != 0 {
		t.Fatalf("run code = %d, tail = %q", res.Code, res.Tail)
	}
	lc.Close()

	b, err := os.ReadFile(filepath.Join(dir, "job.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, raw := range bytes.Split(b, []byte{'\n'}) {
		if len(raw) == 0 {
			continue
		}
		var r logLineRec
		if json.Unmarshal(raw, &r) != nil {
			t.Fatalf("torn line: %q", raw)
		}
		seen[r.Line]++
	}
	if len(seen) != 2000 {
		t.Fatalf("got %d distinct lines, want 2000 (lost or duplicated)", len(seen))
	}
	for line, n := range seen {
		if n != 1 {
			t.Errorf("line %q written %d times, want exactly once", line, n)
		}
	}
}
