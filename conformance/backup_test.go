package conformance

// TestBackupRestoreRoundTrip — the full backup/restore cycle end to
// end (previously only the store level was covered). Create data,
// stream the backup, stop the daemon, wipe the state dir, extract the
// archive into it, restart, and verify repos, pipelines, job records, and
// content all come back. Restore is the documented procedure: stop the
// daemon, extract into the state dir, start the daemon.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"sandman/client"
)

func TestBackupRestoreRoundTrip(t *testing.T) {
	state := filepath.Join(os.TempDir(), "sandman-backup-"+uniq(t))
	os.MkdirAll(state, 0o755)
	port := freePort()
	oldC, oldPort, oldState := c, daemonPort, daemonStateDir

	var cmd *exec.Cmd
	startDaemon := func() {
		cmd = exec.Command(binPath, "daemon", "-name", daemonName, "-port", strconv.Itoa(port), "-state", state, "-runner", "process")
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start daemon: %v", err)
		}
		if !waitPort(port, 15*time.Second) {
			t.Fatalf("daemon did not come up")
		}
		c = client.New(fmt.Sprintf("127.0.0.1:%d", port))
		daemonPort = port
		daemonStateDir = state
	}
	stopDaemon := func() {
		if cmd == nil || cmd.Process == nil {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		cmd = nil
	}
	t.Cleanup(func() {
		stopDaemon()
		os.RemoveAll(state)
		c, daemonPort, daemonStateDir = oldC, oldPort, oldState
	})
	startDaemon()

	// data: a pipeline with an input commit and a flushed output
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{Name: pipe, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x", "big": strings.Repeat("y", 200*1024)})
	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("setup: %d jobs, want 1", len(jobs))
	}
	oc := jobs[0].OutputCommit
	if b, err := c.GetFile(oc, "file"); err != nil || string(b) != "x" {
		t.Fatalf("pre-backup output = %q (%v), want x", string(b), err)
	}

	// stream the backup
	var buf bytes.Buffer
	if err := c.Backup(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("backup is empty")
	}

	// restore: stop the daemon, wipe the state dir, extract the archive
	stopDaemon()
	os.RemoveAll(state)
	os.MkdirAll(state, 0o755)
	gz, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("gzip open: %v", err)
	}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar read: %v", err)
		}
		path := filepath.Join(state, hdr.Name)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", hdr.Name, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", hdr.Name, err)
			}
			f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
			if err != nil {
				t.Fatalf("create %s: %v", hdr.Name, err)
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				t.Fatalf("extract %s: %v", hdr.Name, err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("close %s: %v", hdr.Name, err)
			}
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	// start the daemon on the restored state and verify the world came back
	startDaemon()
	repos := mustList(t, c.ListRepos)
	found := false
	for _, r := range repos {
		if r.Name == repo {
			found = true
		}
	}
	if !found {
		t.Fatalf("repo %s lost after restore (have %v)", repo, repos)
	}
	if _, err := c.InspectPipeline(pipe); err != nil {
		t.Fatalf("pipeline %s lost after restore: %v", pipe, err)
	}
	js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
	if err != nil || len(js) != 1 {
		t.Fatalf("jobs after restore: %d (err %v), want 1", len(js), err)
	}
	if js[0].OutputCommit != oc {
		t.Fatalf("job output commit after restore = %s, want %s", js[0].OutputCommit, oc)
	}
	if b, err := c.GetFile(cm.ID, "file"); err != nil || string(b) != "x" {
		t.Fatalf("input file after restore = %q (%v), want x", string(b), err)
	}
	if b, err := c.GetFile(oc, "file"); err != nil || string(b) != "x" {
		t.Fatalf("output file after restore = %q (%v), want x", string(b), err)
	}
	if b, err := c.GetFile(cm.ID, "big"); err != nil || len(b) != 200*1024 {
		t.Fatalf("large input after restore = %d bytes (err %v), want %d", len(b), err, 200*1024)
	}
}
