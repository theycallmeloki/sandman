package conformance

// Placement (SB-167/169): work can be placed on execution hosts selected
// by placement label — a host joins the cluster by registering with the
// control plane (configuration set at host setup time), and a pipeline
// requiring a label never names a host address. Unplaceable work surfaces
// as the pipeline's crashed state instead of hanging, and re-places
// automatically when a host bearing the label returns.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// workerProc is a started execution-host worker: the same binary in
// worker mode, the harness's stand-in for a remote execution host. It
// registers with the control plane at setup (join configuration) and
// executes the datums the control plane places on it.
type workerProc struct {
	cmd *exec.Cmd
}

// startWorker launches a worker that joins the harness daemon under the
// given host name bearing the given placement labels.
func startWorker(t *testing.T, name string, labels ...string) *workerProc {
	t.Helper()
	args := []string{"worker", "-name", name, "-control", fmt.Sprintf("http://127.0.0.1:%d", daemonPort), "-token", conformanceToken}
	for _, l := range labels {
		args = append(args, "-label", l)
	}
	cmd := exec.Command(binPath, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	return &workerProc{cmd: cmd}
}

// waitHostRegistered polls the control plane's host list until the named
// host's registration is visible (its first heartbeat succeeded).
func waitHostRegistered(t *testing.T, name string) {
	t.Helper()
	pollFor(t, "host "+name+" registered", 15*time.Second, func() bool {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api/v1/hosts", daemonPort), nil)
		if err != nil {
			return false
		}
		req.Header.Set("X-Sandbox-Token", conformanceToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var hs []struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&hs); err != nil {
			return false
		}
		for _, h := range hs {
			if h.Name == name {
				return true
			}
		}
		return false
	})
}

// TestSB167_PlacementLabels: a pipeline requiring a placement label runs
// its job on the registered host bearing that label — the definition
// names no host address, and the output provably came from the host's
// execution: only the worker sets the HOSTNAME environment, which the
// transform echoes into the output next to the copied input content.
func TestSB167_PlacementLabels(t *testing.T) {
	r := uniq(t)
	mustRepo(t, r)
	cm := commitFiles(t, r, "master", map[string]string{"file": "foo"})

	w := startWorker(t, "hostA", "gpu")
	waitHostRegistered(t, "hostA")
	defer func() { w.cmd.Process.Kill() }()

	name := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      name,
		Input:     &client.Input{Repo: r, Glob: "/*"},
		Placement: "gpu",
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", "cp /sandman/in/" + r + "/file /sandman/out/file && echo $HOSTNAME > /sandman/out/host"},
		},
	})

	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want exactly one", len(jobs))
	}
	if jobs[0].OutputCommit == "" {
		t.Fatalf("job %s produced no output commit", jobs[0].ID)
	}
	b, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if string(b) != "foo" {
		t.Fatalf("output file = %q, want the input content %q", string(b), "foo")
	}
	h, err := c.GetFile(jobs[0].OutputCommit, "host")
	if err != nil {
		t.Fatalf("read host marker: %v", err)
	}
	if got := strings.TrimSpace(string(h)); got != "hostA" {
		t.Fatalf("host marker = %q, want %q — the datum did not run on the registered host", got, "hostA")
	}
	js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(js) != 1 || js[0].State != "success" {
		t.Fatalf("want exactly one successful job, got %d (state %q)", len(js), js[0].State)
	}
}

// TestSB169_UnplaceableRecovery: a pipeline whose placement label no host
// bears must surface the outage as the crashed pipeline state instead of
// hanging; when a host bearing the label registers, the pending job
// re-places on its own and completes — exactly one output commit for the
// original input commit, no recreation or manual re-trigger.
func TestSB169_UnplaceableRecovery(t *testing.T) {
	r := uniq(t)
	mustRepo(t, r)
	name := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      name,
		Input:     &client.Input{Repo: r, Glob: "/*"},
		Placement: "offline",
		Transform: copyTransform(r),
	})
	cm := commitFiles(t, r, "master", map[string]string{"file": "foo"})

	// the job triggered by the commit cannot be placed: the pipeline's
	// inspected state must become the failed (crashed) state within a
	// bounded retry window — never a silent hang (SB-169 clause 1)
	pollFor(t, "pipeline crashed", 30*time.Second, func() bool {
		pi, err := c.InspectPipeline(name)
		return err == nil && pi.State == "crashed"
	})

	// a host bearing the label registers: the pending work re-places
	// automatically and the same job completes (SB-169 clause 2)
	w := startWorker(t, "hostB", "offline")
	waitHostRegistered(t, "hostB")
	defer func() { w.cmd.Process.Kill() }()
	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want exactly one", len(jobs))
	}
	b, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if string(b) != "foo" {
		t.Fatalf("output file = %q, want %q — the re-placed datum must produce the same result", string(b), "foo")
	}
	js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(js) != 1 || js[0].State != "success" {
		t.Fatalf("want exactly one successful job for the one input commit (no duplicates), got %d (state %q)", len(js), js[0].State)
	}
	// once placement became possible the pipeline is no longer crashed
	pi, err := c.InspectPipeline(name)
	if err != nil {
		t.Fatalf("inspect pipeline: %v", err)
	}
	if pi.State == "crashed" {
		t.Fatalf("pipeline still crashed after the host returned")
	}
}
