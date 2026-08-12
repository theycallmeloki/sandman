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
	"testing"
	"time"
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
	args := []string{"worker", "-name", name, "-control", fmt.Sprintf("http://127.0.0.1:%d", daemonPort)}
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
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
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

// TestSB169_UnplaceableRecovery: a pipeline whose placement label no host
// bears must surface the outage as the crashed pipeline state instead of
// hanging; when a host bearing the label registers, the pending job
// re-places on its own and completes — exactly one output commit for the
// original input commit, no recreation or manual re-trigger.
