// SB-067/068/069/070 — resource requests and limits declared on a
// pipeline are applied to the environment that executes its jobs (docker
// inspect of the live execution participant); a pipeline with no declared
// resources runs with none injected; partial or empty specifications are
// accepted and the pipeline reaches the running state.
package conformance

import (
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// hostConfig inspects a running job's container for the docker host
// config values (Memory, MemoryReservation, NanoCpus), returning them in
// that order.
func hostConfig(t *testing.T, jobID string, timeout time.Duration) (int64, int64, int64) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := exec.Command("docker", "ps", "-aq", "--filter", "name=sandman-"+jobID).Output()
		if err == nil {
			for _, id := range strings.Fields(string(out)) {
				cfg, err := exec.Command("docker", "inspect", "--format",
					"{{.HostConfig.Memory}} {{.HostConfig.MemoryReservation}} {{.HostConfig.NanoCpus}}", id).Output()
				if err == nil {
					f := strings.Fields(string(cfg))
					if len(f) == 3 {
						mem, _ := strconv.ParseInt(f[0], 10, 64)
						resv, _ := strconv.ParseInt(f[1], 10, 64)
						cpu, _ := strconv.ParseInt(f[2], 10, 64)
						return mem, resv, cpu
					}
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("no container for job %s appeared within %s", jobID, timeout)
	return 0, 0, 0
}

const hundredM = 100 * 1024 * 1024 // docker's M is MiB
const halfCPU = int64(500000000)   // 0.5 cores in nanos
