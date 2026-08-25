// Resource requests and limits declared on a
// pipeline are applied to the environment that executes its jobs (the OCI
// spec of the live execution participant); a pipeline with no declared
// resources runs with none injected; partial or empty specifications are
// accepted and the pipeline reaches the running state.
package conformance

import (
	"context"
	"testing"
	"time"
)

// hostConfig inspects a running job's container's OCI spec for the
// resource values the containerd backend applies, returning them in the
// order (memory limit, memory reservation, cpu nanos). Memory comes from
// the cgroup resource limit/reservation; the cpu value is the CFS
// quota/period pair converted to docker-style nanoseconds (0.5 cores →
// 500000000).
func hostConfig(t *testing.T, jobID string, timeout time.Duration) (int64, int64, int64) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if id := findContainerID(t, "sandman-"+jobID); id != "" {
			cli := rtClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, err := cli.LoadContainer(ctx, id)
			if err != nil {
				continue
			}
			spec, err := c.Spec(ctx)
			if err != nil || spec.Linux == nil || spec.Linux.Resources == nil {
				continue
			}
			var mem, resv, cpu int64
			if spec.Linux.Resources.Memory != nil {
				if spec.Linux.Resources.Memory.Limit != nil {
					mem = *spec.Linux.Resources.Memory.Limit
				}
				if spec.Linux.Resources.Memory.Reservation != nil {
					resv = *spec.Linux.Resources.Memory.Reservation
				}
			}
			if spec.Linux.Resources.CPU != nil && spec.Linux.Resources.CPU.Quota != nil && spec.Linux.Resources.CPU.Period != nil {
				cpu = int64(float64(*spec.Linux.Resources.CPU.Quota) / float64(*spec.Linux.Resources.CPU.Period) * 1e9)
			}
			return mem, resv, cpu
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("no container for job %s appeared within %s", jobID, timeout)
	return 0, 0, 0
}

const hundredM = 100 * 1024 * 1024 // the backend's M is MiB
const halfCPU = int64(500000000)   // 0.5 cores in nanos
