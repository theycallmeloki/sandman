// SB-067/068/069/070 — resource requests and limits declared on a
// pipeline are applied to the environment that executes its jobs (docker
// inspect of the live execution participant); a pipeline with no declared
// resources runs with none injected; partial or empty specifications are
// accepted and the pipeline reaches the running state.
package conformance

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"sandman/client"
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

func TestSB067_ResourceRequestsApplied(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	name := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: name,
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", "sleep 15"},
			ResourceRequests: &client.ResourceRequests{
				Memory: "100M",
				CPU:    0.5,
				Disk:   "10M",
			},
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	})
	job := waitJobFor(t, name, 30*time.Second)
	_, resv, cpu := hostConfig(t, job.ID, 45*time.Second)
	if resv != hundredM {
		t.Fatalf("memory reservation = %d, want %d (100M)", resv, hundredM)
	}
	if cpu != halfCPU {
		t.Fatalf("cpu = %d, want %d (0.5)", cpu, halfCPU)
	}
	flushSetOK(t, []string{cm.ID})
}

func TestSB068_ResourceLimitsApplied(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	name := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: name,
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", "sleep 15"},
			ResourceLimits: &client.ResourceLimits{
				Memory: "100M",
				CPU:    0.5,
			},
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	})
	job := waitJobFor(t, name, 30*time.Second)
	mem, _, cpu := hostConfig(t, job.ID, 45*time.Second)
	if mem != hundredM {
		t.Fatalf("memory limit = %d, want %d (100M)", mem, hundredM)
	}
	if cpu != halfCPU {
		t.Fatalf("cpu limit = %d, want %d (0.5)", cpu, halfCPU)
	}
	flushSetOK(t, []string{cm.ID})
}

func TestSB069_NoLimitsInjected(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	name := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      name,
		Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", "sleep 15"}},
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	})
	job := waitJobFor(t, name, 30*time.Second)
	mem, resv, cpu := hostConfig(t, job.ID, 45*time.Second)
	if mem != 0 || resv != 0 || cpu != 0 {
		t.Fatalf("no limits declared, but participant has memory=%d reservation=%d cpu=%d, want all zero", mem, resv, cpu)
	}
	flushSetOK(t, []string{cm.ID})
}

func TestSB070_PartialResourceSpecsAccepted(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	specs := []*client.Transform{
		{Image: "alpine", Cmd: []string{"sh", "-c", "true"},
			ResourceRequests: &client.ResourceRequests{Memory: "100M", CPU: 0.5}},
		{Image: "alpine", Cmd: []string{"sh", "-c", "true"},
			ResourceRequests: &client.ResourceRequests{Memory: "100M"}},
		{Image: "alpine", Cmd: []string{"sh", "-c", "true"}},
	}
	for i, tr := range specs {
		name := fmt.Sprintf("%s-%d", uniq(t), i)
		mustPipeline(t, client.Pipeline{
			Name:      name,
			Transform: tr,
			Input:     &client.Input{Repo: repo, Glob: "/*"},
		})
		waitJobFor(t, name, 30*time.Second)
	}
	flushSetOK(t, []string{cm.ID})
}
