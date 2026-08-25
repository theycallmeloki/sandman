// The execution-backend seam:
// job execution goes through the Runner interface. The control plane
// describes the run (image, argv, env, mounts, resources, identity,
// stdin) and the runner executes it; the container backend and a future
// process backend are interchangeable behind it. RunResult classifies
// provisioning failures (the environment could not be produced — a bad
// image, an unavailable runtime — the command never started) separately
// from user-code failures (a non-zero exit). Signal delivery (timeout
// kills, cancel) is in the seam.
package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"

	"sandman/client"
)

// Runner executes one command per the control plane's description.
type Runner interface {
	// Run executes the spec and returns its outcome.
	Run(spec JobSpec) RunResult
	// Kill terminates a running execution by its stable name (the datum
	// timeout kill, job timeout, cancel). The error is the runtime's (a
	// kill of a not-yet-started execution can be lost; cancel retries).
	Kill(name string) error
}

// JobSpec is everything the control plane passes to the execution backend
// for one run of a command (the fields formerly hard-coded at the
// container-invocation site).
type JobSpec struct {
	// Image is the execution image; empty means the backend's default.
	Image string
	// NodeName is the host identity the run is labelled with.
	NodeName string
	// Name is the run's stable identity (the container name).
	Name string
	Cmd  []string
	// Stdin lines, fed on the run's stdin (empty = no stdin).
	Stdin []string
	// Env is the environment as KEY=VALUE entries.
	Env []string
	// Mounts is the backend's mount vocabulary ("-v" pairs for the
	// container backend; ignored by the process backend).
	Mounts []string
	// OutDir is where the run's /sandman/out maps.
	OutDir string
	// PathMap translates the execution-internal paths the spec's env and
	// workdir reference (/sandman/out, /sandman/in/<side>, /sandman/view/
	// <side>, /tmp) to host paths — the process backend runs the command
	// directly against the staging dirs, with no mounts.
	PathMap map[string]string
	// Capture receives the full combined output (the log store), when set.
	Capture io.Writer
	// Workdir is the working directory inside the execution; empty means
	// the backend default.
	Workdir string
	// User is the identity to run user code as; empty = root.
	User             string
	ResourceLimits   *client.ResourceLimits
	ResourceRequests *client.ResourceRequests
}

// RunResult is one execution's outcome.
type RunResult struct {
	// Code is the exit code (0 = success). User-code failures report a
	// non-zero code with ProvisioningErr nil.
	Code int
	// Tail is the last 2KB of combined output, for failure reporting.
	Tail string
	// ProvisioningErr reports an environment failure — the command never
	// started because the runtime could not produce the execution (bad
	// image, runtime unavailable). The control plane surfaces it as the
	// pipeline's crashed state, never as user-code failure.
	ProvisioningErr error
}

// isProvisioningError reports whether a failed run never started the
// execution — an environment problem, not a user-code failure.
func isProvisioningError(tail string) bool {
	// A first-run pull that succeeded is NOT a provisioning failure:
	// docker prints "Unable to find image" as an informational notice
	// even when the pull completes, so that marker alone would
	// misclassify a user-code failure that follows a slow first pull as
	// an environment problem — crashing the pipeline (which then stops
	// scheduling) on a perfectly fixable transform. The pull-success
	// markers say the image WAS obtainable; whatever failed after that
	// is not provisioning. (The containerd runner classifies pull/start
	// failures directly as ProvisioningErr; the marker list here covers
	// the tail-text classification used by the worker and remote paths.)
	for _, ok := range []string{"Pull complete", "Downloaded newer image", "Status: Downloaded"} {
		if strings.Contains(tail, ok) {
			return false
		}
	}
	for _, marker := range []string{
		"invalid reference format",
		"Unable to find image",
		"pull access denied",
		"No such image",
		"failed to resolve reference",
		"manifest unknown",
		"image not found",
		"repository does not exist or may require authorization",
		"containerd unavailable",
		"no such host",
	} {
		if strings.Contains(tail, marker) {
			return true
		}
	}
	return false
}

// containerRunner executes runs in throwaway containers (the production
// backend): a containerd container per run, with the pipeline's declared
// resources applied exactly to the environment that executes its jobs.
// The memory request becomes the cgroup's memory reservation (soft
// target) and the CPU request an allocation (CPU 0.5 requested as a 500ms
// CFS quota per 1s period); the memory limit becomes the hard cgroup
// limit and the CPU limit a CFS quota/period pair. The disk/ephemeral-
// storage request is accepted, recorded, and round-tripped by
// InspectPipeline, but not enforced — the container backend has no
// portable per-container writable-layer quota. When no resource limits
// are declared, no limits are injected at all: absence of declared limits
// never causes an implicit or default limit to be applied, even though
// the pipeline has a parallelism spec. The configured user identity and
// working directory are applied inside the environment — the user's code
// runs as that user (observable via whoami) with that working directory
// (observable via pwd), while inputs remain readable at the standard
// input mount regardless of the working directory. Provisioning failures
// (bad image, runtime unavailable) are classified directly from the
// containerd errors.
//
// The runtime itself is containerd + runc (see containerd_rt.go):
// sandman translates the JobSpec into a container/task and owns policy;
// containerd owns the container mechanics. No docker, no nerdctl.
//
// Networking deviation: containers run in the host network namespace
// (containerd alone provides no bridge/CNI, and service pipelines need
// host-reachable ports); the fabric assumes a trusted LAN.
type containerRunner struct{}

// Run executes one command in a throwaway container under an explicit
// name, with the spec's mounts and output directory, and returns the exit
// code plus a tail of combined stderr/stdout for failure reporting.
func (containerRunner) Run(spec JobSpec) RunResult {
	return rtRun(spec)
}

// Kill force-kills a running container by name (SIGKILL to its init
// process; containerd's shim reaps the rest).
func (containerRunner) Kill(name string) error {
	return rtKill(name)
}

// processRunner executes runs as local processes: the command runs
// against the staging directories directly,
// with the execution-internal paths translated via the spec's PathMap. No
// container runtime, no images, millisecond-scale. Documented policy
// differences from the container backend: resource requests/limits
// are not enforced (accept-and-record), the configured user
// identity is not applied (the process runs as the daemon's user), and
// provisioning never fails — there is no image to obtain, so the crashed
// state is unreachable here.
type processRunner struct{}

// procMu guards the running-process registry (name → command) so Kill can
// terminate a run in flight (datum timeout, job timeout, cancel).
var (
	procMu  sync.Mutex
	procs   = map[string]*exec.Cmd{}
	procSeq int
)

// Run executes the command as a local process: env and workdir translated
// through PathMap, output captured (log store + tail), exit code mapped
// 1:1. The command's working directory is created if missing.
func (processRunner) Run(spec JobSpec) RunResult {
	// env values and the workdir translate through the /sandman/
	// execution namespace only: an already-host value (the staging dirs
	// live under /tmp) must never be re-translated through the /tmp key
	translatePath := func(p string) string {
		for container, host := range spec.PathMap {
			if !strings.HasPrefix(container, "/sandman/") {
				continue
			}
			if p == container {
				return host
			}
			if strings.HasPrefix(p, container+"/") {
				return host + p[len(container):]
			}
		}
		return p
	}
	// the command text may reference the execution-internal paths
	// literally, embedded anywhere in the string (transforms hard-code
	// /sandman/view/<side>/... or /tmp/... inside a script): replace
	// every occurrence with its host staging directory. /tmp is replaced
	// FIRST — the /sandman/ translations emit host paths under the state
	// dir (a /tmp/... prefix), which a later /tmp pass would mangle; the
	// /tmp pass output, by contrast, never contains a /sandman/ key.
	translateText := func(s string) string {
		for container, host := range spec.PathMap {
			if container == "/tmp" {
				s = strings.ReplaceAll(s, container, host)
			}
		}
		for container, host := range spec.PathMap {
			if container == "/tmp" || !strings.HasPrefix(container, "/sandman/") {
				continue
			}
			s = strings.ReplaceAll(s, container, host)
		}
		return s
	}
	env := make([]string, 0, len(spec.Env)+1)
	for _, e := range spec.Env {
		if k, v, ok := strings.Cut(e, "="); ok && strings.HasPrefix(v, "/sandman/") {
			env = append(env, k+"="+translatePath(v))
			continue
		}
		env = append(env, e)
	}
	workdir := spec.Workdir
	if workdir == "" {
		workdir = "/sandman/out"
	}
	workdir = translatePath(workdir)
	os.MkdirAll(workdir, 0o755)

	cmdArgs := make([]string, len(spec.Cmd))
	for i, a := range spec.Cmd {
		cmdArgs[i] = translateText(a)
	}
	if os.Getenv("SANDMAN_DEBUG_RUNNER") != "" {
		fmt.Fprintf(os.Stderr, "DEBUG runner: name=%s env=%v cmd=%v dir=%s pathmap=%v\n", spec.Name, env, cmdArgs, workdir, spec.PathMap)
	}
	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Dir = workdir
	if len(spec.Stdin) > 0 {
		cmd.Stdin = strings.NewReader(strings.Join(spec.Stdin, "\n") + "\n")
	}
	var buf bytes.Buffer
	w := io.Writer(&buf)
	if spec.Capture != nil {
		w = io.MultiWriter(&buf, spec.Capture)
	}
	cmd.Stdout = w
	cmd.Stderr = w
	// the process runs in its own group so a kill terminates the whole
	// tree: sh -c forks children (sleep etc.) that inherit the output
	// pipes — killing only the shell would leave the pipes open and
	// cmd.Wait would block on their EOF forever
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		// the command itself could not start (missing interpreter):
		// a provisioning-class failure
		return RunResult{Code: 1, Tail: err.Error(), ProvisioningErr: err}
	}
	procMu.Lock()
	procSeq++
	procName := spec.Name
	if procName == "" {
		procName = fmt.Sprintf("proc-%d", procSeq)
	}
	procs[procName] = cmd
	procMu.Unlock()
	runErr := cmd.Wait()
	procMu.Lock()
	delete(procs, procName)
	procMu.Unlock()
	res := RunResult{}
	if runErr != nil {
		if ee, ok := runErr.(*exec.ExitError); ok {
			res.Code = ee.ExitCode()
		} else {
			res.Code = 1
		}
	}
	res.Tail = buf.String()
	if len(res.Tail) > 2000 {
		res.Tail = res.Tail[len(res.Tail)-2000:]
	}
	return res
}

// Kill terminates a running process by its stable name.
func (processRunner) Kill(name string) error {
	procMu.Lock()
	cmd, ok := procs[name]
	procMu.Unlock()
	if os.Getenv("SANDMAN_DEBUG_RUNNER") != "" {
		fmt.Fprintf(os.Stderr, "DEBUG kill: name=%s found=%v\n", name, ok)
	}
	if !ok {
		return fmt.Errorf("no process named %q", name)
	}
	// kill the whole process group (negative pid): the shell's children
	// must die too, or the inherited output pipes keep Wait blocked
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
