// The execution-backend seam (TESTING_ARCHITECTURE.md, D-23 R-1/R-2):
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
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

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
	// NodeName is the host identity the run is labelled with (SB-167).
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
	// User is the identity to run user code as (SB-128); empty = root.
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
	// pipeline's crashed state (SB-043, SB-091), never as user-code
	// failure.
	ProvisioningErr error
}

// isProvisioningError reports whether a failed run never started the
// execution — an environment problem, not a user-code failure.
func isProvisioningError(tail string) bool {
	for _, marker := range []string{
		"invalid reference format",
		"Unable to find image",
		"pull access denied",
		"No such image",
		"failed to resolve reference",
		"manifest unknown",
		"image not found",
	} {
		if strings.Contains(tail, marker) {
			return true
		}
	}
	return false
}

// containerRunner executes runs in throwaway containers (the production
// backend): `docker run --rm` per run, resources applied to the
// environment (SB-067..070), user identity and working directory inside
// the environment (SB-128), provisioning failures classified from the
// runtime's error output.
type containerRunner struct{}

// Run executes one command in a throwaway container under an explicit
// name, with the spec's mounts and output directory, and returns the exit
// code plus a tail of combined stderr/stdout for failure reporting.
func (containerRunner) Run(spec JobSpec) RunResult {
	image := spec.Image
	if image == "" {
		image = "alpine"
	}
	args := []string{"run", "--rm", "--name", spec.Name,
		"--label", "sandman.node=" + spec.NodeName,
		"-v", spec.OutDir + ":/sandman/out",
	}
	// resource requests and limits are applied to the execution
	// environment (SB-067/068/069/070). Sandbox deviation: docker
	// expresses a CPU request only as an allocation, so a CPU request
	// without a limit sets the container's CPU allocation; an
	// ephemeral-storage (disk) request is recorded but not enforceable
	// on docker's default driver.
	if spec.ResourceLimits != nil {
		if spec.ResourceLimits.Memory != "" {
			args = append(args, "--memory", spec.ResourceLimits.Memory)
		}
		if spec.ResourceLimits.CPU > 0 {
			args = append(args, "--cpus", fmt.Sprintf("%g", spec.ResourceLimits.CPU))
		}
	}
	if spec.ResourceRequests != nil {
		if spec.ResourceRequests.Memory != "" {
			args = append(args, "--memory-reservation", spec.ResourceRequests.Memory)
		}
		if spec.ResourceRequests.CPU > 0 && (spec.ResourceLimits == nil || spec.ResourceLimits.CPU == 0) {
			args = append(args, "--cpus", fmt.Sprintf("%g", spec.ResourceRequests.CPU))
		}
	}
	args = append(args, spec.Mounts...)
	for _, e := range spec.Env {
		args = append(args, "-e", e)
	}
	if len(spec.Stdin) > 0 {
		args = append(args, "-i")
	}
	workdir := spec.Workdir
	if workdir == "" {
		workdir = "/sandman/out"
	}
	args = append(args, "-w", workdir)
	if spec.User != "" {
		// Run user code as the configured identity: create the user and
		// working directory in-container, then su to it. Needs a
		// busybox-style image (alpine): adduser + su are provided.
		inner := "cd " + shQuote(workdir) + " && exec " + joinSh(spec.Cmd)
		script := fmt.Sprintf("adduser -D %s 2>/dev/null; mkdir -p %s; chown -R %s %s 2>/dev/null; chown %s /sandman/out 2>/dev/null; su %s -c %s",
			shQuote(spec.User), shQuote(workdir), shQuote(spec.User), shQuote(workdir), shQuote(spec.User), shQuote(spec.User), shQuote(inner))
		spec.Cmd = []string{"sh", "-c", script}
	}

	cmd := exec.Command("docker", append(append(args, image), spec.Cmd...)...)
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
	res := RunResult{}
	if runErr := cmd.Run(); runErr != nil {
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
	if res.Code != 0 && isProvisioningError(res.Tail) {
		res.ProvisioningErr = fmt.Errorf("%s", strings.TrimSpace(res.Tail))
	}
	return res
}

// Kill force-kills a running container by name.
func (containerRunner) Kill(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", "kill", name).Run()
}

// processRunner executes runs as local processes (TESTING_ARCHITECTURE.md
// R-2/R-3): the command runs against the staging directories directly,
// with the execution-internal paths translated via the spec's PathMap. No
// container runtime, no images, millisecond-scale. Documented policy
// differences from the container backend (R-4): resource requests/limits
// are not enforced (accept-and-record, D-15/SB-070), the configured user
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
	if os.Getenv("SANDBOX_DEBUG_RUNNER") != "" {
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
	if os.Getenv("SANDBOX_DEBUG_RUNNER") != "" {
		fmt.Fprintf(os.Stderr, "DEBUG kill: name=%s found=%v\n", name, ok)
	}
	if !ok {
		return fmt.Errorf("no process named %q", name)
	}
	// kill the whole process group (negative pid): the shell's children
	// must die too, or the inherited output pipes keep Wait blocked
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
