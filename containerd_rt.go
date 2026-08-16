package main

// containerd_rt.go — the container runtime adapter. Sandman's container
// backend speaks containerd's Go API directly: no docker, no nerdctl, no
// shelling out to ctr. The control plane's Runner seam stays intact; this
// file translates a JobSpec into a containerd container/task and back into
// a RunResult.
//
// Runtime model: sandman owns policy, containerd owns mechanics. Every
// sandman container lives in the "sandman" containerd namespace and is
// labelled with its owning node (orphan pruning). Images are pulled into
// containerd's content store on first use. The OCI spec carries the job's
// env, argv (entrypoint-preserving, like docker), working directory,
// mounts, user identity, and resource declarations; the kernel — via runc
// and the cgroup v2 filesystem — enforces them. Sandman never writes to
// /sys/fs/cgroup itself.
//
// Networking: sandman containers run in the host network namespace.
// containerd alone provides no bridge networking (CNI is nerdctl
// territory, and the distro containerd package does not ship CNI plugins);
// service pipelines also need their internal ports reachable at the host
// loopback the proxy dials. The fabric already assumes a trusted LAN, so
// host networking is the deliberate v1 choice; a CNI bridge is a later
// migration, not a v1 prerequisite.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	distref "github.com/distribution/reference"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const (
	// rtSocket is the distro containerd's default unix socket.
	rtSocket = "/run/containerd/containerd.sock"
	// rtNamespace isolates every sandman container in its own containerd
	// namespace: job containers never collide with system or nerdctl
	// containers in the default namespace, and prune/stats operations are
	// naturally scoped to the fabric's own work.
	rtNamespace = "sandman"
	// rtSnapshotter is the distro default (overlayfs).
	rtSnapshotter = "overlayfs"
	// rtProvisionBudget bounds image pull + container creation; a stalled
	// containerd fails the job (provisioning) instead of wedging it in
	// "running" forever. The task's own wait is unbounded — the control
	// plane owns timeouts and terminates the task via Kill.
	rtProvisionBudget = 10 * time.Minute
	// rtOpTimeout bounds single runtime operations (connect, kill,
	// remove, list).
	rtOpTimeout = 30 * time.Second
)

// rtConnect opens the containerd client scoped to the sandman namespace.
func rtConnect(ctx context.Context) (*containerd.Client, error) {
	return containerd.New(rtSocket, containerd.WithDefaultNamespace(rtNamespace))
}

// rtClose releases a containerd client: best-effort teardown, like the
// socket/connection closes elsewhere in the daemon.
func rtClose(client *containerd.Client) {
	_ = client.Close()
}

// rtEnsureImage returns the image, pulling it (and unpacking it into the
// snapshotter) on first use. A missing image or an unreachable registry is
// a provisioning failure — the command never starts.
func rtEnsureImage(ctx context.Context, client *containerd.Client, ref string) (containerd.Image, error) {
	named, err := distref.ParseNormalizedNamed(ref)
	if err != nil {
		return nil, fmt.Errorf("invalid image reference %q: %w", ref, err)
	}
	name := named.String()
	if img, err := client.GetImage(ctx, name); err == nil {
		// already present: make sure it is unpacked for the snapshotter
		unpacked, err := img.IsUnpacked(ctx, rtSnapshotter)
		if err == nil && !unpacked {
			if err := img.Unpack(ctx, rtSnapshotter); err != nil {
				return nil, fmt.Errorf("unpack image %q: %w", name, err)
			}
		}
		return img, nil
	}
	img, err := client.Pull(ctx, name, containerd.WithPullUnpack)
	if err != nil {
		return nil, fmt.Errorf("pull image %q: %w", name, err)
	}
	return img, nil
}

// rtMounts translates the Runner's mount vocabulary ("-v" bind pairs, "-p"
// port-publish entries) into OCI mounts. A "-p" entry selects the host
// network namespace: the service process binds the host directly, so no
// CNI port mapping is performed.
func rtMounts(mounts []string) ([]specs.Mount, bool) {
	var out []specs.Mount
	hostNet := false
	for i := 0; i+1 < len(mounts); i += 2 {
		switch mounts[i] {
		case "-v":
			parts := strings.Split(mounts[i+1], ":")
			if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
				continue
			}
			m := specs.Mount{Type: "bind", Source: parts[0], Destination: parts[1], Options: []string{"rbind"}}
			if len(parts) >= 3 && strings.Contains(parts[2], "ro") {
				m.Options = append(m.Options, "ro")
			}
			out = append(out, m)
		case "-p":
			hostNet = true
		}
	}
	return out, hostNet
}

// parseMemSize parses a docker-style memory size ("100M", "1g", "1048576",
// "1000000000000b") into bytes. A bare number is bytes; a suffix letter is
// binary, so M means MiB (the container backend's documented convention).
func parseMemSize(s string) (uint64, error) {
	s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(s, "b"), "B"))
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("invalid memory size %q", s)
	}
	num, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memory size %q", s)
	}
	var mult float64
	switch strings.ToLower(s[i:]) {
	case "":
		mult = 1
	case "k":
		mult = 1 << 10
	case "m":
		mult = 1 << 20
	case "g":
		mult = 1 << 30
	case "t":
		mult = 1 << 40
	default:
		return 0, fmt.Errorf("invalid memory unit %q", s[i:])
	}
	return uint64(num * mult), nil
}

// rtResources maps the spec's resource declarations onto the OCI spec: the
// memory limit → Memory.Limit, the memory request → Memory.Reservation
// (the soft target), the CPU limit → a CFS quota/period pair, and a CPU
// request with no declared limit → the same allocation (docker expresses a
// CPU request only as an allocation). A disk request stays accept-and-
// record: the container backend has no portable per-container writable-
// layer quota. When nothing is declared, nothing is injected.
func rtResources(spec JobSpec) oci.SpecOpts {
	return func(_ context.Context, _ oci.Client, _ *containers.Container, s *specs.Spec) error {
		if s.Linux == nil {
			return nil
		}
		if s.Linux.Resources == nil {
			s.Linux.Resources = &specs.LinuxResources{}
		}
		setCPU := func(cpu float64) {
			quota := int64(cpu * 100000)
			period := uint64(100000)
			s.Linux.Resources.CPU = &specs.LinuxCPU{Quota: &quota, Period: &period}
		}
		if spec.ResourceLimits != nil {
			if spec.ResourceLimits.Memory != "" {
				if b, err := parseMemSize(spec.ResourceLimits.Memory); err == nil {
					l := int64(b)
					if s.Linux.Resources.Memory == nil {
						s.Linux.Resources.Memory = &specs.LinuxMemory{}
					}
					s.Linux.Resources.Memory.Limit = &l
				}
			}
			if spec.ResourceLimits.CPU > 0 {
				setCPU(spec.ResourceLimits.CPU)
			}
		}
		if spec.ResourceRequests != nil {
			if spec.ResourceRequests.Memory != "" {
				if b, err := parseMemSize(spec.ResourceRequests.Memory); err == nil {
					r := int64(b)
					if s.Linux.Resources.Memory == nil {
						s.Linux.Resources.Memory = &specs.LinuxMemory{}
					}
					s.Linux.Resources.Memory.Reservation = &r
				}
			}
			if spec.ResourceRequests.CPU > 0 && (spec.ResourceLimits == nil || spec.ResourceLimits.CPU == 0) {
				setCPU(spec.ResourceRequests.CPU)
			}
		}
		return nil
	}
}

// rtUserScript wraps a command to run as the configured identity: create
// the user and working directory in-container, then su to it. This keeps
// the container backend's observable behavior (whoami/pwd report the
// configured identity) without requiring the user to pre-exist in the
// image; the OCI process user field cannot name a user that is absent from
// the image's /etc/passwd. Needs a busybox-style image (alpine), exactly
// like the backend it replaces.
func rtUserScript(user, workdir string, cmd []string) []string {
	inner := "cd " + shQuote(workdir) + " && exec " + joinSh(cmd)
	script := fmt.Sprintf("adduser -D %s 2>/dev/null; mkdir -p %s; chown -R %s %s 2>/dev/null; chown %s /sandman/out 2>/dev/null; su %s -c %s",
		shQuote(user), shQuote(workdir), shQuote(user), shQuote(workdir), shQuote(user), shQuote(user), shQuote(inner))
	return []string{"sh", "-c", script}
}

// rtSpecOpts builds the OCI spec options for one JobSpec: the containerd
// default spec (proc/sys/dev mounts, namespaces, capabilities), the
// image's config with the job's argv (entrypoint-preserving, like docker
// run), the working directory (default /sandman/out), env, bind mounts,
// the user identity wrapper, host networking, and the resource
// declarations. docker's no_new_privileges default (false) is restored:
// containerd defaults to true, which would break setuid tools and the
// identity wrapper's su.
func rtSpecOpts(spec JobSpec, img containerd.Image) []oci.SpecOpts {
	workdir := spec.Workdir
	if workdir == "" {
		workdir = "/sandman/out"
	}
	cmd := spec.Cmd
	if spec.User != "" {
		cmd = rtUserScript(spec.User, workdir, spec.Cmd)
	}
	mounts, hostNet := rtMounts(spec.Mounts)
	// the runner mounts the spec's output directory at /sandman/out (the
	// docker backend did the same): skip when the spec already declared it
	if spec.OutDir != "" && !hasMountDest(mounts, "/sandman/out") {
		mounts = append([]specs.Mount{{Type: "bind", Source: spec.OutDir, Destination: "/sandman/out", Options: []string{"rbind"}}}, mounts...)
	}
	env := spec.Env
	if spec.NodeName != "" && !hasEnv(env, "HOSTNAME") {
		env = append(env, "HOSTNAME="+spec.NodeName)
	}
	opts := []oci.SpecOpts{
		oci.WithDefaultSpec(),
		oci.WithImageConfigArgs(img, cmd),
		oci.WithProcessCwd(workdir),
		oci.WithEnv(env),
		// append the job's binds to the default mounts (proc/sys/dev...)
		func(_ context.Context, _ oci.Client, _ *containers.Container, s *specs.Spec) error {
			if len(mounts) > 0 {
				s.Mounts = append(s.Mounts, mounts...)
			}
			return nil
		},
		func(_ context.Context, _ oci.Client, _ *containers.Container, s *specs.Spec) error {
			if s.Process != nil {
				s.Process.NoNewPrivileges = false
			}
			return nil
		},
		rtResources(spec),
	}
	if hostNet {
		opts = append(opts, oci.WithHostNamespace(specs.NetworkNamespace))
	}
	return opts
}

// hasMountDest reports whether the mount list already binds the
// destination (duplicate destinations are not mounted twice).
func hasMountDest(mounts []specs.Mount, dest string) bool {
	for _, m := range mounts {
		if m.Destination == dest {
			return true
		}
	}
	return false
}

// hasEnv reports whether the KEY=VALUE list already carries KEY.
func hasEnv(env []string, key string) bool {
	for _, e := range env {
		if k, _, ok := strings.Cut(e, "="); ok && k == key {
			return true
		}
	}
	return false
}

// rtNewContainer creates (but does not start) a container for the spec:
// image ensured, snapshot prepared, spec built, node label applied. The
// container's name is the spec's stable name — Kill(name) and the job
// timeout find it by that name.
func rtNewContainer(ctx context.Context, client *containerd.Client, spec JobSpec) (containerd.Container, containerd.Image, error) {
	image := spec.Image
	if image == "" {
		image = "alpine"
	}
	img, err := rtEnsureImage(ctx, client, image)
	if err != nil {
		return nil, nil, err
	}
	c, err := client.NewContainer(ctx, spec.Name,
		containerd.WithImage(img),
		containerd.WithSnapshotter(rtSnapshotter),
		containerd.WithNewSnapshot(spec.Name, img),
		containerd.WithNewSpec(rtSpecOpts(spec, img)...),
		containerd.WithContainerLabels(map[string]string{"sandman.node": spec.NodeName}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create container %q: %w", spec.Name, err)
	}
	return c, img, nil
}

// rtCleanup deletes a task and its container (and snapshot), bounded so a
// wedged shim cannot hold a job forever.
func rtCleanup(c containerd.Container, task containerd.Task) {
	if task != nil {
		dctx, cancel := context.WithTimeout(context.Background(), rtOpTimeout)
		_, _ = task.Delete(dctx)
		cancel()
	}
	dctx, cancel := context.WithTimeout(context.Background(), rtOpTimeout)
	_ = c.Delete(dctx, containerd.WithSnapshotCleanup)
	cancel()
}

// rtRun executes one JobSpec to completion: ensure the image, create the
// container, create the task with the spec's IO, start, wait for exit, and
// clean up. Output is captured to the result tail and, when set, the
// spec's Capture writer. Failures before the user code starts (image pull,
// container/task creation, start) are provisioning failures; a non-zero
// exit is a user-code failure.
func rtRun(spec JobSpec) RunResult {
	ctx, cancel := context.WithTimeout(context.Background(), rtProvisionBudget)
	defer cancel()
	client, err := rtConnect(ctx)
	if err != nil {
		return rtProvisioningResult(fmt.Errorf("containerd unavailable: %w", err))
	}
	defer rtClose(client)
	c, _, err := rtNewContainer(ctx, client, spec)
	if err != nil {
		return rtProvisioningResult(err)
	}
	defer rtCleanup(c, nil)

	var buf bytes.Buffer
	w := io.Writer(&buf)
	if spec.Capture != nil {
		w = io.MultiWriter(&buf, spec.Capture)
	}
	// containerd's IO layer copies stdout and stderr in separate
	// goroutines; the writers must serialize (the bytes.Buffer tail and
	// the log capture are not goroutine-safe on their own).
	sw := &rtSyncWriter{w: w}
	var stdin io.Reader
	if len(spec.Stdin) > 0 {
		stdin = strings.NewReader(strings.Join(spec.Stdin, "\n") + "\n")
	}
	task, err := c.NewTask(ctx, cio.NewCreator(cio.WithStreams(stdin, sw, sw)))
	if err != nil {
		return rtProvisioningResult(fmt.Errorf("create task %q: %w", spec.Name, err))
	}
	defer rtCleanup(c, task)
	if err := task.Start(ctx); err != nil {
		return rtProvisioningResult(fmt.Errorf("start task %q: %w", spec.Name, err))
	}
	// The wait is deliberately unbounded: the control plane owns timeouts
	// (datum timeout, job timeout, cancel) and terminates the task via
	// Kill; this call returns when the task exits or is killed.
	exitCh, err := task.Wait(context.Background())
	if err != nil {
		return rtProvisioningResult(fmt.Errorf("wait task %q: %w", spec.Name, err))
	}
	st := <-exitCh
	res := RunResult{}
	if st.Error() != nil {
		res.Code = 1
		tail := buf.String()
		if tail != "" {
			tail += "\n"
		}
		tail += "wait: " + st.Error().Error()
		res.Tail = tail
		res.ProvisioningErr = st.Error()
		return res
	}
	res.Code = int(st.ExitCode())
	res.Tail = buf.String()
	if len(res.Tail) > 2000 {
		res.Tail = res.Tail[len(res.Tail)-2000:]
	}
	return res
}

// rtSyncWriter serializes writes to an underlying writer: containerd's IO
// layer copies each stream in its own goroutine, and the shared capture
// path must not see concurrent writes.
type rtSyncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *rtSyncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// rtProvisioningResult builds a failed RunResult for an environment
// failure — the command never started.
func rtProvisioningResult(err error) RunResult {
	return RunResult{Code: 1, Tail: err.Error(), ProvisioningErr: err}
}

// rtStartDetached creates and starts a container whose IO is discarded — a
// long-lived participant (a remote service). The caller removes it via
// rtRemove; the container keeps running after the client connection closes
// (the task lives under the containerd shim).
func rtStartDetached(spec JobSpec) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	client, err := rtConnect(ctx)
	if err != nil {
		return fmt.Errorf("containerd unavailable: %w", err)
	}
	defer rtClose(client)
	c, _, err := rtNewContainer(ctx, client, spec)
	if err != nil {
		return err
	}
	task, err := c.NewTask(ctx, cio.NewCreator(cio.WithStreams(nil, io.Discard, io.Discard)))
	if err != nil {
		rtCleanup(c, nil)
		return fmt.Errorf("create task %q: %w", spec.Name, err)
	}
	if err := task.Start(ctx); err != nil {
		rtCleanup(c, task)
		return fmt.Errorf("start task %q: %w", spec.Name, err)
	}
	return nil
}

// rtKill force-kills a task by its container's stable name (the datum
// timeout, job timeout, cancel path).
func rtKill(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), rtOpTimeout)
	defer cancel()
	client, err := rtConnect(ctx)
	if err != nil {
		return err
	}
	defer rtClose(client)
	task, err := rtTask(ctx, client, name)
	if err != nil {
		return err
	}
	return task.Kill(ctx, syscall.SIGKILL)
}

// rtTask returns the named container's task, or a not-found error.
func rtTask(ctx context.Context, client *containerd.Client, name string) (containerd.Task, error) {
	c, err := client.LoadContainer(ctx, name)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, fmt.Errorf("no container %q", name)
		}
		return nil, err
	}
	task, err := c.Task(ctx, nil)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, fmt.Errorf("no task for %q", name)
		}
		return nil, err
	}
	return task, nil
}

// rtRemove force-removes a container by name: kill the task if one exists,
// delete the task, delete the container and its snapshot. Used by orphan
// pruning and service teardown; the run path removes its own container at
// exit.
func rtRemove(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), rtOpTimeout)
	defer cancel()
	client, err := rtConnect(ctx)
	if err != nil {
		return err
	}
	defer rtClose(client)
	c, err := client.LoadContainer(ctx, name)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}
	task, err := c.Task(ctx, nil)
	if err == nil {
		_ = task.Kill(ctx, syscall.SIGKILL)
		_, _ = task.Delete(ctx)
	}
	return c.Delete(ctx, containerd.WithSnapshotCleanup)
}

// rtPrune removes containers a previous instance of THIS daemon left
// behind (a daemon crash cannot run the kill-on-disconnect path). The
// sandman.node label scopes the prune so a daemon never touches jobs a
// sibling daemon owns on a shared containerd.
func rtPrune(node string) {
	ctx, cancel := context.WithTimeout(context.Background(), rtOpTimeout)
	defer cancel()
	client, err := rtConnect(ctx)
	if err != nil {
		log.Printf("orphan scan: %v", err)
		return
	}
	defer rtClose(client)
	conts, err := client.Containers(ctx)
	if err != nil {
		log.Printf("orphan scan: %v", err)
		return
	}
	for _, c := range conts {
		labels, err := c.Labels(ctx)
		if err != nil || labels["sandman.node"] != node {
			continue
		}
		if err := rtRemove(c.ID()); err != nil {
			log.Printf("orphan prune %s: %v", c.ID(), err)
			continue
		}
		log.Printf("pruned orphan job container %s", c.ID())
	}
}

// runtimeVersion reports the container runtime's version (containerd) for
// the fleet views; "?" when unavailable. The wire protocol keeps the
// historical "docker=" token so older peers keep parsing the field.
var (
	rtVerOnce sync.Once
	rtVer     string
)

func runtimeVersion() string {
	rtVerOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client, err := rtConnect(ctx)
		if err != nil {
			rtVer = "?"
			return
		}
		defer rtClose(client)
		v, err := client.Version(ctx)
		if err != nil {
			rtVer = "?"
			return
		}
		rtVer = v.Version
	})
	return rtVer
}

// rtListContainers lists the node's running execution participants for the
// fleet stats view: container identity and image from containerd, plus
// live CPU/memory/pids read from the task's cgroup v2 accounting files
// (located via /proc/<pid>/cgroup). Metrics degrade to zeros when the
// cgroup is unreadable (e.g. a non-root sandman); the view never fails.
func rtListContainers() []containerInfo {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := rtConnect(ctx)
	if err != nil {
		return nil
	}
	defer rtClose(client)
	conts, err := client.Containers(ctx)
	if err != nil {
		return nil
	}
	var out []containerInfo
	for _, c := range conts {
		task, err := c.Task(ctx, nil)
		if err != nil {
			continue // no task or not running
		}
		status, err := task.Status(ctx)
		if err != nil || status.Status != containerd.Running {
			continue
		}
		info := containerInfo{ID: c.ID(), Name: c.ID(), Status: "running"}
		if img, err := c.Image(ctx); err == nil {
			info.Image = img.Name()
		}
		info.CPU, info.MemBytes, info.MemLimit, info.MemPerc, info.PIDs = rtTaskMetrics(task.Pid())
		out = append(out, info)
	}
	return out
}

// rtTaskMetrics reads a running task's live resource usage from its cgroup
// v2 accounting files. CPU percent is a short-window sample of cpu.stat
// (usage_usec), like docker stats; memory comes from memory.current /
// memory.max ("max" = no limit → 0); pids from pids.current.
func rtTaskMetrics(pid uint32) (cpu float64, memBytes, memLimit uint64, memPerc float64, pids int) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return 0, 0, 0, 0, 0
	}
	line := strings.TrimSpace(string(b))
	path := ""
	// cgroup v2: "0::/path"; cgroup v1: "N:name:/path"
	if i := strings.Index(line, "::"); i >= 0 {
		path = strings.TrimPrefix(line[i+2:], "/")
	} else if i := strings.Index(line, ":/"); i >= 0 {
		path = strings.TrimPrefix(line[i+1:], "/")
	}
	if path == "" {
		return 0, 0, 0, 0, 0
	}
	root := "/sys/fs/cgroup/" + path
	usage := func() uint64 {
		b, err := os.ReadFile(root + "/cpu.stat")
		if err != nil {
			return 0
		}
		for _, l := range strings.Split(string(b), "\n") {
			f := strings.Fields(l)
			if len(f) == 2 && f[0] == "usage_usec" {
				v, _ := strconv.ParseUint(f[1], 10, 64)
				return v
			}
		}
		return 0
	}
	u1 := usage()
	if u1 > 0 {
		time.Sleep(500 * time.Millisecond)
		u2 := usage()
		if u2 > u1 {
			// usage_usec over a 500ms window: 100% = one full core
			cpu = float64(u2-u1) / 500000 * 100
		}
	}
	if b, err := os.ReadFile(root + "/memory.current"); err == nil {
		memBytes, _ = strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	}
	if b, err := os.ReadFile(root + "/memory.max"); err == nil {
		if s := strings.TrimSpace(string(b)); s != "max" {
			if v, err := strconv.ParseUint(s, 10, 64); err == nil {
				memLimit = v
			}
		}
	}
	if memLimit > 0 {
		memPerc = 100 * float64(memBytes) / float64(memLimit)
	}
	if b, err := os.ReadFile(root + "/pids.current"); err == nil {
		pids, _ = strconv.Atoi(strings.TrimSpace(string(b)))
	}
	return cpu, memBytes, memLimit, memPerc, pids
}
