package main

// The containerd-backed execution integration tests: these exercise the
// real runtime stack (containerd → runc → cgroups) end to end and SKIP
// cleanly when the runtime is unreachable (a non-root dev box, a host
// without containerd) — the repository's normal test suite never requires
// a privileged environment. CI runs them with a reachable containerd.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"

	"sandman/client"
)

// rtAvailable reports whether the sandbox's containerd is reachable from
// this test process.
func rtAvailable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cli, err := containerd.New(rtSocket, containerd.WithDefaultNamespace(rtNamespace))
	if err != nil {
		return false
	}
	defer func() { _ = cli.Close() }()
	_, err = cli.Version(ctx)
	return err == nil
}

func skipNoRT(t *testing.T) {
	t.Helper()
	if !rtAvailable() {
		t.Skip("containerd unreachable (need root or the containerd group): integration test")
	}
}

// uniqueRTName gives each integration run a collision-free container name.
func uniqueRTName(t *testing.T, tag string) string {
	return fmt.Sprintf("rt-%s-%d-%d", tag, os.Getpid(), time.Now().UnixNano()%100000)
}

// findRTContainer reports whether a container with the id still exists.
func findRTContainer(t *testing.T, id string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := containerd.New(rtSocket, containerd.WithDefaultNamespace(rtNamespace))
	if err != nil {
		return ""
	}
	defer func() { _ = cli.Close() }()
	if _, err := cli.LoadContainer(ctx, id); err != nil {
		return ""
	}
	return id
}

// TestRTImagePullAndRun pulls an image on first use and runs a command,
// verifying exit code, combined-output capture, and post-run cleanup.
func TestRTImagePullAndRun(t *testing.T) {
	skipNoRT(t)
	name := uniqueRTName(t, "run")
	res := containerRunner{}.Run(JobSpec{
		Image:    "alpine:3.21",
		Name:     name,
		NodeName: "rt-test",
		Cmd:      []string{"sh", "-c", "echo out-line; echo err-line >&2; cat /etc/alpine-release"},
	})
	if res.Code != 0 {
		t.Fatalf("run code = %d, provisioning=%v tail=%q", res.Code, res.ProvisioningErr, res.Tail)
	}
	if !strings.Contains(res.Tail, "out-line") || !strings.Contains(res.Tail, "err-line") {
		t.Fatalf("combined output missing streams: %q", res.Tail)
	}
	if !strings.Contains(res.Tail, "3.21") {
		t.Fatalf("output missing expected content: %q", res.Tail)
	}
	// the container and its snapshot are removed after the run
	if id := findRTContainer(t, name); id != "" {
		t.Fatalf("container %s survived the run's cleanup", id)
	}
}

// TestRTExitCode verifies a non-zero user exit maps to the exit code.
func TestRTExitCode(t *testing.T) {
	skipNoRT(t)
	res := containerRunner{}.Run(JobSpec{
		Image: "alpine:3.21",
		Name:  uniqueRTName(t, "exit"),
		Cmd:   []string{"sh", "-c", "exit 7"},
	})
	if res.Code != 7 {
		t.Fatalf("exit code = %d, want 7 (provisioning=%v tail=%q)", res.Code, res.ProvisioningErr, res.Tail)
	}
	if res.ProvisioningErr != nil {
		t.Fatalf("a user-code failure must not be a provisioning error: %v", res.ProvisioningErr)
	}
}

// TestRTStdin feeds the run's stdin and reads it back.
func TestRTStdin(t *testing.T) {
	skipNoRT(t)
	res := containerRunner{}.Run(JobSpec{
		Image: "alpine:3.21",
		Name:  uniqueRTName(t, "stdin"),
		Cmd:   []string{"cat"},
		Stdin: []string{"line one", "line two"},
	})
	if res.Code != 0 || !strings.Contains(res.Tail, "line one\nline two") {
		t.Fatalf("stdin round-trip: code=%d tail=%q", res.Code, res.Tail)
	}
}

// TestRTEnv verifies the environment reaches the process.
func TestRTEnv(t *testing.T) {
	skipNoRT(t)
	res := containerRunner{}.Run(JobSpec{
		Image: "alpine:3.21",
		Name:  uniqueRTName(t, "env"),
		Env:   []string{"FOO=bar", "BAZ=qux"},
		Cmd:   []string{"sh", "-c", "echo $FOO-$BAZ"},
	})
	if res.Code != 0 || strings.TrimSpace(res.Tail) != "bar-qux" {
		t.Fatalf("env round-trip: code=%d tail=%q", res.Code, res.Tail)
	}
}

// TestRTWorkdir verifies the working directory (default /sandman/out with
// the out mount present, and an explicit workdir).
func TestRTWorkdir(t *testing.T) {
	skipNoRT(t)
	out := t.TempDir()
	// default workdir is the mounted out dir
	res := containerRunner{}.Run(JobSpec{
		Image:  "alpine:3.21",
		Name:   uniqueRTName(t, "wd1"),
		OutDir: out,
		Cmd:    []string{"pwd"},
	})
	if res.Code != 0 || strings.TrimSpace(res.Tail) != "/sandman/out" {
		t.Fatalf("default workdir: code=%d tail=%q", res.Code, res.Tail)
	}
	// explicit workdir inside the image
	res = containerRunner{}.Run(JobSpec{
		Image:   "alpine:3.21",
		Name:    uniqueRTName(t, "wd2"),
		Workdir: "/etc",
		Cmd:     []string{"pwd"},
	})
	if res.Code != 0 || strings.TrimSpace(res.Tail) != "/etc" {
		t.Fatalf("explicit workdir: code=%d tail=%q", res.Code, res.Tail)
	}
}

// TestRTMounts verifies bind mounts (the out dir and a ro input dir).
func TestRTBindMounts(t *testing.T) {
	skipNoRT(t)
	out := t.TempDir()
	in := t.TempDir()
	if err := os.WriteFile(filepath.Join(in, "input.txt"), []byte("hello-mount"), 0o644); err != nil {
		t.Fatal(err)
	}
	res := containerRunner{}.Run(JobSpec{
		Image:  "alpine:3.21",
		Name:   uniqueRTName(t, "mounts"),
		OutDir: out,
		Mounts: []string{"-v", in + ":/sandman/in/repo:ro"},
		Cmd:    []string{"sh", "-c", "cat /sandman/in/repo/input.txt > /sandman/out/result.txt"},
	})
	if res.Code != 0 {
		t.Fatalf("mount run: code=%d provisioning=%v tail=%q", res.Code, res.ProvisioningErr, res.Tail)
	}
	b, err := os.ReadFile(filepath.Join(out, "result.txt"))
	if err != nil || string(b) != "hello-mount" {
		t.Fatalf("mounted input = %q (err %v), want hello-mount", b, err)
	}
}

// TestRTUser verifies the configured identity is applied (whoami).
func TestRTUser(t *testing.T) {
	skipNoRT(t)
	res := containerRunner{}.Run(JobSpec{
		Image:   "alpine:3.21",
		Name:    uniqueRTName(t, "user"),
		User:    "test",
		Workdir: "/home/test",
		Cmd:     []string{"sh", "-c", "whoami; pwd"},
	})
	if res.Code != 0 {
		t.Fatalf("user run: code=%d provisioning=%v tail=%q", res.Code, res.ProvisioningErr, res.Tail)
	}
	if !strings.Contains(res.Tail, "test") || !strings.Contains(res.Tail, "/home/test") {
		t.Fatalf("user identity not applied: %q", res.Tail)
	}
}

// TestRTResources verifies a resource declaration is accepted and the run
// completes (the cgroup values are set; enforcement is the kernel's).
func TestRTResources(t *testing.T) {
	skipNoRT(t)
	res := containerRunner{}.Run(JobSpec{
		Image: "alpine:3.21",
		Name:  uniqueRTName(t, "resources"),
		Cmd:   []string{"true"},
		ResourceLimits: &client.ResourceLimits{
			Memory: "100M",
			CPU:    0.5,
		},
	})
	if res.Code != 0 {
		t.Fatalf("resource run: code=%d provisioning=%v tail=%q", res.Code, res.ProvisioningErr, res.Tail)
	}
}

// TestRTKill starts a sleeping run, kills it by name mid-flight, and
// verifies the run reports the SIGKILL exit (137) and cleans up.
func TestRTKill(t *testing.T) {
	skipNoRT(t)
	name := uniqueRTName(t, "kill")
	resCh := make(chan RunResult, 1)
	go func() {
		resCh <- containerRunner{}.Run(JobSpec{
			Image: "alpine:3.21",
			Name:  name,
			Cmd:   []string{"sh", "-c", "sleep 30"},
		})
	}()
	// wait for the container to exist, then kill it
	deadline := time.Now().Add(30 * time.Second)
	for findRTContainer(t, name) == "" {
		if time.Now().After(deadline) {
			t.Fatal("container never appeared")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := (containerRunner{}).Kill(name); err != nil {
		t.Fatalf("kill: %v", err)
	}
	select {
	case res := <-resCh:
		if res.Code != 137 {
			t.Fatalf("killed run code = %d, want 137 (SIGKILL); tail=%q", res.Code, res.Tail)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run did not settle after kill")
	}
	if id := findRTContainer(t, name); id != "" {
		t.Fatalf("container %s survived the kill's cleanup", id)
	}
}

// TestRTProvisioningFailure verifies a bad image is a provisioning
// failure, not a user-code failure.
func TestRTProvisioningFailure(t *testing.T) {
	skipNoRT(t)
	res := containerRunner{}.Run(JobSpec{
		Image: "sandman-no-such-image-xyz",
		Name:  uniqueRTName(t, "provision"),
		Cmd:   []string{"true"},
	})
	if res.ProvisioningErr == nil {
		t.Fatalf("bad image: want a provisioning error, got code=%d tail=%q", res.Code, res.Tail)
	}
	if !isProvisioningError(res.Tail) {
		t.Fatalf("bad image tail not classified as provisioning: %q", res.Tail)
	}
}
