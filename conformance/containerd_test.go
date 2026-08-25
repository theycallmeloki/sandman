package conformance

// The container-runtime test helpers: the container-facing subset gates on
// a reachable containerd (the runtime the container backend speaks) and
// inspects live execution participants through containerd's API — the
// docker CLI is no longer part of the stack.

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
)

// rtNamespace must match the backend's namespace (containerd_rt.go).
const rtNamespace = "sandman"

func rtClient(t *testing.T) *containerd.Client {
	t.Helper()
	ctx := namespaces.WithNamespace(context.Background(), rtNamespace)
	cli, err := containerd.New("/run/containerd/containerd.sock",
		containerd.WithDefaultNamespace(rtNamespace))
	if err != nil {
		t.Fatalf("containerd connect: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	if _, err := cli.Version(ctx); err != nil {
		t.Fatalf("containerd version: %v", err)
	}
	return cli
}

// runtimeAvailable reports whether the container runtime is present and
// reachable (the container-facing subset runs only when it is).
func runtimeAvailable() bool {
	st, err := os.Stat("/run/containerd/containerd.sock")
	if err != nil {
		return false
	}
	if st == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cli, err := containerd.New("/run/containerd/containerd.sock",
		containerd.WithDefaultNamespace(rtNamespace))
	if err != nil {
		return false
	}
	defer func() { _ = cli.Close() }()
	_, err = cli.Version(ctx)
	return err == nil
}

// removeSandmanContainers force-removes containers whose names start with
// the given prefix (a crashed daemon's stragglers that poison later
// tests' container-count assertions). Safe without a test handle (TestMain
// uses it for the pre/post-suite sweep).
func removeSandmanContainers(prefix string) {
	if !runtimeAvailable() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cli, err := containerd.New("/run/containerd/containerd.sock",
		containerd.WithDefaultNamespace(rtNamespace))
	if err != nil {
		return
	}
	defer func() { _ = cli.Close() }()
	conts, err := cli.Containers(ctx)
	if err != nil {
		return
	}
	for _, c := range conts {
		if !strings.HasPrefix(c.ID(), prefix) {
			continue
		}
		if task, err := c.Task(ctx, nil); err == nil {
			_ = task.Kill(ctx, 9)
			_, _ = task.Delete(ctx)
		}
		_ = c.Delete(ctx, containerd.WithSnapshotCleanup)
	}
}

// findContainerID returns the id of a container whose name starts with the
// prefix, or "".
func findContainerID(t *testing.T, prefix string) string {
	t.Helper()
	cli := rtClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conts, err := cli.Containers(ctx)
	if err != nil {
		return ""
	}
	for _, c := range conts {
		if strings.HasPrefix(c.ID(), prefix) {
			return c.ID()
		}
	}
	return ""
}
