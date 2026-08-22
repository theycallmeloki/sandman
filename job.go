package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"syscall"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
)

// jobSpec is the text description of a job: an image, an argv, env, and a
// workdir. Jobs are ephemeral containerd containers — no state outlives
// the container except the exit code and whatever the job wrote to its
// workdir.
type jobSpec struct {
	ID      string
	Image   string
	Env     []string // "K=V"
	Workdir string
	Argv    []string
	Node    string // owning daemon, for orphan ownership on shared runtimes
}

// job is a running container and its stdio pipes.
type job struct {
	cid    string
	Stdin  io.WriteCloser
	Stdout io.ReadCloser
	Stderr io.ReadCloser
	done   chan int

	client *containerd.Client
	c      containerd.Container
	task   containerd.Task
	// the task-side pipe ends: closed at cleanup so the reader ends
	// (the daemon's output relays) see EOF when the task exits
	stdoutW, stderrW io.WriteCloser
	clean            sync.Once
}

// startJob creates and starts the container. The container name is known
// synchronously (containerd create is a metadata operation; the task's
// pid is known after start), so signals work from the first frame.
func startJob(spec jobSpec) (*job, error) {
	// The container name is the job's identity on the node; the label is
	// its owner. A fresh daemon prunes only containers carrying its own
	// label, so jobs orphaned by a crash do not outlive their owner —
	// without touching jobs a sibling daemon owns on a shared containerd
	// (Rule of Repair).
	name := "sandman-" + spec.ID
	js := JobSpec{
		Image:    spec.Image,
		NodeName: spec.Node,
		Name:     name,
		Cmd:      spec.Argv,
		Env:      spec.Env,
		Workdir:  spec.Workdir,
	}
	// the create is a control op (plus any image pull): bound it so a
	// stalled containerd fails the job instead of wedging it "running"
	// forever — an unbounded create was observed wedging jobs on a
	// stalled daemon, which then poisoned every later flush and GC
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	client, err := rtConnect(ctx)
	if err != nil {
		return nil, fmt.Errorf("containerd unavailable: %w", err)
	}
	c, _, err := rtNewContainer(ctx, client, js)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	// the job's stdio is wired through pipes: the daemon relays frames
	// between the wire protocol and these ends (the client's stdin pump
	// and the two output relays)
	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	task, err := c.NewTask(ctx, cio.NewCreator(cio.WithStreams(stdinR, stdoutW, stderrW)))
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("create task: %w", err)
	}
	if err := task.Start(ctx); err != nil {
		_, _ = task.Delete(ctx)
		_ = client.Close()
		return nil, fmt.Errorf("start task: %w", err)
	}
	exitCh, err := task.Wait(context.Background())
	if err != nil {
		_, _ = task.Delete(ctx)
		_ = client.Close()
		return nil, fmt.Errorf("wait task: %w", err)
	}
	j := &job{
		cid:     name,
		Stdin:   stdinW,
		Stdout:  stdoutR,
		Stderr:  stderrR,
		done:    make(chan int, 1),
		client:  client,
		c:       c,
		task:    task,
		stdoutW: stdoutW,
		stderrW: stderrW,
	}
	// The exit code of the task is the container's exit code; signal
	// deaths report 128+signal, like a shell (containerd normalizes
	// signaled exits the same way docker does).
	go func() {
		st := <-exitCh
		code := 0
		if st.Error() != nil {
			code = 125 // runtime-level failure, docker's own convention
		} else {
			code = int(st.ExitCode())
		}
		j.cleanup()
		j.done <- code
	}()
	return j, nil
}

// cleanup deletes the task and container (and snapshot), releases the
// client, and closes the task-side pipe ends so the relays see EOF; safe
// to call once from the exit goroutine or a kill path.
func (j *job) cleanup() {
	j.clean.Do(func() {
		if j.stdoutW != nil {
			j.stdoutW.Close()
		}
		if j.stderrW != nil {
			j.stderrW.Close()
		}
		if j.task != nil {
			dctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, _ = j.task.Delete(dctx)
			cancel()
		}
		if j.c != nil {
			dctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = j.c.Delete(dctx, containerd.WithSnapshotCleanup)
			cancel()
		}
		if j.client != nil {
			_ = j.client.Close()
		}
	})
}

// sigByName maps a signal name ("INT", "TERM", "QUIT", "KILL") to the
// syscall signal; unknown names default to KILL.
func sigByName(name string) syscall.Signal {
	switch strings.TrimPrefix(strings.ToUpper(name), "SIG") {
	case "INT":
		return syscall.SIGINT
	case "TERM":
		return syscall.SIGTERM
	case "QUIT":
		return syscall.SIGQUIT
	case "HUP":
		return syscall.SIGHUP
	default:
		return syscall.SIGKILL
	}
}

func (j *job) Signal(sig string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return j.task.Kill(ctx, sigByName(sig))
}

// Kill force-kills the container (client vanished, no orphans); the exit
// goroutine's cleanup removes the container once the task dies.
func (j *job) Kill() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := j.task.Kill(ctx, syscall.SIGKILL)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		// a task that is already gone needs no kill; the container may
		// still need removal
		j.cleanup()
	}
	return err
}
