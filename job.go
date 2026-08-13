package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// jobSpec is the text description of a job: an image, an argv, env, and a
// workdir. Jobs are ephemeral `docker run`s — no state outlives the container
// except the exit code and whatever the job wrote to its workdir.
type jobSpec struct {
	ID      string
	Image   string
	Env     []string // "K=V"
	Workdir string
	Argv    []string
	Node    string // owning daemon, for orphan ownership on shared dockers
}

// job is a running container and its stdio pipes.
type job struct {
	spec   jobSpec
	cid    string
	Stdin  io.WriteCloser
	Stdout io.ReadCloser
	Stderr io.ReadCloser
	done   chan int
}

// startJob creates and starts the container. The container id is known
// synchronously (docker create -q), so signals work from the first frame.
func startJob(spec jobSpec) (*job, error) {
	// The container name is the job's identity on the node; the label is its
	// owner. A fresh daemon prunes only containers carrying its own label, so
	// jobs orphaned by a crash do not outlive their owner — without touching
	// jobs a sibling daemon owns on a shared dockerd (Rule of Repair).
	args := []string{"create", "-q", "--rm", "-i",
		"--name", "sandman-" + spec.ID,
		"--label", "sandman.node=" + spec.Node,
		"-w", spec.Workdir}
	for _, e := range spec.Env {
		args = append(args, "-e", e)
	}
	args = append(args, spec.Image)
	args = append(args, spec.Argv...)
	// the create is a control op (plus any image pull): bound it so a
	// stalled dockerd fails the job instead of wedging it "running"
	// forever — an unbounded create was observed wedging jobs on a
	// stalled daemon, which then poisoned every later flush and GC
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).Output()
	if err != nil {
		return nil, fmt.Errorf("docker create: %w", err)
	}
	cid := strings.TrimSpace(string(out))
	if cid == "" {
		return nil, fmt.Errorf("docker create returned no container id")
	}

	cmd := exec.Command("docker", "start", "-a", "-i", cid)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	j := &job{
		spec:   spec,
		cid:    cid,
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
		done:   make(chan int, 1),
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("docker start: %w", err)
	}
	// The exit code of `docker start -a` is the container's exit code.
	// (docker wait is NOT usable here: on --rm containers it races removal
	// and lies with 0.) Signal deaths report 128+signal, like a shell.
	go func() {
		err := cmd.Wait()
		code := 0
		if err != nil {
			var ee *exec.ExitError
			if errors.As(err, &ee) && ee.ExitCode() >= 0 {
				code = ee.ExitCode()
			} else if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
				code = 128 + int(ws.Signal())
			} else {
				code = 125 // docker-level failure, docker's own convention
			}
		}
		j.done <- code
	}()
	return j, nil
}

func (j *job) Signal(sig string) error {
	name := strings.TrimPrefix(sig, "SIG")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", "kill", "-s", "SIG"+name, j.cid).Run()
}

// Kill force-kills the container (client vanished, no orphans).
func (j *job) Kill() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", "kill", j.cid).Run()
}
