package conformance

// The container-facing subset (TESTING_ARCHITECTURE.md D-23 R-4):
// behaviors whose contract is the execution runtime keep the container
// backend. Each test runs against a dedicated container-backed daemon
// and skips cleanly when the runtime is absent; the main matrix runs on
// the process backend (no runtime required).

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// withContainerDaemon swaps the shared harness client (and its daemon
// globals) to a fresh container-backed daemon for the duration of the
// test: the container-facing subset needs the real runtime, the matrix
// daemon (process backend) cannot serve it. Tests are sequential, so the
// swap is safe; the matrix daemon keeps running untouched on its own
// port.
func withContainerDaemon(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("container runtime unavailable (docker version failed): this test needs the container backend (D-23 R-4)")
	}
	state := daemonStateDir + "-container"
	os.RemoveAll(state)
	os.MkdirAll(state, 0o755)
	port := freePort()
	cmd := exec.Command(binPath, "daemon", "-name", daemonName, "-port", strconv.Itoa(port), "-state", state)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start container daemon: %v", err)
	}
	if !waitPort(port, 15*time.Second) {
		t.Fatalf("container daemon did not come up")
	}

	oldC, oldPort, oldState := c, daemonPort, daemonStateDir
	c = client.New(fmt.Sprintf("127.0.0.1:%d", port))
	daemonPort = port
	daemonStateDir = state
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		os.RemoveAll(state)
		c, daemonPort, daemonStateDir = oldC, oldPort, oldState
	})
}

func TestSB043_CrashThenUpdate(t *testing.T) {
	withContainerDaemon(t)
	repo := uniq(t)
	mustRepo(t, repo)
	name := uniq(t)
	bad := &client.Transform{Image: "INVALID_IMAGE_REF", Cmd: []string{"sh", "-c", "true"}}
	mustPipeline(t, client.Pipeline{Name: name, Transform: bad, Input: &client.Input{Repo: repo, Glob: "/*"}})
	commitFiles(t, repo, "master", map[string]string{"file": "x"})
	// the first job cannot be provisioned: the pipeline crashes
	pollFor(t, "pipeline crashed", 30*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State == "crashed" && info.Reason != ""
	})
	// crashing pipelines do not schedule
	time.Sleep(500 * time.Millisecond)
	if js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: name}); len(js) == 0 {
		t.Fatalf("no job record after crash attempt")
	}

	mustUpdate(t, name, copyTransform(repo), &client.Input{Repo: repo, Glob: "/*"}, false)
	pollFor(t, "pipeline running", 30*time.Second, func() bool {
		info, err := c.InspectPipeline(name)
		return err == nil && info.State == "running"
	})
	cm2 := replaceCommit(t, repo, "master", map[string]string{"file": "y"})
	jobs := flushOK(t, cm2.ID)
	if len(jobs) != 1 {
		t.Fatalf("post-recovery flush: %d jobs, want 1", len(jobs))
	}
	got, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil || string(got) != "y" {
		t.Fatalf("output = %q (err %v), want y", got, err)
	}
}
func TestSB067_ResourceRequestsApplied(t *testing.T) {
	withContainerDaemon(t)
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
	// the disk request is accept-and-record (D-15): docker has no portable
	// per-container ephemeral-storage knob (the runner comment documents
	// the deviation), so the observable contract is the round-trip
	pi, err := c.InspectPipeline(name)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if pi.Transform == nil || pi.Transform.ResourceRequests == nil ||
		pi.Transform.ResourceRequests.Disk != "10M" {
		t.Fatalf("disk request round-trip = %+v, want 10M", pi.Transform)
	}
	flushSetOK(t, []string{cm.ID})
}
func TestSB068_ResourceLimitsApplied(t *testing.T) {
	withContainerDaemon(t)
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
	withContainerDaemon(t)
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
	withContainerDaemon(t)
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
func TestSB091_UnprovisionableEnvironmentCrashes(t *testing.T) {
	withContainerDaemon(t)
	images := []string{
		"sandman-no-such-image-xyz",
		"docker.io/library/sandman-no-such-image-xyz:latest",
	}
	for _, img := range images {
		repo := uniq(t)
		mustRepo(t, repo)
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name: pipe,
			Transform: &client.Transform{
				Image: img,
				Cmd:   []string{"sh", "-c", "true"},
			},
			Input: &client.Input{Repo: repo, Glob: "/*"},
		})
		cm := commitFiles(t, repo, "master", map[string]string{"f": "x"})
		if _, err := c.FlushSet([]string{cm.ID}, 60*time.Second); err != nil {
			t.Fatalf("flush of the unprovisionable pipeline: %v", err)
		}

		// the provisioning failure converges on the crashing state with a
		// recorded reason, not a hang in running
		var pi client.PipelineInfo
		pollFor(t, "pipeline "+pipe+" to crash", 120*time.Second, func() bool {
			got, err := c.InspectPipeline(pipe)
			if err != nil {
				return false
			}
			pi = got
			return got.State == "crashed"
		})
		if pi.Reason == "" {
			t.Fatalf("pipeline %s crashed with no reason", pipe)
		}
	}
}
func TestSB128_UserIdentityAndWorkingDir(t *testing.T) {
	withContainerDaemon(t)
	repo := uniq(t)
	mustRepo(t, repo)

	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine",
			Cmd: []string{"sh", "-c",
				fmt.Sprintf(`whoami > ${OUT}/whoami; pwd > ${OUT}/pwd; cp ${%s}/* ${OUT}/`, repo)},
			User:    "test",
			Workdir: "/home/test",
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	}
	mustPipeline(t, p)

	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo"})

	jobs := flushOK(t, cm.ID)
	out := jobs[0].OutputCommit

	read := func(path string) string {
		t.Helper()
		b, err := c.GetFile(out, path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return strings.TrimSpace(string(b))
	}
	if got := read("whoami"); got != "test" {
		t.Fatalf("whoami = %q, want test", got)
	}
	if got := read("pwd"); got != "/home/test" {
		t.Fatalf("pwd = %q, want /home/test", got)
	}
	if got := read("file"); got != "foo" {
		t.Fatalf("file = %q, want foo", got)
	}
}
func TestSB139_SpoutPipelines(t *testing.T) {
	withContainerDaemon(t)
	t.Run("spout accumulates commits", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: spoutCmd(100, 5, false)},
			Spout:     &client.Spout{},
		})
		ch := waitSpoutCommits(t, pipe, 5)
		// the latest commit holds the file at its final, grown size
		last := ch[len(ch)-1]
		b, err := c.GetFile(last.ID, "file")
		if err != nil {
			t.Fatalf("read spout file: %v", err)
		}
		if len(b) != 500 {
			t.Fatalf("final file = %d bytes, want 500 (grown across cycles)", len(b))
		}
		// the job settles success when the container's loop ends
		js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		pollFor(t, "spout job settled", 60*time.Second, func() bool {
			if len(js) == 0 {
				return false
			}
			j, err := c.InspectJob(js[0].ID)
			return err == nil && j.State != "running"
		})
		j, _ := c.InspectJob(js[0].ID)
		if j.State != "success" {
			t.Fatalf("spout job state = %s (reason %q), want success", j.State, j.Reason)
		}
		// clause 14: after sustained spout activity, a full consistency
		// check reports no errors and a system-wide reset completes
		if err := c.Check(); err != nil {
			t.Fatalf("consistency check: %v", err)
		}
		if err := c.Reset(); err != nil {
			t.Fatalf("reset: %v", err)
		}
		if err := c.Check(); err != nil {
			t.Fatalf("consistency check after reset: %v", err)
		}
	})

	t.Run("overwrite keeps size constant", func(t *testing.T) {
		pipe := uniq(t)
		// every cycle writes the same 100 bytes
		script := "for i in $(seq 1 5); do yes $i | head -c 100 > ${OUT}/file; sleep 1; done"
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", script}},
			Spout:     &client.Spout{Overwrite: true},
		})
		ch := waitSpoutCommits(t, pipe, 5)
		b, err := c.GetFile(ch[len(ch)-1].ID, "file")
		if err != nil {
			t.Fatalf("read spout file: %v", err)
		}
		if len(b) != 100 {
			t.Fatalf("overwrite file = %d bytes, want a constant 100", len(b))
		}
	})

	t.Run("deleting the head does not stop the spout", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: spoutCmd(100, 8, false)},
			Spout:     &client.Spout{},
		})
		ch := waitSpoutCommits(t, pipe, 3)
		// delete the head commit of the spout's branch
		if err := c.DeleteCommit(ch[len(ch)-1].ID); err != nil {
			t.Fatalf("delete spout head: %v", err)
		}
		// the spout keeps producing: more commits appear after the delete
		pollFor(t, "more spout commits after deletion", 120*time.Second, func() bool {
			got, err := c.CommitHistory(pipe, "master")
			return err == nil && len(got) > 2
		})
		// once the spout's cycles finish, the branch holds 8 minus the
		// deleted head
		js0, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		if len(js0) > 0 {
			pollFor(t, "spout job settled", 120*time.Second, func() bool {
				j, err := c.InspectJob(js0[0].ID)
				return err == nil && j.State != "running"
			})
		}
		after, _ := c.CommitHistory(pipe, "master")
		if len(after) != 7 {
			t.Fatalf("spout history has %d commits after deletion, want 7 (8 cycles minus the deleted head)", len(after))
		}
	})

	t.Run("downstream consumes the spout", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: spoutCmd(100, 5, false)},
			Spout:     &client.Spout{},
		})
		waitSpoutCommits(t, pipe, 5)
		js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		if len(js) == 0 {
			t.Fatalf("no spout job")
		}
		pollFor(t, "spout job settled", 60*time.Second, func() bool {
			j, err := c.InspectJob(js[0].ID)
			return err == nil && j.State != "running"
		})
		// attach a downstream pipeline: exactly one job against the head
		down := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      down,
			Transform: &client.Transform{Image: "alpine"},
			Input:     &client.Input{Repo: pipe, Glob: "/*"},
		})
		head, err := c.HeadCommit(pipe, "master")
		if err != nil {
			t.Fatalf("spout head: %v", err)
		}
		jobs := flushOK(t, head.ID)
		var downJob client.Job
		for _, j := range jobs {
			if j.Pipeline == down {
				downJob = j
			}
		}
		if downJob.ID == "" {
			t.Fatalf("no downstream job for the spout's head")
		}
		b, err := c.GetFile(downJob.OutputCommit, "file")
		if err != nil || len(b) != 500 {
			t.Fatalf("downstream file = %d bytes (%v), want the spout's 500", len(b), err)
		}
	})

	t.Run("marker branch", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: spoutCmd(100, 5, true)},
			Spout:     &client.Spout{Marker: "markers"},
		})
		waitSpoutCommits(t, pipe, 5)
		pollFor(t, "marker commits", 60*time.Second, func() bool {
			ch, err := c.CommitHistory(pipe, "markers")
			return err == nil && len(ch) >= 5
		})
		mh, err := c.HeadCommit(pipe, "markers")
		if err != nil {
			t.Fatalf("marker head: %v", err)
		}
		b, err := c.GetFile(mh.ID, "marker")
		if err != nil {
			t.Fatalf("read marker: %v", err)
		}
		if string(b) != "m5\n" {
			t.Fatalf("marker = %q, want the latest marker content", string(b))
		}
	})

	t.Run("rapid open/close keeps every cycle's file", func(t *testing.T) {
		// clause 2: ten cycles at 50ms spacing — 20x faster than the
		// accumulation test's 1s cadence but wider than the daemon's
		// 250ms poll, so every cycle is caught separately; the trailing
		// sleep keeps the container alive past the last write so the
		// final poll commits it. Nothing may be lost or skipped.
		pipe := uniq(t)
		script := "for i in $(seq 1 10); do echo content-$i > ${OUT}/f$i; sleep 0.05; done; sleep 1"
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", script}},
			Spout:     &client.Spout{},
		})
		waitSpoutCommits(t, pipe, 1)
		pollSpoutJobSettled(t, pipe)
		ch, err := c.CommitHistory(pipe, "master")
		if err != nil {
			t.Fatalf("history: %v", err)
		}
		if len(ch) == 0 {
			t.Fatalf("no spout commits at all")
		}
		head := ch[len(ch)-1]
		for i := 1; i <= 10; i++ {
			want := "content-" + itoa(i) + "\n"
			b, err := c.GetFile(head.ID, "f"+itoa(i))
			if err != nil || string(b) != want {
				t.Fatalf("f%d = %q, err %v; want %q — a cycle was lost or skipped", i, b, err, want)
			}
		}
		// no empty commit: every commit in the history carries files
		for _, cm := range ch {
			fs, err := c.ListFiles(cm.ID)
			if err != nil {
				t.Fatalf("list %s: %v", cm.ID, err)
			}
			if len(fs) == 0 {
				t.Fatalf("commit %s is empty — an empty cycle surfaced a commit", cm.ID)
			}
		}
	})

	t.Run("busy-loop hammer yields strictly growing commits", func(t *testing.T) {
		// clause 3: a no-delay-ish loop (30ms spacing, spanning ~1.2s)
		// appends to one file; each poll-detected change is one commit
		// with exactly one file and a strictly larger size; cycles with
		// an empty payload (nothing changed) surface NO commit.
		pipe := uniq(t)
		script := "for i in $(seq 1 40); do echo x >> ${OUT}/file; sleep 0.03; done; sleep 1"
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", script}},
			Spout:     &client.Spout{},
		})
		waitSpoutCommits(t, pipe, 2)
		pollSpoutJobSettled(t, pipe)
		ch, err := c.CommitHistory(pipe, "master")
		if err != nil {
			t.Fatalf("history: %v", err)
		}
		sizes := make([]int, len(ch))
		for i, cm := range ch {
			fs, err := c.ListFiles(cm.ID)
			if err != nil {
				t.Fatalf("list %s: %v", cm.ID, err)
			}
			if len(fs) != 1 {
				t.Fatalf("commit %s holds %d files, want exactly one", cm.ID, len(fs))
			}
			b, err := c.GetFile(cm.ID, "file")
			if err != nil {
				t.Fatalf("read file at %s: %v", cm.ID, err)
			}
			sizes[i] = len(b)
		}
		for i := 1; i < len(sizes); i++ {
			if sizes[i] <= sizes[i-1] {
				t.Fatalf("file size not strictly increasing: %v", sizes)
			}
		}
		head := ch[len(ch)-1]
		b, err := c.GetFile(head.ID, "file")
		if err != nil || len(b) != 80 {
			t.Fatalf("final file = %d bytes, err %v; want 80 (40 x \"x\\n\" appends committed)", len(b), err)
		}
		// after the job settles, the unchanged snapshot surfaces no
		// extra/empty commit
		n := len(ch)
		time.Sleep(1500 * time.Millisecond)
		after, err := c.CommitHistory(pipe, "master")
		if err != nil {
			t.Fatalf("history after settle: %v", err)
		}
		if len(after) != n {
			t.Fatalf("commit count grew %d -> %d after settle; an empty cycle surfaced a commit", n, len(after))
		}
	})

	t.Run("arbitrary programs drive the spout", func(t *testing.T) {
		// clause 4: the mechanism is the output-directory watch, not a
		// shell convention — a python program writes the output.
		pipe := uniq(t)
		script := "import time\nfor i in range(1, 6):\n    open('/sandman/out/py%d' % i, 'w').write('py%d' % i)\n    time.sleep(0.3)\ntime.sleep(1)\n"
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "python:3-alpine", Cmd: []string{"python3", "-c", script}},
			Spout:     &client.Spout{},
		})
		waitSpoutCommits(t, pipe, 3)
		pollSpoutJobSettled(t, pipe)
		ch, err := c.CommitHistory(pipe, "master")
		if err != nil {
			t.Fatalf("history: %v", err)
		}
		head := ch[len(ch)-1]
		for i := 1; i <= 5; i++ {
			want := "py" + itoa(i)
			b, err := c.GetFile(head.ID, "py"+itoa(i))
			if err != nil || string(b) != want {
				t.Fatalf("py%d = %q, err %v; want %q", i, b, err, want)
			}
		}
		for _, cm := range ch {
			fs, err := c.ListFiles(cm.ID)
			if err != nil {
				t.Fatalf("list %s: %v", cm.ID, err)
			}
			if len(fs) == 0 {
				t.Fatalf("commit %s is empty", cm.ID)
			}
		}
	})

	t.Run("input and marker validation", func(t *testing.T) {
		repo := uniq(t)
		mustRepo(t, repo)
		bad := client.Pipeline{
			Name:      uniq(t),
			Transform: &client.Transform{Image: "alpine"},
			Input:     &client.Input{Repo: repo, Glob: "/*"},
			Spout:     &client.Spout{},
		}
		if err := c.CreatePipeline(bad); err == nil {
			t.Fatalf("a spout with an input must be rejected")
		} else if !containsStr(err.Error(), "cannot have inputs") {
			t.Fatalf("spout-with-input error = %q", err.Error())
		}
		badMarker := client.Pipeline{
			Name:      uniq(t),
			Transform: &client.Transform{Image: "alpine"},
			Spout:     &client.Spout{Marker: "bad*name"},
		}
		if err := c.CreatePipeline(badMarker); err == nil {
			t.Fatalf("a spout with a glob-metacharacter marker must be rejected")
		} else if !containsStr(err.Error(), "marker") {
			t.Fatalf("marker validation error = %q", err.Error())
		}
	})
}
func TestSB140_SpoutEpochsAndMarker(t *testing.T) {
	withContainerDaemon(t)
	t.Run("provenance epochs across updates", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: spoutCmd(50, 6, false)},
			Spout:     &client.Spout{},
		})
		ch := waitSpoutCommits(t, pipe, 3)
		s1 := ch[0].Provenance
		if len(s1) != 1 {
			t.Fatalf("epoch provenance = %v, want one spec commit", s1)
		}
		for _, cm := range ch {
			if len(cm.Provenance) != 1 || cm.Provenance[0] != s1[0] {
				t.Fatalf("commit %s provenance = %v, want %v (one epoch)", cm.ID, cm.Provenance, s1)
			}
		}
		// a plain update starts a new epoch: the new spec commit anchors
		// the new commits' provenance
		updateSpout(t, pipe, &client.Transform{Image: "alpine", Cmd: spoutCmd(50, 6, false)}, &client.Spout{}, false)
		ch2 := waitSpoutCommits(t, pipe, 6)
		next := ch2[3:] // the commits after the update
		if len(next) != 3 {
			t.Fatalf("expected 3 post-update commits, got %d", len(next))
		}
		s2 := next[0].Provenance
		if len(s2) != 1 || s2[0] == s1[0] {
			t.Fatalf("new epoch provenance = %v, want a fresh spec commit != %v", s2, s1[0])
		}
		for _, cm := range next {
			if len(cm.Provenance) != 1 || cm.Provenance[0] != s2[0] {
				t.Fatalf("post-update commit %s provenance = %v, want %v", cm.ID, cm.Provenance, s2)
			}
		}
	})

	t.Run("marker persists across plain update, resets on reprocess", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: markerCmd(2)},
			Spout:     &client.Spout{Marker: "markers"},
		})
		epoch1 := markerHeadContent(t, pipe, 2)
		if strings.Count(epoch1, "\n") != 2 {
			t.Fatalf("first epoch marker = %q, want two lines", epoch1)
		}
		// a plain update restarts the spout but keeps the marker state:
		// the marker continues accumulating from its previous content
		updateSpout(t, pipe, &client.Transform{Image: "alpine", Cmd: markerCmd(2)}, &client.Spout{Marker: "markers"}, false)
		acc := markerHeadContent(t, pipe, 4)
		if strings.Count(acc, "\n") != 4 || !strings.HasPrefix(acc, epoch1) {
			t.Fatalf("marker after plain update = %q, want it to continue from %q", acc, epoch1)
		}
		// a reprocess update resets the marker state: the new epoch's
		// marker no longer reflects the previous content
		updateSpout(t, pipe, &client.Transform{Image: "alpine", Cmd: markerCmd(2)}, &client.Spout{Marker: "markers"}, true)
		ep2 := markerHeadContent(t, pipe, 6)
		if strings.Count(ep2, "\n") != 2 || strings.Contains(ep2, epoch1) {
			t.Fatalf("marker after reprocess update = %q, want fresh state without %q", ep2, epoch1)
		}
	})

	t.Run("downstream subvenance of the spec commit", func(t *testing.T) {
		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine", Cmd: spoutCmd(100, 5, false)},
			Spout:     &client.Spout{},
		})
		ch := waitSpoutCommits(t, pipe, 5)
		var spec string
		for _, cm := range ch {
			if len(cm.Provenance) != 1 {
				t.Fatalf("spout commit %s provenance = %v, want the spec commit", cm.ID, cm.Provenance)
			}
			spec = cm.Provenance[0]
		}
		down := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      down,
			Transform: &client.Transform{Image: "alpine"},
			Input:     &client.Input{Repo: pipe, Glob: "/*"},
		})
		head, err := c.HeadCommit(pipe, "master")
		if err != nil {
			t.Fatalf("spout head: %v", err)
		}
		jobs := flushOK(t, head.ID)
		var downJob client.Job
		for _, j := range jobs {
			if j.Pipeline == down {
				downJob = j
			}
		}
		if downJob.ID == "" {
			t.Fatalf("no downstream job for the spout's head")
		}
		downOut, err := c.InspectCommit(downJob.OutputCommit)
		if err != nil {
			t.Fatalf("inspect downstream commit: %v", err)
		}
		if !containsStrList(downOut.Provenance, head.ID) || !containsStrList(downOut.Provenance, spec) {
			t.Fatalf("downstream provenance = %v, want the spout commit %s and the spec %s", downOut.Provenance, head.ID, spec)
		}
		// the spec commit's subvenants are exactly the spout's output and
		// the downstream output
		sc, err := c.InspectCommit(spec)
		if err != nil {
			t.Fatalf("inspect spec commit: %v", err)
		}
		if !containsStrList(sc.Subvenants, head.ID) || !containsStrList(sc.Subvenants, downJob.OutputCommit) {
			t.Fatalf("spec commit subvenants = %v, want the spout output %s and the downstream output %s", sc.Subvenants, head.ID, downJob.OutputCommit)
		}
	})
}
func TestSB158_StandbyLifecycle(t *testing.T) {
	withContainerDaemon(t)
	// D-09: no degraded/crashing standby state in Sandman — partial
	// capacity surfaces as failure or crashed; the extracted contract is
	// the lifecycle: idle in standby, wake on input, rest after the work.
	repo := uniq(t) + "r"
	pipe := uniq(t) + "p"
	mustRepo(t, repo)
	mustPipeline(t, client.Pipeline{Name: pipe, Standby: true, Transform: standbyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	pollFor(t, "idle in standby", 30*time.Second, func() bool {
		return standbyState(t, pipe) == "standby"
	})

	// input activates: the running state is observable while the job runs
	cm := commitFiles(t, repo, "", map[string]string{"file": "foo\n"})
	pollFor(t, "running while the job runs", 30*time.Second, func() bool {
		return standbyState(t, pipe) == "running"
	})
	flushOK(t, cm.ID)
	pollFor(t, "resting in standby again", 30*time.Second, func() bool {
		return standbyState(t, pipe) == "standby"
	})

	// a provisioning failure degrades to crashed (SB-043), never to a
	// standby-resting state — the D-09 mapping of partial capacity
	bad := uniq(t) + "bad"
	mustPipeline(t, client.Pipeline{Name: bad, Standby: true,
		Transform: &client.Transform{Image: "INVALID_IMAGE_REF", Cmd: []string{"true"}},
		Input:     &client.Input{Repo: repo, Glob: "/*"}})
	cm2 := commitFiles(t, repo, "", map[string]string{"file": "bar\n"})
	_ = cm2
	pollFor(t, "pipeline "+bad+" crashed", 60*time.Second, func() bool {
		p, err := c.InspectPipeline(bad)
		return err == nil && p.State == "crashed"
	})
}
func TestSB167_PlacementLabels(t *testing.T) {
	withContainerDaemon(t)
	r := uniq(t)
	mustRepo(t, r)
	cm := commitFiles(t, r, "master", map[string]string{"file": "foo"})

	w := startWorker(t, "hostA", "gpu")
	waitHostRegistered(t, "hostA")
	defer func() { _ = w.cmd.Process.Kill() }()

	name := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      name,
		Input:     &client.Input{Repo: r, Glob: "/*"},
		Placement: "gpu",
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", "cp /sandman/in/" + r + "/file /sandman/out/file && echo $HOSTNAME > /sandman/out/host"},
		},
	})

	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want exactly one", len(jobs))
	}
	if jobs[0].OutputCommit == "" {
		t.Fatalf("job %s produced no output commit", jobs[0].ID)
	}
	b, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if string(b) != "foo" {
		t.Fatalf("output file = %q, want the input content %q", string(b), "foo")
	}
	h, err := c.GetFile(jobs[0].OutputCommit, "host")
	if err != nil {
		t.Fatalf("read host marker: %v", err)
	}
	if got := strings.TrimSpace(string(h)); got != "hostA" {
		t.Fatalf("host marker = %q, want %q — the datum did not run on the registered host", got, "hostA")
	}
	js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(js) != 1 || js[0].State != "success" {
		t.Fatalf("want exactly one successful job, got %d (state %q)", len(js), js[0].State)
	}
}
func TestSB169_UnplaceableRecovery(t *testing.T) {
	withContainerDaemon(t)
	r := uniq(t)
	mustRepo(t, r)
	name := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      name,
		Input:     &client.Input{Repo: r, Glob: "/*"},
		Placement: "offline",
		Transform: copyTransform(r),
	})
	cm := commitFiles(t, r, "master", map[string]string{"file": "foo"})

	// the job triggered by the commit cannot be placed: the pipeline's
	// inspected state must become the failed (crashed) state within a
	// bounded retry window — never a silent hang (SB-169 clause 1)
	pollFor(t, "pipeline crashed", 30*time.Second, func() bool {
		pi, err := c.InspectPipeline(name)
		return err == nil && pi.State == "crashed"
	})

	// a host bearing the label registers: the pending work re-places
	// automatically and the same job completes (SB-169 clause 2)
	w := startWorker(t, "hostB", "offline")
	waitHostRegistered(t, "hostB")
	defer func() { _ = w.cmd.Process.Kill() }()
	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush returned %d jobs, want exactly one", len(jobs))
	}
	b, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if string(b) != "foo" {
		t.Fatalf("output file = %q, want %q — the re-placed datum must produce the same result", string(b), "foo")
	}
	js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: name})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(js) != 1 || js[0].State != "success" {
		t.Fatalf("want exactly one successful job for the one input commit (no duplicates), got %d (state %q)", len(js), js[0].State)
	}
	// once placement became possible the pipeline is no longer crashed
	pi, err := c.InspectPipeline(name)
	if err != nil {
		t.Fatalf("inspect pipeline: %v", err)
	}
	if pi.State == "crashed" {
		t.Fatalf("pipeline still crashed after the host returned")
	}
}

// TestD01_StandbyIdlesWithZeroContainers — the D-01 scale-to-zero
// assertion: a standby pipeline with no pending work holds NO standing
// execution participants (docker ps shows zero sandman-* containers).
// The standby family asserts state transitions only; this pins the
// container count, the observable side of scale-to-zero.
func TestD01_StandbyIdlesWithZeroContainers(t *testing.T) {
	withContainerDaemon(t)
	// a fresh container daemon must start with a clean slate
	if n := sandmanContainerCount(); n != 0 {
		t.Fatalf("%d sandman-* containers before any work, want 0", n)
	}
	repo := uniq(t) + "r"
	pipe := uniq(t) + "p"
	mustRepo(t, repo)
	mustPipeline(t, client.Pipeline{Name: pipe, Standby: true, Transform: standbyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	pollFor(t, "idle in standby", 30*time.Second, func() bool {
		return standbyState(t, pipe) == "standby"
	})
	if n := sandmanContainerCount(); n != 0 {
		t.Fatalf("%d standing containers while idle in standby, want 0 (D-01)", n)
	}
	// a commit wakes it: a container exists while the job runs. The
	// container must start within the poll; a slow runner's docker
	// overhead has blown 30s, so the poll is generous.
	cm := commitFiles(t, repo, "", map[string]string{"file": "foo\n"})
	pollFor(t, "container while the job runs", 60*time.Second, func() bool {
		return sandmanContainerCount() > 0
	})
	flushOK(t, cm.ID)
	pollFor(t, "resting in standby with zero containers", 30*time.Second, func() bool {
		return standbyState(t, pipe) == "standby" && sandmanContainerCount() == 0
	})
}

// sandmanContainerCount counts RUNNING sandbox containers (docker ps, not
// -a: exited ones are removed by --rm; a standing execution participant
// is a running container).
func sandmanContainerCount() int {
	out, err := exec.Command("docker", "ps", "-q", "--filter", "name=sandman-").Output()
	if err != nil {
		return -1
	}
	return len(strings.Fields(string(out)))
}

// TestD15_UnsatisfiableResourcesAcceptedAndRecorded — D-15's
// accept-and-record contract: a provably-unsatisfiable declaration
// (memory beyond any host's RAM) is NOT a creation gate — the spec is
// accepted, the declared values are recorded, and the pipeline is not
// prevented from running. Enforcement is the worker runtime's: docker
// refuses to provision the over-large container, and the failure
// converges on the crashed state with a reason (SB-091's provisioning
// path) rather than a rejection or a hang.
func TestD15_UnsatisfiableResourcesAcceptedAndRecorded(t *testing.T) {
	withContainerDaemon(t)
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{"file": "x"})
	name := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: name,
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", "true"},
			ResourceLimits: &client.ResourceLimits{
				Memory: "1000000000000b", // 1TB: beyond any host's RAM
				CPU:    9999,             // beyond any core count
			},
		},
		Input: &client.Input{Repo: repo, Glob: "/*"},
	})
	// the declaration is recorded, not rejected
	p, err := c.InspectPipeline(name)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if p.Transform.ResourceLimits == nil || p.Transform.ResourceLimits.Memory != "1000000000000b" || p.Transform.ResourceLimits.CPU != 9999 {
		t.Fatalf("declared limits = %+v, want 1TB/9999 recorded", p.Transform.ResourceLimits)
	}
	// the pipeline is not prevented from running: the job attempt reaches
	// the runtime, docker refuses the over-large CPU range at exec (exit
	// 125), and the datum fails with the RUNTIME's reason — never a
	// create-time rejection. The pipeline stays operational.
	jobs, err := c.Flush(cm.ID, 60*time.Second)
	if err != nil {
		t.Fatalf("flush of the unsatisfiable-resource job: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("unsatisfiable-resource flush = %d jobs, want 1", len(jobs))
	}
	if jobs[0].State != "failure" {
		t.Fatalf("job state = %s, want failure (runtime rejected the CPU range)", jobs[0].State)
	}
	if jobs[0].Reason == "" || !strings.Contains(jobs[0].Reason, "docker") {
		t.Fatalf("job reason = %q, want the docker runtime's rejection named", jobs[0].Reason)
	}
	pi, err := c.InspectPipeline(name)
	if err != nil {
		t.Fatalf("re-inspect: %v", err)
	}
	if pi.State == "crashed" || pi.State == "failure" {
		t.Fatalf("pipeline state = %s, want operational (declaration is not a gate)", pi.State)
	}
}

// TestServiceLocalContainerReachable — a LOCAL service on the container
// backend (the production default runner) must be reachable through the
// external-port proxy. The matrix daemon runs -runner process (where the
// service process binds the host directly), so the container path was
// untested — and broken: without a -p publish the container sits on the
// docker bridge, unreachable at the loopback the proxy dials (reviewer
// finding, fixed by publishing 127.0.0.1:<internal>:<internal>).
func TestServiceLocalContainerReachable(t *testing.T) {
	withContainerDaemon(t)
	repo := uniq(t)
	mustRepo(t, repo)
	commitFiles(t, repo, "master", map[string]string{"file1": "foo"})
	ext := freePort()
	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "python:3-alpine",
			Cmd:   []string{"sh", "-c", "cd /sandman/in && exec python3 -m http.server 8001"},
		},
		Parallelism: &client.Parallelism{Constant: 1},
		Input:       &client.Input{Repo: repo, Glob: "/"},
		Service: &client.Service{
			InternalPort: 8001,
			ExternalPort: ext,
		},
	}
	mustPipeline(t, p)
	// a service job never completes: deleting the pipeline kills the job
	// (and its container) while the daemon is still alive — without this,
	// the leaked container's published internal port collides with the
	// next service test on the same dockerd
	t.Cleanup(func() { _ = c.DeletePipeline(p.Name, false, false) })
	if b := getUntil(t, fmt.Sprintf("http://127.0.0.1:%d/%s/file1", ext, repo), "foo"); b != "foo" {
		t.Fatalf("local container service = %q, want foo (the publish must reach the proxy's loopback dial)", b)
	}
}
