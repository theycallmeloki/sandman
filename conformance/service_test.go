// Service pipelines (SB-100/168): one long-lived process serving the
// pipeline's input over HTTP, reachable on the control-plane host and
// proxied by the control plane's API.
package conformance

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// TestSB100_ServicePipeline serves the input over HTTP — the long-running
// process (clause 1), the external endpoint (clause 2), the control-plane
// proxy (clause 3), the annotation passthrough (clause 4), and the live
// refresh when a new revision lands (clause 5). Port 31800 is the
// external port of the reference test's service declaration.
func TestSB100_ServicePipeline(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm1 := commitFiles(t, repo, "master", map[string]string{"file1": "foo"})

	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", "cd /sandman/in && exec python3 -m http.server 8000"},
		},
		Parallelism: &client.Parallelism{Constant: 1},
		Input:       &client.Input{Repo: repo, Glob: "/"},
		Service: &client.Service{
			InternalPort: 8000,
			ExternalPort: 31800,
			Annotations:  map[string]string{"foo": "bar"},
		},
	}
	mustPipeline(t, p)
	t.Cleanup(func() { _ = c.DeletePipeline(p.Name, false, false) })

	// the external endpoint converges as the process comes up — the
	// contract is eventual reachability, so probe until it serves
	svcURL := "http://127.0.0.1:31800/" + repo + "/file1"
	if b := getUntil(t, svcURL, "foo"); b != "foo" {
		t.Fatalf("direct endpoint = %q, want %q", b, "foo")
	}

	// the control-plane proxy returns the same content as the direct
	// endpoint (clause 3)
	proxied, err := c.ServiceProxy(p.Name, repo+"/file1")
	if err != nil {
		t.Fatalf("service proxy: %v", err)
	}
	if string(proxied) != "foo" {
		t.Fatalf("service proxy returned %q, want %q", string(proxied), "foo")
	}

	// the endpoint's annotations are the user's own plus the system's
	// identifying pipelineName annotation (clause 4)
	info, err := c.InspectService(p.Name)
	if err != nil {
		t.Fatalf("inspect service: %v", err)
	}
	if info.Internal != 8000 || info.External != 31800 {
		t.Fatalf("service ports = %d/%d, want 8000/31800", info.Internal, info.External)
	}
	actual := map[string]string{}
	for k, v := range info.Annotations {
		if k != "pipelineName" {
			actual[k] = v
		}
	}
	if !reflect.DeepEqual(actual, map[string]string{"foo": "bar"}) {
		t.Fatalf("service annotations = %v, want user {foo: bar} plus system pipelineName", info.Annotations)
	}

	// a new revision is served through the same endpoint with no restart
	// (clause 5); the file2 lookup retries while the refresh lands
	commitFiles(t, repo, "master", map[string]string{"file2": "bar"})
	svcURL2 := "http://127.0.0.1:31800/" + repo + "/file2"
	getUntil(t, svcURL2, "bar")

	// and the first file still serves the new revision's state (the view
	// replaced, not appended)
	if b := getUntil(t, svcURL, "foo"); b != "foo" {
		t.Fatalf("file1 after refresh = %q, want %q", b, "foo")
	}
	_ = cm1
}

// getUntil polls an HTTP URL until it returns 200 with the expected body —
// the reference's reachability probe: non-200 and connection failures are
// transient while the service converges.
func getUntil(t *testing.T, url, want string) string {
	t.Helper()
	var last string
	pollFor(t, "GET "+url+" returns "+want, 60*time.Second, func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return false
		}
		if resp.StatusCode != 200 {
			last = fmt.Sprintf("status %d", resp.StatusCode)
			return false
		}
		last = string(b)
		return string(b) == want
	})
	return last
}

// TestSB100_RejectsBadServiceSpecs — the declaration rules: ports are
// required, one process only, and the external port is exclusive to one
// service.
func TestSB100_RejectsBadServiceSpecs(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	mk := func(svc *client.Service) client.Pipeline {
		return client.Pipeline{
			Name: uniq(t),
			Transform: &client.Transform{Image: "alpine",
				Cmd: []string{"sh", "-c", "cd /sandman/in && exec python3 -m http.server 8000"}},
			Parallelism: &client.Parallelism{Constant: 1},
			Input:       &client.Input{Repo: repo, Glob: "/"},
			Service:     svc,
		}
	}
	cases := []struct {
		name    string
		p       client.Pipeline
		wantErr string
	}{
		{"no ports", mk(&client.Service{InternalPort: 0, ExternalPort: 31810}), "internal and external ports"},
		{"too parallel", func() client.Pipeline {
			p := mk(&client.Service{InternalPort: 8000, ExternalPort: 31811})
			p.Parallelism = &client.Parallelism{Constant: 2}
			return p
		}(), "parallelism 1"},
		{"spout conflict", func() client.Pipeline {
			p := mk(&client.Service{InternalPort: 8000, ExternalPort: 31812})
			p.Spout = &client.Spout{}
			return p
		}(), "both a service and a spout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := c.CreatePipeline(tc.p)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("create err = %v, want %q", err, tc.wantErr)
			}
		})
	}

	// a second service may not claim the same external port
	p1 := mk(&client.Service{InternalPort: 8000, ExternalPort: 31813})
	if err := c.CreatePipeline(p1); err != nil {
		t.Fatalf("create first service: %v", err)
	}
	t.Cleanup(func() { _ = c.DeletePipeline(p1.Name, false, false) })
	p2 := mk(&client.Service{InternalPort: 8001, ExternalPort: 31813})
	err := c.CreatePipeline(p2)
	if err == nil || !strings.Contains(err.Error(), "already declared") {
		t.Fatalf("second service err = %v, want external-port conflict", err)
	}
}

// TestSB168_RemoteServiceReachable — a placed service is reachable at the
// control-plane host's external port even though the process runs on the
// execution host: the control plane forwards the external port to the
// worker's internal port (SB-168). The worker is a sandman worker
// process bearing the placement label; docker runs the service container.
func TestSB168_RemoteServiceReachable(t *testing.T) {
	// FIXED (api batch 60): the hang was a missing half-close propagation
	// in proxyListener (service.go) — the relay's wg.Wait() coupled both
	// copy directions, so a close-delimited (HTTP/1.0) service response
	// never propagated its close to the keep-alive client and Go's
	// transport blocked forever. See
	// implementation-review/SB168_D22_ISSUE.md (reviewer verdict).
	if !dockerAvailable() {
		t.Skip("docker not available")
	}
	repo := uniq(t)
	mustRepo(t, repo)
	commitFiles(t, repo, "master", map[string]string{"file1": "foo"})

	label := "svc-" + uniq(t)
	workerPort := freePort()
	workerName := "w-" + uniq(t)
	cmd := exec.Command(binPath, "worker", "-name", workerName, "-control", c.Base(),
		"-port", strconv.Itoa(workerPort), "-label", label)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", "apk add --no-cache python3 >/dev/null 2>&1; cd /sandman/in && exec python3 -m http.server 8001"},
		},
		Parallelism: &client.Parallelism{Constant: 1},
		Input:       &client.Input{Repo: repo, Glob: "/"},
		Placement:   label,
		Service: &client.Service{
			InternalPort: 8001,
			ExternalPort: 31801,
		},
	}
	mustPipeline(t, p)
	t.Cleanup(func() { _ = c.DeletePipeline(p.Name, false, false) })

	// reachable at the control-plane host's external port, converged via
	// the retry loop (SB-168 clause 4)
	svcURL := "http://127.0.0.1:31801/" + repo + "/file1"
	getUntil(t, svcURL, "foo")

	// a new revision is forwarded through the same path without a
	// redeploy (SB-100 clause 5 applied to the placed service)
	commitFiles(t, repo, "master", map[string]string{"file2": "bar"})
	getUntil(t, "http://127.0.0.1:31801/"+repo+"/file2", "bar")
}
