package conformance

// Focused HTTP Basic Auth coverage against a real daemon process: the
// API gate (client and raw HTTP), the MCP transport, the embedded
// dashboard, and startup rejection of a partial credential pair.
//
// These tests own their daemons (the shared harness daemon is
// unauthenticated) and never touch container jobs, so they are cheap.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"sandman/client"
)

func startAuthDaemon(t *testing.T, statesuffix string, args ...string) string {
	t.Helper()
	state := filepath.Join(testTempBase(), "sandman-auth-"+statesuffix+"-"+uniq(t))
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	port := freePort()
	full := append([]string{
		"daemon", "-name", daemonName + "-auth", "-port", strconv.Itoa(port),
		"-state", state, "-runner", "process",
	}, args...)
	cmd := exec.Command(binPath, full...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start auth daemon: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(testTimeout(20 * time.Second)):
			_ = cmd.Process.Kill()
			<-done
		}
		os.RemoveAll(state)
	})
	if !waitPort(port, 15*time.Second) {
		_ = cmd.Process.Kill()
		t.Fatalf("auth daemon did not come up")
	}
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func TestBasicAuthGate(t *testing.T) {
	addr := startAuthDaemon(t, "gate", "-auth-user", "confuser", "-auth-password", "conf-secret")
	base := "http://" + addr

	unauth := client.New(addr)
	if _, err := unauth.Version(); err == nil {
		t.Fatal("API answered without credentials")
	} else if apiErr, ok := err.(*client.Error); !ok || apiErr.Status != http.StatusUnauthorized {
		t.Fatalf("unauthenticated API error = %v, want 401", err)
	}

	wrong := client.New(addr, client.WithBasicAuth("confuser", "wrong"))
	if _, err := wrong.Version(); err == nil {
		t.Fatal("API answered with the wrong password")
	} else if apiErr, ok := err.(*client.Error); !ok || apiErr.Status != http.StatusUnauthorized {
		t.Fatalf("wrong-password API error = %v, want 401", err)
	}

	authed := client.New(addr, client.WithBasicAuth("confuser", "conf-secret"))
	if v, err := authed.Version(); err != nil || v == "" {
		t.Fatalf("authenticated Version = %q, %v", v, err)
	}

	// The dashboard and MCP transport are on the same authenticated surface.
	for _, p := range []string{"/", "/mcp/messages", "/api/v1/repos", "/nope"} {
		req, err := http.NewRequest(http.MethodGet, base+p, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s: status %d body %q, want 401", p, resp.StatusCode, body)
		}
		if got := resp.Header.Get("WWW-Authenticate"); got != `Basic realm="sandman"` {
			t.Fatalf("%s: WWW-Authenticate = %q", p, got)
		}
		if !strings.Contains(string(body), `"error"`) {
			t.Fatalf("%s: 401 body %q is not JSON", p, body)
		}
	}

	// A malformed Authorization header is rejected too.
	req, err := http.NewRequest(http.MethodGet, base+"/api/v1/version", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Basic !!!")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("malformed Authorization: status %d, want 401", resp.StatusCode)
	}

	// With credentials the dashboard renders and MCP reaches its handler
	// (missing sessionId is the MCP handler's own 400).
	authReq := func(method, p string) *http.Response {
		req, err := http.NewRequest(method, base+p, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.SetBasicAuth("confuser", "conf-secret")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	resp = authReq(http.MethodGet, "/")
	html, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(html), "<html") {
		t.Fatalf("authenticated dashboard: status %d", resp.StatusCode)
	}
	resp = authReq(http.MethodPost, "/mcp/messages")
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("authenticated MCP: status %d, want the handler's 400", resp.StatusCode)
	}
}

func TestAuthPartialConfigRejected(t *testing.T) {
	state := t.TempDir()
	cases := []struct {
		name string
		args []string
		leak string
	}{
		{name: "user only", args: []string{"-auth-user", "alice"}},
		{name: "password only", args: []string{"-auth-password", "leak-me-not"}, leak: "leak-me-not"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout(10*time.Second))
			defer cancel()
			args := append([]string{
				"daemon", "-state", state, "-port", strconv.Itoa(freePort()), "-runner", "process",
			}, tc.args...)
			out, err := exec.CommandContext(ctx, binPath, args...).CombinedOutput()
			if err == nil {
				t.Fatalf("daemon started with a partial auth config: %s", out)
			}
			if tc.leak != "" && strings.Contains(string(out), tc.leak) {
				t.Fatalf("startup output leaks the password: %s", out)
			}
			if !strings.Contains(strings.ToLower(string(out)), "auth") {
				t.Fatalf("startup error does not explain the auth config: %s", out)
			}
		})
	}
}
