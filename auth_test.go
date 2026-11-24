package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func clearAuthEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SANDMAN_AUTH_USER", "")
	t.Setenv("SANDMAN_AUTH_PASSWORD", "")
	t.Setenv("SANDMAN_AUTH_PASSWORD_FILE", "")
}

func TestResolveAuthConfig(t *testing.T) {
	clearAuthEnv(t)

	t.Run("disabled", func(t *testing.T) {
		cfg, err := resolveAuthConfig("", "", "")
		if err != nil {
			t.Fatalf("resolve empty: %v", err)
		}
		if cfg.enabled() {
			t.Fatalf("empty config must disable auth")
		}
	})

	t.Run("user only rejected", func(t *testing.T) {
		_, err := resolveAuthConfig("alice", "", "")
		if err == nil {
			t.Fatal("partial config (user only) must be rejected")
		}
	})

	t.Run("password only rejected", func(t *testing.T) {
		_, err := resolveAuthConfig("", "hunter2-secret", "")
		if err == nil {
			t.Fatal("partial config (password only) must be rejected")
		}
		if strings.Contains(err.Error(), "hunter2-secret") {
			t.Fatalf("error leaks the password: %q", err)
		}
	})

	t.Run("password file", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "passwd")
		if err := os.WriteFile(p, []byte("s3cret-value\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := resolveAuthConfig("alice", "", p)
		if err != nil {
			t.Fatalf("resolve password file: %v", err)
		}
		if cfg.User != "alice" || cfg.Password != "s3cret-value" {
			t.Fatalf("resolved %+v, want alice/s3cret-value", cfg)
		}
		if !cfg.enabled() {
			t.Fatal("configured auth must be enabled")
		}
	})

	t.Run("password and file conflict", func(t *testing.T) {
		_, err := resolveAuthConfig("alice", "one", filepath.Join(t.TempDir(), "passwd"))
		if err == nil {
			t.Fatal("password plus password-file must be rejected as ambiguous")
		}
	})

	t.Run("missing password file", func(t *testing.T) {
		_, err := resolveAuthConfig("alice", "", filepath.Join(t.TempDir(), "absent"))
		if err == nil {
			t.Fatal("missing password file must be rejected")
		}
	})
}

func TestAuthFromEnvFallback(t *testing.T) {
	t.Setenv("SANDMAN_AUTH_USER", "env-user")
	t.Setenv("SANDMAN_AUTH_PASSWORD", "env-pass")
	t.Setenv("SANDMAN_AUTH_PASSWORD_FILE", "")
	cfg, err := authFromEnv("", "", "")
	if err != nil {
		t.Fatalf("authFromEnv: %v", err)
	}
	if cfg.User != "env-user" || cfg.Password != "env-pass" {
		t.Fatalf("env fallback = %+v, want env-user/env-pass", cfg)
	}

	// an explicit flag wins over the environment
	cfg, err = authFromEnv("flag-user", "flag-pass", "")
	if err != nil {
		t.Fatalf("authFromEnv flags: %v", err)
	}
	if cfg.User != "flag-user" || cfg.Password != "flag-pass" {
		t.Fatalf("flag precedence = %+v", cfg)
	}
}

func TestBasicAuthMiddleware(t *testing.T) {
	cfg := authConfig{User: "alice", Password: "s3cret"}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	h := basicAuth(cfg, next)

	cases := []struct {
		name       string
		user, pass string
		setHeader  bool
		header     string
		want       int
	}{
		{name: "valid", user: "alice", pass: "s3cret", setHeader: true, want: http.StatusOK},
		{name: "missing", want: http.StatusUnauthorized},
		{name: "wrong password", user: "alice", pass: "nope", setHeader: true, want: http.StatusUnauthorized},
		{name: "wrong user", user: "bob", pass: "s3cret", setHeader: true, want: http.StatusUnauthorized},
		{name: "malformed base64", setHeader: true, header: "Basic !!!", want: http.StatusUnauthorized},
		{name: "wrong scheme", setHeader: true, header: "Bearer abc", want: http.StatusUnauthorized},
		{name: "empty basic", setHeader: true, header: "Basic ", want: http.StatusUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			if tc.setHeader {
				if tc.header != "" {
					req.Header.Set("Authorization", tc.header)
				} else {
					req.SetBasicAuth(tc.user, tc.pass)
				}
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
			}
			if tc.want == http.StatusUnauthorized {
				if got := rec.Header().Get("WWW-Authenticate"); got != `Basic realm="sandman"` {
					t.Fatalf("WWW-Authenticate = %q", got)
				}
				var body map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"] != "unauthorized" {
					t.Fatalf("401 body = %q (err %v)", rec.Body.String(), err)
				}
				if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
					t.Fatalf("401 content type = %q", ct)
				}
			}
		})
	}
}

// TestAPIHandlerAuthCoverage pins that the middleware gates every HTTP
// surface the daemon serves — the JSON API, MCP, and the embedded
// dashboard/UI — and that authorized requests still reach the original
// handlers (including their JSON error shapes for non-auth failures).
func TestAPIHandlerAuthCoverage(t *testing.T) {
	d := &daemon{auth: authConfig{User: "alice", Password: "s3cret"}}
	h := d.apiHandler()

	unauthPaths := []string{
		"/api/v1/version",
		"/api/v1/repos",
		"/mcp/messages",
		"/mcp/sse",
		"/",
		"/ui/app.js",
		"/nope",
	}
	for _, p := range unauthPaths {
		req := httptest.NewRequest("GET", p, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: status = %d, want 401", p, rec.Code)
		}
		if got := rec.Header().Get("WWW-Authenticate"); got != `Basic realm="sandman"` {
			t.Fatalf("%s: WWW-Authenticate = %q", p, got)
		}
	}

	authorized := func(method, p string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, p, nil)
		req.SetBasicAuth("alice", "s3cret")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	if rec := authorized("GET", "/api/v1/version"); rec.Code != http.StatusOK {
		t.Fatalf("authorized version: status %d body %q", rec.Code, rec.Body.String())
	}
	if rec := authorized("GET", "/"); rec.Code != http.StatusOK || !strings.Contains(rec.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("authorized dashboard: status %d type %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	if rec := authorized("GET", "/ui/app.js"); rec.Code != http.StatusOK {
		t.Fatalf("authorized asset: status %d", rec.Code)
	}
	// MCP reached its handler; missing sessionId is its own 400.
	if rec := authorized("POST", "/mcp/messages"); rec.Code != http.StatusBadRequest {
		t.Fatalf("authorized MCP: status %d body %q", rec.Code, rec.Body.String())
	}
	// unknown paths keep the uniform JSON 404 once authenticated
	rec := authorized("GET", "/nope")
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), `"error"`) {
		t.Fatalf("authorized unknown path: status %d body %q", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("authorized unknown path content type = %q", ct)
	}
}

// TestAPIHandlerAuthDisabled pins the default-off contract: no auth
// configuration means the API behaves exactly as before.
func TestAPIHandlerAuthDisabled(t *testing.T) {
	d := &daemon{}
	h := d.apiHandler()

	req := httptest.NewRequest("GET", "/api/v1/version", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("disabled auth version: status %d", rec.Code)
	}
	if got := rec.Header().Get("WWW-Authenticate"); got != "" {
		t.Fatalf("disabled auth sent WWW-Authenticate %q", got)
	}

	req = httptest.NewRequest("GET", "/nope", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("disabled auth unknown path: status %d", rec.Code)
	}
}
