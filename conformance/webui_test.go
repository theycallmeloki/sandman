package conformance

// The embedded read-only dashboard: index at /, static assets under
// /ui/, and the API on the same port untouched — every /api/v1/... path
// keeps working and unknown paths keep the uniform JSON 404.

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestWebUIDashboard(t *testing.T) {
	get := func(path string) (*http.Response, string) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d%s", daemonPort, path))
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, string(b)
	}

	// the root serves the dashboard
	resp, body := get("/")
	if resp.StatusCode != 200 {
		t.Fatalf("GET / status %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("GET / content-type %q, want text/html", ct)
	}
	if !strings.Contains(body, `<div id="app">`) {
		t.Fatalf("GET / body lacks the dashboard mount point")
	}

	// assets resolve with the right content types
	for _, p := range []string{
		"/ui/app.js",
		"/ui/shared.js",
		"/ui/style.css",
		"/ui/views/Overview.js",
	} {
		resp, body = get(p)
		if resp.StatusCode != 200 {
			t.Fatalf("GET %s status %d, want 200", p, resp.StatusCode)
		}
		if len(body) == 0 {
			t.Fatalf("GET %s empty body", p)
		}
	}
	if resp, _ = get("/ui/app.js"); !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/javascript") {
		t.Fatalf("app.js content-type %q, want text/javascript", resp.Header.Get("Content-Type"))
	}

	// the API on the same port is untouched
	resp, body = get("/api/v1/version")
	if resp.StatusCode != 200 || !strings.Contains(body, `"version"`) {
		t.Fatalf("GET /api/v1/version: %d %s", resp.StatusCode, body)
	}
	resp, body = get("/api/v1/no-such-endpoint")
	if resp.StatusCode != 404 || !strings.Contains(body, `"error"`) {
		t.Fatalf("unknown endpoint: %d %s, want JSON 404", resp.StatusCode, body)
	}
	resp, body = get("/no-such-page")
	if resp.StatusCode != 404 || !strings.Contains(body, `"error"`) {
		t.Fatalf("unknown GET path: %d %s, want JSON 404 (the dashboard owns only / and /ui/)", resp.StatusCode, body)
	}
	resp, body = get("/ui/nope.js")
	if resp.StatusCode != 404 || !strings.Contains(body, `"error"`) {
		t.Fatalf("missing asset: %d %s, want JSON 404", resp.StatusCode, body)
	}

	// and a write still works through the client
	repo := uniq(t)
	mustRepo(t, repo)
	if cm := commitFiles(t, repo, "master", map[string]string{"f": "x"}); cm.ID == "" {
		t.Fatalf("commit produced no id")
	}
}
