package client

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// TestBasicAuthPropagation pins that WithBasicAuth reaches every request
// shape the client emits: the JSON verbs, the raw byte verbs, the
// streaming uploads, the file stream, the log follow, and the backup
// stream (which uses its own HTTP client).
func TestBasicAuthPropagation(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]string{} // path -> "user:pass" or "<none>"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		mu.Lock()
		if ok {
			seen[r.URL.Path] = user + ":" + pass
		} else {
			seen[r.URL.Path] = "<none>"
		}
		mu.Unlock()
		if !ok || user != "alice" || pass != "s3cret" {
			w.Header().Set("WWW-Authenticate", `Basic realm="sandman"`)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		switch {
		case r.URL.Path == "/api/v1/version":
			_, _ = w.Write([]byte(`{"version":"test"}`))
		case r.URL.Path == "/api/v1/backup":
			gz := gzip.NewWriter(w)
			tw := tar.NewWriter(gz)
			_ = tw.Close()
			_ = gz.Close()
		case r.URL.Path == "/api/v1/logs":
			_, _ = w.Write([]byte(`{"line":"hello"}` + "\n"))
		case strings.Contains(r.URL.Path, "/files/"):
			if r.Method == "GET" {
				w.Header().Set("Content-Type", "text/plain")
				_, _ = w.Write([]byte("file-content"))
				return
			}
			_, _ = io.Copy(io.Discard, r.Body)
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer srv.Close()

	c := New(strings.TrimPrefix(srv.URL, "http://"), WithBasicAuth("alice", "s3cret"))

	if v, err := c.Version(); err != nil || v != "test" {
		t.Fatalf("Version = %q, %v", v, err)
	}
	if err := c.PutFile("cmt", "a.txt", []byte("x")); err != nil {
		t.Fatalf("PutFile: %v", err)
	}
	if b, err := c.GetFile("cmt", "a.txt"); err != nil || string(b) != "file-content" {
		t.Fatalf("GetFile = %q, %v", b, err)
	}
	if err := c.PutFileStream("cmt", "stream.txt", strings.NewReader("x")); err != nil {
		t.Fatalf("PutFileStream: %v", err)
	}
	if err := c.PutFileAppendStream("cmt", "append.txt", strings.NewReader("x")); err != nil {
		t.Fatalf("PutFileAppendStream: %v", err)
	}
	var buf bytes.Buffer
	if err := c.Backup(&buf); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	var fetch bytes.Buffer
	if _, err := c.FetchFileTo(&fetch, "cmt", "a.txt", false, 0); err != nil {
		t.Fatalf("FetchFileTo: %v", err)
	}
	rc, err := c.FollowLogs(LogParams{})
	if err != nil {
		t.Fatalf("FollowLogs: %v", err)
	}
	_, _ = io.Copy(io.Discard, rc)
	_ = rc.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("server saw no requests")
	}
	for p, got := range seen {
		if got != "alice:s3cret" {
			t.Fatalf("%s: Authorization = %q, want alice:s3cret", p, got)
		}
	}
}

func TestClientWithoutAuthIsRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, ok := r.BasicAuth(); !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="sandman"`)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		_, _ = w.Write([]byte(`{"version":"test"}`))
	}))
	defer srv.Close()

	c := New(strings.TrimPrefix(srv.URL, "http://"))
	_, err := c.Version()
	if err == nil {
		t.Fatal("Version without credentials must fail")
	}
	apiErr, ok := err.(*Error)
	if !ok || apiErr.Status != http.StatusUnauthorized {
		t.Fatalf("error = %v, want a 401 *Error", err)
	}

	// An empty option pair must not send an Authorization header either.
	c = New(strings.TrimPrefix(srv.URL, "http://"), WithBasicAuth("", ""))
	if _, err := c.Version(); err == nil {
		t.Fatal("empty credentials must not authenticate")
	}
}
