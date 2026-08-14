package client

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestBackupRejectsTruncatedGzip pins the H3 invariant: a server that
// fails mid-stream (returns without closing its gzip writer, so the
// stream ends with no trailer) must surface as an error — never as a
// nil return over a corrupt archive. backupH has exactly this shape:
// tw/gz are only closed on the success path.
func TestBackupRejectsTruncatedGzip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		gz := gzip.NewWriter(w)
		tw := tar.NewWriter(gz)
		if err := tw.WriteHeader(&tar.Header{Name: "a", Mode: 0o644, Size: 5}); err != nil {
			t.Error(err)
			return
		}
		if _, err := tw.Write([]byte("hello")); err != nil {
			t.Error(err)
			return
		}
		if err := gz.Flush(); err != nil { // push the partial stream out, then die without a trailer
			t.Error(err)
			return
		}
	}))
	defer srv.Close()

	c := New(strings.TrimPrefix(srv.URL, "http://"))
	var buf bytes.Buffer
	if err := c.Backup(&buf); err == nil {
		t.Fatal("Backup returned nil over a truncated gzip stream")
	}
}

// TestBackupSucceedsOnCompleteGzip: a server that closes its writers
// (trailer present) must yield a complete, readable tar.gz — the
// trailer validation must not false-positive on valid archives.
func TestBackupSucceedsOnCompleteGzip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		gz := gzip.NewWriter(w)
		tw := tar.NewWriter(gz)
		if err := tw.WriteHeader(&tar.Header{Name: "a", Mode: 0o644, Size: 5}); err != nil {
			t.Error(err)
			return
		}
		if _, err := tw.Write([]byte("hello")); err != nil {
			t.Error(err)
			return
		}
		if err := tw.Close(); err != nil {
			t.Error(err)
			return
		}
		if err := gz.Close(); err != nil {
			t.Error(err)
			return
		}
	}))
	defer srv.Close()

	c := New(strings.TrimPrefix(srv.URL, "http://"))
	var buf bytes.Buffer
	if err := c.Backup(&buf); err != nil {
		t.Fatalf("Backup over a complete archive: %v", err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("output is not gzip: %v", err)
	}
	tr := tar.NewReader(gz)
	hdr, err := tr.Next()
	if err != nil || hdr.Name != "a" {
		t.Fatalf("first entry: %v %v", hdr, err)
	}
	b, err := io.ReadAll(tr)
	if err != nil || string(b) != "hello" {
		t.Fatalf("entry content: %q %v", b, err)
	}
	if _, err := tr.Next(); err != io.EOF {
		t.Fatalf("want clean tar EOF, got %v", err)
	}
}

// M10-batch B: the raw-GET caps, the streaming FetchFileTo, the per-call
// timeout override, and CommitHistory's cycle guard.

func TestReadCappedLimit(t *testing.T) {
	if _, err := readCappedLimit(strings.NewReader("hello"), 10); err != nil {
		t.Fatalf("within limit: %v", err)
	}
	if _, err := readCappedLimit(strings.NewReader("hello world"), 10); err == nil {
		t.Fatalf("over limit: expected an error, got nil")
	}
}

func TestCopyCapped(t *testing.T) {
	var buf bytes.Buffer
	if err := copyCapped(&buf, strings.NewReader("hello"), 10); err != nil || buf.String() != "hello" {
		t.Fatalf("within limit: %q %v", buf.String(), err)
	}
	buf.Reset()
	if err := copyCapped(&buf, strings.NewReader("hello world"), 10); err == nil {
		t.Fatalf("over limit: expected an error, got nil")
	}
}

// TestFetchFileToStreamsAndOverridesTimeout pins the M10-batch B client
// IO: FetchFileTo streams the body to the writer without buffering it in
// the FileFetch (Data stays nil), and a per-call timeout override can be
// shorter than the client's default 60s without touching other calls.
func TestFetchFileToStreamsAndOverridesTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/commits/c1/files/big":
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(200)
			// 4 MiB of zeros: bigger than any realistic buffer heuristic
			zeros := make([]byte, 4<<20)
			for i := range zeros {
				zeros[i] = byte(i % 251)
			}
			_, _ = w.Write(zeros)
		case "/api/v1/commits/c1/files/slow":
			// the override timeout must abort mid-transfer, not hang
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := New(strings.TrimPrefix(srv.URL, "http://"))

	var buf bytes.Buffer
	f, err := c.FetchFileTo(&buf, "c1", "big", false, 0)
	if err != nil {
		t.Fatalf("FetchFileTo: %v", err)
	}
	if f.ContentType != "application/octet-stream" {
		t.Fatalf("content type: %q", f.ContentType)
	}
	if len(buf.Bytes()) != 4<<20 {
		t.Fatalf("streamed %d bytes, want %d", len(buf.Bytes()), 4<<20)
	}
	if f.Data != nil {
		t.Fatalf("streaming variant must not buffer into Data (len %d)", len(f.Data))
	}

	// the override: a 100ms budget on a server that never responds must
	// fail the call (the shared 60s would hang it instead)
	start := time.Now()
	if _, err := c.FetchFileTo(io.Discard, "c1", "slow", false, 100*time.Millisecond); err == nil {
		t.Fatalf("override timeout: expected an error")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("override timeout took %v, want ~100ms", elapsed)
	}
}

// TestCommitHistoryCycleGuard pins the cycle guard: a corrupt parent
// chain (a record whose parent points back into the visited set) must
// stop the walk with an error, not spin forever.
func TestCommitHistoryCycleGuard(t *testing.T) {
	commits := map[string]Commit{
		"a": {ID: "a", Repo: "r", Branch: "master", ParentID: "b"},
		"b": {ID: "b", Repo: "r", Branch: "master", ParentID: "a"}, // cycle
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/repos/r/branches/master/head":
			writeTestJSON(w, commits["a"])
		case strings.HasPrefix(r.URL.Path, "/api/v1/commits/"):
			id := strings.TrimPrefix(r.URL.Path, "/api/v1/commits/")
			cm, ok := commits[id]
			if !ok {
				http.NotFound(w, r)
				return
			}
			writeTestJSON(w, cm)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := New(strings.TrimPrefix(srv.URL, "http://"))

	done := make(chan error, 1)
	go func() {
		_, err := c.CommitHistory("r", "master")
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("cycle: expected an error, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("cycle: CommitHistory did not terminate")
	}
}

func writeTestJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
