package client

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
