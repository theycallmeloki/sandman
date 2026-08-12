package main

// Unit tests for the self-update path (update.go): version comparison,
// checksum verification, release/asset parsing, and the end-to-end
// install against an httptest server. The install target is a temp dir —
// updatePath (/usr/local/bin/sandman) is never touched.

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCmpVersions(t *testing.T) {
	cases := []struct {
		a, b string
		want int // sign
	}{
		{"0.0.1", "0.0.1", 0},
		{"v0.0.1", "0.0.1", 0},
		{"1.0.0", "0.9.9", 1},
		{"0.1.0", "0.1.1", -1},
		{"0.1.2", "0.1.10", -1},
		{"2.0.0", "10.0.0", -1},
		{"1.2.3", "1.2.4", -1},
		{"1.2.4", "1.2.3", 1},
		// unparseable sides sort before parseable ones (dev < release)
		{"dev", "0.0.1", -1},
		{"0.0.1", "dev", 1},
		{"1.2", "0.0.1", -1},
		{"", "0.0.1", -1},
		{"a.b.c", "1.2.3", -1},
		// both unparseable compare equal
		{"dev", "dev", 0},
		{"1.2", "a.b", 0},
	}
	for _, c := range cases {
		got := cmpVersions(c.a, c.b)
		if (got < 0) != (c.want < 0) || (got > 0) != (c.want > 0) || (got == 0) != (c.want == 0) {
			t.Errorf("cmpVersions(%q, %q) = %d, want sign %d", c.a, c.b, got, c.want)
		}
	}
}

func TestSplitVersion(t *testing.T) {
	if m, n, p, err := splitVersion("v1.2.3"); err != nil || m != 1 || n != 2 || p != 3 {
		t.Fatalf("splitVersion(v1.2.3) = %d.%d.%d, %v", m, n, p, err)
	}
	if _, _, _, err := splitVersion("1.2"); err == nil {
		t.Fatalf("splitVersion(1.2): want error")
	}
	if _, _, _, err := splitVersion("a.b.c"); err == nil {
		t.Fatalf("splitVersion(a.b.c): want error")
	}
	if _, _, _, err := splitVersion("1.2.3.4"); err == nil {
		t.Fatalf("splitVersion(1.2.3.4): want error")
	}
	if _, _, _, err := splitVersion(""); err == nil {
		t.Fatalf("splitVersion(empty): want error")
	}
}

func TestValidVersion(t *testing.T) {
	for _, ok := range []string{"0.0.1", "v1.2.3", "10.0.0"} {
		if !validVersion(ok) {
			t.Errorf("validVersion(%q): want true", ok)
		}
	}
	for _, bad := range []string{"", "dev", "1.2", "1.2.x", "1.2.3.4"} {
		if validVersion(bad) {
			t.Errorf("validVersion(%q): want false", bad)
		}
	}
}

func TestReleaseAsset(t *testing.T) {
	rel := &ghRelease{Assets: []ghAsset{
		{Name: "sandman-linux-amd64", BrowserDownloadURL: "https://x/bin"},
		{Name: "sandman-linux-amd64.sha256", BrowserDownloadURL: "https://x/sha"},
		{Name: "sandman-linux-arm64", BrowserDownloadURL: "https://x/arm"},
	}}
	if got := releaseAsset(rel, "linux", "amd64"); got != "https://x/bin" {
		t.Errorf("linux/amd64 = %q", got)
	}
	if got := releaseAsset(rel, "linux", "amd64.sha256"); got != "https://x/sha" {
		t.Errorf("checksum asset = %q", got)
	}
	if got := releaseAsset(rel, "darwin", "arm64"); got != "" {
		t.Errorf("darwin/arm64 = %q, want empty", got)
	}
	if got := releaseAsset(rel, "linux", "amd64x"); got != "" {
		t.Errorf("suffix-clash = %q, want empty (exact match only)", got)
	}
}

func TestFetchChecksum(t *testing.T) {
	good := "abc123" + strings.Repeat("0", 58) // 64 hex chars
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/good":
			fmt.Fprintf(w, "%s  sandman-linux-amd64\n", good)
		case "/multi":
			fmt.Fprintf(w, "%s  sandman-linux-amd64\n%s  other\n", good, good)
		case "/empty":
			_, _ = w.Write([]byte("\n"))
		case "/short":
			_, _ = w.Write([]byte("abc\n"))
		case "/nothex":
			_, _ = w.Write([]byte("zzzz" + strings.Repeat("0", 60) + "\n"))
		case "/notfound":
			http.NotFound(w, r)
		default:
			http.Error(w, "nope", http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	if sum, err := fetchChecksum(ts.URL + "/good"); err != nil || hex.EncodeToString(sum) != good {
		t.Fatalf("good: sum=%x err=%v", sum, err)
	}
	if sum, err := fetchChecksum(ts.URL + "/multi"); err != nil || hex.EncodeToString(sum) != good {
		t.Fatalf("multi-field: sum=%x err=%v", sum, err)
	}
	if _, err := fetchChecksum(ts.URL + "/empty"); err == nil {
		t.Fatalf("empty: want error")
	}
	if _, err := fetchChecksum(ts.URL + "/short"); err == nil {
		t.Fatalf("short: want error")
	}
	if _, err := fetchChecksum(ts.URL + "/nothex"); err == nil {
		t.Fatalf("nothex: want error")
	}
	if _, err := fetchChecksum(ts.URL + "/notfound"); err == nil {
		t.Fatalf("notfound: want error")
	}
}

func TestVerifyChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bin")
	data := []byte("fake sandman binary")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if err := verifyChecksum(path, sum[:]); err != nil {
		t.Fatalf("verify good: %v", err)
	}
	bad := sha256.Sum256([]byte("other"))
	if err := verifyChecksum(path, bad[:]); err == nil {
		t.Fatalf("verify mismatch: want error")
	}
	if err := verifyChecksum(filepath.Join(dir, "missing"), sum[:]); err == nil {
		t.Fatalf("verify missing file: want error")
	}
}

// releaseServer serves a fake release + binary + checksum; the binary's
// checksum is computed so installs can be verified end to end.
// latestRelease requests <base>/theycallmeloki/sandman/releases/latest,
// so the handler matches by suffix; the asset URLs are absolute.
func releaseServer(t *testing.T, tag string, bin []byte) *httptest.Server {
	t.Helper()
	sum := sha256.Sum256(bin)
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/releases/latest"):
			fmt.Fprintf(w, `{"tag_name": %q, "assets": [
				{"name": "sandman-linux-amd64", "browser_download_url": %q},
				{"name": "sandman-linux-amd64.sha256", "browser_download_url": %q}
			]}`, tag, ts.URL+"/bin", ts.URL+"/sha")
		case r.URL.Path == "/bin":
			_, _ = w.Write(bin)
		case r.URL.Path == "/sha":
			fmt.Fprintf(w, "%x  sandman-linux-amd64\n", sum)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

func TestLatestRelease(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// latestRelease requests <base>/theycallmeloki/sandman/releases/latest;
		// the test's <base> path selects the case
		switch strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")[0] {
		case "none":
			http.NotFound(w, r)
		case "empty":
			_, _ = w.Write([]byte(`{"tag_name": ""}`))
		case "good":
			_, _ = w.Write([]byte(`{"tag_name": "v0.0.1", "assets": [{"name": "x"}]}`))
		default:
			http.Error(w, "boom", http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	updateAPIBase = ts.URL + "/none"
	defer func() { updateAPIBase = "https://api.github.com/repos" }()

	if _, err := latestRelease(); err != errNoReleases {
		t.Fatalf("404: err=%v, want errNoReleases", err)
	}
	// the "no releases yet" path serves 200 with an empty tag
	updateAPIBase = ts.URL + "/empty"
	if _, err := latestRelease(); err == nil {
		t.Fatalf("empty tag: want error")
	}
	updateAPIBase = ts.URL + "/good"
	rel, err := latestRelease()
	if err != nil || rel.TagName != "v0.0.1" {
		t.Fatalf("good: rel=%+v err=%v", rel, err)
	}
}

func TestInstallRelease(t *testing.T) {
	bin := []byte("the new sandman binary")
	ts := releaseServer(t, "v0.0.2", bin)
	updateAPIBase = ts.URL
	defer func() { updateAPIBase = "https://api.github.com/repos" }()

	dst := filepath.Join(t.TempDir(), "bin", "sandman")
	rel, err := latestRelease()
	if err != nil {
		t.Fatalf("latestRelease: %v", err)
	}
	binURL := releaseAsset(rel, "linux", "amd64")
	shaURL := releaseAsset(rel, "linux", "amd64.sha256")
	if err := installRelease(binURL, shaURL, dst); err != nil {
		t.Fatalf("installRelease: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil || string(got) != string(bin) {
		t.Fatalf("installed binary = %q, %v", got, err)
	}
	if st, _ := os.Stat(dst); st.Mode().Perm() != 0o755 {
		t.Fatalf("installed mode = %v, want 0755", st.Mode().Perm())
	}
	// no .new staging file left behind
	if _, err := os.Stat(dst + ".new"); !os.IsNotExist(err) {
		t.Fatalf("staging file left behind: %v", err)
	}
}

func TestInstallReleaseChecksumMismatch(t *testing.T) {
	bin := []byte("binary")
	sum := sha256.Sum256([]byte("something else"))
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bin":
			_, _ = w.Write(bin)
		case "/sha":
			fmt.Fprintf(w, "%x  sandman-linux-amd64\n", sum)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	dst := filepath.Join(t.TempDir(), "sandman")
	if err := installRelease(ts.URL+"/bin", ts.URL+"/sha", dst); err == nil {
		t.Fatalf("mismatched checksum: want error")
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("binary installed despite checksum mismatch: %v", err)
	}
}

// TestCmdUpdateCheck exercises cmdUpdate's --check reporting against a
// fake release server: the no-releases, up-to-date, and newer-available
// branches. The install branches are covered by TestInstallRelease.
func TestCmdUpdateCheck(t *testing.T) {
	capture := func(fn func()) string {
		old := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w
		defer func() { os.Stdout = old }()
		fn()
		w.Close()
		b, _ := io.ReadAll(r)
		return string(b)
	}

	// no releases: the API answers 404
	none := httptest.NewServer(http.NotFoundHandler())
	defer none.Close()
	updateAPIBase = none.URL
	defer func() { updateAPIBase = "https://api.github.com/repos" }()
	if out := capture(func() { cmdUpdate([]string{"--check"}) }); !strings.Contains(out, "no releases published yet") {
		t.Fatalf("no-releases output = %q", out)
	}

	// up to date: the release tag equals the binary's version
	same := releaseServer(t, "v"+Version, []byte("x"))
	updateAPIBase = same.URL
	if out := capture(func() { cmdUpdate([]string{"--check"}) }); !strings.Contains(out, "up to date") {
		t.Fatalf("up-to-date output = %q", out)
	}

	// newer available: --check reports without installing
	newer := releaseServer(t, "v9.9.9", []byte("x"))
	updateAPIBase = newer.URL
	if out := capture(func() { cmdUpdate([]string{"--check"}) }); !strings.Contains(out, "new version available: v9.9.9") {
		t.Fatalf("newer output = %q", out)
	}
}
