package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// update implements `sandman update`: check GitHub releases for the
// latest tagged build and, when behind, download the release asset and
// install it over the daemon at /usr/local/bin/sandman. The binary is
// both CLI and daemon, so one install updates the whole node.
//
// Releases are tagged v0.0.1+ and carry a per-platform binary asset
// (sandman-linux-amd64) plus its sha256; the update verifies the
// checksum before replacing the file (atomic: temp file + rename).
// A non-root install re-executes through sudo when the target directory
// is not writable.

const (
	updateOwner = "theycallmeloki"
	updateRepo  = "sandman"
	updatePath  = "/usr/local/bin/sandman"
)

// errNoReleases marks a repo with no published releases yet.
var errNoReleases = fmt.Errorf("no releases")

func cmdUpdate(args []string) {
	checkOnly := false
	for _, a := range args {
		switch a {
		case "--check", "-check":
			checkOnly = true
		case "-h", "--help":
			fmt.Fprint(os.Stderr, "usage: sandman update [--check]\n  check GitHub releases and install the latest build over "+updatePath+"\n  --check: report the latest release without installing\n")
			os.Exit(0)
		default:
			die("update: unknown flag "+a+" (try --check)", 2)
		}
	}

	rel, err := latestRelease()
	if err == errNoReleases {
		fmt.Printf("no releases published yet — sandman %s is current\n", Version)
		return
	}
	if err != nil {
		die("update: "+err.Error(), 1)
	}

	if !validVersion(Version) {
		fmt.Printf("sandman %s is a dev build; run `sandman update` on a release build to track releases\n", Version)
		fmt.Printf("latest release: %s\n", rel.TagName)
		return
	}

	// current >= latest: nothing to do
	if cmpVersions(Version, strings.TrimPrefix(rel.TagName, "v")) >= 0 {
		fmt.Printf("sandman %s is up to date (latest release: %s)\n", Version, rel.TagName)
		return
	}

	fmt.Printf("new version available: %s (you have %s)\n", rel.TagName, Version)
	if checkOnly {
		return
	}

	asset := releaseAsset(rel, runtime.GOOS, runtime.GOARCH)
	if asset == "" {
		die(fmt.Sprintf("update: release %s has no %s-%s asset; install from https://github.com/%s/%s/releases", rel.TagName, runtime.GOOS, runtime.GOARCH, updateOwner, updateRepo), 1)
	}

	// the sha256 asset must ride the same release
	shasum := releaseAsset(rel, runtime.GOOS, runtime.GOARCH+".sha256")
	if shasum == "" {
		die(fmt.Sprintf("update: release %s is missing the %s checksum asset; refusing unsigned install", rel.TagName, runtime.GOOS+"-"+runtime.GOARCH+".sha256"), 1)
	}

	if err := installRelease(asset, shasum); err != nil {
		die("update: "+err.Error(), 1)
	}
	fmt.Printf("updated to %s — installed at %s\n", rel.TagName, updatePath)
}

// validVersion reports whether Version is a parseable semver-ish
// (major.minor.patch, possibly with a v prefix).
func validVersion(s string) bool {
	_, _, _, err := splitVersion(s)
	return err == nil
}

// splitVersion parses "vX.Y.Z" into its numeric parts.
func splitVersion(s string) (major, minor, patch int, err error) {
	s = strings.TrimPrefix(s, "v")
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return 0, 0, 0, fmt.Errorf("version %q is not major.minor.patch", s)
	}
	if major, err = strconv.Atoi(parts[0]); err != nil {
		return 0, 0, 0, err
	}
	if minor, err = strconv.Atoi(parts[1]); err != nil {
		return 0, 0, 0, err
	}
	if patch, err = strconv.Atoi(parts[2]); err != nil {
		return 0, 0, 0, err
	}
	return major, minor, patch, nil
}

// cmpVersions compares two version strings; returns <0, 0, >0.
func cmpVersions(a, b string) int {
	amaj, amin, apat, aerr := splitVersion(a)
	bmaj, bmin, bpat, berr := splitVersion(b)
	// an unparseable side sorts before any parseable one (dev < release)
	if aerr != nil || berr != nil {
		if aerr != nil && berr != nil {
			return 0
		}
		if aerr != nil {
			return -1
		}
		return 1
	}
	if amaj != bmaj {
		return amaj - bmaj
	}
	if amin != bmin {
		return amin - bmin
	}
	return apat - bpat
}

type ghRelease struct {
	TagName string    `json:"tag_name"`
	Assets  []ghAsset `json:"assets"`
}

type ghAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

// latestRelease fetches the newest tagged release from GitHub.
func latestRelease() (*ghRelease, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/releases/latest", updateOwner, updateRepo)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "sandman-update/"+Version)
	req.Header.Set("Accept", "application/vnd.github+json")
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("checking %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, errNoReleases
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		return nil, fmt.Errorf("GitHub returned %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var rel ghRelease
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		return nil, fmt.Errorf("decoding release: %v", err)
	}
	if rel.TagName == "" {
		return nil, fmt.Errorf("no releases found (repo %s/%s has none yet)", updateOwner, updateRepo)
	}
	return &rel, nil
}

// releaseAsset finds the asset named sandman-<goos>-<goarch>[.suffix].
func releaseAsset(rel *ghRelease, goos, goarch string) string {
	want := "sandman-" + goos + "-" + goarch
	for _, a := range rel.Assets {
		if a.Name == want {
			return a.BrowserDownloadURL
		}
	}
	return ""
}

// installRelease downloads the binary and its checksum, verifies the
// hash, and atomically replaces updatePath (sudo re-exec when the target
// directory is not writable).
func installRelease(binURL, shaURL string) error {
	tmp, err := os.CreateTemp("", "sandman-update-*.bin")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := downloadTo(tmp, binURL); err != nil {
		return fmt.Errorf("downloading release: %v", err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	sum, err := fetchChecksum(shaURL)
	if err != nil {
		return err
	}
	if err := verifyChecksum(tmpPath, sum); err != nil {
		return err
	}
	if err := os.Chmod(tmpPath, 0o755); err != nil {
		return err
	}

	if err := replaceBinary(tmpPath); err != nil {
		return installAsRoot()
	}
	return nil
}

func downloadTo(w io.Writer, url string) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "sandman-update/"+Version)
	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("asset download returned %s", resp.Status)
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

// fetchChecksum downloads the "sandman-<os>-<arch>.sha256" asset and
// extracts the hex digest.
func fetchChecksum(url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "sandman-update/"+Version)
	client := &http.Client{Timeout: time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("checksum download returned %s", resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(string(b))
	if len(fields) == 0 {
		return nil, fmt.Errorf("empty checksum file")
	}
	sum, err := hex.DecodeString(fields[0])
	if err != nil {
		return nil, fmt.Errorf("malformed checksum %q", fields[0])
	}
	if len(sum) != sha256.Size {
		return nil, fmt.Errorf("checksum has %d bytes, want %d", len(sum), sha256.Size)
	}
	return sum, nil
}

func verifyChecksum(path string, want []byte) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	if got := h.Sum(nil); !strings.EqualFold(hex.EncodeToString(got), hex.EncodeToString(want)) {
		return fmt.Errorf("checksum mismatch — downloaded binary does not match the release checksum")
	}
	return nil
}

// replaceBinary atomically swaps the downloaded binary over updatePath.
func replaceBinary(src string) error {
	dir := filepath.Dir(updatePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// writing into /usr/local/bin as a regular user fails with EACCES;
	// any error here (including a read-only mount) escalates to sudo
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	staged := updatePath + ".new"
	if err := os.WriteFile(staged, data, 0o755); err != nil {
		return err
	}
	return os.Rename(staged, updatePath)
}

// installAsRoot re-executes the same update through sudo (the target
// directory is root-owned). The check passes again under root; the
// install then succeeds. os.Args[1:] is the original verb + flags.
func installAsRoot() error {
	self := os.Args[0]
	if !strings.Contains(self, "/") {
		if p, err := exec.LookPath(self); err == nil {
			self = p
		}
	}
	cmd := exec.Command("sudo", append([]string{"-p", "sudo password: ", self}, os.Args[1:]...)...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return fmt.Errorf("could not write %s (permission denied) and sudo failed (exit %d) — run `sudo %s %s`", updatePath, ee.ExitCode(), self, strings.Join(os.Args[1:], " "))
		}
		return err
	}
	return nil
}
