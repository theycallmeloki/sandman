package store

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBackupTarRoundTrip: a backup captures the repo's refs, commits, and
// content-addressed blobs plus tags, and the captured ref points at a
// commit that is present in the same archive (the write lock guarantees
// the pair is captured atomically).
func TestBackupTarRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)

	if err := s.CreateRepo("r"); err != nil {
		t.Fatal(err)
	}
	cm, err := s.StartCommit("r", "master", "")
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("hello backup\n")
	if err := s.PutFile(cm.ID, "f.txt", content); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FinishCommit(cm.ID, "", false); err != nil {
		t.Fatal(err)
	}
	if err := s.PutTag("t1", []byte("ref:"+cm.ID)); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := s.BackupTar(tw); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	// extract into a fresh dir and verify the tree
	dst := t.TempDir()
	tr := tar.NewReader(&buf)
	names := map[string]bool{}
	head := ""
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names[hdr.Name] = true
		if hdr.Name == "repos/r/refs/master" {
			b, _ := io.ReadAll(tr)
			head = strings.TrimSpace(string(b))
		}
		if hdr.Name == "repos/r/commits/"+cm.ID+".json" || strings.HasPrefix(hdr.Name, "repos/.objects/") {
			if err := extractEntry(dst, hdr, tr); err != nil {
				t.Fatal(err)
			}
		}
	}
	if head != cm.ID {
		t.Fatalf("captured head %q, want %q (ref and commit must ride together)", head, cm.ID)
	}
	if !names["repos/r/commits/"+cm.ID+".json"] {
		t.Fatal("backup missing the commit record")
	}
	if !names["tags/t1"] {
		t.Fatal("backup missing the tag")
	}
	sum := sha256.Sum256(content)
	sha := hex.EncodeToString(sum[:])
	got := strings.TrimSpace(readFile(t, filepath.Join(dst, "repos", ".objects", sha[:2], sha[2:])))
	if got != "hello backup" {
		t.Fatalf("blob content %q, want the committed file", got)
	}
}

func extractEntry(dst string, hdr *tar.Header, tr *tar.Reader) error {
	if hdr.Typeflag == tar.TypeDir {
		return nil
	}
	p := filepath.Join(dst, filepath.FromSlash(hdr.Name))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	b, err := io.ReadAll(tr)
	if err != nil {
		return err
	}
	return os.WriteFile(p, b, 0o644)
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
