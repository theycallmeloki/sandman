package conformance

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"sandman/client"
)

// SB-038 — a job that recursively copies whole input directories completes
// and yields one output commit.
func TestSB038_RecursiveDirectoryCopy(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cm := commitFiles(t, repo, "master", map[string]string{
		"dir/sub/deep": "deep content",
		"dir/other":    "other",
		"top":          "top",
	})

	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name:      pipe,
		Transform: &client.Transform{Image: "alpine", Cmd: []string{"sh", "-c", fmt.Sprintf("cp -r ${%s} ${OUT}/", repo)}},
		Input:     &client.Input{Repo: repo, Glob: "/*"},
	})

	jobs := flushOK(t, cm.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush: %d jobs, want 1", len(jobs))
	}
}

// SB-052 — a job processing the head commit sees and copies the full
// accumulated file content.
func TestSB052_HeadJobSeesAccumulatedContent(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{Name: pipe, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})

	cm1 := commitFiles(t, repo, "master", map[string]string{"file": "foo\n"})
	jobs1 := flushOK(t, cm1.ID)
	got, err := c.GetFile(jobs1[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("read output 1: %v", err)
	}
	if string(got) != "foo\n" {
		t.Fatalf("output 1 = %q, want %q", got, "foo\n")
	}

	cm2 := commitFiles(t, repo, "master", map[string]string{"file": "foo\nbar\n"})
	jobs2 := flushOK(t, cm2.ID)
	got, err = c.GetFile(jobs2[0].OutputCommit, "file")
	if err != nil {
		t.Fatalf("read output 2: %v", err)
	}
	if string(got) != "foo\nbar\n" {
		t.Fatalf("output 2 = %q, want %q", got, "foo\nbar\n")
	}
}

// SB-087 — each new input commit triggers a successful job whose output is
// cumulative across the branch.
func TestSB087_EachCommitCumulativeJob(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{Name: pipe, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})

	cm1 := commitFiles(t, repo, "master", map[string]string{"file": "foo\n"})
	jobs1 := flushOK(t, cm1.ID)
	if len(jobs1) != 1 {
		t.Fatalf("commit 1: %d jobs, want 1", len(jobs1))
	}
	if got, _ := c.GetFile(jobs1[0].OutputCommit, "file"); string(got) != "foo\n" {
		t.Fatalf("commit 1 output = %q, want %q", got, "foo\n")
	}

	cm2 := commitFiles(t, repo, "master", map[string]string{"file2": "bar\n"})
	jobs2 := flushOK(t, cm2.ID)
	if len(jobs2) != 1 {
		t.Fatalf("commit 2: %d jobs, want 1", len(jobs2))
	}
	out2 := jobs2[0].OutputCommit
	if got, _ := c.GetFile(out2, "file"); string(got) != "foo\n" {
		t.Fatalf("commit 2 output file = %q, want %q (cumulative)", got, "foo\n")
	}
	if got, _ := c.GetFile(out2, "file2"); string(got) != "bar\n" {
		t.Fatalf("commit 2 output file2 = %q, want %q", got, "bar\n")
	}

	all, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("pipeline has %d jobs, want 2", len(all))
	}
}

// SB-099 — files are fetchable with raw bytes, an attachment disposition on
// request, and a content type detected from the bytes.
func TestSB099_FileFetchHeaders(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	// a minimal 1x1 transparent GIF (GIF89a)
	gif := []byte("\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff\x21\xf9\x04\x01\x00\x00\x00\x00\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02\x44\x01\x00\x3b")
	cm := commitFiles(t, repo, "master", map[string]string{"file": "foo", "giphy.gif": string(gif)})

	plain, err := c.FetchFile(cm.ID, "file", false)
	if err != nil {
		t.Fatalf("plain fetch: %v", err)
	}
	if string(plain.Data) != "foo" {
		t.Fatalf("body = %q, want %q", plain.Data, "foo")
	}
	if plain.ContentDisp != "" {
		t.Fatalf("plain fetch has Content-Disposition %q, want none", plain.ContentDisp)
	}

	dl, err := c.FetchFile(cm.ID, "file", true)
	if err != nil {
		t.Fatalf("download fetch: %v", err)
	}
	if string(dl.Data) != "foo" {
		t.Fatalf("download body = %q, want %q", dl.Data, "foo")
	}
	if dl.ContentDisp != `attachment; filename="file"` {
		t.Fatalf("Content-Disposition = %q, want attachment; filename=\"file\"", dl.ContentDisp)
	}

	img, err := c.FetchFile(cm.ID, "giphy.gif", false)
	if err != nil {
		t.Fatalf("gif fetch: %v", err)
	}
	if string(img.Data) != string(gif) {
		t.Fatal("gif bytes do not round-trip")
	}
	if img.ContentType != "image/gif" {
		t.Fatalf("Content-Type = %q, want image/gif", img.ContentType)
	}
}

// SB-150 — tags are durable named references to data objects: 1000 tags
// listable, every one with a non-empty reference, and retrievable by name
// byte-for-byte.
func TestSB150_TagsListAndRetrieve(t *testing.T) {
	for i := range 1000 {
		if err := c.PutTag(fmt.Sprintf("tag%d", i), []byte(fmt.Sprintf("Object %d", i))); err != nil {
			t.Fatalf("put tag%d: %v", i, err)
		}
	}

	tags, err := c.ListTags()
	if err != nil {
		t.Fatalf("list tags: %v", err)
	}
	if len(tags) != 1000 {
		t.Fatalf("listed %d tags, want 1000", len(tags))
	}
	seen := map[string]bool{}
	for _, tg := range tags {
		if tg.Ref == "" {
			t.Fatalf("tag %q has an empty reference", tg.Name)
		}
		seen[tg.Name] = true
	}
	for i := range 1000 {
		if !seen[fmt.Sprintf("tag%d", i)] {
			t.Fatalf("tag%d missing from listing", i)
		}
	}

	got, err := c.GetTag("tag0")
	if err != nil {
		t.Fatalf("get tag0: %v", err)
	}
	if string(got) != "Object 0" {
		t.Fatalf("tag0 = %q, want %q", got, "Object 0")
	}
}

// SB-155 — every data-plane API endpoint validates its request and never
// panics on missing fields: each malformed call yields a well-formed HTTP
// response (never a dropped connection), and the service survives.
func TestSB155_NoPanicOnEmptyRequests(t *testing.T) {
	check := func(vals ...any) {
		if len(vals) > 0 {
			if err, ok := vals[len(vals)-1].(error); ok {
				noPanic(t, err)
			}
		}
	}

	check(c.CreateRepo(""))
	check(c.CreateRepo("bad/name"))
	check(c.InspectRepo(""))
	check(c.DeleteRepo("", false))
	check(c.StartCommit("", "", ""))
	check(c.FinishCommit("nope", "", false))
	check(c.InspectCommit("nope"))
	check(c.HeadCommit("nope", "master"))
	check(c.PutFile("nope", "f", []byte("x")))
	check(c.GetFile("nope", "f"))
	check(c.ListFiles("nope"))
	check(c.CopyFile("nope", "f", "nope", "f"))
	check(c.FetchFile("nope", "f", false))
	check(c.CreatePipeline(client.Pipeline{}))
	check(c.InspectPipeline("nope"))
	check(c.DeletePipeline("nope", false, false))
	check(c.StopPipeline("nope"))
	check(c.StartPipeline("nope"))
	check(c.InspectJob("nope"))
	check(c.CancelJob("nope"))
	check(c.DeleteJob("nope"))
	check(c.ListJobsFiltered(client.JobFilter{OutputCommit: "nope"}))
	check(c.GetTag("nope"))

	// garbage JSON bodies must be rejected, not crash the server
	for _, path := range []string{"/api/v1/repos", "/api/v1/pipelines"} {
		resp, err := httpPost(c.Base()+path, "{not json")
		if err != nil {
			t.Fatalf("garbage body to %s: transport error: %v", path, err)
		}
		resp.Body.Close()
	}

	// the daemon is still fully operational
	name := uniq(t)
	if err := c.CreateRepo(name); err != nil {
		t.Fatalf("daemon unhealthy after malformed requests: %v", err)
	}
}

// SB-156 — files and directories can be copied from a pipeline's output
// into an input repo; existing destination paths are protected on both copy
// and put.
func TestSB156_CopyOutToInWithOverwriteProtection(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{Name: pipe, Transform: copyTransform(repo), Input: &client.Input{Repo: repo, Glob: "/*"}})
	cm1 := commitFiles(t, repo, "master", map[string]string{"foo": "foo"})
	jobs := flushOK(t, cm1.ID)
	srcCommit := jobs[0].OutputCommit

	// open a new commit on the input repo
	dst, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// copy the output file into the input repo at a new path
	if err := c.CopyFile(dst.ID, "file2", srcCommit, "foo"); err != nil {
		t.Fatalf("copy file→file2: %v", err)
	}
	// copy onto an existing path (from the parent revision) is rejected
	if err := c.CopyFile(dst.ID, "foo", srcCommit, "foo"); err == nil {
		t.Fatal("copy onto existing path succeeded, want error")
	}
	// put onto a path already written in this commit is rejected
	if err := c.PutFile(dst.ID, "file2", []byte("x")); err == nil {
		t.Fatal("put onto existing path succeeded, want error")
	}

	// new paths are writable; then copy the directory subtree
	if err := c.PutFile(dst.ID, "dir/file3", []byte("foo")); err != nil {
		t.Fatalf("put dir/file3: %v", err)
	}
	if err := c.PutFile(dst.ID, "dir/file4", []byte("bar")); err != nil {
		t.Fatalf("put dir/file4: %v", err)
	}
	if err := c.CopyFile(dst.ID, "dir2", dst.ID, "dir"); err != nil {
		t.Fatalf("copy dir→dir2: %v", err)
	}
	if _, err := c.FinishCommit(dst.ID, "", false); err != nil {
		t.Fatalf("finish: %v", err)
	}

	// flushing the input branch propagates downstream; later runs see the
	// copied files
	jobs = flushOK(t, dst.ID)
	if len(jobs) != 1 {
		t.Fatalf("flush: %d jobs, want 1", len(jobs))
	}
	out := jobs[0].OutputCommit
	read := func(p string) string {
		t.Helper()
		b, err := c.GetFile(out, p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		return string(b)
	}
	if got := read("file2"); got != "foo" {
		t.Fatalf("file2 = %q, want foo", got)
	}
	if got := read("dir2/file3"); got != "foo" {
		t.Fatalf("dir2/file3 = %q, want foo", got)
	}
	if got := read("dir2/file4"); got != "bar" {
		t.Fatalf("dir2/file4 = %q, want bar", got)
	}
}

func httpPost(u, body string) (*http.Response, error) {
	return http.Post(u, "application/json", strings.NewReader(body))
}
