// Execution-environment customization (SB-072/152): a pipeline's
// transform may carry a full customization document (PodSpec) and a JSON
// modification list (PodPatch); both are validated as JSON at creation
// and applied to every execution participant.
package conformance

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// TestSB072_RejectsMalformedCustomization — malformed spec/patch JSON
// fails pipeline creation before any execution (SB-072 clause 1).
func TestSB072_RejectsMalformedCustomization(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	cases := []struct {
		name    string
		p       client.Pipeline
		wantErr string
	}{
		{"bad spec", client.Pipeline{
			Name:      uniq(t),
			Transform: &client.Transform{Image: "alpine:3.21", PodSpec: "not json"},
			Input:     &client.Input{Repo: repo, Glob: "/"},
		}, "not valid JSON"},
		{"bad patch", client.Pipeline{
			Name:      uniq(t),
			Transform: &client.Transform{Image: "alpine:3.21", PodPatch: "{not json"},
			Input:     &client.Input{Repo: repo, Glob: "/"},
		}, "not valid JSON"},
		{"unknown key", client.Pipeline{
			Name:      uniq(t),
			Transform: &client.Transform{Image: "alpine:3.21", PodSpec: `{"bogus": 1}`},
			Input:     &client.Input{Repo: repo, Glob: "/"},
		}, "unknown customization key"},
		{"two sources", client.Pipeline{
			Name: uniq(t),
			Transform: &client.Transform{Image: "alpine:3.21",
				PodPatch: `[{"op":"add","path":"/volumes/v0","value":{"hostPath":"/x","emptyDir":true}}]`},
			Input: &client.Input{Repo: repo, Glob: "/"},
		}, "exactly one source kind"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := c.CreatePipeline(tc.p)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("create err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// TestSB072_AppliesCustomization — a well-formed spec and a well-formed
// patch both reach the execution environment (SB-072 clauses 2/3): the
// document's env vars are visible to the job. The scheduling constraint
// alongside customization is the placement mechanism (SB-167/169).
func TestSB072_AppliesCustomization(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	commitFiles(t, repo, "master", map[string]string{"file": "foo\n"})

	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "echo ${CUSTOM_SPEC}:${CUSTOM_PATCH} > ${OUT}/envs"},
			// the full spec sets one env var; the patch adds another
			PodSpec:  `{"env": {"CUSTOM_SPEC": "specval"}}`,
			PodPatch: `[{"op":"add","path":"/env/CUSTOM_PATCH","value":"patchval"}]`,
		},
		Input: &client.Input{Repo: repo, Glob: "/"},
	}
	mustPipeline(t, p)
	cm := commitFiles(t, repo, "master", map[string]string{"file2": "bar\n"})
	jobs := flushOK(t, cm.ID)
	got, err := c.GetFile(jobs[0].OutputCommit, "envs")
	if err != nil {
		t.Fatalf("read envs: %v", err)
	}
	if got := strings.TrimRight(string(got), "\n"); got != "specval:patchval" {
		t.Fatalf("customized envs = %q, want %q", got, "specval:patchval")
	}
}

// TestSB152_PodPatchVolume — a patch adding a volume reaches the
// execution participant (its host path is observable from user code) and
// does not disturb data processing (SB-152 clauses 1/2).
func TestSB152_PodPatchVolume(t *testing.T) {
	repo := uniq(t)
	mustRepo(t, repo)
	commitFiles(t, repo, "master", map[string]string{"file": "foo\n"})

	dst := t.TempDir()
	p := client.Pipeline{
		Name: uniq(t),
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd: []string{"sh", "-c",
				"echo written > /sandman/volumes/vol0/note; cp ${" + repo + "}/file ${OUT}/file"},
			PodPatch: `[{"op":"add","path":"/volumes/vol0","value":{"hostPath":"` + dst + `"}}]`,
		},
		Input: &client.Input{Repo: repo, Glob: "/"},
	}
	mustPipeline(t, p)
	cm := commitFiles(t, repo, "master", map[string]string{"file2": "bar\n"})
	jobs := flushOK(t, cm.ID)

	// the patched volume reached the participant: the user code's write
	// landed on the host path
	pollFor(t, "patched volume file appears", 30*time.Second, func() bool {
		b, err := os.ReadFile(filepath.Join(dst, "note"))
		return err == nil && string(b) == "written\n"
	})
	// data processing is undisturbed: the output commit is correct
	b, err := c.GetFile(jobs[0].OutputCommit, "file")
	if err != nil || string(b) != "foo\n" {
		t.Fatalf("output file = %q err=%v, want %q", string(b), err, "foo\n")
	}
}
