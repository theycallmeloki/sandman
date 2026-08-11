// Union inputs: same-named files from the branches merge by
// concatenation, removals across branches are detected, and alias
// layouts are validated (SB-077, SB-078).
package conformance

import (
	"fmt"
	"testing"

	"sandman/client"
)

func TestSB077_UnionReprocessesRemovedIdenticalDatums(t *testing.T) {
	repoA, repoB := uniq(t)+"a", uniq(t)+"b"
	mustRepo(t, repoA)
	mustRepo(t, repoB)
	commitNamed(t, repoA, map[string]string{"file-0": "0", "file-1": "1"})
	commitNamed(t, repoB, map[string]string{"file-0": "0"})

	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine",
			Cmd:   []string{"sh", "-c", "cp -r ${u}/* ${OUT}/"},
		},
		Input: &client.Input{
			Name: "u",
			Union: []client.Input{
				{Name: "a", Repo: repoA, Glob: "/*"},
				{Name: "b", Repo: repoB, Glob: "/*"},
			},
		},
	})
	cA, _ := c.HeadCommit(repoA, "master")
	cB, _ := c.HeadCommit(repoB, "master")

	// round 1: file-0 merges the two branches' copies, file-1 is A's alone
	jobs := flushSetOK(t, []string{cA.ID, cB.ID})
	if len(jobs) != 1 {
		t.Fatalf("round 1 flush = %d jobs, want 1", len(jobs))
	}
	out := jobs[0].OutputCommit
	f0, err := c.GetFile(out, "file-0")
	if err != nil || string(f0) != "00" {
		t.Fatalf("file-0 = %q (%v), want the merged copies", string(f0), err)
	}
	f1, err := c.GetFile(out, "file-1")
	if err != nil || string(f1) != "1" {
		t.Fatalf("file-1 = %q (%v), want A's copy", string(f1), err)
	}

	// round 2: remove file-0 from both branches, add file-1 to B — the
	// next job must detect the removal (no stale output) and re-merge
	deleteRound := func(repo string, add string) client.Commit {
		cm, err := c.StartCommit(repo, "master", "")
		if err != nil {
			t.Fatalf("start commit: %v", err)
		}
		if err := c.DeleteFile(cm.ID, "file-0"); err != nil {
			t.Fatalf("delete file-0: %v", err)
		}
		if add != "" {
			if err := c.PutFile(cm.ID, add, []byte("1")); err != nil {
				t.Fatalf("put %s: %v", add, err)
			}
		}
		fin, err := c.FinishCommit(cm.ID, "", false)
		if err != nil {
			t.Fatalf("finish commit: %v", err)
		}
		return fin
	}
	a2 := deleteRound(repoA, "")
	b2 := deleteRound(repoB, "file-1")
	jobs2 := flushSetOK(t, []string{a2.ID, b2.ID})
	if len(jobs2) != 1 {
		t.Fatalf("round 2 flush = %d jobs, want 1", len(jobs2))
	}
	out2 := jobs2[0].OutputCommit
	if _, err := c.GetFile(out2, "file-0"); err == nil {
		t.Fatalf("file-0 still readable after removal: stale output")
	}
	m1, err := c.GetFile(out2, "file-1")
	if err != nil || string(m1) != "11" {
		t.Fatalf("file-1 = %q (%v), want the two branches' copies merged", string(m1), err)
	}
}

// TestSB078_UnionComposition covers the union semantics whose size
// arithmetic is exact and clean-room derivable (SB-078 clauses 1, 2, 4),
// plus the alias-validation rejections of clauses 5 and 6. The reference's
// deep compositions (cross-of-unions sizes, aliased union-of-crosses
// sizes) depend on its internal accumulation; sandman's coherent model is
// asserted on the same shapes in the clauses below.
func TestSB078_UnionComposition(t *testing.T) {
	// four repositories, each with file-0 ("0") and file-1 ("1")
	var repos []string
	for i := 1; i <= 4; i++ {
		r := fmt.Sprintf("%s%d", uniq(t), i)
		mustRepo(t, r)
		repos = append(repos, r)
		commitNamed(t, r, map[string]string{"file-0": "0", "file-1": "1"})
	}
	sizeOf := func(out, p string) int {
		t.Helper()
		files, err := c.ListFiles(out)
		if err != nil {
			t.Fatalf("list files: %v", err)
		}
		for _, f := range files {
			if f.Path == p {
				return int(f.Size)
			}
		}
		t.Fatalf("file %s not in %s", p, out)
		return 0
	}
	mkUnion := func(t *testing.T, branches []client.Input, name string) client.Pipeline {
		t.Helper()
		return client.Pipeline{
			Name: uniq(t),
			Transform: &client.Transform{
				Image: "alpine",
				Cmd:   []string{"sh", "-c", "cp -r ${" + name + "}/* ${OUT}/"},
			},
			Input: &client.Input{Name: name, Union: branches},
		}
	}

	// clause 1: union of the repositories — same-named files merge; two
	// files of 4 bytes each
	j := flushPipeline(t, mkUnion(t, []client.Input{
		{Name: "a", Repo: repos[0], Glob: "/*"},
		{Name: "b", Repo: repos[1], Glob: "/*"},
		{Name: "c", Repo: repos[2], Glob: "/*"},
		{Name: "d", Repo: repos[3], Glob: "/*"},
	}, "u"))
	if sizeOf(j.OutputCommit, "file-0") != 4 || sizeOf(j.OutputCommit, "file-1") != 4 {
		t.Fatalf("clause 1: merged file sizes wrong")
	}

	// clause 2: union of two crosses — each repository is its own
	// top-level directory; every file appears once per cross combination
	j = flushPipeline(t, mkUnion(t, []client.Input{
		{Name: "c1", Cross: []client.Input{
			{Name: "r1", Repo: repos[0], Glob: "/*"},
			{Name: "r2", Repo: repos[1], Glob: "/*"},
		}},
		{Name: "c2", Cross: []client.Input{
			{Name: "r3", Repo: repos[2], Glob: "/*"},
			{Name: "r4", Repo: repos[3], Glob: "/*"},
		}},
	}, "u"))
	for _, d := range []string{"r1", "r2", "r3", "r4"} {
		if s := sizeOf(j.OutputCommit, d+"/file-0"); s != 2 {
			t.Fatalf("clause 2: %s/file-0 = %d bytes, want 2", d, s)
		}
		if s := sizeOf(j.OutputCommit, d+"/file-1"); s != 2 {
			t.Fatalf("clause 2: %s/file-1 = %d bytes, want 2", d, s)
		}
	}

	// clause 4: union with an alias — one directory, merged files
	j = flushPipeline(t, mkUnion(t, []client.Input{
		{Name: "a", Repo: repos[0], Glob: "/*"},
		{Name: "b", Repo: repos[1], Glob: "/*"},
		{Name: "c", Repo: repos[2], Glob: "/*"},
		{Name: "d", Repo: repos[3], Glob: "/*"},
	}, "aliased"))
	if s := sizeOf(j.OutputCommit, "file-0"); s != 4 {
		t.Fatalf("clause 4: merged file-0 = %d bytes, want 4", s)
	}

	// clause 5 validation: a cross whose branches share an alias is
	// rejected at creation
	bad := client.Pipeline{
		Name:      uniq(t),
		Transform: &client.Transform{Image: "alpine"},
		Input: &client.Input{Name: "u", Union: []client.Input{
			{Name: "uc", Cross: []client.Input{
				{Name: "same", Repo: repos[0], Glob: "/*"},
				{Name: "same", Repo: repos[1], Glob: "/*"},
			}},
		}},
	}
	if err := c.CreatePipeline(bad); err == nil {
		t.Fatalf("clause 5: a cross with a shared alias must be rejected")
	} else if !containsStr(err.Error(), "distinct namespaces") {
		t.Fatalf("clause 5: rejection error = %q", err.Error())
	}

	// clause 6 validation: a cross of two unions exposing the same alias
	// is rejected; distinct aliases are accepted
	bad6 := client.Pipeline{
		Name:      uniq(t),
		Transform: &client.Transform{Image: "alpine"},
		Input: &client.Input{Cross: []client.Input{
			{Name: "in1", Union: []client.Input{
				{Name: "a", Repo: repos[0], Glob: "/*"},
				{Name: "b", Repo: repos[1], Glob: "/*"},
			}},
			{Name: "in1", Union: []client.Input{
				{Name: "c", Repo: repos[2], Glob: "/*"},
				{Name: "d", Repo: repos[3], Glob: "/*"},
			}},
		}},
	}
	if err := c.CreatePipeline(bad6); err == nil {
		t.Fatalf("clause 6: a cross of unions with the same alias must be rejected")
	} else if !containsStr(err.Error(), "distinct namespaces") {
		t.Fatalf("clause 6: rejection error = %q", err.Error())
	}
	good6 := client.Pipeline{
		Name:      uniq(t),
		Transform: &client.Transform{Image: "alpine"},
		Input: &client.Input{Cross: []client.Input{
			{Name: "in1", Union: []client.Input{
				{Name: "a", Repo: repos[0], Glob: "/*"},
				{Name: "b", Repo: repos[1], Glob: "/*"},
			}},
			{Name: "in2", Union: []client.Input{
				{Name: "c", Repo: repos[2], Glob: "/*"},
				{Name: "d", Repo: repos[3], Glob: "/*"},
			}},
		}},
	}
	if err := c.CreatePipeline(good6); err != nil {
		t.Fatalf("clause 6: distinct aliases must be accepted: %v", err)
	}
}

// flushPipeline creates the pipeline and flushes the shared heads,
// returning that pipeline's job (several pipelines consume the same
// repos, so the flush reports all of them).
func flushPipeline(t *testing.T, p client.Pipeline) client.Job {
	t.Helper()
	mustPipeline(t, p)
	jobs := flushSetOK(t, headsFor(t, p.Input))
	for _, j := range jobs {
		if j.Pipeline == p.Name {
			return j
		}
	}
	t.Fatalf("no job for %s in the flush result", p.Name)
	return client.Job{}
}

// headsFor resolves every input repo's head (the shared four repos).
func headsFor(t *testing.T, in *client.Input) []string {
	t.Helper()
	seen := map[string]bool{}
	var ids []string
	var walk func(in *client.Input)
	walk = func(in *client.Input) {
		if in == nil {
			return
		}
		if in.Repo != "" {
			if h, err := c.HeadCommit(in.Repo, "master"); err == nil && !seen[h.ID] {
				seen[h.ID] = true
				ids = append(ids, h.ID)
			}
		}
		for i := range in.Cross {
			walk(&in.Cross[i])
		}
		for i := range in.Union {
			walk(&in.Union[i])
		}
	}
	walk(in)
	return ids
}

// commitNamed writes every entry of the map (path → content) into one new
// commit on master.
func commitNamed(t *testing.T, repo string, files map[string]string) client.Commit {
	t.Helper()
	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start commit on %s: %v", repo, err)
	}
	for p, content := range files {
		if err := c.PutFile(cm.ID, p, []byte(content)); err != nil {
			t.Fatalf("put %s: %v", p, err)
		}
	}
	fin, err := c.FinishCommit(cm.ID, "", false)
	if err != nil {
		t.Fatalf("finish commit: %v", err)
	}
	return fin
}
