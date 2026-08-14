// Join and group inputs: files paired by captured glob groups, outer
// joins, and grouping across inputs (SB-074, SB-075, SB-076).
package conformance

import (
	"fmt"
	"sort"
	"testing"

	"sandman/client"
)

// spaceBinary renders i as a 4-char space-padded binary (the reference's
// naming for SB-076: " 10" for 2, etc.).
func spaceBinary(i int) string {
	return fmt.Sprintf("%4s", fmt.Sprintf("%b", i))
}

func TestSB074_JoinInputPairsByKey(t *testing.T) {
	r0, r1 := uniq(t)+"0", uniq(t)+"1"
	mustRepo(t, r0)
	mustRepo(t, r1)
	var names0, names1 []string
	for i := 0; i < 16; i++ {
		names0 = append(names0, fmt.Sprintf("file-0.%04b", i))
		names1 = append(names1, fmt.Sprintf("file-1.%04b", i))
	}
	c0 := commitMany(t, r0, names0)
	c1 := commitMany(t, r1, names1)

	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", "a=$(ls -r ${a} | tr -d '\\n'); b=$(ls -r ${b} | tr -d '\\n'); touch ${OUT}/${a}${b}"},
		},
		Input: &client.Input{Join: []client.Input{
			{Name: "a", Repo: r0, Glob: "/file-?.(11*)", JoinOn: "$1"},
			{Name: "b", Repo: r1, Glob: "/file-?.(*0)", JoinOn: "$1"},
		}},
		EnableStats: true,
	})
	flushSetOK(t, []string{c0.ID, c1.ID})

	js, err := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(js) != 1 {
		t.Fatalf("join produced %d jobs, want 1", len(js))
	}
	files, err := c.ListFiles(js[0].OutputCommit)
	if err != nil {
		t.Fatalf("list output files: %v", err)
	}
	var names []string
	for _, f := range files {
		names = append(names, f.Path)
	}
	sort.Strings(names)
	want := []string{"file-0.1100file-1.1100", "file-0.1110file-1.1110"}
	if fmt.Sprint(names) != fmt.Sprint(want) {
		t.Fatalf("output files = %v, want %v (only the join keys 1100 and 1110 present in both inputs)", names, want)
	}
}

func TestSB075_OuterJoinKeepsUnmatchedOuterOnly(t *testing.T) {
	r0, r1 := uniq(t)+"0", uniq(t)+"1"
	mustRepo(t, r0)
	mustRepo(t, r1)
	for i := 0; i < 8; i++ {
		putSingle(t, r0, fmt.Sprintf("%d", i))
		putSingle(t, r1, fmt.Sprintf("%d", i))
	}
	putSingle(t, r0, "foo")
	putSingle(t, r1, "bar")
	c0, _ := c.HeadCommit(r0, "master")
	c1, _ := c.HeadCommit(r1, "master")

	pipe := uniq(t)
	mustPipeline(t, client.Pipeline{
		Name: pipe,
		Transform: &client.Transform{
			Image: "alpine:3.21",
			Cmd:   []string{"sh", "-c", `out=""; if [ -d ${a} ]; then out="${out}$(ls -r ${a} | tr -d '\n')"; fi; if [ -d ${b} ]; then out="${out}$(ls -r ${b} | tr -d '\n')"; fi; touch ${OUT}/${out}`},
		},
		Input: &client.Input{Join: []client.Input{
			{Name: "a", Repo: r0, Glob: "/(*)", JoinOn: "$1", Outer: true},
			{Name: "b", Repo: r1, Glob: "/(*)", JoinOn: "$1"},
		}},
	})
	flushSetOK(t, []string{c0.ID, c1.ID})

	js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
	if len(js) != 1 {
		t.Fatalf("outer join produced %d jobs, want 1", len(js))
	}
	files, err := c.ListFiles(js[0].OutputCommit)
	if err != nil {
		t.Fatalf("list output files: %v", err)
	}
	names := map[string]bool{}
	for _, f := range files {
		names[f.Path] = true
	}
	for i := 0; i < 8; i++ {
		if !names[fmt.Sprintf("%d%d", i, i)] {
			t.Fatalf("missing matched output for key %d (have %v)", i, names)
		}
	}
	if !names["foo"] {
		t.Fatalf("missing the unmatched outer file foo (have %v)", names)
	}
	for _, f := range files {
		if containsStr(f.Path, "bar") {
			t.Fatalf("the unmatched inner file bar must not appear in the output")
		}
	}
}

func TestSB076_GroupInputs(t *testing.T) {
	t.Run("single input grouping", func(t *testing.T) {
		repo := uniq(t)
		mustRepo(t, repo)
		for i := 0; i < 16; i++ {
			putSingle(t, repo, "file."+spaceBinary(i))
		}
		cm, _ := c.HeadCommit(repo, "master")

		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine:3.21"},
			Input: &client.Input{Group: []client.Input{
				{Name: "a", Repo: repo, Glob: "/file.(?)(?)(?)(?)", GroupBy: "$3"},
			}},
			EnableStats: true,
		})
		flushOK(t, cm.ID)
		js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		pg, err := c.ListDatums(js[0].ID, 0, 0)
		if err != nil {
			t.Fatalf("list datums: %v", err)
		}
		sizes := map[int]bool{}
		for _, d := range pg.Datums {
			sizes[len(d.InputFiles)] = true
		}
		if !sizes[2] || !sizes[8] || !sizes[6] {
			t.Fatalf("group sizes = %v, want one datum each of 2, 8, and 6 files", sizes)
		}
	})

	t.Run("multi input grouping", func(t *testing.T) {
		r0, r1 := uniq(t)+"0", uniq(t)+"1"
		mustRepo(t, r0)
		mustRepo(t, r1)
		for i := 0; i < 16; i++ {
			putSingle(t, r0, fmt.Sprintf("file-0.%s", spaceBinary(i)))
			putSingle(t, r1, fmt.Sprintf("file-1.%s", spaceBinary(i)))
		}
		c0, _ := c.HeadCommit(r0, "master")
		c1, _ := c.HeadCommit(r1, "master")

		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine:3.21"},
			Input: &client.Input{Group: []client.Input{
				{Name: "a", Repo: r0, Glob: "/file-0.(?)(?)(?)(?)", GroupBy: "$3"},
				{Name: "b", Repo: r1, Glob: "/file-1.(?)(?)(?)(?)", GroupBy: "$2"},
			}},
			EnableStats: true,
		})
		flushSetOK(t, []string{c0.ID, c1.ID})
		js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		pg, err := c.ListDatums(js[0].ID, 0, 0)
		if err != nil {
			t.Fatalf("list datums: %v", err)
		}
		if len(pg.Datums) != 3 {
			t.Fatalf("got %d groups, want 3", len(pg.Datums))
		}
		sizes := map[int]bool{}
		for _, d := range pg.Datums {
			sizes[len(d.InputFiles)] = true
		}
		if !sizes[6] || !sizes[16] || !sizes[10] {
			t.Fatalf("group sizes = %v, want 6, 16, and 10", sizes)
		}
	})

	t.Run("group of join", func(t *testing.T) {
		r0, r1 := uniq(t)+"0", uniq(t)+"1"
		mustRepo(t, r0)
		mustRepo(t, r1)
		for i := 0; i < 16; i++ {
			// space-padded, as the reference's data: only the fully
			// unpadded names have reverses that pair (SB-076 subtest c)
			putSingle(t, r0, fmt.Sprintf("file-0.%s", spaceBinary(i)))
			putSingle(t, r1, fmt.Sprintf("file-1.%s", spaceBinary(i)))
		}
		c0, _ := c.HeadCommit(r0, "master")
		c1, _ := c.HeadCommit(r1, "master")

		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:      pipe,
			Transform: &client.Transform{Image: "alpine:3.21"},
			Input: &client.Input{Group: []client.Input{
				{Name: "a", Repo: r0, Glob: "/file-0.(?)(?)(?)(?)", JoinOn: "$1$2$3$4", GroupBy: "$3"},
				{Name: "b", Repo: r1, Glob: "/file-1.(?)(?)(?)(?)", JoinOn: "$4$3$2$1", GroupBy: "$2"},
			}},
			EnableStats: true,
		})
		flushSetOK(t, []string{c0.ID, c1.ID})
		js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		pg, err := c.ListDatums(js[0].ID, 0, 0)
		if err != nil {
			t.Fatalf("list datums: %v", err)
		}
		if len(pg.Datums) != 2 {
			t.Fatalf("got %d groups, want 2 (the joined pairs grouped)", len(pg.Datums))
		}
		for _, d := range pg.Datums {
			if len(d.InputFiles) != 4 {
				t.Fatalf("a group has %d files, want 4 (two whole pairs)", len(d.InputFiles))
			}
		}
	})

	t.Run("patid grouping with stats", func(t *testing.T) {
		repo := uniq(t)
		mustRepo(t, repo)
		for _, f := range []string{"a-PATID(123)-x.txt", "b-PATID(45)-x.txt", "c-PATID(45)-y.txt", "d-PATID(6)-x.txt", "e-PATID(78)-x.txt", "f-PATID(78)-y.txt"} {
			putSingle(t, repo, f)
		}
		cm, _ := c.HeadCommit(repo, "master")

		pipe := uniq(t)
		mustPipeline(t, client.Pipeline{
			Name:        pipe,
			Transform:   &client.Transform{Image: "alpine:3.21"},
			Input:       &client.Input{Group: []client.Input{{Name: "a", Repo: repo, Glob: "/*-PATID(*)-*.txt", GroupBy: "$1"}}},
			EnableStats: true,
		})
		flushOK(t, cm.ID)
		js, _ := c.ListJobsFiltered(client.JobFilter{Pipeline: pipe})
		if js[0].State != "success" {
			t.Fatalf("patid job state = %s (reason %q)", js[0].State, js[0].Reason)
		}
		pg, err := c.ListDatums(js[0].ID, 0, 0)
		if err != nil {
			t.Fatalf("list datums: %v", err)
		}
		sizes := map[int]int{}
		for _, d := range pg.Datums {
			sizes[len(d.InputFiles)]++
		}
		if sizes[1] != 2 || sizes[2] != 2 {
			t.Fatalf("group sizes = %v, want two singletons and two pairs (by PATID)", sizes)
		}
	})
}

// commitMany writes every name into one new commit on master.
func commitMany(t *testing.T, repo string, names []string) client.Commit {
	t.Helper()
	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start commit on %s: %v", repo, err)
	}
	for _, n := range names {
		if err := c.PutFile(cm.ID, n, []byte(n)); err != nil {
			t.Fatalf("put %s: %v", n, err)
		}
	}
	fin, err := c.FinishCommit(cm.ID, "", false)
	if err != nil {
		t.Fatalf("finish commit: %v", err)
	}
	return fin
}

func putSingle(t *testing.T, repo, name string) {
	t.Helper()
	cm, err := c.StartCommit(repo, "master", "")
	if err != nil {
		t.Fatalf("start commit: %v", err)
	}
	if err := c.PutFile(cm.ID, name, []byte(name)); err != nil {
		t.Fatalf("put %s: %v", name, err)
	}
	if _, err := c.FinishCommit(cm.ID, "", false); err != nil {
		t.Fatalf("finish commit: %v", err)
	}
}
