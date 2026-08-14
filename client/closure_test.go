package client

import (
	"math/rand"
	"testing"
)

// downstreamReference is the pre-M6 closure algorithm, verbatim: the
// indexed walk must produce byte-identical results (same jobs, same
// order) or a flush's intersection semantics change.
func downstreamReference(jobs []Job, commitIDs []string) []Job {
	if len(commitIDs) == 0 {
		return nil
	}
	type closure struct {
		set map[string]bool
		ord []string
	}
	byID := map[string]Job{}
	for _, j := range jobs {
		byID[j.ID] = j
	}
	cl := make([]closure, len(commitIDs))
	for i, id := range commitIDs {
		c := closure{set: map[string]bool{}}
		seen := map[string]bool{}
		var queue []string
		for _, j := range jobs {
			for _, ic := range j.InputCommits {
				if ic == id {
					seen[j.ID] = true
					c.set[j.ID] = true
					c.ord = append(c.ord, j.ID)
					if j.OutputCommit != "" {
						queue = append(queue, j.OutputCommit)
					}
					if j.StatsCommit != "" {
						queue = append(queue, j.StatsCommit)
					}
					break
				}
			}
		}
		for len(queue) > 0 {
			head := queue[0]
			queue = queue[1:]
			for _, j := range jobs {
				if seen[j.ID] || j.OutputCommit == "" {
					continue
				}
				for _, ic := range j.InputCommits {
					if ic == head {
						seen[j.ID] = true
						c.set[j.ID] = true
						c.ord = append(c.ord, j.ID)
						queue = append(queue, j.OutputCommit)
						if j.StatsCommit != "" {
							queue = append(queue, j.StatsCommit)
						}
						break
					}
				}
			}
		}
		cl[i] = c
	}
	first := cl[0]
	var out []Job
	for _, id := range first.ord {
		ok := true
		for _, c := range cl[1:] {
			if !c.set[id] {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, byID[id])
		}
	}
	return out
}

func ids(jobs []Job) []string {
	var out []string
	for _, j := range jobs {
		out = append(out, j.ID)
	}
	return out
}

func sameIDs(a, b []Job) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID {
			return false
		}
	}
	return true
}

// TestDownstreamJobsSetMatchesReference pins M6: the indexed closure walk
// must produce byte-identical results to the previous algorithm — same
// jobs, same order — across the intersection semantics (SB-056 wave 1),
// sink jobs (no output commit: not a downstream consumer), stats-commit
// chains, duplicate input commits, and random graphs.
func TestDownstreamJobsSetMatchesReference(t *testing.T) {
	chain := []Job{
		{ID: "A", InputCommits: []string{"ca"}, OutputCommit: "oa"},
		{ID: "B", InputCommits: []string{"oa"}, OutputCommit: "ob"},
		{ID: "C", InputCommits: []string{"ob"}, OutputCommit: "oc"},
	}
	sink := []Job{
		{ID: "A", InputCommits: []string{"ca"}, OutputCommit: "oa"},
		{ID: "S", InputCommits: []string{"ca"}}, // seeds, produces nothing
		{ID: "T", InputCommits: []string{"oa"}}, // consumes oa, produces nothing: not a downstream consumer
		{ID: "B", InputCommits: []string{"oa"}, OutputCommit: "ob"},
	}
	stats := []Job{
		{ID: "A", InputCommits: []string{"ca"}, OutputCommit: "oa", StatsCommit: "sa"},
		{ID: "B", InputCommits: []string{"sa"}, OutputCommit: "ob"},
	}
	dups := []Job{
		{ID: "A", InputCommits: []string{"ca", "ca"}, OutputCommit: "oa"},
	}
	cases := []struct {
		name    string
		jobs    []Job
		commits []string
	}{
		{"chain", chain, []string{"ca"}},
		{"chain mid", chain, []string{"ob"}},
		{"intersection", chain, []string{"ca", "ob"}}, // ca-closure ∩ ob-closure = C only
		{"pairing", chain, []string{"ca", "oa"}},      // ∩ = B, C
		{"sink seed", sink, []string{"ca"}},
		{"sink not consumer", sink, []string{"oa"}},
		{"stats chain", stats, []string{"ca"}},
		{"duplicate inputs", dups, []string{"ca"}},
		{"no match", chain, []string{"zz"}},
		{"empty commits", chain, nil},
		{"unrelated commit", chain, []string{"cx"}},
	}
	for _, c := range cases {
		got := DownstreamJobsSet(c.jobs, c.commits)
		want := downstreamReference(c.jobs, c.commits)
		if !sameIDs(got, want) {
			t.Errorf("%s: got %v, want %v", c.name, ids(got), ids(want))
		}
	}

	// random graphs: chained outputs/stats across a small commit pool
	rng := rand.New(rand.NewSource(42))
	pool := []string{"c0", "c1", "c2", "c3", "c4"}
	for iter := 0; iter < 300; iter++ {
		n := 1 + rng.Intn(12)
		var jobs []Job
		for i := 0; i < n; i++ {
			j := Job{ID: string(rune('A' + i))}
			k := 1 + rng.Intn(3)
			for x := 0; x < k; x++ {
				switch rng.Intn(3) {
				case 0: // a seed commit
					j.InputCommits = append(j.InputCommits, pool[rng.Intn(len(pool))])
				case 1: // an earlier job's output
					if i > 0 {
						j.InputCommits = append(j.InputCommits, "o"+string(rune('A'+rng.Intn(i))))
					}
				case 2: // an earlier job's stats commit
					if i > 0 {
						j.InputCommits = append(j.InputCommits, "s"+string(rune('A'+rng.Intn(i))))
					}
				}
			}
			if rng.Intn(4) > 0 { // some jobs produce output
				j.OutputCommit = "o" + j.ID
			}
			if rng.Intn(4) == 0 {
				j.StatsCommit = "s" + j.ID
			}
			jobs = append(jobs, j)
		}
		var commits []string
		for _, c := range pool {
			if rng.Intn(2) == 0 {
				commits = append(commits, c)
			}
		}
		if len(commits) == 0 {
			commits = pool[:1]
		}
		got := DownstreamJobsSet(jobs, commits)
		want := downstreamReference(jobs, commits)
		if !sameIDs(got, want) {
			t.Fatalf("iter %d: got %v, want %v (jobs %+v commits %v)", iter, ids(got), ids(want), jobs, commits)
		}
	}
}
