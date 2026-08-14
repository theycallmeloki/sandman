package cli

import "testing"

// TestListingGlob pins M5: the [:path] argument is a path, but the
// listing API accepts only prefix patterns (SB-047 clause 4) — a bare
// path was passed through and rejected ("unsupported listing glob").
// A path without its own wildcard becomes a prefix of the listing; one
// that already carries a * is passed through unchanged.
func TestListingGlob(t *testing.T) {
	cases := []struct{ in, want string }{
		{"subdir", "subdir*"},
		{"subdir/", "subdir/*"},
		{"a/b/c.txt", "a/b/c.txt*"},
		{"subdir/*.md", "subdir/*.md"},
		{"*.txt", "*.txt"},
		{"a*b", "a*b"},
	}
	for _, c := range cases {
		if got := listingGlob(c.in); got != c.want {
			t.Errorf("listingGlob(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
