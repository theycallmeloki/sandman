package cli

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestDeltaVerb feeds a wire-contract payload on stdin and checks the
// verb prints the receiver's report JSON, including a non-applied case.
func TestDeltaVerb(t *testing.T) {
	f := newFakeDaemon()
	payload := `{"url":"https://github.com/theycallmeloki/example.git",` +
		`"branch":"master","revision":"abcdef1234567890abcdef12",` +
		`"base":"feedface0000111122223333",` +
		`"files":{"a.txt":"hi"},"deleted":[],"private":false}`
	out, _, code := runCLI(t, f, strings.NewReader(payload), "delta", "-")
	if code != 0 {
		t.Fatalf("delta exit %d (stdout %q)", code, out)
	}
	var res map[string]any
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("delta not JSON: %v (%q)", err, out)
	}
	if res["applied"] != true {
		t.Fatalf("delta applied = %v, want true", res["applied"])
	}
	if res["head"] != "feedface0000111122223333" {
		t.Fatalf("delta head = %v", res["head"])
	}
}

// TestDeltaVerbRejectedBase — a stale base is reported (applied=false),
// not surfaced as a process error.
func TestDeltaVerbRejectedBase(t *testing.T) {
	f := newFakeDaemon()
	payload := `{"url":"https://github.com/theycallmeloki/example.git",` +
		`"branch":"master","revision":"abcdef1234567890abcdef12",` +
		`"base":"stale","files":{},"deleted":[]}`
	out, _, code := runCLI(t, f, strings.NewReader(payload), "delta", "-")
	if code != 0 {
		t.Fatalf("delta exit %d, want 0 (applied is data): %q", code, out)
	}
	var res map[string]any
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("delta not JSON: %v (%q)", err, out)
	}
	if res["applied"] != false {
		t.Fatalf("delta applied = %v, want false", res["applied"])
	}
	if res["reason"] != "base mismatch" {
		t.Fatalf("delta reason = %v", res["reason"])
	}
}
