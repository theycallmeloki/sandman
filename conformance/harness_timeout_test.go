package conformance

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"sandman/client"
)

// $SANDMAN_TEST_TIMEOUT_FACTOR (issue #7) exists because the container-gated
// waits are tuned for a native Linux runner and overrun on Docker Desktop,
// where an overrun reads as a product failure. The harness's own polls are
// scaled by pollFor; the waits whose deadline travels to the *server* — a
// flush, a job wait — are scaled by testClient, and the server-side wait is
// the one that overruns first. This pins that contract at the wire: what the
// tests pass is what the request carries, times the factor. Without it the
// knob would silently cover only half the budgets, and the flake would
// survive the configuration meant to fix it.
func TestTimeoutFactorReachesServerDeadlines(t *testing.T) {
	var flushBody, jobWaitQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/v1/flush":
			var body struct {
				Timeout string `json:"timeout"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			flushBody = body.Timeout
			_, _ = w.Write([]byte(`{"jobs":[],"timedOut":false}`))
		case strings.HasSuffix(r.URL.Path, "/wait"):
			jobWaitQuery = r.URL.Query().Get("timeout")
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	t.Setenv("SANDMAN_TEST_TIMEOUT_FACTOR", "3")
	tc := &testClient{client.New(strings.TrimPrefix(srv.URL, "http://"))}

	if _, err := tc.FlushSet([]string{"cm1"}, 60*time.Second); err != nil {
		t.Fatalf("flush against the stub: %v", err)
	}
	if _, err := tc.WaitJob("job1", 30*time.Second); err != nil {
		t.Fatalf("job wait against the stub: %v", err)
	}

	if flushBody != "3m0s" {
		t.Fatalf("flush deadline on the wire = %q, want 3m0s (60s scaled by 3)", flushBody)
	}
	if jobWaitQuery != "1m30s" {
		t.Fatalf("job wait deadline on the wire = %q, want 1m30s (30s scaled by 3)", jobWaitQuery)
	}
}

// The default factor is 1: an unset environment keeps every budget exactly
// as written, so the suite's timings stay the tuned ones on a native runner.
func TestTimeoutFactorDefaultsToOne(t *testing.T) {
	t.Setenv("SANDMAN_TEST_TIMEOUT_FACTOR", "")
	if got := testTimeout(45 * time.Second); got != 45*time.Second {
		t.Fatalf("testTimeout(45s) with no factor = %s, want 45s", got)
	}
	t.Setenv("SANDMAN_TEST_TIMEOUT_FACTOR", "not-a-number")
	if got := testTimeout(45 * time.Second); got != 45*time.Second {
		t.Fatalf("testTimeout(45s) with a malformed factor = %s, want 45s", got)
	}
}
