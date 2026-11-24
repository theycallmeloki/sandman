package cli

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClientForAddrAuth(t *testing.T) {
	oldUser, oldPass := authUser, authPass
	t.Cleanup(func() { authUser, authPass = oldUser, oldPass })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "alice" || pass != "s3cret" {
			w.Header().Set("WWW-Authenticate", `Basic realm="sandman"`)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		_, _ = w.Write([]byte(`{"version":"test"}`))
	}))
	defer srv.Close()
	address := strings.TrimPrefix(srv.URL, "http://")

	SetAuth("", "")
	if _, err := ClientForAddr(address).Version(); err == nil {
		t.Fatal("unauthenticated CLI client must be rejected")
	}

	SetAuth("alice", "s3cret")
	if v, err := ClientForAddr(address).Version(); err != nil || v != "test" {
		t.Fatalf("authenticated CLI client: version %q, err %v", v, err)
	}
}
