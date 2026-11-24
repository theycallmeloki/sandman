package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRegisterHostSendsControlPlaneAuth(t *testing.T) {
	var gotUser, gotPass string
	var saw bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/hosts" || r.Method != http.MethodPost {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		user, pass, ok := r.BasicAuth()
		gotUser, gotPass, saw = user, pass, ok
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="sandman"`)
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	addr := srv.URL
	cfg := authConfig{User: "worker-user", Password: "worker-pass"}
	if err := registerHost(addr, "worker-1", "10.0.0.2:9595", nil, nil, cfg); err != nil {
		t.Fatalf("registerHost with auth: %v", err)
	}
	if !saw || gotUser != cfg.User || gotPass != cfg.Password {
		t.Fatalf("credentials seen = %q/%q (present=%v), want %q/%q", gotUser, gotPass, saw, cfg.User, cfg.Password)
	}
}
