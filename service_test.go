package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestServiceProxyForwardsQuery pins M4: the proxy must forward the
// client's query string to the service — GET /api/v1/services/svc/x?a=1
// must reach the service as /x?a=1, not /x (a dropped query silently
// changes the service's behavior).
func TestServiceProxyForwardsQuery(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.RequestURI()
		w.WriteHeader(200)
	}))
	defer srv.Close()
	defer unregisterService("svc")
	registerService("svc", 0, 0, nil, strings.TrimPrefix(srv.URL, "http://"))

	req := httptest.NewRequest("GET", "http://sandman/api/v1/services/svc/x?a=1&b=two", nil)
	req.SetPathValue("pipeline", "svc")
	req.SetPathValue("path", "x")
	rec := httptest.NewRecorder()
	if err := (&daemon{}).serviceProxyH(rec, req); err != nil {
		t.Fatalf("proxy: %v", err)
	}
	if got != "/x?a=1&b=two" {
		t.Fatalf("service saw %q, want /x?a=1&b=two (query dropped)", got)
	}
}

// TestServiceProxyTimesOut pins M4: a wedged service must fail the
// proxy request (bounded client), not hold the handler goroutine and
// the client's connection forever.
func TestServiceProxyTimesOut(t *testing.T) {
	hang := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-hang // never respond
	}))
	defer srv.Close()
	defer close(hang)

	defer unregisterService("svc2")
	registerService("svc2", 0, 0, nil, strings.TrimPrefix(srv.URL, "http://"))

	old := serviceProxyClient
	serviceProxyClient = &http.Client{Timeout: 100 * time.Millisecond}
	defer func() { serviceProxyClient = old }()

	req := httptest.NewRequest("GET", "http://sandman/api/v1/services/svc2/x", nil)
	req.SetPathValue("pipeline", "svc2")
	req.SetPathValue("path", "x")
	rec := httptest.NewRecorder()
	start := time.Now()
	if err := (&daemon{}).serviceProxyH(rec, req); err == nil {
		t.Fatal("proxy returned nil over a hung service")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("proxy took %v, want ~the client timeout (100ms)", d)
	}
}
