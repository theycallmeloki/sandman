package main

import (
	"net/http"
	"testing"
)

// TestDecodeBodyEmpty pins that a POST with no body decodes as an empty
// document instead of failing: POST /commits/{id}/finish with no -d is a
// legitimate "no description" request (G3 from the nanomilady exercise).
func TestDecodeBodyEmpty(t *testing.T) {
	req, err := http.NewRequest("POST", "http://x/api/v1/commits/abc/finish", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Body = http.NoBody // a real server presents NoBody for an empty POST
	var body struct {
		Description string `json:"description"`
		Empty       bool   `json:"empty"`
	}
	if err := decodeBody(req, &body); err != nil {
		t.Fatalf("decodeBody on an empty body must succeed: %v", err)
	}
	if body.Description != "" || body.Empty {
		t.Fatalf("empty body must decode to zero values, got %+v", body)
	}
}

// TestDecodeBodyEmptyChunked pins the same for chunked (ContentLength -1)
// requests that carry no bytes.
func TestDecodeBodyEmptyChunked(t *testing.T) {
	req, err := http.NewRequest("POST", "http://x/api/v1/commits/abc/finish", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = -1 // chunked: the decoder sees EOF on an empty stream
	req.Body = http.NoBody
	var body struct{ Description string }
	if err := decodeBody(req, &body); err != nil {
		t.Fatalf("decodeBody on an empty chunked body must succeed: %v", err)
	}
}
