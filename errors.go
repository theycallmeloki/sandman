package main

import (
	"errors"
	"fmt"
)

// Typed handler-error markers. The HTTP classifier (hErr) maps error
// TYPE to status: a "not found" marker is 404, an "internal" marker is
// 500, everything else is a client error (400). The marker rides in the
// error chain (errors.Is), never in the message text — a substring scan
// of the wording silently misclassified any error that happened to
// mention "not found", and daemon-side failures masqueraded as client
// errors. Messages stay exactly as the caller wrote them: logs, clients,
// and conformance assertions match on text and must not see it change.

var (
	errNotFound = errors.New("not found")
	errInternal = errors.New("internal")
)

// notFound marks a handler error as "the named resource does not exist"
// (HTTP 404). The returned error's message is the caller's message,
// byte-identical to an unwrapped fmt.Errorf.
func notFound(format string, args ...any) error {
	return &markerError{msg: fmt.Sprintf(format, args...), marker: errNotFound}
}

// internal marks a handler error as a daemon-side failure (HTTP 500):
// docker, network, or I/O errors — the request was fine, the daemon
// broke.
func internal(format string, args ...any) error {
	return &markerError{msg: fmt.Sprintf(format, args...), marker: errInternal}
}

type markerError struct {
	msg    string
	marker error
}

func (e *markerError) Error() string { return e.msg }
func (e *markerError) Unwrap() error { return e.marker }
