package main

import (
	"crypto/rand"
	"encoding/hex"

	"sandman/internal/store"
)

// defaultBranch is the wire-visible primary branch name; the definition
// lives in the store (DefaultBranch), this is the main-side spelling.
const defaultBranch = store.DefaultBranch

// now returns the current UTC time in the wire format every durable
// record timestamp uses (RFC3339Nano); the definition lives in the store
// (store.Now), this is the main-side spelling.
func now() string { return store.Now() }

// The closed state and reason vocabularies are consts, not literals:
// these strings are persisted in JSON records and asserted byte-for-byte
// by the conformance suite, so one spelling per vocabulary prevents a
// silent desync (a typo in a literal would change the wire contract).
const (
	stateRunning   = "running"
	stateStandby   = "standby"
	statePaused    = "paused"
	stateQueued    = "queued"
	stateFailure   = "failure"
	stateCrashed   = "crashed"
	stateStopped   = "stopped"
	stateSuccess   = "success"
	stateKilled    = "killed"
	stateFailed    = "failed"
	stateSkipped   = "skipped"
	stateRecovered = "recovered"

	reasonJobCancelled    = "job cancelled"
	reasonDaemonRestarted = "daemon restarted mid-job"
	reasonNoCommandStdin  = "no command specified but stdin lines provided"
)

// newID generates a unique id with a node prefix, an optional kind, and
// a random component. crypto/rand never fails (Go 1.24+), so the read is
// unchecked; the fabric RUN verb uses the same scheme (its old
// pid+nanos%1e6 id was a six-digit collision space).
func newID(node, kind string) string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	if kind == "" {
		return node + "-" + hex.EncodeToString(b)
	}
	return node + "-" + kind + "-" + hex.EncodeToString(b)
}
