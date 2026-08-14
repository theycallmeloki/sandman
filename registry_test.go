package main

import (
	"testing"
	"time"
)

// TestPruneExpiresEphemeralPeers: mdns and gossip (sync) peers that fall
// silent expire after mdnsStale; hand-edited static peers never do. The
// sync case is a regression: a gossip-merged peer was never pruned
// (observed live as conformance-daemon ghosts in the fleet view after
// their processes died).
func TestPruneExpiresEphemeralPeers(t *testing.T) {
	r := newRegistry(t.TempDir(), "me")
	old := time.Now().Add(-2 * mdnsStale)

	r.peers["mdns-ghost"] = &peer{Name: "mdns-ghost", Addr: "1.1.1.1:1", Source: "mdns", LastSeen: old}
	r.peers["sync-ghost"] = &peer{Name: "sync-ghost", Addr: "2.2.2.2:2", Source: "sync", LastSeen: old}
	r.peers["static-friend"] = &peer{Name: "static-friend", Addr: "3.3.3.3:3", Source: "static", LastSeen: old}
	r.peers["sync-live"] = &peer{Name: "sync-live", Addr: "4.4.4.4:4", Source: "sync", LastSeen: time.Now()}

	r.prune()

	for _, gone := range []string{"mdns-ghost", "sync-ghost"} {
		if _, ok := r.peers[gone]; ok {
			t.Errorf("%s survived the prune", gone)
		}
	}
	for _, kept := range []string{"static-friend", "sync-live"} {
		if _, ok := r.peers[kept]; !ok {
			t.Errorf("%s was pruned", kept)
		}
	}
}
