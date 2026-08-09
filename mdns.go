package main

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/grandcat/zeroconf"
)

// advertise publishes this node as a _sandman._tcp service — the Bonjour side
// of the fabric. Zero configuration: a node that runs `sandman daemon` is
// visible to the fleet; nothing to register, nothing to configure.
func advertise(name string, port int) (*zeroconf.Server, error) {
	txt := []string{
		"docker=" + dockerVersion(),
		"arch=" + runtime.GOARCH,
		"os=" + runtime.GOOS,
	}
	return zeroconf.Register(name, ServiceType, "local.", port, txt, nil)
}

// browse streams _sandman._tcp service entries until ctx is done.
// The channel is closed when the context ends.
func browse(ctx context.Context, ch chan<- *zeroconf.ServiceEntry) {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		close(ch)
		return
	}
	if err := resolver.Browse(ctx, ServiceType, "local.", ch); err != nil {
		close(ch)
	}
}

func firstAddr(e *zeroconf.ServiceEntry) string {
	if len(e.AddrIPv4) > 0 {
		return e.AddrIPv4[0].String()
	}
	if len(e.AddrIPv6) > 0 {
		return e.AddrIPv6[0].String()
	}
	return ""
}

func textValue(txt []string, key string) string {
	for _, kv := range txt {
		if k, v, ok := strings.Cut(kv, "="); ok && k == key {
			return v
		}
	}
	return ""
}

// mergeMdns folds one discovered service entry into the registry.
// TTL 0 is a goodbye: remove immediately. Otherwise the entry lives until
// its TTL lapses, then prune() drops it — a node that goes silent leaves the
// fleet on its own, no heartbeat protocol required.
func (r *registry) mergeMdns(e *zeroconf.ServiceEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e.Instance == r.own {
		return
	}
	if e.TTL == 0 {
		delete(r.peers, e.Instance)
		return
	}
	addr := firstAddr(e)
	if addr == "" {
		return
	}
	r.peers[e.Instance] = &peer{
		Name:     e.Instance,
		Addr:     fmt.Sprintf("%s:%d", addr, e.Port),
		Docker:   textValue(e.Text, "docker"),
		Source:   "mdns",
		LastSeen: time.Now(),
	}
}

// mdnsLookup resolves one instance name to host:port with a short browse.
func mdnsLookup(name string, timeout time.Duration) string {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ch := make(chan *zeroconf.ServiceEntry, 16)
	go browse(ctx, ch)
	for e := range ch {
		if e.Instance == name {
			if ip := firstAddr(e); ip != "" {
				return fmt.Sprintf("%s:%d", ip, e.Port)
			}
		}
	}
	return ""
}
