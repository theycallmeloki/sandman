package main

import (
	"context"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/grandcat/zeroconf"
)

// advertise publishes this node as a _sandman._tcp service — the Bonjour side
// of the fabric. Zero configuration: a node that runs `sandman daemon` is
// visible to the fleet; nothing to register, nothing to configure. role
// distinguishes a control plane ("daemon") from an execution host
// ("worker") in the fleet view; addr, when non-empty, is the host:port
// consumers must dial (a worker advertises its -advertise address so the
// fleet never falls back to a loopback interface address from the browse).
func advertiseMDNS(name string, port int, role, addr string) (*zeroconf.Server, error) {
	txt := []string{
		"docker=" + dockerVersion(),
		"arch=" + runtime.GOARCH,
		"os=" + runtime.GOOS,
		"role=" + role,
		"ver=" + Version,
	}
	if addr != "" {
		txt = append(txt, "addr="+addr)
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
	// A node that advertises an explicit addr (a worker's -advertise) is
	// dialed there; the browse's interface addresses can be loopback or a
	// docker bridge, which would make peers dial the wrong host.
	addr := textValue(e.Text, "addr")
	if addr == "" {
		if ip := firstAddr(e); ip != "" {
			addr = net.JoinHostPort(ip, strconv.Itoa(e.Port))
		}
	}
	if addr == "" {
		return
	}
	role := textValue(e.Text, "role")
	if role == "" {
		role = "daemon"
	}
	r.peers[e.Instance] = &peer{
		Name:     e.Instance,
		Addr:     addr,
		Docker:   textValue(e.Text, "docker"),
		Role:     role,
		Source:   "mdns",
		Version:  textValue(e.Text, "ver"),
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
				return net.JoinHostPort(ip, strconv.Itoa(e.Port))
			}
		}
	}
	return ""
}

// discoverDaemon browses _sandman._tcp for a control plane (role=daemon)
// and returns its http URL, or "" when none responds within the timeout.
// The worker uses it when -control is unset: the daemon advertises
// role=daemon, so with one daemon on the LAN the worker joins it with
// zero configuration.
func discoverDaemon(timeout time.Duration) string {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ch := make(chan *zeroconf.ServiceEntry, 16)
	go browse(ctx, ch)
	for e := range ch {
		if textValue(e.Text, "role") != "daemon" {
			continue
		}
		addr := textValue(e.Text, "addr")
		if addr == "" {
			if ip := firstAddr(e); ip != "" {
				addr = net.JoinHostPort(ip, strconv.Itoa(e.Port))
			}
		}
		if addr != "" {
			return "http://" + addr
		}
	}
	return ""
}
