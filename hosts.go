package main

// Execution hosts. An operator designates execution hosts with placement
// labels; a pipeline may require a label, and its jobs run on a host that
// registered it. A host joins the cluster by establishing contact with the
// control plane using configuration set at host setup time (the worker's
// -control/-advertise flags) — the pipeline definition never enumerates a
// host address or identity.
//
// Hosts are ephemeral: registration carries a TTL that the worker's
// heartbeat refreshes, so a vanished worker stops being schedulable on its
// own. The registry is in-memory only — a control-plane restart drops the
// fleet, and each worker re-registers within a heartbeat. Placement is a
// live-cluster property, not durable state. A pipeline whose required
// label no live host bears surfaces the outage visibly (crashed with a
// recorded reason) rather than hanging, and its pending work re-places
// automatically once a host bearing the label registers.

import (
	"sort"
	"sync"
	"time"
)

// execHost is one registered execution host: a worker process that
// executes datums on the control plane's behalf and reports its own
// exec endpoint (the address the control plane pushes datum runs to).
type execHost struct {
	Name   string   `json:"name"`
	Addr   string   `json:"addr"`
	Labels []string `json:"labels,omitempty"`
	Seen   string   `json:"seen"` // last heartbeat, RFC3339Nano
}

// hostRegistry tracks the joined execution hosts by name.
type hostRegistry struct {
	mu    sync.Mutex
	hosts map[string]*execHost
	ttl   time.Duration
}

func newHostRegistry(ttl time.Duration) *hostRegistry {
	return &hostRegistry{hosts: map[string]*execHost{}, ttl: ttl}
}

// register upserts a host's registration (a heartbeat refresh when the
// host is already known).
func (r *hostRegistry) register(name, addr string, labels []string) execHost {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := &execHost{Name: name, Addr: addr, Labels: labels, Seen: now()}
	r.hosts[name] = h
	return *h
}

// liveLocked reports whether the host's heartbeat is fresh enough to
// schedule on.
func (r *hostRegistry) liveLocked(h *execHost) bool {
	if r.ttl <= 0 {
		return true
	}
	seen, err := time.Parse(time.RFC3339Nano, h.Seen)
	if err != nil {
		return false
	}
	return time.Since(seen) < r.ttl
}

// pick returns a live host bearing the label, or false when none is
// registered — a pipeline that requires the label waits for a host that
// bears it, since the pipeline definition never enumerates a host address
// or identity and the registry is the only scheduling input. The choice
// is deterministic (name order) so a stable fleet places the same
// pipeline on the same host.
func (r *hostRegistry) pick(label string) (execHost, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var names []string
	for n, h := range r.hosts {
		if !r.liveLocked(h) {
			continue
		}
		for _, l := range h.Labels {
			if l == label {
				names = append(names, n)
				break
			}
		}
	}
	if len(names) == 0 {
		return execHost{}, false
	}
	sort.Strings(names)
	return *r.hosts[names[0]], true
}

// drop forgets a host (operator deregistration, or a worker that left).
func (r *hostRegistry) drop(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.hosts, name)
}

// list returns every registered host, live or stale.
func (r *hostRegistry) list() []execHost {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]execHost, 0, len(r.hosts))
	for _, h := range r.hosts {
		out = append(out, *h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
