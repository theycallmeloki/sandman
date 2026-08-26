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
//
// GPUs are the same registry's second scheduling axis: a worker advertises
// its devices as capacity, and a pipeline requests a count. The registry
// allocates specific device indices per job (never "all GPUs by default"),
// so parallel jobs get distinct devices; the allocation lives and dies
// with the registry like the host list itself.

import (
	"sort"
	"sync"
	"time"
)

// execHost is one registered execution host: a worker process that
// executes datums on the control plane's behalf and reports its own
// exec endpoint (the address the control plane pushes datum runs to).
type execHost struct {
	Name   string    `json:"name"`
	Addr   string    `json:"addr"`
	Labels []string  `json:"labels,omitempty"`
	Gpus   []GpuInfo `json:"gpus,omitempty"`
	Seen   string    `json:"seen"` // last heartbeat, RFC3339Nano
}

// GpuInfo is one GPU a worker advertises for allocation: the device index
// (as docker/CUDA enumerate it), the vendor-reported name, and its total
// memory. Busy is computed by the registry at list time — the device is
// currently allocated to a running job.
type GpuInfo struct {
	Index     int    `json:"index"`
	Name      string `json:"name,omitempty"`
	MemoryMiB int64  `json:"memoryMiB,omitempty"`
	Busy      bool   `json:"busy,omitempty"`
}

// hostRegistry tracks the joined execution hosts by name.
type hostRegistry struct {
	mu    sync.Mutex
	hosts map[string]*execHost
	// busy is each host's GPU device indices currently allocated to
	// running jobs. Like the registry itself it is in-memory only: a
	// control-plane restart drops it, and every worker re-registers
	// (re-asserting capacity) within a heartbeat. In-flight jobs never
	// survive the restart, so a fresh all-free registry is correct.
	busy map[string]map[int]bool
	ttl  time.Duration
}

func newHostRegistry(ttl time.Duration) *hostRegistry {
	return &hostRegistry{hosts: map[string]*execHost{}, busy: map[string]map[int]bool{}, ttl: ttl}
}

// register upserts a host's registration (a heartbeat refresh when the
// host is already known). The host's GPU capacity is refreshed from the
// registration; allocations for devices the host no longer reports are
// dropped, and allocations for devices it still reports survive the
// heartbeat (in-flight jobs keep their devices).
func (r *hostRegistry) register(name, addr string, labels []string, gpus []GpuInfo) execHost {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := &execHost{Name: name, Addr: addr, Labels: labels, Gpus: gpus, Seen: now()}
	r.hosts[name] = h
	if b, ok := r.busy[name]; ok {
		keep := map[int]bool{}
		for _, g := range gpus {
			if b[g.Index] {
				keep[g.Index] = true
			}
		}
		if len(keep) == 0 {
			delete(r.busy, name)
		} else {
			r.busy[name] = keep
		}
	}
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

// pickAndReserve returns a live host for the job: bearing the placement
// label (when label is non-empty) with at least wantGPU free GPU devices,
// and reserves those devices for the job's lifetime (released by
// release). A pipeline that requests GPUs never runs with fewer — and
// never with "all GPUs" — than it asked for: the registry hands out
// specific device indices, so parallel jobs get distinct devices.
// Deterministic: hosts are ordered by free-GPU count (most free first; a
// GPU-less request keeps plain name order) then name, so a stable fleet
// spreads GPU jobs and places non-GPU jobs as before. When no host
// satisfies the request nothing is reserved and ok is false — the
// pipeline waits (surfaced as the crashed state) for a host that can.
func (r *hostRegistry) pickAndReserve(label string, wantGPU int) (execHost, []int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	type cand struct {
		name string
		free []int
	}
	var cands []cand
	for n, h := range r.hosts {
		if !r.liveLocked(h) {
			continue
		}
		if label != "" {
			matched := false
			for _, l := range h.Labels {
				if l == label {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		var free []int
		for _, g := range h.Gpus {
			if !r.busy[n][g.Index] {
				free = append(free, g.Index)
			}
		}
		sort.Ints(free)
		cands = append(cands, cand{name: n, free: free})
	}
	sort.Slice(cands, func(i, j int) bool {
		if wantGPU > 0 && len(cands[i].free) != len(cands[j].free) {
			return len(cands[i].free) > len(cands[j].free)
		}
		return cands[i].name < cands[j].name
	})
	for _, c := range cands {
		if len(c.free) < wantGPU {
			continue
		}
		got := c.free[:wantGPU]
		if r.busy[c.name] == nil {
			r.busy[c.name] = map[int]bool{}
		}
		for _, g := range got {
			r.busy[c.name][g] = true
		}
		return *r.hosts[c.name], got, true
	}
	return execHost{}, nil, false
}

// release frees a job's reserved GPU devices back to its host.
func (r *hostRegistry) release(name string, gpus []int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(gpus) == 0 {
		return
	}
	b := r.busy[name]
	if b == nil {
		return
	}
	for _, g := range gpus {
		delete(b, g)
	}
	if len(b) == 0 {
		delete(r.busy, name)
	}
}

// drop forgets a host (operator deregistration, or a worker that left).
func (r *hostRegistry) drop(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.hosts, name)
	delete(r.busy, name)
}

// list returns every registered host, live or stale, with each device's
// busy state overlaid from the current allocations.
func (r *hostRegistry) list() []execHost {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]execHost, 0, len(r.hosts))
	for _, h := range r.hosts {
		c := *h
		if len(h.Gpus) > 0 {
			c.Gpus = make([]GpuInfo, len(h.Gpus))
			copy(c.Gpus, h.Gpus)
			for i := range c.Gpus {
				c.Gpus[i].Busy = r.busy[h.Name][c.Gpus[i].Index]
			}
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
