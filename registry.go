package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// peer is one node known to the fabric.
type peer struct {
	Name     string
	Addr     string // host:port
	Docker   string
	Role     string // "daemon" (control plane) | "worker" (execution host)
	Source   string // "mdns" (ephemeral) | "sync" (gossip) | "static" (hand-edited peers file)
	Version  string // the node's build version ("-" when unknown) — lets an operator spot lagging nodes
	LastSeen time.Time
}

// mdnsStale is how long a peer may stay silent before the fabric forgets it.
// The resolver's ServiceEntry.TTL is a cache hint, not wire TTL, so liveness
// is decided by last-seen, not by trusting that number.
const mdnsStale = 90 * time.Second

// registry is the daemon's in-memory view of the fleet.
// Static peers come from the editable peers file; mdns peers from browsing
// _sandman._tcp. The merged view is written back to the registry file so the
// state stays inspectable as plain text (Rule of Transparency).
type registry struct {
	mu    sync.Mutex
	dir   string
	own   string // advertised instance name, filtered out of browse results
	peers map[string]*peer
}

func newRegistry(dir, own string) *registry {
	return &registry{dir: dir, own: own, peers: map[string]*peer{}}
}

// loadStatic re-reads the peers file (hand edits, attach/detach).
// Format per line: "name addr [docker-version]". Static peers removed from
// the file are dropped from the in-memory view — no ghosts.
func (r *registry) loadStatic() error {
	b, err := os.ReadFile(filepath.Join(r.dir, "peers"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	inFile := map[string]bool{}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		inFile[f[0]] = true
		p := &peer{Name: f[0], Addr: f[1], Source: "static"}
		if len(f) > 2 {
			p.Docker = f[2]
		}
		r.peers[p.Name] = p
	}
	for k, p := range r.peers {
		if p.Source == "static" && !inFile[k] {
			delete(r.peers, k)
		}
	}
	return nil
}

// prune drops mdns peers that have gone silent. Static peers never expire.
func (r *registry) prune() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for k, p := range r.peers {
		if p.Source == "mdns" && now.Sub(p.LastSeen) > mdnsStale {
			delete(r.peers, k)
		}
	}
}

func (r *registry) list() []peer {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]peer, 0, len(r.peers))
	for _, p := range r.peers {
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// writeSnapshot rewrites the registry file: the fleet as plain text.
func (r *registry) writeSnapshot() {
	r.mu.Lock()
	defer r.mu.Unlock()
	var b strings.Builder
	b.WriteString("# sandman registry: mdns-discovered peers (ephemeral) + static peers (hand-edited)\n")
	b.WriteString("# name addr docker source seen role version\n")
	for _, p := range r.peers {
		seen := "-"
		if p.Source == "mdns" {
			seen = time.Since(p.LastSeen).Round(time.Second).String()
		}
		ver := p.Version
		if ver == "" {
			ver = "-"
		}
		fmt.Fprintf(&b, "%s %s %s %s %s %s %s\n", p.Name, p.Addr, p.Docker, p.Source, seen, p.Role, ver)
	}
	tmp := filepath.Join(r.dir, "registry.tmp")
	if err := os.WriteFile(tmp, []byte(b.String()), 0o644); err != nil {
		return
	}
	os.Rename(tmp, filepath.Join(r.dir, "registry"))
}

// mergeSync folds a peer learned over the wire (registry gossip) into the
// view. Sync peers are ephemeral like mdns peers: last-seen refreshed,
// staleness applies. Gossip is the safety net for mDNS's lossiness — the
// kernel delivers multicast to one shared-5353 socket per packet, so a
// specific peer pair can be starved; TCP pull of a peer's registry always
// converges (Rule of Robustness).
func (r *registry) mergeSync(name, addr, docker, role, version string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if name == r.own {
		return
	}
	if role == "" {
		role = "daemon"
	}
	r.peers[name] = &peer{Name: name, Addr: addr, Docker: docker, Role: role, Source: "sync", Version: version, LastSeen: time.Now()}
}

// addStatic records a manual peer in the peers file (the attach verb).
func addStatic(dir, name, addr string) error {
	path := filepath.Join(dir, "peers")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, _ := os.ReadFile(path)
	var out []string
	added := false
	for _, line := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
		f := strings.Fields(line)
		if len(f) >= 2 && f[0] == name {
			if !added {
				out = append(out, fmt.Sprintf("%s %s", name, addr))
				added = true
			}
			continue
		}
		out = append(out, line)
	}
	if !added {
		out = append(out, fmt.Sprintf("%s %s", name, addr))
	}
	return os.WriteFile(path, []byte(strings.Join(out, "\n")+"\n"), 0o644)
}

// removeStatic forgets a manual peer (the detach verb).
func removeStatic(dir, name string) error {
	path := filepath.Join(dir, "peers")
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var out []string
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) >= 2 && f[0] == name {
			continue
		}
		out = append(out, line)
	}
	return os.WriteFile(path, []byte(strings.Join(out, "\n")+"\n"), 0o644)
}
