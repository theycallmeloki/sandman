// Shared API client and format helpers. Kept in their own module so the
// views and the app shell never import each other — a circular import
// through the entry module leaves these bindings undefined at render
// time (views/Overview.js crashed with "inputSummary is not a function"
// when the first real datum rendered).
//
// The API client is GET-only by construction: the dashboard is read-only.

export async function api(path) {
  const r = await fetch("/api/v1" + path, { headers: { Accept: "application/json" } });
  if (!r.ok) {
    let msg = String(r.status);
    try {
      const j = await r.json();
      if (j && j.error) msg = j.error;
    } catch {}
    throw new Error(msg);
  }
  return r.json();
}

export function shortID(id) {
  return id && id.length > 12 ? id.slice(0, 12) + "…" : id;
}

export function relTime(iso) {
  if (!iso) return "—";
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return iso;
  const s = Math.max(0, Math.floor((Date.now() - t) / 1000));
  if (s < 5) return "just now";
  if (s < 60) return s + "s ago";
  if (s < 3600) return Math.floor(s / 60) + "m ago";
  if (s < 86400) return Math.floor(s / 3600) + "h ago";
  return Math.floor(s / 86400) + "d ago";
}

export function dur(started, finished) {
  if (!started) return "—";
  const a = new Date(started).getTime();
  const b = finished ? new Date(finished).getTime() : Date.now();
  if (Number.isNaN(a) || Number.isNaN(b) || b < a) return "—";
  const s = Math.floor((b - a) / 1000);
  if (s < 60) return s + "s";
  if (s < 3600) return Math.floor(s / 60) + "m " + (s % 60) + "s";
  return Math.floor(s / 3600) + "h " + Math.floor((s % 3600) / 60) + "m";
}

export function fmtTime(iso) {
  if (!iso) return "—";
  const t = new Date(iso);
  if (Number.isNaN(t.getTime())) return iso;
  return t.toISOString().slice(0, 19).replace("T", " ");
}

// inputSummary renders a pipeline's input as one compact line.
export function inputSummary(inp) {
  if (!inp) return "";
  if (inp.cross) return "cross: " + inp.cross.map((m) => m.name || m.repo).join(" + ");
  if (inp.join) return "join: " + inp.join.map((m) => m.name || m.repo).join(" + ");
  if (inp.group) return "group: " + inp.group.map((m) => m.name || m.repo).join(" + ");
  if (inp.union) return "union: " + inp.union.map((m) => m.name || m.repo).join(" + ");
  if (inp.repo) {
    let s = inp.repo + (inp.branch && inp.branch !== "master" ? "@" + inp.branch : "");
    if (inp.glob && inp.glob !== "/") s += ":" + inp.glob;
    return s;
  }
  if (inp.cron) return "cron " + inp.cron;
  if (inp.git) return "git " + (inp.git.url || "");
  if (inp.trigger) return "size-trigger";
  return JSON.stringify(inp).slice(0, 80);
}

export function jobHref(j) {
  return "#/pipelines/" + encodeURIComponent(j.pipeline) + "/jobs/" + encodeURIComponent(j.id);
}

export function stateClass(s) {
  return s || "";
}
// inputRepos flattens a pipeline input into its repo inputs. Cross/join/
// group/union members each contribute their repo; cron/git/trigger/spout
// contribute no repo (they are external drivers, rendered as source
// pills by the flow view).
export function inputRepos(inp) {
  if (!inp) return [];
  const out = [];
  const add = (m) => {
    const repo = (m && (m.repo || m.name)) || "";
    if (repo) out.push({ repo, branch: m.branch || "" });
  };
  if (inp.repo) add(inp);
  else if (inp.cross) inp.cross.forEach(add);
  else if (inp.join) inp.join.forEach(add);
  else if (inp.group) inp.group.forEach(add);
  else if (inp.union) inp.union.forEach(add);
  return out;
}

// chainLayout derives the pipeline chain from the pipeline list alone:
// a pipeline's output repo is named after the pipeline, so pipeline B is
// downstream of A exactly when one of B's input repos is A's name.
// Returns { levels, edges, sources } where levels[0] is the source
// (non-pipeline) repos and levels[i>0] are the pipelines at that depth,
// topologically ordered.
export function chainLayout(pipelines) {
  const byName = new Map(pipelines.map((p) => [p.name, p]));
  const level = new Map();
  const edges = [];
  const edgeKey = new Set();
  const sources = [];
  const seenSources = new Set();
  for (const p of pipelines) level.set(p.name, 1);
  // relaxation, cycle-safe: level(P) = 1 + max(level of the pipeline
  // producing each of P's input repos); cap at the pipeline count so a
  // (rejected at creation but conceivable) indirect cycle terminates
  const cap = pipelines.length + 1;
  let changed = true;
  while (changed) {
    changed = false;
    for (const p of pipelines) {
      let lv = level.get(p.name);
      for (const r of inputRepos(p.input)) {
        if (byName.has(r.repo)) {
          const k = r.repo + "\u0000" + p.name;
          if (!edgeKey.has(k)) {
            edgeKey.add(k);
            edges.push({ from: r.repo, to: p.name });
          }
          lv = Math.max(lv, level.get(r.repo) + 1);
        } else if (!seenSources.has(r.repo)) {
          seenSources.add(r.repo);
          sources.push(r);
        }
      }
      if (lv !== level.get(p.name)) {
        if (lv > cap) lv = cap;
        level.set(p.name, lv);
        changed = true;
      }
    }
  }
  const maxLevel = Math.max(1, ...level.values());
  const levels = [];
  for (let i = 0; i <= maxLevel; i++) levels.push([]);
  for (const p of pipelines) levels[level.get(p.name)].push(p);
  return { levels, edges, sources };
}

// jobPulse tallies the job states a lightweight listing carries. State
// vocabulary: running | queued | success | failure | killed | skipped.
export function jobPulse(jobs) {
  const p = { running: 0, queued: 0, failure: 0, killed: 0, success: 0, skipped: 0, paused: 0 };
  for (const j of jobs || []) {
    if (p[j.state] !== undefined) p[j.state] += 1;
  }
  return p;
}

// attentionCount is the header/nav "needs you" badge: pipelines in
// failure/crashed, jobs failed/killed, hosts with no heartbeat for more
// than a minute (the registry TTL is 30s, so that is well past dead).
export function attentionCount(pipelines, jobs, hosts) {
  let n = 0;
  for (const p of pipelines || []) {
    if (p.state === "failure" || p.state === "crashed") n += 1;
  }
  const pulse = jobPulse(jobs);
  n += pulse.failure + pulse.killed;
  for (const h of hosts || []) {
    if (hostStale(h)) n += 1;
  }
  return n;
}

// hostStale reports a host whose last heartbeat is older than 60s — the
// registry keeps stale hosts listed until dropped, so the dashboard can
// flag them before the operator would notice a missing worker.
export function hostStale(h) {
  if (!h || !h.seen) return false;
  const t = new Date(h.seen).getTime();
  if (Number.isNaN(t)) return false;
  return Date.now() - t > 60 * 1000;
}
