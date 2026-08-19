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
  return s || "stopped";
}
