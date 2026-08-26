// Read-only dashboard shell: hash router + reactive route state. Views
// are plain ES modules exporting Vue options objects; the router swaps
// them via <component :is>. Helpers live in shared.js so the shell and
// the views never import each other (no circular-import cycles). Vue is
// loaded from a CDN via the import map in index.html — no vendored
// build, no build step.
//
// The console is organized around the operator's questions, not the
// API's resource types: Flow (is it moving, and how does data flow),
// Attention (is anything wrong), Jobs (what happened), Fleet (what is
// out there). The header carries a live pulse on every view, so the
// health read never depends on which page you are on.
import { createApp, reactive } from "vue";
import { api, jobPulse, attentionCount } from "./shared.js";
import Flow from "./views/Flow.js";
import Attention from "./views/Attention.js";
import Jobs from "./views/Jobs.js";
import Fleet from "./views/Fleet.js";
import Pipeline from "./views/Pipeline.js";
import Job from "./views/Job.js";
import Datum from "./views/Datum.js";
import { inputSummary, jobHref, shortID, relTime, dur, fmtTime, stateClass } from "./shared.js";

// reactive at module scope: onHash mutates it OUTSIDE the component, so
// it must be a Vue reactive proxy itself — a plain object would be
// wrapped by data() and raw-target mutations would never notify.
const state = reactive({ view: null, props: {}, err: "" });

function onHash() {
  const h = location.hash || "#/";
  const m = routes.find((r) => r.re.test(h));
  if (!m) {
    state.view = null;
    state.err = "unknown route: " + h;
    return;
  }
  state.err = "";
  state.view = m.view;
  state.props = m.props(h.match(m.re));
}

const routes = [
  { re: /^#\/pipelines\/([^/]+)\/jobs\/([^/]+)\/datums\/([^/]+)$/,
    view: Datum, props: (m) => ({ pipeline: decodeURIComponent(m[1]), job: m[2], datum: m[3] }) },
  { re: /^#\/pipelines\/([^/]+)\/jobs\/([^/]+)$/,
    view: Job, props: (m) => ({ pipeline: decodeURIComponent(m[1]), job: m[2] }) },
  { re: /^#\/pipelines\/([^/]+)$/,
    view: Pipeline, props: (m) => ({ name: decodeURIComponent(m[1]) }) },
  { re: /^#\/jobs$/,
    view: Jobs, props: () => ({}) },
  { re: /^#\/fleet$/,
    view: Fleet, props: () => ({}) },
  { re: /^#\/attention$/,
    view: Attention, props: () => ({}) },
  { re: /^#\/?$/,
    view: Flow, props: () => ({}) },
];

const tabs = [
  { id: "flow", hash: "#/flow", label: "Flow" },
  { id: "attention", hash: "#/attention", label: "Attention" },
  { id: "jobs", hash: "#/jobs", label: "Jobs" },
  { id: "fleet", hash: "#/fleet", label: "Fleet" },
];

const viewTab = { Flow: "flow", Attention: "attention", Jobs: "jobs", Fleet: "fleet" };

const app = createApp({
  data: () => ({ state, tabs, version: "", clock: "", pulse: { running: 0, queued: 0, failure: 0, attention: 0 } }),
  mounted() {
    onHash();
    window.addEventListener("hashchange", onHash);
    api("/version").then((v) => { this.version = v.version || ""; }).catch(() => {});
    this.loadPulse();
    this.pulseTimer = setInterval(this.loadPulse, 5000);
    // a live UTC clock in the header — it never navigates, so a 1s tick is cheap
    this.tickClock();
    this.clockTimer = setInterval(this.tickClock, 1000);
  },
  beforeUnmount() {
    window.removeEventListener("hashchange", onHash);
    clearInterval(this.pulseTimer);
    clearInterval(this.clockTimer);
  },
  methods: {
    tickClock() {
      const d = new Date();
      const p = (n) => String(n).padStart(2, "0");
      this.clock = "UTC " + p(d.getUTCHours()) + ":" + p(d.getUTCMinutes()) + ":" + p(d.getUTCSeconds());
    },
    // the header pulse: a persistent health read that survives every
    // navigation — running/queued/failed job counts plus the attention
    // tally for the nav badge
    async loadPulse() {
      try {
        const [pipelines, jobs, hosts] = await Promise.all([
          api("/pipelines"),
          api("/jobs?history=0"),
          api("/hosts").catch(() => []),
        ]);
        const jp = jobPulse(jobs);
        this.pulse = {
          running: jp.running,
          queued: jp.queued,
          failure: jp.failure,
          attention: attentionCount(pipelines, jobs, hosts),
        };
      } catch {}
    },
  },
  computed: {
    tab() { return viewTab[this.state.view && this.state.view.name] || ""; },
  },
  template: `
    <header class="top">
      <h1><a href="#/flow">sandmand</a></h1>
      <nav class="tabs">
        <a v-for="t in tabs" :key="t.id" :href="'#' + t.hash" :class="{ active: tab === t.id }">
          {{ t.label }}
          <span v-if="t.id === 'attention' && pulse.attention > 0" class="badge">{{ pulse.attention }}</span>
        </a>
      </nav>
      <span class="hdr-pulse">
        <span class="p-num running">{{ pulse.running }}</span><span class="p-lbl">running</span>
        <span class="p-num queued">{{ pulse.queued }}</span><span class="p-lbl">queued</span>
        <span v-if="pulse.failure > 0" class="p-num failure">{{ pulse.failure }}</span><span v-if="pulse.failure > 0" class="p-lbl">failed</span>
      </span>
      <span v-if="state.err" class="error" style="margin-left:14px">{{ state.err }}</span>
      <span class="clock">{{ clock }}</span>
    </header>
    <main>
      <component :is="state.view" v-bind="state.props" v-if="state.view" />
      <p v-if="!state.view && state.err" class="error">{{ state.err }}</p>
    </main>
    <footer class="foot">
      <span class="live"><span class="dot"></span>LIVE</span>
      <span>sandmand · operations console</span>
      <span class="spacer"></span>
      <span class="pill">{{ version ? "build " + version : "" }}</span>
      <span>read-only — writes go through the CLI</span>
    </footer>
  `,
});

// Runtime-compiled templates resolve identifiers through the component
// instance, not module scope — shared helpers must be registered on
// globalProperties or every template call would be undefined.
for (const [k, v] of Object.entries({ inputSummary, jobHref, shortID, relTime, dur, fmtTime, stateClass })) {
  app.config.globalProperties[k] = v;
}

app.mount("#app");
