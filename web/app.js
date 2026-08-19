// Read-only dashboard shell: hash router + reactive route state. Views
// are plain ES modules exporting Vue options objects; the router swaps
// them via <component :is>. Helpers live in shared.js so the shell and
// the views never import each other (no circular-import cycles). Vue is
// loaded from a CDN via the import map in index.html — no vendored
// build, no build step.
import { createApp, reactive } from "vue";
import { api, inputSummary, jobHref, shortID, relTime, dur, fmtTime, stateClass } from "./shared.js";
import Overview from "./views/Overview.js";
import Pipeline from "./views/Pipeline.js";
import Job from "./views/Job.js";
import Datum from "./views/Datum.js";

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
  { re: /^#\/?$/,
    view: Overview, props: () => ({}) },
];

const app = createApp({
  data: () => ({ state, version: "" }),
  mounted() {
    onHash();
    window.addEventListener("hashchange", onHash);
    api("/version").then((v) => { this.version = v.version || ""; }).catch(() => {});
  },
  beforeUnmount() {
    window.removeEventListener("hashchange", onHash);
  },
  template: `
    <header class="top">
      <h1><a href="#/">sandmand</a></h1>
      <span class="sub">{{ version ? "v" + version + " · " : "" }}read-only dashboard — writes go through the CLI</span>
      <span style="flex:1"></span>
      <span v-if="state.err" class="error">{{ state.err }}</span>
    </header>
    <main>
      <component :is="state.view" v-bind="state.props" v-if="state.view" />
      <p v-if="!state.view && state.err" class="error">{{ state.err }}</p>
    </main>
  `,
});

// Runtime-compiled templates resolve identifiers through the component
// instance, not module scope — shared helpers must be registered on
// globalProperties or every template call would be undefined.
for (const [k, v] of Object.entries({ inputSummary, jobHref, shortID, relTime, dur, fmtTime, stateClass })) {
  app.config.globalProperties[k] = v;
}

app.mount("#app");
