// Flow: the system as a chain — a live pulse row (is it moving? is
// anything wrong?), the pipeline DAG laid out by data flow (source
// repos -> pipelines -> downstream, edges derived from the output-repo
// naming convention), and recent activity. Auto-refresh.
//
// The chain is the dashboard's primary artifact: an operator of a
// pipeline system reasons in terms of data moving through stages, not
// in terms of an inventory of pipelines and jobs.
import { api, chainLayout, inputRepos, jobPulse, jobHref, shortID, relTime, dur } from "../shared.js";

// inspected live jobs: the progress snapshot rides inspect only, so the
// running/queued jobs are re-inspected each refresh, capped to bound
// the request rate
const LIVE_CAP = 8;

export default {
  name: "Flow",
  data: () => ({ pipelines: [], jobs: [], progs: {}, err: "" }),
  mounted() {
    this.load();
    this.tick = setInterval(this.load, 5000);
  },
  beforeUnmount() { clearInterval(this.tick); },
  methods: {
    async load() {
      try {
        const [p, j] = await Promise.all([
          api("/pipelines"),
          api("/jobs?history=0"),
        ]);
        this.pipelines = p || [];
        this.jobs = j || [];
        this.err = "";
        await this.loadProgress();
      } catch (e) { this.err = String(e.message || e); }
    },
    async loadProgress() {
      const live = this.jobs
        .filter((j) => j.state === "running" || j.state === "queued")
        .slice(0, LIVE_CAP);
      const out = {};
      await Promise.all(live.map(async (j) => {
        try {
          const insp = await api("/jobs/" + encodeURIComponent(j.id));
          out[j.id] = insp;
        } catch {}
      }));
      this.progs = out;
    },
    stateOf(p) { return p.state; },
    isPipeline(repo) { return this.pipeNames.has(repo); },
    nodeInputs(p) { return inputRepos(p.input); },
    driverOf(p) {
      if (p.spout) return "spout";
      if (p.input && p.input.cron) return "cron";
      if (p.input && p.input.git) return "git";
      if (p.input && p.input.trigger) return "size-trigger";
      return "";
    },
    // the progress snapshot of the pipeline's live job, if any
    liveProg(p) {
      for (const j of this.jobs) {
        if (j.pipeline !== p.name) continue;
        const x = this.progs[j.id];
        if (x && x.progress) return x.progress;
      }
      return null;
    },
    barPct(prog) {
      return prog && prog.total ? Math.round(prog.done / prog.total * 100) : 0;
    },
    // nodeLive: does the pipeline have a job in flight? (drives the
    // pulsing node border and the animated connector — the pipeline
    // state stays "running" even when idle, so only live jobs mean
    // flow)
    nodeLive(p) {
      return this.jobs.some((j) => j.pipeline === p.name && (j.state === "running" || j.state === "queued"));
    },
    // is any pipeline in levels[levelIdx] actually processing? (the
    // animated connector into that column)
    colLive(levelIdx) {
      const col = this.layout.levels[levelIdx];
      return !!(col && col.some((p) => this.nodeLive(p)));
    },
    // per-pipeline live job-state counts, tallied from the jobs listing
    // (the pipelines listing does not carry jobCounts — inspect does,
    // and inspecting every pipeline per refresh is wasteful)
    countsOf(p) {
      const acc = {};
      for (const j of this.jobs) {
        if (j.pipeline !== p.name) continue;
        acc[j.state] = (acc[j.state] || 0) + 1;
      }
      return Object.entries(acc).sort((a, b) => b[1] - a[1]).slice(0, 3);
    },
  },
  computed: {
    layout() { return chainLayout(this.pipelines); },
    pipeNames() { return new Set(this.pipelines.map((p) => p.name)); },
    pipelineCols() { return this.layout.levels.slice(1); },
    pulse() {
      const jp = jobPulse(this.jobs);
      // datums not yet done: in flight (running) plus still queued
      let queuedDatums = 0;
      for (const j of this.jobs) {
        const p = this.progs[j.id];
        if (p && p.progress) queuedDatums += p.progress.running + p.progress.queued;
      }
      const day = Date.now() - 24 * 3600 * 1000;
      let failedDatums = 0;
      for (const j of this.jobs) {
        const t = new Date(j.finished || "").getTime();
        if (!Number.isNaN(t) && t >= day) failedDatums += j.failed || 0;
      }
      return { jp, queuedDatums, failedDatums };
    },
    trouble() {
      return this.pipelines.filter((p) => p.state === "failure" || p.state === "crashed");
    },
    recentJobs() {
      return [...this.jobs]
        .sort((a, b) => (b.started || "").localeCompare(a.started || ""))
        .slice(0, 15);
    },
  },
  template: `
    <div v-if="err" class="error">{{ err }}</div>
    <section class="pulse">
      <div class="stat">
        <span :key="pulse.jp.running" class="num running tick">{{ pulse.jp.running }}</span>
        <span class="lbl">jobs running</span>
      </div>
      <div class="stat">
        <span :key="pulse.jp.queued" class="num queued tick">{{ pulse.jp.queued }}</span>
        <span class="lbl">jobs queued</span>
      </div>
      <div class="stat">
        <span :key="pulse.queuedDatums" class="num queued tick">{{ pulse.queuedDatums }}</span>
        <span class="lbl">datums in flight</span>
      </div>
      <div class="stat">
        <span :key="pulse.failedDatums" class="num tick" :class="{ failure: pulse.failedDatums > 0 }">{{ pulse.failedDatums }}</span>
        <span class="lbl">datums failed · 24h</span>
      </div>
      <div class="stat">
        <span :key="trouble.length" class="num tick" :class="{ failure: trouble.length > 0 }">{{ trouble.length }}</span>
        <span class="lbl">pipelines in trouble</span>
      </div>
    </section>

    <section>
      <h2>Flow</h2>
      <div class="chain" v-if="pipelines.length">
        <div class="chain-col" v-if="layout.sources.length">
          <div class="chain-col-head">sources</div>
          <span v-for="s in layout.sources" :key="s.repo" class="pill">{{ s.repo }}</span>
        </div>
        <template v-for="(col, i) in pipelineCols" :key="'col' + col.map((p) => p.name).join('+')">
          <div class="chain-arrow" :class="{ live: colLive(i + 1) }">▶</div>
          <div class="chain-col">
            <div v-for="p in col" :key="p.name" class="chain-node"
                 :class="{ running: nodeLive(p), trouble: stateOf(p) === 'failure' || stateOf(p) === 'crashed' }">
              <div class="node-head">
                <a :href="'#/pipelines/' + encodeURIComponent(p.name)"><b>{{ p.name }}</b></a>
                <span :class="'chip ' + stateOf(p)">{{ stateOf(p) }}</span>
              </div>
              <div class="node-in" v-if="nodeInputs(p).length">
                <span class="in-lbl">from</span>
                <a v-for="r in nodeInputs(p)" :key="r.repo" class="pill"
                   :class="{ link: isPipeline(r.repo) }"
                   :href="isPipeline(r.repo) ? '#/pipelines/' + encodeURIComponent(r.repo) : undefined">{{ r.repo }}<template v-if="r.branch && r.branch !== 'master'">@{{ r.branch }}</template></a>
              </div>
              <div class="node-in" v-else-if="driverOf(p)">
                <span class="in-lbl">driver</span>
                <span class="pill driver">{{ driverOf(p) }}</span>
              </div>
              <p v-if="p.description" class="muted node-desc">{{ p.description }}</p>
              <div class="node-live" v-if="liveProg(p)">
                <span class="minibar" :title="liveProg(p).done + '/' + liveProg(p).total + ' done, ' + liveProg(p).failed + ' failed'">
                  <span class="fill ok" :style="{ width: barPct(liveProg(p)) + '%' }"></span>
                </span>
                <span class="muted">{{ liveProg(p).done }}/{{ liveProg(p).total }}</span>
              </div>
              <div class="node-counts" v-if="countsOf(p).length">
                <span v-for="c in countsOf(p)" :key="c[0]" :class="'chip ' + c[0]">{{ c[1] }} {{ c[0] }}</span>
              </div>
              <p v-if="p.reason" class="reason">{{ p.reason }}</p>
            </div>
          </div>
        </template>
      </div>
      <p v-if="!pipelines.length" class="muted">no pipelines yet — create one with the CLI</p>
      <p class="muted chain-note">chain edges follow the output repo of each pipeline; a pipeline downstream of another lists it under "from"</p>
    </section>

    <section>
      <h2>Recent jobs</h2>
      <table>
        <thead>
          <tr><th>job</th><th>pipeline</th><th>state</th><th>outcome</th><th>started</th><th>duration</th><th>reason</th></tr>
        </thead>
        <tbody>
          <tr v-for="j in recentJobs" :key="j.id" :class="{ rowfail: j.state === 'failure' || j.state === 'killed' }">
            <td><a :href="jobHref(j)">{{ shortID(j.id) }}</a></td>
            <td>{{ j.pipeline }}</td>
            <td><span :class="'chip ' + j.state">{{ j.state }}</span></td>
            <td class="outcome">
              <span v-if="j.processed" class="ok">{{ j.processed }} ✓</span>
              <span v-if="j.recovered" class="rec">{{ j.recovered }} ↻</span>
              <span v-if="j.failed" class="bad">{{ j.failed }} ✗</span>
              <span v-if="j.skipped" class="muted">{{ j.skipped }} –</span>
              <span v-if="!j.processed && !j.recovered && !j.failed && !j.skipped" class="muted">—</span>
            </td>
            <td class="muted"><span :title="j.started" style="cursor:help">{{ relTime(j.started) }}</span></td>
            <td class="muted">{{ dur(j.started, j.finished) }}</td>
            <td class="muted" :title="j.reason">{{ j.reason || "" }}</td>
          </tr>
        </tbody>
      </table>
      <p v-if="!jobs.length" class="muted">no jobs yet</p>
    </section>
  `,
};
