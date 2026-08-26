// Pipeline: spec summary + job history with state filtering.
import { api } from "../shared.js";

export default {
  name: "Pipeline",
  props: { name: { type: String, required: true } },
  data: () => ({ p: null, jobs: [], progs: {}, filter: "", allVersions: false, err: "" }),
  mounted() {
    this.load();
    this.tick = setInterval(() => { if (this.live) this.load(); }, 5000);
  },
  beforeUnmount() { clearInterval(this.tick); },
  watch: { name() { this.load(); } },
  methods: {
    async load() {
      try {
        const list = await api("/pipelines?name=" + encodeURIComponent(this.name));
        this.p = list.find((x) => x.name === this.name) || null;
        let q = "/jobs?pipeline=" + encodeURIComponent(this.name);
        q += this.allVersions ? "&history=-1" : "&history=0";
        if (this.filter) q += "&state=" + encodeURIComponent(this.filter);
        this.jobs = await api(q);
        // live progress for the executing/queued jobs — the inspect
        // payload carries the progress snapshot; lightweight listings
        // stay lean without it
        const live = this.jobs.filter((j) => j.state === "running" || j.state === "queued");
        const out = {};
        await Promise.all(live.map(async (j) => {
          try { out[j.id] = await api("/jobs/" + encodeURIComponent(j.id)); } catch (e) { out[j.id] = null; }
        }));
        this.progs = out;
        this.err = "";
      } catch (e) { this.err = String(e.message || e); }
    },
    progOf(j) { const x = this.progs[j.id]; return x && x.progress; },
    barPct(p) { return p && p.total ? Math.round(p.done / p.total * 100) : 0; },
    setFilter(s) { this.filter = this.filter === s ? "" : s; this.load(); },
    toggleVersions() { this.allVersions = !this.allVersions; this.load(); },
    counts(jc) {
      if (!jc) return [];
      return Object.entries(jc).sort((a, b) => b[1] - a[1]);
    },
    stateOf() { return this.p.state; },
    envSummary(t) {
      if (!t || !t.env) return "";
      return Object.keys(t.env).join(", ");
    },
  },
  computed: {
    states() {
      const s = new Set(this.jobs.map((j) => j.state));
      return [...s].sort();
    },
    live() { return this.p && !this.p.stopped; },
  },
  template: `
    <div class="breadcrumb"><a href="#/flow">flow</a> / {{ name }}</div>
    <div v-if="err" class="error">{{ err }}</div>
    <template v-if="p">
      <section>
        <h2>{{ p.name }} <span :class="'chip ' + stateOf()">{{ stateOf() }}</span>
          <span v-if="p.reason" class="muted" style="text-transform:none;letter-spacing:0">— {{ p.reason }}</span>
        </h2>
        <div class="card" :class="{ running: stateOf() === 'running' }">
          <div class="keyvals">
            <span class="k">input</span><span>{{ inputSummary(p.input) }}</span>
            <span class="k">image</span><span>{{ p.transform && p.transform.image || "—" }}</span>
            <span class="k">cmd</span><span>{{ p.transform && p.transform.cmd ? p.transform.cmd.join(" ") : "—" }}</span>
            <span class="k">env</span><span class="muted">{{ envSummary(p.transform) || "—" }}</span>
            <span class="k">parallelism</span><span>{{ p.parallelism ? (p.parallelism.constant !== undefined ? p.parallelism.constant : JSON.stringify(p.parallelism)) : "—" }}</span>
            <span class="k">options</span>
              <span>
                <span v-if="p.reprocess" class="chip success">reprocess</span>
                <span v-if="p.service" class="chip queued">service</span>
                <span v-if="p.spout" class="chip queued">spout</span>
                <span v-if="p.enableStats" class="chip queued">stats</span>
                <span v-if="p.standby" class="chip queued">standby</span>
                <span v-if="p.autoscaling" class="chip queued">autoscale</span>
                <span v-if="!p.reprocess && !p.service && !p.spout" class="muted">—</span>
              </span>
            <span class="k">jobs</span>
              <span><span v-for="c in counts(p.jobCounts)" :key="c[0]" :class="'chip ' + c[0]">{{ c[1] }} {{ c[0] }}</span></span>
          </div>
        </div>
      </section>
      <section>
        <h2>Jobs
          <button class="filter" :class="{active: !filter}" @click="setFilter('')">all</button>
          <button v-for="s in states" :key="s" class="filter" :class="{active: filter === s}" @click="setFilter(s)">{{ s }}</button>
          <label class="toggle"><input type="checkbox" :checked="allVersions" @change="toggleVersions"> all versions</label>
        </h2>
        <table>
          <thead>
            <tr><th>job</th><th>state</th><th>progress</th><th>started</th><th>duration</th><th>proc</th><th>rec</th><th>fail</th><th>skip</th><th>reason</th></tr>
          </thead>
          <tbody>
            <tr v-for="j in jobs" :key="j.id">
              <td><a :href="jobHref(j)">{{ shortID(j.id) }}</a></td>
              <td><span :class="'chip ' + j.state">{{ j.state }}</span></td>
              <td>
                <span v-if="progOf(j) && progOf(j).total > 0" :title="progOf(j).done + '/' + progOf(j).total + ' done'">
                  <span class="minibar"><span class="fill ok" :style="{ width: barPct(progOf(j)) + '%' }"></span></span>
                  {{ progOf(j).done }}/{{ progOf(j).total }}
                </span>
                <span v-else class="muted">—</span>
              </td>
              <td class="muted"><span :title="fmtTime(j.started)" style="cursor:help">{{ relTime(j.started) }}</span></td>
              <td class="muted">{{ dur(j.started, j.finished) }}</td>
              <td>{{ j.processed }}</td>
              <td>{{ j.recovered }}</td>
              <td>{{ j.failed }}</td>
              <td>{{ j.skipped }}</td>
              <td class="muted" :title="j.reason">{{ j.reason || "" }}</td>
            </tr>
          </tbody>
        </table>
        <p v-if="!jobs.length" class="muted">no jobs</p>
      </section>
    </template>
    <p v-else class="muted">loading…</p>
  `,
};
