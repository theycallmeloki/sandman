// Jobs: the full cross-pipeline job ledger — filter by pipeline, state,
// and version depth; live jobs carry their progress bar (inspected, per
// the API's inspect-only progress snapshot).
import { api, jobHref, shortID, relTime, dur } from "../shared.js";

const LIVE_CAP = 12;
const JOB_STATES = ["running", "queued", "paused", "success", "failure", "killed", "skipped"];

export default {
  name: "Jobs",
  data: () => ({ pipelines: [], jobs: [], progs: {}, filter: "", pipeline: "", allVersions: false, err: "", JOB_STATES }),
  mounted() {
    this.load();
    this.tick = setInterval(() => { this.load(); }, 5000);
  },
  beforeUnmount() { clearInterval(this.tick); },
  watch: { filter() { this.load(); }, pipeline() { this.load(); }, allVersions() { this.load(); } },
  methods: {
    async load() {
      try {
        const [pl] = await Promise.all([api("/pipelines")]);
        this.pipelines = pl;
        const q = ["history=" + (this.allVersions ? "-1" : "0")];
        if (this.pipeline) q.push("pipeline=" + encodeURIComponent(this.pipeline));
        if (this.filter) q.push("state=" + encodeURIComponent(this.filter));
        this.jobs = await api("/jobs?" + q.join("&"));
        this.err = "";
        await this.loadProgress();
      } catch (e) { this.err = String(e.message || e); }
    },
    async loadProgress() {
      const live = this.jobs
        .filter((j) => j.state === "running" || j.state === "queued" || j.state === "paused")
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
    setFilter(s) { this.filter = this.filter === s ? "" : s; },
    progOf(j) { const x = this.progs[j.id]; return x && x.progress; },
    barPct(p) { return p && p.total ? Math.round(p.done / p.total * 100) : 0; },
  },
  template: `
    <div v-if="err" class="error">{{ err }}</div>
    <section>
      <h2>Jobs
        <select class="filter select" v-model="pipeline">
          <option value="">all pipelines</option>
          <option v-for="p in pipelines" :key="p.name" :value="p.name">{{ p.name }}</option>
        </select>
        <button class="filter" :class="{ active: !filter }" @click="setFilter('')">all</button>
        <button v-for="s in JOB_STATES" :key="s" class="filter" :class="{ active: filter === s }" @click="setFilter(s)">{{ s }}</button>
        <label class="toggle"><input type="checkbox" v-model="allVersions"> all versions</label>
      </h2>
      <table>
        <thead>
          <tr><th>job</th><th>pipeline</th><th>state</th><th>progress</th><th>started</th><th>duration</th><th>proc</th><th>rec</th><th>fail</th><th>skip</th><th>reason</th></tr>
        </thead>
        <tbody>
          <tr v-for="j in jobs" :key="j.id" :class="{ rowfail: j.state === 'failure' || j.state === 'killed' }">
            <td><a :href="jobHref(j)">{{ shortID(j.id) }}</a></td>
            <td>{{ j.pipeline }}</td>
            <td><span :class="'chip ' + j.state">{{ j.state }}</span></td>
            <td>
              <span v-if="progOf(j) && progOf(j).total > 0" :title="progOf(j).done + '/' + progOf(j).total + ' done'">
                <span class="minibar"><span class="fill ok" :style="{ width: barPct(progOf(j)) + '%' }"></span></span>
                {{ progOf(j).done }}/{{ progOf(j).total }}
              </span>
              <span v-else class="muted">—</span>
            </td>
            <td class="muted"><span :title="j.started" style="cursor:help">{{ relTime(j.started) }}</span></td>
            <td class="muted">{{ dur(j.started, j.finished) }}</td>
            <td>{{ j.processed }}</td>
            <td>{{ j.recovered }}</td>
            <td>{{ j.failed }}</td>
            <td>{{ j.skipped }}</td>
            <td class="muted" :title="j.reason">{{ j.reason || "" }}</td>
          </tr>
        </tbody>
      </table>
      <p v-if="!jobs.length" class="muted">no jobs match</p>
    </section>
  `,
};
