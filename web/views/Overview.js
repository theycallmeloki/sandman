// Overview: fleet state at a glance — pipeline cards with live job-state
// counts, recent jobs across pipelines, hosts and services. Auto-refresh.
import { api } from "../shared.js";

export default {
  name: "Overview",
  data: () => ({ pipelines: [], jobs: [], hosts: [], services: [], err: "" }),
  mounted() {
    this.load();
    this.tick = setInterval(this.load, 5000);
  },
  beforeUnmount() { clearInterval(this.tick); },
  methods: {
    async load() {
      try {
        const [p, j, h, s] = await Promise.all([
          api("/pipelines"),
          api("/jobs?history=0"),
          api("/hosts").catch(() => []),
          api("/services").catch(() => []),
        ]);
        this.pipelines = p;
        this.jobs = j;
        this.hosts = h;
        this.services = s;
        this.err = "";
      } catch (e) { this.err = String(e.message || e); }
    },
    counts(jc) {
      if (!jc) return [];
      return Object.entries(jc).sort((a, b) => b[1] - a[1]);
    },
    stateOf(p) { return p.state; },
  },
  computed: {
    recentJobs() {
      return [...this.jobs]
        .sort((a, b) => (b.started || "").localeCompare(a.started || ""))
        .slice(0, 40);
    },
  },
  template: `
    <div v-if="err" class="error">{{ err }}</div>
    <section>
      <h2>Pipelines</h2>
      <div class="grid">
        <div v-for="p in pipelines" :key="p.name" class="card" :class="{ running: stateOf(p) === 'running' }">
          <a :href="'#/pipelines/' + encodeURIComponent(p.name)"><b>{{ p.name }}</b></a>
          <span :class="'chip ' + stateOf(p)">{{ stateOf(p) }}</span>
          <p v-if="p.description" class="muted" style="margin:6px 0 0">{{ p.description }}</p>
          <p class="muted" style="margin:6px 0 0;font-size:12px">{{ inputSummary(p.input) }}</p>
          <p v-if="counts(p.jobCounts).length" style="margin:8px 0 0">
            <span v-for="c in counts(p.jobCounts)" :key="c[0]" :class="'chip ' + c[0]">{{ c[1] }} {{ c[0] }}</span>
          </p>
        </div>
        <div v-if="!pipelines.length" class="card muted">no pipelines yet</div>
      </div>
    </section>
    <section>
      <h2>Recent jobs</h2>
      <table>
        <thead>
          <tr><th>job</th><th>pipeline</th><th>state</th><th>started</th><th>duration</th><th>proc</th><th>rec</th><th>fail</th><th>skip</th><th>reason</th></tr>
        </thead>
        <tbody>
          <tr v-for="j in recentJobs" :key="j.id">
            <td><a :href="jobHref(j)">{{ shortID(j.id) }}</a></td>
            <td>{{ j.pipeline }}</td>
            <td><span :class="'chip ' + j.state">{{ j.state }}</span></td>
            <td class="muted">{{ relTime(j.started) }}</td>
            <td class="muted">{{ dur(j.started, j.finished) }}</td>
            <td>{{ j.processed }}</td>
            <td>{{ j.recovered }}</td>
            <td>{{ j.failed }}</td>
            <td>{{ j.skipped }}</td>
            <td class="muted" :title="j.reason">{{ j.reason || "" }}</td>
          </tr>
        </tbody>
      </table>
      <p v-if="!jobs.length" class="muted">no jobs yet</p>
    </section>
    <section v-if="hosts.length || services.length">
      <h2>Fleet</h2>
      <div class="grid">
        <div class="card" v-for="h in hosts" :key="h.name">
          <b>{{ h.name }}</b> <span class="muted">{{ h.addr }}</span>
        </div>
        <div class="card" v-for="s in services" :key="s.pipeline">
          <b>{{ s.pipeline }}</b> <span class="muted">:{{ s.internalPort }} → :{{ s.externalPort }}</span>
        </div>
      </div>
    </section>
  `,
};
