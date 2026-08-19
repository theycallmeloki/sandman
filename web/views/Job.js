// Job: header (state, commits, outcome counts), paginated datum table,
// and a log tail with optional follow.
import { api } from "../shared.js";

const PAGE = 100;

export default {
  name: "Job",
  props: {
    pipeline: { type: String, required: true },
    job: { type: String, required: true },
  },
  data: () => ({ j: null, dp: null, page: 0, lines: [], follow: false, err: "", logErr: "" }),
  mounted() {
    this.load();
    this.loadLogs();
    this.tick = setInterval(() => {
      this.load();
      if (this.follow) this.loadLogs();
    }, 4000);
  },
  beforeUnmount() { clearInterval(this.tick); },
  // the router reuses this component instance across job→job
  // navigations (same component type): mounted() fires once, so a prop
  // change must reload — otherwise the datum table and the log tail
  // keep showing the previous job's data
  watch: {
    job() { this.page = 0; this.lines = []; this.load(); this.loadLogs(); },
  },
  methods: {
    async load() {
      try {
        const [j, dp] = await Promise.all([
          api("/jobs/" + encodeURIComponent(this.job)),
          api("/jobs/" + encodeURIComponent(this.job) + "/datums?limit=" + PAGE + "&page=" + this.page),
        ]);
        this.j = j;
        this.dp = dp;
        this.err = "";
      } catch (e) { this.err = String(e.message || e); }
    },
    async loadLogs() {
      try {
        const r = await api("/logs?job=" + encodeURIComponent(this.job));
        this.lines = r.lines || [];
        this.logErr = "";
      } catch (e) { this.logErr = String(e.message || e); }
    },
    setPage(p) {
      if (p < 0 || (this.dp && p >= this.dp.totalPages)) return;
      this.page = p;
      this.load();
    },
    toggleFollow() {
      this.follow = !this.follow;
      if (this.follow) this.loadLogs();
    },
    // joined in a method, not the template: an escaped "\n" inside the
    // template literal double-escapes through the template compiler's
    // expression parser and corrupts the generated render code
    logText() {
      return this.lines.join("\n");
    },
    // range and PAGE are module/instance state the template cannot see
    // (runtime-compiled templates resolve only component bindings)
    range() {
      if (!this.dp) return "—";
      const first = this.dp.page * PAGE + 1;
      const last = Math.min((this.dp.page + 1) * PAGE, this.total());
      return first + "–" + last + " of " + this.total();
    },
    total() {
      // the live progress snapshot carries the exact datum count; the
      // outcome counters only accumulate once the job finishes
      if (this.j && this.j.progress) return this.j.progress.total;
      if (!this.dp) return 0;
      if (!this.j) return 0;
      return (this.j.processed || 0) + (this.j.recovered || 0) +
             (this.j.failed || 0) + (this.j.skipped || 0);
    },
    // ---- live progress (see Job.Progress on the API) ----
    prog() { return this.j && this.j.progress; },
    pct(kind) {
      const p = this.prog();
      if (!p || !p.total) return 0;
      if (kind === "failed") return Math.round(p.failed / p.total * 100);
      // the green segment is the done portion that is NOT failed; the
      // red failed segment sits inside it, so the two never overlap
      return Math.round((p.done - p.failed) / p.total * 100);
    },
    fmtETA(sec) {
      if (!isFinite(sec) || sec <= 0) return "—";
      if (sec < 90) return "≈ " + Math.round(sec) + "s";
      if (sec < 5400) return "≈ " + Math.round(sec / 60) + " min";
      return "≈ " + (sec / 3600).toFixed(1) + " h";
    },
    etaText() {
      const p = this.prog();
      if (!p || !this.live) return "—";
      if (p.queued <= 0) return "all datums dispatched";
      if (!p.avgProcessTime || !this.j.workers || !this.j.workers.length) {
        return "warming up — no finished datums yet";
      }
      const eta = p.queued * p.avgProcessTime / this.j.workers.length;
      return this.fmtETA(eta) + " — " + p.queued + " queued × " +
        p.avgProcessTime.toFixed(1) + "s avg ÷ " + this.j.workers.length + " workers";
    },
    workerList() { return (this.j && this.j.workers) || []; },
    summary() {
      if (!this.j) return [];
      return [
        ["processed", this.j.processed, "success"],
        ["recovered", this.j.recovered, "recovered"],
        ["failed", this.j.failed, "failure"],
        ["skipped", this.j.skipped, "skipped"],
      ].filter((x) => x[1] > 0);
    },
  },
  computed: {
    state() { return this.j ? this.j.state : ""; },
    live() { return this.state === "running" || this.state === "queued" || this.state === "paused"; },
  },
  template: `
    <div class="breadcrumb">
      <a href="#/">overview</a> / <a :href="'#/pipelines/' + encodeURIComponent(pipeline)">{{ pipeline }}</a> / <span>{{ shortID(job) }}</span>
    </div>
    <div v-if="err" class="error">{{ err }}</div>
    <template v-if="j">
      <section>
        <h2>{{ shortID(j.id) }} <span :class="'chip ' + state">{{ state }}</span>
          <span v-if="j.reason" class="muted" style="text-transform:none;letter-spacing:0">— {{ j.reason }}</span>
        </h2>
        <div class="card">
          <div class="keyvals">
            <span class="k">started</span><span>{{ fmtTime(j.started) }} ({{ relTime(j.started) }})</span>
            <span class="k">finished</span><span>{{ fmtTime(j.finished) }}</span>
            <span class="k">duration</span><span>{{ dur(j.started, j.finished) }}</span>
            <span class="k">input commits</span><span>{{ (j.inputCommits || []).map(shortID).join(", ") || "—" }}</span>
            <span class="k">output commit</span><span>{{ shortID(j.outputCommit) || "—" }}</span>
            <span class="k">outcome</span>
              <span><span v-for="s in summary()" :key="s[0]" :class="'chip ' + s[2]">{{ s[1] }} {{ s[0] }}</span><span v-if="!summary().length" class="muted">—</span></span>
          </div>
        </div>
      </section>
      <section v-if="prog() && prog().total > 0">
        <h2>Progress
          <span class="muted" style="text-transform:none;letter-spacing:0">— {{ prog().done }}/{{ prog().total }} datums</span>
        </h2>
        <div class="card">
          <div class="progressbar" :title="prog().done + '/' + prog().total + ' done, ' + prog().failed + ' failed'">
            <div class="fill ok" :style="{ width: pct('ok') + '%' }"></div>
            <div class="fill bad" :style="{ width: pct('failed') + '%', left: pct('ok') + '%' }"></div>
          </div>
          <div class="keyvals">
            <span class="k">done</span>
              <span><span class="chip success">{{ prog().done }}</span> <span class="chip failure">{{ prog().failed }} failed</span></span>
            <span class="k">in progress</span>
              <span><span class="chip running">{{ prog().running }} running</span> <span class="chip queued">{{ prog().queued }} queued</span></span>
            <span class="k">avg / datum</span><span>{{ prog().avgProcessTime ? prog().avgProcessTime.toFixed(1) + "s" : "—" }}</span>
            <span class="k">eta</span><span>{{ etaText() }}</span>
            <span v-if="live" class="k">workers</span><span v-if="live" class="workers">
              <span v-for="w in workerList()" :key="w.worker" class="worker"
                    :title="w.datum ? 'processing ' + w.datum + (w.started ? ' since ' + w.started : '') : 'idle'">
                w{{ w.worker }}<span v-if="w.datum" class="muted"> · {{ shortID(w.datum) }}</span><span v-if="w.queue > 0" class="muted"> · {{ w.queue }} queued</span>
              </span>
              <span v-if="!workerList().length" class="muted">—</span>
            </span>
          </div>
        </div>
      </section>
      <section>
        <h2>Datums
          <span class="muted" style="text-transform:none;letter-spacing:0">— {{ range() }} (state-ordered: failed first)</span>
        </h2>
        <div class="pager" v-if="dp && dp.totalPages > 1">
          <button :disabled="page <= 0" @click="setPage(page - 1)">prev</button>
          <span class="muted">page {{ page + 1 }} / {{ dp.totalPages }}</span>
          <button :disabled="page >= dp.totalPages - 1" @click="setPage(page + 1)">next</button>
        </div>
        <table>
          <thead>
            <tr><th>datum</th><th>state</th><th>process time</th><th>worker</th><th>started</th><th>reason</th></tr>
          </thead>
          <tbody>
            <tr v-for="d in (dp ? (dp.datums || []) : [])" :key="d.id">
              <td>
                <a :href="'#/pipelines/' + encodeURIComponent(pipeline) + '/jobs/' + encodeURIComponent(job) + '/datums/' + encodeURIComponent(d.id)">{{ d.id }}</a>
              </td>
              <td><span :class="'chip ' + stateClass(d.state)">{{ d.state }}</span></td>
              <td>{{ d.processTime ? Math.round(d.processTime) + "s" : "—" }}</td>
              <td class="muted">{{ d.worker !== undefined && d.worker !== null ? d.worker : "—" }}</td>
              <td class="muted">{{ relTime(d.started) }}</td>
              <td class="muted" :title="d.reason">{{ d.reason || "" }}</td>
            </tr>
          </tbody>
        </table>
        <p v-if="dp && (!dp.datums || !dp.datums.length)" class="muted">no datums</p>
      </section>
      <section>
        <h2>Logs
          <label class="toggle"><input type="checkbox" :checked="follow" @change="toggleFollow"> follow</label>
        </h2>
        <div v-if="logErr" class="error">{{ logErr }}</div>
        <div class="logs">{{ logText() || "(no log lines)" }}</div>
      </section>
    </template>
    <p v-else class="muted">loading…</p>
  `,
};
