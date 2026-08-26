// Attention: everything that needs a human, in one place, with reasons
// inline — no click-through to learn why something failed. Sections:
// pipelines in failure/crashed, failed/killed jobs, failed datums (from
// the newest failing jobs), and hosts whose heartbeat has lapsed. A
// clear system gets one confident line, not an empty page.
//
// Datums have no cross-job endpoint and the listing is failed-first
// ordered, so the failed datums of the newest failing jobs are pulled
// per job, capped — failures are rare in practice, so the request
// volume stays flat when nothing is wrong.
import { api, jobHref, shortID, relTime, hostStale, stateClass } from "../shared.js";

const DATUM_JOBS_CAP = 3;
const DATUM_LIMIT = 25;

export default {
  name: "Attention",
  data: () => ({ pipelines: [], jobs: [], hosts: [], datumJobs: {}, err: "" }),
  mounted() {
    this.load();
    this.tick = setInterval(this.load, 5000);
  },
  beforeUnmount() { clearInterval(this.tick); },
  methods: {
    async load() {
      try {
        const [p, j, h] = await Promise.all([
          api("/pipelines"),
          api("/jobs?history=0"),
          api("/hosts").catch(() => []),
        ]);
        this.pipelines = p || [];
        this.jobs = j || [];
        this.hosts = h || [];
        this.err = "";
        await this.loadFailedDatums();
      } catch (e) { this.err = String(e.message || e); }
    },
    // failed datums of the newest failing/killed jobs — the listing is
    // failed-first ordered, so page 0 carries them. Datum listings are
    // identity-only (no state, no reason) unless the pipeline records
    // per-datum statistics, so only stats-enabled pipelines are pulled;
    // the pipeline inspection is cheap because there are few.
    async loadFailedDatums() {
      const targets = this.problemJobs.slice(0, DATUM_JOBS_CAP);
      const pipeInfo = {};
      await Promise.all(targets.map(async (j) => {
        if (pipeInfo[j.pipeline] === undefined) {
          try {
            const info = await api("/pipelines/" + encodeURIComponent(j.pipeline));
            pipeInfo[j.pipeline] = !!info.enableStats;
          } catch { pipeInfo[j.pipeline] = false; }
        }
      }));
      const out = {};
      await Promise.all(targets.map(async (j) => {
        if (!pipeInfo[j.pipeline]) return;
        try {
          const dp = await api("/jobs/" + encodeURIComponent(j.id) + "/datums?limit=" + DATUM_LIMIT + "&page=0");
          out[j.id] = {
            job: j,
            datums: (dp.datums || []).filter((d) => d.state === "failed"),
          };
        } catch {}
      }));
      this.datumJobs = out;
    },
    counts(jc) {
      if (!jc) return [];
      return Object.entries(jc).sort((a, b) => b[1] - a[1]);
    },
  },
  computed: {
    problemPipelines() {
      return this.pipelines.filter((p) => p.state === "failure" || p.state === "crashed");
    },
    problemJobs() {
      return this.jobs
        .filter((j) => j.state === "failure" || j.state === "killed")
        .sort((a, b) => (b.started || "").localeCompare(a.started || ""));
    },
    failedDatumGroups() {
      return Object.values(this.datumJobs).filter((g) => g.datums.length);
    },
    staleHosts() {
      return this.hosts.filter(hostStale);
    },
    allClear() {
      return !this.problemPipelines.length && !this.problemJobs.length &&
             !this.failedDatumGroups.length && !this.staleHosts.length;
    },
  },
  template: `
    <div v-if="err" class="error">{{ err }}</div>
    <div v-if="allClear" class="allclear">
      <span class="big">ALL CLEAR</span>
      <span class="muted">{{ pipelines.length }} pipelines · no failed jobs · no failed datums · {{ hosts.length }} hosts heartbeating</span>
    </div>
    <template v-else>
      <section v-if="problemPipelines.length">
        <h2>Pipelines</h2>
        <div class="card" v-for="p in problemPipelines" :key="p.name" :class="{ trouble: true }">
          <a :href="'#/pipelines/' + encodeURIComponent(p.name)"><b>{{ p.name }}</b></a>
          <span :class="'chip ' + p.state">{{ p.state }}</span>
          <p class="reason" style="margin:6px 0 0">{{ p.reason || "no reason recorded" }}</p>
          <p v-if="counts(p.jobCounts).length" style="margin:6px 0 0">
            <span v-for="c in counts(p.jobCounts)" :key="c[0]" :class="'chip ' + c[0]">{{ c[1] }} {{ c[0] }}</span>
          </p>
        </div>
      </section>
      <section v-if="problemJobs.length">
        <h2>Failed jobs</h2>
        <table>
          <thead>
            <tr><th>job</th><th>pipeline</th><th>state</th><th>outcome</th><th>started</th><th>reason</th></tr>
          </thead>
          <tbody>
            <tr v-for="j in problemJobs" :key="j.id" class="rowfail">
              <td><a :href="jobHref(j)">{{ shortID(j.id) }}</a></td>
              <td>{{ j.pipeline }}</td>
              <td><span :class="'chip ' + j.state">{{ j.state }}</span></td>
              <td class="muted">{{ j.processed }} / {{ j.recovered }} / {{ j.failed }} / {{ j.skipped }}</td>
              <td class="muted">{{ relTime(j.started) }}</td>
              <td :title="j.reason">{{ j.reason || "" }}</td>
            </tr>
          </tbody>
        </table>
      </section>
      <section v-if="problemJobs.length && !failedDatumGroups.length">
        <h2>Failed datums</h2>
        <p class="muted">per-datum statistics are not enabled for the failing pipelines — set <code>enableStats</code> on the spec to inspect individual datums</p>
      </section>
      <section v-if="staleHosts.length">
        <h2>Hosts</h2>
        <div class="card" v-for="h in staleHosts" :key="h.name" :class="{ trouble: true }">
          <b>{{ h.name }}</b> <span class="muted">{{ h.addr }}</span>
          <p class="reason" style="margin:6px 0 0">no heartbeat since {{ relTime(h.seen) }} — worker may be down</p>
        </div>
      </section>
    </template>
  `,
};
