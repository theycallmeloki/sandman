// Datum: one datum's full record (input/output files, timing, reason)
// plus a best-effort viewer of its first output file from the job's
// output commit.
import { api, shortID } from "../shared.js";

export default {
  name: "Datum",
  props: {
    pipeline: { type: String, required: true },
    job: { type: String, required: true },
    datum: { type: String, required: true },
  },
  data: () => ({ j: null, info: null, out: null, outErr: "", err: "" }),
  mounted() { this.load(); },
  methods: {
    async load() {
      try {
        const [j, info] = await Promise.all([
          api("/jobs/" + encodeURIComponent(this.job)),
          api("/jobs/" + encodeURIComponent(this.job) + "/datums/" + encodeURIComponent(this.datum)),
        ]);
        this.j = j;
        this.info = info;
        this.err = "";
        await this.loadOutput();
      } catch (e) { this.err = String(e.message || e); }
    },
    async loadOutput() {
      this.out = null;
      this.outErr = "";
      const f = this.info && this.info.outputFiles && this.info.outputFiles[0];
      if (!f || !this.j || !this.j.outputCommit) return;
      const seg = f.path.split("/").map(encodeURIComponent).join("/");
      try {
        const r = await fetch("/api/v1/commits/" + encodeURIComponent(this.j.outputCommit) + "/files/" + seg);
        if (!r.ok) throw new Error("file fetch " + r.status);
        const text = await r.text();
        let json = null;
        try { json = JSON.parse(text); } catch {}
        this.out = { path: f.path, text, json };
      } catch (e) { this.outErr = String(e.message || e); }
    },
    fileRow(f) {
      return f ? f.path + (f.hash ? " (" + shortID(f.hash) + ")" : "") : "";
    },
  },
  computed: {
    state() { return this.info ? this.info.state : ""; },
  },
  template: `
    <div class="breadcrumb">
      <a href="#/">overview</a> / <a :href="'#/pipelines/' + encodeURIComponent(pipeline)">{{ pipeline }}</a> / <a :href="'#/pipelines/' + encodeURIComponent(pipeline) + '/jobs/' + encodeURIComponent(job)">{{ shortID(job) }}</a> / <span>{{ shortID(datum) }}</span>
    </div>
    <div v-if="err" class="error">{{ err }}</div>
    <template v-if="info">
      <section>
        <h2>{{ shortID(datum) }} <span :class="'chip ' + stateClass(state)">{{ state }}</span></h2>
        <div class="card">
          <div class="keyvals">
            <span class="k">state</span><span>{{ state }}</span>
            <span class="k">process time</span><span>{{ info.processTime ? Math.round(info.processTime) + "s" : "—" }}</span>
            <span class="k">started</span><span>{{ fmtTime(info.started) }}</span>
            <span class="k">finished</span><span>{{ fmtTime(info.finished) }}</span>
            <span class="k">worker</span><span>{{ info.worker !== undefined && info.worker !== null ? info.worker : "—" }}</span>
            <span class="k">reason</span><span>{{ info.reason || "—" }}</span>
            <span class="k">input files</span>
              <span><ul class="files" style="margin:0;padding-left:16px">
                <li v-for="f in (info.inputFiles || [])" :key="f.path">{{ fileRow(f) }}</li>
                <li v-if="!(info.inputFiles || []).length" class="muted">—</li>
              </ul></span>
            <span class="k">output files</span>
              <span><ul class="files" style="margin:0;padding-left:16px">
                <li v-for="f in (info.outputFiles || [])" :key="f.path">{{ fileRow(f) }}</li>
                <li v-if="!(info.outputFiles || []).length" class="muted">—</li>
              </ul></span>
          </div>
        </div>
      </section>
      <section v-if="out">
        <h2>Output: {{ out.path }}</h2>
        <pre v-if="out.json" class="json">{{ JSON.stringify(out.json, null, 2) }}</pre>
        <pre v-else class="json">{{ out.text }}</pre>
      </section>
      <p v-else-if="outErr" class="muted" style="font-size:12px">output viewer: {{ outErr }}</p>
    </template>
    <p v-else class="muted">loading…</p>
  `,
};
