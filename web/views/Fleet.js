// Fleet: the infrastructure inventory — execution hosts (with heartbeat
// freshness), service pipelines, and build/version info. Auto-refresh.
import { api, relTime, hostStale } from "../shared.js";

export default {
  name: "Fleet",
  data: () => ({ hosts: [], services: [], version: "", err: "" }),
  mounted() {
    this.load();
    this.tick = setInterval(this.load, 5000);
  },
  beforeUnmount() { clearInterval(this.tick); },
  methods: {
    async load() {
      try {
        const [h, s, v] = await Promise.all([
          api("/hosts").catch(() => []),
          api("/services").catch(() => []),
          api("/version").catch(() => ({})),
        ]);
        this.hosts = h;
        this.services = s;
        this.version = v.version || "";
        this.err = "";
      } catch (e) { this.err = String(e.message || e); }
    },
  },
  template: `
    <div v-if="err" class="error">{{ err }}</div>
    <section>
      <h2>Hosts</h2>
      <table>
        <thead>
          <tr><th>host</th><th>addr</th><th>labels</th><th>gpus</th><th>last heartbeat</th></tr>
        </thead>
        <tbody>
          <tr v-for="h in hosts" :key="h.name" :class="{ rowfail: hostStale(h) }">
            <td><b>{{ h.name }}</b></td>
            <td class="muted">{{ h.addr }}</td>
            <td><span v-for="l in (h.labels || [])" :key="l" class="pill">{{ l }}</span><span v-if="!(h.labels || []).length" class="muted">—</span></td>
            <td><span v-for="g in (h.gpus || [])" :key="g.index" class="pill" :class="{ failure: g.busy }">GPU{{ g.index }} {{ g.name }}<template v-if="g.memoryMiB"> · {{ g.memoryMiB }}MiB</template><template v-if="g.busy"> · busy</template></span><span v-if="!(h.gpus || []).length" class="muted">—</span></td>
            <td class="muted">{{ relTime(h.seen) }}<span v-if="hostStale(h)" class="chip failure" style="margin-left:8px">no heartbeat</span></td>
          </tr>
        </tbody>
      </table>
      <p v-if="!hosts.length" class="muted">no execution hosts registered — jobs run on the control plane itself</p>
    </section>
    <section>
      <h2>Services</h2>
      <table>
        <thead>
          <tr><th>pipeline</th><th>port</th><th>annotations</th></tr>
        </thead>
        <tbody>
          <tr v-for="s in services" :key="s.pipeline">
            <td><a :href="'#/pipelines/' + encodeURIComponent(s.pipeline)"><b>{{ s.pipeline }}</b></a></td>
            <td class="muted">:{{ s.internalPort }} → :{{ s.externalPort }}</td>
            <td class="muted">{{ s.annotations ? JSON.stringify(s.annotations) : "—" }}</td>
          </tr>
        </tbody>
      </table>
      <p v-if="!services.length" class="muted">no service pipelines</p>
    </section>
    <section v-if="version">
      <h2>Build</h2>
      <div class="card"><span class="muted">build</span> <b>{{ version }}</b> · read-only console — writes go through the CLI</div>
    </section>
  `,
};
