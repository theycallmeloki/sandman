// DAG: the whole fabric graph from /api/v1/dag — every pipeline and every
// repo it consumes or produces. Edges are plain SVG paths; nodes are HTML
// so their text wraps and clicks work without a graph library. The layout
// is a longest-path layering: sources at the left, each consumer one column
// to the right.
import { api, inputSummary, shortID } from "../shared.js";

const COL = 264;
const ROW = 100;
const NODE_W = 200;
const NODE_H = 78;
const PAD = 30;

// layeredLayout assigns every node a column by longest incoming path, then
// stacks nodes within a column. A cycle (which pipeline validation tries to
// forbid, but a stored graph may still carry) stops at nodeCount columns.
function layeredLayout(graph) {
  const nodes = graph.nodes || [];
  const edges = graph.edges || [];
  const pos = new Map();
  if (!nodes.length) return { nodes: [], edges: [], width: 420, height: 140 };

  const level = new Map(nodes.map((n) => [n.id, 0]));
  const incoming = new Map();
  for (const e of edges) {
    if (!incoming.has(e.to)) incoming.set(e.to, []);
    incoming.get(e.to).push(e.from);
  }
  const cap = nodes.length + 1;
  for (let iter = 0; iter <= cap; iter++) {
    let changed = false;
    for (const n of nodes) {
      let lv = 0;
      for (const src of incoming.get(n.id) || []) lv = Math.max(lv, (level.get(src) || 0) + 1);
      if (lv > cap) lv = cap;
      if (lv !== level.get(n.id)) {
        level.set(n.id, lv);
        changed = true;
      }
    }
    if (!changed) break;
  }

  const cols = [];
  for (const n of nodes) {
    const lv = level.get(n.id);
    while (cols.length <= lv) cols.push([]);
    cols[lv].push(n);
  }
  let maxRows = 0;
  cols.forEach((col, lv) => {
    col.sort((a, b) => (a.kind === b.kind ? a.name.localeCompare(b.name) : a.kind < b.kind ? -1 : 1));
    maxRows = Math.max(maxRows, col.length);
    col.forEach((n, i) => pos.set(n.id, { x: PAD + lv * COL, y: PAD + i * ROW }));
  });

  const laid = nodes.map((n) => ({ ...n, ...pos.get(n.id) }));
  const paths = edges
    .filter((e) => pos.has(e.from) && pos.has(e.to))
    .map((e) => {
      const a = pos.get(e.from);
      const b = pos.get(e.to);
      const x1 = a.x + NODE_W;
      const y1 = a.y + NODE_H / 2;
      const x2 = b.x;
      const y2 = b.y + NODE_H / 2;
      const bend = Math.max(24, Math.abs(x2 - x1) / 2);
      return {
        ...e,
        path: `M ${x1} ${y1} C ${x1 + bend} ${y1}, ${x2 - bend} ${y2}, ${x2} ${y2}`,
      };
    });

  return {
    nodes: laid,
    edges: paths,
    width: PAD * 2 + (cols.length - 1) * COL + NODE_W,
    height: PAD * 2 + Math.max(0, maxRows - 1) * ROW + NODE_H,
  };
}

export default {
  name: "DAG",
  data: () => ({
    graph: { nodes: [], edges: [], orphans: [] },
    selected: "",
    spec: null,
    specErr: "",
    err: "",
  }),
  mounted() {
    this.load();
    this.tick = setInterval(this.load, 5000);
  },
  beforeUnmount() { clearInterval(this.tick); },
  methods: {
    async load() {
      try {
        const g = await api("/dag");
        this.graph = {
          nodes: (g && g.nodes) || [],
          edges: (g && g.edges) || [],
          orphans: (g && g.orphans) || [],
        };
        if (this.selected && !this.graph.nodes.some((n) => n.id === this.selected)) {
          this.selected = "";
          this.spec = null;
        }
        this.err = "";
      } catch (e) {
        this.err = String(e.message || e);
      }
    },
    async select(n) {
      if (this.selected === n.id) {
        this.selected = "";
        this.spec = null;
        this.specErr = "";
        return;
      }
      this.selected = n.id;
      this.spec = null;
      this.specErr = "";
      if (n.kind !== "pipeline") return;
      try {
        const p = await api("/pipelines/" + encodeURIComponent(n.name));
        if (this.selected === n.id) this.spec = p;
      } catch (e) {
        if (this.selected === n.id) this.specErr = String(e.message || e);
      }
    },
    nodeClass(n) {
      return [
        "dag-node",
        n.kind,
        n.state || "",
        n.orphan ? "orphan" : "",
        this.selected === n.id ? "selected" : "",
      ].filter(Boolean).join(" ");
    },
    parCell(p) {
      if (!p) return "—";
      if (p.constant !== undefined && p.constant !== null && p.constant !== 0) return String(p.constant);
      if (p.coefficient) return String(p.coefficient);
      return "1";
    },
    branchLine(branches) {
      if (!branches || !branches.length) return "no commits";
      return branches.map((b) => b.branch + "@" + shortID(b.head)).join(" · ");
    },
    jobLink(n) {
      if (!n.lastJob || !n.lastJob.id) return "";
      return "#/pipelines/" + encodeURIComponent(n.name) + "/jobs/" + encodeURIComponent(n.lastJob.id);
    },
  },
  computed: {
    layout() { return layeredLayout(this.graph); },
    selectedNode() { return this.graph.nodes.find((n) => n.id === this.selected) || null; },
  },
  template: `
    <div v-if="err" class="error">{{ err }}</div>
    <section>
      <h2>Fabric DAG <span class="muted dag-count">{{ graph.nodes.length }} nodes · {{ graph.edges.length }} edges</span></h2>
      <div class="dag-wrap">
        <div class="dag-canvas" :style="{ width: layout.width + 'px', height: layout.height + 'px' }">
          <svg class="dag-edges" :width="layout.width" :height="layout.height">
            <path v-for="(e, i) in layout.edges" :key="i" :d="e.path" :class="'dag-edge ' + e.type"></path>
          </svg>
          <button v-for="n in layout.nodes" :key="n.id" type="button"
                  :class="nodeClass(n)"
                  :style="{ left: n.x + 'px', top: n.y + 'px', width: '200px', height: '78px' }"
                  :title="n.kind + ': ' + n.name"
                  @click="select(n)">
            <span class="dn-name">{{ n.name }}</span>
            <template v-if="n.kind === 'pipeline'">
              <span class="dn-meta">
                <span :class="'chip ' + (n.state || '')">{{ n.state || "unknown" }}</span>
                <span class="muted">v{{ n.version || "?" }} · {{ n.outputBranch || "master" }}</span>
              </span>
              <span class="dn-meta" v-if="n.cron">cron {{ n.cron }}</span>
              <span class="dn-meta" v-else-if="n.lastJob">last {{ n.lastJob.state }} {{ shortID(n.lastJob.id) }}</span>
              <span class="dn-meta muted" v-else>no jobs</span>
            </template>
            <template v-else>
              <span class="dn-meta" v-if="n.branches && n.branches.length">{{ branchLine(n.branches) }}</span>
              <span class="dn-meta" v-else-if="n.orphan">orphan repo</span>
              <span class="dn-meta muted" v-else>no commits</span>
            </template>
          </button>
        </div>
      </div>
      <p v-if="!graph.nodes.length" class="muted">no pipelines or repos yet — create one with the CLI</p>
      <p v-else class="muted dag-note">
        solid edges are repo → pipeline inputs; dashed edges are pipeline → output repos (named after the pipeline).
        click a node to inspect it.
      </p>
    </section>

    <section v-if="selectedNode">
      <h2>{{ selectedNode.name }}
        <span v-if="selectedNode.kind === 'pipeline'" :class="'chip ' + (selectedNode.state || '')">{{ selectedNode.state || "unknown" }}</span>
        <span v-else class="chip">repo</span>
      </h2>
      <div class="card">
        <div class="keyvals" v-if="selectedNode.kind === 'pipeline'">
          <span class="k">version</span><span>{{ selectedNode.version || "—" }}</span>
          <span class="k">output branch</span><span>{{ selectedNode.outputBranch || "master" }}</span>
          <span class="k">parallelism</span><span>{{ parCell(selectedNode.parallelism) }}</span>
          <span class="k">cron</span><span>{{ selectedNode.cron || "—" }}</span>
          <span class="k">last job</span>
            <span v-if="selectedNode.lastJob">
              <a :href="jobLink(selectedNode)">{{ shortID(selectedNode.lastJob.id) }}</a>
              <span :class="'chip ' + selectedNode.lastJob.state">{{ selectedNode.lastJob.state }}</span>
              <span class="muted">{{ selectedNode.lastJob.time }}</span>
            </span>
            <span v-else class="muted">—</span>
          <template v-if="spec">
            <span class="k">input</span><span>{{ inputSummary(spec.input) }}</span>
            <span class="k">image</span><span>{{ spec.transform && spec.transform.image || "—" }}</span>
            <span class="k">cmd</span><span>{{ spec.transform && spec.transform.cmd ? spec.transform.cmd.join(" ") : "—" }}</span>
            <span class="k">description</span><span class="muted">{{ spec.description || "—" }}</span>
          </template>
          <template v-if="specErr">
            <span class="k">spec</span><span class="error">{{ specErr }}</span>
          </template>
        </div>
        <div class="keyvals" v-else>
          <span class="k">repo</span><span>{{ selectedNode.name }}</span>
          <span class="k">branches</span><span>{{ branchLine(selectedNode.branches) }}</span>
          <span class="k">orphan</span><span>{{ selectedNode.orphan ? "yes" : "no" }}</span>
        </div>
      </div>
    </section>
  `,
};
