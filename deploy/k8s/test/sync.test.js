// Unit tests for the sandman-worker DecoratorController sync hook.
//
// The hook logic lives inside deploy/k8s/hook-configmap.yaml (data.sync.js)
// — the single source of truth ArgoCD applies. This suite extracts that
// exact script, runs it under Node's require() with realistic Node
// fixtures (taken from the live cluster), and asserts the generated
// worker Pod. Run: node --test deploy/k8s/test/
'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const root = path.join(__dirname, '..');
const yaml = fs.readFileSync(path.join(root, 'hook-configmap.yaml'), 'utf8');

// extract the sync.js block: lines after "  sync.js: |" indented 4 spaces
function extractSyncJs(yamlText) {
  const lines = yamlText.split('\n');
  const out = [];
  let inBlock = false;
  for (const line of lines) {
    if (line === '  sync.js: |') { inBlock = true; continue; }
    if (inBlock) {
      if (line.startsWith('    ')) out.push(line.slice(4));
      else if (line.trim() === '') continue;
      else break;
    }
  }
  if (out.length === 0) throw new Error('sync.js block not found in hook-configmap.yaml');
  return out.join('\n');
}

const hookFile = path.join(os.tmpdir(), `sandman-sync-${process.pid}.js`);
fs.writeFileSync(hookFile, extractSyncJs(yaml));
const hook = require(hookFile);

// --- fixtures (shapes mirror the live cluster's Node objects) ---

const workerNode = {
  metadata: {
    name: 'talos-ga5-yk4',
    labels: { 'kubernetes.io/hostname': 'talos-ga5-yk4' },
  },
  spec: { taints: null },
  status: {
    addresses: [
      { address: '192.168.1.97', type: 'InternalIP' },
      { address: 'talos-ga5-yk4', type: 'Hostname' },
    ],
    capacity: { cpu: '4' },
  },
};

const controlPlaneNode = {
  metadata: {
    name: 'talos-8qv-27y',
    labels: { 'node-role.kubernetes.io/control-plane': '' },
  },
  spec: { taints: [{ effect: 'NoSchedule', key: 'node-role.kubernetes.io/control-plane' }] },
  status: { addresses: [{ address: '192.168.1.42', type: 'InternalIP' }] },
};

// control plane identified by taint alone (no label)
const taintOnlyCpNode = JSON.parse(JSON.stringify(controlPlaneNode));
delete taintOnlyCpNode.metadata.labels['node-role.kubernetes.io/control-plane'];

const gpuByLabelNode = JSON.parse(JSON.stringify(workerNode));
gpuByLabelNode.metadata.labels['sandman/gpu'] = 'true';
gpuByLabelNode.metadata.labels['sandman/placement'] = 'ml';

const gpuByCapacityNode = JSON.parse(JSON.stringify(workerNode));
gpuByCapacityNode.status.capacity['nvidia.com/gpu'] = '1';

const labeledNode = JSON.parse(JSON.stringify(workerNode));
labeledNode.metadata.labels['sandman/placement'] = 'k8s';
labeledNode.metadata.labels['sandman/class'] = 'ml';
labeledNode.metadata.labels['unrelated'] = 'ignored';

const noIpNode = JSON.parse(JSON.stringify(workerNode));
noIpNode.status.addresses = [{ address: 'talos-ga5-yk4', type: 'Hostname' }];

// decorator sync request shape: the decorated object is `object`, the
// response carries desired pods in `attachments` (a list)
async function sync(node) {
  return hook({ request: { body: { object: node, attachments: {}, related: {}, finalizing: false } } });
}

const BASE_ARGS = [
  'worker',
  '-name', 'talos-ga5-yk4',
  '-control', 'http://192.168.1.15:4242',
  '-advertise', '192.168.1.97:9595',
  '-port', '9595',
];

test('worker node produces exactly one pinned hostNetwork pod', async () => {
  const res = await sync(workerNode);
  assert.strictEqual(res.status, 200);
  const [pod] = res.body.attachments;
  assert.strictEqual(res.body.attachments.length, 1);
  assert.strictEqual(pod.kind, 'Pod');
  assert.strictEqual(pod.metadata.name, 'sandman-worker-talos-ga5-yk4');
  assert.strictEqual(pod.metadata.namespace, 'sandman');
  assert.strictEqual(pod.metadata.labels.app, 'sandman-worker');
  assert.strictEqual(pod.spec.hostNetwork, true);
  assert.strictEqual(pod.spec.nodeName, 'talos-ga5-yk4');
  // must not auto-mount a service-account token: the random-suffixed
  // kube-api-access volume makes every Recreate dry-run differ
  // (infinite delete/recreate loop)
  assert.strictEqual(pod.spec.automountServiceAccountToken, false);
});

test('worker args: name/control/advertise/port, no labels when none set', async () => {
  const res = await sync(workerNode);
  const worker = res.body.attachments[0].spec.containers.find((c) => c.name === 'worker');
  assert.deepStrictEqual(worker.args, BASE_ARGS);
});

test('worker env: DOCKER_HOST loopback dind + TMPDIR scratch', async () => {
  const res = await sync(workerNode);
  const worker = res.body.attachments[0].spec.containers.find((c) => c.name === 'worker');
  const env = Object.fromEntries(worker.env.map((e) => [e.name, e.value]));
  // unix socket on a shared volume: no TCP listener, so dockerd's
  // per-restart port state can never block startup
  assert.strictEqual(env.DOCKER_HOST, 'unix:///var/run/docker/docker.sock');
  assert.strictEqual(env.TMPDIR, '/scratch');
});

test('plain node gets docker:dind sidecar, loopback-only, privileged', async () => {
  const res = await sync(workerNode);
  const dind = res.body.attachments[0].spec.containers.find((c) => c.name === 'dind');
  assert.strictEqual(dind.image, 'docker:dind');
  // command bypasses dockerd-entrypoint.sh, which would append its own
  // unix:/var/run/docker.sock + tcp://0.0.0.0:2375 defaults — the TCP
  // listener would expose an unauthenticated daemon on the node
  assert.deepStrictEqual(dind.command, ['dockerd']);
  assert.deepStrictEqual(dind.args, ['--host=unix:///var/run/docker/docker.sock']);
  assert.strictEqual(dind.securityContext.privileged, true);
  // docker:dind enables TLS by default; the worker CLI is plain HTTP.
  // The env entry must NOT carry value:'' — the API omits empty values,
  // so the stored pod would differ from the desired spec and Recreate
  // would delete+recreate forever.
  assert.deepStrictEqual(dind.env, [{ name: 'DOCKER_TLS_CERTDIR' }]);
});

test('scratch + docker-data volumes shared at identical paths', async () => {
  const res = await sync(workerNode);
  const pod = res.body.attachments[0];
  const names = pod.spec.volumes.map((v) => v.name).sort();
  assert.deepStrictEqual(names, ['docker-data', 'docker-sock', 'scratch']);
  for (const c of pod.spec.containers) {
    const scratch = c.volumeMounts.find((m) => m.name === 'scratch');
    assert.ok(scratch, `${c.name} mounts scratch`);
    assert.strictEqual(scratch.mountPath, '/scratch');
    const sock = c.volumeMounts.find((m) => m.name === 'docker-sock');
    assert.ok(sock, `${c.name} mounts docker-sock`);
    assert.strictEqual(sock.mountPath, '/var/run/docker');
  }
  const dind = pod.spec.containers.find((c) => c.name === 'dind');
  assert.strictEqual(dind.volumeMounts.find((m) => m.name === 'docker-data').mountPath, '/var/lib/docker');
});

test('control-plane node (label + taint) gets no worker', async () => {
  const res = await sync(controlPlaneNode);
  assert.deepStrictEqual(res.body.attachments, []);
});

test('control-plane node identified by taint alone gets no worker', async () => {
  const res = await sync(taintOnlyCpNode);
  assert.deepStrictEqual(res.body.attachments, []);
});

test('sandman/* node labels become -label key=value args', async () => {
  const res = await sync(labeledNode);
  const worker = res.body.attachments[0].spec.containers.find((c) => c.name === 'worker');
  assert.deepStrictEqual(worker.args, [
    ...BASE_ARGS,
    '-label', 'placement=k8s',
    '-label', 'class=ml',
  ]);
});

test('gpu node via sandman/gpu label: nvidia dind + driver mounts', async () => {
  const res = await sync(gpuByLabelNode);
  const pod = res.body.attachments[0];
  const dind = pod.spec.containers.find((c) => c.name === 'dind');
  assert.strictEqual(dind.image, 'nvidia/docker:dind');
  const nv = pod.spec.volumes.find((v) => v.name === 'nvidia');
  assert.deepStrictEqual(nv, { name: 'nvidia', hostPath: { path: '/usr/local/nvidia' } });
  for (const c of pod.spec.containers) {
    const m = c.volumeMounts.find((x) => x.name === 'nvidia');
    assert.ok(m, `${c.name} mounts nvidia`);
    assert.strictEqual(m.mountPath, '/usr/local/nvidia');
    assert.strictEqual(m.readOnly, true);
  }
  const worker = pod.spec.containers.find((c) => c.name === 'worker');
  assert.ok(worker.args.includes('-label') && worker.args.includes('placement=ml'));
});

test('gpu node via nvidia.com/gpu capacity gets the same nvidia treatment', async () => {
  const res = await sync(gpuByCapacityNode);
  const dind = res.body.attachments[0].spec.containers.find((c) => c.name === 'dind');
  assert.strictEqual(dind.image, 'nvidia/docker:dind');
});

test('node without InternalIP advertises the port with empty host (documented)', async () => {
  const res = await sync(noIpNode);
  const worker = res.body.attachments[0].spec.containers.find((c) => c.name === 'worker');
  assert.ok(worker.args.includes(':9595'));
});
test('decoratorcontroller.yaml uses the v4 attachments field', () => {
  const txt = fs.readFileSync(path.join(root, 'decoratorcontroller.yaml'), 'utf8');
  assert.match(txt, /^\s+attachments:/m, 'DecoratorController must list children under `attachments` (v4); childResources is pruned and the controller never sees its pods');
  assert.doesNotMatch(txt, /childResources:/, 'childResources is the pre-v4 field name and gets silently dropped');
  assert.match(txt, /resource:\s*pods/, 'attachment resource must be pods');
});

test('decoratorcontroller.yaml recreates worker pods on label change', () => {
  // k8s forbids pod updates to args, so label changes need Recreate
  // (delete+recreate). OnDelete never touches existing children; InPlace
  // updates are rejected for args. Convergence needs the desired spec to
  // match the stored pod byte-for-byte.
  const txt = fs.readFileSync(path.join(root, 'decoratorcontroller.yaml'), 'utf8');
  assert.match(txt, /method:\s*Recreate/, 'updateStrategy must be Recreate');
  assert.doesNotMatch(txt, /method:\s*(OnDelete|InPlace)/, 'OnDelete/InPlace cannot apply arg changes');
  assert.match(txt, /resyncPeriodSeconds:\s*300/, 'periodic resync must converge CM-only drift (image bumps)');
});

test('decoratorcontroller.yaml hooks point at the served sync endpoint', () => {
  const txt = fs.readFileSync(path.join(root, 'decoratorcontroller.yaml'), 'utf8');
  assert.match(txt, /url:\s*http:\/\/sandman-worker-hook\.sandman\/sync/);
});

test('hook server serves /sync from the hook configmap', () => {
  const server = fs.readFileSync(path.join(root, 'hook-server.yaml'), 'utf8');
  assert.match(server, /image:\s*metacontroller\/nodejs-server:0\.1/);
  assert.match(server, /configMap:\s*[\r\n]+\s+name:\s*sandman-worker-hook/);
});
