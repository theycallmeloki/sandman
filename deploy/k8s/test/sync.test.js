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
  assert.strictEqual(env.DOCKER_HOST, 'tcp://127.0.0.1:2375');
  assert.strictEqual(env.TMPDIR, '/scratch');
});

test('plain node gets docker:dind sidecar, loopback-only, privileged', async () => {
  const res = await sync(workerNode);
  const dind = res.body.attachments[0].spec.containers.find((c) => c.name === 'dind');
  assert.strictEqual(dind.image, 'docker:dind');
  assert.deepStrictEqual(dind.args, ['--host=unix:///var/run/docker.sock', '--host=tcp://127.0.0.1:2375']);
  assert.strictEqual(dind.securityContext.privileged, true);
});

test('scratch + docker-data volumes shared at identical paths', async () => {
  const res = await sync(workerNode);
  const pod = res.body.attachments[0];
  const names = pod.spec.volumes.map((v) => v.name).sort();
  assert.deepStrictEqual(names, ['docker-data', 'scratch']);
  for (const c of pod.spec.containers) {
    const scratch = c.volumeMounts.find((m) => m.name === 'scratch');
    assert.ok(scratch, `${c.name} mounts scratch`);
    assert.strictEqual(scratch.mountPath, '/scratch');
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
