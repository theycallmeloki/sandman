# sandman

<img width="1402" height="1122" alt="ChatGPT Image Aug 15, 2026, 01_20_39 PM" src="https://github.com/user-attachments/assets/e20a907a-7f79-4923-8638-ecd4c34214a3" />


*"Exit light, enter night."* — Metallica, "Enter Sandman"

sandman is a peer-to-peer data and compute fabric for a trusted LAN. Nodes
discover each other with zero configuration (mDNS, Bonjour-style), expose
their container runtime, and speak two things over one port: a shell-like job protocol
and a versioned-data API. Data lives in content-addressed repositories,
pipelines turn revisions into revisions, and the fleet runs the jobs —
no Kubernetes, no database, no coordinator. The daemon is the only moving
part, and it is just another verb.

**Thesis: a remote container node is just a process you can't see.** The whole
fabric is process verbs you already know — run, signals, exit codes, `&&`
chains across machines. No scheduler, no job queue, no database, no
coordinator. The shell is the scheduler:

```sh
sandman nodes | grep gpu | head -1 | xargs sandman run -- pytorch/train ...
```

---

## If you've used other MLOps tools

Most of the vocabulary maps onto tools you already know. The differences are
where the fun starts.

| sandman concept | Feels like | But instead of |
|---|---|---|
| `sandman run` | `ssh` + `containerd run` | a job queue or K8s `Job` |
| repos / commits / branches | `git` | a central server or object store |
| dedup'd content store | `dvc` cache | a `.dvc` layer over a cloud bucket |
| pipelines (transforms) | `Airflow` DAG tasks | a scheduler daemon with retries |
| input composition (cross/union/join) | SQL `JOIN` over datasets | orchestrator glue code |
| output repos chained | Airflow `XCom` / Kubeflow pipelines | passing artifact paths around |
| `stats` / `dashboard` | `htop` for the fleet | Grafana + Prometheus stack |
| everything on disk as files | Unix philosophy | a control-plane database |

The mental model in one line: **commits are data; pipelines are functions;
provenance is the graph; the LAN is the cluster.** You commit data, a
pipeline sees the commit, runs a container transform, and commits the
result to its own repo — which the next pipeline sees. Finish is recursive,
so chains of pipelines fan out on their own, one commit and one job per
stage per wave.

Three things are deliberately *not* here, and they change how you operate:

- **No cluster scheduler.** Jobs are placed by the shell (`xargs sandman
  run`), or by pipeline placement labels. There is no backfill, no
  preemption, no resource broker.
- **No central database.** State is plain files under `/var/lib/sandman`;
  `cat /var/lib/sandman/registry` is the fleet, `ls repos/in/commits` is
  the history. Everything is inspectable with standard tools.
- **No auth on the wire.** Trusted-LAN by design: *the firewall is the
  auth*. See [Security posture](#security-posture).

## Design principles

- **Composition** — a job is argv + env + stdin. `echo data | sandman run b2 -- alpine cat`
- **Representation** — the fleet is a text file (`/var/lib/sandman/registry`);
  node knowledge is data, not code
- **Transparency** — output streams live, like ssh; job state is a directory
  you can cat
- **Silence** — only the job's stdout on stdout; diagnostics on stderr
- **Repair** — exit codes return verbatim, so `&&`/`||`/`$?` compose across
  the fabric; failures surface as broken pipes, never phantom jobs
- **Separation** — policy lives in the shell, mechanism in the container
  runtime; the daemon owns the containerd socket, clients never see it
- **Parsimony** — one static binary, busybox-style verbs; the daemon is just
  another verb

---

## Quickstart: ten minutes to a dream

Requirements: Linux, containerd + runc (the distro packages; no docker,
no Docker repository, no PPA), and a LAN with multicast (one L2 segment).

### 1. Install the daemon

```sh
make build                # or: CGO_ENABLED=0 go build -o sandman .
sudo make install         # binary + systemd unit
sudo systemctl enable --now sandman
```

That's it. The node advertises `_sandman._tcp` and browses for peers on
boot — it joins the fleet by itself. Nothing to register, nothing to
configure.

Or install the Debian package (no Go toolchain, no docker anywhere):

```sh
make deb                                   # build/sandman_<version>_amd64.deb
sudo apt install ./build/sandman_<version>_amd64.deb   # pulls in containerd + runc
sudo systemctl enable --now sandman
```

The package declares `Depends: containerd, runc` — the distro supplies the
container machinery (containerd → runc → Linux namespaces/cgroups), and
the systemd unit starts after `containerd.service`. Docker is not
required, not installed, and never invoked: the execution backend speaks
containerd's Go API directly.

### The runtime stack

Sandman owns execution policy (which image, command, inputs, outputs,
resources, when to kill); containerd owns container mechanics (image
storage, snapshots, task lifecycle, OCI runtime invocation, cgroups):

```
Sandman control plane → Runner → containerRunner → containerd → runc → Linux
```

A job runs in a containerd container in the `sandman` namespace, named
`sandman-<id>` and labeled with its owning node (orphan pruning). Images
are pulled into containerd's content store on first use. Containers share
the host network namespace — containerd alone provides no bridge (CNI is
nerdctl territory), the fabric assumes a trusted LAN, and service
pipelines need host-reachable ports.

### 2. Run a job on a remote node

```sh
sandman nodes             # who's awake?
sandman run b2 -- alpine echo "exit light, enter night"
```

`sandman run` behaves like ssh: stdin pipes through, stdout/stderr stream
live, Ctrl-C becomes SIGINT on the remote container (twice = SIGKILL), and
the container's exit code returns verbatim:

```sh
sandman run b2 -- alpine sh -c 'exit 7'; echo $?    # 7
```

The container is ephemeral — here tonight, gone by morning; only the exit
code remains.

### 3. Version some data

Repos hold immutable revisions (commits) on branches:

```sh
curl -X POST localhost:4242/api/v1/repos            -d '{"name":"in"}'
curl -X POST localhost:4242/api/v1/repos/in/commits -d '{}'        # → commit id
curl -X PUT  localhost:4242/api/v1/commits/$ID/files/features.csv \
     --data-binary @features.csv
curl -X POST localhost:4242/api/v1/commits/$ID/finish -d '{}'
```

Every commit is a dream you can revisit. Tags are the names you give a
dream so you can find it again later.

### 4. Point a pipeline at it

```sh
curl -X POST localhost:4242/api/v1/pipelines -d '{
  "name":"clean",
  "transform":{"image":"alpine","cmd":["sh","-c","cp $in/* $OUT/"]},
  "input":{"repo":"in","glob":"/*"}
}'
```

The pipeline subscribes to the `in` repo. The next commit triggers one job;
the job's `OUT` directory becomes a new commit in the pipeline's own output
repo (`clean`). The environment sees each input as a variable named after
it (`$in` = the datum's input directory), plus `OUT`, `JOB_ID`,
`OUTPUT_COMMIT`, and `<input>_COMMIT`.

### 5. Chain a second stage

Point another pipeline at the first one's output repo:

```sh
curl -X POST localhost:4242/api/v1/pipelines -d '{
  "name":"train",
  "transform":{"image":"pytorch/pytorch","cmd":["python","train.py"]},
  "input":{"repo":"clean","glob":"/*"}
}'
```

Now commit once and the whole chain runs: `in` → `clean` → `train`, one job
and one commit per stage. That's a DAG. The daemon — the **Master of
Puppets** — pulls the strings: every commit records which commit begot it
(provenance), and the control plane walks those strings to decide what runs
next (see [The Master of Puppets](#the-master-of-puppets-how-the-control-plane-works)).

Exit light, enter night: your pipeline runs while you sleep.

---

## Core concepts

### The fleet

A fleet is a set of nodes that can see each other. Each node runs the same
binary in one of two roles:

- **daemon** — the control plane (default; binds `:4242`, publishes mDNS).
  Expect one daemon per LAN. The daemon owns the containerd socket, serves the
  API, and places pipeline jobs.
- **worker** — an execution host (`sandman worker -control <url>`): no
  control-plane duties, registers with the daemon, runs the jobs the
  daemon places on it.

Nodes discover each other via mDNS/DNS-SD (`_sandman._tcp`) and merge
registries over TCP, so discovery is eventual and self-healing. Cross-L2
subnets use `sandman attach` for static peers.

### Repos, commits, branches

- **Repo** — a named, versioned dataset. Repos are auto-created as pipeline
  outputs; inputs must exist (or be another pipeline's output).
- **Commit** — an immutable revision. Writing files happens into an *open*
  commit; `finish` seals it and triggers subscribed pipelines.
- **Branch** — a movable pointer to a commit. Finishing a commit advances
  its branch; the "view" of a branch is the accumulated content of its
  ancestry, newest write to a path wins.
- **Content addressing** — file content is stored by SHA, so identical
  content across commits is stored once (dedup is automatic).
- **Tags** — durable global names bound to a commit's content.

### Pipelines, transforms, jobs

- **Pipeline** — a function: an input spec, a transform (image + command),
  and output config. Versioned: every update is a new immutable version.
- **Transform** — the container image and command that do the work.
- **Job** — one execution of a pipeline for one input revision. Runs a
  containerd container with the datum's files, captures logs, commits `OUT`.
- **Datum** — the unit of work: each file (or directory subtree) matched by
  the input glob. A job processes its datums with a worker pool sized by
  the pipeline's parallelism.

### The DAG and provenance

Pipelines chain: a pipeline's output repo is a normal repo, so another
pipeline can consume it. Every output commit records its provenance — the
input commits and spec version that produced it. That record is the graph
edge; the daemon walks edges to propagate work downstream and to answer
"what does this commit depend on?"

Key DAG laws:

- One commit and one job per stage per wave — a mid-DAG commit propagates
  forward only, never re-triggering stages that don't consume it.
- A racing pair of input commits on the two sides of a cross never
  double-spawns the pairing job.
- A job with no datums settles successful with nothing to produce; an
  empty wave never propagates.
- A failed stage fails every downstream stage — recorded as un-executed
  jobs, and flushing the failing commit reports every stage's terminal
  state.

---

## The Master of Puppets: how the control plane works

*"Master of puppets, I'm pulling your strings."* — Metallica,
"Master of Puppets"

The daemon is the Master; pipelines are the puppets; **provenance is the
string**. Every commit carries its parentage, so the Master always knows
whose strings to pull, in what order, and what to do when a string goes
slack (an empty wave) or snaps (a failure).

### Discovery

Each daemon publishes a `_sandman._tcp.local.` service (TXT records carry
the runtime version and arch) and browses the same type. Peers are kept in the
registry file with a last-seen timestamp and are forgotten after 90s of
silence; a graceful shutdown announces an mDNS goodbye so peers drop the
node immediately. `cat /var/lib/sandman/registry` is the fleet.

mDNS on Linux delivers multicast to one socket per packet when several
processes share UDP 5353 (reuseport hashing), so a specific peer pair can
be starved while others work. The daemon therefore also gossips: each
maintenance tick it pulls one known peer's registry over the wire (`NODES`
verb) and merges it — mDNS bootstraps, TCP converges. Discovery is
eventual, never a hard dependency.

### Jobs

A job is an ephemeral containerd container (`sandman run` streams a
scratch-workdir container the way `docker run` used to): the daemon
creates the container with the image, starts its task attached, and relays
the three fds over the connection. Exit codes come from the task's exit
status (never a wait race on an auto-removed container); signal deaths
report 128+signal, shell-style. If the client vanishes, the daemon kills
the container — no orphans. Containers are named `sandman-<jobid>` and
labeled with their owning node, so a fresh daemon prunes only the
containers its own crashed predecessor left behind.

### Wire protocol

Line-oriented text over TCP; control lines, length-prefixed frames for data:

```
C: HELLO sandman/0.1
S: OK node=b2 docker=2.2.2   # "docker" is the runtime version token, kept for fleet interop
C: RUN
C: b2-4172-88123
C: alpine
C: CMD
C: sh
C: -c
C: echo hi
C: (blank line ends argv)
S: RUNNING b2-4172-88123
C: DATA <len>   (stdin, streaming)
C: SIGNAL INT
C: EOF
S: OUT <0|1> <len>   (stdout/stderr, streaming)
S: EXIT 0
```

One job per connection, no idle timeout — long jobs hang like local
processes.

### One port, two protocols

The JSON API lives on the **same port** as the job protocol — each
connection is routed by its first bytes (an HTTP method line goes to the
API, `HELLO` to the text protocol). No separate server, no port to
remember.

---

## The fleet

### Commands

```sh
sandman daemon                # run the node side (TCP :4242, mDNS)
sandman nodes                 # list the fleet
sandman stats                 # poll every node; fleet state as JSONL on stdout
sandman dashboard             # live TUI: nodes, containers, per-container cpu/mem
sandman run <node> -- <image> <cmd...>   # stream a job; exit code is the container's
sandman run -e K=V b2 -- alpine sh -c 'echo $K'
sandman attach <name> <addr>  # static peer (non-mDNS networks)
sandman detach <name>
```

Cross-subnet nodes (no multicast): `sandman attach wan-node 10.0.0.9:4242`
adds a static peer; `detach` removes it.

### Adding an execution worker

A machine that runs jobs for your control plane — needs containerd + runc
(installed automatically from the distro packages; no docker), a trusted
LAN; make and curl are fetched if missing:

```sh
curl -sSL https://raw.githubusercontent.com/theycallmeloki/sandman/main/install.sh | sh
```

One command, everything auto-filled: worker name (hostname), exec port
(4343), advertise address (the host's default-route LAN IP, so the daemon
can dial back and place jobs), and the control plane — the worker
discovers the daemon itself via mDNS (`role=daemon`; the fleet expects one
daemon per LAN). The worker's systemd unit is written with these values
baked in — edit `/etc/systemd/system/sandman-worker.service` and
`systemctl restart sandman-worker` to change them. Set `CONTROL` to skip
discovery:

```sh
CONTROL=http://192.168.1.147:4242 \
  curl -sSL https://raw.githubusercontent.com/theycallmeloki/sandman/main/install.sh | sh
```

### Placement

A pipeline may require a placement label; its jobs run on a worker that
registered that label (`sandman worker -control <url> -advertise <addr>
-label <label>`). The pipeline definition never names a host address. Work
that no registered host can take surfaces as the pipeline's crashed state
instead of hanging; when a host bearing the label registers, the pending
job re-places on its own and completes — one output commit, same result as
a local run (the execution host's identity is visible to the transform as
`HOSTNAME`).

### Stats and dashboard

The `STATS` wire verb answers with every running container on the node plus
live resource usage (cpu %, mem bytes/limit, pids) as JSON lines, and the
host's own figures ride in the header: cpu count, real memory from
/proc/meminfo, and host-wide cpu utilization sampled over the daemon's 5s
tick. The client polls every known node and emits **JSONL to stdout** — one
object per node, pipe-friendly:

```sh
sandman stats | jq -r '[.node, .hostCpuPerc, .hostMemPerc] | @tsv'
```

`sandman dashboard` is a thin renderer over the same data: a tview/tcell TUI
(the Go equivalent of htop's ncurses) — cell-based table layout that adapts
to any terminal size, colors, and `q`/ctrl-c to quit. Each node row carries
its own cpu/mem gauge; the rows below it break the node down per container.
On short terminals the table scrolls — mouse wheel, `j`/`k`, `PgUp`/`PgDn`,
`Home`/`End`. Any tool can consume the JSONL — the dashboard is just one
consumer.

---

## The data plane: repos, commits, files

All endpoints hang off `/api/v1` on the daemon's port.

### Repositories

- `POST/GET/DELETE /api/v1/repos[/{name}]` — create, list, inspect
  (name, size, branches), delete. `DELETE` honors `?force=1` (a pipeline's
  output repo is protected from accidental deletion); the internal `spec`
  repository holding pipeline definitions can never be deleted.

### Commits

- `POST /api/v1/repos/{repo}/commits` — start a revision on a branch.
- `PUT /api/v1/commits/{id}/files/{path}` — write a file into an open
  revision.
- `DELETE /api/v1/commits/{id}/files/{path}` — tombstone a path: the file
  is removed from the branch's view at this revision, and a pipeline's
  output reflects the deletion (the deleted file is absent, not stale).
- `POST /api/v1/commits/{id}/files/{path}` — copy a file or directory
  subtree from another commit (destinations already in the view are
  rejected; so are paths already written in the open commit).
- `POST /api/v1/commits/{id}/finish` — close it (`empty: true` makes it an
  explicit empty commit: nothing is readable through it, even at the head).
- `GET /api/v1/commits/{id}` / `GET …/files` / `GET …/files/{path}` — read;
  `?download=true` adds an attachment disposition, and the content type is
  sniffed from the bytes.

### Tags

- `PUT/GET /api/v1/tags/{name}` / `GET /api/v1/tags` — durable global
  names bound to file content, listed with their object reference.

### File-level features

- **Delimited uploads** — `PutFileSplit` uploads data split into records at
  a delimiter, each stored at `path/<i>`; with a header, the first chunk is
  replicated into every record's file. Appending under the same header
  continues the numbering, so earlier records keep their identity and the
  dedup skips them; a changed header re-identifies every record and
  replaces them, so all are reprocessed. `PutFileURL` ingests a file from
  an HTTP URL into a commit.
- **File revision history** — `ListFileHistory` returns the revisions of a
  path across a commit's ancestry, newest first, with full-depth listings
  supported on multi-commit cross outputs.
- **Symlinked output** — a transform may emit its output as symbolic links:
  the output revision stores the linked content (a link to a file yields
  the target's content at the link's path; a link to a directory its
  files), and a linked file's stored content is identical to the input's —
  referenced, not copied. Links to the in-container input paths
  (`/sandman/in/...`, `/sandman/view/...`) and to temp files the transform
  wrote (the job's directory is mounted at the container's `/tmp`) are
  resolved when the output is scanned.

### Backup, restore, reset, garbage collection

- `GET /api/v1/backup` — full control-plane state as a tar.gz: repos, tags,
  pipelines, jobs, dedup, logs, spout markers, secrets, transactions,
  triggers. The store part is captured under the store's write lock — the
  single-writer's buffer — so the archive is a consistent point-in-time
  state (a captured ref always has its commit in the same archive).
  Restore: stop the daemon, extract into the state dir, start.
  `sandman backup [dest]`
- `POST /api/v1/reset?yes=1` — destroy every repo, pipeline, job, secret,
  tag, spout marker, and trigger ledger; the internal `spec` repo is
  recreated empty and the fleet keeps running. Refuses without `yes=1`.
  `sandman reset --yes`. Idempotent, and it refuses to run on corrupted
  metadata.
- **Garbage collection** — `CollectGarbage` reclaims durable blobs no
  longer referenced by any commit tree, tag, or spec record; it refuses
  while a job is running, and it never touches reachable data (automatic
  collection defaults off). Stopping a pipeline ends its in-flight work,
  so collection can proceed. A system-wide reset clears statistics state
  along with everything else, so names are reusable.

---

## Pipelines in depth

### Anatomy of a pipeline

```json
{
  "name": "clean",
  "transform": {"image": "alpine", "cmd": ["sh", "-c", "cp $in/* $OUT/"]},
  "input": {"repo": "in", "glob": "/*"},
  "parallelism": {"constant": 1},
  "outputBranch": "master",
  "reprocess": false,
  "standby": false,
  "description": "copies the input, filtered"
}
```

The input is a repo plus a glob; the job sees each input as an environment
variable named after it whose value is the input directory (so `$repo/*`
addresses the data). Jobs also receive `OUT`, `JOB_ID`, `OUTPUT_COMMIT`,
and `<input>_COMMIT`.

The executable contract is mirrored in `client/` and enforced by
`conformance/`:

- Finishing a commit triggers one job per subscribed pipeline; the job's
  `OUT` directory becomes a new commit in the pipeline's output repo
  (named after the pipeline) — finish is recursive, so pipelines chain.
- A pipeline created over existing history processes the branch head once.
- A stopped pipeline replays the commits finished while it was stopped when
  started (backlog — consumed together as one job over the branch head).
- Updating a pipeline applies a new version, kills its in-flight jobs, and
  processes the input head under the new transform.
- A pipeline with no command and no stdin copies inputs to output; with
  stdin but no command it is accepted and immediately fails.
- Job output is all-or-nothing: a failed or killed job's output commit is
  finished empty.
- A commit's view accumulates across its ancestors — the newest write to a
  path wins, and a tombstoned path (`DeleteFile`) is gone — and the job
  for the head revision always sees the full accumulated content.
- Pipeline states: `running`, `standby`, `paused` (stopped), `failure`,
  `crashed`.

### Lifecycle

- `POST/GET/DELETE /api/v1/pipelines[/{name}]` — create, list, inspect,
  delete. The create body carries `update: true` to apply a new version
  (creating when absent); `reprocess: true` is a persisted spec field:
  every job re-executes all of its datums instead of skipping datums
  unchanged from a previous successful run. Every update processes the
  input head. `DELETE` honors `?force=1` (removes a mid-DAG pipeline
  despite downstream consumers) and `?keepRepo=1` (preserves the output
  repo for reuse). `GET` takes `?history=<n>` (`0` current version, `n`
  current plus n older, `-1` every version), `?name=` and
  `?allowIncomplete=1` (lists pipelines whose definition is lost, by name
  only); inspection takes `?ancestry=<k>` for historical versions.
- `POST /api/v1/pipelines/{name}/stop` / `/start` — pause/resume: a stopped
  pipeline ignores new commits and replays them on start; an update does
  not restart it.
- **Manual pipeline runs** — `RunPipeline` triggers a job on demand: with
  no provenance it re-processes the current branch heads; with explicit
  provenance it processes exactly those input revisions (validated: a
  commit outside the pipeline's input lineage or two commits of one branch
  are rejected; a pipeline with nothing to process cannot be run); with a
  job id it re-executes that job's input pairing. A run's output never
  propagates downstream — a manual run is not a processing wave. Deleting
  an already-deleted pipeline is a no-op.
- **Provisioning failures** — a pipeline whose execution environment cannot
  be provisioned (a nonexistent image — obviously invalid or
  plausible-but-absent) converges on the crashed state with a recorded
  reason instead of hanging; one whose output repo is deleted mid-run
  fails with a reason. Both recover by updating the pipeline.
- **Config extraction** — inspecting a pipeline echoes every
  user-settable field (transform, parallelism, chunk spec, queue size,
  autoscaling, standby, output branch, reprocess, stats, spout,
  description) with the input's name/branch defaults materialized,
  deep-equal to the creation request; request flags (update) are not
  echoed, and an unsupported execution framework (e.g. TFJob) is rejected
  at creation naming it.

### Datums and parallelism

A job processes its input as per-datum units of work: every path matched by
the input glob is a datum (a directory match is a datum of its whole
subtree), executed by a bounded worker pool sized by
`parallelism.constant` and capped at the datum count. Each datum runs the
transform against its own files (`$<input>` is the datum's input dir; the
full input view is mounted read-only at `/sandman/view/<input>`); its
output merges into the job's single output commit, files at the same path
from different datums concatenating in datum order (the order is not
contractual). Datums whose content is unchanged from a previous successful
run are skipped and their output carried forward, unless `reprocess` is
set. `transform.datumTries` retries a failing datum that many times, one
log entry per attempt; the transform's `acceptReturnCode` applies per
datum. A failing datum fails the job — reason names the datum — and the
job's `processed`/`recovered`/`failed`/`skipped` fields count the
outcomes.

- **Per-datum statistics** — `enableStats: true` in the pipeline spec
  (one-way: an update cannot disable it) records each datum's outcome,
  input/output files, process time, and timing. `GET
  /api/v1/jobs/{id}/datums` lists them — live during execution (queued
  datums included as pending), state-ordered (failed, recovered, success,
  skipped), paginated with `?limit=&page=` and an out-of-range error;
  `GET …/datums/{datumID}` inspects one (an error when stats are off).
  The pipeline's output repo gains a `stats` branch — one commit per job
  holding one record file per datum — that downstream pipelines can
  consume.
- **Scheduling knobs** — `chunkSpec` (a target datum count or chunk size)
  groups a side's glob matches into datums without changing the output;
  `maxQueueSize` bounds each worker's pending datums; `autoscaling` sizes
  the worker pool to the datum count, capped at the configured parallelism
  (scale-to-zero via standby). A running job's `workers` status reports
  each worker's current datum, its start time, and its queue, and
  `POST /api/v1/jobs/{id}/datums/{datumID}/restart` aborts a datum and
  starts it over with fresh progress.

### Resource enforcement

Resource requests and limits declared on a pipeline (memory, CPU, disk) are
applied to the environment that executes its jobs: memory limits become
the cgroup's hard memory limit, requests the memory reservation (soft
target), and a CPU limit or request a CFS quota/period pair (a CPU request
maps to the allocation; a disk request is recorded but not enforceable —
the container backend has no portable per-container writable-layer quota).
No declared resources → none injected.

### Standby: sleep with one eye open

`standby: true` in the pipeline spec — the pipeline idles in the standby
state with no work, activates (state `running`) when input arrives, and
returns to standby once the work settles. Stopping it pauses it; commits
written while paused never wake it. No fixed activation cap and no
warm-participant contract (tuning choices, not requirements); a standby
pipeline that cannot be provisioned degrades to crashed, never to a
degraded standby state.

### Job queueing

A pipeline's jobs run strictly one at a time, in spawn order: successive
input commits queue on the pipeline's gate and come up in commit order, so
with parallelism 1 exactly one job runs. Cancelling the running job lets
the next queued job start, and cancelling one job never cancels the
others; a cancel that arrives while a job is queued settles it killed
without doing any work. The system stays correct under a burst of rapid
revisions across many pipelines: every revision is consumed with a job,
the head converges, and the job index stays queryable.

### DAG behavior

- **Propagation** — chains produce exactly one commit and one job per stage
  per wave, repeatedly; a mid-DAG commit propagates forward only, never
  re-triggering stages that do not consume it. A failed stage fails every
  downstream stage: the failure propagates through the DAG as recorded,
  un-executed jobs, and flushing the failing commit reports every stage's
  terminal state instead of erroring.
- **Provenance pairing** — a cross whose sides derive from the same source
  branch pairs them at the same source revision: a trigger that would
  pair a fresh commit with the other side's still-stale head is deferred
  until the other side catches up, so diamond DAGs (A→B, A→C, D=cross(B,C))
  produce exactly one commit per source revision per repository, never one
  per dependency path. A downstream pipeline can consume an upstream
  pipeline's output as one arm of a cross.
- **Output branches + deferred processing** — a pipeline's `outputBranch`
  names where its output lands (default `master`); output commits parent
  against that branch's head. Pipelines trigger on watched branch heads:
  a commit on a non-watched branch produces no jobs (and flushes to zero
  immediately), retargeting the watched branch onto an existing commit
  (`CreateBranch`) processes it exactly once, and a downstream stage that
  watches a different branch of an upstream repo does not run until that
  branch is promoted onto the output commit.
- **Transactions** — `POST /api/v1/transactions` opens one; `POST
  /api/v1/pipelines?transaction=<id>` stages a create or update into it;
  `POST …/transactions/{id}/finish` applies every staged operation
  atomically — all or nothing — and `DELETE …/{id}` aborts. Pipelines
  staged together may consume each other's output (the input repo need
  not exist yet); after finish the chain runs end to end, and updating
  two pipelines in one transaction yields exactly one new job and one new
  commit per pipeline. Finishing fails with "outside of transaction" if a
  staged pipeline was modified outside the transaction meanwhile.
- **Commit deletion** — `DeleteCommit` (by id or `repo@branch`) removes a
  commit and everything derived from it across the whole downstream DAG:
  every commit whose provenance includes it, and the jobs that consumed
  them. Surviving commits whose parent was removed become the first commit
  of their branch, and branch heads that pointed at a removed commit move
  to the nearest surviving ancestor or disappear — the DAG stays
  functional. Deleting a branch head supersedes the in-flight job
  processing it: the job is cancelled and removed, the branch reverts to
  the previous commit, and later commits are processed normally.

### Input types

- **Cross inputs** — `input.cross` is a list of file-scoped inputs whose
  datums combine as the cartesian product: a job's datum set is one glob
  match from every side. Each side is addressable by its own name
  (`$<name>` and `$<name>_COMMIT`); `input.branch` selects a side's branch
  (default master). Every input commit on any side creates a job pairing
  that commit with the other sides' current heads — a side with no head
  yet contributes no datums — and flushing a set of commits
  (`client.FlushSet`) returns only the pairing job. Datum enumeration is
  available standalone via `POST /api/v1/datums`.
- **Union inputs** — a union exposes its branches under one namespace and
  merges same-named files by concatenation in branch order, one datum per
  distinct path; the merge is tracked per occurrence, so removing a file
  from every branch is detected as a removal, never hidden by hash
  matching. Branches may be plain repos, crosses (each constituent repo
  its own directory, every file once per cross combination), or nested
  unions; a cross's immediate branches must expose distinct namespaces,
  and a cross of unions with the same alias is rejected at creation.
  A union inside a cross resolves its branches' heads independently (two
  branches of one repo stay distinct) and the cross's other legs resolve
  to their branch heads at job-creation time.
- **Join and group inputs** — a join pairs files across repositories by a
  captured glob group (`JoinOn` selects it, e.g. `$1` or `$1$3`): a datum
  exists for every key present in all members, containing one file per
  member; an outer member's unmatched keys each form a datum carrying only
  that member's file, with the absent members' directories unexposed. A
  group collects every file sharing a `GroupBy` capture value across all
  members into one datum (union, never a cross product), and a group whose
  members carry join-ons joins first then groups the whole pairs.
- **Lazy inputs** — the lazy flag is part of the input spec and is recorded
  on every job's input snapshot, through output-repo hops; lazy jobs
  complete even when some input files go unread. The glob `/` selects the
  whole commit as one datum. Output upload rejects special files (pipes,
  sockets, devices) — a transform that emits one fails the job promptly
  instead of hanging the scan.
- **Cron inputs** — an input with a schedule (`@every <duration>`) ticks on
  its own clock: each tick commits a file named by the tick time (UTC
  RFC3339 — a legal path) to the input's auto-created repository (named
  after the pipeline and the input), triggering the pipeline and its
  downstream. Overwrite mode tombstones the previous tick so the branch
  holds exactly one tick file; crosses of cron and regular inputs run when
  both are available. `TriggerCron` creates the tick immediately on every
  cron input of the pipeline, and scheduled ticks keep flowing around it.
  Rapid spec updates never restart the ticker — the cadence survives with
  no bursts or stalls.
- **Size triggers** — an input with a `Trigger` accumulates the bytes newly
  committed to its watched branch (durably — a ledger file, so an
  interruption never loses or double-counts), and every completed
  threshold unit commits the accumulated view to the input's dedicated
  accumulation branch — derived from the pipeline and the input's
  position, and reused across updates so state is never orphaned — where
  the pipeline runs on it. One oversized commit fires once per threshold
  unit, accumulation resets after firing, and triggers compose across a
  DAG.
- **Spouts** — the insomniacs of the DAG: a pipeline with no input whose
  transform runs in the background, committing each data-bearing cycle to
  its own output branch (accumulating or replacing per the overwrite
  option) and its marker directory to a separate markers branch. Every
  spout commit records its pipeline's specification commit as provenance,
  so updates start observable provenance epochs and a spec commit's
  subvenants are its spout output plus the downstream output; the marker
  directory is per-pipeline, so a plain update continues the marker while
  a reprocess update resets it. The job settles when the transform's loop
  ends; a stop, update, or delete kills it. Deleting the spout's head
  commit does not stop it, downstream pipelines consume its output
  normally, a spout with a declared input is rejected, and a marker name
  with glob metacharacters is rejected.

### Secrets

Named typed metadata blobs with create/inspect/list/delete (the type is
reported as "Opaque", the creation timestamp is system-assigned). The
trusted-LAN model (no tokens; see [Security posture](#security-posture))
means any peer on the LAN can manage secrets and bind them into pipelines.

---

## Observing

### Logs

`GET /api/v1/logs` — a job's log lines: every container's stdout/stderr is
captured timestamped and complete. `?pipeline=` or `?job=` (either a job id
or its output commit) select the stream; `?datumPath=` and `?datum=` (a
file path or its content hash) narrow by input file and require a pipeline
or job; `?since=` is a relative window excluding older lines; `?follow=1`
streams new lines live as they are produced (newline-delimited
`{"line":…}` until the client disconnects). An unconstrained query
searches every job's logs.

### Jobs

`GET /api/v1/jobs[/{id}]` — jobs; `?pipeline=`, `?outputCommit=`,
`?state=` (repeatable), `?history=` (version depth), `?full=1` (each job's
own version's transform and input spec — history survives updates).
Listing jobs of a pipeline that does not exist is an error. Job inspection
accepts a job id or its output commit. `POST …/{id}/cancel` and `…/stop`
kill the in-flight work and mark the job killed; `DELETE …/{id}` removes
the record after finalizing its output revision. Flushing is a client-side
wait: poll jobs until every job for a revision — including downstream
stages — is terminal (the `client.Flush` helper does exactly that,
confirming the graph has stopped growing, and terminating empty when every
consumer is stopped or failed).

### Metrics

`/api/v1/metrics` serves Prometheus-format invocation counters and latency
sum/count aggregates for file reads (split by outcome — two series), file
writes, and job listings (one series each).

---

## Operations

### State is plain files

Under the state dir (`/var/lib/sandman` by default) — cat it:

```
repos/<repo>/refs/<branch>       one-line commit ids
repos/<repo>/commits/<id>.json   revision records
repos/<repo>/.objects/<sha>/…    file content, content-addressed (dedup)
repos/spec/…                     internal repo: one spec commit per pipeline definition
pipelines/<name>.json            pipeline records (current version)
pipelines/versions/<name>/<v>.json  immutable version history (ancestry)
jobs/<id>/job.json               job records (+ in/ and out/ scratch dirs)
logs/<jobid>.jsonl               captured container output, timestamped lines
transactions/<id>/000000.json    staged transaction operations, in order
```

### Backup and restore

`sandman backup [dest]` — the full control-plane state as a tar.gz
(consistent point-in-time; see the [data plane](#backup-restore-reset-garbage-collection)).
Restore: stop the daemon, extract into the state dir, start.

### Updating the binary

- **Self-update** — `sandman update` checks GitHub releases and installs the
  latest published build (see below for how releases are cut).
- **Release install** — `sudo make install-release VERSION=0.2.22` (no
  leading `v`) installs the newest published release binary instead of
  building — no Go toolchain needed on the target.

### Cutting a release

The release workflow (`.github/workflows/release.yml`) builds and publishes
on every pushed `v*` tag — no manual build or upload:

```sh
git tag v0.2.22 && git push origin v0.2.22
```

It builds with the tag baked in (`-X main.Version`), checksum-ships
`sandman-<os>-<arch>` + `.sha256` for linux/amd64 and linux/arm64, and
creates the release with a changelog of the commits since the previous tag.

### Rolling the fleet

`sandman update` per node, workers first, the daemon last (its restart is a
brief control-plane blip that the workers' heartbeats ride through). The
companion `sandman-pipelines` checkout ships a `roll-update.sh` that walks
a fixed fleet manifest over ssh, restarts each unit only when an update
actually landed, and prints the final fleet view.

---

## Security posture

Trusted-LAN by design: **the firewall is the auth**. The daemon is the only
thing that touches the containerd socket — clients speak the text protocol,
never the runtime. A raw containerd socket is root-equivalent; treat this
fabric accordingly: private networks, or an L2 segment you control. No TLS,
no tokens, no encryption (v1).

---

## Limitations

- mDNS is link-local: discovery works on one broadcast segment; `attach`
  covers the rest
- Trusted-LAN only — no auth or encryption on the wire (v1, by design)
- No cluster scheduler or backfill: pipelines process commits in order, one
  job at a time per pipeline; parallelism and autoscaling are per-pipeline
- Fabric `run` artifacts stay in the node's scratch dir
  (`/var/lib/sandman/jobs/<id>/`); pipeline jobs publish to output repos
  instead, which a fetch verb is not built for yet (use the API or the
  `client` library)
- Inputs are file-scoped (repo + glob): there is no watch of an on-disk
  directory — data must be committed to a repo (cron inputs and size
  triggers cover scheduled and volume-based ingestion)

---

## The contract: client library and conformance suite

The `client` package is the typed Go client for the whole API, and the
conformance suite (`go test ./conformance/`) is the behaviour contract: one
test per spec record, driving this API through the `client` package against
a booted daemon. If you change behaviour, the suite is what says so.

```sh
curl -X POST localhost:4242/api/v1/repos        -d '{"name":"in"}'
curl -X POST localhost:4242/api/v1/pipelines    -d '{"name":"cp","transform":{"image":"alpine","cmd":["sh","-c","cp $in/* $OUT/"]},"input":{"repo":"in","glob":"/*"}}'
curl -X POST localhost:4242/api/v1/repos/in/commits -d '{}'   # → commit id
# PUT files…, finish, then flush for the job
```

## Development

```sh
make build        # static binary
go vet ./...      # sanity
go test ./conformance/   # behaviour contract: boots a daemon, drives the API
```

Test fabric on one host (mDNS loopback lets two daemons see each other):

```sh
./sandman daemon -name b1 -port 4242 -state /tmp/sandman-a &
./sandman daemon -name b2 -port 4243 -state /tmp/sandman-b &
sleep 3 && ./sandman nodes -state /tmp/sandman-a
echo hi | ./sandman run -state /tmp/sandman-a b2 -- alpine cat
```

---

*"Dreams of war, dreams of liars, dreams of dragon's fire — and of things
that will bite."* The DAG runs all night. Check the logs in the morning.
