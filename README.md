# sandman

A naive peer-to-peer docker fabric. Nodes discover each other on the LAN
(Bonjour-style, via mDNS/DNS-SD), attach to each other with zero
configuration, and expose their docker for distributed jobs.

**Thesis: a remote docker node is just a process you can't see.** The whole
API is process verbs you already know — run, signals, exit codes, `&&` chains
across machines. No scheduler, no job queue, no database, no coordinator.
The shell is the scheduler:

```sh
sandman nodes | grep gpu | head -1 | xargs sandman run -- pytorch/train ...
```

## Unix philosophy, applied

- **Composition** — a job is argv + env + stdin. `echo data | sandman run b2 -- alpine cat`
- **Representation** — the fleet is a text file (`/var/lib/sandman/registry`); node
  knowledge is data, not code
- **Transparency** — output streams live, like ssh; job state is a directory you can cat
- **Silence** — only the job's stdout on stdout; diagnostics on stderr
- **Repair** — exit codes return verbatim, so `&&`/`||`/`$?` compose across the fabric;
  failures surface as broken pipes, never phantom jobs
- **Separation** — policy lives in the shell, mechanism in docker; `sandmand` owns the
  docker socket, clients never see it
- **Parsimony** — one static binary, busybox-style verbs; the daemon is just another verb

## Install

Requirements: Linux, docker, and a LAN with multicast (one L2 segment).

```sh
make build                # or: CGO_ENABLED=0 go build -o sandman .
sudo make install         # binary + systemd unit
sudo systemctl enable --now sandman
```

That's it. The node advertises `_sandman._tcp` and browses for peers on boot —
it joins the fleet by itself. Nothing to register, nothing to configure.

Cross-subnet nodes (no multicast): `sandman attach wan-node 10.0.0.9:4242` adds a
static peer; `detach` removes it.

## Usage

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

`sandman run` behaves like ssh: stdin pipes through, stdout/stderr stream live,
Ctrl-C becomes SIGINT on the remote container (twice = SIGKILL), and the
container's exit code is returned verbatim:

```sh
sandman run b1 -- alpine true && sandman run b2 -- alpine echo done
sandman run b2 -- alpine sh -c 'exit 7'; echo $?    # 7
```

## Monitoring: stats and dashboard

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

## How it works

**Discovery.** Each daemon publishes a `_sandman._tcp.local.` service (TXT
records carry docker version and arch) and browses the same type. Peers are
kept in the registry file with a last-seen timestamp and are forgotten after
90s of silence; a graceful shutdown announces an mDNS goodbye so peers drop
the node immediately. The registry file is the transparent view — `cat
/var/lib/sandman/registry` is the fleet.

mDNS on Linux delivers multicast to one socket per packet when several
processes share UDP 5353 (reuseport hashing), so a specific peer pair can be
starved while others work. The daemon therefore also gossips: each
maintenance tick it pulls one known peer's registry over the wire (`NODES`
verb) and merges it — mDNS bootstraps, TCP converges. Discovery is eventual,
never a hard dependency.

**Jobs.** A job is an ephemeral `docker run`: the daemon creates the container
with a scratch workdir, starts it attached, and relays the three fds over the
connection. Exit codes come from `docker start -a` (never `docker wait`, which
lies on `--rm` containers); signal deaths report 128+signal, shell-style. If
the client vanishes, the daemon kills the container — no orphans. Containers
are named `sandman-<jobid>` and labeled with their owning node, so a fresh
daemon prunes only the containers its own crashed predecessor left behind.

**Wire protocol.** Line-oriented text over TCP; control lines, length-prefixed
frames for data:

```
C: HELLO sandman/0.1
S: OK node=b2 docker=29.6.1
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

One job per connection, no idle timeout — long jobs hang like local processes.

## The HTTP API: repos, pipelines, jobs

The daemon also serves a JSON API on the **same port** — each connection is
routed by its first bytes (an HTTP method line goes to the API, `HELLO` to
the text protocol). This is the data and control plane: immutable revisions
in content-addressed repositories, pipelines that turn revisions into
revisions, and jobs that run them.

- `POST/GET/DELETE /api/v1/repos[/{name}]` — repositories. `DELETE` honors
  `?force=1` (a pipeline's output repo is protected from accidental
  deletion); the internal `spec` repository holding pipeline definitions
  can never be deleted
- `POST /api/v1/repos/{repo}/commits` — start a revision on a branch
- `PUT /api/v1/commits/{id}/files/{path}` — write a file into an open revision
- `DELETE /api/v1/commits/{id}/files/{path}` — tombstone a path: the file
  is removed from the branch's view at this revision (SB-007), and a
  pipeline's output reflects the deletion (the deleted file is absent, not
  stale)
- `POST /api/v1/commits/{id}/files/{path}` — copy a file or directory
  subtree from another commit (destinations already in the view are
  rejected; so are paths already written in the open commit)
- `POST /api/v1/commits/{id}/finish` — close it (`empty: true` makes it an
  explicit empty commit: nothing is readable through it, even at the head)
- `GET /api/v1/commits/{id}` `GET …/files` `GET …/files/{path}` — read;
  `?download=true` adds an attachment disposition, and the content type is
  sniffed from the bytes
- `PUT/GET /api/v1/tags/{name}` `GET /api/v1/tags` — durable global names
  bound to file content, listed with their object reference
- `POST/GET/DELETE /api/v1/pipelines[/{name}]` — pipelines. The create body
  carries `update: true` to apply a new version (creating when absent);
  `reprocess: true` is a persisted spec field: every job re-executes all
  of its datums instead of skipping datums unchanged from a previous
  successful run (SB-166, D-13). Every update processes the input head.
  `DELETE` honors `?force=1` (removes a mid-DAG pipeline despite
  downstream consumers) and `?keepRepo=1` (preserves the output repo for
  reuse). `GET` takes `?history=<n>` (`0` current version, `n` current
  plus n older, `-1` every version), `?name=` and `?allowIncomplete=1`
  (lists pipelines whose definition is lost, by name only); inspection
  takes `?ancestry=<k>` for historical versions
- `POST /api/v1/pipelines/{name}/stop` `/start` — pause/resume: a stopped
  pipeline ignores new commits and replays them on start (backlog — the
  commits finished while it was stopped are consumed together as one job
  over the branch head, SB-050); an update does not restart it
- `standby: true` in the pipeline spec — the pipeline idles in the standby
  state with no work, activates (state `running`) when input arrives, and
  returns to standby once the work settles. Stopping it pauses it; commits
  written while paused never wake it. D-11/12: no fixed activation cap and
  no warm-participant contract (tuning choices, not requirements); D-09:
  a standby pipeline that cannot be provisioned degrades to crashed
  (SB-043), never to a degraded standby state
- **Datums** — a job processes its input as per-datum units of work: every
  path matched by the input glob is a datum (a directory match is a datum
  of its whole subtree), executed by a bounded worker pool sized by
  `parallelism.constant` and capped at the datum count. Each datum runs
  the transform against its own files (`$<input>` is the datum's input
  dir; the full input view is mounted read-only at
  `/sandman/view/<input>`); its output merges into the job's single output
  commit, files at the same path from different datums concatenating in
  datum order (SB-063; D-14: the order is not contractual). Datums whose
  content is unchanged from a previous successful run are skipped
  (SB-006/084/085) and their output carried forward, unless `reprocess`
  is set. `transform.datumTries` retries a failing datum that many times,
  one log entry per attempt (SB-134); the transform's `acceptReturnCode`
  applies per datum. A failing datum fails the job — reason names the
  datum (SB-011) — and the job's `processed`/`recovered`/`failed`/
  `skipped` fields count the outcomes (SB-012)
- **Per-datum statistics** — `enableStats: true` in the pipeline spec
  (one-way: an update cannot disable it, SB-081) records each datum's
  outcome, input/output files, process time, and timing. `GET
  /api/v1/jobs/{id}/datums` lists them — live during execution (queued
  datums included as pending, SB-080/114), state-ordered (failed,
  recovered, success, skipped — SB-082/084), paginated with
  `?limit=&page=` and an out-of-range error (SB-083); `GET
  …/datums/{datumID}` inspects one (an error when stats are off,
  SB-081). The pipeline's output repo gains a `stats` branch — one
  commit per job holding one record file per datum — that downstream
  pipelines can consume (SB-086, SB-113's two-commit count)
- **Scheduling knobs** — `chunkSpec` (a target datum count or chunk size)
  groups a side's glob matches into datums without changing the output
  (SB-102); `maxQueueSize` bounds each worker's pending datums (SB-097);
  `autoscaling` sizes the worker pool to the datum count, capped at the
  configured parallelism (SB-165, D-01's scale-to-zero via standby). A
  running job's `workers` status reports each worker's current datum,
  its start time, and its queue (SB-065), and `POST
  /api/v1/jobs/{id}/datums/{datumID}/restart` aborts a datum and starts
  it over with fresh progress (SB-064)
- **Job queueing** — a pipeline's jobs run strictly one at a time, in
  spawn order: successive input commits queue on the pipeline's gate and
  come up in commit order, so with parallelism 1 exactly one job runs
  (SB-123). Cancelling the running job lets the next queued job start,
  and cancelling one job never cancels the others; a cancel that arrives
  while a job is queued settles it killed without doing any work. The
  system stays correct under a burst of rapid revisions across many
  pipelines: every revision is consumed with a job, the head converges,
  and the job index stays queryable (SB-121)
- **Symlinked output** — a transform may emit its output as symbolic
  links: the output revision stores the linked content (a link to a file
  yields the target's content at the link's path; a link to a directory
  its files), and a linked file's stored content is identical to the
  input's — referenced, not copied (SB-054). Links to the in-container
  input paths (`/sandman/in/...`, `/sandman/view/...`) and to temp files
  the transform wrote (the job's directory is mounted at the container's
  `/tmp`) are resolved when the output is scanned
- **Cross inputs** — `input.cross` is a list of file-scoped inputs whose
  datums combine as the cartesian product (SB-063/161): a job's datum set
  is one glob match from every side. Each side is addressable by its own
  name (`$<name>` and `$<name>_COMMIT`); `input.branch` selects a side's
  branch (default master). Every input commit on any side creates a job
  pairing that commit with the other sides' current heads (SB-120) — a
  side with no head yet contributes no datums — and flushing a set of
  commits (`client.FlushSet`) returns only the pairing job. Datum
  enumeration is available standalone via `POST /api/v1/datums`
  (SB-161)
- `GET /api/v1/jobs[/{id}]` — jobs; `?pipeline=`, `?outputCommit=`,
  `?state=` (repeatable), `?history=` (version depth), `?full=1` (each
  job's own version's transform and input spec — history survives updates).
  Listing jobs of a pipeline that does not exist is an error. Job
  inspection accepts a job id or its output commit. `POST …/{id}/cancel`
  and `…/stop` kill the in-flight work and mark the job killed; `DELETE
  …/{id}` removes the record after finalizing its output revision.
  Flushing is a client-side wait: poll jobs until every job for a
  revision — including downstream stages — is terminal (the `client.Flush`
  helper does exactly that, confirming the graph has stopped growing, and
  terminating empty when every consumer is stopped or failed)
- `GET /api/v1/logs` — a job's log lines: every container's stdout/stderr
  is captured timestamped and complete. `?pipeline=` or `?job=` (either a
  job id or its output commit) select the stream; `?datumPath=` and
  `?datum=` (a file path or its content hash) narrow by input file and
  require a pipeline or job; `?since=` is a relative window excluding
  older lines; `?follow=1` streams new lines live as they are produced
  (newline-delimited `{"line":…}` until the client disconnects). An
  unconstrained query searches every job's logs (D-21)
- `POST /api/v1/transactions` — open a transaction; `POST
  /api/v1/pipelines?transaction=<id>` stages a create or update into it;
  `POST …/transactions/{id}/finish` applies every staged operation
  atomically — all or nothing — and `DELETE …/{id}` aborts. Pipelines
  staged together may consume each other's output (the input repo need
  not exist yet); after finish the chain runs end to end, and updating
  two pipelines in one transaction yields exactly one new job and one new
  commit per pipeline. Finishing fails with "outside of transaction" if a
  staged pipeline was modified outside the transaction meanwhile (SB-163)
- `POST /api/v1/reset` — remove every repository, pipeline, and job;
  idempotent, and it refuses to run on corrupted metadata (D-08)

**State is plain files** under the state dir — cat it:

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

**Pipeline conventions** (the executable contract, mirrored in
`client/` and enforced by `conformance/`): an input is a repo plus a glob,
and the job sees each input as an environment variable named after it whose
value is the input directory (so `$repo/*` addresses the data). Jobs also
receive `OUT`, `JOB_ID`, `OUTPUT_COMMIT`, and `<input>_COMMIT`. Finishing a
commit triggers one job per subscribed pipeline; the job's `OUT` directory
becomes a new commit in the pipeline's output repo (named after the
pipeline) — finish is recursive, so pipelines chain. A pipeline created
over existing history processes the branch head once; a stopped pipeline
replays the commits finished while it was stopped when started; updating a
pipeline applies a new version, kills its in-flight jobs, and processes the
input head under the new transform. A pipeline with no command and no
stdin copies inputs to output; with stdin but no command it is accepted and
immediately fails. Job output is all-or-nothing: a failed or killed job's
output commit is finished empty. A commit's view accumulates across its
ancestors — the newest write to a path wins, and a tombstoned path
(DeleteFile) is gone — and the job for the head revision always sees the
full accumulated content. A pipeline whose
execution environment cannot be provisioned enters the crashed state, and
one whose output repo is deleted mid-run fails with a reason; both recover
by updating the pipeline. Pipeline states: running, standby, paused
(stopped), failure, crashed.

```sh
curl -X POST localhost:4242/api/v1/repos        -d '{"name":"in"}'
curl -X POST localhost:4242/api/v1/pipelines    -d '{"name":"cp","transform":{"image":"alpine","cmd":["sh","-c","cp $in/* $OUT/"]},"input":{"repo":"in","glob":"/*"}}'
curl -X POST localhost:4242/api/v1/repos/in/commits -d '{}'   # → commit id
# PUT files…, finish, then flush for the job
```

The conformance suite (`go test ./conformance/`) is the behaviour contract:
one test per spec record, driving this API through the `client` package.


## Security posture

Trusted-LAN by design: the firewall is the auth. The daemon is the only thing
that touches the docker socket — clients speak the text protocol, never docker.
A raw docker socket is root-equivalent; treat this fabric accordingly: private
networks, or an L2 segment you control. No TLS, no tokens, no encryption (v1).

## Limitations

- mDNS is link-local: discovery works on one broadcast segment; `attach` covers
  the rest
- No scheduler, retries, or queueing — placement is your pipeline, by design
- No auth or encryption — see above
- Fabric `run` artifacts stay in the node's scratch dir
  (`/var/lib/sandman/jobs/<id>/`); pipeline jobs publish to output repos
  instead, which a fetch verb is not built for yet
- Pipelines see one input repo and no cron/aggregate/service inputs yet

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
