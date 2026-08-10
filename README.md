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

- `POST/GET/DELETE /api/v1/repos[/{name}]` — repositories
- `POST /api/v1/repos/{repo}/commits` — start a revision on a branch
- `PUT /api/v1/commits/{id}/files/{path}` — write a file into an open revision
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
- `POST/GET/DELETE /api/v1/pipelines[/{name}]` — pipelines
- `POST /api/v1/pipelines/{name}/stop` `/start` — pause/resume: a stopped
  pipeline ignores new commits and replays them on start (backlog)
- `GET /api/v1/jobs[/{id}]` — jobs; `?pipeline=`, `?outputCommit=`,
  `?state=` (repeatable), `?full=1` (include the pipeline spec). Job
  inspection accepts a job id or its output commit. `POST …/{id}/cancel`
  kills the in-flight work and marks the job killed; `DELETE …/{id}`
  removes the record after finalizing its output revision. Flushing is a
  client-side wait: poll jobs until every job for a revision — including
  downstream stages — is terminal (the `client.Flush` helper does exactly
  that, confirming the graph has stopped growing)

**State is plain files** under the state dir — cat it:

```
repos/<repo>/refs/<branch>       one-line commit ids
repos/<repo>/commits/<id>.json   revision records
repos/<repo>/.objects/<sha>/…    file content, content-addressed (dedup)
pipelines/<name>.json            pipeline records
jobs/<id>/job.json               job records (+ in/ and out/ scratch dirs)
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
replays the commits finished while it was stopped when started. A pipeline
with no command and no stdin copies inputs to output; with stdin but no
command it is accepted and immediately fails. Job output is all-or-nothing:
a failed or killed job's output commit is finished empty. A commit's view
accumulates across its ancestors — the newest write to a path wins — and
the job for the head revision always sees the full accumulated content.

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
