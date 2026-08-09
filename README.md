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

## How it works

**Discovery.** Each daemon publishes a `_sandman._tcp.local.` service (TXT
records carry docker version and arch) and browses the same type. Peers are
kept in the registry file with a last-seen timestamp and are forgotten after
90s of silence; a graceful shutdown announces an mDNS goodbye so peers drop
the node immediately. The registry file is the transparent view — `cat
/var/lib/sandman/registry` is the fleet.

**Jobs.** A job is an ephemeral `docker run`: the daemon creates the container
with a scratch workdir, starts it attached, and relays the three fds over the
connection. Exit codes come from `docker start -a` (never `docker wait`, which
lies on `--rm` containers); signal deaths report 128+signal, shell-style. If
the client vanishes, the daemon kills the container — no orphans.

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
- Artifacts stay in the node's scratch dir (`/var/lib/sandman/jobs/<id>/`);
  a fetch verb is not built yet

## Development

```sh
make build        # static binary
go vet ./...      # sanity
```

Test fabric on one host (mDNS loopback lets two daemons see each other):

```sh
./sandman daemon -name b1 -port 4242 -state /tmp/sandman-a &
./sandman daemon -name b2 -port 4243 -state /tmp/sandman-b &
sleep 3 && ./sandman nodes -state /tmp/sandman-a
echo hi | ./sandman run -state /tmp/sandman-a b2 -- alpine cat
```
