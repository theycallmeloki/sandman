# Sandman threat model and deferral conditions

Status: working security note for the single-team Clive deployment,
2026-11-24. It records the decision to **defer full identity, RBAC, and
tenancy**, the seven conditions that make that deferral acceptable, and the
risks that remain. It is not a claim that the fabric is safe on an
untrusted network.

Companion operations material: `deploy/ops/README.md` (service user,
rollback, firewall runbook) and `deploy/ops/sandman-fabric-firewall.sh`
(host nftables rule). Kubernetes limits are documented in
`deploy/k8s/README.md`.

## Scope and trust boundaries

Sandman is a LAN control plane at `192.168.1.15:4242` plus execution
workers that join it. The deployment is one producing team in one private
cluster/LAN. The relevant boundaries are:

| Boundary | What crosses it | Current control |
| --- | --- | --- |
| Operator → daemon | HTTP API, MCP, dashboard, CLI text protocol on 4242 | Optional HTTP Basic Auth on the HTTP surface; the text protocol is outside it; trusted LAN |
| Worker → daemon | host registration (`/api/v1/hosts`), heartbeats, job lifecycle | Basic Auth on registration when configured; worker not otherwise identified |
| Daemon → worker | job payloads and `/exec` calls to 9595 | **No authentication**; the exec endpoint is a separate protocol |
| Pod → host | worker `hostNetwork`, privileged dockerd-in-Docker, GPU hostPath | Node/CNI policy; **pod NetworkPolicy does not cover hostNetwork** |
| Daemon → docker | `/var/run/docker.sock` | Service user in the socket group; socket is root-equivalent |
| Sandman → transforms | images, bind-mounted inputs, output sides, secret bindings | Container isolation and explicit process environment allowlist; no tenant boundary |
| Operator ingress → services | REST/MCP exposed through ingress (`whitelist-source-range`) | External ingress configuration, not in this repository |
| Agent/automation → fabric | AX/ATE egress | External egress policy |

The trusted-LAN rule is the baseline: private network or an L2 segment you
control, no direct internet exposure. HTTP Basic Auth is defence against
accidental access, not a replacement for transport security: there is no
TLS, so a network observer can read credentials and payloads.

## Assets and adversaries

- **Docker socket / host root.** Anyone who can reach the socket can start a
  privileged container and become root. Sandman holds that power by design;
  so does every worker with a docker socket or a privileged dind sidecar.
- **Secrets at rest and in backup.** Pipeline secret bindings, environment
  values, and any credentials in the store. `/api/v1/backup` streams state;
  it must not stream secret material to an unauthenticated caller.
- **Job inputs/outputs and provenance.** Business data and commits; a
  malicious or buggy transform can read its mounted sides.
- **Fleet capacity.** A caller who can reach 9595 can submit arbitrary
  containers to that worker, subject only to the worker's own limits.
- **Audit evidence.** Who submitted which mutation, from where, and whether
  it succeeded.

Adversaries in scope for the deferral: a mistaken operator, a compromised
workstation on the same LAN, a misconfigured agent with broad egress, a
worker whose endpoint is exposed beyond the fabric, and a malicious
transform. A determined external attacker with network access is **not**
in scope: that deployment needs the full identity/RBAC/tenancy work.

## Principal risks

| Risk | Why it matters | Current mitigation | Residual |
| --- | --- | --- | --- |
| Unauthenticated worker `/exec` | Arbitrary job execution on a worker; worker is privileged | 9595 firewall rule to the control-plane host; trusted LAN | No caller authentication; rule must actually be applied on every node; token work is pending |
| Unauthenticated text protocol | Non-HTTP verbs on 4242 are outside Basic Auth | 4242 peer-only firewall rule | LAN peer can still speak the protocol; no TLS |
| Plaintext secret exposure through backup | `/api/v1/backup` can carry plaintext secrets | Being hardened by the secret/backup P0 slice | Must verify unauthenticated/off-host backup exposes no values before production data |
| World-readable secrets or inherited env | Job/user code could read credentials outside its binding | Secret-store/job-mount 0600/0700, backup secret pruning, and process-runner env allowlist implemented | Process backend still runs as the daemon user; docker group remains root-equivalent |
| Broad 4242/9595 exposure | Turns a LAN misconfiguration into remote execution | Host/CNI rule artifact and runbook | Host rules are not installed by this repository and not persisted automatically |
| Docker socket root-equivalence | Service user is not a security boundary against a malicious job | `NoNewPrivileges`, `ProtectSystem`, `PrivateTmp`, empty capability set | **Not removed**: docker group membership is effectively root; dind/privileged pods are worse |
| Worker pods are privileged + hostNetwork | Pod escape or node compromise; NetworkPolicy bypass | Namespace PodSecurity `privileged` is intentional; node/CNI rule required | Cluster compromise is node compromise; no tenant isolation |
| No TLS | Credentials and data readable on-path | Private LAN | Observer/ARP/MITM |
| No audit trail | Misuse cannot be detected or reconstructed | Append-only mutation audit (op, source IP, timestamp, status, duration; no bodies/secrets) | No attribution to a principal without RBAC |
| mDNS discovery | Advertises the daemon on the local segment | Link-local only; attach for routed peers | Same-segment observer sees the service |
| Agent egress | Autonomous agents could reach 4242/9595 | AX/ATE egress policy outside this repo | All-port cleartext egress would re-open the fabric |

### Non-claims

- Running the daemon as `sandman` does **not** make the docker socket safe.
  The service user reduces accidental root-owned state and limits the
  daemon process itself; a docker-socket holder can still become root.
- The Kubernetes NetworkPolicy is **not** a worker-fleet policy. Worker pods
  use `hostNetwork: true`; pod policy is bypassed by definition, and a CNI
  that does not implement NetworkPolicy makes the manifest a no-op. The
  host or host-capable CNI rule remains mandatory.
- Basic Auth does **not** cover the text protocol or `/exec`, and there is
  no mTLS.
- No per-team identity or tenancy is implied. A caller who reaches the API
  acts as the one trusted operator role.

## The seven deferral conditions (2026-11-24 memo)

Tenancy, full wire authentication, and per-principal authorization are
deferred only while **all seven** hold. The list is verbatim from the memo:

1. 4242 is reachable only from the pemqual operator host(s) and the sandman
   worker nodes; 9595 reachable only from the control-plane host. Enforced
   by host firewall on 192.168.1.15 and every worker node (or a CNI
   host-firewall that actually covers hostNetwork pods), and verified by an
   off-host probe.
2. No ingress route exposes MCP or REST beyond the internal hostname;
   whitelist-source-range is narrowed from all RFC1918 to the actual client
   CIDRs, and the absence of auth is documented on the ingress object.
3. Sandman stores no credential that grants control-plane, cluster,
   registry-push, or long-lived cloud access. Today's litellm and typesafe
   keys qualify; a future registry credential does not.
4. Secret store and job mounts are 0600/0700; the backup endpoint no longer
   serves secret material unauthenticated; the daemon does not inherit
   secret-bearing environment into transforms.
5. AX/ATE egress policies cannot reach 4242/9595. Any widening to all-port
   cleartext egress re-opens the fabric to autonomous agents.
6. A mutation audit trail exists (at minimum: op, source IP, timestamp,
   status), so misuse can be detected even if it cannot be attributed to a
   principal.
7. Still exactly one producing team. The moment a second team or program
   writes pipelines/secrets, condition 7 fails and tenancy becomes required.

### Status tracking

- Condition 1: implementation support landed as a reviewable host script
  and runbook; enforcement is an operational step and must be verified with
  off-host probes before relying on it. Kubernetes hostNetwork bypass is
  documented.
- Condition 2: external ingress configuration, outside this repository.
- Conditions 3 and 4: Basic Auth gates the HTTP surface, and the secret store,
  job mounts, backup exclusion, and process-runner env isolation are now
  implemented. Condition 3 still requires an operational audit of which
  long-lived credentials live in Sandman.
- Condition 5: external AX/ATE egress policy.
- Condition 6: mutation audit is implemented; verify the live audit file on
  the daemon host.
- Condition 7: true for the current single-team Clive workload.

If any condition fails, stop treating the LAN as the security boundary and
complete the deferred work below before continuing with production data.

## P0/P1 scope of the deferral

P0 (must hold before Clive production data):

- secret hygiene: 0600/0700 store and job mounts, backup redaction,
  transform environment isolation;
- peer-only firewall/CNI rules for 4242 and 9595, verified off-host;
- optional HTTP Basic Auth on the HTTP surface (implemented) with secrets
  supplied out of band;
- non-root daemon service account and state-directory ownership, with
  rollback documented.

P1 (required before the fabric can be considered trusted beyond the
current LAN):

- authenticated worker join and `/exec` (worker token), so a reachable
  worker is not automatically an open executor;
- mutation audit log (op, source IP, timestamp, status);
- explicit ingress/egress policy for MCP/REST and agent networks;
- continued secret-at-rest verification and backup tests.

P2: this document, the runbook, and operational hardening guidance.

## When full RBAC/tenancy becomes required

Full identity, RBAC, and tenancy are required as soon as any of these is
true (condition 7 failing is sufficient on its own):

- a second team, program, or external party writes pipelines or secrets;
- any user or service other than the current trusted operator role needs
  differentiated access (read-only, job-submit, admin, per-project);
- the daemon or a worker is exposed outside the private LAN, or ingress is
  widened beyond the named client CIDRs;
- workers are supplied by an untrusted party, run on shared infrastructure,
  or the docker socket is not effectively controlled by the operator;
- regulated, customer, or cross-team confidential data is processed;
- audit must attribute an action to a principal rather than only detect it;
- the deployment becomes multi-cluster or multi-tenant.

Until then, the security boundary is the set of peer CIDRs, the private
network, and the operational discipline in `deploy/ops/README.md` — not the
optional Basic Auth password and certainly not the non-root unit alone.
