# Sandman host operations: service user and fabric firewall

These are the Linux/systemd host steps that sit beside the installer. They
do not replace the trusted-LAN model: the daemon's HTTP surface and the
worker's `/exec` endpoint are separate protocols, HTTP Basic Auth is
optional, and the docker socket remains root-equivalent.

## Service user and state ownership

`make install` (Linux) now creates the `sandman` user and group, adds the
user to the `docker` group, creates `/var/lib/sandman` and
`/var/lib/sandman/jobs` as `sandman:sandman` mode `0750`, and re-owns any
existing state left by a root-run install. The operations are idempotent:

```sh
sudo make install daemon
# an already-running root daemon keeps its old credentials until restart
sudo systemctl restart sandman
```

Both units run with `User=sandman`, `Group=sandman`,
`SupplementaryGroups=docker`, and `StateDirectory=sandman`. The daemon also
gets `PrivateTmp=true`; the worker deliberately does not (it hands
`/tmp/sandman-worker-*` bind-mount paths to the host dockerd, which
resolves them in the host mount namespace).

`ProtectSystem=strict` plus the drop-ins make only `/var/lib/sandman`
(and the worker's host `/tmp`) writable. A pipeline that mounts a writable
host path elsewhere needs an explicit drop-in:

```sh
sudo systemctl edit sandman
# [Service]
# ReadWritePaths=/srv/sandman-scratch
```

Read-only host volumes remain readable (`ProtectHome=read-only`). Do not
add `ReadWritePaths=/` to the daemon or `PrivateTmp=true` to the worker
unless you accept re-opening the host to job containers.

Check what is actually running:

```sh
systemctl show sandman -p User -p Group -p SupplementaryGroups
systemctl show sandman-worker -p User -p Group -p SupplementaryGroups
stat -c '%U:%G %a %n' /var/lib/sandman /var/lib/sandman/jobs
```

`make install` preserves an already-enabled
`/etc/systemd/system/sandman-worker.service` instead of overwriting it.
`install.sh` bakes each worker's `-name`, `-advertise`, `-control`, and
labels into that file, so replacing it on every daemon install would
silently re-point or strand the worker. The worker hardening drop-in is
still refreshed. Force a unit refresh with `make install worker` on a host
where the worker is not enabled, or edit the existing unit and restart.

If `/var/run/docker.sock` is owned by a group other than `docker`,
`make install` warns. Add the user to the real group and add a drop-in:

```sh
sudo usermod -aG <socket-group> sandman
sudo systemctl edit sandman
# [Service]
# SupplementaryGroups=<socket-group>
sudo systemctl restart sandman
```

### HTTP Basic Auth secret file

`EnvironmentFile=-/etc/sandman/auth.env` is read by systemd before it drops
to `sandman`, so the env file itself can stay `root:root 0600`. A password
**file** referenced from it (`SANDMAN_AUTH_PASSWORD_FILE=...`) is opened
by the daemon process, so it must be readable by the service user, e.g.
`root:sandman 0640`. Do not move the auth env into the service-owned state
directory.

### Rollback

The units and drop-ins are ordinary systemd files:

```sh
sudo systemctl stop sandman sandman-worker
sudo make uninstall                 # binary, units, and 10-hardening drop-ins
sudo systemctl daemon-reload
```

`make uninstall` intentionally leaves the `sandman` user/group and
`/var/lib/sandman` in place, so a rollback to a root-run daemon does not
destroy repos or jobs. To roll back the account/ownership as well:

```sh
sudo chown -R root:root /var/lib/sandman
sudo userdel sandman
sudo groupdel sandman
```

If you need the old root unit temporarily, install the previous
`sandman.service` and run `systemctl daemon-reload`; do not put
`User=root` in a persistent drop-in — it silently removes the isolation
the unit is meant to provide.

## Fabric firewall

`deploy/ops/sandman-fabric-firewall.sh` installs a dedicated nftables table
(`inet sandman_fabric`) that allows:

- TCP **4242** only from the operator and worker CIDRs (daemon host);
- TCP **9595** only from the control-plane host (worker hosts);
- a final drop for the configured port, so non-fabric peers fail.

All other host traffic (SSH, DNS, egress, other listeners) is untouched;
the chain policy is `accept`. The script never flushes the global ruleset.

### Check, apply, verify

Run `--check` first. It validates the generated batch with `nft -c` and
prints both the currently loaded table and what would be applied. It makes
no changes.

```sh
# daemon host 192.168.1.15
sudo ./sandman-fabric-firewall.sh --role daemon \
  --peer 192.168.1.20/32 \
  --peer 192.168.1.21/32 \
  --peer 192.168.1.0/24 \
  --check

# worker node
sudo ./sandman-fabric-firewall.sh --role worker \
  --control-plane 192.168.1.15 --check

# apply only after the check output is correct
sudo ./sandman-fabric-firewall.sh --role daemon --peer 192.168.1.20/32 ...
sudo ./sandman-fabric-firewall.sh --role worker --control-plane 192.168.1.15

nft list table inet sandman_fabric
```

A host that is both a control plane and a worker uses `--role both`. The
`--peer` list should be the smallest set of operator and worker addresses
that actually need 4242; do not use `0.0.0.0/0` or a whole RFC1918 range
unless that is genuinely the fabric. IPv4 and IPv6 CIDRs are accepted.

Verify from a machine **outside** the allowed set that both ports are
unreachable, and from a worker/operator machine that the intended path
still works:

```sh
nc -vz -w 2 192.168.1.15 4242
nc -vz -w 2 <worker-ip> 9595
```

### Rollback and lockout prevention

```sh
sudo ./sandman-fabric-firewall.sh --rollback
# equivalent:
sudo nft delete table inet sandman_fabric
```

Lockout notes:

- The rule matches only TCP 4242/9595. SSH and the rest of the host are
  never dropped by this table.
- Keep the current SSH session open while applying and test from a second
  session before closing it; a serial/console path is the fallback.
- `--check` performs a parse/transaction check and does not commit.
- The example `sandman-fabric-firewall.service` persists the daemon-side
  rule. Install the script at `/usr/local/sbin/sandman-fabric-firewall`,
  adjust its `--peer` list, then `systemctl enable --now
  sandman-fabric-firewall`. The Kubernetes worker DaemonSet persists the
  worker-side 9595 rule on its nodes. The service file here is a template,
  not an automatic install step.
- `--rollback` removes only `inet sandman_fabric`. It cannot restore a
  different firewall's rules that were changed outside this script.

### Kubernetes and hostNetwork

The cluster worker pods set `hostNetwork: true` and a privileged
dockerd-in-Docker sidecar. **A pod `NetworkPolicy` does not apply to
hostNetwork pods**, so the `deploy/k8s/networkpolicy.yaml` policy only
covers the ordinary hook pod. The 4242/9595 rule must still be enforced on
each Kubernetes node (host firewall, CNI with host-firewall support, or the
node's own enforcement), and this script is the host-side reference for
that rule.

## Worker exec endpoint is separate

HTTP Basic Auth covers the HTTP API, MCP, and dashboard (and worker
*registration*). The fabric's text protocol and the worker's `/exec`
endpoint are different protocols on different paths: `/exec` remains
unauthenticated and, when `-advertise` is set, listens on all interfaces.
The 9595 firewall rule restricts **who can reach** that endpoint; it does
not authenticate the caller and does not change the protocol. Do not treat
the firewall as a substitute for the worker-token work
(`pemqual-9q2.6.3`).

See `docs/security/sandman-threat-model.md` for the full risk model and the
2026-11-24 deferral conditions.
