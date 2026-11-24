# Sandman on Kubernetes

The `sandman` ArgoCD application applies this directory into the `sandman`
namespace. It contains the per-node privileged worker fleet, the
metacontroller hook, and the Prometheus monitor for the LAN daemon.

## HTTP Basic Auth

When the daemon is configured with HTTP Basic Auth, the worker fleet and
Prometheus must use the same credentials. Create the two Kubernetes
Secrets out of band (never commit the values):

```sh
kubectl -n sandman create secret generic sandman-auth \
  --from-literal=username=laneone \
  --from-literal=password='replace-me'
kubectl -n monitoring create secret generic sandman-auth \
  --from-literal=username=laneone \
  --from-literal=password='replace-me'
```

- `hook-configmap.yaml` injects `SANDMAN_AUTH_USER` and
  `SANDMAN_AUTH_PASSWORD` into each worker from `sandman/sandman-auth`.
  Workers use those credentials for `/api/v1/hosts` registration.
- `sandmand-monitor.yaml` attaches the same credentials to the
  ServiceMonitor scrape.
- The worker image must be new enough to send the credentials; the hook
  currently pins `sandman:worker-v0.2.57`.

If the daemon has auth disabled, the headers are harmless but the Secrets
may be omitted once the hook is also updated to not require them.

## NetworkPolicy and hostNetwork

`networkpolicy.yaml` adds a default-deny ingress policy for the `sandman`
namespace and allows only TCP 80 to the `sandman-worker-hook` pod that
metacontroller calls. It is deliberately narrow: it covers the ordinary
hook pod, not the worker fleet. It is only effective with a CNI that
implements NetworkPolicy; on a CNI without enforcement this is a no-op.

The worker pods are created with `hostNetwork: true` and a privileged
dockerd-in-Docker sidecar. **Pod NetworkPolicy does not apply to
hostNetwork pods.** The worker's unauthenticated exec endpoint on TCP 9595
(and its traffic to the LAN daemon on 4242) is therefore not restricted by
this manifest. The node/CNI rule remains required:

- enforce 4242 only from operator/worker CIDRs and 9595 only from the
  control-plane host on each node (host firewall, or a CNI that actually
  covers hostNetwork pods);
- `deploy/ops/sandman-fabric-firewall.sh` and `deploy/ops/README.md` are
  the reference host-side rule and runbook.

If metacontroller runs in a known namespace, narrow the hook ingress rule
with a `namespaceSelector` instead of allowing the port from any source.
Validate the directory with `kubectl kustomize deploy/k8s` before letting
ArgoCD reconcile it.

## Worker exec firewall

The `worker-firewall.yaml` DaemonSet runs a privileged hostNetwork nft helper on every non-control-plane node. It allows TCP 9595 only from `192.168.1.15` and drops all other ingress to that port. This is required because the worker pods themselves are hostNetwork and cannot be protected by Kubernetes NetworkPolicy.
