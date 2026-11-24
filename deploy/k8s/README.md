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
  currently pins `sandman:worker-v0.2.56`.

If the daemon has auth disabled, the headers are harmless but the Secrets
may be omitted once the hook is also updated to not require them.
