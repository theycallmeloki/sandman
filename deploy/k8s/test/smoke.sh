#!/usr/bin/env bash
# Cluster-backed smoke test for the sandman worker fleet
# (deploy/k8s). Asserts the DecoratorController exists, the hook server
# is ready, every worker node has a 2/2 worker pod advertising the
# node's IP:9595, and (optionally) that the control plane registered
# them.
#
# Usage: from the repo root:  bash deploy/k8s/test/smoke.sh
# Optional: SANDMAN_ADDR=<daemon> sandman on PATH to verify registration.
set -euo pipefail

kubectl get decoratorcontroller sandman-worker >/dev/null 2>&1 || {
  echo "FAIL: decoratorcontroller sandman-worker not found" >&2; exit 1
}
kubectl -n sandman rollout status deploy/sandman-worker-hook --timeout=120s >/dev/null

ok=1
while read -r name ip; do
  pod="sandman-worker-$name"
  ready=$(kubectl -n sandman get pod "$pod" -o jsonpath='{.status.containerStatuses[*].ready}' 2>/dev/null || true)
  if [ "$ready" != "true true" ]; then
    echo "FAIL: $pod not 2/2 ready (got '$ready')" >&2; ok=0; continue
  fi
  if ! kubectl -n sandman get pod "$pod" -o jsonpath='{.spec.containers[0].args}' | grep -q "$ip:9595"; then
    echo "FAIL: $pod does not advertise $ip:9595" >&2; ok=0
  fi
done < <(
  kubectl get nodes -o name | sed 's|^node/||' | while read -r n; do
    if kubectl get node "$n" -o jsonpath='{.spec.taints}' | grep -q 'control-plane'; then
      continue
    fi
    ip=$(kubectl get node "$n" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
    echo "$n $ip"
  done
)

[ "$ok" = 1 ] || exit 1
echo "PASS: worker fleet running on all worker nodes"

if [ -n "${SANDMAN_ADDR:-}" ] && command -v sandman >/dev/null; then
  if SANDMAN_ADDR="$SANDMAN_ADDR" sandman nodes | grep -q 'talos-'; then
    echo "PASS: workers registered with $SANDMAN_ADDR"
  else
    echo "FAIL: no talos workers registered with $SANDMAN_ADDR" >&2; exit 1
  fi
fi
