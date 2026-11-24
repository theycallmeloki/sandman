#!/bin/sh
# Apply the worker-side 9595 firewall in the host network namespace.
# The pod runs hostNetwork + privileged; nft therefore edits the node's
# network namespace. The table is namespaced to this purpose and is
# recreated idempotently on pod start.
set -eu

CONTROL_PLANE=${SANDMAN_CONTROL_PLANE:-192.168.1.15}
TABLE=sandman_fabric_worker

if nft list table inet "$TABLE" >/dev/null 2>&1; then
	nft flush table inet "$TABLE"
else
	nft add table inet "$TABLE"
fi

nft add chain inet "$TABLE" input '{ type filter hook input priority filter; policy accept; }'
nft add rule inet "$TABLE" input tcp dport 9595 ip saddr "$CONTROL_PLANE" accept
nft add rule inet "$TABLE" input tcp dport 9595 drop

nft list table inet "$TABLE"
exec sleep infinity
