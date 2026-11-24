# Small privileged host-network firewall helper for Sandman worker nodes.
#
# The worker exec port is 9595 on the node's LAN address. The worker pod is
# hostNetwork, so Kubernetes NetworkPolicy cannot protect it; this
# DaemonSet applies an nftables ingress rule in the host network namespace
# that admits only the Sandman control-plane host.
FROM alpine:3.21
RUN apk add --no-cache nftables
COPY apply.sh /usr/local/bin/apply-sandman-worker-firewall
RUN chmod 0755 /usr/local/bin/apply-sandman-worker-firewall
ENTRYPOINT ["/usr/local/bin/apply-sandman-worker-firewall"]
