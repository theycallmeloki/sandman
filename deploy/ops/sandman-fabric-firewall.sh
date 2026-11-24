#!/bin/sh
# Restrict the Sandman fabric ports to their intended peers.
#
#   daemon host : TCP 4242 only from the operator and worker CIDRs
#   worker host : TCP 9595 only from the control-plane host
#
# The rule lives in its own nftables table (`inet sandman_fabric`), so
# applying it never flushes SSH, the container runtime, or any other
# firewall system. Every other packet is left to the host's existing
# policy. Run with --check first; see deploy/ops/README.md for rollback
# and lockout prevention.
#
# This is an ingress filter only. It does not authenticate a caller and
# it does not replace the trusted-LAN assumption: 9595 remains an
# unauthenticated worker exec endpoint, and 4242 is only as safe as the
# CIDRs named here.
set -eu

PROG=${0##*/}
TABLE=sandman_fabric
ROLE=daemon
CHECK=0
ROLLBACK=0
IFACE=
PEERS=
CONTROL=

usage() {
	cat <<EOF
usage: $PROG --role daemon|worker|both [options]

  --role daemon        allow TCP 4242 from --peer CIDRs
  --role worker        allow TCP 9595 from --control-plane HOST
  --role both          both rules on one host
  --peer CIDR          operator/worker CIDR allowed to reach 4242
                       (repeatable, e.g. 192.168.1.20/32)
  --control-plane HOST
                       daemon host IP allowed to reach 9595
  --interface IFACE    only accept on this interface (optional)
  --check              validate the ruleset and print it; apply nothing
  --rollback           remove the inet $TABLE table; apply nothing
  -h, --help           this text

Examples:
  sudo $PROG --role daemon --peer 192.168.1.20/32 --peer 192.168.1.0/24
  sudo $PROG --role worker --control-plane 192.168.1.15
  sudo $PROG --role daemon --peer 192.168.1.20/32 --check
EOF
}

die() {
	echo "$PROG: $*" >&2
	exit 1
}

valid_addr() {
	[ -n "$1" ] || return 1
	# Address/CIDR only. Reject quotes, braces, whitespace and shell
	# metacharacters so a value cannot inject an nft statement.
	case "$1" in
		*[!0-9A-Fa-f:./]*) return 1 ;;
	esac
	return 0
}

append_peer() {
	if [ -z "$PEERS" ]; then
		PEERS=$1
	else
		PEERS="$PEERS
$1"
	fi
}

while [ $# -gt 0 ]; do
	case "$1" in
		--check) CHECK=1 ;;
		--rollback) ROLLBACK=1 ;;
		--role)
			[ $# -ge 2 ] || die "--role needs a value"
			ROLE=$2
			shift
			;;
		--peer)
			[ $# -ge 2 ] || die "--peer needs a value"
			valid_addr "$2" || die "invalid --peer address: $2"
			append_peer "$2"
			shift
			;;
		--control-plane)
			[ $# -ge 2 ] || die "--control-plane needs a value"
			valid_addr "$2" || die "invalid --control-plane address: $2"
			CONTROL=$2
			shift
			;;
		--interface)
			[ $# -ge 2 ] || die "--interface needs a value"
			case "$2" in
				*[!A-Za-z0-9_.:-]*) die "invalid interface name: $2" ;;
			esac
			IFACE=$2
			shift
			;;
		-h|--help) usage; exit 0 ;;
		*) die "unknown argument: $1 (try --help)" ;;
	esac
	shift
done

case "$ROLE" in
	daemon|worker|both) ;;
	*) die "--role must be daemon, worker, or both" ;;
esac

command -v nft >/dev/null 2>&1 || die "nft not found; this script targets nftables hosts"

if [ "$ROLLBACK" = 1 ]; then
	[ "$(id -u)" -eq 0 ] || die "run --rollback as root"
	if nft list table inet "$TABLE" >/dev/null 2>&1; then
		nft delete table inet "$TABLE"
		echo "removed inet $TABLE; the ports now follow the host's existing policy"
	else
		echo "inet $TABLE is already absent"
	fi
	exit 0
fi

case "$ROLE" in
	daemon)
		[ -n "$PEERS" ] || die "--role daemon needs at least one --peer CIDR"
		[ -z "$CONTROL" ] || die "--control-plane is only valid with --role worker/both"
		;;
	worker)
		[ -n "$CONTROL" ] || die "--role worker needs --control-plane HOST"
		[ -z "$PEERS" ] || die "--peer is only valid with --role daemon/both"
		;;
	both)
		[ -n "$PEERS" ] || die "--role both needs at least one --peer CIDR"
		[ -n "$CONTROL" ] || die "--role both needs --control-plane HOST"
		;;
esac

tmp=$(mktemp) || die "cannot create a temporary file"
check_tmp=$(mktemp) || die "cannot create a temporary file"
trap 'rm -f "$tmp" "$check_tmp"' EXIT HUP INT TERM

# Emit one accept rule per CIDR, then a final drop for that port. The
# chain policy stays accept so unrelated traffic is untouched.
emit_accept() { # port, family, address
	if [ -n "$IFACE" ]; then
		printf '    iifname "%s" tcp dport %s %s saddr %s accept\n' "$IFACE" "$1" "$2" "$3"
	else
		printf '    tcp dport %s %s saddr %s accept\n' "$1" "$2" "$3"
	fi
}

emit_port_rules() { # port, newline-separated addresses
	port=$1
	addrs=$2
	while IFS= read -r addr; do
		[ -n "$addr" ] || continue
		case "$addr" in
			*:*) emit_accept "$port" ip6 "$addr" ;;
			*) emit_accept "$port" ip "$addr" ;;
		esac
	done <<EOF
$addrs
EOF
	printf '    tcp dport %s drop\n' "$port"
}

{
	echo "table inet $TABLE {"
	echo "  chain input {"
	echo "    type filter hook input priority filter; policy accept;"
	case "$ROLE" in
		daemon) emit_port_rules 4242 "$PEERS" ;;
		worker) emit_port_rules 9595 "$CONTROL" ;;
		both)
			emit_port_rules 4242 "$PEERS"
			emit_port_rules 9595 "$CONTROL"
			;;
	esac
	echo "  }"
	echo "}"
} >"$tmp"

if [ "$CHECK" = 1 ]; then
	# A check batch must mirror apply: delete an existing table first, or
	# nft reports the table/chain as already present.
	if nft list table inet "$TABLE" >/dev/null 2>&1; then
		{ echo "delete table inet $TABLE"; cat "$tmp"; } >"$check_tmp"
		echo "--- currently loaded inet $TABLE ---"
		nft list table inet "$TABLE"
	else
		cp "$tmp" "$check_tmp"
		echo "--- inet $TABLE is not currently loaded ---"
	fi
	nft -c -f "$check_tmp" || die "nft rejected the generated ruleset"
	echo "--- generated ruleset (nothing applied) ---"
	cat "$tmp"
	exit 0
fi

[ "$(id -u)" -eq 0 ] || die "run as root (or use --check to validate)"
if nft list table inet "$TABLE" >/dev/null 2>&1; then
	nft delete table inet "$TABLE"
fi
nft -f "$tmp"
echo "applied inet $TABLE:"
nft list table inet "$TABLE"
