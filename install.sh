#!/bin/sh
# sandman worker bootstrap: fetch the repo and delegate the install to the
# Makefile (the single source of install logic — nothing is duplicated
# here). Usage:
#
#   curl -sSL https://raw.githubusercontent.com/theycallmeloki/sandman/main/install.sh | sh
#
# Everything is auto-filled: worker name (hostname), exec port (4343),
# advertise address (the host's default-route LAN IP, so the daemon can
# dial back), and the control plane — the worker discovers the daemon
# itself via mDNS (role=daemon; the fleet expects one daemon per LAN).
# The worker's systemd unit is written with these values baked in; edit
# /etc/systemd/system/sandman-worker.service and restart to change them.
#
# Optional env: CONTROL=http://host:4242  NAME=worker-1  PORT=4343
#               LABELS="-label gpu -label fast"
set -e

need() {
	command -v "$1" >/dev/null 2>&1 && return 0
	echo "install.sh: $1 not found — installing it"
	sudo apt-get install -y -qq "$1" || {
		echo "install.sh: apt install failed — refreshing package lists and retrying"
		sudo apt-get update -qq
		sudo apt-get install -y -qq "$1"
	}
	command -v "$1" >/dev/null 2>&1 || { echo "install.sh: failed to install $1" >&2; exit 1; }
}
need curl
need make

if ! command -v docker >/dev/null 2>&1; then
	echo "install.sh: docker is required — install it first (https://docs.docker.com/engine/install/)" >&2
	exit 1
fi

# fetch the repo — the Makefile does the installing
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
curl -fsSL https://github.com/theycallmeloki/sandman/archive/refs/heads/main.tar.gz | tar -xz -C "$tmp"
cd "$tmp"/sandman-main

NAME=${NAME:-$(hostname)}
PORT=${PORT:-4343}
# the default-route interface's source address is the LAN-reachable IP —
# `hostname -I` can lead with the docker bridge (172.x), which the daemon
# could not dial. ADVERTISE is an explicit opt-in: it flips the worker's
# exec endpoint from loopback to 0.0.0.0 (worker.go), and that endpoint
# is unauthenticated — remote placement needs it, a single-host install
# can leave ADVERTISE empty to keep the loopback bind.
ADVERTISE=${ADVERTISE:-$(ip route get 1.1.1.1 2>/dev/null | sed -n 's/.*src \([0-9.]*\).*/\1/p')}
if [ -z "$ADVERTISE" ]; then
	ADVERTISE=$(hostname -I 2>/dev/null | awk '{print $1}')
fi
if [ -z "$ADVERTISE" ]; then
	echo "install.sh: cannot determine this host's LAN address" >&2
	exit 1
fi

# unit_safe rejects values that would corrupt the root-written systemd
# unit below: a newline injects a fresh directive, % triggers systemd's
# specifier expansion, and quotes/whitespace misparse ExecStart's argv.
# NAME additionally gets the daemon's default dot-to-dash rule (main.go
# sanitizeName), so a DHCP-style "host.local" stays a single argv word.
unit_safe() { # $1 = field label, $2 = value, $3 = allow-space (1)
	case "$2" in
		*$'\n'*|*$'\r'*|*'%'*|*'"'*)
			echo "install.sh: $1 contains characters that would corrupt the systemd unit (newline, %, or quote)" >&2
			exit 1 ;;
	esac
	if [ "$3" != 1 ]; then
		case "$2" in
			*' '*) echo "install.sh: $1 must not contain spaces" >&2; exit 1 ;;
		esac
	fi
}
NAME=$(printf '%s' "$NAME" | tr '.' '-')
unit_safe "NAME" "$NAME" 0
case "$PORT" in
	''|*[!0-9]*) echo "install.sh: PORT must be numeric" >&2; exit 1 ;;
esac
unit_safe "ADVERTISE" "$ADVERTISE" 0
unit_safe "CONTROL" "${CONTROL:-}" 0
unit_safe "LABELS" "${LABELS:-}" 1

# build from source when Go exists, else install the release binary — both
# paths are Makefile targets, so the install logic lives in one place.
# The explicit GO= is required: sudo resets PATH to root's secure_path
# (user-local Go installs like ~/sdk/go/bin are not on it), so a bare
# `sudo make` would resolve GO ?= go against that PATH and fail with
# "make: go: No such file or directory".
if command -v go >/dev/null 2>&1; then
	sudo make GO="$(command -v go)" install worker
else
	echo "install.sh: go not found — installing the release binary (make install-release)"
	sudo make install-release worker
fi

# the worker's config lives in its unit, with the flags baked in: name,
# port, advertise (so the daemon can dial back and place jobs), and any
# placement labels. CONTROL is added only when given explicitly — by
# default the worker discovers the daemon via mDNS.
labels=""
if [ -n "$LABELS" ]; then
	labels=" $LABELS"
fi
control=""
if [ -n "$CONTROL" ]; then
	control=" -control $CONTROL"
fi
sudo tee /etc/systemd/system/sandman-worker.service >/dev/null <<EOF
[Unit]
Description=Sandman execution worker (joins a control plane)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/sandman worker -name $NAME -port $PORT -advertise $ADVERTISE:$PORT$control$labels
# a crashed or OOM-killed worker must come back: without a restart
# policy the control plane's host TTL silently drops it from placement
Restart=on-failure
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload

echo "install.sh: wrote /etc/systemd/system/sandman-worker.service (name=$NAME advertise=$ADVERTISE:$PORT control=${CONTROL:-<mDNS discovery>})"
if [ -n "$ADVERTISE" ]; then
	echo "install.sh: WARNING: -advertise $ADVERTISE:$PORT binds the worker's unauthenticated exec endpoint on all interfaces — any LAN host can submit jobs to this worker. Leave ADVERTISE empty (single-host install) to keep the loopback bind."
fi

sudo systemctl enable --now sandman-worker
echo "install.sh: worker $NAME is up — it registers with the discovered daemon and appears in the fleet"
echo "install.sh: check with:  sandman nodes   (from the control-plane host)"
