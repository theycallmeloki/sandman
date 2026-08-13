#!/bin/sh
# sandman worker bootstrap: fetch the repo and delegate the install to the
# Makefile (the single source of install logic — nothing is duplicated
# here). Usage:
#
#   curl -sSL https://raw.githubusercontent.com/theycallmeloki/sandman/main/install.sh | sh
#
# Everything is auto-filled: worker name (hostname), exec port (4343),
# advertise address (the host's LAN IP, so the daemon can dial back), and
# the control plane — the worker discovers the daemon itself via mDNS
# when CONTROL is unset (the daemon advertises _sandman._tcp with
# role=daemon; the fleet expects exactly one daemon per LAN).
#
# Optional env: CONTROL=http://host:4242  NAME=worker-1  PORT=4343
#               LABELS="-label gpu"  GO=/path/to/go
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

# fetch the repo — the Makefile does the installing
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
curl -fsSL https://github.com/theycallmeloki/sandman/archive/refs/heads/main.tar.gz | tar -xz -C "$tmp"
cd "$tmp"/sandman-main

NAME=${NAME:-$(hostname)}
PORT=${PORT:-4343}
ADVERTISE=$(hostname -I 2>/dev/null | awk '{print $1}')
if [ -z "$ADVERTISE" ]; then
	echo "install.sh: cannot determine this host's LAN address (hostname -I empty)" >&2
	exit 1
fi

# build from source when Go exists, else install the release binary — both
# paths are Makefile targets, so the install logic lives in one place
if command -v go >/dev/null 2>&1; then
	sudo make install worker
else
	echo "install.sh: go not found — installing the release binary (make install-release)"
	sudo make install-release worker
fi

# the real worker config (the Makefile's example is a placeholder):
# overwrite /etc/sandman/worker.env with the fleet values. An empty
# CONTROL leaves the worker to discover the daemon via mDNS.
sudo tee /etc/sandman/worker.env >/dev/null <<EOF
SANDBOX_WORKER_NAME=$NAME
SANDBOX_CONTROL=$CONTROL
SANDBOX_PORT=$PORT
SANDBOX_ADVERTISE=$ADVERTISE:$PORT
SANDBOX_LABELS=$LABELS
EOF
sudo chmod 0644 /etc/sandman/worker.env
echo "install.sh: wrote /etc/sandman/worker.env (name=$NAME control=${CONTROL:-<mDNS discovery>} advertise=$ADVERTISE:$PORT)"

sudo systemctl enable --now sandman-worker
echo "install.sh: worker $NAME is up — it registers with the discovered daemon and appears in the fleet"
echo "install.sh: check with:  sandman nodes   (from the control-plane host)"
