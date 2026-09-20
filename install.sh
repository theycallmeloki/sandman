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
# The worker's service definition is written with these values baked in:
# /etc/systemd/system/sandman-worker.service on Linux (edit and
# `systemctl restart sandman-worker`), a launchd agent in
# ~/Library/LaunchAgents on macOS (edit and `launchctl kickstart -k
# gui/$(id -u)/dev.sandman.worker`).
#
# Optional env: CONTROL=http://host:4242  NAME=worker-1  PORT=4343
#               LABELS="-label gpu -label fast"
set -e

# darwin installs entirely under $HOME (per-user launchd agent, per-user
# state directory, per-user Docker Desktop): no sudo, and the log lives
# beside the agent rather than in a journal.
DARWIN=0
if [ "$(uname -s)" = Darwin ]; then
	DARWIN=1
fi

need() {
	command -v "$1" >/dev/null 2>&1 && return 0
	if [ "$DARWIN" = 1 ]; then
		echo "install.sh: $1 not found — install the Xcode command line tools (xcode-select --install) and retry" >&2
		exit 1
	fi
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
#
# macOS has neither `ip` nor `hostname -I`: ask the default route which
# interface it uses, then that interface for its address.
lan_ip() {
	if [ "$DARWIN" = 1 ]; then
		iface=$(route -n get default 2>/dev/null | awk '/interface:/{print $2}')
		# a tunnel is not LAN-reachable: a full-tunnel VPN routes the
		# default through utunN, and advertising the tunnel address would
		# give the control plane an address it cannot dial back
		case "$iface" in
			utun*|tun*|tap*|ppp*|ipsec*|gpd*|ipsec*) iface="" ;;
		esac
		if [ -n "$iface" ]; then
			ip=$(ipconfig getifaddr "$iface" 2>/dev/null)
			if [ -z "$ip" ]; then
				# ipconfig reports the DHCP-managed address only: an
				# interface with a manual address has one and no lease
				ip=$(ifconfig "$iface" 2>/dev/null | awk '/inet /{print $2; exit}')
			fi
			[ -n "$ip" ] && printf '%s' "$ip" && return 0
		fi
		# no usable default route: ask the physical interfaces in order
		for i in $(ifconfig -l 2>/dev/null); do
			case "$i" in en*) ;; *) continue ;; esac
			ip=$(ipconfig getifaddr "$i" 2>/dev/null)
			[ -n "$ip" ] || ip=$(ifconfig "$i" 2>/dev/null | awk '/inet /{print $2; exit}')
			[ -n "$ip" ] || continue
			case "$ip" in 169.254.*|127.*|0.0.0.0) continue ;; esac
			printf '%s' "$ip"
			return 0
		done
		return 1
	fi
	ip route get 1.1.1.1 2>/dev/null | sed -n 's/.*src \([0-9.]*\).*/\1/p'
}
if [ -z "${ADVERTISE:-}" ]; then
	ADVERTISE=$(lan_ip || true)
fi
if [ -z "$ADVERTISE" ] && [ "$DARWIN" = 0 ]; then
	ADVERTISE=$(hostname -I 2>/dev/null | awk '{print $1}')
fi
if [ -z "$ADVERTISE" ]; then
	echo "install.sh: cannot determine this host's LAN address — set ADVERTISE=<address> to place jobs here, or ADVERTISE=127.0.0.1 for a loopback-only install (this machine runs jobs, but no remote control plane can place work on it)" >&2
	exit 1
fi

# unit_safe rejects values that would corrupt the root-written systemd
# unit below: a newline injects a fresh directive, % triggers systemd's
# specifier expansion, and quotes/whitespace misparse ExecStart's argv.
# NAME additionally gets the daemon's default dot-to-dash rule (main.go
# sanitizeName), so a DHCP-style "host.local" stays a single argv word.
unit_safe() { # $1 = field label, $2 = value, $3 = allow-space (1)
	# A newline or CR in a value injects a fresh directive into the systemd
	# unit written below (and a fresh element into a launchd plist's XML).
	# $'\n' spelled those inline, which is bash-only: under dash — the /bin/sh
	# of every Debian-family host — it is a literal four-character pattern
	# that never matches, so the guard was dead exactly where the unit is
	# written. Build the patterns portably instead.
	nl=$(printf '\n_')
	nl=${nl%_}
	cr=$(printf '\r_')
	cr=${cr%_}
	case "$2" in
		*"$nl"*|*"$cr"*|*'%'*|*'"'*)
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
# The explicit GO= is required on Linux: sudo resets PATH to root's
# secure_path (user-local Go installs like ~/sdk/go/bin are not on it), so
# a bare `sudo make` would resolve GO ?= go against that PATH and fail with
# "make: go: No such file or directory". macOS installs under $HOME (the
# Makefile defaults PREFIX to ~/.local there) and needs no sudo at all.
SUDO=sudo
if [ "$DARWIN" = 1 ]; then
	SUDO=""
fi
if command -v go >/dev/null 2>&1; then
	$SUDO make GO="$(command -v go)" install worker
else
	echo "install.sh: go not found — installing the release binary (make install-release)"
	$SUDO make install-release worker
fi

# the worker's config lives in its service definition, with the flags baked
# in: name, port, advertise (so the daemon can dial back and place jobs),
# and any placement labels. CONTROL is added only when given explicitly —
# by default the worker discovers the daemon via mDNS.
labels=""
if [ -n "$LABELS" ]; then
	labels=" $LABELS"
fi
control=""
if [ -n "$CONTROL" ]; then
	control=" -control $CONTROL"
fi

if [ "$DARWIN" = 1 ]; then
	# macOS: the same agent the Makefile just wrote, with this node's
	# flags in ProgramArguments. launchd reads a plist as XML, so the
	# operator-supplied values are entity-escaped here — the systemd
	# heredoc below needs the opposite treatment (unit_safe rejects what
	# would inject a directive).
	xml_escape() {
		printf '%s' "$1" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'
	}
	# the paths the Makefile installed to: asking it beats re-deriving
	# PREFIX here, where the two could drift and leave the agent pointing
	# at a binary nobody installed.
	paths=$(make -s install-paths)
	bin=$(printf '%s\n' "$paths" | sed -n 1p)
	log_dir=$(printf '%s\n' "$paths" | sed -n 2p)
	[ -n "$bin" ] && [ -n "$log_dir" ] || { echo "install.sh: make install-paths returned no paths" >&2; exit 1; }
	agent="$HOME/Library/LaunchAgents/dev.sandman.worker.plist"
	mkdir -p "$HOME/Library/LaunchAgents" "$log_dir"
	# the placement flags, one <string> element per argv word — the same
	# whitespace split ExecStart gets on Linux.
	flag_args=""
	for w in $control $labels; do
		flag_args="$flag_args
		<string>$(xml_escape "$w")</string>"
	done
	cat > "$agent" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>dev.sandman.worker</string>

	<key>ProgramArguments</key>
	<array>
		<string>$(xml_escape "$bin")</string>
		<string>worker</string>
		<string>-name</string>
		<string>$(xml_escape "$NAME")</string>
		<string>-port</string>
		<string>$(xml_escape "$PORT")</string>
		<string>-advertise</string>
		<string>$(xml_escape "$ADVERTISE:$PORT")</string>$flag_args
	</array>

	<!-- see deploy/sandman.plist: launchd's PATH cannot see docker under
	     Docker Desktop ($HOME/.docker/bin, where it installs its CLI and
	     which it advertises only from ~/.zprofile, which launchd never
	     reads) or Homebrew (/opt/homebrew/bin) without this. -->
	<key>EnvironmentVariables</key>
	<dict>
		<key>PATH</key>
		<string>/opt/homebrew/bin:/usr/local/bin:$(xml_escape "$HOME")/.docker/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
	</dict>

	<key>RunAtLoad</key>
	<true/>

	<!-- Restart=on-failure: a crashed or OOM-killed worker must come
	     back, or the control plane's host TTL drops it from placement. -->
	<key>KeepAlive</key>
	<dict>
		<key>SuccessfulExit</key>
		<false/>
	</dict>
	<key>ThrottleInterval</key>
	<integer>2</integer>

	<key>StandardOutPath</key>
	<string>$(xml_escape "$log_dir")/worker.log</string>
	<key>StandardErrorPath</key>
	<string>$(xml_escape "$log_dir")/worker.log</string>
</dict>
</plist>
EOF

	# bootout first: bootstrap refuses an already-loaded label, which is
	# exactly the re-run (upgrade) case.
	launchctl bootout "gui/$(id -u)/dev.sandman.worker" 2>/dev/null || true
	launchctl bootstrap "gui/$(id -u)" "$agent"

	echo "install.sh: wrote $agent (name=$NAME advertise=$ADVERTISE:$PORT control=${CONTROL:-<mDNS discovery>})"
	if [ -n "$ADVERTISE" ]; then
		echo "install.sh: WARNING: -advertise $ADVERTISE:$PORT binds the worker's unauthenticated exec endpoint on all interfaces — any LAN host can submit jobs to this worker. Leave ADVERTISE empty (single-host install) to keep the loopback bind."
	fi

	echo "install.sh: worker $NAME is up — it registers with the discovered daemon and appears in the fleet (log: $log_dir/worker.log)"
	echo "install.sh: check with:  sandman nodes   (from the control-plane host)"
	exit 0
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
