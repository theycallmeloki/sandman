# GO is overridable: user-local Go installs (e.g. ~/sdk/go/bin/go) are not
# on root's secure_path, so `sudo make install` needs
# `sudo make GO=$HOME/sdk/go/bin/go install`.
GO ?= go

# Two install shapes, picked by the host OS:
#
#   Linux — /usr/local, root, systemd units in /etc/systemd/system.
#   macOS — ~/.local, no root at all: a per-user launchd agent in
#           ~/Library/LaunchAgents, beside the per-user state directory
#           (platform.go) and the per-user Docker Desktop. Nothing on the
#           macOS path needs sudo, which is also what lets `sandman
#           update` replace the binary it is running from unelevated.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
PREFIX ?= $(HOME)/.local
LAUNCH_AGENTS ?= $(HOME)/Library/LaunchAgents
SANDMAN_LOG_DIR ?= $(HOME)/Library/Logs/sandman
else
PREFIX ?= /usr/local
endif

.PHONY: build install install-release uninstall clean daemon worker test-k8s smoke-k8s install-paths

# Role selection: `make install daemon` (default) vs `make install worker`
# picks the post-install enable hint only — do-install always writes both
# deploy/sandman.service (control plane) and deploy/sandman-worker.service
# (execution host), so a node can switch roles without reinstalling.
# The role word is a goal, so MAKECMDGOALS picks it up before `install` runs.
ifneq ($(filter worker,$(MAKECMDGOALS)),)
ROLE := worker
else
ROLE := daemon
endif

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-s -w$(if $(VERSION), -X main.Version=$(VERSION),)" -o sandman .
# test-k8s: unit-test the metacontroller worker-fleet hook (deploy/k8s)
# without a cluster — requires node on PATH.
test-k8s:
	node --test deploy/k8s/test/sync.test.js

# smoke-k8s: cluster-backed check that the worker fleet is up and
# registered (requires kubectl; SANDMAN_ADDR + sandman on PATH for the
# registration check).
smoke-k8s:
	bash deploy/k8s/test/smoke.sh

# install: build from source, then install the binary + the unit for the
# chosen role. `systemctl enable --now sandman` (daemon) or
# `sandman-worker` (worker) and the node joins the fleet.
install: build do-install

# install-release: install the newest published release binary instead of
# building — no Go toolchain needed on the target. The release workflow
# (.github/workflows/release.yml) publishes sandman-<os>-<arch> + .sha256
# per v* tag; this fetches, checksum-verifies, and installs them.
install-release: release-fetch do-install

# release-fetch downloads into a private 0700 mktemp dir: the old fixed
# /tmp/sandman-<os>-<arch> paths were a local-user attack surface — a
# pre-created symlink at the predictable path made root's curl write
# through it (an arbitrary-file overwrite on curl versions that follow
# symlinks; newer curls refuse the write, which breaks the installer
# either way).
release-fetch:
	@test -n "$(VERSION)" || (echo "no release tags yet — use 'make install' (builds from source)" >&2; exit 1)
	@set -e; os=$$(uname -s | tr 'A-Z' 'a-z'); arch=$$(uname -m); \
	case "$$arch" in x86_64|amd64) goarch=amd64;; aarch64|arm64) goarch=arm64;; *) echo "unsupported arch $$arch" >&2; exit 1;; esac; \
	asset="sandman-$$os-$$goarch"; \
	base="https://github.com/theycallmeloki/sandman/releases/download/v$(VERSION)"; \
	tmp=$$(mktemp -d) || exit 1; \
	trap 'rm -rf "$$tmp"' EXIT; \
	curl -fsSL -o "$$tmp/$$asset" "$$base/$$asset"; \
	curl -fsSL -o "$$tmp/$$asset.sha256" "$$base/$$asset.sha256"; \
	(cd "$$tmp" && (sha256sum -c "$$asset.sha256" >/dev/null 2>&1 || shasum -a 256 -c "$$asset.sha256" >/dev/null)); \
	install -m 0755 "$$tmp/$$asset" sandman

# do-install: the actual install (binary + service definition). The role
# word is a goal, so ROLE was picked up at parse time above. Linux writes
# systemd units; macOS writes launchd agents with this node's paths baked
# in (a plist cannot express $HOME, and the macOS install is per-user).
ifeq ($(UNAME_S),Darwin)
do-install:
	install -d $(PREFIX)/bin $(LAUNCH_AGENTS) $(SANDMAN_LOG_DIR)
	install -m 0755 sandman $(PREFIX)/bin/sandman
	sed -e 's|@SANDMAN_BIN@|$(PREFIX)/bin/sandman|g' -e 's|@SANDMAN_LOG_DIR@|$(SANDMAN_LOG_DIR)|g' \
		-e 's|@SANDMAN_HOME@|$(HOME)|g' \
		deploy/sandman.plist > $(LAUNCH_AGENTS)/dev.sandman.daemon.plist
	sed -e 's|@SANDMAN_BIN@|$(PREFIX)/bin/sandman|g' -e 's|@SANDMAN_LOG_DIR@|$(SANDMAN_LOG_DIR)|g' \
		-e 's|@SANDMAN_HOME@|$(HOME)|g' \
		deploy/sandman-worker.plist > $(LAUNCH_AGENTS)/dev.sandman.worker.plist
	@echo "installed $(PREFIX)/bin/sandman ($(ROLE) role)"
	@if [ "$(ROLE)" = "worker" ]; then \
		echo "start the worker:  launchctl bootstrap gui/$$(id -u) $(LAUNCH_AGENTS)/dev.sandman.worker.plist"; \
	else \
		echo "start the node:  launchctl bootstrap gui/$$(id -u) $(LAUNCH_AGENTS)/dev.sandman.daemon.plist"; \
	fi
	@echo "an agent that is already loaded must be re-booted first:  launchctl bootout gui/$$(id -u)/dev.sandman.$(ROLE)"
else
do-install:
	install -m 0755 sandman $(PREFIX)/bin/sandman
	install -m 0644 deploy/sandman.service /etc/systemd/system/sandman.service
	install -m 0644 deploy/sandman-worker.service /etc/systemd/system/sandman-worker.service
	systemctl daemon-reload || true
	@echo "installed $(PREFIX)/bin/sandman ($(ROLE) role)"
	@if [ "$(ROLE)" = "worker" ]; then \
		echo "start the worker:  systemctl enable --now sandman-worker"; \
	else \
		echo "start the node:  systemctl enable --now sandman"; \
	fi
endif

# `make install daemon` / `make install worker`: the role goal is a no-op
# target so the command line parses; ROLE above already saw it.
daemon worker:
	@true

# install-paths prints what the macOS installer bakes into the launchd
# agent (binary, then log directory), so install.sh cannot drift from
# do-install's PREFIX — an agent pointing at a binary nobody installed
# would fail silently at login. It is the darwin half of the install
# surface; on Linux the units are static files and nothing needs it.
install-paths:
	@echo $(PREFIX)/bin/sandman
	@echo $(SANDMAN_LOG_DIR)

ifeq ($(UNAME_S),Darwin)
uninstall:
	-launchctl bootout gui/$$(id -u)/dev.sandman.daemon 2>/dev/null
	-launchctl bootout gui/$$(id -u)/dev.sandman.worker 2>/dev/null
	rm -f $(PREFIX)/bin/sandman $(LAUNCH_AGENTS)/dev.sandman.daemon.plist $(LAUNCH_AGENTS)/dev.sandman.worker.plist
else
uninstall:
	rm -f $(PREFIX)/bin/sandman /etc/systemd/system/sandman.service /etc/systemd/system/sandman-worker.service
	systemctl daemon-reload || true
endif

# VERSION defaults to the highest semver tag (v stripped) — git describe
# picks arbitrarily when several tags share one commit (the 0.0.x re-cut
# line all point at the same revision). A source tarball (install.sh
# fetches the archive, which has no .git) falls back to the latest
# release tag from the GitHub API. Releases themselves are built and
# published by .github/workflows/release.yml on every v* tag push — there
# is no local release target; `sandman update` installs release builds.
VERSION ?= $(shell git tag --sort=-v:refname 2>/dev/null | head -1 | sed 's/^v//')
ifeq ($(VERSION),)
VERSION := $(shell curl -fsSL https://api.github.com/repos/theycallmeloki/sandman/releases/latest 2>/dev/null | grep -m1 '"tag_name"' | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/' | sed 's/^v//')
endif

clean:
	rm -f sandman
