GO ?= go
PREFIX ?= /usr/local

.PHONY: build install uninstall clean daemon worker release

# Role selection: `make install daemon` (default) installs the control-plane
# unit; `make install worker` installs the execution-host unit + a config
# template at /etc/sandman/worker.env (never overwrites an existing one).
# The role word is a goal, so MAKECMDGOALS picks it up before `install` runs.
ifneq ($(filter worker,$(MAKECMDGOALS)),)
ROLE := worker
else
ROLE := daemon
endif

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-s -w$(if $(VERSION), -X main.Version=$(VERSION),)" -o sandman .

# install: one binary + the unit for the chosen role. `systemctl enable --now
# sandman` (daemon) or `sandman-worker` (worker) and the node joins the fleet.
install: build
	install -m 0755 sandman $(PREFIX)/bin/sandman
	install -m 0644 deploy/sandman.service /etc/systemd/system/sandman.service
	install -m 0644 deploy/sandman-worker.service /etc/systemd/system/sandman-worker.service
	install -m 0644 deploy/sandman-update.service /etc/systemd/system/sandman-update.service
	install -m 0644 deploy/sandman-update.timer /etc/systemd/system/sandman-update.timer
	@if [ "$(ROLE)" = "worker" ]; then \
		if [ ! -f /etc/sandman/worker.env ]; then \
			install -d /etc/sandman; \
			install -m 0644 deploy/worker.env.example /etc/sandman/worker.env; \
			echo "created /etc/sandman/worker.env — edit it for your control plane"; \
		fi; \
	fi
	systemctl daemon-reload || true
	# auto-roll: the root oneshot installs the latest release over
	# /usr/local/bin/sandman daily; disable with `systemctl disable
	# sandman-update.timer`. The update never restarts the daemon — the
	# new binary applies at the next natural restart.
	systemctl enable sandman-update.timer || true
	@echo "installed $(PREFIX)/bin/sandman ($(ROLE) role)"
	@echo "auto-updates: enabled (sandman-update.timer, daily) — disable: systemctl disable sandman-update.timer"
	@if [ "$(ROLE)" = "worker" ]; then \
		echo "start the worker:  systemctl enable --now sandman-worker"; \
	else \
		echo "start the node:  systemctl enable --now sandman"; \
	fi

# `make install daemon` / `make install worker`: the role goal is a no-op
# target so the command line parses; ROLE above already saw it.
daemon worker:
	@true

uninstall:
	rm -f $(PREFIX)/bin/sandman /etc/systemd/system/sandman.service /etc/systemd/system/sandman-worker.service /etc/systemd/system/sandman-update.service /etc/systemd/system/sandman-update.timer
	systemctl daemon-reload || true

# release: build the tagged version's release binary + sha256 asset.
# VERSION defaults to the newest git tag (v stripped). Publishing (after
# `git tag v$(VERSION) && git push origin v$(VERSION)`):
#   gh release create v$(VERSION) sandman-linux-amd64 sandman-linux-amd64.sha256 --notes "..."
# VERSION defaults to the highest semver tag (v stripped) — git describe
# picks arbitrarily when several tags share one commit (the 0.0.x re-cut
# line all point at the same revision).
VERSION ?= $(shell git tag --sort=-v:refname 2>/dev/null | head -1 | sed 's/^v//')

release:
	@test -n "$(VERSION)" || (echo "no git tags yet — set VERSION=x.y.z" >&2; exit 1)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags "-s -w -X main.Version=$(VERSION)" -o sandman-linux-amd64 .
	sha256sum sandman-linux-amd64 > sandman-linux-amd64.sha256
	@echo "built sandman-linux-amd64 ($(VERSION)) + checksum"
	@echo "publish:  gh release create v$(VERSION) sandman-linux-amd64 sandman-linux-amd64.sha256 --notes ..."

clean:
	rm -f sandman sandman-linux-amd64 sandman-linux-amd64.sha256
