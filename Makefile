GO ?= go
PREFIX ?= /usr/local

.PHONY: build install uninstall clean daemon worker

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
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-s -w" -o sandman .

# install: one binary + the unit for the chosen role. `systemctl enable --now
# sandman` (daemon) or `sandman-worker` (worker) and the node joins the fleet.
install: build
	install -m 0755 sandman $(PREFIX)/bin/sandman
	install -m 0644 deploy/sandman.service /etc/systemd/system/sandman.service
	install -m 0644 deploy/sandman-worker.service /etc/systemd/system/sandman-worker.service
	@if [ "$(ROLE)" = "worker" ]; then \
		if [ ! -f /etc/sandman/worker.env ]; then \
			install -d /etc/sandman; \
			install -m 0644 deploy/worker.env.example /etc/sandman/worker.env; \
			echo "created /etc/sandman/worker.env — edit it for your control plane"; \
		fi; \
	fi
	systemctl daemon-reload || true
	@echo "installed $(PREFIX)/bin/sandman ($(ROLE) role)"
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
	rm -f $(PREFIX)/bin/sandman /etc/systemd/system/sandman.service /etc/systemd/system/sandman-worker.service
	systemctl daemon-reload || true

clean:
	rm -f sandman
