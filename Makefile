GO ?= go
PREFIX ?= /usr/local

.PHONY: build install uninstall clean

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-s -w" -o sandman .

# install: one binary + one unit file. `systemctl enable --now sandman` and
# the node joins the fleet by itself (mDNS) — that is the whole install story.
install: build
	install -m 0755 sandman $(PREFIX)/bin/sandman
	install -m 0644 deploy/sandman.service /etc/systemd/system/sandman.service
	systemctl daemon-reload || true
	@echo "installed $(PREFIX)/bin/sandman"
	@echo "start the node:  systemctl enable --now sandman"

uninstall:
	rm -f $(PREFIX)/bin/sandman /etc/systemd/system/sandman.service
	systemctl daemon-reload || true

clean:
	rm -f sandman
