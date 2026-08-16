#!/bin/sh
# Build a sandman .deb without requiring debhelper or a Go toolchain on
# the target: stage the binary + systemd units into a package tree and run
# dpkg-deb (present on every Debian system). The package declares the
# distro-provided container runtime as its dependency:
#
#     sudo apt install ./sandman_<version>_amd64.deb
#
# pulls in containerd + runc, installs /usr/bin/sandman and the units, and
# needs no docker, no Docker repository, no PPA.
#
# Usage: ./packaging/make-deb.sh [version]   (from the repo root)
#   version defaults to the highest semver git tag (v stripped), like the
#   Makefile. Architecture defaults to amd64; override with ARCH=arm64.

set -e

cd "$(dirname "$0")/.."

VERSION="${1:-$(git tag --sort=-v:refname 2>/dev/null | head -1 | sed 's/^v//')}"
[ -n "$VERSION" ] || { echo "make-deb: no version given and no git tags found" >&2; exit 1; }
ARCH="${ARCH:-amd64}"

echo "make-deb: building sandman $VERSION ($ARCH)"

# the binary: same flags as `make build`, version baked in
CGO_ENABLED=0 go build -trimpath \
  -ldflags "-s -w -X main.Version=$VERSION" \
  -o build/sandman .

# stage the package tree
rm -rf build/deb
PACKAGE_ROOT=build/deb/sandman_${VERSION}_${ARCH}
mkdir -p "$PACKAGE_ROOT/usr/bin"
mkdir -p "$PACKAGE_ROOT/usr/lib/systemd/system"
mkdir -p "$PACKAGE_ROOT/DEBIAN"
install -m 0755 build/sandman "$PACKAGE_ROOT/usr/bin/sandman"

# the packaged units point at /usr/bin/sandman (the deb's path), unlike
# the deploy/ units used by `make install` (/usr/local/bin)
sed 's|/usr/local/bin/sandman|/usr/bin/sandman|g' \
  deploy/sandman.service > "$PACKAGE_ROOT/usr/lib/systemd/system/sandman.service"
sed 's|/usr/local/bin/sandman|/usr/bin/sandman|g' \
  deploy/sandman-worker.service > "$PACKAGE_ROOT/usr/lib/systemd/system/sandman-worker.service"

cat > "$PACKAGE_ROOT/DEBIAN/control" <<EOF
Package: sandman
Version: $VERSION
Section: utils
Priority: optional
Architecture: $ARCH
Depends: containerd, runc
Maintainer: Sandman <theycallmeloki>
Description: naive peer-to-peer container fabric (containerd backend)
 Sandman is a peer-to-peer data pipeline fabric. Pipelines describe a
 transform (image, command, resources), Sandman schedules jobs across the
 fleet and manages the data: inputs, outputs, provenance, logs, secrets.
 Jobs execute in OCI containers provided by containerd + runc; Docker is
 not required and not used.
EOF

# the units are static; enabling them is the operator's choice at install
# time (systemctl enable --now sandman), matching the Makefile install
dpkg-deb --build --root-owner-group "$PACKAGE_ROOT" build/
echo "make-deb: built build/sandman_${VERSION}_${ARCH}.deb"
echo "install with: sudo apt install ./build/sandman_${VERSION}_${ARCH}.deb"
