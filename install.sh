#!/bin/sh
# Installs a prebuilt `palimpsest` binary from GitHub Releases.
#
#   curl -fsSL https://raw.githubusercontent.com/ThousandBirdsInc/reactive-palimpsest/main/install.sh | sh
#
# Environment:
#   PALIMPSEST_VERSION   tag to install, e.g. v0.1.1 (default: latest release)
#   PALIMPSEST_INSTALL   directory to install into
#                        (default: /usr/local/bin if writable, else ~/.local/bin)
#   PALIMPSEST_REPO      owner/repo to download from
#   PALIMPSEST_BASE_URL  override the download base URL (internal mirrors);
#                        tarballs and SHA256SUMS are fetched from <base>/<file>
#                        and PALIMPSEST_VERSION must then be set
#
# No Rust toolchain, no package manager: the script picks the tarball
# for this OS and CPU, checks its SHA-256 against the release's
# SHA256SUMS, and copies the binary into place.

set -eu

REPO="${PALIMPSEST_REPO:-ThousandBirdsInc/reactive-palimpsest}"
VERSION="${PALIMPSEST_VERSION:-latest}"
BIN="palimpsest"

say() { printf '%s\n' "$*" >&2; }
die() { say "error: $*"; exit 1; }

need() {
  command -v "$1" >/dev/null 2>&1 || die "'$1' is required but not installed"
}

need uname
need tar

fetch() {
  # fetch <url> <out>
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL --retry 3 -o "$2" "$1"
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O "$2" "$1"
  else
    die "need curl or wget"
  fi
}

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | cut -d' ' -f1
  else
    say "warning: no sha256sum/shasum found; skipping checksum verification"
    echo ""
  fi
}

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
  Linux)  os_part="unknown-linux-musl" ;;
  Darwin) os_part="apple-darwin" ;;
  *)      die "unsupported OS '$os'. Use the container image: ghcr.io/$(printf '%s' "$REPO" | tr '[:upper:]' '[:lower:]')" ;;
esac

case "$arch" in
  x86_64|amd64)   arch_part="x86_64" ;;
  aarch64|arm64)  arch_part="aarch64" ;;
  *)              die "unsupported CPU architecture '$arch'" ;;
esac

target="${arch_part}-${os_part}"

if [ -n "${PALIMPSEST_BASE_URL:-}" ]; then
  [ "$VERSION" != "latest" ] || die "PALIMPSEST_VERSION must be set when PALIMPSEST_BASE_URL is used"
  base="${PALIMPSEST_BASE_URL%/}"
elif [ "$VERSION" = "latest" ]; then
  base="https://github.com/${REPO}/releases/latest/download"
  # Resolve the concrete tag so the tarball name can be built.
  tmp_json="$(mktemp)"
  fetch "https://api.github.com/repos/${REPO}/releases/latest" "$tmp_json" \
    || die "could not query the latest release of ${REPO}"
  VERSION="$(sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' "$tmp_json" | head -n1)"
  rm -f "$tmp_json"
  [ -n "$VERSION" ] || die "could not determine the latest release tag"
else
  base="https://github.com/${REPO}/releases/download/${VERSION}"
fi

version="${VERSION#v}"
name="${BIN}-${version}-${target}"
tarball="${name}.tar.gz"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT INT TERM

say "Downloading ${tarball} (${VERSION})..."
fetch "${base}/${tarball}" "${workdir}/${tarball}" \
  || die "no prebuilt binary for ${target} in release ${VERSION}: ${base}/${tarball}"

if fetch "${base}/SHA256SUMS" "${workdir}/SHA256SUMS" 2>/dev/null; then
  expected="$(grep " ${tarball}\$" "${workdir}/SHA256SUMS" | cut -d' ' -f1 || true)"
  actual="$(sha256_of "${workdir}/${tarball}")"
  if [ -n "$actual" ]; then
    [ -n "$expected" ] || die "${tarball} is not listed in SHA256SUMS"
    [ "$expected" = "$actual" ] || die "checksum mismatch for ${tarball}"
    say "Checksum OK."
  fi
else
  say "warning: SHA256SUMS not found for ${VERSION}; skipping checksum verification"
fi

tar -xzf "${workdir}/${tarball}" -C "${workdir}"
[ -f "${workdir}/${name}/${BIN}" ] || die "tarball did not contain ${name}/${BIN}"

if [ -n "${PALIMPSEST_INSTALL:-}" ]; then
  dest="$PALIMPSEST_INSTALL"
elif [ -w /usr/local/bin ]; then
  dest="/usr/local/bin"
else
  dest="${HOME}/.local/bin"
fi
mkdir -p "$dest"
install -m 0755 "${workdir}/${name}/${BIN}" "${dest}/${BIN}" 2>/dev/null \
  || { cp "${workdir}/${name}/${BIN}" "${dest}/${BIN}" && chmod 0755 "${dest}/${BIN}"; }

say "Installed ${dest}/${BIN} (${VERSION}, ${target})."
case ":${PATH}:" in
  *":${dest}:"*) ;;
  *) say "note: ${dest} is not on your PATH; add it, e.g.  export PATH=\"${dest}:\$PATH\"" ;;
esac
say "Next: copy palimpsest.example.toml from the tarball to palimpsest.toml, edit it, then run:  ${BIN} validate-config palimpsest.toml && ${BIN} serve palimpsest.toml"
