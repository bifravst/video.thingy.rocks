#!/bin/bash
# Installs the GStreamer srtpdec/srtpenc element on this instance.
#
# Amazon Linux 2023 ships gstreamer1-plugins-bad-free without the plain SRTP
# plugin (it has the DTLS-SRTP wrappers, but not the static-key srtpdec the
# ingest helper needs), so the element is built from the matching
# gst-plugins-bad release. It is two C files and links against the system
# libsrtp - the measured cost is about 2.5 minutes, almost all of it dependency
# install, tarball download and meson setup (the compile itself takes a second),
# and it is skipped entirely when the element is already present.
#
# Idempotent: exits 0 without doing anything when srtpdec is already in the
# registry. Callers treat a failure as non-fatal - see cdk/user-data.sh for
# why an SRTP-only bootstrap failure must never stop the unencrypted path.
#
# The GStreamer version is detected from the system so the plugin is always
# built against the matching headers; override with GST_SRTP_VERSION.
set -euo pipefail

if gst-inspect-1.0 srtpdec >/dev/null 2>&1; then
	echo "srtpdec is already installed"
	exit 0
fi

SYSTEM_GST_VERSION="$(
	gst-inspect-1.0 --version 2>/dev/null | head -1 | awk '{print $3}' | cut -d, -f1
)"
if [ -z "$SYSTEM_GST_VERSION" ]; then
	echo "ERROR: could not detect the system GStreamer version" >&2
	exit 1
fi
GST_SRTP_VERSION="${GST_SRTP_VERSION:-$SYSTEM_GST_VERSION}"

yum install -y libsrtp-devel ninja-build python3-pip

# gst-plugins-bad 1.24 requires meson >= 1.1; AL2023 ships 0.63. Meson is
# pure Python, so a current one comes from pip. Pinned below 1.12, which is
# the first release that needs Python >= 3.10 (the instances run 3.9).
INSTALLED_MESON="$(meson --version 2>/dev/null || true)"
if [ -z "$INSTALLED_MESON" ] || [ "$(printf '%s\n1.1\n' "$INSTALLED_MESON" | sort -V | head -1)" != "1.1" ]; then
	python3 -m pip install --no-cache-dir 'meson>=1.5,<1.12'
fi

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT
cd "$WORKDIR"

echo "Building the GStreamer SRTP plugin from gst-plugins-bad $GST_SRTP_VERSION"
curl -fsSL \
	"https://gstreamer.freedesktop.org/src/gst-plugins-bad/gst-plugins-bad-$GST_SRTP_VERSION.tar.xz" \
	-o gpb.tar.xz
tar xJf gpb.tar.xz
cd "gst-plugins-bad-$GST_SRTP_VERSION"

meson setup build -Dauto_features=disabled -Dsrtp=enabled --buildtype=release
ninja -C build ext/srtp/libgstsrtp.so
install -m 0755 build/ext/srtp/libgstsrtp.so /usr/lib64/gstreamer-1.0/

gst-inspect-1.0 srtpdec >/dev/null
echo "srtpdec installed"
