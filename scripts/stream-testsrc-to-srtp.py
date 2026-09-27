#!/usr/bin/env python3
"""Streams video as SRTP-encrypted RTP/H.264 to an SRTP ingest port.

Usage:
    printf '%s\\n' "$KEY" | ./scripts/stream-testsrc-to-srtp.py <host> <port> [--ssrc N]
    ./scripts/stream-testsrc-to-srtp.py <host> <port> --key-file key.txt [--ssrc N]
    ./scripts/stream-testsrc-to-srtp.py <host> <port> [--ssrc N]    # prompts, no echo
    ./scripts/stream-testsrc-to-srtp.py <host> <port> --source webcam [--device /dev/video0] [--ssrc N]
    ./scripts/stream-testsrc-to-srtp.py --check    # can this machine run it?

<port> is an SRTP ingest port (6000-6009) and --ssrc the SSRC provisioned for it,
3735928559 by default. The key is the 60 hex characters provisioned for the port with
scripts/provision-srtp-key.sh, read from --key-file, from stdin, or from a prompt.

--source webcam captures a local V4L2 device instead of the synthetic test source -
real video data from a real camera, for watching the deployed stack behave under it
(scripts/stream-webcam-to-srtp.sh wraps this mode).

The key is never an argument, to this script or to anything it runs. A process's
argument vector is readable by every local user through /proc/<pid>/cmdline for as
long as it runs, and a sender runs for the whole stream - so this is a GStreamer
application rather than a gst-launch-1.0 command line, and it sets the key on srtpenc
in-process, the way backend/src/srtp_pipeline.py does on the receiving side. The same
two guards the receiver applies are applied here: key-shaped material in argv, and
GStreamer debug settings verbose enough to log it. srtpenc copies the key into the caps
it sends downstream, so caps logging is a real way out for it, not a theoretical one.

Each run is a new SRTP session starting at rollover counter zero, so running this twice
under one key rewinds the packet index and reuses the keystream. Rotate the key between
runs against anything that matters - see the SRTP section of backend/README.md; the
receiver logs the reuse when it can see it.
"""

from __future__ import annotations

import argparse
import getpass
import os
import re
import signal
import sys
from typing import Any, NoReturn

#: Anything this long and hexadecimal in argv would be key material. The receiver's
#: check_environment in backend/src/srtp_pipeline.py applies the same rule.
KEY_SHAPED = re.compile(r"^[0-9a-fA-F]{40,}$")
#: GST_DEBUG above this makes GStreamer log caps, which carry the key; the receiver
#: uses the same threshold.
MAX_SAFE_GST_DEBUG = 3
KEY = re.compile(r"^[0-9a-fA-F]{60}$")
#: The exact canonical form: "06000" would pass a numeric test and then be a port the
#: load balancer does not forward.
PORT = re.compile(r"^600[0-9]$")
#: Canonical decimal only; length is checked separately, because a regex alone would
#: let an out-of-range value through.
SSRC = re.compile(r"^(0|[1-9][0-9]*)$")
UINT32_MAX = 4294967295
DEFAULT_SSRC = "3735928559"
#: How long a clean end of stream gets after Ctrl+C before the pipeline is torn down.
EOS_GRACE_MS = 3000

#: Elements the pipeline needs, and the package each one ships in, per package
#: manager: Ubuntu/Debian's apt, and Arch's pacman. The plugin sets are the
#: same, only the names differ; --check and the failure hints report the
#: machine's detected distro, and the guide documents both.
REQUIRED_ELEMENTS = {
    "videotestsrc": ("gstreamer1.0-plugins-base", "gst-plugins-base"),
    "videoconvert": ("gstreamer1.0-plugins-base", "gst-plugins-base"),
    "x264enc": ("gstreamer1.0-plugins-ugly", "gst-plugins-ugly"),
    "rtph264pay": ("gstreamer1.0-plugins-good", "gst-plugins-good"),
    "srtpenc": ("gstreamer1.0-plugins-bad", "gst-plugins-bad"),
    "udpsink": ("gstreamer1.0-plugins-good", "gst-plugins-good"),
    # Only needed by --source webcam, but it ships in a package the test source
    # needs anyway, so --check simply requires it and the guide's install
    # commands are unchanged.
    "v4l2src": ("gstreamer1.0-plugins-good", "gst-plugins-good"),
}
#: The GStreamer Python bindings per package manager. Arch needs no separate
#: typelib package name: the gstreamer core package ships the typelib, and
#: gst-plugins-base depends on it.
BINDINGS_PACKAGES = (
    ("python3-gi", "gir1.2-gstreamer-1.0"),
    ("python-gobject", "gstreamer"),
)
#: The install command per package manager, as the hints print it.
PACKAGE_MANAGERS = {
    "apt": "sudo apt install",
    "pacman": "sudo pacman -S",
}
#: Index of each package manager's column in the tables above.
APT, PACMAN = 0, 1
DISTRO_INDEX = {"apt": APT, "pacman": PACMAN}


def package_manager(ids: set[str] | None = None) -> str:
    """The package manager this machine's distro uses, from /etc/os-release.

    `ids` overrides the read (for tests). Arch and its derivatives identify as
    arch or archlinux in ID/ID_LIKE; everything else gets apt names, which is
    what the guide documents first.
    """
    if ids is None:
        try:
            with open("/etc/os-release", encoding="utf-8") as handle:
                release = {
                    key: value.strip().strip('"')
                    for key, value in (
                        line.split("=", 1) for line in handle if "=" in line
                    )
                }
        except OSError:
            release = {}
        ids = {
            release.get("ID", "").lower(),
            *release.get("ID_LIKE", "").lower().split(),
        }
    return "pacman" if ids & {"arch", "archlinux"} else "apt"


def required_packages(distro: str = "apt") -> list[str]:
    """Every package the sender needs for a distro, bindings first, each once."""
    i = DISTRO_INDEX[distro]
    return [
        *BINDINGS_PACKAGES[i],
        *dict.fromkeys(pkgs[i] for pkgs in REQUIRED_ELEMENTS.values()),
    ]


def fail(message: str, code: int = 2) -> NoReturn:
    print(f"Error: {message}", file=sys.stderr)
    sys.exit(code)


def check_environment(argv: list[str]) -> None:
    """Refuses to run in a way that would expose the key.

    Before argument parsing, so that a key passed the way the previous shell version of
    this sender took it - as a positional argument - is refused with a reason rather
    than rejected as a usage error. It cannot be taken back out of argv; the point is
    to say so, so the habit does not survive.
    """
    for token in argv:
        for part in token.split("="):
            if KEY_SHAPED.match(part):
                fail(
                    "refusing to run with key-shaped material in the arguments, where "
                    "any local user can read it from /proc; pass the key on stdin or "
                    "with --key-file instead"
                )
    debug = os.environ.get("GST_DEBUG", "")
    levels = [int(m) for m in re.findall(r"(?::|^)(\d+)", debug)]
    if levels and max(levels) > MAX_SAFE_GST_DEBUG:
        fail(
            f"GST_DEBUG={debug} would log caps, and srtpenc puts the SRTP key in its "
            f"caps; use {MAX_SAFE_GST_DEBUG} or lower"
        )
    if os.environ.get("GST_DEBUG_DUMP_DOT_DIR"):
        fail("GST_DEBUG_DUMP_DOT_DIR would write pipeline graphs containing the SRTP key")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream video as SRTP to an SRTP ingest port.",
        epilog="The key is read from --key-file, from stdin, or from a prompt - "
        "never from the command line.",
    )
    parser.add_argument("host", help="ingest endpoint, e.g. the instance IP")
    parser.add_argument("port", help="SRTP ingest port, 6000-6009")
    parser.add_argument("--ssrc", default=DEFAULT_SSRC, help="SSRC provisioned for the port")
    parser.add_argument("--key-file", help="file whose first line is the key")
    parser.add_argument(
        "--source",
        choices=("testsrc", "webcam"),
        default="testsrc",
        help="the synthetic test source, or a local webcam (V4L2)",
    )
    parser.add_argument(
        "--device",
        default="/dev/video0",
        help="the webcam device to capture (with --source webcam)",
    )
    args = parser.parse_args(argv)

    if not PORT.match(args.port):
        fail("port must be one of 6000-6009, in canonical decimal form (no leading zeros)")
    if not SSRC.match(args.ssrc) or len(args.ssrc) > 10 or int(args.ssrc) > UINT32_MAX:
        fail("ssrc must be a canonical decimal uint32 (0-4294967295, no leading zeros)")
    if args.source == "webcam" and not os.path.exists(args.device):
        fail(
            f"no such webcam device: {args.device} (list yours with 'ls /dev/video*', "
            "and pass it with --device)"
        )
    return args


def read_key(key_file: str | None) -> str:
    """The key, from wherever it was offered - but never from argv."""
    if key_file is not None:
        try:
            with open(key_file, encoding="utf-8") as handle:
                key = handle.readline()
        except OSError as err:
            fail(f"cannot read key file: {err.strerror}: {key_file}")
    elif sys.stdin.isatty():
        # Without echo, so the key is not left on screen or in the scrollback.
        key = getpass.getpass("SRTP key (60 hex characters): ")
    else:
        key = sys.stdin.readline()
    key = key.strip()
    if not KEY.match(key):
        fail("the key must be exactly 60 hex characters (30-byte master key+salt)")
    return key


def load_gstreamer() -> Any:
    """The Gst module, initialised - or a failure naming what to install."""
    try:
        import gi

        gi.require_version("Gst", "1.0")
        from gi.repository import Gst
    except (ImportError, ValueError):
        apt_hint = f"{PACKAGE_MANAGERS['apt']} {' '.join(BINDINGS_PACKAGES[APT])}"
        pacman_hint = (
            f"{PACKAGE_MANAGERS['pacman']} {' '.join(BINDINGS_PACKAGES[PACMAN])}"
        )
        fail(
            "the GStreamer Python bindings are not installed "
            f"(Ubuntu/Debian: {apt_hint}; Arch: {pacman_hint})",
            1,
        )
    Gst.init(None)
    return Gst


def missing_elements(gst: Any, distro: str = "apt") -> dict[str, str]:
    """The required elements this machine lacks, each with the package it
    ships in for the given distro."""
    index = DISTRO_INDEX[distro]
    return {
        element: packages[index]
        for element, packages in REQUIRED_ELEMENTS.items()
        if gst.ElementFactory.find(element) is None
    }


def check() -> int:
    """--check: whether this machine can run the sender, and what to install if not."""
    distro = package_manager()
    missing = missing_elements(load_gstreamer(), distro)
    if not missing:
        print("ok")
        return 0
    for element, package in missing.items():
        print(f"missing: {element} (ships in {package})")
    print(
        f"install with: {PACKAGE_MANAGERS[distro]} "
        f"{' '.join(dict.fromkeys(missing.values()))}"
    )
    return 1


def main(argv: list[str]) -> int:
    check_environment(argv)
    if argv == ["--check"]:
        return check()
    args = parse_args(argv)
    key = read_key(args.key_file)

    # Imported only now, so that every refusal above works on a machine without the
    # GStreamer bindings installed.
    Gst = load_gstreamer()
    from gi.repository import GLib

    for element, package in missing_elements(Gst, package_manager()).items():
        fail(f"GStreamer element '{element}' not found (ships in {package})", 1)

    # Built without any of the caller's input. Host, port, SSRC and the webcam
    # device are set as properties below rather than interpolated here, so
    # nothing typed on the command line can add elements to the pipeline - and
    # the key is not in this string at all. --source is a fixed choice, which
    # is what lets it select the string at all.
    source = (
        "videotestsrc is-live=true"
        if args.source == "testsrc"
        else "v4l2src name=cam"
    )
    pipeline = Gst.parse_launch(
        f"{source} ! videoconvert "
        # key-int-max=30 bounds the GOP to about a second, so the receiver
        # resumes at the next keyframe after any gap - the same shape the
        # committed e2e fixture has, for the same reason.
        "! x264enc tune=zerolatency speed-preset=ultrafast key-int-max=30 "
        "! rtph264pay name=pay config-interval=1 pt=96 "
        "! srtpenc name=enc ! udpsink name=sink"
    )
    pipeline.get_by_name("pay").set_property("ssrc", int(args.ssrc))
    if args.source == "webcam":
        pipeline.get_by_name("cam").set_property("device", args.device)
    sink = pipeline.get_by_name("sink")
    sink.set_property("host", args.host)
    sink.set_property("port", int(args.port))

    # The key goes here and nowhere else: straight onto the element, in process.
    enc = pipeline.get_by_name("enc")
    enc.set_property("key", Gst.Buffer.new_wrapped(bytes.fromhex(key)))
    for prop in ("rtp-cipher", "rtcp-cipher"):
        Gst.util_set_object_arg(enc, prop, "aes-128-icm")
    for prop in ("rtp-auth", "rtcp-auth"):
        Gst.util_set_object_arg(enc, prop, "hmac-sha1-80")

    loop = GLib.MainLoop()
    exit_code = 0
    ending = False

    def on_message(_bus: Gst.Bus, message: Gst.Message) -> None:
        nonlocal exit_code
        if message.type == Gst.MessageType.EOS:
            loop.quit()
        elif message.type == Gst.MessageType.ERROR:
            err, _debug = message.parse_error()
            # The error text only, not the debug string: that one can name caps.
            print(f"Error: {err.message}", file=sys.stderr)
            exit_code = 1
            loop.quit()

    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", on_message)

    def give_up() -> bool:
        loop.quit()
        return GLib.SOURCE_REMOVE

    def end_stream(signum: int) -> bool:
        # The first signal ends the stream cleanly, the way gst-launch-1.0 -e did; a
        # second one, or a stream that will not end in time, stops at once. Ctrl+C is
        # how a sender is meant to be stopped, so it exits 0, as gst-launch-1.0 did.
        nonlocal ending, exit_code
        if ending:
            return give_up()
        ending = True
        exit_code = 0 if signum == signal.SIGINT else 128 + signum
        pipeline.send_event(Gst.Event.new_eos())
        GLib.timeout_add(EOS_GRACE_MS, give_up)
        return GLib.SOURCE_CONTINUE

    for signum in (signal.SIGINT, signal.SIGTERM):
        GLib.unix_signal_add(GLib.PRIORITY_HIGH, signum, end_stream, signum)

    video = (
        "640x480 @ 30fps (videotestsrc)"
        if args.source == "testsrc"
        else f"webcam ({args.device})"
    )
    print(
        "\nStreaming configuration:\n"
        f"  Target: {args.host}:{args.port}\n"
        f"  Video:  {video}\n"
        "  Codec:  H.264 over RTP (RFC 6184), SRTP-encrypted\n"
        f"  SSRC:   {args.ssrc}\n"
        "\nPress Ctrl+C to stop streaming\n",
        flush=True,
    )

    if pipeline.set_state(Gst.State.PLAYING) == Gst.StateChangeReturn.FAILURE:
        fail("the pipeline could not start", 1)
    try:
        loop.run()
    finally:
        pipeline.set_state(Gst.State.NULL)
    return exit_code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
