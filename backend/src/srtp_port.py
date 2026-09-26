#!/usr/bin/env python3
"""SRTP ingest port helper.

One process owns one SRTP ingest port for the whole of its life. It binds the
public UDP port itself - the supervisor never relays a datagram - and hands
that same kernel socket to every GStreamer pipeline it builds, so rebuilding a
pipeline never unbinds the port or loses the datagrams the kernel already
buffered during the rebuild.

It runs in two modes over that one socket:

* Searching (the default, and the resting state):

      udpsrc ! srtpdec ! fakesink

  srtpdec is seeded with rollover-counter candidates one at a time, and the
  pad that only emits what libsrtp authenticated is the sole oracle that
  decides between them. Nothing but an authenticated packet can produce a
  report, so traffic that does not hold the key - which anyone can send to a
  public port - cannot make this process say anything but "still searching".

* Producing (granted by the supervisor once it has acquired the port's Kinesis
  lock, retracted with `stop` when it must give it up):

      udpsrc ! srtpdec ! rtpjitterbuffer ! rtph264depay ! h264parse ! capsfilter ! kvssink

  The same search machinery runs on the producing pipeline too: a stream that
  wrapped between the grant and the rebuild is found by it, the same way it
  would have been found from cold.

It exists as a GStreamer application rather than a `gst-launch-1.0` command
line because the master key has to reach srtpdec through its `request-key`
callback, in process: never in an argument vector (readable through
/proc/<pid>/cmdline for the pipeline's entire lifetime), an environment
variable, or a log line.

Authenticated is not the same as fresh. libsrtp's replay window lives in this
process and starts empty, so on its own it would accept a recording of earlier
traffic again after every restart. The supervisor therefore hands over a
floor - the highest packet index ever accepted under this key and SSRC - and
nothing at or below it gets through; see SrtpPort._record_authenticated.

The supervisor speaks to it over stdin and stdout: one init line in (which
carries the key), `start` and `stop` commands in, line-delimited JSON frames
out. See SrtpHelperProtocol.ts for the consuming side.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
import threading

import gi

gi.require_version("Gst", "1.0")
gi.require_version("Gio", "2.0")
from gi.repository import Gio, GLib, Gst  # noqa: E402

PROTOCOL_VERSION = 3
SUPERVISOR_TICK_MS = 50
#: How often the highest accepted packet index is reported while it moves; see _tick.
INDEX_REPORT_MS = 1000
#: Half the 16-bit RTP sequence number space: a jump of more than this is read as the
#: short way round in the other direction, per RFC 3711 appendix A.
SEQ_MIDPOINT = 1 << 15
#: The 16-bit sequence number space.
SEQ_MAX = 0xFFFF
#: The rollover counter is a uint32, so this is both the highest candidate worth
#: trying and the modulus the counter wraps at.
ROC_MODULO = 1 << 32
ROC_MAX = ROC_MODULO - 1
#: A packet index is the rollover counter and the sequence number side by side.
INDEX_MAX = (ROC_MODULO << 16) - 1
#: Anything this long and hexadecimal in argv would be key material.
KEY_SHAPED = re.compile(r"^[0-9a-fA-F]{40,}$")
#: GST_DEBUG at this level or above makes GStreamer log element caps, which would
#: include the SRTP key.
MAX_SAFE_GST_DEBUG = 3
#: How many counters just above the floor are re-offered after every floor
#: re-offer, see Search. A burst of traffic that cannot authenticate - a sender
#: holding a stale key, an attacker, or another test run against the same port -
#: advances the climbing search one candidate per few drops, and once the climb has
#: passed the real counter, only this re-sweep reaches it again: the climb ascends
#: without descending, and the floor re-offer on its own repeats only the floor
#: itself. Sixteen covers a sender that wrapped while the receiver was down for
#: about ten hours (one rollover per ~36 minutes at 30 fps) - far beyond any
#: plausible drift, at a cost of sixteen candidates per cycle.
RESWEEP_WINDOW = 16

#: The receive buffer for the port's socket. Its only job is to hold datagrams
#: while a pipeline is being torn down and rebuilt, so a mode switch does not cost
#: the keyframe that arrives during it. The kernel caps this at net.core.rmem_max
#: where the caller lacks CAP_NET_ADMIN, which is fine: any buffer is better than
#: an unbound port.
SOCKET_RCVBUF_BYTES = 8 * 1024 * 1024


class Search:
    """The rollover-counter candidates to try, in order of likelihood.

    `base` is the rollover counter of the floor: the highest packet index already
    accepted under this key and SSRC. Nothing below it is ever offered, because every
    packet it could authenticate would be below the floor and be dropped anyway - and
    that is the point. A stream below the floor is either a recording of traffic this
    port already accepted, or a sender that restarted its packet index under a key it
    had already used, which reuses its keystream. The two look identical from here,
    and neither may be accepted; the second is fixed by provisioning a new key, which
    starts a new floor.

    The floor comes first, because the common case is that nothing wrapped while
    ingestion was down, and it is offered again every `floor_every` candidates: it
    stays the most likely answer however far the search has climbed, and the reason
    the search climbed is usually traffic that never held the key at all.

    Above it, two climbs take turns. The near one starts just above the floor in every
    session, so the counters a sender most plausibly reached are tried early each time.
    The far one starts at `search_from` and carries on where the previous session's
    far climb stopped - `far` is reported with each failed trial so the parent can hand
    it to the next session. A session is short: the supervisor tears a session down
    when nothing has authenticated for its provisional window, and starts a new
    helper. When each helper started the search from scratch, no session got further
    than one window allowed and every later one tried the same candidates again, so a
    counter beyond that was never found. With the far climb carried over, every
    counter above the floor is reached eventually, and the near one keeps the likely
    ones from waiting behind it.

    The far climb wraps round to just above the floor once it passes the top of the
    counter space, so the sequence never ends: a stream that only becomes decryptable
    later is still picked up. Nothing is retried while no datagrams are arriving, so
    this costs nothing when the port is idle.
    """

    def __init__(self, base: int, search_from: int | None, floor_every: int) -> None:
        self.base = min(max(base, 0), ROC_MAX)
        self.floor_every = max(int(floor_every), 1)
        start = self.base + 1 if search_from is None else search_from
        #: Where the far climb starts this session; the near one stops short of it.
        self.far_start = start if self.base < start <= ROC_MAX else self.base + 1
        #: The next far candidate, which is what the parent carries to the next session.
        self.far = self.far_start
        self.near = self.base + 1
        self.since_floor: int | None = None
        self.far_turn = False
        self.resweep = 0

    def __iter__(self) -> "Search":
        return self

    def __next__(self) -> int:
        if self.base == ROC_MAX:
            return self.base
        if self.since_floor is None or self.since_floor >= self.floor_every:
            self.since_floor = 0
            self.resweep = RESWEEP_WINDOW
            return self.base
        # The re-sweep runs before the climbs and does not count towards the
        # floor interval: these are the counters a sender that kept counting
        # through the receiver's downtime has most plausibly reached, and they
        # must be reachable again after traffic that cannot authenticate has
        # walked the climbing search past them.
        if self.resweep > 0:
            self.resweep -= 1
            return min(self.base + (RESWEEP_WINDOW - self.resweep), ROC_MAX)
        self.since_floor += 1
        for _ in range(2):
            self.far_turn = not self.far_turn
            if not self.far_turn:
                if self.near < self.far_start:
                    self.near += 1
                    return self.near - 1
            else:
                candidate = self.far
                self.far = candidate + 1 if candidate < ROC_MAX else self.base + 1
                return candidate
        return self.base


def estimated_index(local_index: int, s: int) -> int:
    """The packet index libsrtp will estimate for sequence number `s`.

    A faithful port of libsrtp's srtp_rdbx_estimate_index and the srtp_index_guess
    it calls, because the tracker has to make the same decision about the same
    packet the authenticator makes, or the floor this tracker feeds will reject
    traffic that authenticated (or accept a rollover the authenticator did not).

    The two halves of libsrtp's rule, both load-bearing:

    * Until the session's highest index has passed the first half of a rollover
      (index <= 32768), NO estimation happens: the sequence number is the index,
      roc 0. libsrtp does this because the estimate below would otherwise read a
      forward jump - a stream starting at a low sequence number and skipping
      ahead - as a late packet from rollover 0xffffffff, which is never what a
      fresh session means. The seeded roc, when it is not zero, is already folded
      into the local index by request-key, so this shortcut only applies while
      the seed itself was zero and nothing has moved far.
    * Past that point, RFC 3711 appendix A's rule with plain integer arithmetic -
      s - local_seq is computed as ints, with no modular wraparound, which is
      what makes a forward jump of more than half the sequence space from a low
      sequence number read as backward, as the RFC intends.
    """
    if local_index <= SEQ_MIDPOINT:
        return s
    local_roc = local_index >> 16
    local_seq = local_index & SEQ_MAX
    if local_seq < SEQ_MIDPOINT:
        if s - local_seq > SEQ_MIDPOINT:
            return (((local_roc - 1) % ROC_MODULO) << 16) | s
        return (local_roc << 16) | s
    if local_seq - SEQ_MIDPOINT > s:
        return (((local_roc + 1) % ROC_MODULO) << 16) | s
    return (local_roc << 16) | s


def emit(**fields: object) -> None:
    """Writes one protocol frame. Never includes key material."""
    print(json.dumps(fields), flush=True)


def fatal(reason: str, message: str, code: int = 2) -> None:
    emit(t="fatal", reason=reason, message=message)
    sys.exit(code)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    #: The public UDP port this process owns for its whole life. 0 lets the OS pick
    #: one, which tests use so parallel runs cannot collide; the bound port is
    #: reported in the ready frame either way.
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--payload-type", type=int, default=96)
    parser.add_argument("--clock-rate", type=int, default=90000)
    parser.add_argument("--encoding-name", default="H264")
    parser.add_argument("--jitter-latency-ms", type=int, default=200)
    parser.add_argument("--stream-name", default="")
    parser.add_argument("--aws-region", default="")
    parser.add_argument("--kvs-log-config", default="")
    parser.add_argument("--kvs-storage-size", type=int, default=128)
    parser.add_argument("--stats-interval-ms", type=int, default=5000)
    parser.add_argument("--auth-loss-ms", type=int, default=3000)
    parser.add_argument("--floor-every", type=int, default=512)
    parser.add_argument("--trial-drops", type=int, default=4)
    parser.add_argument("--trial-timeout-ms", type=int, default=1500)
    #: Test only. Replaces kvssink, so the helper can run where kvssink is not built.
    parser.add_argument("--fake-sink", action="store_true")
    return parser.parse_args(argv)


def check_environment(argv: list[str]) -> None:
    """Fails closed if this process could leak the key.

    Cheap, permanent regression guards for the two ways the key used to escape: the
    command line, and GStreamer's own debug logging of caps.
    """
    for token in argv:
        for part in token.split("="):
            if KEY_SHAPED.match(part):
                fatal("key-in-argv", "refusing to run with key-shaped material in argv")
    debug = os.environ.get("GST_DEBUG", "")
    levels = [int(m) for m in re.findall(r"(?::|^)(\d+)", debug)]
    if levels and max(levels) > MAX_SAFE_GST_DEBUG:
        fatal(
            "unsafe-debug-env",
            f"GST_DEBUG={debug} would log caps containing the SRTP key",
        )
    if os.environ.get("GST_DEBUG_DUMP_DOT_DIR"):
        fatal(
            "unsafe-debug-env",
            "GST_DEBUG_DUMP_DOT_DIR would write pipeline graphs containing the SRTP key",
        )


def read_init() -> dict[str, object]:
    """Reads the init frame, which carries the key, before any pipeline exists.

    Read synchronously so no datagram can reach srtpdec before a key is available.
    """
    line = sys.stdin.readline()
    if not line:
        fatal("bad-init", "stdin closed before the init frame arrived")
    try:
        init = json.loads(line)
    except ValueError as err:
        fatal("bad-init", f"init frame is not valid JSON: {err}")
    if not isinstance(init, dict) or init.get("type") != "init":
        fatal("bad-init", "first stdin line must be an init frame")
    if init.get("v") != PROTOCOL_VERSION:
        fatal("bad-init", f"unsupported init version {init.get('v')!r}")
    key = init.get("key")
    if not isinstance(key, str) or not re.fullmatch(r"[0-9a-fA-F]{60}", key):
        fatal("bad-init", "key must be 60 hexadecimal characters")
    return init


class Counters:
    """Shared between GStreamer's streaming threads and the supervisor tick."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.inputs = 0
        #: Bytes of UDP payload that reached the socket, cumulative across every
        #: pipeline this process builds. The supervisor publishes it as the
        #: transport's traffic metric: it is the one number that answers "is anything
        #: arriving", which the zero-ingestion alarms pair with "is it reaching
        #: Kinesis".
        self.input_bytes = 0
        self.authenticated = 0
        #: Access units that reached the producing pipeline's sink - counted only
        #: there, after the depayloader and the parser, so the number cannot be
        #: satisfied by authenticated RTP packets alone.
        self.access_units = 0
        #: Highest authenticated sequence number, i.e. the s_l of RFC 3711 appendix A.
        #: With roc it forms the highest authenticated packet index, which is what a
        #: wrap has to be judged against - not the previous arrival. It follows
        #: everything libsrtp authenticated, stale packets included, because it has to
        #: agree with libsrtp's own estimate of the next packet's index.
        self.highest_seq: int | None = None
        #: Highest packet index accepted - authenticated and above the floor. This,
        #: not the tracker above, is what the supervisor persists as the next floor.
        self.highest_index: int | None = None
        #: Authenticated datagrams dropped for being at or below the floor, since the
        #: last report. Counted, like foreign_ssrc, so a replay cannot write to the
        #: logs at line rate.
        self.stale = 0
        self.last_auth_ms = 0
        #: Rollover counter of the authenticated stream, tracked from authenticated
        #: packets only - never from the plaintext headers of traffic that failed to
        #: authenticate, which anyone able to reach the port can forge. Seeded from the
        #: candidate libsrtp was given, then advanced by authenticated packets.
        self.roc = 0
        self.roc_changed = False
        #: The candidate srtpdec was last handed in request-key - the key it actually
        #: holds - and how many packets have authenticated under it. Both are written
        #: on the streaming thread, which is where request-key and the authentication
        #: probe run, one packet after another. Crediting authentication here rather
        #: than against a baseline the supervisor snapshots is what makes it exact: a
        #: snapshot taken from the other thread always leaves a gap on one side of
        #: the candidate switch or the other, and each side has been a bug.
        self.key_candidate: int | None = None
        self.key_authenticated = 0
        #: Datagrams for some other SSRC since the last report. Counted rather than
        #: reported one by one: the SSRC is in the clear, anyone who can reach the
        #: port picks it, and a report per datagram would let them write to the logs
        #: at line rate. See _emit_stats.
        self.foreign_ssrc = 0


class SrtpPort:
    """One SRTP port: one socket, one key, one search, two pipeline tails."""

    def __init__(self, args: argparse.Namespace, init: dict[str, object]) -> None:
        self.args = args
        self.key_hex = str(init["key"])
        self.cipher = str(init.get("cipher") or "aes-128-icm")
        self.auth = str(init.get("auth") or "hmac-sha1-80")
        self.ssrc = int(init["ssrc"])
        floor = init.get("floor")
        #: Nothing at or below this packet index is accepted; None when this key and
        #: SSRC have never been accepted anywhere.
        self.floor: int | None = (
            int(floor)
            if isinstance(floor, int) and not isinstance(floor, bool) and 0 <= floor <= INDEX_MAX
            else None
        )
        #: Where the previous session's far climb got to. Kept across this process's
        #: own session resets too, so a restart within the process does not repeat the
        #: climb the way a fresh process would.
        self.carried_search_from: int | None = (
            int(init["searchFrom"])
            if isinstance(init.get("searchFrom"), int)
            and not isinstance(init.get("searchFrom"), bool)
            else None
        )
        self.socket: Gio.Socket | None = None
        self.bound_port = 0
        #: 'searching' or 'producing': which tail is currently built.
        self.mode = "searching"
        self.pipeline: Gst.Pipeline | None = None
        self.dec: Gst.Element | None = None
        self.src: Gst.Element | None = None
        #: Search state; reset by _reset_session.
        self.candidates = Search(0, None, args.floor_every)
        self.candidate = 0
        self.trial_started_ms = 0
        self.trial_inputs0 = 0
        self.trial_drops0 = 0
        self.confirmed = False
        self.counters = Counters()
        self.key_requests = 0
        self.reported_index: int | None = None
        self.index_reported_ms = 0
        self.loop = GLib.MainLoop()
        self.shutting_down = False
        #: A stop command arrived and its stopped frame has not been sent yet.
        self.stopping = False
        self.exit_code = 0

    # -- the socket ------------------------------------------------------------

    def bind_socket(self) -> None:
        """Creates and binds this port's socket, once, for the process's whole life.

        Every pipeline built after this is handed the same socket via udpsrc's
        `socket` property, so a mode switch - or a self-recovery after an error -
        never unbinds the port. The datagrams that arrive while no pipeline is
        reading it sit in the kernel's receive buffer, in order, and are the first
        thing the next pipeline reads. That is the whole reason the previous
        implementation's relay, startup buffer and paced replay do not exist here:
        the kernel was already doing that job.

        An IPv6 socket with IPV6_V6ONLY off, matching the unencrypted listener: the
        NLB's target groups are IPv6 and forward every datagram over IPv6, and
        dual-stack keeps local IPv4 senders working too.
        """
        assert self.socket is None
        socket = Gio.Socket.new(
            Gio.SocketFamily.IPV6, Gio.SocketType.DATAGRAM, Gio.SocketProtocol.UDP
        )
        try:
            # IPV6_V6ONLY = 0 makes the socket dual-stack like saga's udp6 listener.
            socket.set_option(41, 26, 0)
        except Exception:
            pass  # IPv6-only still serves the NLB; only local IPv4 senders care.
        try:
            socket.set_option(1, 8, SOCKET_RCVBUF_BYTES)  # SOL_SOCKET, SO_RCVBUF
        except Exception:
            pass  # Capped at net.core.rmem_max without CAP_NET_ADMIN; any buffer helps.
        address = Gio.InetSocketAddress.new(
            Gio.InetAddress.new_any(Gio.SocketFamily.IPV6), self.args.port
        )
        try:
            # reuse=false: a port already taken must fail loudly here, not silently
            # share it with whatever else is bound.
            socket.bind(address, False)
        except GLib.Error as err:
            fatal(
                "bind-failed",
                f"could not bind UDP port {self.args.port}: {err.message}",
                3,
            )
        self.socket = socket
        bound = socket.get_local_address()
        self.bound_port = bound.get_port()
        if self.bound_port == 0:
            fatal("bind-failed", "the socket did not report a bound port", 3)

    # -- pipelines --------------------------------------------------------------

    def _sink_description(self) -> str:
        if self.args.fake_sink:
            return "fakesink sync=false name=sink"
        return (
            f"kvssink name=sink stream-name={self.args.stream_name}"
            f" aws-region={self.args.aws_region}"
            f" storage-size={self.args.kvs_storage_size}"
            f" log-config={self.args.kvs_log_config}"
        )

    def _build(self, producing: bool) -> None:
        """Builds a pipeline on this port's socket.

        The caps on udpsrc carry only the non-secret description of the stream; the
        key arrives separately, through request-key. `close-socket=false` because
        the socket belongs to this process, not to the pipeline: udpsrc is borrowing
        it, and must leave it open so the next pipeline can borrow it too.
        """
        assert self.socket is not None
        caps = (
            "application/x-srtp"
            ",media=(string)video"
            f",payload=(int){self.args.payload_type}"
            f",clock-rate=(int){self.args.clock_rate}"
            f",encoding-name=(string){self.args.encoding_name}"
            f",ssrc=(uint){self.ssrc}"
        )
        tail = (
            f" ! rtpjitterbuffer latency={self.args.jitter_latency_ms}"
            " ! rtph264depay"
            " ! h264parse config-interval=-1"
            " ! capsfilter caps=video/x-h264,stream-format=avc,alignment=au"
            f" ! {self._sink_description()}"
            if producing
            else " ! fakesink sync=false"
        )
        description = (
            # close-socket=false: the socket belongs to this process, not to the
            # pipeline - udpsrc borrows it and must leave it open for the next one.
            f"udpsrc name=src close-socket=false caps={caps}"
            " ! srtpdec name=dec"
            f"{tail}"
        )
        try:
            pipeline = Gst.parse_launch(description)
        except GLib.Error as err:
            fatal("missing-element", f"could not build the pipeline: {err.message}")
        assert pipeline is not None
        src = pipeline.get_by_name("src")
        dec = pipeline.get_by_name("dec")
        if src is None or dec is None:
            fatal("missing-element", "pipeline is missing udpsrc or srtpdec")
        # The socket is set as a property rather than in the launch string, and it is
        # set before the pipeline is given any state, so nothing can read the port
        # before it is our socket.
        src.set_property("socket", self.socket)
        pipeline.set_name("srtp-port")
        self.pipeline = pipeline
        self.src = src
        self.dec = dec
        self.mode = "producing" if producing else "searching"
        dec.connect("request-key", self._on_request_key)
        self._attach_probes(pipeline)
        self._watch_bus(pipeline)

    def _teardown_pipeline(self) -> None:
        """Sets the current pipeline to NULL without touching the socket."""
        if self.pipeline is not None:
            self.pipeline.set_state(Gst.State.NULL)
        self.pipeline = None
        self.src = None
        self.dec = None

    def _attach_probes(self, pipeline: Gst.Pipeline) -> None:
        assert self.src is not None and self.dec is not None
        src_pad = self.src.get_static_pad("src")
        if src_pad is not None:
            src_pad.add_probe(Gst.PadProbeType.BUFFER, self._on_input)
        # srtpdec only emits on rtp_src what libsrtp authenticated, which makes this
        # pad - and nothing else - the authentication oracle.
        auth_pad = self.dec.get_static_pad("rtp_src")
        if auth_pad is not None:
            auth_pad.add_probe(Gst.PadProbeType.BUFFER, self._on_authenticated)
        # The access-unit probe is attached only where a named "sink" exists:
        # the producing pipeline's, which follows the depayloader and the
        # parser. The searching pipeline's fakesink is deliberately unnamed, so
        # nothing counts srtpdec's output there - an authenticated RTP packet
        # is not an access unit, and the aus stat must never be satisfiable
        # without the producing tail actually having parsed anything.
        sink = pipeline.get_by_name("sink")
        sink_pad = sink.get_static_pad("sink") if sink is not None else None
        if sink_pad is not None and self.mode == "producing":
            sink_pad.add_probe(Gst.PadProbeType.BUFFER, self._on_access_unit)

    def _on_input(self, pad: Gst.Pad, info: Gst.PadProbeInfo) -> Gst.PadProbeReturn:
        buffer = info.get_buffer()
        size = buffer.get_size() if buffer is not None else 0
        with self.counters.lock:
            self.counters.inputs += 1
            self.counters.input_bytes += size
        return Gst.PadProbeReturn.OK

    def _on_authenticated(
        self, _pad: Gst.Pad, info: Gst.PadProbeInfo
    ) -> Gst.PadProbeReturn:
        buffer = info.get_buffer()
        seq: int | None = None
        if buffer is not None:
            # The GstRtp typelib is not guaranteed to be installed, so read the
            # sequence number out of the RTP header directly: bytes 2-3, big endian.
            ok, mapped = buffer.map(Gst.MapFlags.READ)
            if ok:
                try:
                    if mapped.size >= 4:
                        seq = (mapped.data[2] << 8) | mapped.data[3]
                finally:
                    buffer.unmap(mapped)
        if self._record_authenticated(seq):
            return Gst.PadProbeReturn.OK
        return Gst.PadProbeReturn.DROP

    def _record_authenticated(self, seq: int | None) -> bool:
        """Decides whether one packet libsrtp authenticated is accepted.

        Accepted means above the floor. A packet at or below it authenticated only
        because libsrtp's replay window starts empty in every new process, and after
        every remove-key: it is a copy of something this port already accepted, or a
        sender reusing its packet index under this key. It is dropped before the
        depayloader, and it counts for nothing - not as authenticated, not towards
        confirming a candidate, and not as the traffic that keeps the stream alive.
        """
        if seq is None:
            # Too short to carry a sequence number, so its index cannot be shown to be
            # above the floor. libsrtp does not authenticate such a packet in practice.
            return False
        with self.counters.lock:
            c = self.counters
            # The tracker's local index mirrors libsrtp's rdbx index: the seeded
            # candidate from request-key until the first packet, then the highest
            # index of everything libsrtp authenticated - stale packets included,
            # because their indexes advanced libsrtp's window too.
            if c.highest_seq is None:
                local_index = (c.roc << 16) | seq
                c.highest_seq = seq
            else:
                local_index = (c.roc << 16) | c.highest_seq
            index = estimated_index(local_index, seq)
            if index > local_index:
                # Only a forward index advances the window, exactly like
                # srtp_rdbx_add_index; a reordered packet within the window does not,
                # which is what makes this safe under reordering. An authenticated
                # arrival order of 65534, 0, 65535, 1 counts two rollovers otherwise,
                # because the late pre-wrap packet moves the comparison point.
                c.roc = index >> 16
                c.highest_seq = index & SEQ_MAX
                if (index >> 16) != (local_index >> 16):
                    c.roc_changed = True
            if self.floor is not None and index <= self.floor:
                c.stale += 1
                return False
            c.authenticated += 1
            c.key_authenticated += 1
            c.last_auth_ms = now_ms()
            if c.highest_index is None or index > c.highest_index:
                c.highest_index = index
            # The floor rises with what this session accepts, so a duplicate from
            # further back than libsrtp's replay window is refused in-session too.
            if self.floor is None or index > self.floor:
                self.floor = index
            return True

    def _on_access_unit(
        self, _pad: Gst.Pad, _info: Gst.PadProbeInfo
    ) -> Gst.PadProbeReturn:
        with self.counters.lock:
            self.counters.access_units += 1
        return Gst.PadProbeReturn.OK

    def _on_request_key(self, _element: Gst.Element, ssrc: int) -> Gst.Caps | None:
        # A datagram carrying someone else's SSRC gets no key, so it cannot even be
        # attempted - and cannot disturb the stream this port is configured for.
        # srtpdec asks again for every such datagram, so this only counts it; the
        # count is reported with the stats, at most once an interval.
        if ssrc != self.ssrc:
            with self.counters.lock:
                self.counters.foreign_ssrc += 1
            return None
        with self.counters.lock:
            c = self.counters
            if c.key_candidate is not None and c.key_authenticated > 0:
                # The key srtpdec held has authenticated traffic, so it is asking again
                # only because the search stepped on and removed it - after the packet
                # that proved it right was already on its way. That key was the
                # answer: hand it back, with its credit, rather than the candidate the
                # search moved to in the meantime.
                self.candidate = c.key_candidate
            else:
                c.key_candidate = self.candidate
                c.key_authenticated = 0
            candidate = self.candidate
            # libsrtp is about to start counting from this candidate, so the tracker
            # has to start from it too: the caps below say roc=candidate, and a
            # request means there is no rollover or replay state left from any
            # earlier attempt.
            c.roc = candidate
            c.highest_seq = None
            c.roc_changed = False
        self.key_requests += 1
        emit(t="searching", candidate=candidate, trial=self.key_requests - 1)
        # Built as a caps string in process. The key is in this string and nowhere
        # else: not in argv, not in the environment, not in any emitted line.
        return Gst.Caps.from_string(
            "application/x-srtp"
            f",srtp-key=(buffer){self.key_hex}"
            f",srtp-cipher=(string){self.cipher}"
            f",srtp-auth=(string){self.auth}"
            f",srtcp-cipher=(string){self.cipher}"
            f",srtcp-auth=(string){self.auth}"
            f",roc=(uint){candidate}"
        )

    # -- bus and signals --------------------------------------------------------

    def _watch_bus(self, pipeline: Gst.Pipeline) -> None:
        bus = pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_bus_message)

    def _on_bus_message(self, _bus: Gst.Bus, message: Gst.Message) -> bool:
        if message.type == Gst.MessageType.ERROR:
            # The message only, never the debug string. The debug text is GStreamer's
            # own and nothing constrains it - the GST_DEBUG guard does not cover it -
            # while this pipeline's key is in the caps it hands srtpdec, and a debug
            # string describing caps would carry it onto this protocol.
            err, _debug = message.parse_error()
            emit(
                t="error",
                element=message.src.get_name() if message.src else "",
                message=err.message,
            )
            self._on_pipeline_error()
        elif message.type == Gst.MessageType.WARNING:
            err, _debug = message.parse_warning()
            emit(
                t="warning",
                element=message.src.get_name() if message.src else "",
                message=err.message,
            )
        elif message.type == Gst.MessageType.EOS:
            if self.stopping:
                # The flush a stop asked for has finished.
                self._finish_stop()
            else:
                emit(t="eos")
                self._stop(0)
        return True

    def _on_pipeline_error(self) -> None:
        """Recovers from a pipeline error without giving up the socket.

        In producing mode the supervisor still believes this port produces, so the
        producing tail is torn down and reported with a stopped frame - the same
        frame a stop command would have produced - and the supervisor releases the
        port's lock on it. In searching mode there is no producing state to report,
        so the process exits and lets the supervisor restart it: an error in a
        pipeline with nothing but udpsrc, srtpdec and fakesink is an environment
        problem, and retrying it in-process would hide a crash loop.
        """
        if self.mode == "producing":
            self._teardown_pipeline()
            emit(t="stopped", index=self._highest_index())
            self._reset_session()
            self._build_searching()
        else:
            self._stop(4)

    def _install_signal_handlers(self) -> None:
        for sig in (signal.SIGTERM, signal.SIGINT):
            GLib.unix_signal_add(GLib.PRIORITY_DEFAULT, sig, self._on_signal)
        # Losing the parent must not leave an orphan holding a Kinesis stream: if
        # the parent dies without stopping us, stdin reaches EOF and we exit. The
        # commands come on the same fd, so this is also the command reader.
        GLib.unix_fd_add_full(
            GLib.PRIORITY_DEFAULT,
            sys.stdin.fileno(),
            GLib.IOCondition.IN | GLib.IOCondition.HUP | GLib.IOCondition.ERR,
            self._on_stdin,
        )

    def _on_signal(self) -> bool:
        if self.shutting_down:
            return False
        self.shutting_down = True
        # End the stream rather than killing the process, so the sink can flush what
        # it already has instead of losing the fragment in flight. Five seconds to do
        # it in, then whatever state it reached is taken down the hard way.
        if self.pipeline is not None:
            self.pipeline.send_event(Gst.Event.new_eos())
        GLib.timeout_add_seconds(5, lambda: self._stop(0) or False)
        return False

    def _on_stdin(self, _fd: int, condition: GLib.IOCondition) -> bool:
        if condition & (GLib.IOCondition.HUP | GLib.IOCondition.ERR):
            self._stop(0)
            return False
        line = sys.stdin.readline()
        if not line:
            self._stop(0)
            return False
        command = None
        try:
            command = json.loads(line)
        except ValueError:
            pass
        if isinstance(command, dict):
            kind = command.get("type")
            if kind == "start":
                self._cmd_start()
                return True
            if kind == "stop":
                self._cmd_stop()
                return True
        emit(t="warning", element="stdin", message="unexpected input after init")
        return True

    # -- commands ---------------------------------------------------------------

    def _cmd_start(self) -> None:
        """Grants production: rebuild with the producing tail on the same socket.

        The session is reset and re-seeded from the confirmed candidate, so the
        producing pipeline re-confirms it - usually on the first packet, and via the
        same search machinery if the stream wrapped between the grant and now.
        """
        if self.shutting_down or self.stopping:
            return
        if self.mode == "producing":
            return
        seed = self.counters.roc if self.confirmed else None
        self._teardown_pipeline()
        self._reset_session(seed=seed)
        self._build(producing=True)
        if self._set_state(Gst.State.PLAYING):
            emit(t="producing")

    def _cmd_stop(self) -> None:
        """Retracts production: flush, tear down, ack, and return to searching.

        EOS first so kvssink can upload the fragment it is holding, then the
        stopped frame - the supervisor releases the port's lock on it, so it is
        only sent once nothing this process does can reach Kinesis.
        """
        if self.shutting_down or self.stopping:
            return
        if self.mode != "producing":
            # Nothing to retract; still ack, so the supervisor's state machine can
            # rely on the reply existing for every stop it sends.
            emit(t="stopped", index=self._highest_index())
            return
        self.stopping = True
        assert self.pipeline is not None
        self.pipeline.send_event(Gst.Event.new_eos())
        # If the flush never completes, finish anyway: a stop that goes unanswered
        # would hold the port's lock for as long as this process lives.
        GLib.timeout_add_seconds(5, self._finish_stop)

    def _finish_stop(self) -> None:
        if not self.stopping:
            return
        self.stopping = False
        index = self._highest_index()
        self._teardown_pipeline()
        emit(t="stopped", index=index)
        self._reset_session()
        self._build_searching()

    def _build_searching(self) -> None:
        self._build(producing=False)
        self._set_state(Gst.State.PLAYING)

    # -- session state ----------------------------------------------------------

    def _reset_session(self, seed: int | None = None) -> None:
        """Starts a fresh search on top of everything already accepted.

        Called when a pipeline is rebuilt: a mode switch, a stop, or an auth loss.
        The floor has already been raised by everything accepted so far, so the new
        search starts from the floor - never below it - and the far climb resumes
        where it had got to. `seed` is a candidate the caller knows is likely: the
        confirmed counter when production is granted on the heels of a
        confirmation.
        """
        base = 0 if self.floor is None else self.floor >> 16
        self.candidates = Search(base, self.carried_search_from, self.args.floor_every)
        self.candidate = seed if seed is not None else next(self.candidates)
        self.trial_started_ms = now_ms()
        with self.counters.lock:
            self.counters.key_candidate = None
            self.counters.key_authenticated = 0
            self.counters.highest_seq = None
            self.counters.roc_changed = False
        self.confirmed = False

    def _highest_index(self) -> int | None:
        with self.counters.lock:
            return self.counters.highest_index

    # -- readiness and supervision ----------------------------------------------

    def start(self) -> None:
        self.bind_socket()
        self._reset_session()
        self._build_searching()
        emit(t="ready", v=PROTOCOL_VERSION, port=self.bound_port, pid=os.getpid())

    def _set_state(self, state: Gst.State) -> bool:
        assert self.pipeline is not None
        change = self.pipeline.set_state(state)
        if change == Gst.StateChangeReturn.FAILURE:
            fatal(
                "state-change",
                self._drain_bus_error() or f"could not reach {state.value_nick}",
            )
        if state == Gst.State.PAUSED:
            # The state that must complete: it is where a provided socket and every
            # element's setup either works or fails, and a live pipeline reaches
            # PAUSED without waiting for data.
            self.pipeline.get_state(5 * Gst.SECOND)
        # PLAYING is deliberately not waited for: a live pipeline legitimately
        # stays ASYNC there until data flows, and waiting for it to complete
        # would stall every rebuild for the length of the timeout for nothing.
        # Failures arrive on the bus, which is watched.
        return True

    def _drain_bus_error(self) -> str | None:
        assert self.pipeline is not None
        message = self.pipeline.get_bus().timed_pop_filtered(
            Gst.SECOND, Gst.MessageType.ERROR
        )
        if message is None:
            return None
        err, _debug = message.parse_error()
        return err.message

    def run(self) -> int:
        self._install_signal_handlers()
        GLib.timeout_add(SUPERVISOR_TICK_MS, self._tick)
        GLib.timeout_add(self.args.stats_interval_ms, self._emit_stats)
        self.loop.run()
        return self.exit_code

    def _stop(self, code: int) -> int:
        if self.shutting_down and self.pipeline is None:
            return code
        # The last word on the floor, however the helper ends, so the supervisor's
        # next start begins from everything this one accepted rather than from the
        # last periodic report.
        self._report_index(force=True)
        if self.pipeline is not None:
            self.pipeline.set_state(Gst.State.NULL)
        self.exit_code = code
        self.loop.quit()
        return code

    def _tick(self) -> bool:
        if self.shutting_down:
            return True
        with self.counters.lock:
            c = self.counters
            # What authenticated under the key srtpdec holds - credited as it happened,
            # on the streaming thread, rather than inferred from a snapshot of a
            # running total taken here.
            authenticated = c.key_authenticated
            if authenticated > 0 and not self.confirmed and c.key_candidate is not None:
                # That key is the answer whatever the search has stepped to since, so
                # it is the one reported, and the one request-key hands out again.
                self.candidate = c.key_candidate
            roc = c.roc
            roc_changed = c.roc_changed
            highest_seq = c.highest_seq
            last_auth_ms = c.last_auth_ms
            c.roc_changed = False

        now = now_ms()

        # Auth loss comes first, because it applies whether or not this session has
        # confirmed: a grant whose re-confirmation never arrived is producing, and
        # a sender that stopped right after the grant must be reported just the
        # same - otherwise the producing pipeline (kvssink included) would sit
        # there until the port's lease went stale.
        if (
            last_auth_ms > 0
            and now - last_auth_ms > self.args.auth_loss_ms
            and (self.confirmed or self.mode == "producing")
        ):
            # The sender has stopped, or restarted its session in a way that
            # invalidates the jitter buffer and the fragment in flight. Either
            # way the pipeline is rebuilt rather than recovered in place:
            # production is given up, and the next authenticated packet starts a
            # fresh search from the floor.
            #
            # The frames are ordered so nothing the supervisor acts on can
            # arrive before the producing pipeline is down: `stopped` carries
            # the final index and releases the port's lock, and - exactly as in
            # _cmd_stop - it is only sent once nothing this process does can
            # still reach Kinesis. `auth lost` follows it as a report, after the
            # teardown it describes has actually happened; in earlier designs it
            # came first, which released the lock while the producer could still
            # be flushing.
            if self.mode == "producing":
                self._teardown_pipeline()
                emit(t="stopped", index=self._highest_index())
            self._reset_session()
            if self.pipeline is None:
                self._build_searching()
            emit(
                t="auth",
                status="lost",
                sinceMs=now - last_auth_ms,
                drops=self._drop_count(),
            )
            return True

        if authenticated == 0 and not self.confirmed:
            self._advance_search_if_tried()
            return True

        if authenticated > 0 and not self.confirmed:
            self.confirmed = True
            # The counter is only ever reported once libsrtp authenticated traffic
            # under the candidate it was seeded from, which is what makes the value
            # safe to persist. It is read rather than assumed: a stream that wrapped
            # between the first authenticated packet and this tick is already past
            # the candidate.
            emit(
                t="auth",
                status="ok",
                first=True,
                roc=roc,
                seq=highest_seq,
                candidate=self.candidate,
                trials=self.key_requests,
                authenticated=authenticated,
            )
        elif self.confirmed and roc_changed:
            emit(t="auth", status="ok", first=False, roc=roc, seq=highest_seq)
        if self.confirmed:
            self._report_index(force=False)
        return True

    def _report_index(self, force: bool) -> None:
        """Reports the highest accepted packet index, if it moved.

        At most once an interval while it moves, and once more on the way out. The
        supervisor persists it as the floor for the next start, here or on another
        instance.
        """
        index = self._highest_index()
        if index is None or index == self.reported_index:
            return
        if not force and now_ms() - self.index_reported_ms < INDEX_REPORT_MS:
            return
        self.reported_index = index
        self.index_reported_ms = now_ms()
        emit(t="index", index=index)

    def _begin_trial(self) -> None:
        """Snapshots what decides whether this trial was actually tried.

        Only that. Whether a candidate authenticated is not judged from a snapshot at
        all - see Counters.key_candidate - because this runs after the new key is
        live, and a baseline taken here absorbed any packet that authenticated under
        it first. Inputs and drops miscounted by a packet or two only move the moment
        a trial is deemed tried, which is harmless.
        """
        with self.counters.lock:
            self.trial_inputs0 = self.counters.inputs
        self.trial_drops0 = self._drop_count()
        self.trial_started_ms = now_ms()

    def _advance_search_if_tried(self) -> None:
        """Steps to the next candidate, but only on evidence it was actually tried.

        Stepping on a timer alone would burn through the candidate list whenever a
        port is simply idle, so the trial ends only when datagrams were rejected
        under this candidate, or when datagrams arrived and the trial timed out.
        """
        with self.counters.lock:
            inputs = self.counters.inputs
        tried_inputs = inputs - self.trial_inputs0
        tried_drops = self._drop_count() - self.trial_drops0
        timed_out = now_ms() - self.trial_started_ms > self.args.trial_timeout_ms

        if tried_drops < self.args.trial_drops and not (tried_inputs > 0 and timed_out):
            return

        with self.counters.lock:
            # The last look and the switch happen together, under the lock that
            # request-key and the authentication probe take. A packet that
            # authenticated since the caller looked means the key srtpdec holds is the
            # answer, and stepping past it would throw that away.
            if self.counters.key_authenticated > 0:
                return
            failed = self.candidate
            self.candidate = next(self.candidates)
            self.carried_search_from = self.candidates.far

        emit(
            t="auth",
            status="fail",
            candidate=failed,
            inputs=tried_inputs,
            drops=tried_drops,
            searchFrom=self.candidates.far,
        )
        if self.dec is not None:
            # Resetting the stream is what lets the next candidate be applied without
            # rebuilding anything: the next datagram re-requests the key, and no
            # replay-window or rollover state from the failed attempt survives. A
            # packet that authenticates under the old key before this takes effect is
            # still credited to that key, and request-key then hands it back.
            #
            # Not called under the counters lock: srtpdec takes its own lock around
            # this, and its streaming thread takes ours from inside request-key, so
            # holding both here could deadlock.
            self.dec.emit("remove-key", self.ssrc)
        self._begin_trial()

    def _drop_count(self) -> int:
        if self.dec is None:
            return 0
        stats = self.dec.get_property("stats")
        if stats is None:
            return 0
        try:
            return int(stats.get_value("recv-drop-count") or 0)
        except (TypeError, ValueError):
            return 0

    def _emit_stats(self) -> bool:
        if self.shutting_down:
            return True
        with self.counters.lock:
            c = self.counters
            emit(
                t="stats",
                inputs=c.inputs,
                inputBytes=c.input_bytes,
                authenticated=c.authenticated,
                aus=c.access_units,
                roc=c.roc if self.confirmed else None,
                drops=self._drop_count(),
            )
            foreign, c.foreign_ssrc = c.foreign_ssrc, 0
            stale, c.stale = c.stale, 0
        # One line per interval however many there were, so what reaches the logs is
        # bounded by the clock rather than by whoever is sending. The SSRCs themselves
        # are left out: they are the sender's choice, and nothing here needs them.
        if foreign > 0:
            emit(
                t="warning",
                element="srtpdec",
                message=f"ignored {foreign} datagrams for SSRCs other than {self.ssrc}",
            )
        if stale > 0:
            emit(
                t="warning",
                element="srtpdec",
                message=(
                    f"dropped {stale} authenticated datagrams at or below the highest"
                    " packet index already accepted under this key: a replay, or a"
                    " sender that restarted its packet index without a new key"
                ),
            )
        return True


def now_ms() -> int:
    return int(GLib.get_monotonic_time() / 1000)


def main(argv: list[str]) -> int:
    check_environment(argv)
    args = parse_args(argv)
    if not args.fake_sink and args.stream_name == "":
        fatal("bad-init", "--stream-name is required unless --fake-sink is used")
    Gst.init(None)
    init = read_init()
    if "ssrc" not in init:
        fatal("bad-init", "the init frame must carry the port's SSRC")
    port = SrtpPort(args, init)
    port.start()
    return port.run()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
