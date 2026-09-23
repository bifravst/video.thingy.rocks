#!/usr/bin/env python3
"""SRTP ingest pipeline host.

Runs one GStreamer pipeline for one SRTP ingest port:

    udpsrc ! srtpdec ! rtpjitterbuffer ! rtph264depay ! h264parse ! capsfilter ! kvssink

It exists as a GStreamer application rather than a `gst-launch-1.0` command line for
three reasons, each of which removes a whole class of problem rather than mitigating it:

* The SRTP master key is delivered through srtpdec's `request-key` callback, in process.
  It never appears in an argument vector (readable through /proc/<pid>/cmdline for the
  pipeline's entire lifetime), an environment variable, or a log line.
* Readiness is the port `udpsrc` actually bound, reported after the state change that
  binds it, instead of a guess made by matching stdout against a regex and sleeping.
* srtpdec reports which packets it authenticated, so the parent can tell traffic that
  holds the key from traffic that merely reached a public UDP port.

The parent speaks to it with one JSON line on stdin (the key material) and reads
line-delimited JSON on stdout. See SrtpHelperProtocol.ts for the consuming side.
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
from gi.repository import GLib, Gst  # noqa: E402

PROTOCOL_VERSION = 1
SUPERVISOR_TICK_MS = 50
#: Half the 16-bit RTP sequence number space: a jump of more than this is read as the
#: short way round in the other direction, per RFC 3711 appendix A.
SEQ_MIDPOINT = 1 << 15
#: The rollover counter is a uint32, so this is both the highest candidate worth
#: trying and the modulus the counter wraps at.
ROC_MODULO = 1 << 32
ROC_MAX = ROC_MODULO - 1
#: Anything this long and hexadecimal in argv would be key material.
KEY_SHAPED = re.compile(r"^[0-9a-fA-F]{40,}$")
#: GST_DEBUG at this level or above makes GStreamer log element caps, which would
#: include the SRTP key.
MAX_SAFE_GST_DEBUG = 3


def candidates(hint: int, max_offset: int) -> "object":
    """Yields rollover-counter candidates to try, in order of likelihood.

    The hint comes first, because the common case is that nothing wrapped while
    ingestion was down. Zero comes second, because the next most likely explanation
    is a sender that restarted its SRTP session - which used to leave the port stuck
    until an operator edited the database by hand.

    Recovering from that is not the same as endorsing it. A sender that rewinds its
    packet index while keeping its key reuses the keystream, and the operator still
    has to rotate the key; this search exists for the case where *ingestion* was down
    while the sender kept counting. SrtpProducer reports the rewind when it can see
    one - see its reportCounterRewind, and the SRTP section of backend/README.md.

    After that the search walks outwards from the hint in *both* directions, and
    downwards is not optional: a hint one above the actual counter is what a stale
    value left by a longer previous session looks like, and an upwards-only search
    can never reach it.

    `max_offset` is the width of a band, not the end of the search. Each band moves
    another `max_offset` away from the hint and from zero, and the bands keep coming
    until the whole uint32 counter space has been offered - so no reachable value is
    permanently excluded by a fixed offset cycle. Every band re-offers the hint and
    zero first, because they stay the two most likely answers no matter how far the
    search has wandered, and because the reason a band was exhausted is usually
    traffic that never held the key at all.

    The sequence repeats rather than ending, so a stream that only becomes
    decryptable later - a sender that starts long after the search began - is still
    picked up. Nothing is retried while no datagrams are arriving, so repeating costs
    nothing when the port is idle.
    """
    hint = min(max(hint, 0), ROC_MAX)
    width = max(int(max_offset), 1)
    band = 0
    while True:
        first = band * width + 1
        if first > ROC_MAX:
            # Every candidate in the counter space has been offered; start over rather
            # than ending, for the same reason each band repeats the hint.
            band = 0
            continue
        seen: set[int] = set()
        for value in _candidate_band(hint, first, first + width - 1):
            if value in seen:
                continue
            seen.add(value)
            yield value
        band += 1


def _candidate_band(hint: int, first: int, last: int) -> "object":
    """One band of candidates: the hint, zero, then `first`..`last` either side."""
    yield hint
    yield 0
    for k in range(first, last + 1):
        # One step further than the hint: that many rollovers happened while
        # ingestion was down.
        if hint + k <= ROC_MAX:
            yield hint + k
        # One step below the hint: the persisted value outlived the session it was
        # written for, so the stream is *behind* it. This is the branch that makes a
        # hint which is too high recoverable at all.
        if hint - k >= 0:
            yield hint - k
        # One step further from zero: the sender restarted its session and has
        # wrapped that many times since.
        if k <= ROC_MAX:
            yield k


def rollover_of(roc: int, highest_seq: int, seq: int) -> int:
    """The rollover counter a sequence number belongs to.

    RFC 3711 appendix A: a sequence number more than half the sequence space away
    from the highest one seen is the short way round in the other direction, so it
    belongs to the neighbouring rollover rather than this one. This is the same
    decision libsrtp makes about the same packet, which is the point - the counter
    tracked here has to agree with the one the authenticator is using.
    """
    if highest_seq < SEQ_MIDPOINT:
        if seq - highest_seq > SEQ_MIDPOINT:
            return (roc - 1) % ROC_MODULO
        return roc
    if highest_seq - SEQ_MIDPOINT > seq:
        return (roc + 1) % ROC_MODULO
    return roc


def advance_index(roc: int, highest_seq: int, seq: int) -> tuple[int, int]:
    """Folds one authenticated sequence number into the highest packet index seen.

    Returns the rollover counter and highest sequence number after `seq`. A packet
    below the highest index changes neither, which is what makes this safe under
    reordering; comparing consecutive *arrivals* is not. An authenticated arrival
    order of 65534, 0, 65535, 1 counts two rollovers that way, because the late
    pre-wrap packet moves the comparison point back up and makes the next post-wrap
    packet look like another wrap.
    """
    rollover = rollover_of(roc, highest_seq, seq)
    if rollover == roc:
        return roc, max(highest_seq, seq)
    if rollover == (roc + 1) % ROC_MODULO:
        return rollover, seq
    # Below this rollover: a late packet from before the last wrap, which the highest
    # index has already passed.
    return roc, highest_seq


def emit(**fields: object) -> None:
    """Writes one protocol line. Never includes key material."""
    print(json.dumps(fields), flush=True)


def fatal(reason: str, message: str, code: int = 2) -> None:
    emit(t="fatal", reason=reason, message=message)
    sys.exit(code)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--ssrc", type=int, required=True)
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
    parser.add_argument("--search-max-offset", type=int, default=512)
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
    """Reads the init frame, which carries the key, before the pipeline exists.

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
        self.authenticated = 0
        self.access_units = 0
        #: Highest authenticated sequence number, i.e. the s_l of RFC 3711 appendix A.
        #: With roc it forms the highest authenticated packet index, which is what a
        #: wrap has to be judged against - not the previous arrival.
        self.highest_seq: int | None = None
        self.last_auth_ms = 0
        #: Rollover counter of the authenticated stream, tracked from authenticated
        #: packets only - never from the plaintext headers of traffic that failed to
        #: authenticate, which anyone able to reach the port can forge. Seeded from the
        #: candidate libsrtp was given, then advanced by authenticated packets.
        self.roc = 0
        self.roc_changed = False
        #: Datagrams for some other SSRC since the last report. Counted rather than
        #: reported one by one: the SSRC is in the clear, anyone who can reach the port
        #: picks it, and a report per datagram would let them write to the logs at
        #: line rate. See _emit_stats.
        self.foreign_ssrc = 0


class SrtpPipeline:
    def __init__(self, args: argparse.Namespace, init: dict[str, object]) -> None:
        self.args = args
        self.key_hex = str(init["key"])
        self.cipher = str(init.get("cipher") or "aes-128-icm")
        self.auth = str(init.get("auth") or "hmac-sha1-80")
        self.ssrc = int(init.get("ssrc") or args.ssrc)
        hint = init.get("rocHint")
        self.hint = int(hint) if isinstance(hint, (int, float)) else 0
        self.candidates = candidates(self.hint, args.search_max_offset)
        self.candidate = next(self.candidates)
        self.trial_index = 0
        self.trial_started_ms = 0
        self.trial_inputs0 = 0
        self.trial_drops0 = 0
        #: Authenticated count when this trial began. The count is cumulative across
        #: trials, so only the difference says anything about the current candidate -
        #: comparing the total against zero credits one candidate with another's work.
        self.trial_auth0 = 0
        self.counters = Counters()
        self.confirmed = False
        self.loop = GLib.MainLoop()
        self.shutting_down = False
        self.key_requests = 0
        self.pipeline: Gst.Pipeline | None = None
        self.dec: Gst.Element | None = None
        self.src: Gst.Element | None = None

    # -- construction -----------------------------------------------------------

    def _sink_description(self) -> str:
        if self.args.fake_sink:
            return "fakesink sync=false name=sink"
        return (
            f"kvssink name=sink stream-name={self.args.stream_name}"
            f" aws-region={self.args.aws_region}"
            f" storage-size={self.args.kvs_storage_size}"
            f" log-config={self.args.kvs_log_config}"
        )

    def build(self) -> None:
        # The caps on udpsrc carry only the non-secret description of the stream. The
        # key arrives separately, through request-key.
        caps = (
            "application/x-srtp"
            ",media=(string)video"
            f",payload=(int){self.args.payload_type}"
            f",clock-rate=(int){self.args.clock_rate}"
            f",encoding-name=(string){self.args.encoding_name}"
            f",ssrc=(uint){self.ssrc}"
        )
        description = (
            # reuse=false so a relay port already in use fails loudly instead of
            # silently sharing the port with whatever else is bound to it.
            f"udpsrc name=src port=0 address=127.0.0.1 reuse=false caps={caps}"
            " ! srtpdec name=dec"
            f" ! rtpjitterbuffer latency={self.args.jitter_latency_ms}"
            " ! rtph264depay"
            " ! h264parse config-interval=-1"
            " ! capsfilter caps=video/x-h264,stream-format=avc,alignment=au"
            f" ! {self._sink_description()}"
        )
        try:
            self.pipeline = Gst.parse_launch(description)
        except GLib.Error as err:
            fatal("missing-element", f"could not build the pipeline: {err.message}")

        assert self.pipeline is not None
        self.src = self.pipeline.get_by_name("src")
        self.dec = self.pipeline.get_by_name("dec")
        if self.src is None or self.dec is None:
            fatal("missing-element", "pipeline is missing udpsrc or srtpdec")

        self.dec.connect("request-key", self._on_request_key)
        self._attach_probes()
        self._watch_bus()

    def _attach_probes(self) -> None:
        assert self.src is not None and self.dec is not None
        src_pad = self.src.get_static_pad("src")
        if src_pad is not None:
            src_pad.add_probe(Gst.PadProbeType.BUFFER, self._on_input)
        # srtpdec only emits on rtp_src what libsrtp authenticated, which makes this
        # pad - and nothing else - the authentication oracle.
        auth_pad = self.dec.get_static_pad("rtp_src")
        if auth_pad is not None:
            auth_pad.add_probe(Gst.PadProbeType.BUFFER, self._on_authenticated)
        sink = self.pipeline.get_by_name("sink") if self.pipeline else None
        sink_pad = sink.get_static_pad("sink") if sink else None
        if sink_pad is not None:
            sink_pad.add_probe(Gst.PadProbeType.BUFFER, self._on_access_unit)

    def _on_input(self, _pad: Gst.Pad, _info: Gst.PadProbeInfo) -> Gst.PadProbeReturn:
        with self.counters.lock:
            self.counters.inputs += 1
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
        with self.counters.lock:
            c = self.counters
            c.authenticated += 1
            c.last_auth_ms = now_ms()
            if seq is not None:
                if c.highest_seq is None:
                    c.highest_seq = seq
                else:
                    roc, c.highest_seq = advance_index(c.roc, c.highest_seq, seq)
                    if roc != c.roc:
                        c.roc = roc
                        c.roc_changed = True
        return Gst.PadProbeReturn.OK

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
        self.key_requests += 1
        emit(t="searching", candidate=self.candidate, trial=self.key_requests - 1)
        # libsrtp is about to start counting from this candidate, so the tracker has to
        # start from it too: the caps below say roc=candidate, and a request means there
        # is no rollover or replay state left from any earlier attempt.
        with self.counters.lock:
            self.counters.roc = self.candidate
            self.counters.highest_seq = None
            self.counters.roc_changed = False
        # Built as a caps string in process. The key is in this string and nowhere
        # else: not in argv, not in the environment, not in any emitted line.
        return Gst.Caps.from_string(
            "application/x-srtp"
            f",srtp-key=(buffer){self.key_hex}"
            f",srtp-cipher=(string){self.cipher}"
            f",srtp-auth=(string){self.auth}"
            f",srtcp-cipher=(string){self.cipher}"
            f",srtcp-auth=(string){self.auth}"
            f",roc=(uint){self.candidate}"
        )

    # -- bus and signals --------------------------------------------------------

    def _watch_bus(self) -> None:
        assert self.pipeline is not None
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_bus_message)

    def _on_bus_message(self, _bus: Gst.Bus, message: Gst.Message) -> bool:
        if message.type == Gst.MessageType.ERROR:
            # The message only, never the debug string, exactly as for warnings below.
            # The debug text is GStreamer's own and nothing constrains it - the
            # GST_DEBUG guard does not cover it - while this pipeline's key is in the
            # caps it hands srtpdec, and a debug string describing caps would carry it
            # onto this protocol.
            err, _debug = message.parse_error()
            emit(
                t="error",
                element=message.src.get_name() if message.src else "",
                message=err.message,
            )
            self._stop(4)
        elif message.type == Gst.MessageType.WARNING:
            err, _debug = message.parse_warning()
            emit(
                t="warning",
                element=message.src.get_name() if message.src else "",
                message=err.message,
            )
        elif message.type == Gst.MessageType.EOS:
            emit(t="eos")
            self._stop(0)
        return True

    def _install_signal_handlers(self) -> None:
        for sig in (signal.SIGTERM, signal.SIGINT):
            GLib.unix_signal_add(GLib.PRIORITY_DEFAULT, sig, self._on_signal)
        # Losing the parent must not leave an orphan holding the Kinesis stream: if
        # the parent dies without stopping us, stdin reaches EOF and we exit.
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
        # it already has instead of losing the fragment in flight.
        assert self.pipeline is not None
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
        emit(t="warning", element="stdin", message="unexpected input after init")
        return True

    def _stop(self, code: int) -> int:
        if self.pipeline is not None:
            self.pipeline.set_state(Gst.State.NULL)
        self.exit_code = code
        self.loop.quit()
        return code

    # -- readiness and supervision ---------------------------------------------

    def start(self) -> None:
        assert self.pipeline is not None and self.src is not None
        # Binding happens on the way to PAUSED, so a port conflict is known here
        # rather than inferred later from the absence of traffic.
        if self.pipeline.set_state(Gst.State.PAUSED) == Gst.StateChangeReturn.FAILURE:
            fatal("bind-failed", self._drain_bus_error() or "could not reach PAUSED", 3)
        self.pipeline.get_state(5 * Gst.SECOND)

        # udpsrc writes the port the OS actually gave it back into the property, which
        # is what makes an ephemeral relay port possible: no fixed offset to collide.
        relay_port = int(self.src.get_property("port"))
        if relay_port == 0:
            fatal("bind-failed", "udpsrc did not report a bound port", 3)

        if self.pipeline.set_state(Gst.State.PLAYING) == Gst.StateChangeReturn.FAILURE:
            fatal("state-change", self._drain_bus_error() or "could not reach PLAYING")

        self._begin_trial()
        emit(t="ready", v=PROTOCOL_VERSION, relayPort=relay_port, pid=os.getpid())

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
        self.exit_code = 0
        self._install_signal_handlers()
        GLib.timeout_add(SUPERVISOR_TICK_MS, self._tick)
        GLib.timeout_add(self.args.stats_interval_ms, self._emit_stats)
        self.loop.run()
        return self.exit_code

    def _tick(self) -> bool:
        if self.shutting_down:
            return True
        with self.counters.lock:
            authenticated = self.counters.authenticated
            roc = self.counters.roc
            roc_changed = self.counters.roc_changed
            highest_seq = self.counters.highest_seq
            last_auth_ms = self.counters.last_auth_ms
            self.counters.roc_changed = False

        # Only what authenticated under the candidate now being tried. The counter is
        # cumulative, so testing the total against zero lets a packet that
        # authenticated under the *previous* candidate - in the window between this
        # snapshot and the key being removed - confirm the one that replaced it, and
        # the reported roc is then the new candidate, which authenticated nothing.
        authenticated_this_trial = authenticated - self.trial_auth0

        if authenticated_this_trial == 0 and not self.confirmed:
            self._advance_search_if_tried()
            return True

        if authenticated_this_trial > 0 and not self.confirmed:
            self.confirmed = True
            # The counter is only ever reported once libsrtp authenticated traffic
            # under the candidate it was seeded from, which is what makes the value
            # safe to persist. It is read rather than assumed: a stream that wrapped
            # between the first authenticated packet and this tick is already past the
            # candidate.
            emit(
                t="auth",
                status="ok",
                first=True,
                roc=roc,
                seq=highest_seq,
                candidate=self.candidate,
                trials=self.key_requests,
                authenticated=authenticated_this_trial,
            )
        elif self.confirmed and roc_changed:
            emit(t="auth", status="ok", first=False, roc=roc, seq=highest_seq)
        elif (
            self.confirmed
            and last_auth_ms > 0
            and now_ms() - last_auth_ms > self.args.auth_loss_ms
        ):
            drops = self._drop_count()
            emit(t="auth", status="lost", sinceMs=now_ms() - last_auth_ms, drops=drops)
            # A sender that restarted its session invalidates the jitter buffer and
            # the fragment in flight too, so the parent rebuilds rather than trying to
            # recover this pipeline in place.
            self._stop(5)
            return False
        return True

    def _begin_trial(self) -> None:
        """Snapshots the counters this trial will be judged against."""
        with self.counters.lock:
            self.trial_inputs0 = self.counters.inputs
            self.trial_auth0 = self.counters.authenticated
        self.trial_drops0 = self._drop_count()
        self.trial_started_ms = now_ms()

    def _advance_search_if_tried(self) -> None:
        """Steps to the next candidate, but only on evidence it was actually tried.

        Stepping on a timer alone would burn through the candidate list whenever a
        port is simply idle, so the trial ends only when datagrams were rejected
        under this candidate, or when datagrams arrived and the trial timed out.

        The authenticated count is re-read here rather than taken from the caller's
        snapshot: the caller released the lock before calling, and a packet that
        authenticates in that window means this candidate is the right one. Discarding
        it then would throw away the answer just as it arrived, and cost a full search
        to find it again.
        """
        with self.counters.lock:
            inputs = self.counters.inputs
            authenticated = self.counters.authenticated
        if authenticated > self.trial_auth0:
            return
        tried_inputs = inputs - self.trial_inputs0
        tried_drops = self._drop_count() - self.trial_drops0
        timed_out = now_ms() - self.trial_started_ms > self.args.trial_timeout_ms

        if tried_drops < self.args.trial_drops and not (tried_inputs > 0 and timed_out):
            return

        emit(
            t="auth",
            status="fail",
            candidate=self.candidate,
            inputs=tried_inputs,
            drops=tried_drops,
        )
        self.trial_index += 1
        self.candidate = next(self.candidates)
        if self.dec is not None:
            # Resetting the stream is what lets the next candidate be applied without
            # rebuilding anything: the next datagram re-requests the key, and no
            # replay-window or rollover state from the failed attempt survives.
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
                authenticated=c.authenticated,
                aus=c.access_units,
                roc=c.roc if self.confirmed else None,
                drops=self._drop_count(),
            )
            foreign, c.foreign_ssrc = c.foreign_ssrc, 0
        # One line per interval however many there were, so what reaches the logs is
        # bounded by the clock rather than by whoever is sending. The SSRCs themselves
        # are left out: they are the sender's choice, and nothing here needs them.
        if foreign > 0:
            emit(
                t="warning",
                element="srtpdec",
                message=f"ignored {foreign} datagrams for SSRCs other than {self.ssrc}",
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
    pipeline = SrtpPipeline(args, init)
    pipeline.build()
    pipeline.start()
    return pipeline.run()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
