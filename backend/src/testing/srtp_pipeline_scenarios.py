"""Runs the rollover-counter search through interleavings a real pipeline cannot force.

The search runs on two threads: the supervisor tick decides when to step to the next
candidate, while srtpdec's streaming thread asks for keys and authenticates packets.
Which of them gets to a packet first is timing, so a black-box test against real
libsrtp can only hope to hit a given ordering. Here the helper's own methods are driven
directly, with a stand-in for srtpdec whose `remove-key` runs the streaming thread's
side at exactly the point in question - so each ordering is reproduced every time.

Prints one JSON object: scenario name to outcome. The assertions live in
SrtpPipelineHelper.spec.ts. Nothing printed carries key material: request-key's caps
contain the key, so only the rollover counter is read out of them.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import pathlib
from collections.abc import Callable

HELPER = pathlib.Path(__file__).resolve().parent.parent / "srtp_pipeline.py"
_spec = importlib.util.spec_from_file_location("srtp_pipeline", HELPER)
assert _spec is not None and _spec.loader is not None
sp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sp)
# The helper's main does this; without it request-key cannot build its caps.
sp.Gst.init(None)

KEY = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d"
SSRC = 42


class FakeDecoder:
    """srtpdec, with its streaming thread run at the moment remove-key is called."""

    def __init__(self) -> None:
        self.drops = 0
        self.on_remove_key: Callable[[], None] = lambda: None

    def get_property(self, _name: str) -> FakeDecoder:
        return self

    def get_value(self, _name: str) -> int:
        return self.drops

    def emit(self, signal: str, _ssrc: int) -> None:
        if signal == "remove-key":
            self.on_remove_key()


class Scenario:
    def __init__(self, hint: int) -> None:
        args = sp.parse_args(["--ssrc", str(SSRC), "--fake-sink", "--trial-drops", "4"])
        init = {"key": KEY, "ssrc": SSRC, "rocHint": hint}
        self.pipeline = sp.SrtpPipeline(args, init)
        self.decoder = FakeDecoder()
        self.pipeline.dec = self.decoder
        self.handed_out: list[int] = []
        self.lines: list[dict[str, object]] = []

    def request_key(self) -> None:
        """A datagram with no key yet: srtpdec asks, and is handed a candidate."""
        caps = self.pipeline._on_request_key(None, SSRC)
        _ok, roc = caps.get_structure(0).get_uint("roc")
        self.handed_out.append(roc)

    def authenticate(self, seq: int) -> None:
        """libsrtp authenticated a packet under whatever key srtpdec holds."""
        self.pipeline._record_authenticated(seq)

    def tick(self) -> None:
        self.pipeline._tick()

    def outcome(self) -> dict[str, object]:
        first = [
            line for line in self.lines if line.get("t") == "auth" and line.get("first")
        ]
        return {
            "confirmed": self.pipeline.confirmed,
            "reportedCandidate": first[0]["candidate"] if first else None,
            "reportedRoc": first[0]["roc"] if first else None,
            "handedToSrtpdec": self.handed_out,
            "searchingAt": self.pipeline.candidate,
        }


def run(hint: int, script: Callable[[Scenario], None]) -> dict[str, object]:
    scenario = Scenario(hint)
    captured = io.StringIO()
    with contextlib.redirect_stdout(captured):
        scenario.pipeline._begin_trial()
        script(scenario)
    scenario.lines = [json.loads(line) for line in captured.getvalue().splitlines()]
    return scenario.outcome()


def new_key_authenticates_before_the_trial_baseline(s: Scenario) -> None:
    """The hint is wrong, zero is right, and zero's only packet lands in the switch.

    The next datagram asks for a key and authenticates under the new candidate before
    the new trial is set up. A baseline snapshotted when the trial begins absorbs that
    authentication and credits the candidate with nothing.
    """
    s.request_key()
    s.decoder.drops = 10

    def streaming_thread() -> None:
        s.request_key()
        s.authenticate(seq=1)

    s.decoder.on_remove_key = streaming_thread
    s.tick()
    for _ in range(3):
        s.tick()


def old_key_authenticates_after_the_switch(s: Scenario) -> None:
    """The hint is right, but noise ends its trial just as its first real packet lands.

    The search decides the trial failed and steps on, and a packet authenticates under
    the old key - still installed - before remove-key takes effect. Credited to the
    candidate the search stepped to, the right one is discarded and the wrong one
    reported; credited to the key that authenticated it, srtpdec gets that key back.
    """
    s.request_key()
    s.decoder.drops = 10

    def streaming_thread() -> None:
        s.authenticate(seq=1)
        s.request_key()

    s.decoder.on_remove_key = streaming_thread
    s.tick()
    for _ in range(3):
        s.tick()


def live_stream(s: Scenario) -> None:
    """The ordinary case: the right candidate, and packets that keep authenticating."""
    s.request_key()
    for seq in range(5):
        s.authenticate(seq)
    s.tick()


def nothing_authenticates(s: Scenario) -> None:
    """Noise only: no candidate may be confirmed, and the search has to move."""
    s.request_key()
    s.decoder.on_remove_key = s.request_key
    for trial in range(1, 4):
        s.decoder.drops = 10 * trial
        s.tick()


if __name__ == "__main__":
    print(
        json.dumps(
            {
                "newKeyAuthenticatesBeforeTheTrialBaseline": run(
                    5, new_key_authenticates_before_the_trial_baseline
                ),
                "oldKeyAuthenticatesAfterTheSwitch": run(
                    5, old_key_authenticates_after_the_switch
                ),
                "liveStream": run(5, live_stream),
                "nothingAuthenticates": run(5, nothing_authenticates),
            }
        )
    )
