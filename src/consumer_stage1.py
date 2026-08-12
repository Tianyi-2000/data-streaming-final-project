"""Consumer 1: validate `play-events`, count per listener, re-key to `track-activity`.

THE RE-KEY IS THE POINT. Events arrive keyed by `listener_id`, are re-validated
against the shared contract, counted into a per-listener event-time window, and
leave keyed by `track_id`. This repartition is what the project exists to
demonstrate (CD-3): without the per-listener state the pipeline is one key with a
pointless extra hop, and Topology A detection silently disappears -- nothing
downstream reads this state, so skipping it breaks nothing and detects nothing.

Five things a later reader needs, in the order they matter.

1. THE OUTGOING VALUE IS THE BYTES THAT ARRIVED. Contract section 4 requires the
   record on `track-activity` to carry the same `PlayEventV1` JSON that was
   received, so `Decision.out_value` is `msg.value()` itself. The parsed model is
   used for the decision and for the outgoing `track_id` key, and never to
   produce the payload. This is easy to get wrong invisibly: measured across
   every fixture line and the head of the real stream, parsing a line into the
   model and serializing it straight back out is BYTE-IDENTICAL to the line, so
   a re-serializing implementation passes a naive `forwarded == original` test.
   `test_the_forwarded_value_is_the_original_bytes_not_a_model_round_trip` uses a
   record whose text a round trip WOULD change, which is what makes it
   discriminate. (The prose above deliberately avoids naming pydantic's
   serializer, so that grepping this file for that method name stays a check on
   the code rather than a hit on a comment about it.)

2. THE FLAG IS A HIGH-WATER MARK, BECAUSE `add()`'s RETURN CAN FALL.
   `RollingHourlyWindows`' watermark is per key and its window is 24 hour-buckets
   wide, so as the window slides past hours a listener did not play in, the count
   `add` returns for that listener DECREASES. A flag derived from "the last value
   `add` returned" therefore un-flags a listener it had already flagged, later in
   the same stream, with no error anywhere. `commit_event` deliberately returns
   `add`'s raw value so that fall is observable, and `_ListenerState.peak` is what
   the comparison reads.

   The comparison is a STRICT `>` (CD-4: more than). It is applied to a
   conservative number -- the module's bucket-aligned sum is always <= the
   event-anchored sliding maximum -- so at the margin this stage withholds a flag
   and cannot manufacture one. That is the right direction for a project whose
   stated ethical failure is accusing an innocent artist. The fixture's FN01 sits
   at exactly the threshold so flipping `>` to `>=` fails a test.

3. THE INPUT OFFSET IS COMMITTED AFTER THE PRODUCE IS CONFIRMED AND THE JOURNAL
   LINE IS FLUSHED (contract section 5). The ordering in `_settle` IS the
   guarantee: delivery callback -> journal append + flush -> `store_offsets` ->
   `commit`. `enable.auto.commit` AND `enable.auto.offset.store` are both off:
   librdkafka defaults the latter to true and would store a record's offset the
   moment `poll()` returned it, before anything had been validated, produced or
   journalled -- which would leave the guarantee resting on no code path happening
   to call `commit()` at the wrong moment. The shutdown settle is exactly such a
   moment, and it is safe only because the automatic store is off. There is
   deliberately no `finally` that commits.

   That makes delivery AT-LEAST-ONCE, which is why `event_id` dedup is required
   rather than optional: a crash between produce and commit replays the record.

   DEDUP THEREFORE NEEDS TWO SETS, NOT ONE. `_seen_event_ids` only grows in
   `commit_event`, which `_settle` calls at the END of a batch, so at
   `--commit-every N > 1` every copy of one `event_id` inside a single batch
   passed `decide` and was produced -- measured on the full stream, 903 records
   were forwarded and counted twice. `_in_flight_event_ids` closes that window.
   The two are deliberately not merged: a settle failure must FORGET the
   in-flight ids, because those records were never journalled, counted or
   committed and the broker will redeliver them. Treating them as seen would
   suppress the redelivery of a record that never reached `track-activity` --
   an idempotency fix that loses data. Hence the discard in `_settle`'s
   `finally`, and hence `discard_in_flight` rather than a promotion.

4. THE JOURNAL IS THE ONLY PERSISTED STATE. Counts, flags and the dedup set are
   all derived by replaying it through the same `commit_event` the live path uses,
   so recovery cannot drift from live processing. File order matters -- `add`
   drops an event belonging to an hour that key's watermark has passed. A line
   truncated by a kill is warned about and skipped rather than raising; `flush`
   without `fsync` is the right call, because a SIGKILL leaves the OS buffers
   intact and power loss is not what this defends against.

5. `late_dropped` IS READ OFF THE ACCUMULATOR AND SURFACED, NOT SWALLOWED.
   Watermarks are per key, so a non-zero value means one key's events arrived out
   of `event_time` order INSIDE ITS OWN PARTITION -- genuine disorder, not the
   ordinary consequence of polling three partitions. The affected listeners'
   counts are not trustworthy when it fires, and the run says so.

This stage reads no Topology B signal -- not `played_seconds`, not the stop band,
not unique-track counts. Those belong to Consumer 2 (Phase 4); computing them
behind the `listener_id` key would make CD-9's separability claim false before
Phase 5 asserts it.

Usage:
    # broker up and topics created first:
    #   docker compose up -d && python src/create_topics.py
    python src/consumer_stage1.py
    python src/consumer_stage1.py --thresholds config/thresholds.fixture.json
    python src/consumer_stage1.py --commit-every 200        # full-scale run
    python src/consumer_stage1.py --throttle 0.01           # demo pace
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union

from confluent_kafka import Consumer, KafkaException, Producer
from pydantic import ValidationError

# Same import bootstrap `src/windowing.py` and `src/config.py` use, so this
# module resolves however it is invoked and never depends on a conftest.py.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.play_event_v1 import (  # noqa: E402
    TOPIC_PLAY_EVENTS,
    TOPIC_TRACK_ACTIVITY,
    PlayEventV1,
    key_matches_listener_id,
    parse_event_time,
)
from src.config import Thresholds, load_thresholds  # noqa: E402
from src.windowing import topology_a_windows  # noqa: E402

_LOG = logging.getLogger(__name__)

# Why a record was not forwarded. Contract section 6 lists the invalidity rules;
# the third is not a contract rule but a consequence of at-least-once delivery.
INVALID_VALUE = "invalid_value"
KEY_MISMATCH = "key_mismatch"
DUPLICATE_EVENT_ID = "duplicate_event_id"

# The only persisted state this stage keeps. Named here rather than at the call
# site so recovery and the tests cannot disagree about which file it is.
JOURNAL_FILENAME = "consumer1_journal.jsonl"


@dataclass
class _ListenerState:
    """One listener's Topology A evidence: the peak, the volume and the span.

    `peak` is a HIGH-WATER MARK over every value `RollingHourlyWindows.add`
    returned for this listener, not the last of them.
    """

    peak: int = 0
    plays: int = 0
    first_event_time: str = ""
    last_event_time: str = ""


@dataclass(frozen=True)
class Decision:
    """What to do with one record off `play-events`, and why.

    `out_value` is **the bytes that arrived, unchanged**. Contract section 4
    requires the value on `track-activity` to be the same `PlayEventV1` JSON that
    was received, so the parsed model is used for decisions and for the outgoing
    key and never to re-serialize the payload.
    """

    forward: bool
    out_key: Optional[bytes]
    out_value: Optional[bytes]
    event: Optional[PlayEventV1]
    drop_reason: Optional[str]
    detail: str = ""


class Stage1Processor:
    """Per-listener state for the Topology A rule, plus the forward/drop decision.

    Construct the accumulator through `topology_a_windows` rather than with an
    integer, so retuning `topology_a_window_hours` in configuration reaches the
    running code (CD-4, CTRT-04).
    """

    def __init__(self, thresholds: Thresholds) -> None:
        self._thresholds = thresholds
        self._windows = topology_a_windows(thresholds)
        # Dedup is keyed on `event_id` and on nothing else (contract section 5).
        self._seen_event_ids: set = set()
        # THE SAME DEDUP, FOR RECORDS NOT YET SETTLED. `_seen_event_ids` is only
        # populated by `commit_event`, which `_settle` calls at the END of a
        # batch, so at `--commit-every N > 1` every copy of one `event_id` inside
        # a single batch used to pass `decide` and be produced. This set closes
        # that window. It is deliberately separate rather than merged into
        # `_seen_event_ids`: a settle failure must forget these ids, because
        # those records are redelivered and treating them as already seen would
        # suppress the redelivery and lose them -- turning an idempotency fix
        # into data loss.
        self._in_flight_event_ids: set = set()
        # THE HIGH-WATER MARK, NOT THE LAST VALUE `add` RETURNED. `add`'s return
        # falls as the 24-bucket window slides past hours a listener did not play
        # in, so a flag derived from the last value silently un-flags a listener
        # later in the stream.
        self._listeners: Dict[str, _ListenerState] = {}
        self._counts_seen = 0
        self._counts_forwarded = 0
        self._counts_invalid_value = 0
        self._counts_key_mismatch = 0
        self._counts_duplicate_event_id = 0

    # --- decisions --------------------------------------------------------
    def decide(self, key: Optional[bytes], value: Optional[bytes]) -> Decision:
        """Forward or drop one record. Pure: no Kafka, no file I/O, no counting.

        Every parse happens inside this boundary and returns a drop decision
        rather than raising, so one malformed record cannot stop the poll loop
        (threat T-03-02).
        """
        if value is None:
            return Decision(
                forward=False,
                out_key=None,
                out_value=None,
                event=None,
                drop_reason=INVALID_VALUE,
                detail="record value is null; there is nothing to validate",
            )

        try:
            event = PlayEventV1.model_validate_json(value)
        except ValidationError as exc:
            return Decision(
                forward=False,
                out_key=None,
                out_value=None,
                event=None,
                drop_reason=INVALID_VALUE,
                detail=str(exc),
            )

        # The one contract rule the model structurally cannot enforce: it
        # validates the Kafka value and never sees the key. A mismatch would
        # route one listener's plays into another listener's state.
        if not key_matches_listener_id(key, event):
            return Decision(
                forward=False,
                out_key=None,
                out_value=None,
                event=event,
                drop_reason=KEY_MISMATCH,
                detail=(
                    f"kafka key {key!r} does not equal the value's listener_id "
                    f"{event.listener_id!r}"
                ),
            )

        if event.event_id in self._seen_event_ids:
            return Decision(
                forward=False,
                out_key=None,
                out_value=None,
                event=event,
                drop_reason=DUPLICATE_EVENT_ID,
                detail=(
                    f"event_id {event.event_id!r} was already counted and forwarded"
                ),
            )

        if event.event_id in self._in_flight_event_ids:
            # Same rule, one batch earlier: this id has been produced but not yet
            # settled, so it is not in `_seen_event_ids` yet. Reported under the
            # same reason as a cross-batch duplicate so `duplicate_event_id` means
            # the same thing at every `--commit-every`.
            return Decision(
                forward=False,
                out_key=None,
                out_value=None,
                event=event,
                drop_reason=DUPLICATE_EVENT_ID,
                detail=(
                    f"event_id {event.event_id!r} was already produced in the "
                    f"current unsettled batch"
                ),
            )

        return Decision(
            forward=True,
            # `value` itself, not a re-serialization of `event` (contract 4).
            out_key=event.track_id.encode("utf-8"),
            out_value=value,
            event=event,
            drop_reason=None,
            detail="",
        )

    def tally(self, decision: Decision) -> None:
        """Record one decision in the observability counters.

        Separate from `decide` so that `decide` stays pure and can be called
        twice on the same bytes in a test without moving any number.
        """
        self._counts_seen += 1
        if decision.forward:
            self._counts_forwarded += 1
        elif decision.drop_reason == INVALID_VALUE:
            self._counts_invalid_value += 1
        elif decision.drop_reason == KEY_MISMATCH:
            self._counts_key_mismatch += 1
        elif decision.drop_reason == DUPLICATE_EVENT_ID:
            self._counts_duplicate_event_id += 1

    # --- the unsettled batch ---------------------------------------------
    def mark_in_flight(self, event_id: str) -> None:
        """Record that `event_id` has been produced but not yet settled.

        Called immediately before the produce, so a second copy of the same id
        later in the same batch is a duplicate rather than a second produce.
        """
        self._in_flight_event_ids.add(event_id)

    def discard_in_flight(self) -> None:
        """Forget the unsettled batch's ids, whether the settle succeeded or not.

        On success `commit_event` has already moved every one of them into
        `_seen_event_ids`, so this is a clear. On FAILURE it is the load-bearing
        half: those records were never journalled, never counted and never
        committed, so the broker will redeliver them, and they must look new when
        it does. Merging them into `_seen_event_ids` instead would suppress the
        redelivery of a record that never reached `track-activity`.
        """
        self._in_flight_event_ids.clear()

    # --- counting ---------------------------------------------------------
    def commit_event(self, event_id: str, listener_id: str, event_time: str) -> int:
        """Count one settled event and return the RAW value `add` gave back.

        Called only after the corresponding `track-activity` produce is confirmed
        and journalled -- and by the recovery path, which replays journal entries
        through this same method so recovery cannot drift from live processing.

        The return is `RollingHourlyWindows.add`'s own value, deliberately NOT
        the high-water mark: that value can fall for a live key as the window
        slides, and a caller has to be able to observe the fall.
        """
        self._seen_event_ids.add(event_id)
        count = self._windows.add(listener_id, event_time)

        state = self._listeners.get(listener_id)
        if state is None:
            state = _ListenerState(
                first_event_time=event_time, last_event_time=event_time
            )
            self._listeners[listener_id] = state
        else:
            # Compare instants, store the contract strings: the review document
            # quotes the wire format a reader can match back to the stream.
            moment = parse_event_time(event_time)
            if moment < parse_event_time(state.first_event_time):
                state.first_event_time = event_time
            if moment > parse_event_time(state.last_event_time):
                state.last_event_time = event_time
        state.plays += 1
        state.peak = max(state.peak, count)
        return count

    # --- the Topology A judgment -----------------------------------------
    def peak_for(self, listener_id: str) -> int:
        """The highest rolling count ever seen for `listener_id`, or 0."""
        state = self._listeners.get(listener_id)
        return state.peak if state is not None else 0

    @property
    def window_hours(self) -> int:
        """The accumulator's own window size, not a copy of the config value."""
        return self._windows.window_hours

    @property
    def late_dropped(self) -> int:
        """The accumulator's own count of per-key `event_time` disorder.

        Delegated rather than tracked separately, so this number cannot drift
        from the one the windowing module actually acted on.
        """
        return self._windows.late_dropped

    def flagged_listeners(self) -> List[Dict[str, Any]]:
        """Listeners whose peak rolling count is STRICTLY GREATER than the threshold.

        CD-4 says *more than*. A listener sitting exactly on the threshold is not
        flagged, and `>=` here would quietly change who gets accused -- the
        fixture's FN01 sits exactly there for that reason.

        `threshold` and `window_hours` are read from the loaded configuration, so
        a retune reaches both the decision and the record of it (CTRT-04).
        """
        return [
            {
                "listener_id": listener_id,
                "peak_plays_in_window": state.peak,
                "plays_recorded": state.plays,
                "window_hours": self.window_hours,
                "threshold": self._thresholds.topology_a_plays_over,
                "first_event_time": state.first_event_time,
                "last_event_time": state.last_event_time,
            }
            for listener_id, state in sorted(self._listeners.items())
            if state.peak > self._thresholds.topology_a_plays_over
        ]

    def counts(self) -> Dict[str, int]:
        """What this run saw, forwarded and dropped, plus per-key disorder."""
        return {
            "records_seen": self._counts_seen,
            "forwarded": self._counts_forwarded,
            "invalid_value": self._counts_invalid_value,
            "key_mismatch": self._counts_key_mismatch,
            "duplicate_event_id": self._counts_duplicate_event_id,
            "late_dropped": self.late_dropped,
            "listeners_seen": len(self._listeners),
        }

    def review_document(self) -> Dict[str, Any]:
        """The listener-side review queue: which listeners, and on what evidence.

        This document describes what was counted and against which numbers, and
        reaches no conclusion beyond "flagged". The project's stated ethical
        failure is accusing an innocent artist, so this is a review queue for a
        human, not a verdict -- which is why every entry carries the count, the
        window, the threshold and the config file behind it, and why the strict
        inequality is spelled out rather than left for a reader to assume.
        """
        return {
            "flagged_listeners": self.flagged_listeners(),
            "counts": self.counts(),
            "thresholds_source_path": self._thresholds.source_path,
            "rule": (
                "topology_a: a listener's plays in a rolling "
                "topology_a_window_hours window, compared against "
                "topology_a_plays_over"
            ),
            "comparison": (
                "strictly greater than (>): a listener sitting exactly on "
                "topology_a_plays_over is NOT flagged"
            ),
        }


class Journal:
    """The append-only record of settled events -- this stage's only persisted state.

    One JSON object per line, written and flushed before the input offset is
    committed. `flush` without `fsync` is the right call here: a SIGKILL leaves
    the OS buffers intact, and power loss is not what this design defends against.
    """

    def __init__(self, path: Union[str, Path]) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = None

    @property
    def path(self) -> Path:
        return self._path

    def append(self, event_id: str, listener_id: str, event_time: str) -> None:
        if self._handle is None:
            self._handle = self._path.open("a", encoding="utf-8")
        line = json.dumps(
            {
                "event_id": event_id,
                "listener_id": listener_id,
                "event_time": event_time,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        self._handle.write(line + "\n")

    def flush(self) -> None:
        if self._handle is not None:
            self._handle.flush()

    def entries(self) -> Iterator[Dict[str, Any]]:
        """Every complete entry in file order; incomplete ones warned about and skipped.

        A SIGKILL can land mid-write, so the last line may be half a JSON object.
        Recovery must not be the thing that breaks.
        """
        if not self._path.is_file():
            return
        with self._path.open(encoding="utf-8") as handle:
            for lineno, raw in enumerate(handle, start=1):
                line = raw.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError as exc:
                    _LOG.warning(
                        "skipping unparsable journal line %s:%d (%s) -- most likely a "
                        "write interrupted by a kill",
                        self._path,
                        lineno,
                        exc,
                    )
                    continue
                missing = [
                    field
                    for field in ("event_id", "listener_id", "event_time")
                    if not isinstance(entry, dict) or field not in entry
                ]
                if missing:
                    _LOG.warning(
                        "skipping journal line %s:%d -- missing %s",
                        self._path,
                        lineno,
                        ", ".join(missing),
                    )
                    continue
                yield entry

    def close(self) -> None:
        if self._handle is not None:
            self._handle.flush()
            self._handle.close()
            self._handle = None


def replay_records(
    processor: Stage1Processor,
    records: Iterable[Tuple[Optional[bytes], Optional[bytes]]],
    journal: Optional[Journal] = None,
) -> List[Decision]:
    """Decide and count a sequence of `(key, value)` pairs, with no broker.

    The same decide -> journal -> count ordering the poll loop uses, minus the
    produce and the offset discipline. Tests drive the fixture through this, and
    it is the one place that ordering is written down for the no-broker path.

    `journal` is optional so the same helper can produce a real journal for the
    restart tests rather than having them reimplement the write ordering.
    """
    decisions: List[Decision] = []
    for key, value in records:
        decision = processor.decide(key, value)
        processor.tally(decision)
        decisions.append(decision)
        if decision.forward:
            event = decision.event
            if journal is not None:
                journal.append(event.event_id, event.listener_id, event.event_time)
                journal.flush()
            processor.commit_event(
                event.event_id, event.listener_id, event.event_time
            )
    return decisions


def recover_from_journal(processor: Stage1Processor, journal: Journal) -> int:
    """Rebuild counts, flags and the dedup set from the journal. Returns the count.

    FILE ORDER MATTERS. `RollingHourlyWindows.add` drops an event belonging to an
    hour that key's watermark has already passed, so replaying out of order would
    produce different counts from the run that wrote the file.

    Every entry goes through the live path's own `commit_event`, which is what
    makes recovery incapable of drifting from live processing.
    """
    recovered = 0
    for entry in journal.entries():
        try:
            processor.commit_event(
                entry["event_id"], entry["listener_id"], entry["event_time"]
            )
        except (ValueError, TypeError) as exc:
            # A structurally complete line whose `event_time` is not a contract
            # timestamp. Recovery reports it and moves on rather than refusing to
            # start over one bad line.
            _LOG.warning(
                "skipping journal entry %r: %s", entry.get("event_id"), exc
            )
            continue
        recovered += 1
    _LOG.info("recovered %d event(s) from %s", recovered, journal.path)
    return recovered


def write_listener_review(
    processor: Stage1Processor, path: Union[str, Path]
) -> Dict[str, Any]:
    """Serialize the review document deterministically and return it."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    doc = processor.review_document()
    target.write_text(
        json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return doc


def _build_consumer(broker: str, group: str) -> Consumer:
    """A consumer that neither commits nor STORES an offset by itself.

    `enable.auto.offset.store` is load-bearing and is written down rather than
    left to the library. librdkafka defaults it to true, which stores a record's
    offset the moment `poll()` returns it -- before this stage has validated,
    produced or journalled anything. With that default in place the whole
    "commit only after the produce is confirmed" guarantee would rest on nothing
    but the fact that no code path happened to call `commit()` at the wrong
    moment. Turning the store off and doing it by hand makes the guarantee true
    by construction rather than by luck.
    """
    return Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )


def _build_producer(broker: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": broker,
            "acks": "all",
            "client.id": "consumer-stage1",
        }
    )


@dataclass(frozen=True)
class RunSummary:
    """What one run of this stage did, in numbers a later phase can parse."""

    polled: int
    forwarded: int
    invalid_value: int
    key_mismatch: int
    duplicate_event_id: int
    late_dropped: int
    listeners_seen: int
    recovered_from_journal: int
    flagged_listeners: List[str]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "polled": self.polled,
            "forwarded": self.forwarded,
            "invalid_value": self.invalid_value,
            "key_mismatch": self.key_mismatch,
            "duplicate_event_id": self.duplicate_event_id,
            "late_dropped": self.late_dropped,
            "listeners_seen": self.listeners_seen,
            "recovered_from_journal": self.recovered_from_journal,
            "flagged_listeners": list(self.flagged_listeners),
        }


@dataclass
class _Pending:
    """One decided record awaiting settlement.

    `delivery` is None for a dropped record -- there is nothing to confirm -- and
    a mutable dict the delivery callback writes into for a forwarded one.
    """

    msg: Any
    decision: Decision
    delivery: Optional[Dict[str, Any]]


def _settle(
    consumer: Consumer,
    producer: Producer,
    processor: Stage1Processor,
    journal: Journal,
    pending: List[_Pending],
    out_topic: str,
    flush_timeout: float,
) -> None:
    """Confirm, journal, count, store and commit one batch -- in that order only.

    THE ORDERING IS THE AT-LEAST-ONCE GUARANTEE (contract section 5). Every
    produce in the batch is confirmed by its own delivery callback before any
    journal line is written; the journal is flushed before any offset is stored;
    and only stored offsets can be committed. Nothing else can have been stored,
    because `enable.auto.offset.store` is off.

    The invariant holds at any batch size. A dropped record joins the batch with
    nothing to produce rather than being committed out of turn: committing its
    offset immediately would commit PAST an earlier record in the same partition
    whose produce had not yet been confirmed.

    A delivery failure raises here, so the journal is not written, no offset is
    stored and nothing is committed. The record is redelivered on the next run,
    which is the point -- and `event_id` dedup absorbs the replay.

    THE IN-FLIGHT DEDUP SET IS DISCARDED ON BOTH PATHS, which is why the discard
    lives in a `finally`. On the success path `commit_event` has already promoted
    every id into `_seen_event_ids`, so clearing is housekeeping. On the failure
    path it is the correctness half: nothing was journalled, counted or
    committed, so those records come back, and they have to look new when they
    do.
    """
    if not pending:
        return

    try:
        remaining = producer.flush(flush_timeout)
        if remaining:
            raise RuntimeError(
                f"{remaining} message(s) still queued for '{out_topic}' after "
                f"{flush_timeout:.0f}s; refusing to commit an unconfirmed offset"
            )

        for entry in pending:
            if entry.delivery is None:
                continue
            if not entry.delivery.get("fired"):
                raise RuntimeError(
                    f"no delivery callback fired for '{out_topic}' "
                    f"(event_id {entry.decision.event.event_id}); refusing to "
                    "commit an unconfirmed offset"
                )
            if entry.delivery["err"] is not None:
                raise RuntimeError(
                    f"delivery to '{out_topic}' failed for event_id "
                    f"{entry.decision.event.event_id}: {entry.delivery['err']}; "
                    "the input offset is neither stored nor committed, so the "
                    "record will be redelivered"
                )

        forwarded = [
            entry.decision.event for entry in pending if entry.decision.forward
        ]
        for event in forwarded:
            journal.append(event.event_id, event.listener_id, event.event_time)
        journal.flush()
        for event in forwarded:
            processor.commit_event(
                event.event_id, event.listener_id, event.event_time
            )

        for entry in pending:
            consumer.store_offsets(message=entry.msg)
        consumer.commit(asynchronous=False)
        pending.clear()
    finally:
        processor.discard_in_flight()


def run(
    *,
    broker: str,
    group: str,
    in_topic: str,
    out_topic: str,
    state_dir: Union[str, Path],
    thresholds: Thresholds,
    review_path: Union[str, Path],
    max_events: int = 0,
    commit_every: int = 1,
    idle_timeout: float = 10.0,
    throttle: float = 0.0,
    flush_timeout: float = 30.0,
) -> RunSummary:
    """Consume `in_topic`, re-key onto `out_topic`, commit only after delivery.

    Recovers from the journal at `state_dir` before the first poll, so a restart
    continues the same counts rather than starting a second, emptier history.
    """
    if commit_every < 1:
        raise ValueError(f"commit_every must be at least 1; got {commit_every}")

    processor = Stage1Processor(thresholds)
    journal = Journal(Path(state_dir) / JOURNAL_FILENAME)
    recovered = recover_from_journal(processor, journal)

    consumer = _build_consumer(broker, group)
    producer = _build_producer(broker)
    consumer.subscribe([in_topic])

    pending: List[_Pending] = []
    polled = 0
    try:
        idle_since = time.monotonic()
        while True:
            if max_events and polled >= max_events:
                break

            msg = consumer.poll(1.0)
            if msg is None:
                # An idle poll is a settle point: pending work must not wait for a
                # record that may never arrive.
                _settle(
                    consumer, producer, processor, journal, pending,
                    out_topic, flush_timeout,
                )
                if time.monotonic() - idle_since >= idle_timeout:
                    break
                continue
            if msg.error():
                _LOG.warning("consumer error on %s: %s", in_topic, msg.error())
                continue

            idle_since = time.monotonic()
            polled += 1

            decision = processor.decide(msg.key(), msg.value())
            processor.tally(decision)

            if not decision.forward:
                _LOG.warning(
                    "dropped record: reason=%s event_id=%s partition=%s offset=%s "
                    "detail=%s",
                    decision.drop_reason,
                    decision.event.event_id if decision.event is not None else None,
                    msg.partition(),
                    msg.offset(),
                    decision.detail,
                )
                pending.append(_Pending(msg=msg, decision=decision, delivery=None))
            else:
                delivery: Dict[str, Any] = {}

                def _on_delivery(err, _msg, _slot=delivery) -> None:
                    _slot["fired"] = True
                    _slot["err"] = err

                # Before the produce, not after: a second copy of this id later
                # in the same unsettled batch has to be a duplicate rather than a
                # second produce.
                processor.mark_in_flight(decision.event.event_id)
                producer.produce(
                    out_topic,
                    key=decision.out_key,
                    value=decision.out_value,
                    on_delivery=_on_delivery,
                )
                # Serve delivery callbacks so the internal queue does not fill up.
                producer.poll(0)
                pending.append(
                    _Pending(msg=msg, decision=decision, delivery=delivery)
                )

            if len(pending) >= commit_every:
                _settle(
                    consumer, producer, processor, journal, pending,
                    out_topic, flush_timeout,
                )

            if throttle:
                time.sleep(throttle)

        # The shutdown settle. It is safe only because the automatic offset store
        # is off: a record whose delivery failed never had its offset stored, so
        # this cannot commit one. There is deliberately no `finally` that commits.
        _settle(
            consumer, producer, processor, journal, pending,
            out_topic, flush_timeout,
        )
    finally:
        journal.close()
        producer.flush(flush_timeout)
        consumer.close()

    counts = processor.counts()
    summary = RunSummary(
        polled=counts["records_seen"],
        forwarded=counts["forwarded"],
        invalid_value=counts["invalid_value"],
        key_mismatch=counts["key_mismatch"],
        duplicate_event_id=counts["duplicate_event_id"],
        # Read off the accumulator, never tracked in parallel.
        late_dropped=counts["late_dropped"],
        listeners_seen=counts["listeners_seen"],
        recovered_from_journal=recovered,
        flagged_listeners=[
            entry["listener_id"] for entry in processor.flagged_listeners()
        ],
    )

    write_listener_review(processor, review_path)
    _report(summary, processor, journal, review_path)
    return summary


def _report(
    summary: RunSummary,
    processor: Stage1Processor,
    journal: Journal,
    review_path: Union[str, Path],
) -> None:
    """A human-readable block, then one machine-readable line, on stdout."""
    if summary.late_dropped:
        _LOG.warning(
            "%d event(s) were dropped as LATE. One key's events arrived out of "
            "event_time order inside its own partition, so the affected listeners' "
            "counts are not trustworthy. This is NOT ordinary multi-partition "
            "interleaving -- per-key watermarks already absorb that.",
            summary.late_dropped,
        )

    print("")
    print(f"Consumer 1 done. Polled {summary.polled}, forwarded {summary.forwarded}.")
    print(f"  recovered from journal : {summary.recovered_from_journal}")
    print(f"  dropped, invalid value : {summary.invalid_value}")
    print(f"  dropped, key mismatch  : {summary.key_mismatch}")
    print(f"  dropped, duplicate id  : {summary.duplicate_event_id}")
    print(f"  late dropped           : {summary.late_dropped}")
    print(f"  listeners seen         : {summary.listeners_seen}")
    threshold = processor.review_document()["flagged_listeners"]
    print(
        f"  flagged listeners      : {len(summary.flagged_listeners)} "
        f"{summary.flagged_listeners}"
    )
    for entry in threshold:
        print(
            f"      {entry['listener_id']}: peak {entry['peak_plays_in_window']} "
            f"plays in {entry['window_hours']}h, strictly over {entry['threshold']} "
            f"({entry['first_event_time']} .. {entry['last_event_time']})"
        )
    print(f"  journal                : {journal.path}")
    print(f"  review queue           : {review_path}")
    print("SUMMARY " + json.dumps(summary.as_dict(), sort_keys=True))


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point. Mirrors `src/replay_to_kafka.py`'s argparse shape."""
    parser = argparse.ArgumentParser(
        description=(
            "Consumer 1: validate play-events, count per listener, re-key onto "
            "track-activity."
        )
    )
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--group", default="consumer-stage1")
    parser.add_argument("--in-topic", default=TOPIC_PLAY_EVENTS)
    parser.add_argument("--out-topic", default=TOPIC_TRACK_ACTIVITY)
    parser.add_argument("--state-dir", default=str(REPO_ROOT / "state"))
    parser.add_argument(
        "--review-out",
        default=str(REPO_ROOT / "output" / "listener_review_queue.json"),
    )
    parser.add_argument(
        "--thresholds",
        default=None,
        help=(
            "threshold config path; defaults to config/thresholds.json or "
            "$THRESHOLDS_PATH. There is no built-in fallback number."
        ),
    )
    parser.add_argument(
        "--commit-every",
        type=int,
        default=1,
        help=(
            "settle and commit after N decided records (1 = strictest, and what "
            "the kill/restart proof runs at)"
        ),
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=10.0,
        help="seconds of empty polling before settling and exiting cleanly",
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=0.0,
        help="seconds to sleep between records (0 = as fast as possible)",
    )
    parser.add_argument("--max-events", type=int, default=0, help="0 = unlimited")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )

    try:
        thresholds = load_thresholds(args.thresholds)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except ValidationError as exc:
        print(
            f"ERROR: threshold config {args.thresholds!r} is not valid: {exc}",
            file=sys.stderr,
        )
        return 1

    try:
        run(
            broker=args.broker,
            group=args.group,
            in_topic=args.in_topic,
            out_topic=args.out_topic,
            state_dir=args.state_dir,
            thresholds=thresholds,
            review_path=args.review_out,
            max_events=args.max_events,
            commit_every=args.commit_every,
            idle_timeout=args.idle_timeout,
            throttle=args.throttle,
        )
    except (RuntimeError, KafkaException) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
