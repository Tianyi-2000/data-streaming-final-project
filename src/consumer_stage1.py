"""Consumer 1: validate `play-events`, count per listener, re-key to `track-activity`.

Interim docstring. The full one is written once the behaviour is settled (Task 3
of plan 03-01); what is here now records only what the shipped code already does.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

from confluent_kafka import Consumer, Producer
from pydantic import ValidationError

# Same import bootstrap `src/windowing.py` and `src/config.py` use, so this
# module resolves however it is invoked and never depends on a conftest.py.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.play_event_v1 import (  # noqa: E402
    PlayEventV1,
    key_matches_listener_id,
)
from src.config import Thresholds  # noqa: E402
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
        # THE HIGH-WATER MARK, NOT THE LAST VALUE `add` RETURNED. `add`'s return
        # falls as the 24-bucket window slides past hours a listener did not play
        # in, so a flag derived from the last value silently un-flags a listener
        # later in the stream.
        self._peak: Dict[str, int] = {}
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
        self._peak[listener_id] = max(self._peak.get(listener_id, 0), count)
        return count

    # --- the Topology A judgment -----------------------------------------
    def flagged_listeners(self) -> List[Dict[str, Any]]:
        """Listeners whose peak rolling count is STRICTLY GREATER than the threshold.

        CD-4 says *more than*. A listener sitting exactly on the threshold is not
        flagged, and `>=` here would quietly change who gets accused.
        """
        return [
            {
                "listener_id": listener_id,
                "peak_plays_in_window": peak,
                "window_hours": self.window_hours,
                "threshold": self._thresholds.topology_a_plays_over,
            }
            for listener_id, peak in sorted(self._peak.items())
            if peak > self._thresholds.topology_a_plays_over
        ]

    @property
    def window_hours(self) -> int:
        """The accumulator's own window size, not a copy of the config value."""
        return self._windows.window_hours

    def review_document(self) -> Dict[str, Any]:
        """The listener-side review queue: flags, and the numbers behind them."""
        return {
            "flagged_listeners": self.flagged_listeners(),
            "counts": {
                "records_seen": self._counts_seen,
                "forwarded": self._counts_forwarded,
                "invalid_value": self._counts_invalid_value,
                "key_mismatch": self._counts_key_mismatch,
                "duplicate_event_id": self._counts_duplicate_event_id,
            },
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
    idle_timeout: float = 10.0,
    flush_timeout: float = 30.0,
) -> None:
    """Consume `in_topic`, re-key onto `out_topic`, commit only after delivery."""
    processor = Stage1Processor(thresholds)
    journal = Journal(Path(state_dir) / JOURNAL_FILENAME)
    consumer = _build_consumer(broker, group)
    producer = _build_producer(broker)
    consumer.subscribe([in_topic])

    polled = 0
    try:
        idle_since = time.monotonic()
        while True:
            if max_events and polled >= max_events:
                break

            msg = consumer.poll(1.0)
            if msg is None:
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
                # Nothing to produce, so this offset is safe to store and commit
                # immediately -- see step 5 below.
                _LOG.warning(
                    "dropped record: reason=%s event_id=%s partition=%s offset=%s "
                    "detail=%s",
                    decision.drop_reason,
                    decision.event.event_id if decision.event is not None else None,
                    msg.partition(),
                    msg.offset(),
                    decision.detail,
                )
            else:
                _produce_and_confirm(producer, out_topic, decision, flush_timeout)
                event = decision.event
                journal.append(event.event_id, event.listener_id, event.event_time)
                journal.flush()
                processor.commit_event(
                    event.event_id, event.listener_id, event.event_time
                )

            # The ordering IS the at-least-once guarantee: the produce is
            # confirmed and the journal line is on disk before this offset can
            # be stored, and only a stored offset can be committed.
            consumer.store_offsets(message=msg)
            consumer.commit(asynchronous=False)
    finally:
        # Close only. A `finally` that commits unconditionally would commit the
        # offset of a record whose delivery failed.
        journal.close()
        producer.flush(flush_timeout)
        consumer.close()

    write_listener_review(processor, review_path)


def _produce_and_confirm(
    producer: Producer, out_topic: str, decision: Decision, flush_timeout: float
) -> None:
    """Produce one record and wait for its delivery callback to confirm success.

    Raises on any delivery failure, so the caller never reaches the journal
    append, the offset store or the commit. The record is redelivered on the next
    run, which is the point: at-least-once, absorbed by `event_id` dedup.
    """
    result: Dict[str, Any] = {}

    def _on_delivery(err, _msg) -> None:
        result["fired"] = True
        result["err"] = err

    producer.produce(
        out_topic,
        key=decision.out_key,
        value=decision.out_value,
        on_delivery=_on_delivery,
    )
    remaining = producer.flush(flush_timeout)
    if remaining:
        raise RuntimeError(
            f"{remaining} message(s) still queued for '{out_topic}' after "
            f"{flush_timeout:.0f}s; refusing to commit an unconfirmed offset"
        )
    if not result.get("fired"):
        raise RuntimeError(
            f"no delivery callback fired for '{out_topic}'; refusing to commit an "
            "unconfirmed offset"
        )
    if result["err"] is not None:
        raise RuntimeError(
            f"delivery to '{out_topic}' failed: {result['err']}; the input offset "
            "is neither stored nor committed, so the record will be redelivered"
        )
