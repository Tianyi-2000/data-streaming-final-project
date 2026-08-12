"""Consumer 2: per-track detection over event-time hours, and the review queue.

THE DELIVERABLE END OF THE PIPELINE. This stage reads `track-activity` keyed by
`track_id`, aggregates each track's plays into event-time hour buckets, applies
CD-5's three-condition Topology B rule with all three conditions measured
together, and writes `track_review_queue.json` plus a terminal report.

Four things a later reader needs, in the order they matter.

1. THE AGGREGATION IS A PLAIN DICT KEYED BY `(track_id, hour_bucket(event_time))`,
   AND `RollingHourlyWindows` IS DELIBERATELY NOT USED. That class is the right
   tool for Consumer 1, whose rule is a rolling 24-hour sum, and it is the wrong
   tool here. Its watermark rejects an event belonging to an hour a key has
   already passed, and Phase 3 measured what that costs at this point in the
   pipeline: all 464 tracks draw listeners from all three `play-events`
   partitions, so Consumer 1 emits a track's events in POLL order, and 26,771 of
   45,473 events arrive out of `event_time` order within their own `track_id`.
   Fed through the accumulator, 25,924 of them are dropped as late and the fraud
   window degrades from 901 unique listeners to 493 (`03-VERIFICATION.md` F1).
   That failure is silent: 493 is a plausible number and nothing raises.

   `hour_bucket()` is a pure function with no watermark and no lateness
   rejection, so nothing can be dropped and arrival order cannot change the
   answer. This works because CD-5's window is ONE hour, which is exactly one
   bucket -- Consumer 2 needs per-bucket aggregation, not a rolling sum, so the
   whole watermark mechanism is unnecessary here rather than merely inconvenient.

   Buffer-and-sort is not the alternative either: it would reintroduce a
   completeness guess for the same reason and buy nothing a stateless bucket does
   not already give. `tests/test_consumer_stage2.py` enforces the absence of the
   accumulator with an AST SCAN, not a text grep, precisely so this paragraph can
   name the class and explain why it is wrong here without invalidating the check.

2. DEDUP ON `event_id` RUNS BEFORE ANY BUCKET IS TOUCHED, AND THE FAILURE
   DIRECTION IS A FALSE NEGATIVE. CD-5's ratio condition is `plays_per_listener`
   AT MOST 1.1, and real fraud sits at exactly 1.00 -- one play per bought
   listener. A duplicate therefore does not manufacture a flag, it INFLATES the
   ratio and pushes a real burst over the ceiling, so the fraud stops being
   detected. Consumer 1 delivers at-least-once and a crash can leave up to
   `--commit-every` duplicates on `track-activity`; the documented full-scale run
   uses 200. At 901 listeners it takes ~91 duplicates to cross 1.1, and 1101 over
   901 is 1.22. So this is the direction that HIDES fraud rather than accusing the
   innocent, which is why it is a dedup set and not a comment.

3. ONE DEDUP SET, AND NO JOURNAL. Consumer 1 needs two sets because it counts at
   settle time, after a batch of produces, so a second copy of an id inside one
   unsettled batch would slip past a single set. Consumer 2 counts at DECIDE time
   and produces nothing downstream, so one set closes the window at any
   `--commit-every`. It keeps no journal for the same reason: it forwards nothing,
   its entire state is derived from the topic, and a restart re-reads from the
   committed offset and rebuilds exactly what it consumes. That is honest and
   correct for a run-to-completion consumer, and it is recorded here rather than
   left looking like an omission.

4. THIS STAGE READS NO TOPOLOGY A SIGNAL. There is no per-listener rolling count
   anywhere in this file. Computing one behind the `track_id` key would make
   CD-9's separability claim false before Phase 5 gets to assert it: the two
   topologies have to be detected by two stages on two keys, or "separable" is
   a description of the code rather than a property of it. The flagged LISTENERS
   in the terminal report are read from Consumer 1's own output file, never
   recomputed here.

A NOTE ON `topology_b_window_hours`, WHICH HALF-REOPENS PHASE 1's WARNING W4.
This design holds exactly one bucket per event-time hour and cannot honour a
wider window, so a configured value other than 1 raises at construction rather
than being silently ignored -- ignoring it would produce a confidently wrong
number, which is the failure mode this project exists to avoid. But W4 was
resolved on the grounds that `src/windowing.py::topology_b_windows` was the code
path that finally read the field, and this stage deliberately does not use that
function. So the field's only remaining behaviour in the shipped pipeline is that
guard: it CONSTRAINS this stage rather than parameterising it. That is a smaller
claim than W4's resolution assumed, and writing it down is cheaper than leaving
the next reader to discover it.

Usage:
    # broker up, and Consumer 1 having already filled track-activity:
    python src/consumer_stage2.py
    python src/consumer_stage2.py --thresholds config/thresholds.fixture.json
    python src/consumer_stage2.py --commit-every 200        # full-scale run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from confluent_kafka import Consumer, KafkaException
from pydantic import ValidationError

# Same import bootstrap `src/windowing.py` and `src/config.py` use, so this
# module resolves however it is invoked and never depends on a conftest.py.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.play_event_v1 import (  # noqa: E402
    STOP_BAND_HIGH_SECONDS,
    STOP_BAND_LOW_SECONDS,
    TOPIC_TRACK_ACTIVITY,
    PlayEventV1,
    format_event_time,
    in_stop_band,
    parse_event_time,
)
from src.config import Thresholds, load_thresholds  # noqa: E402

# THE ONLY IMPORT FROM THE WINDOWING MODULE, and the whole of point 1 above.
from src.windowing import hour_bucket  # noqa: E402

_LOG = logging.getLogger(__name__)

# The three CD-5 conditions, named once. These VALUES are the literal strings the
# oracle's `decoys.*.satisfies_conditions` and `fails_conditions` lists hold, so
# renaming one breaks `tests/test_consumer_stage2.py` on purpose rather than
# quietly changing what a review entry claims was measured.
CONDITION_MIN_UNIQUE_LISTENERS = "min_unique_listeners"
CONDITION_MAX_PLAYS_PER_LISTENER = "max_plays_per_listener"
CONDITION_MIN_BAND_SHARE = "min_band_share"

# Why a record was not counted. Same vocabulary as Consumer 1's drop reasons, so
# the two stages' counters mean the same thing when they are read side by side.
INVALID_VALUE = "invalid_value"
KEY_MISMATCH = "key_mismatch"
DUPLICATE_EVENT_ID = "duplicate_event_id"

# This design holds exactly one bucket per event-time hour.
SUPPORTED_WINDOW_HOURS = 1

# The two measured ratios are rendered to a fixed two decimals wherever they are
# spoken about in prose, so the note a human reads and the note a test parses are
# the same string.
_NOTE_DECIMALS = 2


def template_note(entry: Dict[str, Any]) -> str:
    """One deterministic sentence describing what was measured for one bucket.

    PHASE 6's SUMM-03 FALLBACK. This name and signature are a cross-phase
    contract: the LLM summary layer degrades to exactly this function when the
    model is unavailable, refuses, or returns something that fails its bound, so
    the report is never left bare. Renaming either means editing Phase 6.

    TWO PROPERTIES THE TESTS ENFORCE, both of which matter more than the wording.

    First, it introduces NO NUMBER that is not already in `entry`. Every value
    rendered here is read off the entry -- the measured numbers, their thresholds,
    the window and the stop-band edges -- and nothing is typed. That is the bound
    Phase 6's SUMM-02 inherits, and it is the only reason a generated sentence can
    be trusted alongside a JSON record a human may not read.

    Second, it REACHES NO VERDICT. It describes what was measured against what
    threshold and stops. A false positive in this project withholds money from an
    innocent artist, so the sentence must not conclude anything about the artist,
    the track or the listeners -- the review queue names candidates and the
    evidence behind them, and a human decides.
    """
    unique = entry["conditions"][CONDITION_MIN_UNIQUE_LISTENERS]
    ratio = entry["conditions"][CONDITION_MAX_PLAYS_PER_LISTENER]
    share = entry["conditions"][CONDITION_MIN_BAND_SHARE]
    low, high = entry["stop_band_seconds"]

    return (
        f"Track {entry['track_id']} in the {entry['window_hours']}-hour window "
        f"beginning {entry['window_start']}: {unique['measured']} unique "
        f"listeners ({unique['comparison']} {unique['threshold']}), "
        f"{entry['total_plays']} plays at "
        f"{ratio['measured']:.{_NOTE_DECIMALS}f} per listener "
        f"({ratio['comparison']} {ratio['threshold']:.{_NOTE_DECIMALS}f}), and a "
        f"{share['measured']:.{_NOTE_DECIMALS}f} share of plays stopping inside "
        f"the {low}-{high} second band "
        f"({share['comparison']} {share['threshold']:.{_NOTE_DECIMALS}f}). "
        "These are the measured numbers against the configured thresholds; this "
        "entry is a review candidate for a human and states no conclusion."
    )


def read_listener_review(path: Union[str, Path]) -> List[Dict[str, Any]]:
    """Consumer 1's `flagged_listeners`, or an empty list and a warning.

    STG2-04 wants the terminal report to show flagged tracks AND flagged
    listeners. The listeners are Consumer 1's answer, written by another process
    at another time, so this file may be absent, stale or truncated -- and a
    missing artifact from the other stage must not take down this stage's report
    (threat T-04-08). Absent-tolerant, and it names the path it looked at so the
    absence is legible rather than silent.
    """
    target = Path(path)
    try:
        doc = json.loads(target.read_text(encoding="utf-8"))
    except FileNotFoundError:
        _LOG.warning(
            "no listener review queue at %s; the flagged-listener half of the "
            "report will be empty. Run src/consumer_stage1.py to produce it.",
            target,
        )
        return []
    except (OSError, json.JSONDecodeError) as exc:
        _LOG.warning("could not read the listener review queue at %s: %s", target, exc)
        return []

    listeners = doc.get("flagged_listeners") if isinstance(doc, dict) else None
    if not isinstance(listeners, list):
        _LOG.warning(
            "the listener review queue at %s has no 'flagged_listeners' list", target
        )
        return []
    return listeners


@dataclass(frozen=True)
class Decision:
    """What to do with one record off `track-activity`, and why.

    There is no `out_key`/`out_value` pair as in Consumer 1: this stage is the
    end of the pipeline and forwards nothing.
    """

    count: bool
    event: Optional[PlayEventV1]
    drop_reason: Optional[str]
    detail: str = ""


@dataclass
class _BucketState:
    """One `(track_id, event-time hour)` bucket's evidence.

    Everything here is order-independent by construction: a set, two counters and
    a min/max over instants. That is the whole reason the aggregation looks
    unremarkable -- see point 1 of the module docstring.
    """

    listener_ids: set
    plays: int = 0
    plays_in_band: int = 0
    first_event_time: str = ""
    last_event_time: str = ""


class Stage2Processor:
    """Per-track, per-hour state for the Topology B rule, plus the count/drop decision.

    State is a plain `Dict[(track_id, hour), _BucketState]` built with
    `hour_bucket()`. No watermark, no lateness, no drops.
    """

    def __init__(self, thresholds: Thresholds) -> None:
        window_hours = thresholds.topology_b_window_hours
        if window_hours != SUPPORTED_WINDOW_HOURS:
            raise ValueError(
                f"topology_b_window_hours is {window_hours}, but this stage holds "
                f"exactly one bucket per event-time hour and cannot honour a wider "
                f"window; only {SUPPORTED_WINDOW_HOURS} is supported. Silently "
                "ignoring the configured value would produce a confidently wrong "
                "number, so it raises here instead. Note that this half-reopens "
                "Phase 1's warning W4: the field now CONSTRAINS this stage rather "
                "than parameterising it, because the function written to consume it "
                "(the rolling accumulator's Topology B constructor) is the one this "
                "stage deliberately does not use -- see the module docstring, point 1."
            )
        self._thresholds = thresholds
        self._buckets: Dict[Tuple[str, datetime], _BucketState] = {}
        # ONE set, not two. This stage counts at decide time and produces nothing
        # downstream, so there is no unsettled-batch window for a second set to
        # close (module docstring, point 3).
        self._seen_event_ids: set = set()
        self._tracks_seen: set = set()
        self._counts_seen = 0
        self._counts_counted = 0
        self._counts_invalid_value = 0
        self._counts_key_mismatch = 0
        self._counts_duplicate_event_id = 0

    # --- decisions --------------------------------------------------------
    def decide(self, key: Optional[bytes], value: Optional[bytes]) -> Decision:
        """Count or drop one record. Pure: no Kafka, no file I/O, no counting.

        Every parse happens inside this boundary and returns a drop decision
        rather than raising, so one malformed record cannot stop the poll loop
        (threat T-04-08).
        """
        if value is None:
            return Decision(
                count=False,
                event=None,
                drop_reason=INVALID_VALUE,
                detail="record value is null; there is nothing to validate",
            )

        try:
            event = PlayEventV1.model_validate_json(value)
        except ValidationError as exc:
            return Decision(
                count=False,
                event=None,
                drop_reason=INVALID_VALUE,
                detail=str(exc),
            )

        # The key check is written locally rather than delegated: the contract
        # module's `key_matches_listener_id` is listener-specific and belongs to
        # `play-events`, and `contracts/` is frozen for this phase. On
        # `track-activity` the key is the `track_id`, so that is what is compared.
        # A missing key is a mismatch -- an unkeyed record cannot satisfy the rule,
        # and counting it would route one track's plays into another's bucket
        # (threat T-04-01).
        key_text: Optional[str]
        if key is None:
            key_text = None
        elif isinstance(key, (bytes, bytearray)):
            try:
                key_text = bytes(key).decode("utf-8")
            except UnicodeDecodeError:
                key_text = None
        else:
            key_text = str(key)
        if key_text != event.track_id:
            return Decision(
                count=False,
                event=event,
                drop_reason=KEY_MISMATCH,
                detail=(
                    f"kafka key {key!r} does not equal the value's track_id "
                    f"{event.track_id!r}"
                ),
            )

        # BEFORE ANY BUCKET IS TOUCHED. A duplicate inflates plays-per-listener
        # toward a FALSE NEGATIVE (module docstring, point 2; threat T-04-02).
        if event.event_id in self._seen_event_ids:
            return Decision(
                count=False,
                event=event,
                drop_reason=DUPLICATE_EVENT_ID,
                detail=f"event_id {event.event_id!r} was already counted",
            )

        return Decision(count=True, event=event, drop_reason=None, detail="")

    def tally(self, decision: Decision) -> None:
        """Record one decision in the observability counters.

        Separate from `decide` so that `decide` stays pure and can be called
        twice on the same bytes in a test without moving any number.
        """
        self._counts_seen += 1
        if decision.count:
            self._counts_counted += 1
        elif decision.drop_reason == INVALID_VALUE:
            self._counts_invalid_value += 1
        elif decision.drop_reason == KEY_MISMATCH:
            self._counts_key_mismatch += 1
        elif decision.drop_reason == DUPLICATE_EVENT_ID:
            self._counts_duplicate_event_id += 1

    # --- counting ---------------------------------------------------------
    def count_event(self, event: PlayEventV1) -> None:
        """Fold one event into its `(track_id, event-time hour)` bucket.

        The bucket key is taken from the VALUE -- `event.track_id` and
        `hour_bucket(event.event_time)` -- never from the Kafka key and never
        from arrival time (threat T-04-01, CD-7).
        """
        self._seen_event_ids.add(event.event_id)
        self._tracks_seen.add(event.track_id)

        key = (event.track_id, hour_bucket(event.event_time))
        state = self._buckets.get(key)
        if state is None:
            state = _BucketState(
                listener_ids=set(),
                first_event_time=event.event_time,
                last_event_time=event.event_time,
            )
            self._buckets[key] = state
        else:
            # Compare instants, store the contract strings: the review document
            # quotes the wire format a reader can match back to the stream. min/max
            # rather than first-seen/last-seen, so arrival order cannot reach these.
            moment = parse_event_time(event.event_time)
            if moment < parse_event_time(state.first_event_time):
                state.first_event_time = event.event_time
            if moment > parse_event_time(state.last_event_time):
                state.last_event_time = event.event_time

        state.listener_ids.add(event.listener_id)
        state.plays += 1
        # `in_stop_band` from the contract module, never a local numeric
        # comparison: the 30 and 35 edges are inclusive and a hand-written
        # comparison would let this detector, the fixture and the producer diverge
        # on a 16-point swing that raises no error anywhere (threat T-04-04).
        if in_stop_band(event.played_seconds):
            state.plays_in_band += 1

    # --- the Topology B judgment -----------------------------------------
    def evaluate_all(self) -> List[Dict[str, Any]]:
        """Every bucket measured against all three conditions. PURE MEASUREMENT.

        This function attaches no note and reaches no conclusion beyond
        `flagged`; `review_document` is the single place the template note is
        added, so the schema Phase 5 compares and Phase 6 reads has exactly one
        producer.

        ALL THREE CONDITIONS ARE BUILT FOR EVERY BUCKET, FLAGGED OR NOT, and
        `flagged` is `all(...)` over the three already-computed results. There is
        deliberately no early exit: a reviewer has to be able to see WHICH
        condition saved an unflagged bucket, and the decoy tests read all three
        measured numbers off buckets that were not flagged.
        """
        entries: List[Dict[str, Any]] = []
        for (track_id, hour), state in sorted(
            self._buckets.items(), key=lambda item: (item[0][0], item[0][1])
        ):
            unique_listeners = len(state.listener_ids)
            total_plays = state.plays
            plays_per_listener = total_plays / unique_listeners
            band_share = state.plays_in_band / total_plays

            conditions = {
                CONDITION_MIN_UNIQUE_LISTENERS: {
                    "measured": unique_listeners,
                    "threshold": self._thresholds.topology_b_min_unique_listeners,
                    "comparison": "at least",
                    "satisfied": (
                        unique_listeners
                        >= self._thresholds.topology_b_min_unique_listeners
                    ),
                },
                CONDITION_MAX_PLAYS_PER_LISTENER: {
                    "measured": plays_per_listener,
                    "threshold": self._thresholds.topology_b_max_plays_per_listener,
                    "comparison": "at most",
                    "satisfied": (
                        plays_per_listener
                        <= self._thresholds.topology_b_max_plays_per_listener
                    ),
                },
                CONDITION_MIN_BAND_SHARE: {
                    "measured": band_share,
                    "threshold": self._thresholds.topology_b_min_band_share,
                    "comparison": "at least",
                    "satisfied": (
                        band_share >= self._thresholds.topology_b_min_band_share
                    ),
                },
            }

            entries.append(
                {
                    "track_id": track_id,
                    "window_start": format_event_time(hour),
                    "window_hours": self._thresholds.topology_b_window_hours,
                    "unique_listeners": unique_listeners,
                    "total_plays": total_plays,
                    "plays_per_listener": plays_per_listener,
                    "band_share": band_share,
                    "plays_in_band": state.plays_in_band,
                    "stop_band_seconds": [
                        STOP_BAND_LOW_SECONDS,
                        STOP_BAND_HIGH_SECONDS,
                    ],
                    "first_event_time": state.first_event_time,
                    "last_event_time": state.last_event_time,
                    "conditions": conditions,
                    "flagged": all(
                        condition["satisfied"] for condition in conditions.values()
                    ),
                }
            )
        return entries

    def flagged_buckets(self) -> List[Dict[str, Any]]:
        """The `evaluate_all()` entries on which all three conditions held."""
        return [entry for entry in self.evaluate_all() if entry["flagged"]]

    def counts(self) -> Dict[str, int]:
        """What this run saw, counted and dropped, plus the shape of its state."""
        return {
            "records_seen": self._counts_seen,
            "counted": self._counts_counted,
            "invalid_value": self._counts_invalid_value,
            "key_mismatch": self._counts_key_mismatch,
            "duplicate_event_id": self._counts_duplicate_event_id,
            "tracks_seen": len(self._tracks_seen),
            "buckets_seen": len(self._buckets),
            "flagged_buckets": len(self.flagged_buckets()),
        }

    def review_document(self) -> Dict[str, Any]:
        """The track-side review queue: which tracks, and on what evidence.

        THE `note` IS ATTACHED HERE, NOT IN `evaluate_all()`. Both placements
        would emit an identical schema, so this is a choice rather than a
        correctness question -- but that schema is Phase 5's PROF-02 comparison
        target and Phase 6's LLM input, so it gets ONE producer and one
        documented home. `evaluate_all()` stays a pure measurement that the decoy
        tests read without a note attached; this method copies each flagged entry
        and adds its note.

        Every entry carries all three measured numbers, all three thresholds, the
        window, the stop-band edges and the config file behind them, so a named
        artist can dispute a flag on its own evidence (threat T-04-06). The
        project's stated ethical failure is accusing an innocent artist, so this
        is a queue of review candidates for a human, not a verdict.

        `thresholds_source_path` comes from `Thresholds.source_path`, which the
        loader sets AFTER parsing, so a value inside a config file cannot spoof
        which config a run actually used (threat T-04-07).
        """
        return {
            "flagged_tracks": [
                {**entry, "note": template_note(entry)}
                for entry in self.flagged_buckets()
            ],
            "counts": self.counts(),
            "thresholds_source_path": self._thresholds.source_path,
            "rule": (
                "topology_b: within one event-time hour on one track, all three of "
                "min_unique_listeners (at least), max_plays_per_listener (at most) "
                "and min_band_share (at least) must hold TOGETHER"
            ),
            "posture": (
                "these are review candidates carrying the numbers behind each flag, "
                "not a verdict: nothing here concludes that fraud occurred"
            ),
        }


def replay_records(
    processor: Stage2Processor,
    records: Iterable[Tuple[Optional[bytes], Optional[bytes]]],
) -> List[Decision]:
    """Decide, tally and count a sequence of `(key, value)` pairs, with no broker.

    The same decide -> tally -> count ordering the poll loop uses, minus the
    offset discipline. This is the one place that ordering is written down for
    the no-broker path; the tests and Phase 5 drive it.
    """
    decisions: List[Decision] = []
    for key, value in records:
        decision = processor.decide(key, value)
        processor.tally(decision)
        decisions.append(decision)
        if decision.count:
            processor.count_event(decision.event)
    return decisions


def write_review_queue(
    processor: Stage2Processor, path: Union[str, Path]
) -> Dict[str, Any]:
    """Serialize the review document deterministically and return it.

    `sort_keys=True` and a fixed indent are not cosmetic: Phase 5's PROF-02
    compares two runs' review queues byte for byte, which is only possible if
    serialization is deterministic.
    """
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
    deduplicated or counted anything. With that default in place, "the offset is
    committed only after the record has been counted" would rest on nothing but
    the fact that no code path happened to call `commit()` at the wrong moment.
    Turning the store off and doing it by hand makes the guarantee true by
    construction rather than by luck.
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


@dataclass(frozen=True)
class RunSummary:
    """What one run of this stage did, in numbers a later phase can parse."""

    polled: int
    counted: int
    invalid_value: int
    key_mismatch: int
    duplicate_event_id: int
    tracks_seen: int
    buckets_seen: int
    flagged_buckets: int
    flagged_tracks: List[str]
    # STG2-04's other half, read from Consumer 1's output rather than recomputed
    # here -- computing a Topology A signal behind the `track_id` key would make
    # CD-9's separability claim false.
    flagged_listeners: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "polled": self.polled,
            "counted": self.counted,
            "invalid_value": self.invalid_value,
            "key_mismatch": self.key_mismatch,
            "duplicate_event_id": self.duplicate_event_id,
            "tracks_seen": self.tracks_seen,
            "buckets_seen": self.buckets_seen,
            "flagged_buckets": self.flagged_buckets,
            "flagged_tracks": list(self.flagged_tracks),
            "flagged_listeners": self.flagged_listeners,
        }


def _settle(consumer: Consumer, pending: List[Any]) -> None:
    """Store and commit the batch's offsets -- strictly the offset half.

    Counting already happened in the poll loop, so by the time a message reaches
    this list it has been decided, tallied and (if kept) folded into its bucket.
    That ordering is the guarantee: an offset is stored only after the record it
    points at has been counted, and only a stored offset can be committed,
    because the automatic store is off.

    There is no delivery callback to await and no journal to flush; this stage
    produces nothing downstream.
    """
    if not pending:
        return
    for msg in pending:
        consumer.store_offsets(message=msg)
    consumer.commit(asynchronous=False)
    pending.clear()


def run(
    *,
    broker: str,
    group: str,
    in_topic: str,
    thresholds: Thresholds,
    review_path: Union[str, Path],
    listener_review_path: Union[str, Path],
    max_events: int = 0,
    commit_every: int = 1,
    idle_timeout: float = 10.0,
    throttle: float = 0.0,
) -> RunSummary:
    """Consume `in_topic`, aggregate per track per event-time hour, write the queue."""
    if commit_every < 1:
        raise ValueError(f"commit_every must be at least 1; got {commit_every}")

    processor = Stage2Processor(thresholds)
    consumer = _build_consumer(broker, group)
    consumer.subscribe([in_topic])

    pending: List[Any] = []
    polled = 0
    try:
        idle_since = time.monotonic()
        while True:
            if max_events and polled >= max_events:
                break

            msg = consumer.poll(1.0)
            if msg is None:
                # An idle poll is a settle point: counted work must not wait for a
                # record that may never arrive.
                _settle(consumer, pending)
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

            if not decision.count:
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
                processor.count_event(decision.event)

            pending.append(msg)
            if len(pending) >= commit_every:
                _settle(consumer, pending)

            if throttle:
                time.sleep(throttle)

        _settle(consumer, pending)
    finally:
        consumer.close()

    counts = processor.counts()
    flagged_listeners = read_listener_review(listener_review_path)
    summary = RunSummary(
        polled=counts["records_seen"],
        counted=counts["counted"],
        invalid_value=counts["invalid_value"],
        key_mismatch=counts["key_mismatch"],
        duplicate_event_id=counts["duplicate_event_id"],
        tracks_seen=counts["tracks_seen"],
        buckets_seen=counts["buckets_seen"],
        flagged_buckets=counts["flagged_buckets"],
        flagged_tracks=sorted(
            {entry["track_id"] for entry in processor.flagged_buckets()}
        ),
        flagged_listeners=len(flagged_listeners),
    )

    write_review_queue(processor, review_path)
    _report(summary, processor, review_path, listener_review_path)
    return summary


def _report(
    summary: RunSummary,
    processor: Stage2Processor,
    review_path: Union[str, Path],
    listener_review_path: Union[str, Path],
) -> None:
    """A human-readable block, then one machine-readable line, on stdout.

    Both halves of STG2-04 are here: the flagged TRACKS this stage measured, with
    the numbers behind each flag, and the flagged LISTENERS Consumer 1 found,
    read from its output file. When that file is absent the report says which
    path it looked at and continues.
    """
    print("")
    print(f"Consumer 2 done. Polled {summary.polled}, counted {summary.counted}.")
    print(f"  dropped, invalid value : {summary.invalid_value}")
    print(f"  dropped, key mismatch  : {summary.key_mismatch}")
    print(f"  dropped, duplicate id  : {summary.duplicate_event_id}")
    print(f"  tracks seen            : {summary.tracks_seen}")
    print(f"  event-time hour buckets: {summary.buckets_seen}")
    print(f"  flagged windows        : {summary.flagged_buckets}")
    for entry in processor.flagged_buckets():
        print(f"      {entry['track_id']} @ {entry['window_start']} "
              f"({entry['window_hours']}h)")
        for name in (
            CONDITION_MIN_UNIQUE_LISTENERS,
            CONDITION_MAX_PLAYS_PER_LISTENER,
            CONDITION_MIN_BAND_SHARE,
        ):
            condition = entry["conditions"][name]
            print(
                f"          {name}: measured {condition['measured']}, "
                f"{condition['comparison']} {condition['threshold']}"
            )
        print(f"          {template_note(entry)}")

    flagged_listeners = read_listener_review(listener_review_path)
    print(f"  flagged listeners      : {len(flagged_listeners)}")
    if flagged_listeners:
        for listener in flagged_listeners:
            print(
                f"      {listener.get('listener_id')}: peak "
                f"{listener.get('peak_plays_in_window')} plays in "
                f"{listener.get('window_hours')}h, over "
                f"{listener.get('threshold')}"
            )
    else:
        print(
            f"      none read from {listener_review_path} -- run "
            "src/consumer_stage1.py to produce it"
        )
    print(f"  review queue           : {review_path}")
    print("SUMMARY " + json.dumps(summary.as_dict(), sort_keys=True))


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point. Mirrors `src/consumer_stage1.py`'s argparse shape."""
    parser = argparse.ArgumentParser(
        description=(
            "Consumer 2: aggregate track-activity per track per event-time hour, "
            "apply the three-condition Topology B rule, write the review queue."
        )
    )
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--group", default="consumer-stage2")
    parser.add_argument("--in-topic", default=TOPIC_TRACK_ACTIVITY)
    parser.add_argument(
        "--review-out",
        default=str(REPO_ROOT / "output" / "track_review_queue.json"),
    )
    parser.add_argument(
        "--listener-review",
        default=str(REPO_ROOT / "output" / "listener_review_queue.json"),
        help=(
            "Consumer 1's review output, read at report time for the "
            "flagged-listener half of STG2-04. Absent-tolerant."
        ),
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
        help="store and commit offsets after N decided records (1 = strictest)",
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
            thresholds=thresholds,
            review_path=args.review_out,
            listener_review_path=args.listener_review,
            max_events=args.max_events,
            commit_every=args.commit_every,
            idle_timeout=args.idle_timeout,
            throttle=args.throttle,
        )
    except (RuntimeError, KafkaException, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
