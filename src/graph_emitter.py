"""The graph projection: `track-activity` -> three Arango-shaped topics.

WHY THIS IS A SECOND CONSUMER AND NOT A CHANGE TO CONSUMER 2. GRPH-02 originally
said "Stage 2 emits" the three topics, and it is amended rather than obeyed
literally (`.planning/REQUIREMENTS.md`, GRPH-02, amended 2026-08-12).
`src/consumer_stage2.py` carries five phases of verification, Phase 5's PROF-02
compares its output byte for byte, and its docstring already records four
properties a later reader depends on. A graph projection is its own concern, and
Consumer 2 produces to no topic at all today -- so these three topics are a
genuinely new output rather than a redirect of an existing one. A second consumer
of `track-activity` on its own consumer group is zero-risk by construction: it
cannot change a byte of what Consumer 2 does.

Four things a later reader needs, in the order they matter.

1. THERE IS NO `event_id` DEDUP SET HERE, AND THAT IS THE INTERESTING DIFFERENCE
   FROM CONSUMER 2. Consumer 2 keeps one because its rule is a ratio --
   `plays_per_listener` at most 1.1 -- so a duplicate INFLATES the ratio and
   pushes a real burst over the ceiling, hiding fraud. A graph upserted by key is
   structurally immune to the same input: a redelivered event carries the same
   `event_id`, becomes the same edge `_key`, and overwrites itself. Idempotency
   here is a property of the write path (`src/graph_loader.py`, `overwrite_mode`
   replace) rather than of a set held in memory, which is why GRPH-04 is proven
   by running the load twice rather than by counting duplicates.

2. WHAT THIS MODULE *DOES* KEEP IS A PER-RUN VERTEX EMIT-ONCE SET, AND IT IS A
   VOLUME COMPRESSION, NOT A CORRECTNESS MECHANISM. Without it the emitter would
   produce 45,473 listener records and 45,473 track records instead of 1,308 and
   464. Every suppressed record is byte-identical to the one already emitted, so
   dropping the set changes the resulting graph in no way at all -- it only makes
   the topics twenty times larger. It is recorded here so a later reader does not
   mistake it for a correctness guard and does not delete it as dead weight.

3. THE TWO COLLECTION-NAME PREFIXES ARE THE MOST FRAGILE STRINGS IN THIS PHASE,
   SO THEY ARE DEFINED ONCE. ArangoDB does not validate that an edge's endpoints
   resolve to real documents. A wrong prefix therefore yields a graph whose
   traversals return nothing while erroring on nothing -- this project's recurring
   failure mode, the same shape as the 0.762-versus-0.923 band-share swing and
   the 901-versus-493 windowing regression. `src/graph_loader.py` IMPORTS
   `LISTENERS_COLLECTION` and `TRACKS_COLLECTION` from this module rather than
   restating them, so there is exactly one place a typo can live, and the loader
   asserts the dangling-edge count is 0 after every load rather than assuming it.

4. THE STOP-BAND BOOLEAN COMES FROM THE CONTRACT, NEVER FROM A LOCAL COMPARISON.
   `in_stop_band` is imported from `contracts/play_event_v1.py`. The 30-35 second
   edges are a semantic the producer and every consumer must agree on
   identically; Phase 1 measured what a hand-written comparison costs, and it is a
   quietly wrong number that raises nothing anywhere. A `30 <= s <= 35` written
   here would be correct today and would silently diverge the first time the
   contract moved.

5. `--max-events N` TAKES A NONDETERMINISTIC SLICE, AND THAT MATTERS THE MOMENT
   ANYONE TRIES TO PROVE IDEMPOTENCY WITH IT. `track-activity` has 3 partitions,
   and librdkafka returns whichever partition's fetch response arrives first --
   so a bounded run drains ONE partition rather than sampling across all three.
   Measured here on two fresh consumer groups reading 1,500 records each: run A
   took all 1,500 from partition 0, run B took all 1,500 from partition 1, and
   the two sets shared NOTHING. Consumer 2's `--max-events` behaves the same way;
   this is a property of a bounded read, not something this module introduces.

   The consequence is a trap. "Emit a slice, load it, emit a slice again, load
   again, and diff the counts" does NOT test idempotency -- it feeds two
   different inputs and correctly reports two different graphs. GRPH-04's claim
   is that loading the SAME input twice produces identical counts, so the input
   has to be held still: emit once, then run `src/graph_loader.py` twice on fresh
   consumer groups and diff the `graph_state` object. At FULL scale the trap
   disappears, because an unbounded run drains every partition and two runs read
   the same 45,473 records.

Usage:
    # broker up, and Consumer 1 having already filled track-activity:
    python src/graph_emitter.py
    python src/graph_emitter.py --max-events 3000      # a slice, see point 5
    python src/graph_emitter.py --group graph-emitter-full
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import ValidationError

# Same import bootstrap `src/windowing.py` and `src/config.py` use, so this
# module resolves however it is invoked and never depends on a conftest.py.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.play_event_v1 import (  # noqa: E402
    TOPIC_TRACK_ACTIVITY,
    PlayEventV1,
    in_stop_band,
)

_LOG = logging.getLogger(__name__)

# --- The graph's shape, named once ------------------------------------------
# THESE THREE NAMES ARE THE INTERFACE. `src/graph_loader.py` imports them, and
# Task 4's Kafka Connect configurations name the same collections, which is what
# makes the connector an alternative write path rather than a second
# implementation. See point 3 of the module docstring for why they are not
# repeated anywhere.
LISTENERS_COLLECTION = "listeners"
TRACKS_COLLECTION = "tracks"
PLAYED_COLLECTION = "played"

# The `_from` / `_to` prefixes, derived rather than typed, so the collection name
# and the prefix that points at it cannot disagree.
LISTENER_PREFIX = LISTENERS_COLLECTION + "/"
TRACK_PREFIX = TRACKS_COLLECTION + "/"

# --- The three output topics -------------------------------------------------
TOPIC_GRAPH_LISTENERS = "graph-listeners"
TOPIC_GRAPH_TRACKS = "graph-tracks"
TOPIC_GRAPH_PLAYED = "graph-played"
GRAPH_TOPICS = (TOPIC_GRAPH_LISTENERS, TOPIC_GRAPH_TRACKS, TOPIC_GRAPH_PLAYED)

# Matching `src/create_topics.py`. Three partitions, not one, so the graph topics
# follow the same convention as the rest of the pipeline.
PARTITIONS = 3
REPLICATION_FACTOR = 1
_METADATA_TIMEOUT_SECONDS = 30.0

# Why a record was not projected. The SAME vocabulary Consumer 1 and Consumer 2
# use, so the three stages' counters mean the same thing read side by side.
# There is no `duplicate_event_id` here on purpose -- see point 1 above.
INVALID_VALUE = "invalid_value"
KEY_MISMATCH = "key_mismatch"


@dataclass(frozen=True)
class Decision:
    """Project or drop one record. Mirrors Consumer 2's decision shape."""

    project: bool
    event: Optional[PlayEventV1]
    drop_reason: Optional[str]
    detail: str = ""


# --- Document shaping --------------------------------------------------------
# Pure functions. Every one of them is pinned by `tests/test_graph_emitter.py`,
# because these shapes are what two independent write paths both have to accept.


def listener_document(event: PlayEventV1) -> Dict[str, Any]:
    """A `listeners` vertex, keyed by `listener_id`."""
    return {"_key": event.listener_id, "listener_id": event.listener_id}


def track_document(event: PlayEventV1) -> Dict[str, Any]:
    """A `tracks` vertex, keyed by `track_id`, carrying its artist.

    THE ARTIST IS A PROPERTY OF THE TRACK HERE, NOT A THIRD VERTEX COLLECTION.
    GRPH-01 names two vertex collections and one edge collection; an `artists`
    collection would be a third, and that is scope this phase does not have.
    Carrying `artist_id` on the track keeps the artist queryable without
    inventing structure the requirement did not ask for.
    """
    return {
        "_key": event.track_id,
        "track_id": event.track_id,
        "artist_id": event.artist_id,
    }


def played_document(event: PlayEventV1) -> Dict[str, Any]:
    """A `played` edge, keyed by `event_id`, running listener -> track.

    `_from` and `_to` are built from the module's prefix constants, never from a
    literal, for the reason in point 3 of the module docstring. `stopped_in_band`
    comes from the contract helper, for the reason in point 4.
    """
    return {
        "_key": event.event_id,
        "_from": LISTENER_PREFIX + event.listener_id,
        "_to": TRACK_PREFIX + event.track_id,
        "event_id": event.event_id,
        "event_time": event.event_time,
        "played_seconds": event.played_seconds,
        "track_duration_seconds": event.track_duration_seconds,
        "artist_id": event.artist_id,
        "stopped_in_band": in_stop_band(event.played_seconds),
    }


@dataclass
class GraphProjector:
    """Decide, shape and count. No Kafka, no file I/O, no database."""

    # Point 2: a volume compression, discarded at the end of every run.
    _listeners_emitted: Set[str] = field(default_factory=set)
    _tracks_emitted: Set[str] = field(default_factory=set)

    _records_seen: int = 0
    _projected: int = 0
    _invalid_value: int = 0
    _key_mismatch: int = 0
    _listener_records: int = 0
    _track_records: int = 0
    _edge_records: int = 0

    def decide(self, key: Optional[bytes], value: Optional[bytes]) -> Decision:
        """Project or drop one record. Pure, and it returns rather than raises.

        Every parse happens inside this boundary and returns a drop decision
        rather than raising, so one malformed record cannot stop the poll loop --
        the same boundary Consumer 2 draws for the same reason.
        """
        if value is None:
            return Decision(
                project=False,
                event=None,
                drop_reason=INVALID_VALUE,
                detail="record value is null; there is nothing to validate",
            )

        try:
            event = PlayEventV1.model_validate_json(value)
        except ValidationError as exc:
            return Decision(
                project=False,
                event=None,
                drop_reason=INVALID_VALUE,
                detail=str(exc),
            )

        # `track-activity` is keyed by `track_id`, so that is what is compared --
        # the same local check Consumer 2 writes, and for the same reason: the
        # contract's `key_matches_listener_id` is listener-specific and belongs
        # to `play-events`. A missing key is a mismatch; an unkeyed record cannot
        # satisfy the rule.
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
                project=False,
                event=event,
                drop_reason=KEY_MISMATCH,
                detail=(
                    f"kafka key {key!r} does not equal the value's track_id "
                    f"{event.track_id!r}"
                ),
            )

        return Decision(project=True, event=event, drop_reason=None, detail="")

    def tally(self, decision: Decision) -> None:
        """Count one decision. Separate from `decide` so `decide` stays pure."""
        self._records_seen += 1
        if decision.project:
            self._projected += 1
        elif decision.drop_reason == INVALID_VALUE:
            self._invalid_value += 1
        elif decision.drop_reason == KEY_MISMATCH:
            self._key_mismatch += 1

    def project(self, event: PlayEventV1) -> List[Tuple[str, bytes, bytes]]:
        """The `(topic, key, value)` records one event becomes.

        Always one edge. A listener and a track vertex only the FIRST time this
        run sees each id -- point 2. Kafka keys are the UTF-8 id, matching each
        document's `_key`, so the topic partitions the same way the graph is
        keyed and a compacted replay would keep the right record.
        """
        records: List[Tuple[str, bytes, bytes]] = []

        if event.listener_id not in self._listeners_emitted:
            self._listeners_emitted.add(event.listener_id)
            self._listener_records += 1
            records.append(
                (
                    TOPIC_GRAPH_LISTENERS,
                    event.listener_id.encode("utf-8"),
                    _encode(listener_document(event)),
                )
            )

        if event.track_id not in self._tracks_emitted:
            self._tracks_emitted.add(event.track_id)
            self._track_records += 1
            records.append(
                (
                    TOPIC_GRAPH_TRACKS,
                    event.track_id.encode("utf-8"),
                    _encode(track_document(event)),
                )
            )

        self._edge_records += 1
        records.append(
            (
                TOPIC_GRAPH_PLAYED,
                event.event_id.encode("utf-8"),
                _encode(played_document(event)),
            )
        )
        return records

    def counts(self) -> Dict[str, int]:
        return {
            "records_seen": self._records_seen,
            "projected": self._projected,
            "invalid_value": self._invalid_value,
            "key_mismatch": self._key_mismatch,
            "listener_records": self._listener_records,
            "track_records": self._track_records,
            "edge_records": self._edge_records,
        }


def _encode(document: Dict[str, Any]) -> bytes:
    """Serialize a document deterministically.

    `sort_keys` is not cosmetic: two runs over the same input must produce
    byte-identical values, or a diff of the topics stops being a usable check.
    """
    return json.dumps(document, sort_keys=True).encode("utf-8")


def replay_records(
    projector: GraphProjector,
    records: Iterable[Tuple[Optional[bytes], Optional[bytes]]],
) -> List[Tuple[Decision, List[Tuple[str, bytes, bytes]]]]:
    """Decide, tally and shape a sequence of `(key, value)` pairs, with no broker.

    The same decide -> tally -> project ordering the poll loop uses, minus the
    offset discipline. This is the one place that ordering is written down for
    the no-broker path, and it is what the tests drive.
    """
    out: List[Tuple[Decision, List[Tuple[str, bytes, bytes]]]] = []
    for key, value in records:
        decision = projector.decide(key, value)
        projector.tally(decision)
        produced: List[Tuple[str, bytes, bytes]] = []
        if decision.project and decision.event is not None:
            produced = projector.project(decision.event)
        out.append((decision, produced))
    return out


# --- Topic creation ----------------------------------------------------------


def ensure_graph_topics(broker: str = "localhost:9092") -> List[str]:
    """Create the three graph topics at 3 partitions and confirm it.

    CREATE BEFORE PRODUCING, NOT AFTER. Redpanda auto-creates a topic with ONE
    partition the first time anything produces to it, and no creation path can
    widen it afterwards -- so a produce-first order silently yields topics that
    disagree with the project's 3-partition convention, and the disagreement only
    surfaces when someone looks. This mirrors `src/create_topics.py`, which is
    shipped source with a contract-derived topic list and is not edited here.
    """
    admin = AdminClient({"bootstrap.servers": broker})

    new_topics = [
        NewTopic(name, num_partitions=PARTITIONS, replication_factor=REPLICATION_FACTOR)
        for name in GRAPH_TOPICS
    ]
    for name, future in admin.create_topics(new_topics).items():
        try:
            future.result()
            print(f"created topic '{name}' ({PARTITIONS} partitions)")
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"topic '{name}' already exists")
            else:
                raise

    deadline = time.monotonic() + _METADATA_TIMEOUT_SECONDS
    counts: Dict[str, int] = {}
    while time.monotonic() < deadline:
        metadata = admin.list_topics(timeout=10)
        counts = {
            name: len(metadata.topics[name].partitions)
            for name in GRAPH_TOPICS
            if name in metadata.topics and metadata.topics[name].error is None
        }
        if all(name in counts for name in GRAPH_TOPICS):
            break
        time.sleep(0.5)

    missing = [name for name in GRAPH_TOPICS if name not in counts]
    if missing:
        raise RuntimeError(
            f"topics {missing} were not present in cluster metadata at {broker} "
            f"within {_METADATA_TIMEOUT_SECONDS:.0f}s -- is the broker up? "
            "Try: docker compose up -d"
        )

    undersized = {n: c for n, c in counts.items() if c < PARTITIONS}
    if undersized:
        raise RuntimeError(
            f"topic(s) {undersized} have fewer than {PARTITIONS} partitions. "
            "This usually means the topic was auto-created with 1 partition by "
            "an earlier stray produce. Partitions cannot be reduced, so the fix "
            "is to wipe the broker volume and re-create: docker compose down -v "
            "&& docker compose up -d && python src/create_topics.py"
        )

    for name in GRAPH_TOPICS:
        print(f"confirmed '{name}': {counts[name]} partitions")
    return list(GRAPH_TOPICS)


# --- The poll loop -----------------------------------------------------------


def _build_consumer(broker: str, group: str) -> Consumer:
    """A consumer that neither commits nor STORES an offset by itself.

    `enable.auto.offset.store` is off for the reason Consumer 2 writes down:
    librdkafka defaults it to true, which stores a record's offset the moment
    `poll()` returns it -- before this stage has validated or produced anything.
    Turning it off and storing by hand makes "the offset is committed only after
    the record's produces were confirmed" true by construction rather than by the
    accident of no code path calling `commit()` at the wrong moment.
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
            "client.id": "graph-emitter",
        }
    )


@dataclass
class _Pending:
    """One consumed message and the delivery receipts its produces are owed."""

    message: Any
    deliveries: List[Dict[str, Any]]


def _delivery_callback(receipt: Dict[str, Any]):
    def _on_delivery(err, msg):  # pragma: no cover - driven by librdkafka
        receipt["fired"] = True
        receipt["err"] = err

    return _on_delivery


def _settle(
    consumer: Consumer,
    producer: Producer,
    pending: List[_Pending],
    flush_timeout: float,
) -> None:
    """Confirm every produce in the batch, then store, then commit -- in order.

    The same ordering Consumer 1 uses between a produce and a commit, and for the
    same reason: an input offset must never be committed for a record whose
    output has not been confirmed on the broker. A delivery failure raises here,
    so nothing is stored and nothing is committed, and the record is redelivered
    on the next run. Redelivery is harmless for this stage -- a replayed event
    becomes the same edge `_key` and overwrites itself (point 1).
    """
    if not pending:
        return

    remaining = producer.flush(flush_timeout)
    if remaining:
        raise RuntimeError(
            f"{remaining} message(s) still queued for the graph topics after "
            f"{flush_timeout:.0f}s; refusing to commit an unconfirmed offset"
        )

    for entry in pending:
        for receipt in entry.deliveries:
            if not receipt.get("fired"):
                raise RuntimeError(
                    f"no delivery callback fired for '{receipt['topic']}' "
                    f"(key {receipt['key']!r}); refusing to commit an "
                    "unconfirmed offset"
                )
            if receipt["err"] is not None:
                raise RuntimeError(
                    f"delivery to '{receipt['topic']}' failed for key "
                    f"{receipt['key']!r}: {receipt['err']}; the input offset is "
                    "neither stored nor committed, so the record will be "
                    "redelivered"
                )

    for entry in pending:
        consumer.store_offsets(message=entry.message)
    consumer.commit(asynchronous=False)
    pending.clear()


@dataclass(frozen=True)
class RunSummary:
    """What one run of this stage did, in numbers a later phase can parse."""

    records_seen: int
    projected: int
    invalid_value: int
    key_mismatch: int
    listener_records: int
    track_records: int
    edge_records: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "records_seen": self.records_seen,
            "projected": self.projected,
            "invalid_value": self.invalid_value,
            "key_mismatch": self.key_mismatch,
            "listener_records": self.listener_records,
            "track_records": self.track_records,
            "edge_records": self.edge_records,
        }


def run(
    *,
    broker: str,
    group: str,
    in_topic: str,
    listeners_topic: str,
    tracks_topic: str,
    played_topic: str,
    max_events: int = 0,
    commit_every: int = 200,
    idle_timeout: float = 10.0,
    flush_timeout: float = 30.0,
    throttle: float = 0.0,
) -> RunSummary:
    """Consume `in_topic`, project each event, produce to the three graph topics."""
    if commit_every < 1:
        raise ValueError(f"commit_every must be at least 1; got {commit_every}")

    # The three topic names are parameters so a test or a second run can point
    # elsewhere, but the DOCUMENT shapes are not -- those are the interface.
    topic_for = {
        TOPIC_GRAPH_LISTENERS: listeners_topic,
        TOPIC_GRAPH_TRACKS: tracks_topic,
        TOPIC_GRAPH_PLAYED: played_topic,
    }

    projector = GraphProjector()
    consumer = _build_consumer(broker, group)
    producer = _build_producer(broker)
    consumer.subscribe([in_topic])

    pending: List[_Pending] = []
    try:
        idle_since = time.monotonic()
        while True:
            if max_events and projector.counts()["records_seen"] >= max_events:
                break

            msg = consumer.poll(1.0)
            if msg is None:
                # An idle poll is a settle point: produced work must not wait on
                # a record that may never arrive.
                _settle(consumer, producer, pending, flush_timeout)
                if time.monotonic() - idle_since >= idle_timeout:
                    break
                continue
            if msg.error():
                _LOG.warning("consumer error on %s: %s", in_topic, msg.error())
                continue

            idle_since = time.monotonic()

            decision = projector.decide(msg.key(), msg.value())
            projector.tally(decision)

            receipts: List[Dict[str, Any]] = []
            if not decision.project:
                _LOG.warning(
                    "dropped record: reason=%s partition=%s offset=%s detail=%s",
                    decision.drop_reason,
                    msg.partition(),
                    msg.offset(),
                    decision.detail,
                )
            else:
                for logical_topic, key, value in projector.project(decision.event):
                    receipt: Dict[str, Any] = {
                        "fired": False,
                        "err": None,
                        "topic": topic_for[logical_topic],
                        "key": key,
                    }
                    receipts.append(receipt)
                    producer.produce(
                        topic_for[logical_topic],
                        key=key,
                        value=value,
                        on_delivery=_delivery_callback(receipt),
                    )
                producer.poll(0)

            pending.append(_Pending(message=msg, deliveries=receipts))
            if len(pending) >= commit_every:
                _settle(consumer, producer, pending, flush_timeout)

            if throttle:
                time.sleep(throttle)

        _settle(consumer, producer, pending, flush_timeout)
    finally:
        consumer.close()

    counts = projector.counts()
    summary = RunSummary(
        records_seen=counts["records_seen"],
        projected=counts["projected"],
        invalid_value=counts["invalid_value"],
        key_mismatch=counts["key_mismatch"],
        listener_records=counts["listener_records"],
        track_records=counts["track_records"],
        edge_records=counts["edge_records"],
    )
    _report(summary, listeners_topic, tracks_topic, played_topic)
    return summary


def _report(
    summary: RunSummary,
    listeners_topic: str,
    tracks_topic: str,
    played_topic: str,
) -> None:
    """A human-readable block, then one machine-readable line, on stdout."""
    print("")
    print(
        f"Graph emitter done. Saw {summary.records_seen}, "
        f"projected {summary.projected}."
    )
    print(f"  dropped, invalid value : {summary.invalid_value}")
    print(f"  dropped, key mismatch  : {summary.key_mismatch}")
    print(f"  {listeners_topic:<16}: {summary.listener_records} records")
    print(f"  {tracks_topic:<16}: {summary.track_records} records")
    print(f"  {played_topic:<16}: {summary.edge_records} records")
    print(
        "  vertex records are deduplicated per run as a volume compression, "
        "not as a correctness mechanism"
    )
    print("SUMMARY " + json.dumps(summary.as_dict(), sort_keys=True))


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point. Mirrors the other consumers' argparse shape."""
    parser = argparse.ArgumentParser(
        description=(
            "Project track-activity into the three Arango-shaped graph topics: "
            "listener vertices, track vertices and played edges."
        )
    )
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--group", default="graph-emitter")
    parser.add_argument("--in-topic", default=TOPIC_TRACK_ACTIVITY)
    parser.add_argument("--listeners-topic", default=TOPIC_GRAPH_LISTENERS)
    parser.add_argument("--tracks-topic", default=TOPIC_GRAPH_TRACKS)
    parser.add_argument("--played-topic", default=TOPIC_GRAPH_PLAYED)
    parser.add_argument(
        "--commit-every",
        type=int,
        default=200,
        help="confirm, store and commit offsets after N decided records",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=10.0,
        help="seconds of empty polling before settling and exiting cleanly",
    )
    parser.add_argument(
        "--flush-timeout",
        type=float,
        default=30.0,
        help="seconds to wait for outstanding produces before refusing to commit",
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=0.0,
        help="seconds to sleep between records (0 = as fast as possible)",
    )
    parser.add_argument("--max-events", type=int, default=0, help="0 = unlimited")
    parser.add_argument(
        "--skip-topic-creation",
        action="store_true",
        help="assume the three graph topics already exist at 3 partitions",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )

    try:
        if not args.skip_topic_creation:
            ensure_graph_topics(args.broker)
        run(
            broker=args.broker,
            group=args.group,
            in_topic=args.in_topic,
            listeners_topic=args.listeners_topic,
            tracks_topic=args.tracks_topic,
            played_topic=args.played_topic,
            max_events=args.max_events,
            commit_every=args.commit_every,
            idle_timeout=args.idle_timeout,
            flush_timeout=args.flush_timeout,
            throttle=args.throttle,
        )
    except (RuntimeError, KafkaException, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
