"""Consumer 1: per-listener state and the re-key (`src/consumer_stage1.py`).

NO THRESHOLD LITERAL AND NO ORACLE LITERAL IS TYPED INTO THIS FILE. Every
expected number comes from `tests/fixtures/expected_flags.json` or from a
`Thresholds` loaded through `src.config.load_thresholds`, following
`tests/test_fixture_trips_rules.py`. That is what makes these tests a proof that
configuration reaches the running detector rather than a restatement of the same
numbers in a second place.

The broker-backed tests create throwaway topics with a uuid4 suffix, so re-runs
never read each other's records and the shared `play-events` / `track-activity`
topics are never written by a test.

Requires the broker for the tests marked as such: docker compose up -d
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.play_event_v1 import PlayEventV1  # noqa: E402
from src import consumer_stage1  # noqa: E402
from src.config import load_thresholds  # noqa: E402
from src.consumer_stage1 import Stage1Processor  # noqa: E402

BROKER = "localhost:9092"
REPLAY = REPO_ROOT / "src" / "replay_to_kafka.py"
PARTITIONS = 3

FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "play_events_fixture.jsonl"
EXPECTED_FLAGS_PATH = REPO_ROOT / "tests" / "fixtures" / "expected_flags.json"

EXPECTED: Dict[str, Any] = json.loads(EXPECTED_FLAGS_PATH.read_text(encoding="utf-8"))
FIXTURE_LINES: List[str] = [
    line
    for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
    if line.strip()
]
FIXTURE_THRESHOLDS = load_thresholds(EXPECTED["thresholds_path"])

# The one record the tracer sends: `replay_to_kafka.py` strips each line and
# encodes it, so this is exactly the byte string that reaches `play-events`.
FIRST_FIXTURE_BYTES = FIXTURE_LINES[0].strip().encode("utf-8")
FIRST_FIXTURE_RECORD: Dict[str, Any] = json.loads(FIXTURE_LINES[0])


def _reordered_valid_bytes() -> bytes:
    """A valid PlayEventV1 whose JSON *text* a model round-trip would change.

    `event_time` first and `schema_version` last (the model declares them the
    other way round), and a space after every colon and comma (pydantic emits
    none). Field values are lifted from the fixture's first line and the oracle's
    Topology A listener so nothing about the case is invented.
    """
    return (
        '{"event_time": "%s", "event_id": "fx-bytes-001", "event_type": "play", '
        '"listener_id": "%s", "track_id": "%s", "artist_id": "%s", '
        '"played_seconds": %d, "track_duration_seconds": %d, "schema_version": 1}'
        % (
            FIRST_FIXTURE_RECORD["event_time"],
            EXPECTED["topology_a"]["listener_id"],
            FIRST_FIXTURE_RECORD["track_id"],
            FIRST_FIXTURE_RECORD["artist_id"],
            FIRST_FIXTURE_RECORD["played_seconds"],
            FIRST_FIXTURE_RECORD["track_duration_seconds"],
        )
    ).encode("utf-8")


REORDERED_VALID_BYTES = _reordered_valid_bytes()


# --------------------------------------------------------------------------------
# Broker helpers (throwaway topics only -- never the contract's real topics)
# --------------------------------------------------------------------------------
def _admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": BROKER})


def _create_throwaway_topics(*names: str) -> None:
    """Create uuid-suffixed topics at 3 partitions and wait for metadata.

    3 partitions rather than 1 because the offset-commit assertion below is
    per partition, and a single-partition topic would not exercise it.
    """
    admin = _admin()
    futures = admin.create_topics(
        [
            NewTopic(name, num_partitions=PARTITIONS, replication_factor=1)
            for name in names
        ]
    )
    for name, future in futures.items():
        future.result(timeout=30)

    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        metadata = admin.list_topics(timeout=10)
        if all(
            name in metadata.topics and len(metadata.topics[name].partitions) >= PARTITIONS
            for name in names
        ):
            return
        time.sleep(0.3)
    raise RuntimeError(
        f"throwaway topics {list(names)} did not reach {PARTITIONS} partitions in "
        f"cluster metadata at {BROKER} -- is `docker compose up -d` running?"
    )


def _delete_throwaway_topics(*names: str) -> None:
    """Best-effort cleanup so repeated runs do not litter the broker."""
    try:
        for future in _admin().delete_topics(list(names)).values():
            try:
                future.result(timeout=30)
            except Exception:  # noqa: BLE001 - cleanup must never fail a test
                pass
    except Exception:  # noqa: BLE001
        pass


def _drain(topic: str, want: int, timeout: float = 30.0) -> list:
    """Every record on `topic`, read from the start in a throwaway group.

    Polls once more after `want` records arrive, so a caller asserting "exactly
    one" is asserting it rather than assuming it.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": f"drain-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.subscribe([topic])
        messages: list = []
        deadline = time.monotonic() + timeout
        while len(messages) < want and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            messages.append(msg)
        extra = consumer.poll(2.0)
        if extra is not None and not extra.error():
            messages.append(extra)
        return messages
    finally:
        consumer.close()


def _committed_offsets(group: str, topic: str) -> Dict[int, int]:
    """The group's committed offset per partition, as the broker reports it."""
    probe = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": group,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    try:
        committed = probe.committed(
            [TopicPartition(topic, p) for p in range(PARTITIONS)], timeout=20
        )
        return {tp.partition: tp.offset for tp in committed}
    finally:
        probe.close()


def _replay_into(topic: str, limit: int, input_path: Path = FIXTURE_PATH):
    """Put `limit` events on `topic` using the SHIPPED producer, not a copy."""
    return subprocess.run(
        [
            sys.executable,
            str(REPLAY),
            "--input",
            str(input_path),
            "--topic",
            topic,
            "--limit",
            str(limit),
            "--broker",
            BROKER,
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=300,
    )


# --------------------------------------------------------------------------------
# Task 1: the tracer -- one event through every layer
# --------------------------------------------------------------------------------
def test_the_forwarded_value_is_the_original_bytes_not_a_model_round_trip():
    """Contract section 4: the value on `track-activity` is the value that arrived.

    THIS TEST HAS TO USE A HAND-BUILT INPUT, AND THE THIRD ASSERTION IS WHY.
    Measured across every line of `play_events_fixture.jsonl` and the head of
    `data/play_events.jsonl`, `model_validate_json(line).model_dump_json()` is
    byte-identical to `line`. So `forwarded == original` over fixture data passes
    even for an implementation that round-trips through the model -- a vacuous
    assertion that proves nothing about section 4. `REORDERED_VALID_BYTES` is a
    record the model accepts but whose text a round-trip normalises, so the same
    assertion discriminates.
    """
    event = PlayEventV1.model_validate_json(REORDERED_VALID_BYTES)

    # Without this the case could be an invalid record in disguise, and the test
    # below would pass for the wrong reason.
    assert event.listener_id == EXPECTED["topology_a"]["listener_id"]

    # The load-bearing line: a round trip really does change these bytes.
    assert REORDERED_VALID_BYTES != event.model_dump_json().encode("utf-8")

    processor = Stage1Processor(FIXTURE_THRESHOLDS)
    decision = processor.decide(
        event.listener_id.encode("utf-8"), REORDERED_VALID_BYTES
    )

    assert decision.forward is True
    assert decision.out_value == REORDERED_VALID_BYTES
    assert decision.out_key == event.track_id.encode("utf-8")


def test_the_tracer_path_rekeys_one_real_event_end_to_end(tmp_path):
    """One real event: play-events -> validate -> count -> track-activity -> commit.

    Needs the broker. The input event is placed by the shipped
    `src/replay_to_kafka.py`, so the producer seam is the real one.
    """
    in_topic = f"tracer-in-{uuid.uuid4().hex}"
    out_topic = f"tracer-out-{uuid.uuid4().hex}"
    _create_throwaway_topics(in_topic, out_topic)
    try:
        proc = _replay_into(in_topic, limit=1)
        assert proc.returncode == 0, (
            f"replay_to_kafka.py --limit 1 exited {proc.returncode}\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )

        group = f"stage1-tracer-{uuid.uuid4().hex}"
        review_path = tmp_path / "listener_review_queue.json"
        consumer_stage1.run(
            broker=BROKER,
            group=group,
            in_topic=in_topic,
            out_topic=out_topic,
            state_dir=tmp_path,
            thresholds=FIXTURE_THRESHOLDS,
            review_path=review_path,
            max_events=1,
        )

        # --- the re-key, on the original bytes ---------------------------------
        out_messages = _drain(out_topic, want=1)
        assert len(out_messages) == 1, (
            f"expected exactly one record on '{out_topic}', got {len(out_messages)}"
        )
        forwarded = out_messages[0]
        event = PlayEventV1.model_validate_json(FIRST_FIXTURE_BYTES)
        assert forwarded.key() == event.track_id.encode("utf-8")
        assert forwarded.value() == FIRST_FIXTURE_BYTES

        # --- the journal ------------------------------------------------------
        journal_path = tmp_path / consumer_stage1.JOURNAL_FILENAME
        assert journal_path.is_file()
        journal_lines = [
            line for line in journal_path.read_text(encoding="utf-8").splitlines() if line
        ]
        assert len(journal_lines) == 1
        assert json.loads(journal_lines[0])["event_id"] == event.event_id

        # --- the review artifact ----------------------------------------------
        assert review_path.is_file()
        review = json.loads(review_path.read_text(encoding="utf-8"))
        # One play cannot exceed a threshold of ten, and the artifact path still
        # has to be proven to run.
        assert review["flagged_listeners"] == []

        # --- the offset, committed only after the produce ---------------------
        in_messages = _drain(in_topic, want=1)
        assert len(in_messages) == 1
        landed_on = in_messages[0].partition()
        offsets = _committed_offsets(group, in_topic)
        assert offsets[landed_on] == 1, (
            f"expected committed offset 1 on partition {landed_on} of '{in_topic}', "
            f"got {offsets}"
        )
    finally:
        _delete_throwaway_topics(in_topic, out_topic)
