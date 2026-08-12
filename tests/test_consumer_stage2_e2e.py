"""Consumer 2 observed against a running broker, not simulated in process.

`tests/test_consumer_stage2.py` drives `Stage2Processor` and `replay_records`
directly and proves the arithmetic. The guarantees this phase *claims* are about
a running consumer -- a real 3-partition topic, records keyed by `track_id`, a
real subscription, real committed offsets and a review queue on disk. Those are
only true if they are observed, so every test in this file runs
`src/consumer_stage2.py` as a subprocess and reads what actually crossed the wire.

Two deliberate choices, each of which a later reader would otherwise undo.

1. EVERY TEST CREATES ITS OWN TOPIC. `track-activity` accumulates across runs and
   holds the full-scale stream, and every assertion here is an exact count. A
   throwaway uuid4-suffixed topic is what keeps this file from ever reading or
   writing the contract's real topics.

2. THE HELPERS ARE LOCAL COPIES, NOT IMPORTS. `tests/` has no `__init__.py`, so
   there is no package to import a sibling test module's helpers through. These
   are deliberately the small versions of `test_consumer_stage1_e2e.py`'s
   `_admin` / `_throwaway_topic` / `_run_consumer` / `_parse_summary`.

No count, threshold, track id or timestamp is typed into an assertion. Every
expected number comes from `tests/fixtures/expected_flags.json` or from
`config/thresholds.fixture.json`.

Requires the broker: docker compose up -d
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
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

REPO_ROOT = Path(__file__).resolve().parents[1]
BROKER = "localhost:9092"

CONSUMER_SCRIPT = REPO_ROOT / "src" / "consumer_stage2.py"
FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "play_events_fixture.jsonl"
ORACLE_PATH = REPO_ROOT / "tests" / "fixtures" / "expected_flags.json"

# The same width `src/create_topics.py` uses for the contract's own topics.
PARTITIONS = 3

DRAIN_TIMEOUT_SECONDS = 30.0
SUBPROCESS_TIMEOUT_SECONDS = 240.0
TOPIC_READY_TIMEOUT_SECONDS = 30.0

ORACLE: Dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))
FIXTURE_THRESHOLDS_PATH = REPO_ROOT / ORACLE["thresholds_path"]


# --- fixture facts, read rather than typed -------------------------------
def _fixture_records() -> List[Dict[str, Any]]:
    """Every fixture line as a parsed dict, alongside its exact bytes."""
    return [
        json.loads(line)
        for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _burst_lines() -> List[bytes]:
    """The oracle's Topology B burst: its track, inside its recorded hour.

    The hour is compared as a string prefix on the contract's own wire format --
    `2026-08-09T20:` -- rather than by re-deriving a bucket, so this selection
    cannot silently agree with a bucketing bug in the code under test.
    """
    recorded = ORACLE["topology_b"]
    hour_prefix = recorded["window_start"][: len("YYYY-MM-DDTHH")]
    return [
        json.dumps(record, separators=(",", ":")).encode("utf-8")
        for record in _fixture_records()
        if record["track_id"] == recorded["track_id"]
        and record["event_time"].startswith(hour_prefix)
    ]


# --- topics ---------------------------------------------------------------
def _admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": BROKER})


def _throwaway_topic() -> str:
    """Create a fresh 3-partition topic and wait for it in cluster metadata."""
    name = f"e2e-track-activity-c2-{uuid.uuid4().hex[:12]}"
    admin = _admin()
    futures = admin.create_topics(
        [NewTopic(name, num_partitions=PARTITIONS, replication_factor=1)]
    )
    for future in futures.values():
        future.result(timeout=TOPIC_READY_TIMEOUT_SECONDS)

    deadline = time.monotonic() + TOPIC_READY_TIMEOUT_SECONDS
    while True:
        metadata = admin.list_topics(timeout=10)
        topic = metadata.topics.get(name)
        if topic is not None and len(topic.partitions) == PARTITIONS:
            break
        assert time.monotonic() < deadline, (
            f"topic {name!r} did not reach {PARTITIONS} partitions in cluster "
            f"metadata within {TOPIC_READY_TIMEOUT_SECONDS:.0f}s -- is "
            "`docker compose up -d` running?"
        )
        time.sleep(0.5)
    return name


def _delete_topic(name: str) -> None:
    """Best effort. A cleanup failure must never mask a real assertion."""
    try:
        for future in _admin().delete_topics([name]).values():
            try:
                future.result(timeout=TOPIC_READY_TIMEOUT_SECONDS)
            except Exception:  # noqa: BLE001 -- cleanup only
                pass
    except Exception:  # noqa: BLE001 -- cleanup only
        pass


@pytest.fixture()
def track_topic():
    """A throwaway 3-partition `track-activity`-shaped topic, deleted afterwards."""
    name = _throwaway_topic()
    try:
        yield name
    finally:
        _delete_topic(name)


# --- driving the input ----------------------------------------------------
def _produce_keyed_by_track(topic: str, lines: List[bytes]) -> None:
    """Put records on `topic` KEYED BY `track_id`, as Consumer 1 emits them.

    Consumer 1 is not shelled out to here: this file's boundary is
    `track-activity` -> Consumer 2, so what matters is the record SHAPE on that
    topic, which the contract fixes. Phase 3 already proved Consumer 1 produces
    it.
    """
    producer = Producer(
        {"bootstrap.servers": BROKER, "acks": "all", "client.id": "e2e-c2"}
    )
    failures: List[Any] = []

    def _on_delivery(err, _msg) -> None:
        if err is not None:
            failures.append(err)

    for value in lines:
        record = json.loads(value)
        producer.produce(
            topic,
            key=record["track_id"].encode("utf-8"),
            value=value,
            on_delivery=_on_delivery,
        )
        producer.poll(0)

    remaining = producer.flush(DRAIN_TIMEOUT_SECONDS)
    assert remaining == 0, f"{remaining} record(s) never left the producer"
    assert not failures, f"delivery failures placing records: {failures}"


# --- running the consumer -------------------------------------------------
def _parse_summary(stdout: str) -> Dict[str, Any]:
    """The last `SUMMARY {json}` line on stdout.

    Tests key off these parsed counts and never off the human-readable prose
    above them, so rewording the report cannot fail a test for the wrong reason.
    """
    lines = [line for line in stdout.splitlines() if line.startswith("SUMMARY ")]
    assert lines, f"no 'SUMMARY <json>' line on consumer stdout:\n{stdout}"
    return json.loads(lines[-1][len("SUMMARY "):])


def _run_consumer(
    *,
    in_topic: str,
    review_out: Path,
    thresholds: Path = FIXTURE_THRESHOLDS_PATH,
    commit_every: int = 1,
    idle_timeout: float = 6.0,
) -> Dict[str, Any]:
    """Run `src/consumer_stage2.py` to idle and return its parsed SUMMARY."""
    argv = [
        sys.executable,
        str(CONSUMER_SCRIPT),
        "--broker",
        BROKER,
        "--group",
        f"e2e-c2-{uuid.uuid4()}",
        "--in-topic",
        in_topic,
        "--review-out",
        str(review_out),
        "--thresholds",
        str(thresholds),
        "--commit-every",
        str(commit_every),
        "--idle-timeout",
        str(idle_timeout),
    ]
    proc = subprocess.run(
        argv,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
    )
    assert proc.returncode == 0, (
        f"consumer_stage2.py exited {proc.returncode}\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    return _parse_summary(proc.stdout)


# --------------------------------------------------------------------------
def test_a_real_burst_reaches_the_review_queue_with_the_oracles_own_numbers(
    track_topic: str, tmp_path: Path
):
    """One flagged track, end to end through a running consumer (STG2-01, STG2-03).

    The oracle's Topology B burst is produced onto a real 3-partition topic keyed
    by `track_id`, the shipped script consumes it, and the review queue it wrote
    to disk carries that track with the four numbers `expected_flags.json`
    recorded for it. Every expected value is read from the oracle.
    """
    recorded = ORACLE["topology_b"]
    lines = _burst_lines()
    assert len(lines) == recorded["total_plays"], (
        "the fixture no longer holds the burst the oracle records; this test would "
        "otherwise assert against a different set of events than it names"
    )

    _produce_keyed_by_track(track_topic, lines)

    review_out = tmp_path / "track_review_queue.json"
    summary = _run_consumer(in_topic=track_topic, review_out=review_out)

    assert review_out.is_file(), "the consumer wrote no review queue"
    doc = json.loads(review_out.read_text(encoding="utf-8"))
    flagged = doc["flagged_tracks"]
    assert len(flagged) == 1, f"expected exactly one flagged window, got {flagged}"

    entry = flagged[0]
    assert entry["track_id"] == recorded["track_id"]
    assert entry["window_start"] == recorded["window_start"]
    assert entry["unique_listeners"] == recorded["unique_listeners"]
    assert entry["total_plays"] == recorded["total_plays"]
    assert entry["plays_per_listener"] == pytest.approx(recorded["plays_per_listener"])
    assert entry["band_share"] == pytest.approx(recorded["band_share"])

    # And the machine-readable line agrees with the file.
    assert summary["flagged_tracks"] == [recorded["track_id"]]
    assert summary["flagged_buckets"] == 1
    assert summary["counted"] == recorded["total_plays"]
    assert summary["duplicate_event_id"] == 0
    assert summary["key_mismatch"] == 0
    assert summary["invalid_value"] == 0
