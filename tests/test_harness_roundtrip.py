"""Local harness round trip: the real producer -> Redpanda -> back off the wire.

These tests deliberately shell out to `src/replay_to_kafka.py` rather than
reimplementing a producer. The point is to prove *that shipped script* works
from this consolidated tree (HRNS-02), which a reimplementation would not show.

Requires the broker: docker compose up -d
"""

from __future__ import annotations

import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

from contracts.play_event_v1 import (
    TOPIC_PLAY_EVENTS,
    TOPIC_TRACK_ACTIVITY,
    PlayEventV1,
    key_matches_listener_id,
)
from src.create_topics import ensure_topics

REPO_ROOT = Path(__file__).resolve().parents[1]
BROKER = "localhost:9092"
REPLAY = REPO_ROOT / "src" / "replay_to_kafka.py"
CONSUME_TIMEOUT_SECONDS = 30.0


def _replay(limit: int) -> subprocess.CompletedProcess[str]:
    """Run the real shipped producer for `limit` events."""
    return subprocess.run(
        [sys.executable, str(REPLAY), "--limit", str(limit), "--broker", BROKER],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=300,
    )


def _fresh_consumer() -> Consumer:
    """A consumer in its own throwaway group, reading the topic from the start."""
    return Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": f"harness-test-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def _consume(consumer: Consumer, want: int, timeout: float) -> list:
    """Poll until `want` messages arrive or `timeout` elapses."""
    consumer.subscribe([TOPIC_PLAY_EVENTS])
    messages = []
    deadline = time.monotonic() + timeout
    while len(messages) < want and time.monotonic() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        messages.append(msg)
    return messages


def test_single_event_round_trip():
    """One real event: file -> shared model -> play-events -> back -> key check."""
    ensure_topics(BROKER)

    proc = _replay(1)
    assert proc.returncode == 0, (
        f"replay_to_kafka.py --limit 1 exited {proc.returncode}\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )

    consumer = _fresh_consumer()
    try:
        messages = _consume(consumer, want=1, timeout=CONSUME_TIMEOUT_SECONDS)
        assert messages, (
            f"no message arrived on '{TOPIC_PLAY_EVENTS}' from broker {BROKER} "
            f"within {CONSUME_TIMEOUT_SECONDS:.0f}s -- is `docker compose up -d` running?"
        )
        msg = messages[0]

        # The value parses under the shared, unmodified contract model.
        event = PlayEventV1.model_validate_json(msg.value())

        # The Kafka key agrees with the value's listener_id -- the check the
        # model structurally cannot make for itself.
        assert key_matches_listener_id(msg.key(), event), (
            f"key {msg.key()!r} != listener_id {event.listener_id!r}"
        )

        assert msg.topic() == TOPIC_PLAY_EVENTS
    finally:
        consumer.close()


def _summary_number(stdout: str, label: str) -> int:
    """Pull one labelled count out of the producer's final summary line.

    Keyed off the label rather than matching the whole sentence, so a wording
    change in the producer's output does not fail this test for the wrong reason.
    """
    match = re.search(rf"{label}\s+(\d+)", stdout, flags=re.IGNORECASE)
    assert match is not None, (
        f"could not find a '{label} <n>' count in replay output:\n{stdout}"
    )
    return int(match.group(1))


def test_limit_100_smoke_replays_cleanly():
    """HRNS-02: the documented 100-event smoke, against the real 45,473-event file."""
    ensure_topics(BROKER)

    proc = _replay(100)
    assert proc.returncode == 0, (
        f"replay_to_kafka.py --limit 100 exited {proc.returncode}\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )

    assert _summary_number(proc.stdout, "queued") == 100
    assert _summary_number(proc.stdout, "rejected") == 0
    assert _summary_number(proc.stdout, "delivery failures") == 0


def test_consumed_batch_holds_contract_invariants():
    """Every record off the wire validates, and every key equals its listener_id."""
    ensure_topics(BROKER)

    proc = _replay(100)
    assert proc.returncode == 0, proc.stderr

    consumer = _fresh_consumer()
    try:
        messages = _consume(consumer, want=100, timeout=CONSUME_TIMEOUT_SECONDS)
        # >= rather than == : the tracer event is already on the topic and every
        # re-run appends more, so an exact count would be flaky for a reason that
        # has nothing to do with correctness.
        assert len(messages) >= 100, (
            f"expected at least 100 records on '{TOPIC_PLAY_EVENTS}' at {BROKER}, "
            f"got {len(messages)} within {CONSUME_TIMEOUT_SECONDS:.0f}s"
        )

        for msg in messages:
            event = PlayEventV1.model_validate_json(msg.value())
            assert key_matches_listener_id(msg.key(), event), (
                f"key {msg.key()!r} != listener_id {event.listener_id!r} "
                f"(partition {msg.partition()}, offset {msg.offset()})"
            )
    finally:
        consumer.close()

    # Deliberately no assertion about event_time ordering across this batch.
    # Nondecreasing order is a property of a single replay, not of the
    # accumulated log read from earliest: each re-run starts over at
    # 2026-08-08T00:00:00Z, so run N+1's first timestamp is earlier than run N's
    # last. The guarantee is declared in data/play_events_manifest.json and
    # already verified across all 45,473 events; re-proving it here would need a
    # watermark or a throwaway topic, which is not worth the cost.


def test_both_topics_exist():
    """HRNS-01: both topics present in cluster metadata, and creating twice is safe."""
    first = ensure_topics(BROKER)
    assert TOPIC_PLAY_EVENTS in first
    assert TOPIC_TRACK_ACTIVITY in first

    # Idempotency: a second call is a no-op that still succeeds.
    second = ensure_topics(BROKER)
    assert sorted(second) == sorted(first)

    metadata = AdminClient({"bootstrap.servers": BROKER}).list_topics(timeout=10)
    for name in (TOPIC_PLAY_EVENTS, TOPIC_TRACK_ACTIVITY):
        assert name in metadata.topics, f"'{name}' missing from cluster metadata"
        assert len(metadata.topics[name].partitions) >= 3


def test_compose_declares_no_service_beyond_broker_and_console():
    """HRNS-01: the harness needs nothing but the broker (the console is optional)."""
    proc = subprocess.run(
        ["docker", "compose", "config", "--services"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr

    services = {line.strip() for line in proc.stdout.splitlines() if line.strip()}
    # Subset, not equality: HRNS-01 claims nothing *further* is required, so
    # dropping the console -- a convenience for demo screenshots, not a
    # dependency -- must not fail this test.
    assert services <= {"redpanda", "console"}, (
        f"docker-compose.yml declares unexpected service(s): "
        f"{sorted(services - {'redpanda', 'console'})}"
    )
