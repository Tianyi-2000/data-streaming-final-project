"""Local harness round trip: the real producer -> Redpanda -> back off the wire.

These tests deliberately shell out to `src/replay_to_kafka.py` rather than
reimplementing a producer. The point is to prove *that shipped script* works
from this consolidated tree (HRNS-02), which a reimplementation would not show.

Requires the broker: docker compose up -d
"""

from __future__ import annotations

import subprocess
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer

from contracts.play_event_v1 import (
    TOPIC_PLAY_EVENTS,
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
