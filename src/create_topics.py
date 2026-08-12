"""Create the pipeline's Kafka topics on the local Redpanda broker.

Topics are named through the shared contract module (TOPIC_PLAY_EVENTS /
TOPIC_TRACK_ACTIVITY), never string literals, so the names cannot drift from
the contract the producer and both consumers import.

Both topics are created with 3 partitions -- not 1 -- so the Phase 6 demo can
visibly show different keys landing on different partitions, which is the whole
point of the two-key pipeline.

Usage:
    # make sure the broker is up first:  docker compose up -d
    python src/create_topics.py
    python src/create_topics.py --broker localhost:9092
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from contracts.play_event_v1 import (  # noqa: E402
    TOPIC_PLAY_EVENTS,
    TOPIC_TRACK_ACTIVITY,
)

PARTITIONS = 3
REPLICATION_FACTOR = 1
TOPICS = [TOPIC_PLAY_EVENTS, TOPIC_TRACK_ACTIVITY]

# How long to wait for a freshly created topic to show up in cluster metadata.
_METADATA_TIMEOUT_SECONDS = 30.0


def _partition_counts(admin: AdminClient, topics: list[str]) -> dict[str, int]:
    """Partition count per topic as the *cluster* reports it, not as we asked."""
    metadata = admin.list_topics(timeout=10)
    return {
        name: len(metadata.topics[name].partitions)
        for name in topics
        if name in metadata.topics and metadata.topics[name].error is None
    }


def ensure_topics(broker: str = "localhost:9092") -> list[str]:
    """Idempotently create both topics; return the names confirmed on the broker.

    An already-existing topic is a success, not an error. But "it exists" is not
    enough on its own: Redpanda auto-creates a topic with a single partition the
    first time anything produces to it, and this creation path cannot widen an
    existing topic afterwards. So we read the partition count back out of cluster
    metadata and fail loudly if it is short, rather than letting the 3-partition
    intent quietly not apply until Phase 6's demo has one partition to show.
    """
    admin = AdminClient({"bootstrap.servers": broker})

    new_topics = [
        NewTopic(name, num_partitions=PARTITIONS, replication_factor=REPLICATION_FACTOR)
        for name in TOPICS
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

    # Metadata propagation is not instant right after creation.
    deadline = time.monotonic() + _METADATA_TIMEOUT_SECONDS
    counts: dict[str, int] = {}
    while time.monotonic() < deadline:
        counts = _partition_counts(admin, TOPICS)
        if all(name in counts for name in TOPICS):
            break
        time.sleep(0.5)

    missing = [name for name in TOPICS if name not in counts]
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
            "This usually means the topic was auto-created with 1 partition by an "
            "earlier stray produce. Partitions cannot be reduced, and this path "
            "cannot widen an existing topic, so the fix is to wipe the broker "
            "volume and re-create: docker compose down -v && docker compose up -d "
            "&& python src/create_topics.py"
        )

    for name in TOPICS:
        print(f"confirmed '{name}': {counts[name]} partitions")
    return list(TOPICS)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create the pipeline's Kafka topics.")
    parser.add_argument("--broker", default="localhost:9092")
    args = parser.parse_args()

    try:
        confirmed = ensure_topics(args.broker)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"\nDone. Topics present at {args.broker}: {', '.join(confirmed)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
