"""Replay the event stream into Kafka: play_events.jsonl -> topic `play-events`.

This is the actual Kafka producer. It reads the pre-generated, pre-sorted
event file and publishes each event to the `play-events` topic keyed by
`listener_id` (UTF-8), per the contract. Events are already in nondecreasing
event_time order in the file, so file order == stream order.

Each line is re-validated against the shared PlayEventV1 contract before it is
sent; invalid lines are rejected (never published), as the contract requires.

Usage:
    # make sure the broker is up first:  docker compose up -d
    python src/replay_to_kafka.py                       # replay the whole file
    python src/replay_to_kafka.py --limit 100           # quick smoke test
    python src/replay_to_kafka.py --throttle 0.01       # ~100 events/sec (demo pace)
    python src/replay_to_kafka.py --broker localhost:9092 --topic play-events
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from confluent_kafka import Producer

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from contracts.play_event_v1 import PlayEventV1, play_events_key  # noqa: E402


def build_producer(broker: str) -> Producer:
    # linger.ms batches sends for throughput; acks=all for durability.
    return Producer(
        {
            "bootstrap.servers": broker,
            "linger.ms": 20,
            "acks": "all",
            "client.id": "play-events-replay",
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay events into Kafka.")
    parser.add_argument("--input", default=str(REPO_ROOT / "data" / "play_events.jsonl"))
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--topic", default="play-events")
    parser.add_argument("--limit", type=int, default=0, help="0 = all events")
    parser.add_argument(
        "--throttle",
        type=float,
        default=0.0,
        help="seconds to sleep between events (0 = as fast as possible)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(
            f"ERROR: {input_path} not found. Run generate_events.py first.",
            file=sys.stderr,
        )
        return 1

    producer = build_producer(args.broker)

    sent = 0
    rejected = 0
    failures = {"count": 0}

    def on_delivery(err, _msg):
        if err is not None:
            failures["count"] += 1
            if failures["count"] <= 5:
                print(f"DELIVERY FAILED: {err}", file=sys.stderr)

    print(f"Replaying {input_path.name} -> topic '{args.topic}' at {args.broker}")
    with input_path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Re-validate against the contract before sending.
            try:
                event = PlayEventV1.model_validate_json(line)
            except Exception as exc:  # pydantic.ValidationError
                rejected += 1
                if rejected <= 5:
                    print(f"REJECTED (not sent): {exc}", file=sys.stderr)
                continue

            producer.produce(
                topic=args.topic,
                key=play_events_key(event),
                value=line.encode("utf-8"),
                on_delivery=on_delivery,
            )
            sent += 1
            # Serve delivery callbacks so the internal queue doesn't fill up.
            producer.poll(0)

            if sent % 5000 == 0:
                print(f"  ...queued {sent} events")
            if args.throttle:
                time.sleep(args.throttle)
            if args.limit and sent >= args.limit:
                break

    print("Flushing remaining messages...")
    producer.flush(30)

    print(
        f"\nDone. Queued {sent} events to '{args.topic}', "
        f"rejected {rejected}, delivery failures {failures['count']}."
    )
    return 0 if failures["count"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
