"""Producer: synthetic PlayEventV1 stream -> data/play_events.jsonl.

Implements the generator spec in CONTRACT-DECISIONS.md section 6 and the
producer requirements in PRODUCER-CONSUMER-CONTRACT.md:

  * reads the cached MusicBrainz catalog (never calls the API)
  * three cohorts: Normal, Topology A (volume bot), Topology B (viral fraud)
  * every event is validated against the shared PlayEventV1 contract before
    it is written (invalid events are rejected, not emitted)
  * output is sorted into NONDECREASING event_time order
  * event_id is deterministic, so the same --seed reproduces the same file

MVP decision: we emit a replay file (JSONL), not a live Kafka publish. Each
line already carries listener_id, which is the Kafka key for `play-events`,
so PJ's replay script keys on that field.

Usage:
    python src/generate_events.py --seed 7
    python src/generate_events.py --seed 7 --catalog data/catalog.json \
        --out data/play_events.jsonl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Make the shared contract importable whether run from repo root or src/.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from contracts.play_event_v1 import PlayEventV1, format_event_time  # noqa: E402

# Stable namespace so event_id values are reproducible across runs.
EVENT_ID_NAMESPACE = uuid.UUID("11111111-2222-3333-4444-555555555555")


@dataclass
class GeneratorConfig:
    """All tunables in one place (thresholds injected, not hardcoded inline)."""

    seed: int = 7
    days: int = 3
    start_utc: str = "2026-08-08T00:00:00Z"

    # Drop non-song recordings (skits, intros) too short for the stop-time
    # bands to be meaningful. Real songs are comfortably above this.
    min_track_seconds: int = 45

    # Normal cohort -------------------------------------------------------
    normal_listeners: int = 400
    normal_plays_mean: float = 25.0
    normal_plays_sd: float = 12.0
    normal_plays_min: int = 10
    normal_plays_max: int = 60
    normal_active_hours_min: int = 8
    normal_active_hours_max: int = 14
    normal_pool_size: int = 25
    # stop-time distribution: (under 30s, 30-35s band, beyond 35s)
    normal_stop_probs: tuple[float, float, float] = (0.12, 0.06, 0.82)

    # Topology A cohort (high-volume bot) ---------------------------------
    topo_a_listeners: int = 8
    topo_a_plays_per_day: int = 600
    # Stop times must stay ABOVE 30s and out of the 30-35 band so Topology A
    # never trips the Topology B rule (contract validation requirement).
    topo_a_stop_min: int = 40

    # Topology B cohort (coordinated viral fraud) -------------------------
    topo_b_listeners: int = 900
    topo_b_window_minutes: int = 45
    topo_b_day_index: int = 1          # 2nd of the 3 days
    topo_b_window_start_hour: int = 20
    topo_b_band_ratio: float = 0.92    # 92% stop in the 30-35s band
    topo_b_min_duration: int = 60      # pick a target track at least this long

    id_prefixes: dict = field(
        default_factory=lambda: {"normal": "L", "topo_a": "A", "topo_b": "B"}
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_start(cfg: GeneratorConfig) -> datetime:
    return datetime.fromisoformat(cfg.start_utc.replace("Z", "+00:00"))


def stable_seed(seed: int, salt: str, tag: str) -> int:
    """Deterministic 64-bit RNG seed. Python's built-in hash() is randomized
    per process (PYTHONHASHSEED), so we MUST use a stable hash to keep output
    reproducible across replay runs (contract: 'stable across replay')."""
    h = hashlib.sha256(f"{seed}|{salt}|{tag}".encode()).digest()
    return int.from_bytes(h[:8], "big")


def make_event_id(listener_id: str, track_id: str, event_time: str, seq: int) -> str:
    """Deterministic + unique: uuid5 over stable inputs incl. a per-run seq."""
    return str(
        uuid.uuid5(EVENT_ID_NAMESPACE, f"{listener_id}|{track_id}|{event_time}|{seq}")
    )


def stop_seconds_from_band(rng: random.Random, band: str, duration: int) -> int:
    """Map a stop-time band label to an integer played_seconds <= duration.

    Defensive against short tracks: a band that doesn't fit inside `duration`
    falls back to a full play, so we never call randint with an empty range.
    (main() also filters out very short recordings, so this is belt-and-braces.)
    """
    if band == "under30":
        return rng.randint(0, min(29, duration))
    if band == "band30_35":
        if duration < 30:
            return duration  # track too short to reach the 30-35 band
        return rng.randint(30, min(35, duration))
    # beyond 35 -> anywhere from 36 to full length
    if duration <= 36:
        return duration
    return rng.randint(36, duration)


def pick_band(rng: random.Random, probs: tuple[float, float, float]) -> str:
    return rng.choices(["under30", "band30_35", "beyond35"], weights=probs, k=1)[0]


# ---------------------------------------------------------------------------
# Cohort generators — each yields raw event dicts (validated later)
# ---------------------------------------------------------------------------
def gen_normal(cfg: GeneratorConfig, catalog: list[dict], base: datetime):
    master = random.Random(cfg.seed)
    for i in range(cfg.normal_listeners):
        listener_id = f"{cfg.id_prefixes['normal']}{i:04d}"
        # Per-listener RNG derived from seed -> stable, independent streams.
        rng = random.Random(stable_seed(cfg.seed, "normal", listener_id))
        pool = rng.sample(catalog, k=min(cfg.normal_pool_size, len(catalog)))
        seq = 0
        for day in range(cfg.days):
            n = int(rng.gauss(cfg.normal_plays_mean, cfg.normal_plays_sd))
            n = max(cfg.normal_plays_min, min(cfg.normal_plays_max, n))
            active_hours = rng.randint(
                cfg.normal_active_hours_min, cfg.normal_active_hours_max
            )
            start_hour = rng.randint(0, 24 - active_hours)  # sleep gap outside
            for _ in range(n):
                track = rng.choice(pool)
                offset_min = rng.uniform(0, active_hours * 60)
                et = base + timedelta(
                    days=day, hours=start_hour, minutes=offset_min
                )
                band = pick_band(rng, cfg.normal_stop_probs)
                played = stop_seconds_from_band(
                    rng, band, track["track_duration_seconds"]
                )
                yield _event(listener_id, track, et, played, seq)
                seq += 1


def gen_topology_a(cfg: GeneratorConfig, catalog: list[dict], base: datetime):
    for i in range(cfg.topo_a_listeners):
        listener_id = f"{cfg.id_prefixes['topo_a']}{i:03d}"
        rng = random.Random(stable_seed(cfg.seed, "topo_a", listener_id))
        seq = 0
        for day in range(cfg.days):
            for _ in range(cfg.topo_a_plays_per_day):
                track = rng.choice(catalog)          # evenly across whole catalog
                dur = track["track_duration_seconds"]
                # 24/7, no sleep gap: uniform across the full day.
                et = base + timedelta(days=day, minutes=rng.uniform(0, 24 * 60))
                low = min(cfg.topo_a_stop_min, dur)
                played = rng.randint(low, dur)       # varied, always > 30s band
                yield _event(listener_id, track, et, played, seq)
                seq += 1


def gen_topology_b(cfg: GeneratorConfig, catalog: list[dict], base: datetime):
    rng = random.Random(stable_seed(cfg.seed, "topo_b", "target"))
    # Target a single track long enough for a real 30-35s band.
    eligible = [t for t in catalog if t["track_duration_seconds"] >= cfg.topo_b_min_duration]
    target = rng.choice(eligible or catalog)
    dur = target["track_duration_seconds"]
    window_start = base + timedelta(
        days=cfg.topo_b_day_index, hours=cfg.topo_b_window_start_hour
    )
    for i in range(cfg.topo_b_listeners):
        listener_id = f"{cfg.id_prefixes['topo_b']}{i:04d}"  # unique, 1 play each
        et = window_start + timedelta(
            minutes=rng.uniform(0, cfg.topo_b_window_minutes)
        )
        if rng.random() < cfg.topo_b_band_ratio:
            played = rng.randint(30, min(35, dur))          # early-stop signature
        else:
            played = rng.randint(min(36, dur), dur)
        yield _event(listener_id, target, et, played, 0)


def _event(listener_id: str, track: dict, et: datetime, played: int, seq: int) -> dict:
    event_time = format_event_time(et)
    return {
        "schema_version": 1,
        "event_id": make_event_id(listener_id, track["track_id"], event_time, seq),
        "event_type": "play",
        "listener_id": listener_id,
        "track_id": track["track_id"],
        "artist_id": track["artist_id"],
        "played_seconds": int(played),
        "track_duration_seconds": int(track["track_duration_seconds"]),
        "event_time": event_time,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Generate synthetic PlayEventV1 stream.")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--catalog", default=str(REPO_ROOT / "data" / "catalog.json")
    )
    parser.add_argument(
        "--out", default=str(REPO_ROOT / "data" / "play_events.jsonl")
    )
    args = parser.parse_args()

    catalog_path = Path(args.catalog)
    if not catalog_path.exists():
        print(
            f"ERROR: catalog not found at {catalog_path}.\n"
            "Run: python src/fetch_musicbrainz_catalog.py --contact you@usfca.edu",
            file=sys.stderr,
        )
        return 1

    cfg = GeneratorConfig(seed=args.seed)

    catalog_doc = json.loads(catalog_path.read_text())
    all_tracks = catalog_doc["tracks"]
    catalog = [t for t in all_tracks if t["track_duration_seconds"] >= cfg.min_track_seconds]
    dropped = len(all_tracks) - len(catalog)
    if dropped:
        print(f"Filtered out {dropped} track(s) under {cfg.min_track_seconds}s "
              f"({len(catalog)} remain).")
    if not catalog:
        print("ERROR: catalog has no usable tracks.", file=sys.stderr)
        return 1

    base = parse_start(cfg)

    # Collect all raw events from the three cohorts.
    raw = []
    raw.extend(gen_normal(cfg, catalog, base))
    raw.extend(gen_topology_a(cfg, catalog, base))
    raw.extend(gen_topology_b(cfg, catalog, base))

    # Validate against the shared contract; reject invalid before emitting.
    validated: list[PlayEventV1] = []
    rejected = 0
    for d in raw:
        try:
            validated.append(PlayEventV1.model_validate(d))
        except Exception as exc:  # pydantic.ValidationError
            rejected += 1
            if rejected <= 5:
                print(f"REJECTED event: {exc}", file=sys.stderr)
    if rejected:
        # A correct generator should never produce invalid events -> fail loud.
        print(f"ERROR: {rejected} events failed validation. Aborting.", file=sys.stderr)
        return 1

    # Nondecreasing event_time order (contract requirement). ISO 'Z' strings
    # sort lexicographically; event_id breaks ties for stable output.
    validated.sort(key=lambda e: (e.event_time, e.event_id))

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        for e in validated:
            f.write(e.model_dump_json() + "\n")

    # Manifest for PJ / reproducibility.
    counts = {
        "normal": sum(1 for e in validated if e.listener_id.startswith("L")),
        "topology_a": sum(1 for e in validated if e.listener_id.startswith("A")),
        "topology_b": sum(1 for e in validated if e.listener_id.startswith("B")),
    }
    manifest = {
        "seed": cfg.seed,
        "days": cfg.days,
        "start_utc": cfg.start_utc,
        "total_events": len(validated),
        "counts_by_cohort": counts,
        "catalog_tracks": len(catalog),
        "kafka_topic": "play-events",
        "kafka_key_field": "listener_id",
        "event_time_order": "nondecreasing",
    }
    (out_path.parent / "play_events_manifest.json").write_text(
        json.dumps(manifest, indent=2)
    )

    print(f"Wrote {len(validated)} validated events -> {out_path}")
    print(f"  by cohort: {counts}")
    print(f"  first event_time: {validated[0].event_time}")
    print(f"  last  event_time: {validated[-1].event_time}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
