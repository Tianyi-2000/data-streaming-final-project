# Data Streaming Final Project — Producer (MusicBrainz + synthetic events)

**Branch:** `producer-musicbrainz` · **Owner:** Tianyi (Partner A — producer side)

This branch delivers the **producer half** of our Kafka streaming-fraud pipeline:
a real MusicBrainz catalog + a synthetic `PlayEventV1` event stream that is
designed to trip the consumer's fraud-detection rules. PJ (Partner B) builds the
consumer that reads these events.

> **For PJ / PJ's Claude agent:** everything you need to consume is here.
> Import `contracts/play_event_v1.py` for validation, read
> `data/play_events.jsonl` (or the `play-events` topic once we wire Kafka).
> The schema and topic names below are **frozen** — do not change without a
> contract amendment agreed by both of us.

---

## Pipeline (our slice of the mandated architecture)

```
MusicBrainz API  →  catalog.json (cached, one-time)
                        │
                        ▼
        generate_events.py  →  play_events.jsonl  →  [replay → Kafka topic `play-events`]  →  PJ's consumer
        (Normal / Topology A / Topology B cohorts)      (keyed by listener_id)
```

The producer **never calls MusicBrainz at run time** — it reads the cached
`data/catalog.json`, per the contract.

---

## The contract (frozen — shared by both sides)

**`contracts/play_event_v1.py`** is the single source of truth. Both producer and
consumer import it so the two sides can never drift.

`PlayEventV1` fields:

| Field | Type | Constraint |
|---|---|---|
| `schema_version` | int | must be `1` |
| `event_id` | str | unique, stable across replay with same seed |
| `event_type` | str | must be `"play"` |
| `listener_id` | str | non-empty (also the Kafka key) |
| `track_id` | str | MusicBrainz recording ID |
| `artist_id` | str | MusicBrainz artist ID |
| `played_seconds` | int | `0 <= played_seconds <= track_duration_seconds` |
| `track_duration_seconds` | int | `> 0` |
| `event_time` | str | UTC ISO 8601, e.g. `2026-08-08T20:30:00Z`; **nondecreasing order** |

**Kafka topics:** `play-events` (producer output, key = UTF-8 `listener_id`),
`track-activity` (Consumer 1 output, key = `track_id`).

---

## What the synthetic data contains

~45k events over 3 compressed days (`--seed 7`), across three cohorts:

| Cohort | Listener IDs | Behavior | Trips rule |
|---|---|---|---|
| **Normal** | `L0000`–`L0399` | 10–60 plays/day, sleep gap, personal track pool | none |
| **Topology A** | `A000`–`A007` | 600 plays/day, 24/7, spread across catalog | **>300 plays / rolling 24h** |
| **Topology B** | `B0000`–`B0899` | 1 play each on one target track, 45-min window | **≥200 uniq listeners + ≤1.1 plays/listener + ≥60% stop 30–35s, in 1h** |

Topology A stop-times are kept **above 35s** so it never cross-triggers the
Topology B rule. Verified: normal listeners peak at 60 plays/day (well under
300), all 8 Topology-A listeners trip the 24h rule, and the Topology-B window
hits ~900 unique listeners at ~94% early-stop.

---

## How to run (producer)

```bash
pip install -r requirements.txt

# 1. Fetch the MusicBrainz catalog ONCE (needs a contact email for the API User-Agent)
python src/fetch_musicbrainz_catalog.py --contact you@example.com

# 2. Generate the validated event stream (reproducible for a given seed)
python src/generate_events.py --seed 7
```

Outputs:
- `data/catalog.json` — 500 tracks / ~57 artists, real MusicBrainz IDs + durations
- `data/play_events.jsonl` — validated events, sorted by `event_time` (committed)
- `data/play_events_manifest.json` — counts, seed, topic/key metadata

Re-running with the same `--seed` produces a **byte-identical** file.

---

## Files

```
contracts/play_event_v1.py         # shared PlayEventV1 model (import this)
src/fetch_musicbrainz_catalog.py   # one-time MusicBrainz → data/catalog.json
src/generate_events.py             # producer: catalog → play_events.jsonl
data/catalog.json                  # cached catalog
data/play_events.jsonl             # the event stream (PJ's input)
requirements.txt
```

## Status & next steps

- ✅ Producer done and verified (schema validation, rule-tripping, reproducibility).
- ⏭️ Next (in progress): local Kafka via Docker (Redpanda) + a `replay_to_kafka.py`
  script that streams `play_events.jsonl` into the `play-events` topic.
- ⏭️ PJ: consumer on `play-events` → applies Topology A/B rules → `track-activity`.
