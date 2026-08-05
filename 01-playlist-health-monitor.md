# Option 1 — Playlist Health Monitor

> Draft proposal for discussion. Follows the MSDS 682 proposal template.
> Final Canvas version must be trimmed to **≤ 550 words / 1 page**.

**Project title:** Playlist Health Monitor

**Student name(s):** [You] & [Teammate]

**Project format:** Two-person team

**Contribution plan:** [Partner A] owns the streaming core (event schema, replay
producer, Kafka topic + key, Python consumer, metrics). [Partner B] owns data +
presentation (cached MusicBrainz metadata, Streamlit dashboard, bounded LLM
summary, README/tests). Both write and can explain the whole project; split ~50-50.

## 1. Problem Summary

A playlist curator needs to know which songs to keep, move, or manually review.
The project ingests play events and computes per-track health metrics
(skip rate, completion rate, save rate) and a deterministic status label so the
curator gets a short, actionable review list instead of raw logs.

- **Who needs it:** playlist curator / editorial team.
- **Decision supported:** keep a track, reposition it, reduce exposure, or flag for review.

## 2. Planned Data Source and Classification

- **Data source and URL:** MusicBrainz Web Service (https://musicbrainz.org/doc/MusicBrainz_API) for track/artist metadata; synthetic play events generated locally.
- **Data owner:** MetaBrainz Foundation (metadata); us (synthetic events).
- **Classification:** Hybrid (deterministic batch replay of seeded events into Kafka; cached API metadata).
- **Why:** Play behavior is generated and replayed one event at a time into Kafka; metadata is a slow, cached lookup.
- **Access and limitations:** MusicBrainz requires a descriptive User-Agent and ~1 request/sec — fine for small cached pulls, not a high-rate source.
- **Review path:** Locally runnable minimum demo — reviewer runs the replay producer + consumer against seeded JSONL; metadata comes from a cached fixture.

## 3. Architecture Sketch

```text
seeded_play_events.jsonl
   → replay producer
   → Kafka topic: play-events   (key = track_id)     [realtime layer]
   → Pydantic schema validation
   → Python consumer (per-track aggregation + rules)
   → playlist_health.json
        ├── terminal summary report
        └── Streamlit dashboard + one LLM summary     [batch / other components]

cached_tracks.json (MusicBrainz)  → metadata enrichment (track_id → name/artist)
```

Event fields: `event_id, play_id, listener_id, track_id, played_seconds,
track_duration_seconds, skipped, saved, event_time`.
Metrics: `play_count, skip_rate, completion_rate, save_rate,
average_played_percentage`. Example rule: `play_count >= 10 AND skip_rate >= 0.40
→ REVIEW_HIGH_SKIP`.

## 4. Planned Tools and Packages

Python 3.11; `confluent-kafka` (produce/consume); Pydantic (schema validation);
`requests` (MusicBrainz fetch + cache); Streamlit (dashboard); an LLM SDK
(OpenAI) for the bounded summary; `pytest` for tests.

## 5. Feasibility Risks and Plan

- **Minimum end-to-end result:** Seeded replay → `play-events` → Python consumer → `playlist_health.json` + terminal report, runnable locally with no API.
- **Primary risks:** duplicate events on replay; API rate limits.
- **Fallback:** idempotency via `event_id` dedup; cached metadata fixture if API is down; Streamlit/LLM are optional layers over the same JSON.
- **Milestones:** schema + seeded data → topic + producer + consumer → key/partition/offset/replay demo → cached metadata → dashboard + one LLM summary → validation, README, cleanup.

## 6. AI Element and Disclosure

- **Planned AI element:** Bounded summarization — one LLM call over the final metrics JSON produces ≤ 3 curator recommendations.
- **Input/output boundary:** Input = final metrics JSON only; output = short text; the LLM may not change `health_status` or invent numbers.
- **Verification:** check every cited number exists in the input; test empty input and API failure.
- **Fallback:** deterministic template sentence (e.g. "Track A: skip rate 47% exceeds 40% threshold — review.").

**Why this is my top pick:** clean metrics, a naturally correct key (`track_id`),
low API risk, and it's a real curator *decision tool*, not just a play counter.
