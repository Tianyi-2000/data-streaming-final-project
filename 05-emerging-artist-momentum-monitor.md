# Option 5 — Emerging Artist Momentum Monitor

> Draft proposal for discussion. Follows the MSDS 682 proposal template.
> Final Canvas version must be trimmed to **≤ 550 words / 1 page**.

**Project title:** Emerging Artist Momentum Monitor

**Student name(s):** [You] & [Teammate]

**Project format:** Two-person team

**Contribution plan:** [Partner A] owns the streaming core (event schema, replay
producer, Kafka topic + key, Python consumer, two-batch state + growth logic).
[Partner B] owns data + presentation (cached MusicBrainz metadata, Streamlit
dashboard, bounded LLM summary, README/tests). Split ~50-50; both can explain
everything.

## 1. Problem Summary

A label's A&R team or a platform wants to spot artists whose engagement is rising
fast. The project compares a previous batch of engagement events to a current
batch per artist and outputs a momentum label so scouts get a short "who's rising"
list instead of raw counts.

- **Who needs it:** A&R / discovery team.
- **Decision supported:** which artists to watch, sign, or promote.

## 2. Planned Data Source and Classification

- **Data source and URL:** MusicBrainz Web Service (https://musicbrainz.org/doc/MusicBrainz_API) for artist metadata; synthetic engagement events generated locally.
- **Data owner:** MetaBrainz Foundation (metadata); us (synthetic events).
- **Classification:** Hybrid (deterministic batch replay into Kafka; cached API metadata).
- **Why:** Engagement is generated and replayed event-by-event into Kafka across two batches; metadata is a slow cached lookup.
- **Access and limitations:** MusicBrainz requires a descriptive User-Agent and ~1 request/sec — cache metadata, don't stream it.
- **Review path:** Locally runnable minimum demo — reviewer runs producer + consumer against seeded JSONL for both batches; metadata from a cached fixture.

## 3. Architecture Sketch

```text
seeded_engagement_events.jsonl  (batch A = previous, batch B = current)
   → replay producer
   → Kafka topic: engagement-events   (key = artist_id)   [realtime layer]
   → Pydantic schema validation
   → Python consumer (holds previous vs. current batch state + growth calc)
   → artist_momentum.json
        ├── terminal summary report
        └── Streamlit dashboard + one LLM summary            [batch / other]

cached_artists.json (MusicBrainz) → enrichment (artist_id → name/genre)
```

Event fields: `event_id, artist_id, event_type (play | save | share | search),
batch_id, event_time`.
Metrics: `previous_batch_play_count, current_batch_play_count, growth_rate,
save_rate_change, momentum_status (RISING | STABLE | DECLINING | INSUFFICIENT_DATA)`.

## 4. Planned Tools and Packages

Python 3.11; `confluent-kafka`; Pydantic; `requests` (MusicBrainz fetch + cache);
Streamlit; an LLM SDK (OpenAI) for the bounded summary; `pytest`.

## 5. Feasibility Risks and Plan

- **Minimum end-to-end result:** Seeded two-batch replay → `engagement-events` → Python consumer → `artist_momentum.json` + terminal report, runnable locally with no API.
- **Primary risks:** maintaining two batches of state correctly is the hardest part; duplicate events on replay; API rate limits.
- **Fallback:** if two-batch state is too much, drop to single-batch top-growth ranking; `event_id` dedup for idempotency; cached metadata fixture.
- **Milestones:** schema + two seeded batches → topic + producer + consumer → key/partition/offset/replay demo → previous-vs-current growth logic → dashboard + one LLM summary → validation, README, cleanup.

## 6. AI Element and Disclosure

- **Planned AI element:** Bounded summarization — one LLM call over the final momentum JSON produces ≤ 3 A&R recommendations.
- **Input/output boundary:** Input = final metrics JSON only; output = short text; the LLM may not change `momentum_status` or invent numbers.
- **Verification:** every cited number must exist in the input; test empty input and API failure.
- **Fallback:** deterministic template sentence per rising artist.

**Trade-off note:** the most creative "discovery" angle, but it needs a
cross-batch baseline (two-batch state), which is the riskiest state to get right
in a short timeline.
