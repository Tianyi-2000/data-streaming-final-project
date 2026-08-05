# Option 2 — Music Festival Lineup Demand Monitor

> Draft proposal for discussion. Follows the MSDS 682 proposal template.
> Final Canvas version must be trimmed to **≤ 550 words / 1 page**.

**Project title:** Music Festival Lineup Demand Monitor

**Student name(s):** [You] & [Teammate]

**Project format:** Two-person team

**Contribution plan:** [Partner A] owns the streaming core (event schema, replay
producer, Kafka topic + key, Python consumer, demand metrics). [Partner B] owns
data + presentation (cached MusicBrainz artist metadata, Streamlit dashboard,
bounded LLM summary, README/tests). Split ~50-50; both can explain everything.

## 1. Problem Summary

A festival organizer needs to see which performances are drawing fan interest so
they can plan stages, timing, and promotion. The project ingests interest events
per performance and computes demand metrics + a status label, turning raw
interactions into a short "which acts need attention" list.

- **Who needs it:** festival organizer / booking team.
- **Decision supported:** change stage size, adjust set time, or boost promotion for an act.

## 2. Planned Data Source and Classification

- **Data source and URL:** MusicBrainz Web Service (https://musicbrainz.org/doc/MusicBrainz_API) for artist metadata; synthetic interest events generated locally.
- **Data owner:** MetaBrainz Foundation (metadata); us (synthetic events).
- **Classification:** Hybrid (deterministic batch replay into Kafka; cached API metadata).
- **Why:** Fan interest is generated and replayed event-by-event into Kafka; artist metadata is a slow cached lookup.
- **Access and limitations:** MusicBrainz requires a descriptive User-Agent and ~1 request/sec — cache metadata, don't stream it.
- **Review path:** Locally runnable minimum demo — reviewer runs producer + consumer against seeded JSONL; metadata from a cached fixture.

## 3. Architecture Sketch

```text
seeded_interest_events.jsonl
   → replay producer
   → Kafka topic: lineup-events   (key = performance_id)   [realtime layer]
   → Pydantic schema validation
   → Python consumer (per-performance demand aggregation + rules)
   → lineup_demand.json
        ├── terminal summary report
        └── Streamlit dashboard + one LLM summary           [batch / other]

cached_artists.json (MusicBrainz) → enrichment (artist_id → name/genre)
```

Event fields: `event_id, festival_id, performance_id, artist_id,
event_type (view | favorite | ticket_interest | cancellation), event_time`.
Metrics: `interest_count, favorite_rate, ticket_interest_count,
cancellation_rate, demand_status`.

## 4. Planned Tools and Packages

Python 3.11; `confluent-kafka`; Pydantic; `requests` (MusicBrainz fetch + cache);
Streamlit; an LLM SDK (OpenAI) for the bounded summary; `pytest`.

## 5. Feasibility Risks and Plan

- **Minimum end-to-end result:** Seeded replay → `lineup-events` → Python consumer → `lineup_demand.json` + terminal report, runnable locally with no API.
- **Primary risks:** duplicate events on replay; defining meaningful demand thresholds; API rate limits.
- **Fallback:** `event_id` dedup for idempotency; cached metadata fixture; Streamlit/LLM optional over the same JSON.
- **Milestones:** schema + seeded data → topic + producer + consumer → key/partition/offset/replay demo → cached metadata → dashboard + one LLM summary → validation, README, cleanup.

## 6. AI Element and Disclosure

- **Planned AI element:** Bounded summarization — one LLM call over the final demand JSON produces ≤ 3 organizer recommendations.
- **Input/output boundary:** Input = final metrics JSON only; output = short text; the LLM may not change `demand_status` or invent numbers.
- **Verification:** every cited number must exist in the input; test empty input and API failure.
- **Fallback:** deterministic template sentence per flagged performance.

**Trade-off note:** more creative than Option 1 and a clear operational decision,
but "demand status" thresholds are a bit more subjective than skip/completion rates.
