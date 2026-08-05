# Option 3 — Film Festival Screening Demand Monitor

> Draft proposal for discussion. Follows the MSDS 682 proposal template.
> Final Canvas version must be trimmed to **≤ 550 words / 1 page**.

**Project title:** Film Festival Screening Demand Monitor

**Student name(s):** [You] & [Teammate]

**Project format:** Two-person team

**Contribution plan:** [Partner A] owns the streaming core (event schema, replay
producer, Kafka topic + key, Python consumer, occupancy metrics). [Partner B]
owns data + presentation (cached TMDB metadata, Streamlit dashboard, bounded LLM
summary, README/tests). Split ~50-50; both can explain everything.

## 1. Problem Summary

A film festival organizer needs to know which screenings are near capacity vs.
under-attended so they can move rooms, add showings, or promote. The project
ingests reservation/cancellation/check-in events per screening and computes live
occupancy metrics + a status label.

- **Who needs it:** festival operations / scheduling team.
- **Decision supported:** upsize/downsize the room, add a screening, or push promotion.

## 2. Planned Data Source and Classification

- **Data source and URL:** TMDB API (https://developer.themoviedb.org/docs) for film titles/genre/release metadata; synthetic screening events generated locally.
- **Data owner:** The Movie Database (TMDB) (metadata); us (synthetic events).
- **Classification:** Hybrid — stream processing (event-at-a-time Kafka replay, keyed, ordered, idempotent) over a batch-generated event set, plus cached batch API metadata.
- **Why:** Reservations are generated and replayed event-by-event into Kafka; film metadata is a slow cached lookup.
- **Access and limitations:** TMDB requires an API key — pull once and save as a cached fixture so the demo runs with no live key.
- **Review path:** Locally runnable minimum demo — reviewer runs producer + consumer against seeded JSONL; metadata from a cached fixture.

## 3. Architecture Sketch

```text
seeded_screening_events.jsonl
   → replay producer
   → Kafka topic: screening-events   (key = screening_id)   [realtime layer]
   → Pydantic schema validation
   → Python consumer (per-screening occupancy aggregation + rules)
   → screening_demand.json
        ├── terminal summary report
        └── Streamlit dashboard + one LLM summary            [batch / other]

cached_films.json (TMDB) → enrichment (film_id → title/genre)
```

Event fields: `event_id, screening_id, film_id, event_type (reservation |
cancellation | check_in), seat_count, event_time`.
Metrics: `current_reservations, occupancy_rate, remaining_seats,
cancellation_rate, demand_status (NEAR_CAPACITY | NORMAL | LOW_DEMAND)`.

## 4. Planned Tools and Packages

Python 3.11; `confluent-kafka`; Pydantic; `requests` (TMDB fetch + cache);
Streamlit; an LLM SDK (OpenAI) for the bounded summary; `pytest`.

## 5. Feasibility Risks and Plan

- **Minimum end-to-end result:** Seeded replay → `screening-events` → Python consumer → `screening_demand.json` + terminal report, runnable locally with no API.
- **Primary risks:** duplicate events on replay; correctly netting reservations vs. cancellations; TMDB key handling.
- **Fallback:** `event_id` dedup for idempotency; cached metadata fixture; Streamlit/LLM optional over the same JSON.
- **Milestones:** schema + seeded data → topic + producer + consumer → key/partition/offset/replay demo → cached metadata → dashboard + one LLM summary → validation, README, cleanup.

## 6. AI Element and Disclosure

- **Planned AI element:** Bounded summarization — one LLM call over the final occupancy JSON produces ≤ 3 operations recommendations.
- **Input/output boundary:** Input = final metrics JSON only; output = short text; the LLM may not change `demand_status` or invent numbers.
- **Verification:** every cited number must exist in the input; test empty input and API failure.
- **Fallback:** deterministic template sentence per flagged screening.

**Trade-off note:** the safest option with the clearest business story — occupancy
is intuitive. Slightly less "music-themed" if we want a music project.
