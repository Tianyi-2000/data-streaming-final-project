# Option 4 — Game Player Anomaly Monitor

> Draft proposal for discussion. Follows the MSDS 682 proposal template.
> Final Canvas version must be trimmed to **≤ 550 words / 1 page**.

**Project title:** Game Player Anomaly Monitor

**Student name(s):** [You] & [Teammate]

**Project format:** Two-person team

**Contribution plan:** [Partner A] owns the streaming core (event schema, replay
producer, Kafka topic + key, Python consumer, per-player state + rules).
[Partner B] owns data + presentation (leaderboard + review-queue output,
Streamlit dashboard, bounded LLM summary, README/tests). Split ~50-50; both can
explain everything.

## 1. Problem Summary

A game operator needs to surface suspicious player behavior (impossible scores,
inhuman event frequency) for human review — without automatically labeling anyone
a cheater. The project ingests match telemetry per player, maintains a
leaderboard, and routes anomalies to a review queue.

- **Who needs it:** trust & safety / live-ops team.
- **Decision supported:** manually review a flagged player before any action.

## 2. Planned Data Source and Classification

- **Data source and URL:** Synthetic seeded telemetry generated locally; optional real read-only data from Chess.com PubAPI (https://www.chess.com/news/view/published-data-api).
- **Data owner:** us (synthetic events); Chess.com (optional public data).
- **Classification:** Hybrid (deterministic batch replay into Kafka; optional cached API data).
- **Why:** Telemetry is generated and replayed event-by-event into Kafka; any real API data is cached, not streamed.
- **Access and limitations:** Chess.com PubAPI is read-only and may return 429 under concurrent access — cache any pulls.
- **Review path:** Locally runnable minimum demo — reviewer runs producer + consumer against seeded JSONL; no API required.

## 3. Architecture Sketch

```text
seeded_match_events.jsonl
   → replay producer
   → Kafka topic: match-events   (key = player_id)   [realtime layer]
   → Pydantic schema validation
   → Python consumer (per-player state, leaderboard + anomaly rules)
   → outputs:
        ├── leaderboard.json + terminal report
        └── review-queue.json → Streamlit dashboard + one LLM summary  [batch/other]
```

Event fields: `event_id, player_id, match_id, score_change, duration_seconds,
level, event_time`.
Metrics/state: `current_score, score_per_minute, impossible_transition_flag,
event_frequency`. Example rule: `score_per_minute > threshold → REVIEW`.

## 4. Planned Tools and Packages

Python 3.11; `confluent-kafka`; Pydantic; `requests` (optional Chess.com cache);
Streamlit; an LLM SDK (OpenAI) for the bounded summary; `pytest`.

## 5. Feasibility Risks and Plan

- **Minimum end-to-end result:** Seeded replay → `match-events` → Python consumer → `leaderboard.json` + `review-queue.json` + terminal report, runnable locally with no API.
- **Primary risks:** per-player state is more complex; anomaly-rule tuning; duplicate events on replay.
- **Fallback:** start with one simple rule (score/min), add more only if time allows; `event_id` dedup for idempotency; skip the optional API entirely.
- **Milestones:** schema + seeded data → topic + producer + consumer → key/partition/offset/replay demo → anomaly rules + review queue → dashboard + one LLM summary → validation, README, cleanup.

## 6. AI Element and Disclosure

- **Planned AI element:** Bounded summarization — one LLM call over the review-queue JSON produces ≤ 3 reviewer notes.
- **Input/output boundary:** Input = review-queue JSON only; output = short text; the LLM may not change flags or invent numbers, and must not assert "cheater," only "review."
- **Verification:** every cited number must exist in the input; test empty input and API failure.
- **Fallback:** deterministic template sentence per flagged player.

**Trade-off note:** strongest Kafka learning (real per-key state and anomaly
logic), but the state and rules take more time than the demand/health options.
