# Option 4 — Artificial Streaming Anomaly Monitor

> Draft proposal for discussion. Follows the MSDS 682 proposal template.
> Final Canvas version must be trimmed to **≤ 550 words / 1 page**.

**Project title:** Artificial Streaming Anomaly Monitor

**Student name(s):** [You] & [Teammate]

**Project format:** Two-person team

**Contribution plan:** [Partner A] owns the streaming core (event schema, replay
producer, Kafka topic + key, Python consumer, per-listener state + anomaly rules).
[Partner B] owns data + presentation (cached MusicBrainz metadata, artist exposure
rollup, Streamlit dashboard, bounded LLM summary, README/tests). Split ~50-50;
both write and can explain the whole project.

## 1. Problem Summary

Music platforms pay royalties per stream, which creates an incentive to inflate
play counts using bot accounts and automated playback farms. Platforms and
distributors publicly acknowledge detecting artificial streams and withholding the
associated royalties. The project ingests play events per listener account,
maintains per-listener behavioral state, and routes accounts with implausible
listening patterns to a **human review queue** — it never labels an account or an
artist fraudulent on its own.

- **Who needs it:** royalty-integrity / content-operations team at a streaming platform or distributor.
- **Decision supported:** open a manual review on a listener account, and see which artists have enough exposure to flagged accounts to warrant holding a payout pending that review.

## 2. Planned Data Source and Classification

- **Data source and URL:** Synthetic seeded play telemetry generated locally; MusicBrainz Web Service (https://musicbrainz.org/doc/MusicBrainz_API) for track/artist metadata.
- **Data owner:** us (synthetic events); MetaBrainz Foundation (metadata).
- **Classification:** Hybrid (deterministic batch replay of seeded events into Kafka; cached API metadata).
- **Why:** Listening behavior is generated and replayed one event at a time into Kafka; metadata is a slow, cached lookup.
- **Access and limitations:** MusicBrainz requires a descriptive User-Agent and ~1 request/sec — cache metadata, don't stream it. No real listener data is used, and none is obtainable: per-account listening history is private, which is itself the reason the behavioral signal must be synthetic.
- **Review path:** Locally runnable minimum demo — reviewer runs the replay producer + consumer against seeded JSONL; metadata comes from a cached fixture; no API key required.

## 3. Architecture Sketch

```text
seeded_play_events.jsonl   (normal listeners + seeded anomalous accounts)
   → replay producer
   → Kafka topic: play-events   (key = listener_id)   [realtime layer]
   → Pydantic schema validation
   → Python consumer (per-listener state + anomaly rules)
   → outputs:
        ├── listener_review_queue.json + terminal report
        └── artist_exposure.json → Streamlit dashboard + one LLM summary  [batch/other]

cached_tracks.json (MusicBrainz) → enrichment (track_id → title/artist)
```

**Why the key is `listener_id`:** the anomaly signal is per-account state, so every
event for one account must land on one partition and be processed in order by one
consumer. Keying by `track_id` would scatter a single account's behavior across
partitions and make the per-account rate calculations wrong. This is the central
Kafka design decision in the project, and the demo shows it explicitly —
key → partition assignment, offsets, and a replay that reproduces identical output.

Event fields: `event_id, listener_id, track_id, artist_id, played_seconds,
track_duration_seconds, client_type, event_time`.

Per-listener state: `play_count, distinct_tracks, plays_per_hour,
threshold_clustering_ratio, top_artist_share, overlap_flag, review_status`.

**Primary rule (the one we commit to shipping and testing):**
`plays_per_hour > threshold → REVIEW_IMPLAUSIBLE_RATE`. A real person cannot
plausibly start hundreds of distinct tracks per hour.

**Secondary rules, added only if time allows:**

- `threshold_clustering_ratio` — the share of a listener's plays ending in the 30–35 second window. Major platforms only count a stream toward royalties after roughly 30 seconds of playback, so automated farms cluster tightly just above that line while human listening spreads out.
- `top_artist_share` — one account sending nearly all of its plays to a single artist.
- `overlap_flag` — plays that overlap in time or arrive faster than track durations permit, which implies automation rather than a person.

Flagged listeners are rolled up into `artist_exposure.json`: per artist, how many
plays came from accounts currently in the review queue. That rollup is the output
an operations team would actually act on.

## 4. Planned Tools and Packages

Python 3.11; `confluent-kafka` (produce/consume); Pydantic (schema validation);
`requests` (MusicBrainz fetch + cache); Streamlit (dashboard); an LLM SDK (OpenAI)
for the bounded summary; `pytest` for tests.

## 5. Feasibility Risks and Plan

- **Minimum end-to-end result:** Seeded replay → `play-events` → Python consumer → `listener_review_queue.json` + `artist_exposure.json` + terminal report, runnable locally with no API.
- **Primary risks:** per-listener state is the most complex part of the build; threshold tuning; duplicate events on replay.
- **Fallback:** ship the single rate rule well-tested rather than four thresholds tuned by feel; `event_id` dedup for idempotency; cached metadata fixture if MusicBrainz is unavailable; Streamlit and the LLM summary are optional layers over the same JSON.
- **Seeded-data plan:** the generator emits a known set of normal listeners plus a known set of anomalous ones, so tests can assert exactly which accounts the consumer should flag. Detection correctness is verifiable rather than a matter of opinion.
- **Milestones:** schema + seeded data (normal + anomalous) → topic + producer + consumer → key/partition/offset/replay demo → rate rule + review queue → artist rollup → dashboard + one LLM summary → validation, README, cleanup.

## 6. AI Element and Disclosure

- **Planned AI element:** Bounded summarization — one LLM call over `listener_review_queue.json` produces ≤ 3 reviewer notes describing what a human should check.
- **Input/output boundary:** Input = the review-queue JSON only; output = short text. The LLM may not change flags, may not invent numbers, and may not assert that fraud occurred — it may only describe why an account is queued for review.
- **Verification:** every number cited in the output must exist in the input; tests cover empty input and API failure.
- **Fallback:** deterministic template sentence per flagged account (e.g. "Listener L-104: 412 plays/hour exceeds the 60 plays/hour threshold — review.").

**Ethics note:** a false positive here withholds money from an artist who did
nothing wrong, and independent artists have the least recourse when that happens.
That is why the output is a review queue and not a verdict, why the artist rollup
reports exposure rather than guilt, and why the LLM is constrained to description
instead of accusation. The human in the loop is a requirement of the problem, not
a gap in the implementation.

**Trade-off note:** strongest Kafka learning of the five options — real per-key
state, ordering that genuinely matters, and idempotent replay — with the clearest
real-world problem and a music-domain fit consistent with the other options. Costs
more implementation time than the counting/aggregation options, which is why the
plan commits to one rule and treats the rest as stretch.
