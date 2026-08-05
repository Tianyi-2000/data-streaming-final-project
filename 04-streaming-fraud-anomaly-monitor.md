# Option 4 — Artificial Streaming Anomaly Monitor

> Draft proposal for discussion. Follows the MSDS 682 proposal template.
> Final Canvas version must be trimmed to **≤ 550 words / 1 page**.
> Design evidence: [RESEARCH-artificial-streaming-fraud.md](RESEARCH-artificial-streaming-fraud.md).

**Project title:** Artificial Streaming Anomaly Monitor

**Student name(s):** [You] & [Teammate]

**Project format:** Two-person team

**Contribution plan:** [Partner A] owns the streaming core: event schema, replay
producer, both Kafka topics, the re-key stage, and the per-track detection rules.
[Partner B] owns data and presentation: cached MusicBrainz metadata, the graph
load and cohort query, Streamlit dashboard, bounded LLM summary, README and tests.
Split ~50-50; both write and can explain the whole project.

## 1. Problem Summary

Music platforms pay royalties per stream, so inflating play counts pays. Bot farms
do it at scale: the first US criminal case in this area involved 1,040 bot accounts
producing 661,440 streams a day for seven years before it ended in a guilty plea.
Industry estimates put fraudulent streams near 10% of all streams.

The project ingests play events, flags tracks whose listening patterns are
implausible, and routes them to a **human review queue**. It never labels a track or
an artist fraudulent on its own.

- **Who needs it:** royalty-integrity / content-operations team at a streaming platform or distributor.
- **Decision supported:** open a manual review on a track, and see which listener accounts and artists that review should cover.

## 2. Planned Data Source and Classification

- **Data source and URL:** Synthetic seeded play telemetry generated locally; MusicBrainz Web Service (https://musicbrainz.org/doc/MusicBrainz_API) for track/artist metadata.
- **Data owner:** us (synthetic events); MetaBrainz Foundation (metadata).
- **Classification:** Hybrid — stream processing (event-at-a-time Kafka replay, keyed, ordered, idempotent) over a batch-generated event set, plus cached batch API metadata.
- **Why:** Listening behavior is generated and replayed one event at a time into Kafka; metadata is a slow, cached lookup.
- **Access and limitations:** MusicBrainz requires a descriptive User-Agent and ~1 request/sec, so cache metadata instead of streaming it. No real listener data is used and none is obtainable: per-account listening history is private, which is why the behavioral signal has to be synthetic.
- **Review path:** Locally runnable minimum demo. The reviewer runs the producer and both consumer stages against seeded JSONL. Metadata comes from a cached fixture; no API key required.

## 3. Architecture Sketch

```text
seeded_events.jsonl   (normal listeners + two seeded fraud patterns)
   → replay producer
   → Pydantic schema validation
   │
   ├─ STAGE 1 ─ Kafka topic: play-events        (key = listener_id)
   │     per-listener consumer: session state, per-account features
   │     → re-keys each event and produces to stage 2
   │
   ├─ STAGE 2 ─ Kafka topic: track-activity     (key = track_id)
   │     per-track consumer: distributional detection rules
   │     → track_review_queue.json + terminal report
   │
   └─ STAGE 3 ─ ArangoDB (Docker): listener → track bipartite graph
         AQL co-listening traversal → cohort_report.json
         → Streamlit dashboard + one bounded LLM summary

cached_tracks.json (MusicBrainz) → enrichment (track_id → title/artist)
```

**Why two keys.** A Kafka key decides which partition an event lands on, and a
consumer only sees its own partitions. So the key determines which questions the
pipeline can answer at all. Keying by `listener_id` answers "does this account
behave like a person?" Keying by `track_id` answers "does this track's audience make
sense?" Both kinds of fraud exist and neither key sees the other:

| Fraud pattern | Shape | Visible when keyed by |
|---|---|---|
| Few accounts, high volume, spread across many tracks | 1,040 accounts × ~636 plays/day | `listener_id` |
| Many accounts, one play each, concentrated on a track | 53,000 accounts × 1 play | `track_id` |

The second pattern is the one that matters here. Keyed by listener, all 53,000
accounts have exactly one play each and every partition looks innocent. Re-keyed by
track, the same events show 53,000 first-time listeners hitting one track with no
saves. The re-key between stages is the central design decision in the project, and
the demo shows it explicitly: partition assignment under each key, offsets, and a
replay that reproduces identical output.

Event fields: `event_id, event_type (play | save | follow | playlist_add),
listener_id, track_id, artist_id, played_seconds, track_duration_seconds,
country_code, event_time`.

**Stage 2 detection rules.** Primary rule, the one committed to shipping and testing:
`streams_per_unique_listener > threshold → REVIEW_LISTENER_RATIO`. Secondary rules
added only if time allows:

- `saves_per_stream`: bots don't save, follow, or add to playlists at human rates.
- `duration_clustering`: share of plays ending in the 30–35 second window. Platforms only count a stream toward royalties after roughly 30 seconds, so automated playback clusters just above that line while human listening spreads out.
- `country_concentration`: a sudden majority of plays from one country with no matching activity elsewhere.

**Stage 3, the graph layer.** Camouflage is why this layer exists. Farms pad their
activity with plays across unrelated artists, so flagged accounts look mostly
legitimate by volume and per-key thresholds degrade. What survives padding is the
*shape* of who listened to what. Loading listeners and tracks as vertices and plays
as edges makes a coordinated cohort visible as an unusually dense region of the
graph, a set of accounts whose listening overlaps far more than chance allows.

The query is a two-hop AQL traversal from a flagged track: inbound `played` edges to
its listener set, back out to everything else those listeners played, then a count of
how often the same listener set reappears on the same small set of tracks. High
recurrence is a cohort. Pregel community detection is an optional deeper pass if
time allows, but the traversal alone is enough to demonstrate the idea.

## 4. Planned Tools and Packages

Python 3.11; `confluent-kafka` (produce/consume, both stages); Pydantic (schema
validation); `python-arango` against an `arangodb/arangodb` Docker container;
`requests` (MusicBrainz fetch + cache); Streamlit (dashboard); an LLM SDK (OpenAI)
for the bounded summary; `pytest` for tests.

Writing to ArangoDB with `python-arango` directly from the stage 2 consumer keeps
the moving parts down. The production shape would be a Kafka sink connector instead,
which is worth naming in the writeup but not worth standing up for a project this
size.

## 5. Feasibility Risks and Plan

- **Minimum end-to-end result:** Seeded replay → `play-events` → re-key → `track-activity` → `track_review_queue.json` + terminal report, runnable locally with no API and no database.
- **Primary risks:** the re-key stage is new ground and is where correctness problems will show up; duplicate events on replay; ArangoDB adds a component; threshold tuning.
- **Fallback ladder, in order:** ship the ratio rule well-tested rather than four rules tuned by feel; `event_id` dedup for idempotency across both stages; cached metadata fixture if MusicBrainz is unavailable; if the graph layer runs out of time, `cohort_report.json` degrades to a flat listener-overlap count computed in the stage 2 consumer, and Streamlit plus the LLM summary stay optional layers over whatever JSON exists.
- **Seeded-data plan:** the generator emits normal listeners plus *both* documented fraud patterns (a high-volume spread-thin farm and a many-accounts-one-play farm), so tests can assert that stage 1 catches the first, stage 2 catches the second, and neither catches both alone. The test suite proves the two-key claim and makes detection correctness checkable.
- **Milestones:** schema + seeded data (normal + both fraud patterns) → producer + stage 1 → re-key + stage 2 → partition/offset/replay demo under both keys → ratio rule + review queue → ArangoDB load + co-listening traversal → dashboard + one LLM summary → validation, README, cleanup.

## 6. AI Element and Disclosure

- **Planned AI element:** Bounded summarization. One LLM call over `track_review_queue.json` produces ≤ 3 reviewer notes describing what a human should check.
- **Input/output boundary:** Input = the review-queue JSON only; output = short text. The LLM may not change flags, may not invent numbers, and may not assert that fraud occurred. It may only describe why a track is queued.
- **Verification:** every number in the output must exist in the input; tests cover empty input and API failure.
- **Fallback:** deterministic template sentence per flagged track (e.g. "Track T-12: 53,000 streams from 53,000 unique listeners with 4 saves. Review.").

**Ethics note:** a false positive withholds money from an artist who did nothing
wrong, and independent artists have the least recourse when that happens. Camouflage
makes the problem worse. Farms pad their activity to look ordinary, so individual
accounts are ambiguous by construction and confident automated judgment isn't
available here even in principle. That is why the output is a review queue instead of
a verdict, why the graph layer reports overlap rather than guilt, and why the LLM is
constrained to description instead of accusation.

**Trade-off note:** the strongest streaming design of the five options: two keys, a
re-key between stages, ordering that genuinely matters, idempotent replay, and a
graph layer that does something the keyed stages provably cannot. It also has the
best-evidenced problem statement, since the fraud patterns come from a court record
and published vendor analysis instead of invented thresholds. It costs the most
implementation time, which is why the plan commits to one rule per stage and treats
the graph cohort query as the first thing to cut.
