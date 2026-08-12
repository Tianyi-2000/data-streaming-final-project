# Artificial Streaming Anomaly Monitor

**Producer half:** Tianyi (Partner A) — MusicBrainz catalog, seeded generator, replay producer · **Consumer half:** P.J. Losiewicz (Partner B) — both keyed stages, the re-key, the detection rules, the review queues

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

## Start here

Both halves of the project now live in this one tree. The **producer half** turns a cached
MusicBrainz catalog into a seeded, contract-validated stream of 45,473 play events and replays
them into Kafka keyed by `listener_id`. The **consumer half** reads that topic, applies a
per-listener rule, re-keys every event onto a second topic by `track_id`, applies a
three-condition per-track rule, and writes two human review queues plus a terminal report. The
producer sections below are Tianyi's and describe the first half; the consumer sections after
`Files` are P.J.'s and describe the second.

**If you only want to run it:** jump to
[How to run the whole pipeline](#how-to-run-the-whole-pipeline).
**If you only want the idea:** jump to
[Why there are two topics](#why-there-are-two-topics).

### Contents

Producer half (Tianyi)

- [Pipeline (our slice of the mandated architecture)](#pipeline-our-slice-of-the-mandated-architecture)
- [The contract (frozen — shared by both sides)](#the-contract-frozen--shared-by-both-sides)
- [What the synthetic data contains](#what-the-synthetic-data-contains)
- [How to run (producer)](#how-to-run-producer)
- [Local Kafka (no Confluent needed)](#local-kafka-no-confluent-needed)
- [Files](#files)

Consumer half (P.J.)

- [What the consumer side found](#what-the-consumer-side-found)
- [Why there are two topics](#why-there-are-two-topics)
- [How to run the whole pipeline](#how-to-run-the-whole-pipeline)
- [The review queue and the report](#the-review-queue-and-the-report)
- [The AI element](#the-ai-element)
- [Tests](#tests)
- [What was proposed, what was built, and what was cut](#what-was-proposed-what-was-built-and-what-was-cut)
- [Status & next steps](#status--next-steps)

### The headline result

- **45,473** events replayed through both stages, 0 dropped, 0 duplicated, 0 key mismatches.
- **8** flagged listeners — `A000` through `A007` — found by the `listener_id` key.
- **1** flagged track window — 901 unique listeners in one event-time hour — found only after
  the re-key to `track_id`.

Neither key found both. That is the whole point of the project, and
[`tests/test_two_key_proof.py`](tests/test_two_key_proof.py) asserts it rather than claiming it.

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

## Local Kafka (no Confluent needed)

We run a Kafka-compatible broker (Redpanda) locally via Docker. This is the
"local Kafka-compatible setup" the assignment allows — free, no cloud account.

```bash
docker compose up -d          # start broker + web console
# broker for code:  localhost:9092
# web console:      http://localhost:8080   (see topics/messages/keys)

# create the topics once (idempotent):
docker exec redpanda rpk topic create play-events track-activity -p 3 -r 1

# stream the events into the topic:
python src/replay_to_kafka.py            # all 45k events (keyed by listener_id)
python src/replay_to_kafka.py --limit 100    # quick smoke test

docker compose down           # stop broker when done
```

`replay_to_kafka.py` re-validates every line against the contract before
publishing, so only valid `PlayEventV1` events reach the topic.

---

## Files

```
contracts/play_event_v1.py         # shared PlayEventV1 model (import this)
src/fetch_musicbrainz_catalog.py   # one-time MusicBrainz → data/catalog.json
src/generate_events.py             # producer: catalog → play_events.jsonl
src/replay_to_kafka.py             # replay: play_events.jsonl → topic play-events
docker-compose.yml                 # local Redpanda broker + console
data/catalog.json                  # cached catalog
data/play_events.jsonl             # the event stream (PJ's input)
requirements.txt

# consumer half (P.J.) — added after the two branches were merged
src/create_topics.py               # idempotent creation of both topics
src/config.py                      # threshold loading; no detection number is hardcoded
src/windowing.py                   # event-time hour buckets + rolling 24h sums
src/consumer_stage1.py             # Consumer 1: per-listener rule, then the re-key
src/consumer_stage2.py             # Consumer 2: per-track rule + track_review_queue.json
src/summarize_review_queue.py      # bounded reviewer notes over the track review queue
config/thresholds.json             # production thresholds (45,473-event stream)
config/thresholds.fixture.json     # fixture-scale thresholds (48-event fixture)
tests/                             # 272 tests
output/                            # both review queues (gitignored: produced by a run)
state/                             # Consumer 1's dedup journal (gitignored)
DESIGN-two-key-pipeline.md         # why there are two topics, at length
SUBMISSION-proposal.txt            # the submitted proposal
DEMO.md                            # the demo script and its rehearsal record
```

---

## What the consumer side found

These are the measured results of one full replay of the committed 45,473-event stream through
both consumer stages, read off `output/listener_review_queue.json` and
`output/track_review_queue.json`. Nothing here is rounded or estimated.

**The run.** 45,473 records seen on `play-events`, 45,473 forwarded onto `track-activity`,
45,473 counted by Consumer 2. 0 invalid values, 0 key mismatches, 0 duplicate event ids, 0 late
drops. 1,308 distinct listeners, 464 distinct tracks, 23,302 `(track, event-time hour)` buckets.

**8 flagged listeners, found by the `listener_id` key.** `A000` through `A007` — the Topology A
cohort. Each recorded 1,800 plays across the three simulated days, and each peaked between
**620 and 638** plays inside a rolling 24-hour *event-time* window, against a threshold of more
than 300.

| Listener | Peak plays in a rolling 24h window | Threshold | Plays recorded |
|---|---|---|---|
| `A000` | 635 | > 300 | 1800 |
| `A001` | 626 | > 300 | 1800 |
| `A002` | 620 | > 300 | 1800 |
| `A003` | 633 | > 300 | 1800 |
| `A004` | 623 | > 300 | 1800 |
| `A005` | 623 | > 300 | 1800 |
| `A006` | 621 | > 300 | 1800 |
| `A007` | 638 | > 300 | 1800 |

No normal listener (`L0000`–`L0399`) was flagged. No Topology B listener (`B0000`–`B0899`) was
flagged, because one play each is exactly what an innocent account looks like at this key.

**1 flagged track window, found only after the re-key to `track_id`.** Track
`6fe6c7a3-2f67-4fa5-9ee3-724f5c57e836`, in the one event-time hour beginning
`2026-08-09T20:00:00Z`. All three conditions had to hold together:

| Condition | Measured | Comparison | Threshold |
|---|---|---|---|
| `min_unique_listeners` | 901 | at least | 200 |
| `max_plays_per_listener` | 1.0 | at most | 1.1 |
| `min_band_share` | 0.9234184239733629 | at least | 0.6 |

901 unique listeners produced 901 plays — exactly 1.00 each — and 832 of those 901 plays stopped
inside the 30–35 second band. (The reviewer note in the JSON renders that share to two decimals
as `0.92`; the full measured value is the one in the table.)

**These are review candidates, not verdicts.** Every entry carries the numbers behind its flag,
the thresholds it was measured against, and the config file those thresholds came from, so a
named artist can dispute a flag on its own evidence. Nothing in either queue concludes that
fraud occurred. A false positive withholds money from an innocent artist, so a human makes every
judgment; the system only decides what gets reviewed first.

---

## Why there are two topics

This is the project's whole claim, and it is worth two minutes.

**A Kafka key is just what you sort by.** The key decides which partition a record lands in, and
one consumer handles one partition, in order. You can see patterns *inside* a partition and
never *across* partitions. So whatever you sort by decides which questions you are able to ask
at all.

**Sorted by `listener_id`, you can ask: does this account behave like a person?** Play count,
rate, variety. That catches the sloppy fraudster — a few accounts making enormous volume. Our
`A000`–`A007` cohort is exactly that shape, and Consumer 1 finds all eight.

**And it is blind to the other shape.** The documented real case behind this project is a farm of
53,000 accounts that each played one track exactly once. Sorted by listener, that is 53,000
partitions of one record each. Every account looks innocent. The fraud is not hard to see at
this sort order — it is *invisible*.

**Sorted by `track_id`, the same events become one pile.** 53,000 first-time listeners on one
track, all stopping at the same second. Nothing about the data changed. Only the sort order
changed, and the fraud went from invisible to unmissable.

**The inverse holds too**, which is why one key is not enough in either direction. Sorted by
track, the high-volume account spread thin across the whole catalog dissolves into ordinary
per-track traffic — a few extra plays on each of hundreds of tracks, nothing anomalous anywhere.
Consumer 2 does not flag a single Topology-A-only track.

| Sorted by | Question it answers | Fraud it catches | Fraud it misses |
|---|---|---|---|
| `listener_id` | Does this account act like a person? | Few accounts, high volume, spread across the catalog | Many accounts, one play each |
| `track_id` | Does this track's audience make sense? | Many accounts, one play each on one track | Few accounts, high volume, spread thin |

**The re-key, concretely.** Consumer 1 reads `play-events` keyed by `listener_id`, updates its
per-listener state, and writes each event straight back out to `track-activity` keyed by
`track_id`. The Kafka key changes; the payload is byte-identical. That single line is the
repartition, and it is the reason Consumer 2 can ask a question Consumer 1 structurally cannot.

This is not a claim we make in prose and leave there.
[`tests/test_two_key_proof.py`](tests/test_two_key_proof.py) asserts all four halves of it — the
listener key finds Topology A and misses Topology B, the track key finds Topology B and misses
Topology A — at fixture scale through a live broker and again against the full-scale artifacts.

**The long version, with diagrams and the production argument, is in
[`DESIGN-two-key-pipeline.md`](DESIGN-two-key-pipeline.md).** It also covers the graph layer that
was cut; see [what was cut](#what-was-proposed-what-was-built-and-what-was-cut).

---

## How to run the whole pipeline

From a clean checkout to both review queues on disk. Nothing is assumed except Python 3.11+ and
Docker. **No API key is needed for any command in this section.**

### Two things that will quietly ruin a run

Read these before you run anything. Both produce a result that looks fine and is wrong.

1. **`state/consumer1_journal.jsonl` survives `docker compose down -v`.** Consumer 1 keeps a
   recovery journal of every `event_id` it has already forwarded, and it dedups against that
   journal *across runs*. `state/` is a host directory, so wiping the broker does not touch it.
   Re-run against a fresh broker with a stale journal and Consumer 1 forwards **nothing at all**,
   Consumer 2 flags nothing, and you get an empty, entirely plausible-looking result with no
   error anywhere. **Delete `state/` whenever you start over.** This is the single most likely
   way a demo of this repo fails.
2. **The consumers exit on an idle timeout, not on end-of-topic.** Both poll until 10 seconds
   pass with no record, then settle their offsets and exit 0. So the end of a run looks like a
   pause first. That is normal; wait for the summary block.

### The run

```bash
# 0. one-time: dependencies
pip install -r requirements.txt
```

```bash
# 1. broker up (Redpanda; broker on localhost:9092, web console on localhost:8080)
docker compose up -d
docker compose ps          # wait for redpanda to report "healthy"
```

```bash
# 2. create both topics — idempotent, safe to re-run
docker exec redpanda rpk topic create play-events track-activity -p 3 -r 1
```

`python3 src/create_topics.py` does the same thing from Python if you would rather not use `rpk`.

```bash
# 3. clear the previous run's state and outputs. SEE THE WARNING ABOVE.
rm -rf state output
```

```bash
# 4. smoke test first: 500 events all the way through, about 25 seconds total
python3 src/replay_to_kafka.py --limit 500
python3 src/consumer_stage1.py --thresholds config/thresholds.json
python3 src/consumer_stage2.py --thresholds config/thresholds.json
```

The smoke run flags nothing — 500 events is far short of any threshold. What it proves is that
the broker, the topics, the contract validation, the re-key and both consumers are all wired up.
The replay ends with `Queued 500 events to 'play-events', rejected 0, delivery failures 0`;
Consumer 1 ends with `Polled 500, forwarded 500`; Consumer 2 ends with `Polled 500, counted 500`
and a machine-readable `SUMMARY {...}` line. Each consumer sits idle for about 10 seconds after
its last record before exiting — that is the idle timeout, not a hang.

```bash
# 5. the full run: all 45,473 events. Start from a wiped broker so offsets and
#    the journal agree with each other.
docker compose down -v
docker compose up -d
sleep 20                   # let the broker come up healthy before creating topics
rm -rf state output
docker exec redpanda rpk topic create play-events track-activity -p 3 -r 1
python3 src/replay_to_kafka.py
python3 src/consumer_stage1.py --thresholds config/thresholds.json --commit-every 200
python3 src/consumer_stage2.py --thresholds config/thresholds.json --commit-every 200
```

`--commit-every 200` batches offset commits; the default of 1 is the strictest setting and is
what the kill/restart test runs at, but it is slow over 45,473 records.

**Measured on a 2024 MacBook Pro:** broker wipe and restart 23s, replay of all 45,473 events
under 1s, Consumer 1 12s, Consumer 2 12s. Both consumer figures are almost entirely the 10-second
idle timeout — the actual processing of 45,473 records takes roughly two seconds per stage.

### What you get

```
output/listener_review_queue.json   # Consumer 1: 8 flagged listeners, A000-A007
output/track_review_queue.json      # Consumer 2: 1 flagged track window
```

Consumer 2 also prints the terminal report — flagged tracks with the numbers behind each flag,
then the flagged listeners it read back from Consumer 1's file.

Both files are gitignored. They are the output of a run, not source, so a fresh checkout will not
have them and the full-scale tests skip (naming the commands above) rather than fail.

```bash
# when you are done
docker compose down -v
```

---

## The review queue and the report

`output/track_review_queue.json` is the artifact a human acts on. It holds four things:

- **`flagged_tracks`** — one entry per flagged `(track, event-time hour)` window. Each carries
  the track id, the window start and length, the three measured values, and for each condition a
  `measured`, a `threshold`, a `comparison` and a `satisfied` flag. It also carries the
  first and last event time inside the window, the play counts, and the stop-band edges.
- **`counts`** — what the run saw, counted and dropped, so the numbers can be reconciled against
  the stream.
- **`rule`** — the rule in words, and `thresholds_source_path`, the config file that produced it.
- **`posture`** — the standing statement that these are review candidates and nothing here
  concludes that fraud occurred.

**All three conditions must hold together in the same event-time hour.** Any one of them alone is
not enough, and that is not an aspiration — `tests/test_consumer_stage2.py` runs two near-miss
decoy windows through the real detector and asserts that each fails on exactly the condition it
was built to fail on.

Each flagged entry also carries a one-sentence `note` describing what was measured. That sentence
is generated by `template_note()` in `src/consumer_stage2.py`, which introduces no number that is
not already in the entry and reaches no verdict. It is also the fallback for the AI element
below.

The terminal report looks like this:

```
Consumer 2 done. Polled 45473, counted 45473.
  dropped, invalid value : 0
  dropped, key mismatch  : 0
  dropped, duplicate id  : 0
  tracks seen            : 464
  event-time hour buckets: 23302
  flagged windows        : 1
      6fe6c7a3-2f67-4fa5-9ee3-724f5c57e836 @ 2026-08-09T20:00:00Z (1h)
          min_unique_listeners: measured 901, at least 200
          max_plays_per_listener: measured 1.0, at most 1.1
          min_band_share: measured 0.9234184239733629, at least 0.6
          Track 6fe6c7a3-... : 901 unique listeners (at least 200), 901 plays at
          1.00 per listener (at most 1.10), and a 0.92 share of plays stopping
          inside the 30-35 second band (at least 0.60). These are the measured
          numbers against the configured thresholds; this entry is a review
          candidate for a human and states no conclusion.
  flagged listeners      : 8
      A000: peak 635 plays in 24h, over 300
      ...
```

---

## The AI element

`src/summarize_review_queue.py` is the project's stated AI element, and it is deliberately the
smallest, most tightly bounded piece of the system.

**Its contract.** One model call over `output/track_review_queue.json` and nothing else as input.
At most **3** reviewer notes out of that one call. The model may not change a flag, may not
introduce a number that is not already in its input, and may not assert that fraud occurred.

**Five bounds, each rejecting the whole response rather than the offending note**, because a
partially trusted answer is not a bound:

| Bound | Rejects |
|---|---|
| note count | more than 3 notes |
| track identity | a `track_id` that was not flagged in the input, or one repeated |
| numbers | any number in a note that the entry it describes does not license |
| verdict language | any term from a fixed lexicon of accusation, manipulation and bot vocabulary |
| parse | a response that is not a list of `track_id` / note pairs |

**No API key is required to run anything in this repo.** `default_client()` reads
`OPENAI_API_KEY` from the environment and only from the environment. Absent, it returns nothing,
no HTTP call is attempted, and the summary falls back silently to the deterministic
`template_note()` sentence and exits 0. That is the default path on this machine and probably on
yours — the fallback is the normal case, not an error case. A raising client, an unparseable
response, a bound violation and an empty review queue all take the same path.

**The bounds are proven against a stubbed client, not a live call.** This matters. A live call
samples one response and tells you what happened once; it cannot prove a constraint. So
`summarize(document, client=...)` takes an injected callable, and every bound is asserted against
stubs that raise, return garbage, return four notes, name a track that was never flagged,
fabricate a number, or reach a verdict. No test in this repo touches the network. The number
bound is calibrated by asserting that the shipped `template_note()` output passes it, and shown
non-vacuous by asserting that the same sentence with one digit changed is rejected.

```bash
python3 src/summarize_review_queue.py
python3 src/summarize_review_queue.py --input output/track_review_queue.json
python3 src/summarize_review_queue.py --json     # machine-readable
```

It reads the review queue, never writes it, and prints a provenance block naming the file it
read, which path produced the notes — `model` or `template` — and, if it fell back, why. With no
key set the output looks like this:

```
Reviewer notes from output/track_review_queue.json
  source: template
  fell back because: no model client is configured (OPENAI_API_KEY unset)
  notes: 1 (cap 3)
```

Exit code 0. The **only** exit-1 case is a missing or unparseable *input file*, which means the
pipeline has not been run — and it prints the commands that regenerate it.

---

## Tests

**328 tests** — the **272** that cover the pipeline itself, plus 56 covering the bounded summary
added last. Run them all:

```bash
python3 -m pytest tests/ -q
```

| File | What it proves |
|---|---|
| `tests/test_two_key_proof.py` | **The central claim.** Neither partition key catches both fraud topologies alone, asserted end to end through a live broker and again against the full-scale artifacts; plus PROF-02, that two independent replays of the same input produce identical detection. |
| `tests/test_consumer_stage1.py` | Per-listener rolling counts, the re-key (key changes, payload byte-identical), invalid-record drops, and the dedup journal. |
| `tests/test_consumer_stage1_e2e.py` | Consumer 1 killed mid-run and restarted: nothing lost, nothing double-forwarded. |
| `tests/test_consumer_stage2.py` | The three-condition rule, the two near-miss decoy windows, and an AST scan proving the rolling accumulator is not used in this stage. |
| `tests/test_consumer_stage2_e2e.py` | Consumer 2 against a live broker, end to end to the review queue. |
| `tests/test_windowing.py` | Hourly buckets, rolling 24h sums, watermark and boundary behaviour, and an AST scan proving nothing reads the system clock. |
| `tests/test_contract_model.py`, `test_contract_helpers.py`, `test_contract_acceptance_checks.py` | The shared `PlayEventV1` model and its helpers, including the inclusive 30–35s band. |
| `tests/test_thresholds_config.py` | No detection threshold carries a default, so no detector can silently fall back to a built-in number. |
| `tests/test_fixture_trips_rules.py` | The 48-event fixture trips both rules at fixture scale. |
| `tests/test_harness_roundtrip.py` | One real event round-tripped through the broker with its key checked. |
| `tests/test_summarize_review_queue.py` | The AI element's five bounds and its five fallback paths, all against stubs. |

**Which tests need a live broker:** `test_two_key_proof.py`, `test_consumer_stage1_e2e.py`,
`test_consumer_stage2_e2e.py` and `test_harness_roundtrip.py`. Bring the broker up with
`docker compose up -d` first. Everything else — including all of the AI element's tests — runs
with no broker and no network.

The full-scale assertions in `test_two_key_proof.py` read `output/`, which is gitignored. On a
fresh checkout they **skip** with a message naming the commands that regenerate the artifacts,
rather than failing — the absence of somebody's local run is not a failure of the project's
claim.

---

## What was proposed, what was built, and what was cut

[`SUBMISSION-proposal.txt`](SUBMISSION-proposal.txt) describes more than what shipped. That is
worth stating plainly here rather than leaving a grader to discover it.

**What the proposal described** (sections 3 and 4): the two keyed stages and the re-key, then a
third layer — graph topics feeding an **ArangoDB** Kafka Connect sink, a listener/track graph, an
**AQL** co-listening traversal over it, and a **Streamlit** dashboard on top. The tools list named
ArangoDB, a Kafka Connect worker, Streamlit, and an OpenAI SDK.

**What shipped:** the seeded producer and replay; Consumer 1 with per-listener rolling detection,
the dedup journal and the re-key; Consumer 2 with the three-condition per-track rule;
`output/listener_review_queue.json` and `output/track_review_queue.json`; the terminal report;
the two-key proof and PROF-02 replay determinism; and the bounded LLM summary with its
deterministic fallback. 272 tests.

**What was cut, and why:** the ArangoDB graph sink, the Kafka Connect worker, the AQL traversal
and the Streamlit dashboard. All four. The designed scope in `DESIGN-two-key-pipeline.md` is
roughly 40–60 hours of work; the build window was 2–3 days with a full-time job inside it.
Given that, the choice was between a shallow version of everything and a fully verified version
of the part that carries the argument. We chose the second. The Connect worker was also, by our
own note in the design doc, the piece most likely to break during a live demo.

**The proposal anticipated this.** Section 5 names the minimum result explicitly: *a seeded
replay through both stages to `track_review_queue.json` and a terminal report, with no API and no
database.* That minimum shipped in full, plus the AI element from section 6 and the two-key proof
section 5 promised. What was cut is the layer section 5 already listed as the first thing to
degrade.

**Where the cut work went.** Both are scheduled, not abandoned: the Streamlit review UI is
Phase 7 and the ArangoDB graph sink is Phase 8, both post-submission. Neither was started before
this phase shipped.

---

## Status & next steps

- ✅ **Producer half done and verified** (schema validation, rule-tripping, reproducibility) —
  all 45,473 events land on `play-events` keyed by `listener_id`, 0 errors.
- ✅ **Consumer half done and verified** — Consumer 1 (per-listener rule + re-key onto
  `track-activity`), Consumer 2 (three-condition per-track rule + `track_review_queue.json` +
  terminal report), both review queues, and the bounded LLM summary with its fallback.
- ✅ **The central claim is asserted, not just described** — `tests/test_two_key_proof.py` proves
  neither key catches both topologies, and that two replays produce identical detection.
- ✅ **328 tests passing** — the 272 covering the pipeline, plus 56 for the bounded summary.
- 📋 **The demo:** [`DEMO.md`](DEMO.md) — the script, and the record of running it end to end
  from a wiped broker, with real timings.
- ⏭️ **Post-submission:** Phase 7, a Streamlit review UI over the queues; Phase 8, the ArangoDB
  graph sink and the AQL co-listening traversal. Neither is required for grading.
