# The graph layer

The reason this project was chosen over the other four proposals, finally built.

The two keyed stages each answer a counting question — does this account behave like a person
(Consumer 1, keyed by `listener_id`), does this track's audience make sense (Consumer 2, keyed by
`track_id`). **Neither can see coordination structure**, because structure is a property of the
relationships rather than of any one key. No aggregation behind a single key can express "these
eight accounts touched a near-identical set of tracks", however the window is drawn.

This layer models listeners and tracks as a bipartite graph and asks two questions that the keyed
stages structurally cannot.

---

## Running it

**ArangoDB is opt-in, behind the `graph` compose profile.** Every command below carries
`--profile graph`. Without the flag `docker compose` starts the v1 services, exits 0, and does
nothing about ArangoDB — no error, no container, and then an empty graph. That silent-success shape
is this project's recurring failure mode, so the flag is not optional in any command here.

```bash
docker compose --profile graph up -d        # broker + console + ArangoDB
docker compose --profile graph ps

python3 src/graph_emitter.py --group graph-emitter-full     # track-activity -> 3 topics
python3 src/graph_loader.py --drop --group graph-loader-full  # 3 topics -> the graph
python3 src/graph_queries.py                                  # -> output/cohort_report.json
```

ArangoDB's web UI is at <http://localhost:8531> (user `root`, password from `ARANGO_ROOT_PASSWORD`,
default `streamingfraud`).

### Why the profile exists

`HRNS-01` guarantees that **nothing beyond the broker is required to run the pipeline**, and
`tests/test_harness_roundtrip.py` enforces it by asserting that `docker-compose.yml` declares no
service beyond the broker and the console. An unprofiled graph service breaks that assertion.

The alternative — widening the assertion to admit `arangodb` — was considered and **declined**. It
relaxes a v1 guarantee so that a v2 feature can fit, which is the erosion the freeze exists to
prevent; an assertion widened once gets widened again. The profile makes the optionality
*structural* instead: profiled services are invisible to a plain `docker compose config --services`,
so the v1 test needed no edit at all and its guarantee stays literally true rather than
reinterpreted. Both directions are asserted — that the profile is required to get the service, and
that the v1 stack still comes up clean without it.

### Why port 8531

8529 and 8530 are held by two ArangoDB containers belonging to unrelated projects on this machine.
The graph binds **8531** and uses a distinct container name, `streaming-fraud-arangodb`. Every
`docker compose` command runs from this repository root with no `-f` pointing elsewhere.

---

## How the graph is built

```
track-activity ──> src/graph_emitter.py ──> graph-listeners ─┐
   (45,473)          (its own consumer         graph-tracks  ├─> src/graph_loader.py ──> ArangoDB
                      group; Consumer 2         graph-played ─┘     (python-arango)      (8531)
                      is untouched)
```

**The three topics are the interface**, and that is deliberate: the Kafka Connect sink
([below](#the-kafka-connect-path-grph-05)) consumes exactly the same three topics and writes exactly
the same shapes, which is what makes it an *alternative write path* rather than a second
implementation.

| Topic | Kafka key | Becomes | Keyed by |
|---|---|---|---|
| `graph-listeners` | `listener_id` | `listeners` vertex | `_key = listener_id` |
| `graph-tracks` | `track_id` | `tracks` vertex (carries `artist_id`) | `_key = track_id` |
| `graph-played` | `event_id` | `played` **edge**, `_from` → `_to` | `_key = event_id` |

The collections sit inside a named graph, `listening`, whose single edge definition runs
`played: listeners → tracks`. The named graph is not decoration — it is what makes three collections
a graph rather than three tables.

At full scale the whole stream becomes **1,308 listener vertices, 464 track vertices and 45,473
`played` edges**, with zero dangling endpoints. It loads in about twelve seconds; this fits in
memory and there is no scale here to build for.

**`src/graph_emitter.py` is a second consumer, not a change to Consumer 2.** `GRPH-02` originally
said "Stage 2 emits"; it is amended and dated in `REQUIREMENTS.md`. `src/consumer_stage2.py` carries
five phases of verification and Phase 5's PROF-02 compares its output byte for byte, and Consumer 2
produces to no topic at all today — so these topics are a genuinely new output rather than a
redirect, and the projection is zero-risk by construction.

### Two things that fail silently, and what stops them

**A wrong `_from`/`_to` prefix.** ArangoDB does not validate that an edge's endpoints resolve to real
documents. A typo in a collection prefix therefore produces a graph whose traversals return nothing
while erroring on nothing. The two prefixes are module constants in `src/graph_emitter.py` that
`src/graph_loader.py` **imports**, so there is exactly one place the typo can live — and the loader
counts the edges whose endpoints do not resolve after its final settle. A non-zero count is a hard
failure with a non-zero exit, not a warning.

**A load that wrote nothing, reporting success.** In python-arango 8.3.3, `insert_many` takes
`raise_on_document_error=False` **by default** and returns per-document failures as error *objects*
inside the list it hands back. A loader that ignores the return value completes without an exception
and may have written nothing at all. The loader passes `raise_on_document_error=True` explicitly,
inspects the return anyway, and the tests assert the driver's default really is `False` — so if a
later version changes it, that shows up as a test failure rather than as someone tidying away an
explicit keyword that looked redundant.

---

## Replay idempotency (GRPH-04)

`GRPH-04` exists because dropping the Connect sink silently took its REPSERT semantics with it, and
no shipped document carried that obligation afterwards. The direct writer supplies it explicitly:
`insert_many(..., overwrite_mode="replace")`, keyed on the natural ids, so a redelivered record
becomes the same `_key` and overwrites itself instead of duplicating or raising.

**Proven by running it twice, not by citing the driver.** Two full emit-and-load cycles on fresh
consumer groups:

```
load 1   {"dangling_edges": 0, "listeners": 1308, "played": 45473, "tracks": 464}
load 2   {"dangling_edges": 0, "listeners": 1308, "played": 45473, "tracks": 464}
```

The `SUMMARY` line deliberately separates **graph state** from **run state**, and the separation is
the whole reason the line is diffable. Graph state is replay-invariant. Run state is not:

```
load 1   "run_state": {"documents_upserted": 47245, "records_consumed": 47245, ...}
load 2   "run_state": {"documents_upserted": 94490, "records_consumed": 94490, ...}
```

The second load reads topics that now hold the records twice, so its counters legitimately double
while the graph does not move at all. Extract `graph_state` and diff that; diffing the whole line
would fail on a correct run. Phase 5 learned this exact lesson when PROF-02 had to stop comparing
whole review documents.

### Order-independence, which comes free and is worth stating

Upsert-by-key does not care what order records arrive in. That matters here more than it sounds:
**26,771 of the 45,473 events arrive out of `event_time` order within their own track**, which is
precisely the condition that defeated the windowing approach in Phase 2 — fed through a watermarked
accumulator, 25,924 of them were dropped as late and the fraud window degraded from 901 unique
listeners to 493, silently.

The graph is immune by construction. The three topics also have no ordering relationship to each
other, so edges routinely arrive before their vertices; that is harmless for the same reason, and
the dangling-edge count is asserted to be 0 afterwards rather than assumed.

### A trap in proving this at slice scale

`--max-events N` takes a **nondeterministic** slice. `track-activity` has three partitions, and a
bounded read drains whichever partition's fetch response arrives first. Measured on two fresh
consumer groups reading 1,500 records each: run A took all 1,500 from partition 0, run B all 1,500
from partition 1, and **the two sets shared nothing**.

So "emit a slice, load it, emit a slice again, load again, diff the counts" does **not** test
idempotency — it feeds two different inputs and correctly reports two different graphs. The claim is
that loading the *same* input twice is identical, so the input has to be held still: emit once, then
run the loader twice on fresh consumer groups. At full scale the trap disappears, because an
unbounded run drains every partition and both runs read the same 45,473 records.

---

## The two queries

Two, because the two topologies have **opposite shapes** and one query cannot find both. Writing
only the first would miss the case the review queue actually flagged; writing only the second would
miss the more interesting structure.

Both seeds are **read** from the shipped review-queue artifacts — `output/listener_review_queue.json`
and `output/track_review_queue.json` — and never typed into the module, so the graph layer and the
keyed pipeline cannot disagree about which listener and which track were flagged. An AST scan in the
tests enforces it.

### Every count is DISTINCT tracks, never plays

The graph holds 45,473 plays over **13,985 distinct (listener, track) pairs** — a mean of 3.25 plays
per pair. A query that groups peers and takes `COUNT` counts *edges*, inflating every overlap by
roughly threefold: a true 438 reads as about 1,400, which is a number a reviewer would accept. Both
queries take `UNIQUE` over track ids on both sides, and a construction test rejects a built query
that would count edges.

### Query 1 — co-listening overlap: a dense bipartite block

Traverse listener → track → listener and count how many **distinct tracks** each peer shares with
the seed. This is the FRAUDAR / CopyCatch dense-subgraph pattern from the research, present in our
own data. Seeded on the first flagged listener:

```
peers at or above 100 shared distinct tracks: 7

  A003: 446 shared of 460 distinct tracks
  A001: 442 shared of 456 distinct tracks
  A002: 442 shared of 456 distinct tracks
  A004: 442 shared of 456 distinct tracks
  A005: 440 shared of 454 distinct tracks
  A006: 440 shared of 454 distinct tracks
  A007: 438 shared of 452 distinct tracks

cohort floor 438  vs  highest outside the flagged set 25
normal pairwise baseline: mean 1.2412, max 5, over 1770 pairs
```

**This is a measurement, not a threshold choice.** The cohort's floor is 438 and the nearest
non-cohort pair is 25, so *any* cut between those two numbers returns the same seven listeners. The
threshold of 100 is arbitrary within a range spanning more than an order of magnitude, which is the
point — the separation is a property of the data.

The baseline is reported next to the cohort because a cohort without one is a threshold choice
rather than a finding. The baseline pool deliberately **excludes single-track listeners**: accounts
that played one track each overlap with nobody, and including them would drag the mean toward zero
and make the separation look larger than it is. The conservative direction is the honest one.

### Query 2 — fan-in with no other activity: a maximal star

From the flagged track, every distinct inbound listener, and for each one its activity across the
**entire** graph rather than just this track. The cohort is the subset with exactly one play and
exactly one distinct track — an account that exists in this dataset for exactly one reason.

```
distinct inbound listeners       : 928
of those, one play and one track : 900
```

Their signature is the **absence** of other activity, not overlap — their pairwise overlap with each
other is near zero, so Query 1 cannot find them at all.

### 928 is not a contradiction of 901

`output/track_review_queue.json` reports **901** unique listeners on this track. This graph reports
**928** inbound. Both are correct and they measure different things:

- **901** is measured inside *one event-time hour* (beginning `2026-08-09T20:00:00Z`), because
  Consumer 2's rule is a one-hour bucket. It is 900 fan-in listeners plus one flagged listener.
- **928** is *unwindowed* and covers the whole stream: 900 fan-in listeners, plus normal listeners
  who happened to play the track at some point, plus flagged ones.

Exactly 900 of the 928 have one play and one distinct track in the entire graph. The tests assert
**containment** — every one of the 900 is inbound to the flagged track — rather than equality of two
totals that are not equal and should not be. Asserting `928 == 901` would be asserting that a
correct query is broken, and would push the next reader into "fixing" it until it returned a wrong
number.

---

## `output/cohort_report.json`

Both results, the three collection counts and the dangling count, the seeds and the artifact paths
they were read from, the thresholds, the measured baseline, and a posture line. Serialized with
sorted keys, a fixed indent and a trailing newline, matching `write_review_queue` — two runs over
the same graph produce a byte-identical file.

**It reaches no verdict, and that bound is inherited rather than restated.** `VERDICT_LEXICON` is
imported from `src/summarize_review_queue.py`, so this layer carries the same bound the review queue
and the LLM summary already carry and the three cannot drift. The run **fails and refuses to write**
if any of the 22 terms reaches the document, and the tests prove the check is non-vacuous with
constructed counter-examples.

This is the most dangerous artifact the project produces — it names hundreds of accounts at once, on
evidence a reviewer will not independently recompute, and a false positive withholds money from an
innocent artist. `output/` is gitignored, so it stays local.

---

## The Kafka Connect path (GRPH-05)

**It works, and it produced identical counts.** ArangoDB's own Kafka Connect sink connector consumes
the *same three topics* and writes a **second** database, which is what makes it an alternative write
path rather than a second implementation.

Full setup, the artifact's provenance and every configuration property are in
[`connect/README.md`](connect/README.md). The short version:

| | |
|---|---|
| Connector | `com.arangodb:kafka-connect-arangodb:2.0.0`, Maven Central, sha256 `519afaf0…7ccac` |
| Publisher | the `arangodb` GitHub **Organization** — approved at a blocking provenance gate before download |
| Target | `streaming_fraud_graph_connect` — **never** the database the direct writer owns |
| Opt-in | the **same `--profile graph`** as ArangoDB; a plain `docker compose up -d` starts neither |

```bash
docker compose --profile graph up -d connect
for f in connect/graph-*-sink.json; do
  curl -sS -X POST -H 'Content-Type: application/json' --data @"$f" http://localhost:8083/connectors
done
```

### The proof it is an equivalent write path

Not "the connector started" — a misconfigured sink reports `RUNNING` while writing nothing. The
acceptance test is a count comparison:

```
direct  (python-arango) : {"dangling_edges": 0, "listeners": 1308, "played": 45473, "tracks": 464}
connect (kafka sink)    : {"dangling_edges": 0, "listeners": 1308, "played": 45473, "tracks": 464}
```

Stronger still, both cohort queries run unchanged against the Connect-written graph and return the
same finding — so `_from`/`_to` survived the converter and the edges genuinely resolve:

```
A003 446 | A001 442 | A002 442 | A004 442 | A005 440 | A006 440 | A007 438
fan-in: 928 inbound, 900 with one play and one distinct track
```

`insert.overwriteMode=replace` is the connector-side counterpart of the direct writer's
`overwrite_mode="replace"` — the REPSERT semantic whose loss, when the connector was originally
descoped, is the reason GRPH-04 exists at all. `data.errors.tolerance=none` is the counterpart of
`raise_on_document_error=True`: fail rather than silently skip.

### The fallback, which is required either way

**The direct `python-arango` writer is the working path**, and it satisfies GRPH-01 through GRPH-04
**without the Connect worker running at all**. The ROADMAP's success criterion 6 provides for exactly
this degradation. If the worker misbehaves — a bad plugin path, a converter mismatch, a broker
address pointing at `localhost` from inside a container — the recovery is simply not to use it:

```bash
docker compose --profile graph up -d              # broker + ArangoDB, no Connect worker
python3 src/graph_emitter.py --group graph-emitter-full
python3 src/graph_loader.py --drop --group graph-loader-full
python3 src/graph_queries.py
```

That path is what every number in this document was produced by. The Connect sink is an *additional*
demonstration, and nothing in the phase's claim depends on it.

Because the worker sits behind the `graph` profile, a broken Connect configuration cannot affect
`docker compose up -d`, and because it writes a separate database it cannot damage the graph the
direct writer already proved.
