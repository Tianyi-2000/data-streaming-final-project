# Design — The Two-Key Pipeline, Explained Plainly

> Companion to [Option 4 — Artificial Streaming Anomaly Monitor](04-streaming-fraud-anomaly-monitor.md).
> Evidence behind the design: [RESEARCH-artificial-streaming-fraud.md](RESEARCH-artificial-streaming-fraud.md).
>
> Written for anyone joining the project who needs to understand why there are two
> Kafka topics instead of one. No Kafka background assumed.

## The one idea

Forget Kafka for a moment. Imagine every play on the platform prints a paper slip:

```
listener L-88 played track T-12 for 31 seconds at 03:04
```

There are billions of slips. To find anything in the pile, you have to sort it.

**A Kafka key is just what you sort by.** The key decides which pile (partition) a
slip lands in, and one worker handles one pile, in order.

The catch: you can see patterns inside a pile, but never across piles. So whatever
you sort by decides which questions you are able to ask at all.

## Stage 1, sorted by listener

One pile per person.

```
L-88's pile:  ████████ (412 slips)
L-89's pile:  █        (1 slip)
L-90's pile:  ██       (2 slips)
```

This answers: does this account behave like a human? Play count, speed, variety.

It catches the sloppy fraudster running 400 plays an hour. That was the original
version of this project.

## Why one key isn't enough

The research turned up a real bot farm with 53,000 accounts, each of which played one
track exactly once. Sort those slips by listener:

```
53,000 piles, each holding exactly one slip
```

Every pile looks innocent. One play each. There is nothing to flag. The fraud is not
hard to see at this sort order, it is invisible.

## Stage 2, re-sorted by track

Same slips. New sort order. One pile per song.

```
T-12's pile:  53,000 slips, from 53,000 different people,
              none of whom played anything else,
              all stopping between 30 and 31 seconds,
              4 saves total
```

Now the pattern is obvious. Nothing about the data changed. Only the sort order
changed, and the fraud went from invisible to unmissable.

That re-sort is the whole trick, and it is boring to implement. The stage 1 consumer
reads from one topic and writes each event back out to a second topic under a
different key:

```python
for event in stage1_consumer:              # topic: play-events   (key = listener_id)
    state = listener_state[event.listener_id]
    state.update(event)

    producer.produce(
        topic="track-activity",            # topic: track-activity (key = track_id)
        key=event.track_id,                # the re-key
        value=enrich(event, state),
    )
```

"Repartition" is the formal word for dumping the piles out and sorting them again.

## Why both stages

Each sort answers a question the other cannot, and both kinds of fraud exist:

| Sorted by | Question it answers | Fraud it catches |
|---|---|---|
| `listener_id` | Does this account act like a person? | Few accounts, high volume, spread across many tracks (1,040 accounts, ~636 plays/day each) |
| `track_id` | Does this track's audience make sense? | Many accounts, one play each (53,000 accounts, 1 play each) |

Pick one key and you are blind to half the problem. The two documented cases in the
research are near-inverses of each other, which is the point.

## Stage 3, the graph, and what piles cannot do

One question survives no matter how you sort:

> Are these 500 accounts all listening to the same oddly specific set of 12 obscure
> tracks and nothing else?

That is not a fact about one listener or one track. It is a fact about the overlap
between them. Piles hold things; they do not hold relationships.

Draw it as lines instead. Every listener connects to every track they played. A bot
farm shows up as a dense knot: 500 dots on the left all wired to the same 12 dots on
the right. Real listeners produce a loose, messy web.

```
    listeners            tracks
      ○ ─────────────────→ ●
      ○ ──┬──────────────→ ●     dense knot: the same small track set,
      ○ ──┼──────────────→ ●     hit by the same large account set
      ○ ──┴──────────────→ ●
      ○ ─────────→ ◍             loose web: ordinary listening
      ○ ──→ ◍
```

This is also the layer that beats camouflage. Farms pad their activity with plays
across unrelated artists so their accounts look ordinary one at a time, which drags
per-key thresholds toward normal. The padding adds stray lines around the knot but
does not dissolve it.

In ArangoDB the graph is two vertex collections and one edge collection
(`listeners`, `tracks`, `played`). The cohort query is a two-hop traversal out from a
flagged track and back, counting how often the same listeners reappear on the same
small set of tracks. Sketch, to be validated against real data:

```aql
FOR listener IN 1..1 INBOUND @flaggedTrack played
  FOR other IN 1..1 OUTBOUND listener played
    FILTER other._id != @flaggedTrack
    COLLECT track = other._id WITH COUNT INTO shared_listeners
    FILTER shared_listeners >= @minOverlap
    SORT shared_listeners DESC
    LIMIT 25
    RETURN { track, shared_listeners }
```

High recurrence is a cohort. Pregel community detection is a deeper option if there
is time, but the traversal alone demonstrates the idea.

## How the graph gets loaded

The ArangoDB Kafka Connect sink connector writes the graph, so no Python touches the
database on the ingest path. Stage 2 emits documents that are already valid Arango
vertices and edges, and the connector maps topics to collections.

The connector will not build the graph for you. ArangoDB's docs are explicit that
edges need `_from` and `_to` in Arango's own format, and they recommend a custom
stream application to do that mapping because source data rarely arrives edge-shaped.
So the mapping is stage 2's job. The connector is the writer, not the modeler.

Three topics, three keys:

| Topic | Key | Arango collection | Document shape |
|---|---|---|---|
| `graph-listeners` | `listener_id` | `listeners` (vertex) | `{_key: "L-88", ...features}` |
| `graph-tracks` | `track_id` | `tracks` (vertex) | `{_key: "T-12", ...metrics}` |
| `graph-played` | `event_id` | `played` (edge) | `{_key: "e-9", _from: "listeners/L-88", _to: "tracks/T-12", played_seconds: 31}` |

Two things to know about this:

- **The edge topic is keyed by `event_id`, not by track.** One play is one edge, so the event is the document identity. The connector takes `_key` from the record value when it's present, and the docs recommend keying records so all writes for one document land on one partition. This is a third keying decision in a project already about keying decisions.
- **The vertex topics are not optional.** Arango does not check that an edge's endpoints exist, so loading `played` alone gives you a graph whose traversals return nothing useful.

The connector translates records into REPSERT and DELETE operations, so the load is
idempotent. Replaying the seeded file rebuilds an identical graph instead of
duplicating it, which is the same guarantee the Kafka stages already need.

One risk worth naming: the Connect worker is a second piece of infrastructure and the
most likely thing to break during a live demo. `python-arango` stays in the repo as a
fallback writer behind the same interface, so the graph layer loads either way.

## The honest caveat

At this project's scale, one consumer on one laptop reading a seeded file, you could
skip all of this and keep two dictionaries in memory in a single process. It would
work fine.

Two reasons to build it as two stages anyway:

1. **It is what production forces on you.** With 20 partitions across 20 machines, no
   worker can see another worker's piles. Re-keying is the only way to change the
   question.
2. **It is the strongest thing the project can demonstrate.** "We keyed by listener,
   found the fraud wasn't visible there, and re-keyed by track" shows an
   understanding of why partition keys exist, which is a different and better thing
   than showing that we can call `producer.produce()`.

## Where the pieces live

| Stage | Key | Owner | Output | First to cut if time runs short |
|---|---|---|---|---|
| producer | n/a | Tianyi | seeded events into `play-events` | no, this is the input |
| 1 | `listener_id` | P.J. | per-account features, re-keyed events | no, this is the spine |
| 2 | `track_id` | P.J. | `track_review_queue.json` + graph topics | no, this is the deliverable |
| 3 | `event_id` / vertex ids | P.J. | Arango collections, `cohort_report.json` | yes, degrades to a flat overlap count in stage 2 |

The split runs along the topic contract: Tianyi owns everything up to `play-events`,
P.J. owns everything after it. The schema of that topic is the interface, so it is the
one thing to agree on before either side starts writing code.

The minimum end-to-end result is stages 1 and 2 with no API and no database.
