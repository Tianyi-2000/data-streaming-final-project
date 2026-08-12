# Answers to the v1 Contract's Open Items

> P.J.'s responses to section 10 of [PRODUCER-CONSUMER-CONTRACT.md](PRODUCER-CONSUMER-CONTRACT.md).
> Proposed, not final. Three of these change things Tianyi has already built against,
> so they need agreement before anyone acts on them.
>
> Thresholds and generator settings are one decision, not two. If we pick them
> separately, the tests prove nothing, because the detector would be tuned against
> data shaped by guesswork. Every threshold below is paired with the data shape it
> has to separate.

## Quick answers

| Item | Answer |
|---|---|
| Approve `PlayEventV1` fields and validation | Yes, no changes |
| Approve JSON + Pydantic | Yes, with one shared model file |
| Confirm re-key-only for `track-activity` | Yes, with one word changed |
| Consumer 1 window and threshold (Topology A) | More than 300 plays in a rolling 24 hours |
| Consumer 2 windows and thresholds (Topology B) | Three conditions together, in a 1-hour window |
| Shared test fixture | Yes, about 40 events plus an expected-flags file |

## 1. Event fields: approved as written

No additions. `country_code` is not needed, since no v1 rule uses it, and widening a
schema right before freezing it just creates churn.

One thing to record: dropping `save`, `follow`, and `playlist_add` costs us the
saves-per-stream signal, which the research names as one of the strongest real-world
tells. The three-condition rule in section 5 below works without it, so this is fine
for v1. It should be the first thing added in v2.

## 2. JSON + Pydantic: approved, with one change

Both sides validate, but they should validate with the **same code**, not two
hand-written models. One shared module, imported by the producer and both consumers:

```
contracts/play_event_v1.py
```

Two separate models that agree today will drift within a week.

## 3. Re-key-only: approved, with "may" changed to "must"

Section 4 currently says Consumer 1 *may* keep per-listener state to detect Topology
A. This should read **must**, and Consumer 1 must also emit a listener-side review
output.

The reason is that nothing downstream reads that state. If it is optional, it will
quietly not get built, Topology A detection disappears, and the two-topic design
collapses into a one-key pipeline with an extra hop in the middle.

Otherwise the re-key-only decision is right. Keeping the payload unchanged means one
event model across both topics and a much smaller contract surface.

## 4. Topology A: use a 24-hour window, not an hourly one

The bots in the criminal case ran about 636 plays per account per day, which is only
**26 plays an hour**. A real person skipping around can beat 26 in a busy hour. What
nobody can do is keep it up for 24 hours with no sleep.

So the signal is total volume over a long window, not peak rate.

| | Rule | Normal | Topology A | Margin |
|---|---|---|---|---|
| **Primary** | more than 300 plays in a rolling 24h | up to ~60 | 600 | 5x |
| Secondary, if time allows | distinct tracks ÷ total plays above 0.9, over at least 100 plays | ~0.4 | ~0.85 | clear |

The secondary rule is the more interesting one. It comes straight from the court
filing, which says the operator spread streams across thousands of songs to avoid
looking anomalous on any single song. Bots have no favorites; people do. Treat it as
stretch and commit only to the volume rule.

## 5. Topology B: three conditions, together

The rule currently in the proposal cannot detect this, and Tianyi is right about why.
If 53,000 listeners each play a track once, streams divided by unique listeners is 1,
which is the lowest possible value rather than a high one.

The fix is that a ratio sitting at 1 **while volume is high** is itself the anomaly.
On its own that also describes a genuinely viral track, so it needs company. All
three of these, in a 1-hour window on one track:

| Condition | Normal track | Topology B target |
|---|---|---|
| at least 200 unique listeners | roughly 1 to 20 per hour | 900 |
| plays per unique listener at or below 1.1 | ~1.4 | 1.0 |
| at least 60% of plays stopping in the band `30 <= played_seconds <= 35`, inclusive at both ends | ~6% | 92% |

The stop-time condition is what separates fraud from real popularity. Platforms only
count a play toward royalties after about 30 seconds, so automated playback stops in
a narrow band just past that line. Real listeners stop all over the place.

## 6. Generator settings that make those thresholds mean something

Simulate about 3 days of event time. Roughly 45,000 events total, which replays in
seconds.

| Cohort | Count | Shape |
|---|---|---|
| Catalog | 500 tracks, ~60 artists | one cached MusicBrainz pull, real `track_id` to `artist_id` relationships |
| Normal | 400 listeners | 10 to 60 plays per day, averaging ~25; active 8 to 14 hours a day with a sleep gap; each draws from a personal pool of ~25 tracks, so they repeat; stop times 12% under 30s, 6% in the 30 to 35s band, 82% spread from 35s to full length |
| Topology A | 8 listeners | 600 plays per day each, 24 hours a day with no sleep gap; drawn evenly from all 500 tracks; **stop times varied above 30s** |
| Topology B | 900 listeners | exactly one play each, all on one target track, all inside a 45-minute window; 92% of stop times in the 30 to 35s band; no other activity from these accounts |

**Topology A must have varied stop times.** If both fraud cohorts cluster at 30 to 35
seconds they will trigger each other's rules, and we lose the separation the whole
design is meant to demonstrate.

### Checking that neither rule catches both

- Topology A against the Topology B rule: 4,800 daily plays spread across 500 tracks adds about 0.4 plays per track per hour. Invisible.
- Topology B against the Topology A rule: one play per account, against a threshold of 300. Invisible.

That is the property the test suite should assert.

## 7. Windows are measured on event timestamps, not the clock

This is not currently in the contract and it needs to be.

All windows use `event_time` from inside the event, never wall-clock processing time.
Replay compresses three simulated days into a few seconds, so any clock-based window
would lump everything into one bucket and every rule would break.

Implementation: bucket on `event_time` truncated to the hour. The rolling 24-hour
figure is the sum of the last 24 hourly buckets.

**This puts a requirement on the producer:** events must be emitted in nondecreasing
`event_time` order, so the consumers can use a simple watermark instead of handling
late arrivals. Worth adding to section 5 of the contract.

## 8. Shared fixture

About 40 events, checked in, imported by both test suites, with a companion
`expected_flags.json`:

- several valid normal plays
- boundaries: `played_seconds` of 0, and equal to `track_duration_seconds`
- invalid: `played_seconds` above the track duration, a missing field, a Kafka key that does not match the value's `listener_id`, an `event_type` other than `"play"`
- one small Topology A listener that trips the 24-hour rule
- one small Topology B burst on a single track that trips all three conditions

The mini cohorts have to be small enough to stay readable, so thresholds should be
injected as config rather than hardcoded. We want that anyway for tuning.

## Summary of what changes for Tianyi

1. Section 4: "may maintain per-listener state" becomes "must", plus a listener-side output.
2. Section 5: add the requirement that events are produced in nondecreasing `event_time` order.
3. Section 7: the generator settings in section 6 above replace the current descriptions.
4. New shared file `contracts/play_event_v1.py` owned jointly, rather than a model on each side.
