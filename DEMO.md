# Demo — Artificial Streaming Anomaly Monitor

The demo script, **and the record of running it**. Everything in the
[Rehearsal record](#rehearsal-record) below is pasted from an actual run on
2026-08-12, from a wiped broker and a cleared state directory. Nothing in it is
expected output; it is observed output.

**Total: about 3 minutes of machine time**, plus 34 seconds for the replay-identity beat, which
runs two complete pipelines and is by a wide margin the longest thing here.

---

## Pre-flight

Do these before anybody is watching.

### 1. Clear `state/`. This is the one that kills the demo.

```bash
rm -rf state output
```

Consumer 1 keeps a recovery journal of every `event_id` it has forwarded and dedups against it
**across runs**. `state/` is a host directory, so `docker compose down -v` does not touch it —
it wipes the broker and leaves the journal holding all 45,473 ids. Start a "fresh" demo with a
stale journal and Consumer 1 forwards **zero** events, `track-activity` stays empty, Consumer 2
flags nothing, and no error is raised anywhere.

It is worse than that, and the rehearsal proved it: **Consumer 1 still reports all 8 flagged
listeners**, because it rebuilds its per-listener counts from the journal. So the listener half
of the demo looks completely normal while the track half — the half carrying the whole argument
— is silently empty. See [Finding 1](#findings) for the pasted output.

### 2. Everything else

| Check | Command | Expect |
|---|---|---|
| Docker running | `docker info` | a server version |
| Broker up and healthy | `docker compose up -d && docker compose ps` | `redpanda` reports `healthy` (about 20s) |
| Console reachable | open <http://localhost:8080> | topic list loads |
| Deps installed | `pip install -r requirements.txt` | pydantic, requests, confluent-kafka |
| Stream present | `wc -l data/play_events.jsonl` | 45473 |

No API key is needed. Nothing in this demo makes a network call outside `localhost`.

### 3. Know the two pauses

Both consumers exit on a **10-second idle timeout**, not on end-of-topic. The last ten seconds of
each stage look like a hang and are not. Say so before it happens, not after.

---

## The script

### Beat 0 — wipe and set up (about 45 seconds)

```bash
docker compose down -v          # wipe the broker
docker compose up -d
sleep 20                        # let it come up healthy
rm -rf state output             # THE STEP FROM PRE-FLIGHT 1
docker exec redpanda rpk topic create play-events track-activity -p 3 -r 1
```

**Point at:** two topics, three partitions each. Say that the partition count is the only
structural thing they have in common — what differs is what each is *keyed by*, and that is the
project.

### Beat 1 — replay 45,473 events (under 1 second)

```bash
python3 src/replay_to_kafka.py
```

**Point at:** `Queued 45473 events to 'play-events', rejected 0, delivery failures 0`. Every line
was re-validated against the shared `PlayEventV1` contract before it was published, so only valid
events reached the topic.

### Beat 2 — partition assignment under each key

This is DEMO-02's first requirement, and it has three parts.

**2a. The partition layout of both topics.**

```bash
docker exec redpanda rpk topic describe play-events -p
docker exec redpanda rpk topic describe track-activity -p
```

**Point at:** three partitions on each, and the high watermarks — 17131 / 12478 / 15864 on
`play-events`. The events are spread across all three because the key hashes spread them; nothing
was routed by hand.

**2b. One key lands on exactly one partition.**

```bash
# every A000 record on play-events, and which partition each sits on
docker exec redpanda rpk topic consume play-events -f '%p %k\n' -n 45473 -o start \
  | awk '$2=="A000"{print $1}' | sort | uniq -c

# every record for the flagged track on track-activity
docker exec redpanda rpk topic consume track-activity -f '%p %k\n' -n 45473 -o start \
  | awk '$2=="6fe6c7a3-2f67-4fa5-9ee3-724f5c57e836"{print $1}' | sort | uniq -c
```

**Point at:** one line of output each. All 1,800 of `A000`'s plays are on one partition; all 1,006
records for the flagged track are on one partition. That is *why* a per-key rolling count is
possible at all — one consumer sees the whole of one key's history, in order.

The stronger version, over every key on both topics at once:

```bash
docker exec redpanda rpk topic consume play-events -f '%p %k\n' -n 45473 -o start \
  | awk '{print $2, $1}' | sort -u | awk '{c[$1]++} END{m=0; for(k in c) if(c[k]>m) m=c[k]; print m}'
```

**Point at:** `1`. Not "the keys we checked" — the *worst case across all 1,308 listener keys*, and
the same for all 464 track keys.

**2c. The re-key, made visible: one event, two keys, two topics.**

```bash
EV=863d8ad6-0037-52ea-bbf7-ac0e600316a6
docker exec redpanda rpk topic consume play-events    -f '%p | %k | %v\n' -n 45473 -o start | grep -F "$EV"
docker exec redpanda rpk topic consume track-activity -f '%p | %k | %v\n' -n 45473 -o start | grep -F "$EV"
```

**Point at:** the same `event_id`, keyed `B0070` on partition 1 of `play-events` and keyed
`6fe6c7a3-…` on partition 2 of `track-activity`. **The payload is byte-identical; only the key
changed.** That single line in Consumer 1 is the entire repartition, and it is the reason
Consumer 2 can ask a question Consumer 1 structurally cannot.

**The browser version, and the better visual for an audience:** open
<http://localhost:8080> → **Topics** → `play-events` → **Messages** → the **Key** column shows
listener ids; then `track-activity` → **Messages** → the **Key** column shows track ids. Same
values, different keys.

> Give the CLI version too and run it if the browser misbehaves. A browser is the most likely
> thing to fail in front of an audience, and the `rpk` output above proves exactly the same fact.

### Beat 3 — consumer group offsets (about 25 seconds)

DEMO-02's second requirement. Run the consumer in one pane and the group describe in another.

```bash
# pane 1
python3 src/consumer_stage1.py --thresholds config/thresholds.json --commit-every 200

# pane 2, repeatedly
watch -n1 docker exec redpanda rpk group describe consumer-stage1
```

**Point at**, in order:

1. **Before:** `STATE Dead`, `MEMBERS 0`. The group does not exist yet.
2. **During:** the group appears with one member and `TOTAL-LAG 21273`, then `73`, then `0`,
   across three consecutive one-second samples. Per-partition `CURRENT-OFFSET` climbing toward
   `LOG-END-OFFSET`.
3. **After:** lag `0` on all three partitions and the member still attached until the idle
   timeout fires.

Then the same for Consumer 2:

```bash
python3 src/consumer_stage2.py --thresholds config/thresholds.json --commit-every 200
docker exec redpanda rpk group describe consumer-stage2
```

**On restart-safety, say this rather than performing it.** Consumer 1 turns off librdkafka's
automatic offset store and commits an input offset only *after* the corresponding produce to
`track-activity` has been acknowledged. `tests/test_consumer_stage1_e2e.py` kills it mid-run with
a SIGKILL and asserts nothing is lost and nothing is double-counted. **Do not perform a live
SIGKILL.** It is a real risk with no extra credit — the test already proves it, and a botched kill
leaves the group in a rebalance you then have to explain.

### Beat 4 — the result

```bash
python3 src/summarize_review_queue.py
```

**Point at:** Consumer 2's report — 1 flagged window on track `6fe6c7a3-…`, with 901 unique
listeners, 1.00 plays per listener and a 0.9234184239733629 band share, each next to the threshold
it was measured against; and 8 flagged listeners `A000`–`A007`, found on the *other* key. Then the
summary's provenance line: `source: template`, `fell back because: no model client is configured`.

Say the honest thing about the AI element: **there is no API key here, the fallback is the path
that runs, and its bounds are proven against stubs** — a live call samples one response and cannot
prove a constraint.

### Beat 5 — a replay reproduces identical output (34 seconds — the long one)

DEMO-02's third requirement. This is PROF-02, and it already passes, so running it live is the
cheapest credible demonstration available.

```bash
python3 -m pytest tests/test_two_key_proof.py -k "replays" -q
```

**Point at:** `4 passed`. Those four tests run **two complete independent pipelines** — separate
topics, separate consumer groups, separate state directories — and assert that the flagged tracks
and all their evidence, the flagged listeners and all their evidence, and the set of `event_id`s
that reached `track-activity` are identical across both.

**Warn the room that this beat takes half a minute and prints nothing while it runs.** Knowing that
number before demo day is most of the point of rehearsing.

---

## Rehearsal record

**Run on:** 2026-08-12, starting 21:24:55 UTC. macOS 24.6.0, Docker 28.4.0, Redpanda v24.2.7,
Python 3.12. Broker wiped with `docker compose down -v`; `state/` and `output/` deleted.

### Timings

| # | Beat | Duration | Outcome |
|---|---|---|---|
| 0 | Wipe, up, clear state, create topics | 23 s | pass — both topics `OK` |
| 1 | Replay 45,473 events | 1 s | pass — queued 45473, rejected 0, delivery failures 0 |
| 2 | Consumer 1 (with offset polling) | 12 s | pass — forwarded 45473, 8 flagged listeners |
| 3 | Consumer 2 (with offset polling) | 11 s | pass — counted 45473, 1 flagged window |
| 4 | Partition dump + per-key analysis, both topics | 1 s | pass — max 1 partition per key on both |
| 5 | Re-key: one event under two keys | 2 s | pass — payload byte-identical |
| 6 | Bounded summary CLI | 1 s | pass — fell back to template, exit 0 |
| 7 | Replay identity, 4 PROF-02 tests | 34 s | pass — 4 passed, 8 deselected |
| — | **Wiped broker to last beat** | **187 s** | **all beats pass** |
| + | Stale-journal failure demo (Finding 1) | 60 s | reproduced exactly |
| + | Clean restore run afterwards | 55 s | detection content identical to the pre-rehearsal artifacts |

### Pasted output — partition assignment under each key

```
$ docker exec redpanda rpk topic describe play-events -p
PARTITION  LEADER  EPOCH  REPLICAS  LOG-START-OFFSET  HIGH-WATERMARK
0          0       1      [0]       0                 17131
1          0       1      [0]       0                 12478
2          0       1      [0]       0                 15864

$ docker exec redpanda rpk topic describe track-activity -p
PARTITION  LEADER  EPOCH  REPLICAS  LOG-START-OFFSET  HIGH-WATERMARK
0          0       1      [0]       0                 15947
1          0       1      [0]       0                 13441
2          0       1      [0]       0                 16085
```

Distinct partitions per key — the count, not a sample:

```
--- every A000 record on play-events: which partitions? ---
1800 2
--- every record for the flagged track on track-activity: which partitions? ---
1006 2
--- distinct partitions per key, worst case across ALL keys, both topics ---
play-events  max distinct partitions for any listener_id: 1
track-activity max distinct partitions for any track_id:  1
--- key cardinality on each topic ---
distinct listener_id keys on play-events:   1308
distinct track_id keys on track-activity:    464
```

All 1,800 of `A000`'s plays sit on partition 2 of `play-events`; all 1,006 records for track
`6fe6c7a3-2f67-4fa5-9ee3-724f5c57e836` sit on partition 2 of `track-activity`. The worst case over
every one of the 1,308 listener keys and every one of the 464 track keys is 1.

The re-key, one event under two keys:

```
--- the same event on play-events (partition | key | value) ---
1 | B0070 | {"schema_version":1,"event_id":"863d8ad6-0037-52ea-bbf7-ac0e600316a6","event_type":"play","listener_id":"B0070","track_id":"6fe6c7a3-2f67-4fa5-9ee3-724f5c57e836","artist_id":"f4fdbb4c-e4b7-47a0-b83b-d91bbfcfa387","played_seconds":33,"track_duration_seconds":290,"event_time":"2026-08-09T20:00:03Z"}

--- the same event on track-activity (partition | key | value) ---
2 | 6fe6c7a3-2f67-4fa5-9ee3-724f5c57e836 | {"schema_version":1,"event_id":"863d8ad6-0037-52ea-bbf7-ac0e600316a6","event_type":"play","listener_id":"B0070","track_id":"6fe6c7a3-2f67-4fa5-9ee3-724f5c57e836","artist_id":"f4fdbb4c-e4b7-47a0-b83b-d91bbfcfa387","played_seconds":33,"track_duration_seconds":290,"event_time":"2026-08-09T20:00:03Z"}

=== payload byte-identity check ===
PAYLOAD IDENTICAL across the re-key
```

Web console at <http://localhost:8080> answered `HTTP 200` in 0.0098 s and listed both topics at
3 partitions each.

### Pasted output — consumer group offsets

`consumer-stage1`, before the consumer starts:

```
GROUP        consumer-stage1
COORDINATOR  0
STATE        Dead
BALANCER
MEMBERS      0
TOTAL-LAG    0
```

During, at +1s, +2s and +4s — three consecutive one-second samples:

```
--- sample 2 at +1s ---
MEMBERS      1
TOTAL-LAG    21273
TOPIC        PARTITION  CURRENT-OFFSET  LOG-START-OFFSET  LOG-END-OFFSET  LAG
play-events  0          8649            0                 17131           8482
play-events  1          6552            0                 12478           5926
play-events  2          8999            0                 15864           6865

--- sample 3 at +2s ---
MEMBERS      1
TOTAL-LAG    73
play-events  0          17058           0                 17131           73
play-events  1          12478           0                 12478           0
play-events  2          15864           0                 15864           0

--- sample 4 at +4s ---
MEMBERS      1
TOTAL-LAG    0
play-events  0          17131           0                 17131           0
play-events  1          12478           0                 12478           0
play-events  2          15864           0                 15864           0
```

`consumer-stage2`, same shape, settling on `track-activity`:

```
--- sample 1 at +0s ---
GROUP        consumer-stage2
STATE        Dead
MEMBERS      0
TOTAL-LAG    0

--- sample 2 at +1s ---
MEMBERS         1
TOTAL-LAG       73
TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
track-activity  0          15874           15947           73
track-activity  1          13441           13441           0
track-activity  2          16085           16085           0

--- sample 3 at +2s ---
TOTAL-LAG       0
track-activity  0          15947           15947           0
track-activity  1          13441           13441           0
track-activity  2          16085           16085           0
```

Both groups: `Dead` → member attaches, lag visible → lag `0` on all three partitions, within four
seconds. One-second polling is enough to catch the fall; see [Finding 2](#findings).

### Pasted output — a replay reproducing identical output

```
$ python3 -m pytest tests/test_two_key_proof.py -k "replays" -q
....                                                                     [100%]
4 passed, 8 deselected in 33.34s
```

The four:

- `test_the_two_replays_were_genuinely_independent`
- `test_two_replays_produce_the_same_flagged_tracks_and_evidence`
- `test_two_replays_produce_the_same_flagged_listeners_and_evidence`
- `test_two_replays_put_the_same_event_ids_on_track_activity`

Independently of the tests, the rehearsal's own full run was compared against the artifacts
produced before this phase began:

```
track_review_queue.json: detection content identical = True
listener_review_queue.json: detection content identical = True
```

### Pasted output — the result

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
  flagged listeners      : 8
      A000: peak 635 plays in 24h, over 300
      A001: peak 626 plays in 24h, over 300
      A002: peak 620 plays in 24h, over 300
      A003: peak 633 plays in 24h, over 300
      A004: peak 623 plays in 24h, over 300
      A005: peak 623 plays in 24h, over 300
      A006: peak 621 plays in 24h, over 300
      A007: peak 638 plays in 24h, over 300
SUMMARY {"buckets_seen": 23302, "counted": 45473, "duplicate_event_id": 0, "flagged_buckets": 1, "flagged_listeners": 8, "flagged_tracks": ["6fe6c7a3-2f67-4fa5-9ee3-724f5c57e836"], "invalid_value": 0, "key_mismatch": 0, "polled": 45473, "tracks_seen": 464}
```

```
Reviewer notes from output/track_review_queue.json
  source: template
  fell back because: no model client is configured (OPENAI_API_KEY unset)
  notes: 1 (cap 3)
```

---

## Findings

Five things the rehearsal surfaced. **None of them is a defect in the shipped pipeline, and no
source file was changed as a result.** Five phases of verification rest on that code being exactly
what was verified.

### Finding 1 — the stale journal is worse than documented, and it was reproduced

The known trap is that a wiped broker plus a stale `state/consumer1_journal.jsonl` makes Consumer 1
forward nothing. The rehearsal reproduced it deliberately — broker wiped, journal left in place —
and it is more dangerous than that:

```
recovered 45473 event(s) from state/consumer1_journal.jsonl
Consumer 1 done. Polled 45473, forwarded 0.
  recovered from journal : 45473
  dropped, duplicate id  : 45473
  flagged listeners      : 8 ['A000', 'A001', 'A002', 'A003', 'A004', 'A005', 'A006', 'A007']
```

```
$ docker exec redpanda rpk topic describe track-activity -p
PARTITION  LEADER  EPOCH  REPLICAS  LOG-START-OFFSET  HIGH-WATERMARK
0          0       1      [0]       0                 0
1          0       1      [0]       0                 0
2          0       1      [0]       0                 0
```

**Consumer 1 still reports all 8 flagged listeners.** It rebuilds per-listener state from the
journal, so the listener half of the demo looks completely correct while `track-activity` is empty
and the track half — the half carrying the entire two-key argument — silently produces nothing.
A presenter who opens with the listener result would get several minutes in before noticing.

This is **correct behaviour**, not a bug: the journal exists so a crashed Consumer 1 can restart
without double-forwarding, and dedup across runs is exactly what it is for. It is an *operational*
trap, and the only fix is `rm -rf state` before a fresh run. It is documented in pre-flight above
and in README.md, in both cases first.

### Finding 2 — the pipeline is too fast to show lag falling without one-second polling

45,473 records clear in roughly two seconds per stage. `watch -n1` catches the fall (21273 → 73 → 0
in the pasted output above); polling by hand does not. Use `watch -n1`, start it *before* the
consumer, and do not add `--throttle` to slow it down for the audience — that would make the
demo's numbers differ from the README's.

### Finding 3 — twelve of the fourteen seconds per stage are the idle timeout

Consumer 1 took 12 s wall-clock and Consumer 2 took 11 s, of which ~10 s each is the idle timeout
before a clean exit. The actual work is about two seconds. Budget the demo around the timeouts,
not around the throughput, and tell the audience about the pause before it happens.

### Finding 4 — `rpk topic describe` without `-p` buries the useful line

The bare form prints eighteen lines of default topic config before anything interesting.
`rpk topic describe <topic> -p` prints just the per-partition table with offsets and high
watermarks, which is the thing worth pointing at. The script above uses `-p` throughout.

### Finding 5 — a cosmetic message when Consumer 1 flags nobody

On a small smoke run with zero flagged listeners, Consumer 2's report prints
`none read from …/listener_review_queue.json -- run src/consumer_stage1.py to produce it`, even
though the file exists and was read correctly — it just holds an empty list. The message conflates
"absent" with "empty". It is cosmetic, appears only when nothing is flagged, and **was not fixed**:
`src/consumer_stage2.py` is frozen and carries Phase 4's verification. Recorded here so it is not
mistaken for a real absence during a demo. It does not appear in the full-scale run, which flags
eight.

---

## If something breaks live

| Symptom | Most likely cause | Fix |
|---|---|---|
| Consumer 1 says `forwarded 0` | stale `state/consumer1_journal.jsonl` | `rm -rf state`, re-run from Beat 0 |
| Consumer 2 flags nothing | `track-activity` empty — same cause as above | same |
| A consumer seems hung | the 10-second idle timeout | wait; it exits 0 |
| `rpk` says the topic does not exist | broker was wiped after topic creation | re-run the `rpk topic create` line |
| Console at `:8080` blank | console container still starting | use the `rpk` commands; they prove the same facts |
| Replay says `delivery failures` | broker not healthy yet | `docker compose ps`, wait for `healthy`, re-run Beat 0 |

Fall back to the CLI for every beat. Every fact this demo shows in the browser is also shown by an
`rpk` command in the script above.
