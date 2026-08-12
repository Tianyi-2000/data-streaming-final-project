# Producer–Consumer Contract (Draft v0.1)

**Status:** Proposed — Tianyi and P.J. must approve before implementation  
**Scope:** Minimum two-topic Kafka pipeline  
**Related design:** [Two-Key Pipeline](DESIGN-two-key-pipeline.md) and [Artificial Streaming Research](RESEARCH-artificial-streaming-fraud.md)

## 1. Purpose

This contract lets Tianyi build the replay producer and P.J. build the two consumer stages independently without their code drifting apart.

The minimum pipeline is:

1. Tianyi produces validated play events to `play-events`, keyed by `listener_id`.
2. Consumer 1 validates each event and re-publishes the same payload to `track-activity`, keyed by `track_id`.
3. Consumer 2 aggregates `track-activity` records by track and creates review candidates.

The minimum version includes only play behavior and playback duration. It does not include separate `save`, `follow`, or `playlist_add` events.

## 2. Decisions Already Agreed

| Decision | Minimum-version choice |
|---|---|
| Input behavior | Play events only |
| Source of listening behavior | Deterministic synthetic data |
| Music metadata | Small MusicBrainz cache fetched before the producer runs |
| Topic 1 | `play-events` |
| Topic 1 key | `listener_id` |
| Topic 2 | `track-activity` |
| Topic 2 key | `track_id` |
| Serialization | UTF-8 JSON, validated with Pydantic |
| Timestamp | UTC ISO 8601 ending in `Z` |
| Synthetic traffic | Normal traffic + Topology A + Topology B |

The producer must not call MusicBrainz while replaying events. It reads a local cached metadata file instead.

## 3. Topic 1 Contract: `play-events`

### Kafka record

| Property | Contract |
|---|---|
| Topic | `play-events` |
| Record key | UTF-8 encoded `listener_id` |
| Record value | JSON matching `PlayEventV1` |
| Producer owner | Tianyi |
| Consumer owner | P.J. / Consumer 1 |

### `PlayEventV1` value schema

| Field | Type | Required | Rule / source |
|---|---|---:|---|
| `schema_version` | integer | Yes | Must equal `1` |
| `event_id` | string | Yes | Unique and stable across replay runs |
| `event_type` | string | Yes | Must equal `"play"` |
| `listener_id` | string | Yes | Non-empty synthetic listener ID |
| `track_id` | string | Yes | MusicBrainz recording ID from the metadata cache |
| `artist_id` | string | Yes | MusicBrainz artist ID associated with `track_id` |
| `played_seconds` | integer | Yes | `0 <= played_seconds <= track_duration_seconds` |
| `track_duration_seconds` | integer | Yes | Must be greater than `0`; derived from cached metadata |
| `event_time` | string | Yes | UTC ISO 8601, for example `2026-08-08T20:30:00Z` |

Track title and artist name are intentionally excluded from the Kafka event in v1. They remain available in the local metadata cache for display or later enrichment.

### Valid example

Kafka key:

```text
listener-00042
```

Kafka value:

```json
{
  "schema_version": 1,
  "event_id": "evt-000001",
  "event_type": "play",
  "listener_id": "listener-00042",
  "track_id": "7e1dca28-5c2f-4b8c-9d19-bdd8c01fe5ac",
  "artist_id": "ad279295-653f-4f25-a10c-6383a1d2d4db",
  "played_seconds": 31,
  "track_duration_seconds": 214,
  "event_time": "2026-08-08T20:30:00Z"
}
```

The sample MusicBrainz IDs above are placeholders for contract illustration. The generator must use IDs and relationships from the actual cached metadata file.

## 4. Topic 2 Contract: `track-activity`

### Kafka record

| Property | Contract |
|---|---|
| Topic | `track-activity` |
| Record key | UTF-8 encoded `track_id` |
| Record value | The same `PlayEventV1` JSON received from `play-events` |
| Producer owner | P.J. / Consumer 1 |
| Consumer owner | P.J. / Consumer 2 |

For the minimum version, Consumer 1 does not aggregate before writing to `track-activity`. It validates the event, preserves all value fields, changes the Kafka key from `listener_id` to `track_id`, and produces the record to the second topic.

Consumer 1 may maintain per-listener state to detect Topology A, but those derived features do not need to be added to the `track-activity` payload in v1.

Example transformation:

| Input | Output |
|---|---|
| Topic: `play-events` | Topic: `track-activity` |
| Key: `listener-00042` | Key: `7e1dca28-5c2f-4b8c-9d19-bdd8c01fe5ac` |
| Value: `PlayEventV1` | Value: unchanged `PlayEventV1` |

## 5. Ordering and Replay Rules

- Kafka guarantees order only within a partition.
- In `play-events`, records for the same `listener_id` must reach the same partition and preserve that listener's order.
- In `track-activity`, records for the same `track_id` must reach the same partition and preserve that track's order.
- There is no global order across all listeners, tracks, partitions, or topics.
- The producer emits events in nondecreasing `event_time` order, so consumers can advance a single watermark instead of buffering late arrivals. This guarantee holds per replay of the seeded file, not across repeated replays into the same topic, since a second replay restarts at the same beginning timestamp. This records an existing guarantee rather than new producer work: `src/generate_events.py` already sorts by `(event_time, event_id)` before writing, and `data/play_events_manifest.json` declares `"event_time_order": "nondecreasing"`.
- Replaying the same seeded file must reuse the same `event_id` values.
- Consumers must use `event_id` for idempotent handling or deduplication.
- For a valid input record, Consumer 1 commits its input offset only after producing the corresponding `track-activity` record successfully.

## 6. Validation and Invalid Records

Tianyi's producer validates every event before serialization. P.J.'s Consumer 1 validates again after deserialization and before writing to `track-activity`.

An event is invalid if, for example:

- a required field is missing;
- the Kafka key does not equal the value's `listener_id` on `play-events`;
- `event_type` is not `"play"`;
- an ID is empty;
- `played_seconds` is negative or greater than `track_duration_seconds`;
- `track_duration_seconds` is not positive; or
- `event_time` is not a valid UTC timestamp.

The key-versus-value rule is the one item on that list the shared model cannot enforce. `PlayEventV1` validates the Kafka *value* and never sees the key, so no validator on the model can compare the two. The producer guarantees the match structurally, by deriving the key with `play_events_key()`. Consumer 1 must therefore assert it explicitly, using `key_matches_listener_id` from `contracts/play_event_v1.py`.

Minimum-version behavior:

- The producer logs and rejects invalid generated events instead of sending them to Kafka.
- If Consumer 1 receives an invalid record, it logs the rejection and does not send it to `track-activity`.
- A third dead-letter topic is out of scope for v1.

Invalid example:

```json
{
  "schema_version": 1,
  "event_id": "evt-invalid-001",
  "event_type": "play",
  "listener_id": "listener-00042",
  "track_id": "track-001",
  "artist_id": "artist-001",
  "played_seconds": 250,
  "track_duration_seconds": 214,
  "event_time": "2026-08-08T20:30:00Z"
}
```

This record is invalid because `played_seconds` is greater than `track_duration_seconds`.

## 7. Synthetic Data Scenarios

Scenario labels and expected answers must be stored in a separate ground-truth fixture. They must not appear inside Kafka events, because real production events would not contain a fraud label.

| Scenario | Generated shape | Expected visibility |
|---|---|---|
| Normal | Many listeners, varied tracks, varied playback durations, and non-synchronized event times | Should not be placed in the review queue |
| Topology A | A small listener cohort generates high play volume spread across many tracks | Visible in Consumer 1's per-listener state |
| Topology B | Many distinct listeners each play one target track once; playback durations cluster around 30–35 seconds within a concentrated time window | Visible in Consumer 2's per-track state |

### Important detection note

`streams_per_unique_listener > threshold` cannot detect Topology B by itself. If 53,000 listeners each play the track once, the ratio is `1`.

With the agreed v1 fields, Consumer 2 should combine at least:

- a high `unique_listener_count` for one track within a time window; and
- a high share of `played_seconds` values in the stop band, which is `30 <= played_seconds <= 35` — inclusive at both ends.

The band includes 35 deliberately. The generator spreads Topology B stop times evenly across the six integer values 30 through 35, so treating the upper edge as exclusive discards one sixth of the signal and moves the measured band share on the fraud window from 0.923 to 0.762. Both figures clear the 0.60 threshold, which is precisely why this would never surface as a failing check — only as a wrong number. Both sides use `in_stop_band` from `contracts/play_event_v1.py` so neither re-derives the boundary.

The exact time window and thresholds belong to the consumer detection design and remain `TBD` until Tianyi and P.J. agree.

## 8. MusicBrainz Cache Boundary

Before event generation, a separate setup step fetches and caches a small set of MusicBrainz recordings. The producer uses only the cache during replay.

Each cached track must provide at least:

- `track_id` (MusicBrainz recording ID);
- `artist_id`;
- `track_duration_seconds`; and
- optional title and artist name for display only.

The cache must preserve the real `track_id -> artist_id` relationship. Synthetic behavior may choose among these real tracks, but it must not invent a mismatched artist for a track.

## 9. Acceptance Checks

Before implementation is considered compatible, both sides should be able to verify:

1. A valid example passes the shared Pydantic model.
2. An invalid example is rejected before production.
3. The `play-events` key exactly matches `listener_id`.
4. Consumer 1 preserves `event_id` and the full JSON value.
5. The `track-activity` key exactly matches `track_id`.
6. Replaying the same input produces the same event IDs and logical results.
7. The generator includes normal data and both documented anomaly topologies.

## 10. Items P.J. and Tianyi Must Confirm

- [ ] Approve the `PlayEventV1` fields and validation rules.
- [ ] Approve JSON + Pydantic for v1 serialization.
- [ ] Confirm that `track-activity` keeps the same value and only changes the Kafka key.
- [ ] Define Consumer 1's time window and threshold for Topology A.
- [ ] Define Consumer 2's time window and thresholds for Topology B.
- [ ] Agree on a small shared fixture that both producer and consumer tests will use.

Any incompatible field or semantic change after approval must update this file and increment `schema_version`.
