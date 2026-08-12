#!/usr/bin/env python3
"""Deterministic builder for the shared 63-event fixture and its ground truth.

CD-8 / CTRT-03 ask for a small checked-in fixture plus a companion expected-flags
file. Both generated artifacts are committed; this script exists so their internal
consistency is *guaranteed* rather than hand-maintained, and so the cohorts can be
retuned in one place if `tests/test_fixture_trips_rules.py` finds they no longer
trip their configured thresholds.

Two properties are load-bearing:

1. **No randomness at all.** Not a seeded RNG -- none. Every stop time and every
   timestamp is a literal in one of the four tables below. A fixture this small has
   no value except that a human can read it and see why each event is there.

2. **One writer for both files.** `expected_flags.json` is computed from the events
   this script just emitted, so the oracle cannot silently drift away from the data
   it describes (threat T-03-01).

Run it from anywhere:

    python3 tests/fixtures/build_fixture.py

Two consecutive runs must produce byte-identical output.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

# Same REPO_ROOT bootstrap the repo's other scripts use, so this resolves however
# it is invoked and never depends on a conftest.py being present.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.play_event_v1 import (  # noqa: E402
    PlayEventV1,
    format_event_time,
    in_stop_band,
    parse_event_time,
)
from src.config import load_thresholds  # noqa: E402

CATALOG_PATH = REPO_ROOT / "data" / "catalog.json"
FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "play_events_fixture.jsonl"
EXPECTED_FLAGS_PATH = REPO_ROOT / "tests" / "fixtures" / "expected_flags.json"
INVALID_CASES_PATH = REPO_ROOT / "tests" / "fixtures" / "invalid_events.jsonl"

# Repo-relative strings for the ground-truth file, so it reads the same on any machine.
REL_FIXTURE = "tests/fixtures/play_events_fixture.jsonl"
REL_EXPECTED_FLAGS = "tests/fixtures/expected_flags.json"
REL_INVALID_CASES = "tests/fixtures/invalid_events.jsonl"
REL_FIXTURE_THRESHOLDS = "config/thresholds.fixture.json"
REL_PRODUCTION_THRESHOLDS = "config/thresholds.json"

# The real stream starts here, so the fixture reads on the same clock.
BASE_TIME = datetime(2026, 8, 8, 0, 0, 0, tzinfo=timezone.utc)

MIN_TRACK_SECONDS = 60
POOL_SIZE = 27


# --------------------------------------------------------------------------------
# Track pool
# --------------------------------------------------------------------------------
def load_track_pool() -> List[Dict[str, Any]]:
    """The fixture's 27-track pool: filter, sort, DEDUPE BY track_id, then take 27.

    Real MusicBrainz track and artist IDs keep the fixture contract-shaped and
    preserve the real track_id -> artist_id relationship the contract's section 8
    requires.

    THE DEDUPE IS NOT DEFENSIVE TIDYING. IT IS LOAD-BEARING.

    `data/catalog.json` holds 458 rows at track_duration_seconds >= 60 but only 450
    distinct track_ids: 8 MusicBrainz recordings are credited to two artists and so
    appear twice, same ID and same duration, under different artist_ids. One such
    pair sits at sorted positions 12 and 13 (037441d3-f1dc-496e-aff9-a7b402fb2df4,
    179 seconds). An undeduped 25-position slice therefore holds 24 distinct tracks,
    and the Topology A range at positions 10..21 yields ELEVEN distinct tracks, not
    twelve.

    Nothing about that fails loudly, which is exactly why it has to be handled here.
    Every event still validates. Rule B is unaffected, because the two plays land in
    different hours. What breaks is quieter: the separability invariant documented in
    `build_topology_a_events` would be false about its own data; FA01 would hold two
    events sharing a track_id under conflicting artist_ids, contradicting the section 8
    relationship this builder claims to preserve; and `expected_unflagged_tracks` would
    either carry a duplicate or silently collapse to 24, handing four later phases an
    ambiguous ground truth.

    After deduping, the pool's durations are:
        190, 215, 346, 246, 224, 494, 126, 218, 304, 128, 297, 924, 179,
        290, 196, 255, 106, 223, 113, 385, 128, 96, 170, 221, 108, 289, 285
    Positions 10..21 give 12 genuinely distinct tracks; the Topology B target at
    position 22 runs 170s; the first decoy track at position 23 runs 221s; FD07's
    track at position 24 runs 108s; the Rule A boundary track at position 25 runs
    289s and the Rule B boundary track at position 26 runs 285s.
    """
    catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    eligible = [
        t for t in catalog["tracks"] if t["track_duration_seconds"] >= MIN_TRACK_SECONDS
    ]
    eligible.sort(key=lambda t: t["track_id"])

    seen: set = set()
    pool: List[Dict[str, Any]] = []
    for track in eligible:
        if track["track_id"] in seen:
            continue
        seen.add(track["track_id"])
        pool.append(track)
        if len(pool) == POOL_SIZE:
            break

    if len(pool) != POOL_SIZE:
        raise SystemExit(
            f"catalog yielded only {len(pool)} distinct tracks of "
            f">= {MIN_TRACK_SECONDS}s; the fixture needs {POOL_SIZE}"
        )
    return pool


def at(day: int, hour: int, minute: int) -> str:
    """A contract-shaped UTC timestamp, `day` days after the base time."""
    return format_event_time(BASE_TIME + timedelta(days=day, hours=hour, minutes=minute))


# --------------------------------------------------------------------------------
# The four cohort tables. Columns:
#   (event_id, listener_id, pool_index, day, hour, minute, stop_seconds)
# --------------------------------------------------------------------------------

# Normal -- 10 listeners, 2 plays each, 20 events.
#
# Each listener gets its OWN track from pool positions 0..9, so no normal track ever
# accumulates several unique listeners inside one hour and no normal bucket can come
# near Rule B's unique-listener condition. Plays sit at distinct hours across day 0
# and day 2, avoiding day 1's hour 20 (the Topology B window) entirely.
#
# Stop times stay well outside the 30-35s band. Two of them are the boundary values
# CTRT-03 names, and both must be ACCEPTED by the model:
#   fx-n-001  played_seconds == 0
#   fx-n-003  played_seconds == track_duration_seconds  (pool[1] runs 215s)
NORMAL_PLAYS: List[Tuple[str, str, int, int, int, int, int]] = [
    ("fx-n-001", "FL01", 0, 0, 1, 5, 0),  # boundary: zero seconds played
    ("fx-n-002", "FL01", 0, 2, 9, 12, 58),
    ("fx-n-003", "FL02", 1, 0, 2, 17, 215),  # boundary: played == duration (215s)
    ("fx-n-004", "FL02", 1, 2, 10, 3, 12),
    ("fx-n-005", "FL03", 2, 0, 3, 22, 95),
    ("fx-n-006", "FL03", 2, 2, 11, 41, 120),
    ("fx-n-007", "FL04", 3, 0, 4, 8, 12),
    ("fx-n-008", "FL04", 3, 2, 12, 30, 110),
    ("fx-n-009", "FL05", 4, 0, 5, 44, 58),
    ("fx-n-010", "FL05", 4, 2, 13, 19, 95),
    ("fx-n-011", "FL06", 5, 0, 6, 11, 120),
    ("fx-n-012", "FL06", 5, 2, 14, 52, 12),
    ("fx-n-013", "FL07", 6, 0, 7, 36, 95),
    ("fx-n-014", "FL07", 6, 2, 15, 7, 58),
    ("fx-n-015", "FL08", 7, 0, 8, 29, 110),
    ("fx-n-016", "FL08", 7, 2, 16, 25, 120),
    ("fx-n-017", "FL09", 8, 0, 9, 50, 12),
    ("fx-n-018", "FL09", 8, 2, 17, 38, 95),
    ("fx-n-019", "FL10", 9, 0, 10, 14, 58),
    ("fx-n-020", "FL10", 9, 2, 18, 46, 110),
]

# Topology A -- one listener FA01, 12 plays, all inside a single 24h window on day 0,
# at distinct hours, on 12 distinct tracks drawn from pool positions 10..21. This
# mirrors the real generator's "spread across the whole catalog" shape.
#
# Every stop time is >= 40 seconds. See build_topology_a_events() for why that floor
# is kept even though it is not what protects separability at fixture scale.
TOPOLOGY_A_PLAYS: List[Tuple[str, str, int, int, int, int, int]] = [
    ("fx-a-001", "FA01", 10, 0, 0, 3, 45),
    ("fx-a-002", "FA01", 11, 0, 1, 28, 60),
    ("fx-a-003", "FA01", 12, 0, 2, 41, 72),
    ("fx-a-004", "FA01", 13, 0, 3, 9, 88),
    ("fx-a-005", "FA01", 14, 0, 4, 33, 41),
    ("fx-a-006", "FA01", 15, 0, 5, 17, 95),
    ("fx-a-007", "FA01", 16, 0, 6, 54, 52),
    ("fx-a-008", "FA01", 17, 0, 7, 22, 66),
    ("fx-a-009", "FA01", 18, 0, 8, 46, 79),
    ("fx-a-010", "FA01", 19, 0, 9, 8, 104),
    ("fx-a-011", "FA01", 20, 0, 10, 39, 57),
    ("fx-a-012", "FA01", 21, 0, 11, 51, 48),
]

# Topology B -- 8 listeners, exactly one play each, all on the single target track at
# pool position 22, all inside the one clock hour starting 2026-08-09T20:00:00Z.
#
# Seven of the eight stop inside the 30-35s band, and BOTH EDGES appear deliberately
# (30 and 35): a future exclusive-upper-bound regression would change this fixture's
# measured band share from 0.875 to 0.625 -- TWO events sit at 35, not one -- and get
# caught by the trip test rather than silently shifting a percentage. The eighth stops
# at 48, outside the band.
#
# Result: 8 unique listeners, 1.0 plays per listener, band share 0.875.
TOPOLOGY_B_PLAYS: List[Tuple[str, str, int, int, int, int, int]] = [
    ("fx-b-001", "FB01", 22, 1, 20, 2, 30),  # lower band edge, inclusive
    ("fx-b-002", "FB02", 22, 1, 20, 9, 31),
    ("fx-b-003", "FB03", 22, 1, 20, 15, 32),
    ("fx-b-004", "FB04", 22, 1, 20, 22, 33),
    ("fx-b-005", "FB05", 22, 1, 20, 30, 34),
    ("fx-b-006", "FB06", 22, 1, 20, 37, 35),  # upper band edge, inclusive
    ("fx-b-007", "FB07", 22, 1, 20, 44, 35),  # upper band edge again, on purpose
    ("fx-b-008", "FB08", 22, 1, 20, 51, 48),  # outside the band -> share is 7/8
]

# Decoys -- 7 listeners, 8 events, existing solely so the trip test can prove that
# each Topology B condition ALONE is insufficient. Without them the fixture has no
# near-miss at all: every non-Topology-B bucket would hold exactly one unique
# listener, so two of the three conditions would have nothing that satisfies them in
# isolation and "all three must hold together" would be untestable at fixture scale.
#
# Decoy 1 -- a genuinely popular track. FD01..FD06, one play each, on pool position
# 23, inside one clock hour on day 0 well away from hour 20 of day 1. Stop times are
# ordinary, none in the band. That bucket clears conditions 1 and 2 (6 unique
# listeners, ratio 1.0) and fails condition 3 (band share 0.0), so it stays
# unflagged. This is also the track ROADMAP Phase 4 success criterion 2 requires --
# high unique-listener volume with ordinary stop times, correctly not flagged --
# built now so Phase 4 need not regenerate the fixture and re-baseline the oracle.
#
# Decoy 2 -- one repeat listener. FD07, 2 plays on pool position 24 inside a single
# clock hour on day 2, both inside the band. That bucket clears condition 3 alone
# (band share 1.0) and fails conditions 1 and 2 (1 unique listener, ratio 2.0).
# Keeping this as its own listener rather than reusing a normal one leaves the normal
# cohort uniformly outside the band, which is what makes the fixture readable.
#
# Neither decoy listener comes near the Rule A threshold -- one play and two plays
# against a fixture threshold of 10 -- so they change nothing about Topology A.
DECOY_PLAYS: List[Tuple[str, str, int, int, int, int, int]] = [
    ("fx-d-001", "FD01", 23, 0, 15, 4, 55),
    ("fx-d-002", "FD02", 23, 0, 15, 11, 70),
    ("fx-d-003", "FD03", 23, 0, 15, 19, 88),
    ("fx-d-004", "FD04", 23, 0, 15, 27, 102),
    ("fx-d-005", "FD05", 23, 0, 15, 35, 12),
    ("fx-d-006", "FD06", 23, 0, 15, 48, 47),
    ("fx-d-007", "FD07", 24, 2, 21, 6, 31),  # in band
    ("fx-d-008", "FD07", 24, 2, 21, 33, 33),  # in band -> share 1.0, ratio 2.0
]

# Boundaries -- 6 listeners, 15 events, existing solely to pin the COMPARISON
# OPERATORS. Every other cohort sits comfortably clear of its threshold, so before
# these existed a detector could flip `>` to `>=` (or `>=` to `>`) and the whole
# fixture would still pass. Counts jumped 12 -> 2 with nothing at 10; bucket uniques
# went 8 -> 6 -> 1 with nothing at 5; band shares went 1.0 -> 0.875 -> 0.0 with
# nothing at 0.60. These cases sit EXACTLY on the line.
#
# Boundary A -- FN01, exactly 10 plays inside one 24h window on day 1 at distinct
# hours, all on pool position 25. CD-4 says "MORE THAN 300 plays", strictly, and
# `topology_a_plays_over` is named `_over` to carry that. At fixture scale the
# threshold is 10, so FN01 sits at exactly 10 and must stay UNFLAGGED. Flip Phase 3's
# comparison to `>=` and FN01 flags, which is the regression this cohort exists to
# catch. Stop times stay >= 40s and each play lands in its own hour, so every bucket
# holds one listener and none of this touches Rule B.
#
# Boundary B -- FN02..FN06, one play each on pool position 26 inside a single clock
# hour on day 2, three of the five inside the 30-35s band. That is exactly 5 unique
# listeners against a threshold of 5, and a band share of exactly 3/5 = 0.60 against
# a threshold of 0.60, with a ratio of 1.0. Both of those conditions use `>=`, so
# this bucket MUST be flagged, and flipping either to `>` unflags it. It is the
# fixture's second flagged track, which is why `expected_flagged_tracks` holds two.
#
# Not pinned, deliberately: `topology_b_max_plays_per_listener`. An exact 1.1 ratio
# needs 11 plays across 10 unique listeners -- 11 more events for the least
# consequential of the four operators, since real fraud sits at 1.0 and 1.1 is
# already a fudge factor. Recorded in the oracle's `boundaries.not_pinned` so the
# gap is visible rather than forgotten.
BOUNDARY_PLAYS: List[Tuple[str, str, int, int, int, int, int]] = [
    ("fx-y-001", "FN01", 25, 1, 0, 5, 45),
    ("fx-y-002", "FN01", 25, 1, 1, 12, 60),
    ("fx-y-003", "FN01", 25, 1, 2, 26, 72),
    ("fx-y-004", "FN01", 25, 1, 3, 40, 88),
    ("fx-y-005", "FN01", 25, 1, 4, 8, 41),
    ("fx-y-006", "FN01", 25, 1, 5, 33, 95),
    ("fx-y-007", "FN01", 25, 1, 6, 19, 52),
    ("fx-y-008", "FN01", 25, 1, 7, 47, 66),
    ("fx-y-009", "FN01", 25, 1, 8, 2, 79),
    ("fx-y-010", "FN01", 25, 1, 9, 55, 104),  # exactly 10 plays -> NOT flagged
    ("fx-y-011", "FN02", 26, 2, 8, 3, 31),  # in band
    ("fx-y-012", "FN03", 26, 2, 8, 14, 33),  # in band
    ("fx-y-013", "FN04", 26, 2, 8, 27, 35),  # in band, upper edge
    ("fx-y-014", "FN05", 26, 2, 8, 39, 55),  # outside band
    ("fx-y-015", "FN06", 26, 2, 8, 50, 70),  # outside -> share exactly 3/5 = 0.60
]

COHORT_BY_PREFIX = {
    "FL": "normal",
    "FA": "topology_a",
    "FB": "topology_b",
    "FD": "decoy",
    "FN": "boundary",
}

# Window starts named by the decoy design, asserted against the data below.
DECOY_POPULAR_WINDOW_START = at(0, 15, 0)
DECOY_REPEAT_WINDOW_START = at(2, 21, 0)
BOUNDARY_B_WINDOW_START = at(2, 8, 0)


def cohort_of(listener_id: str) -> str:
    """Cohort from the listener ID prefix.

    No event carries a cohort label. Contract section 7 is explicit that scenario
    labels live in a separate ground-truth fixture and never inside a Kafka event,
    because a real production event would not carry one. The prefix is exactly how
    the real generator's manifest counts its own cohorts.
    """
    return COHORT_BY_PREFIX[listener_id[:2]]


def build_events(pool: List[Dict[str, Any]]) -> List[PlayEventV1]:
    """Materialise all 63 events, validating each through the shared model."""
    events: List[PlayEventV1] = []
    for table in (
        NORMAL_PLAYS,
        TOPOLOGY_A_PLAYS,
        TOPOLOGY_B_PLAYS,
        DECOY_PLAYS,
        BOUNDARY_PLAYS,
    ):
        for event_id, listener_id, pool_index, day, hour, minute, stop in table:
            track = pool[pool_index]
            duration = track["track_duration_seconds"]
            # Clamp EVERY cohort's stop time, not just Topology A's.
            #
            # Once the pool is sorted by track_id and deduped, the first 25 eligible
            # tracks all run 96 seconds or longer, so no literal above exceeds its
            # track today and this clamp never fires. That property belongs to the
            # SORT, not to the catalog: in file order the same >= 60 filter yields
            # tracks as short as 63 seconds, and an unclamped 95 would stall the
            # builder on _played_within_duration the first time it ran. Keep the
            # clamp. It is one line, and it is the line someone deletes after
            # reading only the first half of this paragraph.
            played_seconds = min(stop, duration)
            events.append(
                PlayEventV1.model_validate(
                    {
                        "schema_version": 1,
                        "event_id": event_id,
                        "event_type": "play",
                        "listener_id": listener_id,
                        "track_id": track["track_id"],
                        "artist_id": track["artist_id"],
                        "played_seconds": played_seconds,
                        "track_duration_seconds": duration,
                        "event_time": at(day, hour, minute),
                    }
                )
            )

    # Nondecreasing event_time matters because this file is replayable and the
    # consumers assume that ordering (CD-7, contract section 5).
    events.sort(key=lambda e: (e.event_time, e.event_id))
    return events


def assert_design_invariants(events: List[PlayEventV1]) -> None:
    """Fail loudly here rather than emit a fixture that quietly lies about itself."""
    by_id = {e.event_id: e for e in events}

    if len(events) != 63:
        raise SystemExit(f"expected 63 events, built {len(events)}")

    # The two CTRT-03 boundary values, pinned by identity rather than by hope. If the
    # catalog ever shifts a duration, this fails instead of silently demoting
    # fx-n-003 from "played == duration" to "played happens to be 215".
    if by_id["fx-n-001"].played_seconds != 0:
        raise SystemExit("fx-n-001 must carry the played_seconds == 0 boundary")
    if by_id["fx-n-003"].played_seconds != by_id["fx-n-003"].track_duration_seconds:
        raise SystemExit(
            "fx-n-003 must carry the played_seconds == track_duration_seconds boundary"
        )

    # Section 8's relationship: one artist per track. This is what the pool dedupe
    # buys, and it is asserted rather than assumed.
    per_track_artists = defaultdict(set)
    for event in events:
        per_track_artists[event.track_id].add(event.artist_id)
    conflicted = {t: a for t, a in per_track_artists.items() if len(a) > 1}
    if conflicted:
        raise SystemExit(f"track_id(s) carrying two artist_ids: {sorted(conflicted)}")

    # Topology A's 12 plays must sit on 12 GENUINELY DISTINCT tracks -- see
    # build_topology_a_separability_note() for why this is the invariant that matters.
    topology_a = [e for e in events if e.listener_id == "FA01"]
    if len(topology_a) != 12:
        raise SystemExit(f"FA01 must have 12 plays, has {len(topology_a)}")
    if len({e.track_id for e in topology_a}) != 12:
        raise SystemExit("FA01's 12 plays are not on 12 distinct track_ids")
    if any(e.played_seconds < 40 for e in topology_a):
        raise SystemExit("every Topology A stop time must be >= 40 seconds")


def build_topology_a_separability_note() -> str:
    """Which invariant actually protects separability -- the two are easy to confuse.

    AT FIXTURE SCALE it is the 12 DISTINCT TRACKS, distinct only because the pool was
    deduped by track_id. Each of FA01's buckets holds a single play by a single
    listener, so every one fails Rule B's unique-listener condition long before band
    share is ever consulted. The 40-second stop-time floor is NOT what saves it here.

    KEEP THE FLOOR ANYWAY. CONTRACT-DECISIONS.md section 6 is explicit that at full
    scale, where Topology A's 600 daily plays do concentrate, both fraud cohorts
    clustering inside the 30-35s band would trigger each other's rules and the
    separability this project exists to demonstrate would fail by construction.

    Both facts are stated so that a later "simplification" concentrating Topology A
    onto fewer tracks -- or dropping the floor -- does not quietly remove the thing
    that was doing the work.
    """
    return (
        "At fixture scale, separability is protected by FA01's 12 distinct track_ids "
        "(each Rule B bucket holds one listener, failing the unique-listener "
        "condition), not by the 40s stop floor. The floor is the FULL-SCALE "
        "protection: per CONTRACT-DECISIONS.md section 6, concentrated Topology A "
        "plays inside the 30-35s band would trip Rule B and destroy separability."
    )


# --------------------------------------------------------------------------------
# Ground truth
# --------------------------------------------------------------------------------
def hour_bucket(event: PlayEventV1) -> str:
    return format_event_time(parse_event_time(event.event_time).replace(minute=0, second=0))


def max_window_count(
    listener_events: List[PlayEventV1], window_hours: int
) -> Tuple[int, str]:
    """Max plays in any rolling window anchored at one of this listener's own events."""
    times = sorted(parse_event_time(e.event_time) for e in listener_events)
    window = timedelta(hours=window_hours)
    best_count, best_start = 0, times[0]
    for start in times:
        count = sum(1 for t in times if start <= t < start + window)
        if count > best_count:
            best_count, best_start = count, start
    return best_count, format_event_time(best_start)


def bucket_stats(bucket_events: List[PlayEventV1]) -> Dict[str, Any]:
    unique_listeners = len({e.listener_id for e in bucket_events})
    total_plays = len(bucket_events)
    in_band = sum(1 for e in bucket_events if in_stop_band(e.played_seconds))
    return {
        "unique_listeners": unique_listeners,
        "total_plays": total_plays,
        "plays_per_listener": round(total_plays / unique_listeners, 6),
        "band_share": round(in_band / total_plays, 6),
    }


def build_expected_flags(events: List[PlayEventV1]) -> Dict[str, Any]:
    """Compute the oracle from the events just emitted, then check it against design.

    Every number below is measured, not typed. The design assertions at the end are
    what turn a retuned cohort that no longer trips its rule into a loud builder
    failure rather than a silently weaker fixture.
    """
    thresholds = load_thresholds(REL_FIXTURE_THRESHOLDS)

    by_listener: Dict[str, List[PlayEventV1]] = defaultdict(list)
    for event in events:
        by_listener[event.listener_id].append(event)

    # --- Rule A ---------------------------------------------------------------
    listener_scores = {
        listener: max_window_count(evts, thresholds.topology_a_window_hours)
        for listener, evts in by_listener.items()
    }
    # CD-4 is a strict "more than" comparison.
    flagged_listeners = sorted(
        listener
        for listener, (count, _) in listener_scores.items()
        if count > thresholds.topology_a_plays_over
    )

    # --- Rule B ---------------------------------------------------------------
    buckets: Dict[Tuple[str, str], List[PlayEventV1]] = defaultdict(list)
    for event in events:
        buckets[(event.track_id, hour_bucket(event))].append(event)

    flagged_buckets = []
    for (track_id, window_start), bucket_events in sorted(buckets.items()):
        stats = bucket_stats(bucket_events)
        if (
            stats["unique_listeners"] >= thresholds.topology_b_min_unique_listeners
            and stats["plays_per_listener"] <= thresholds.topology_b_max_plays_per_listener
            and stats["band_share"] >= thresholds.topology_b_min_band_share
        ):
            flagged_buckets.append((track_id, window_start, stats))

    # --- Design assertions ----------------------------------------------------
    if flagged_listeners != ["FA01"]:
        raise SystemExit(
            f"fixture design broken: expected exactly ['FA01'] flagged by Rule A at "
            f"{REL_FIXTURE_THRESHOLDS}, got {flagged_listeners}. Retune the cohorts "
            f"here or the numbers in the fixture config -- never the assertions."
        )
    # Two buckets flag by design: the Topology B burst, and the Boundary B bucket
    # sitting exactly on the unique-listener and band-share thresholds.
    if len(flagged_buckets) != 2:
        raise SystemExit(
            f"fixture design broken: expected exactly 2 Rule B buckets flagged at "
            f"{REL_FIXTURE_THRESHOLDS} (the Topology B burst and the Boundary B "
            f"bucket), got {len(flagged_buckets)}"
        )

    a_count, a_window_start = listener_scores["FA01"]

    boundary_rows = [r for r in flagged_buckets if r[1] == BOUNDARY_B_WINDOW_START]
    burst_rows = [r for r in flagged_buckets if r[1] != BOUNDARY_B_WINDOW_START]
    if len(boundary_rows) != 1 or len(burst_rows) != 1:
        raise SystemExit(
            "fixture design broken: could not tell the Topology B burst apart from "
            "the Boundary B bucket by window start"
        )
    b_track_id, b_window_start, b_stats = burst_rows[0]
    boundary_b_track_id = boundary_rows[0][0]

    # FN01 sits exactly on the Rule A threshold and must NOT be flagged. Assert it
    # here so a threshold change that silently swallows the boundary case fails the
    # build rather than the test suite.
    fn01_count = listener_scores["FN01"][0]
    if fn01_count != thresholds.topology_a_plays_over:
        raise SystemExit(
            f"fixture design broken: FN01 must sit exactly on "
            f"topology_a_plays_over ({thresholds.topology_a_plays_over}), got "
            f"{fn01_count}"
        )
    if "FN01" in flagged_listeners:
        raise SystemExit(
            "fixture design broken: FN01 sits exactly on the Rule A threshold and "
            "must stay unflagged -- CD-4 is strictly MORE THAN"
        )

    all_listeners = sorted(by_listener)
    all_tracks = sorted({e.track_id for e in events})

    counts_by_cohort: Dict[str, int] = {
        "normal": 0,
        "topology_a": 0,
        "topology_b": 0,
        "decoy": 0,
        "boundary": 0,
    }
    for event in events:
        counts_by_cohort[cohort_of(event.listener_id)] += 1

    def decoy_block(track_id: str, window_start: str) -> Dict[str, Any]:
        bucket = buckets[(track_id, window_start)]
        stats = bucket_stats(bucket)
        satisfies, fails = [], []
        for name, ok in (
            (
                "min_unique_listeners",
                stats["unique_listeners"] >= thresholds.topology_b_min_unique_listeners,
            ),
            (
                "max_plays_per_listener",
                stats["plays_per_listener"]
                <= thresholds.topology_b_max_plays_per_listener,
            ),
            (
                "min_band_share",
                stats["band_share"] >= thresholds.topology_b_min_band_share,
            ),
        ):
            (satisfies if ok else fails).append(name)
        if not fails:
            raise SystemExit(
                f"decoy bucket {track_id} @ {window_start} satisfies all three "
                f"Topology B conditions -- it is no longer a near miss"
            )
        return {
            "track_id": track_id,
            "window_start": window_start,
            "listeners": sorted({e.listener_id for e in bucket}),
            **stats,
            "satisfies_conditions": satisfies,
            "fails_conditions": fails,
            "expected_flagged": False,
        }

    return {
        "generated_by": "tests/fixtures/build_fixture.py",
        "fixture_path": REL_FIXTURE,
        "thresholds_path": REL_FIXTURE_THRESHOLDS,
        "production_thresholds_path": REL_PRODUCTION_THRESHOLDS,
        "invalid_cases_path": REL_INVALID_CASES,
        "invalid_cases_note": (
            "Invalid records live outside the replayable fixture. Nothing prevents "
            "someone pointing src/replay_to_kafka.py --input at that file -- the flag "
            "takes any path -- but every line fails PlayEventV1.model_validate_json at "
            "the envelope's top level, so the replay producer rejects all of them and "
            "places no record on play-events. The file is structurally incapable of "
            "contaminating the topic, not structurally impossible to open."
        ),
        "cohort_labels_note": (
            "No fixture event carries a cohort label. Contract section 7 keeps scenario "
            "labels in this ground-truth file because a real production event would not "
            "carry one; the cohort is inferable from the listener ID prefix."
        ),
        "separability_note": build_topology_a_separability_note(),
        "total_events": len(events),
        "counts_by_cohort": counts_by_cohort,
        "topology_a": {
            "listener_id": "FA01",
            "plays_in_window": a_count,
            "window_start": a_window_start,
            "window_hours": thresholds.topology_a_window_hours,
            "distinct_tracks": len({e.track_id for e in by_listener["FA01"]}),
        },
        "topology_b": {
            "track_id": b_track_id,
            "window_start": b_window_start,
            "window_hours": thresholds.topology_b_window_hours,
            "unique_listeners": b_stats["unique_listeners"],
            "total_plays": b_stats["total_plays"],
            "plays_per_listener": b_stats["plays_per_listener"],
            "band_share": b_stats["band_share"],
        },
        "expected_flagged_listeners": flagged_listeners,
        "expected_unflagged_listeners": [
            listener for listener in all_listeners if listener not in flagged_listeners
        ],
        "expected_flagged_tracks": sorted({b_track_id, boundary_b_track_id}),
        "expected_unflagged_tracks": [
            t for t in all_tracks if t not in {b_track_id, boundary_b_track_id}
        ],
        "boundaries": {
            "note": (
                "These cases sit EXACTLY on a threshold so a detector cannot flip a "
                "comparison operator and still pass. Every other cohort clears its "
                "threshold with margin."
            ),
            "topology_a_strict_greater_than": {
                "listener_id": "FN01",
                "plays_in_window": 10,
                "threshold": thresholds.topology_a_plays_over,
                "expected_flagged": False,
                "why": (
                    "CD-4 says MORE THAN, strictly. 10 is not > 10. Flip the "
                    "comparison to >= and FN01 flags."
                ),
            },
            "topology_b_unique_listeners_and_band_share": {
                "track_id": boundary_b_track_id,
                "window_start": BOUNDARY_B_WINDOW_START,
                "unique_listeners": 5,
                "unique_threshold": thresholds.topology_b_min_unique_listeners,
                "band_share": 0.6,
                "band_share_threshold": thresholds.topology_b_min_band_share,
                "plays_per_listener": 1.0,
                "expected_flagged": True,
                "why": (
                    "Both conditions use >=, so a bucket sitting exactly on both "
                    "thresholds must flag. Flip either to > and it unflags."
                ),
            },
            "not_pinned": {
                "topology_b_max_plays_per_listener": (
                    "An exact 1.1 ratio needs 11 plays across 10 unique listeners. "
                    "That is 11 more events for the least consequential operator, "
                    "since real fraud sits at 1.0 and 1.1 is already a fudge factor. "
                    "Deliberate gap, recorded so it is visible rather than forgotten."
                )
            },
        },
        "decoys": {
            "popular_track": decoy_block(
                next(e.track_id for e in events if e.event_id == "fx-d-001"),
                DECOY_POPULAR_WINDOW_START,
            ),
            "repeat_listener": decoy_block(
                next(e.track_id for e in events if e.event_id == "fx-d-007"),
                DECOY_REPEAT_WINDOW_START,
            ),
        },
    }


def main() -> None:
    pool = load_track_pool()
    events = build_events(pool)
    assert_design_invariants(events)
    expected_flags = build_expected_flags(events)

    FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE_PATH.write_text(
        "".join(f"{e.model_dump_json()}\n" for e in events), encoding="utf-8"
    )
    EXPECTED_FLAGS_PATH.write_text(
        json.dumps(expected_flags, indent=2, ensure_ascii=True) + "\n", encoding="utf-8"
    )

    print(f"wrote {len(events)} events -> {REL_FIXTURE}")
    print(f"wrote ground truth   -> {REL_EXPECTED_FLAGS}")
    print(
        f"  flagged listener(s): {expected_flags['expected_flagged_listeners']}; "
        f"flagged track(s): {len(expected_flags['expected_flagged_tracks'])}; "
        f"decoy buckets: {len(expected_flags['decoys'])}"
    )
    if not INVALID_CASES_PATH.exists():
        print(f"  note: {REL_INVALID_CASES} does not exist yet (built by task 2)")


if __name__ == "__main__":
    main()
