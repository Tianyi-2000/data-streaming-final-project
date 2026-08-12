"""The mini cohorts actually trip their rules at the fixture's configured thresholds.

This is the phase's risk test. The cohorts in `play_events_fixture.jsonl` are shrunk
for readability, so their numbers are nothing like production's. If they do not clear
the fixture-scale thresholds, Phase 5's separability proof has nothing to assert at
small scale and the failure surfaces four phases late. It is caught here instead.

WHAT THIS FILE PROVES, IN ORDER
-------------------------------
1. At fixture-scale thresholds the mini Topology A listener trips the rolling-24h
   rule, and only it does.
2. At fixture-scale thresholds the mini Topology B burst trips all three conditions
   together, and only it does.
3. Each of the three Topology B conditions is satisfied, alone, by a named decoy
   bucket that is correctly NOT flagged -- which is what "all three must hold
   together" actually means, as opposed to merely asserting it.
4. Neither fraud cohort trips the other's rule (CD-9 in miniature).
5. The identical fixture under `config/thresholds.json` flags nothing at all.
   Identical code, identical data, different file, different answer. That is CTRT-04
   demonstrated rather than asserted.

NO THRESHOLD LITERAL APPEARS IN THIS FILE. Every number comes from `load_thresholds`
or from `expected_flags.json`. That is what makes this a proof of config injection
rather than a restatement of the same numbers in a second place.

IF AN ASSERTION HERE FAILS, the sanctioned fix is retuning the cohort sizes in
`tests/fixtures/build_fixture.py` or the numbers in `config/thresholds.fixture.json`
and regenerating -- never weakening the assertion, and never touching
`contracts/play_event_v1.py`. Threat T-03-05: loosening the fixture config to force a
pass breaks `test_the_same_fixture_under_production_thresholds_flags_nothing`, which
is why that assertion is paired with these.

THE WINDOW ARITHMETIC BELOW IS DELIBERATELY LOCAL TO THIS FILE. It is about twenty
lines and it is NOT the Phase 2 windowing module. Do not extract it to
`src/windowing.py` from here; Phase 2 exists to get that logic right, and Phases 3
and 4 own the detectors.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.play_event_v1 import (  # noqa: E402
    PlayEventV1,
    format_event_time,
    in_stop_band,
    parse_event_time,
)
from src.config import Thresholds, load_thresholds  # noqa: E402

FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "play_events_fixture.jsonl"
EXPECTED_FLAGS_PATH = REPO_ROOT / "tests" / "fixtures" / "expected_flags.json"

EXPECTED: Dict[str, Any] = json.loads(EXPECTED_FLAGS_PATH.read_text(encoding="utf-8"))
EVENTS: List[PlayEventV1] = [
    PlayEventV1.model_validate_json(line)
    for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
    if line.strip()
]

FIXTURE_THRESHOLDS = load_thresholds(EXPECTED["thresholds_path"])
PRODUCTION_THRESHOLDS = load_thresholds(EXPECTED["production_thresholds_path"])


# --------------------------------------------------------------------------------
# Local window arithmetic (Phase 2 owns the real module; this is not it)
# --------------------------------------------------------------------------------
def rule_a_score(listener_events: List[PlayEventV1], window_hours: int) -> int:
    """Max plays by one listener in any rolling window anchored at its own events."""
    times = sorted(parse_event_time(e.event_time) for e in listener_events)
    window = timedelta(hours=window_hours)
    return max(sum(1 for t in times if start <= t < start + window) for start in times)


def rule_a_count_from(
    listener_events: List[PlayEventV1], window_start: str, window_hours: int
) -> int:
    """Plays inside the specific window the ground truth recorded."""
    start = parse_event_time(window_start)
    window = timedelta(hours=window_hours)
    return sum(
        1
        for e in listener_events
        if start <= parse_event_time(e.event_time) < start + window
    )


def rule_a_scores(thresholds: Thresholds) -> Dict[str, int]:
    by_listener: Dict[str, List[PlayEventV1]] = defaultdict(list)
    for event in EVENTS:
        by_listener[event.listener_id].append(event)
    return {
        listener: rule_a_score(evts, thresholds.topology_a_window_hours)
        for listener, evts in by_listener.items()
    }


def rule_a_flagged(thresholds: Thresholds) -> List[str]:
    # CD-4 is a strict "more than": a listener sitting exactly ON the threshold is
    # not flagged. A `>=` here would quietly change who gets accused.
    return sorted(
        listener
        for listener, score in rule_a_scores(thresholds).items()
        if score > thresholds.topology_a_plays_over
    )


def hour_buckets() -> Dict[Tuple[str, str], List[PlayEventV1]]:
    """Events bucketed by (track_id, event_time truncated to the hour)."""
    buckets: Dict[Tuple[str, str], List[PlayEventV1]] = defaultdict(list)
    for event in EVENTS:
        hour = parse_event_time(event.event_time).replace(
            minute=0, second=0, microsecond=0
        )
        buckets[(event.track_id, format_event_time(hour))].append(event)
    return buckets


def bucket_measures(bucket: List[PlayEventV1]) -> Tuple[int, float, float]:
    """(unique listeners, plays per listener, share of plays inside the stop band).

    The band share uses `in_stop_band` from the shared contract module rather than a
    local comparison, so this fixture and Phase 4's detector cannot disagree about
    the 30 and 35 edges.
    """
    unique = len({e.listener_id for e in bucket})
    plays = len(bucket)
    banded = sum(1 for e in bucket if in_stop_band(e.played_seconds))
    return unique, plays / unique, banded / plays


def rule_b_conditions(bucket: List[PlayEventV1], thresholds: Thresholds) -> Dict[str, bool]:
    unique, ratio, share = bucket_measures(bucket)
    return {
        "min_unique_listeners": unique >= thresholds.topology_b_min_unique_listeners,
        "max_plays_per_listener": ratio <= thresholds.topology_b_max_plays_per_listener,
        "min_band_share": share >= thresholds.topology_b_min_band_share,
    }


def rule_b_flagged(thresholds: Thresholds) -> List[Tuple[str, str]]:
    """All three conditions must hold together, per CD-5."""
    return sorted(
        key
        for key, bucket in hour_buckets().items()
        if all(rule_b_conditions(bucket, thresholds).values())
    )


# --------------------------------------------------------------------------------
# Rule A at fixture scale
# --------------------------------------------------------------------------------
def test_exactly_one_listener_trips_rule_a_and_it_is_the_recorded_one():
    assert rule_a_flagged(FIXTURE_THRESHOLDS) == EXPECTED["expected_flagged_listeners"]
    assert EXPECTED["expected_flagged_listeners"] == [EXPECTED["topology_a"]["listener_id"]]


def test_the_flagged_listener_play_count_matches_the_recorded_window():
    recorded = EXPECTED["topology_a"]
    listener_events = [e for e in EVENTS if e.listener_id == recorded["listener_id"]]
    assert (
        rule_a_count_from(
            listener_events, recorded["window_start"], recorded["window_hours"]
        )
        == recorded["plays_in_window"]
    )
    # And no window does better, so the recorded count really is the listener's score.
    assert (
        rule_a_score(listener_events, recorded["window_hours"])
        == recorded["plays_in_window"]
    )
    assert recorded["plays_in_window"] > FIXTURE_THRESHOLDS.topology_a_plays_over


def test_every_other_listener_scores_at_or_below_the_rule_a_threshold():
    """All 25 of them -- normal, Topology B and decoy alike."""
    scores = rule_a_scores(FIXTURE_THRESHOLDS)
    unflagged = EXPECTED["expected_unflagged_listeners"]
    assert sorted(scores) == sorted(unflagged + EXPECTED["expected_flagged_listeners"])
    over = {
        listener: scores[listener]
        for listener in unflagged
        if scores[listener] > FIXTURE_THRESHOLDS.topology_a_plays_over
    }
    assert not over, f"listeners expected unflagged but over the Rule A threshold: {over}"


# --------------------------------------------------------------------------------
# Rule B at fixture scale
# --------------------------------------------------------------------------------
def test_exactly_two_buckets_trip_rule_b_and_they_are_the_recorded_ones():
    """The Topology B burst, plus the Boundary B bucket sitting on both thresholds.

    Any third bucket flagging means a cohort drifted into Rule B by accident.
    """
    recorded = EXPECTED["topology_b"]
    boundary = EXPECTED["boundaries"]["topology_b_unique_listeners_and_band_share"]
    flagged = rule_b_flagged(FIXTURE_THRESHOLDS)
    assert sorted(flagged) == sorted(
        [
            (recorded["track_id"], recorded["window_start"]),
            (boundary["track_id"], boundary["window_start"]),
        ]
    )
    assert EXPECTED["expected_flagged_tracks"] == sorted(
        {recorded["track_id"], boundary["track_id"]}
    )


def test_the_flagged_bucket_measures_match_the_recorded_numbers():
    recorded = EXPECTED["topology_b"]
    bucket = hour_buckets()[(recorded["track_id"], recorded["window_start"])]
    unique, ratio, share = bucket_measures(bucket)
    assert unique == recorded["unique_listeners"]
    assert len(bucket) == recorded["total_plays"]
    assert ratio == pytest.approx(recorded["plays_per_listener"])
    assert share == pytest.approx(recorded["band_share"])
    # All three conditions hold together -- that is what flags it.
    assert all(rule_b_conditions(bucket, FIXTURE_THRESHOLDS).values())


def test_no_unflagged_track_has_a_bucket_that_trips_rule_b():
    flagged_tracks = {track for track, _ in rule_b_flagged(FIXTURE_THRESHOLDS)}
    assert flagged_tracks.isdisjoint(set(EXPECTED["expected_unflagged_tracks"]))


# --------------------------------------------------------------------------------
# Each Topology B condition, shown insufficient alone against a NAMED decoy
# --------------------------------------------------------------------------------
@pytest.mark.parametrize("decoy_name", ["popular_track", "repeat_listener"])
def test_each_decoy_bucket_measures_match_the_recorded_numbers(decoy_name: str):
    """A later change to the decoys cannot silently hollow out the tests below."""
    recorded = EXPECTED["decoys"][decoy_name]
    bucket = hour_buckets()[(recorded["track_id"], recorded["window_start"])]
    unique, ratio, share = bucket_measures(bucket)
    assert unique == recorded["unique_listeners"]
    assert len(bucket) == recorded["total_plays"]
    assert ratio == pytest.approx(recorded["plays_per_listener"])
    assert share == pytest.approx(recorded["band_share"])


@pytest.mark.parametrize("decoy_name", ["popular_track", "repeat_listener"])
def test_each_decoy_satisfies_and_fails_exactly_the_recorded_conditions(decoy_name: str):
    recorded = EXPECTED["decoys"][decoy_name]
    bucket = hour_buckets()[(recorded["track_id"], recorded["window_start"])]
    conditions = rule_b_conditions(bucket, FIXTURE_THRESHOLDS)
    assert sorted(name for name, ok in conditions.items() if ok) == sorted(
        recorded["satisfies_conditions"]
    )
    assert sorted(name for name, ok in conditions.items() if not ok) == sorted(
        recorded["fails_conditions"]
    )
    assert not all(conditions.values())
    assert (recorded["track_id"], recorded["window_start"]) not in rule_b_flagged(
        FIXTURE_THRESHOLDS
    )


def test_every_topology_b_condition_is_satisfiable_by_something_correctly_unflagged():
    """"All three must hold together" -- demonstrated, not asserted.

    The 6-listener decoy clears the unique-listener condition and the ratio condition
    while failing band share. The FD07 decoy clears band share alone, with a ratio of
    2.0 and one unique listener. Between them every condition is shown to be
    satisfiable by a bucket that is correctly not flagged.
    """
    satisfied_by_an_unflagged_bucket: set = set()
    for recorded in EXPECTED["decoys"].values():
        bucket = hour_buckets()[(recorded["track_id"], recorded["window_start"])]
        conditions = rule_b_conditions(bucket, FIXTURE_THRESHOLDS)
        assert not all(conditions.values()), "a decoy must never be flagged"
        satisfied_by_an_unflagged_bucket |= {n for n, ok in conditions.items() if ok}
    assert satisfied_by_an_unflagged_bucket == {
        "min_unique_listeners",
        "max_plays_per_listener",
        "min_band_share",
    }


def test_the_high_volume_ordinary_stop_times_track_is_not_flagged():
    """ROADMAP Phase 4 success criterion 2, rehearsed at fixture scale.

    A track with high unique-listener volume but ordinary stop times must not be
    flagged. Built here so Phase 4 need not regenerate the fixture and re-baseline
    the oracle mid-phase.
    """
    recorded = EXPECTED["decoys"]["popular_track"]
    bucket = hour_buckets()[(recorded["track_id"], recorded["window_start"])]
    unique, _, share = bucket_measures(bucket)
    assert unique >= FIXTURE_THRESHOLDS.topology_b_min_unique_listeners
    assert share < FIXTURE_THRESHOLDS.topology_b_min_band_share
    assert (recorded["track_id"], recorded["window_start"]) not in rule_b_flagged(
        FIXTURE_THRESHOLDS
    )


# --------------------------------------------------------------------------------
# Separability at fixture scale, in both directions (CD-9 in miniature)
# --------------------------------------------------------------------------------
def test_no_topology_b_listener_trips_rule_a():
    scores = rule_a_scores(FIXTURE_THRESHOLDS)
    offenders = {
        listener: score
        for listener, score in scores.items()
        if listener.startswith("FB") and score > FIXTURE_THRESHOLDS.topology_a_plays_over
    }
    assert not offenders, f"Topology B listeners tripping Rule A: {offenders}"


def test_no_topology_a_track_bucket_trips_rule_b():
    """At fixture scale the Topology A buckets fail Rule B on the unique-listener
    condition, because FA01's 12 plays sit on 12 distinct tracks -- one listener per
    bucket. The 40-second stop-time floor is the FULL-SCALE protection, not this one.
    See the separability note in `expected_flags.json` and the comment in
    `build_fixture.build_topology_a_separability_note`.
    """
    topology_a_tracks = {e.track_id for e in EVENTS if e.listener_id.startswith("FA")}
    assert topology_a_tracks, "no Topology A events found"
    flagged_tracks = {track for track, _ in rule_b_flagged(FIXTURE_THRESHOLDS)}
    assert flagged_tracks.isdisjoint(topology_a_tracks)

    # And the reason is the one documented, not an accident of the band share.
    for key, bucket in hour_buckets().items():
        if key[0] not in topology_a_tracks:
            continue
        assert not rule_b_conditions(bucket, FIXTURE_THRESHOLDS)["min_unique_listeners"]


# --------------------------------------------------------------------------------
# CTRT-04: identical code, identical data, different config file, different answer
# --------------------------------------------------------------------------------
def test_the_same_fixture_under_production_thresholds_flags_nothing():
    """12 plays does not exceed the production Rule A threshold, and 8 unique
    listeners does not reach the production Rule B one. Nothing else changes.
    """
    assert rule_a_flagged(PRODUCTION_THRESHOLDS) == []
    assert rule_b_flagged(PRODUCTION_THRESHOLDS) == []


def test_the_two_configs_really_are_different_files_with_different_numbers():
    """Guards the test above from passing vacuously on two identical configs."""
    assert (
        FIXTURE_THRESHOLDS.source_path != PRODUCTION_THRESHOLDS.source_path
    ), "the fixture and production configs resolved to the same file"
    assert (
        FIXTURE_THRESHOLDS.topology_a_plays_over
        < PRODUCTION_THRESHOLDS.topology_a_plays_over
    )
    assert (
        FIXTURE_THRESHOLDS.topology_b_min_unique_listeners
        < PRODUCTION_THRESHOLDS.topology_b_min_unique_listeners
    )


# --------------------------------------------------------------------------------
# Comparison-operator boundaries
#
# Every other cohort clears its threshold with margin, so before these tests a
# detector could flip `>` to `>=` (or `>=` to `>`) and the whole fixture would still
# pass. Listener counts jumped 12 -> 2 with nothing at 10; bucket uniques went
# 8 -> 6 -> 1 with nothing at 5; band shares went 1.0 -> 0.875 -> 0.0 with nothing at
# 0.60. These cases sit exactly on the line, so the operator itself is under test.
# --------------------------------------------------------------------------------
def test_fn01_sits_exactly_on_the_rule_a_threshold():
    """The fixture must actually contain the boundary, or the next test is vacuous."""
    boundary = EXPECTED["boundaries"]["topology_a_strict_greater_than"]
    assert boundary["listener_id"] == "FN01"
    assert boundary["plays_in_window"] == FIXTURE_THRESHOLDS.topology_a_plays_over
    assert rule_a_scores(FIXTURE_THRESHOLDS)["FN01"] == (
        FIXTURE_THRESHOLDS.topology_a_plays_over
    )


def test_rule_a_is_strictly_more_than_not_at_least():
    """CD-4 says MORE THAN, strictly. Flipping `>` to `>=` flags FN01 and fails here."""
    assert "FN01" not in rule_a_flagged(FIXTURE_THRESHOLDS)
    assert "FN01" in EXPECTED["expected_unflagged_listeners"]

    at_least = [
        listener
        for listener, score in rule_a_scores(FIXTURE_THRESHOLDS).items()
        if score >= FIXTURE_THRESHOLDS.topology_a_plays_over
    ]
    assert "FN01" in at_least, (
        "FN01 must be the case that distinguishes > from >=; if it is not caught by "
        ">=, this fixture no longer pins the operator"
    )


def test_boundary_b_bucket_sits_exactly_on_two_thresholds():
    boundary = EXPECTED["boundaries"]["topology_b_unique_listeners_and_band_share"]
    bucket = hour_buckets()[(boundary["track_id"], boundary["window_start"])]
    unique, ratio, share = bucket_measures(bucket)
    assert unique == FIXTURE_THRESHOLDS.topology_b_min_unique_listeners
    assert share == pytest.approx(FIXTURE_THRESHOLDS.topology_b_min_band_share)
    assert ratio == pytest.approx(1.0)


def test_rule_b_unique_listeners_and_band_share_are_at_least_not_more_than():
    """Both conditions use `>=`. Flipping either to `>` unflags this bucket."""
    boundary = EXPECTED["boundaries"]["topology_b_unique_listeners_and_band_share"]
    key = (boundary["track_id"], boundary["window_start"])
    bucket = hour_buckets()[key]

    assert all(rule_b_conditions(bucket, FIXTURE_THRESHOLDS).values())
    assert key in rule_b_flagged(FIXTURE_THRESHOLDS)

    unique, _, share = bucket_measures(bucket)
    assert not unique > FIXTURE_THRESHOLDS.topology_b_min_unique_listeners
    assert not share > FIXTURE_THRESHOLDS.topology_b_min_band_share


def test_the_unpinned_operator_is_recorded_rather_than_forgotten():
    """The ratio operator is deliberately not pinned. Keep that visible."""
    assert "topology_b_max_plays_per_listener" in EXPECTED["boundaries"]["not_pinned"]
    ratios = {
        bucket_measures(b)[1] for b in hour_buckets().values()
    }
    assert FIXTURE_THRESHOLDS.topology_b_max_plays_per_listener not in ratios, (
        "a bucket now sits exactly on the ratio threshold -- pin the operator with a "
        "real test and drop it from boundaries.not_pinned"
    )
