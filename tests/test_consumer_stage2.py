"""Consumer 2's detection layer, proven against the oracle rather than restated.

`tests/test_fixture_trips_rules.py` already proved the Rule B arithmetic at
fixture scale with its own local window helpers. This file proves that
`src/consumer_stage2.py` -- the shipped detector -- agrees with it, on the same
fixture, against the same `expected_flags.json`.

NO THRESHOLD, COUNT, TRACK ID, LISTENER ID OR TIMESTAMP LITERAL APPEARS IN THIS
FILE. Every expected value is read from the oracle or from a loaded `Thresholds`.
That is what makes this a proof of config injection rather than a restatement of
the same numbers in a second place.

Every processor here is built by encoding events back to their wire bytes and
driving `replay_records`, so the tests exercise the same decide -> tally -> count
ordering the poll loop uses rather than reaching past it into `count_event`.

IF AN ASSERTION HERE FAILS, the sanctioned fix is `src/consumer_stage2.py`.
Never weaken an assertion, never edit the oracle, and never touch
`src/windowing.py`, `contracts/`, `config/` or `tests/fixtures/`.
"""

from __future__ import annotations

import ast
import json
import math
import random
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.play_event_v1 import PlayEventV1  # noqa: E402
from src.config import Thresholds, load_thresholds  # noqa: E402
from src.consumer_stage2 import (  # noqa: E402
    CONDITION_MAX_PLAYS_PER_LISTENER,
    CONDITION_MIN_BAND_SHARE,
    CONDITION_MIN_UNIQUE_LISTENERS,
    DUPLICATE_EVENT_ID,
    Stage2Processor,
    replay_records,
)

FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "play_events_fixture.jsonl"
EXPECTED_FLAGS_PATH = REPO_ROOT / "tests" / "fixtures" / "expected_flags.json"
CONSUMER_SOURCE_PATH = REPO_ROOT / "src" / "consumer_stage2.py"

EXPECTED: Dict[str, Any] = json.loads(
    EXPECTED_FLAGS_PATH.read_text(encoding="utf-8")
)
EVENTS: List[PlayEventV1] = [
    PlayEventV1.model_validate_json(line)
    for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
    if line.strip()
]

FIXTURE_THRESHOLDS = load_thresholds(EXPECTED["thresholds_path"])
PRODUCTION_THRESHOLDS = load_thresholds(EXPECTED["production_thresholds_path"])

# Which oracle field records the number each condition measures, and which
# configured threshold it is compared against. Read rather than typed, so a
# renamed condition constant fails loudly instead of silently matching nothing.
ORACLE_MEASURE_FIELD = {
    CONDITION_MIN_UNIQUE_LISTENERS: "unique_listeners",
    CONDITION_MAX_PLAYS_PER_LISTENER: "plays_per_listener",
    CONDITION_MIN_BAND_SHARE: "band_share",
}
THRESHOLD_FIELD = {
    CONDITION_MIN_UNIQUE_LISTENERS: "topology_b_min_unique_listeners",
    CONDITION_MAX_PLAYS_PER_LISTENER: "topology_b_max_plays_per_listener",
    CONDITION_MIN_BAND_SHARE: "topology_b_min_band_share",
}
ALL_CONDITIONS = frozenset(ORACLE_MEASURE_FIELD)


# --------------------------------------------------------------------------------
# Helpers: drive the shipped module the way the poll loop does
# --------------------------------------------------------------------------------
def wire_records(
    events: Sequence[PlayEventV1],
) -> List[Tuple[bytes, bytes]]:
    """`(key, value)` byte pairs as they sit on `track-activity`: keyed by track."""
    return [
        (event.track_id.encode("utf-8"), event.model_dump_json().encode("utf-8"))
        for event in events
    ]


def processor_over(
    events: Sequence[PlayEventV1], thresholds: Thresholds = FIXTURE_THRESHOLDS
) -> Stage2Processor:
    """A `Stage2Processor` fed `events` through the real decide -> tally -> count path."""
    processor = Stage2Processor(thresholds)
    replay_records(processor, wire_records(events))
    return processor


def entry_for(
    processor: Stage2Processor, track_id: str, window_start: str
) -> Dict[str, Any]:
    """The single `evaluate_all()` entry for one bucket, or a clear failure."""
    matches = [
        entry
        for entry in processor.evaluate_all()
        if entry["track_id"] == track_id and entry["window_start"] == window_start
    ]
    assert len(matches) == 1, (
        f"expected exactly one bucket for track {track_id!r} at {window_start!r}; "
        f"found {len(matches)}. Buckets present for that track: "
        f"{[e['window_start'] for e in processor.evaluate_all() if e['track_id'] == track_id]}"
    )
    return matches[0]


def flagged_track_ids(processor: Stage2Processor) -> List[str]:
    return sorted({entry["track_id"] for entry in processor.flagged_buckets()})


def burst_events() -> List[PlayEventV1]:
    """The oracle's Topology B burst: its track, inside its recorded hour.

    Selected by a string prefix on the contract's own wire format rather than by
    re-deriving a bucket, so the selection cannot silently agree with a bucketing
    bug in the code under test.
    """
    recorded = EXPECTED["topology_b"]
    hour_prefix = recorded["window_start"][: len("YYYY-MM-DDTHH")]
    return [
        event
        for event in EVENTS
        if event.track_id == recorded["track_id"]
        and event.event_time.startswith(hour_prefix)
    ]


# --------------------------------------------------------------------------------
# The whole fixture, and the flagged burst
# --------------------------------------------------------------------------------
def test_the_flagged_track_set_is_exactly_the_oracles():
    processor = processor_over(EVENTS)
    flagged = flagged_track_ids(processor)
    assert flagged == sorted(EXPECTED["expected_flagged_tracks"])
    assert set(flagged).isdisjoint(set(EXPECTED["expected_unflagged_tracks"]))


def test_the_flagged_burst_carries_the_oracles_own_four_numbers():
    recorded = EXPECTED["topology_b"]
    processor = processor_over(EVENTS)
    entry = entry_for(processor, recorded["track_id"], recorded["window_start"])

    assert entry["unique_listeners"] == recorded["unique_listeners"]
    assert entry["total_plays"] == recorded["total_plays"]
    assert entry["plays_per_listener"] == pytest.approx(recorded["plays_per_listener"])
    assert entry["band_share"] == pytest.approx(recorded["band_share"])
    assert entry["flagged"] is True
    assert entry["window_hours"] == FIXTURE_THRESHOLDS.topology_b_window_hours


# --------------------------------------------------------------------------------
# The boundary bucket: both comparisons are `at least`
# --------------------------------------------------------------------------------
def test_the_boundary_bucket_sitting_on_two_thresholds_is_flagged():
    """Flip either comparison to strictly-greater and this bucket unflags.

    Every other cohort clears its threshold with margin, so without this case a
    detector could use `>` for both and the whole fixture would still pass.
    """
    boundary = EXPECTED["boundaries"]["topology_b_unique_listeners_and_band_share"]
    processor = processor_over(EVENTS)
    entry = entry_for(processor, boundary["track_id"], boundary["window_start"])

    assert entry["flagged"] is boundary["expected_flagged"]

    unique = entry["conditions"][CONDITION_MIN_UNIQUE_LISTENERS]
    share = entry["conditions"][CONDITION_MIN_BAND_SHARE]
    assert unique["measured"] == FIXTURE_THRESHOLDS.topology_b_min_unique_listeners
    assert share["measured"] == pytest.approx(
        FIXTURE_THRESHOLDS.topology_b_min_band_share
    )
    # Sitting exactly ON, not above: `>` would unflag it.
    assert not unique["measured"] > unique["threshold"]
    assert not share["measured"] > share["threshold"]
    assert unique["satisfied"] is True
    assert share["satisfied"] is True


# --------------------------------------------------------------------------------
# The decoys: all three conditions readable off a bucket that was NOT flagged
# --------------------------------------------------------------------------------
@pytest.mark.parametrize("decoy_name", ["popular_track", "repeat_listener"])
def test_each_unflagged_decoy_reports_all_three_conditions(decoy_name: str):
    """The enforcement of "no early exit", not a comment about it.

    An implementation that stopped evaluating at the first failed condition could
    not produce these three entries on a bucket it did not flag, so this test is
    unsatisfiable under an early exit rather than merely disapproving of one.
    """
    recorded = EXPECTED["decoys"][decoy_name]
    processor = processor_over(EVENTS)
    entry = entry_for(processor, recorded["track_id"], recorded["window_start"])

    assert entry["flagged"] is recorded["expected_flagged"]
    assert entry["flagged"] is False
    assert set(entry["conditions"]) == ALL_CONDITIONS

    for name, condition in entry["conditions"].items():
        assert condition["measured"] == pytest.approx(
            recorded[ORACLE_MEASURE_FIELD[name]]
        ), f"{decoy_name}: {name} measured the wrong number"
        assert condition["threshold"] == getattr(
            FIXTURE_THRESHOLDS, THRESHOLD_FIELD[name]
        ), f"{decoy_name}: {name} compared against a threshold not in the config"
        assert isinstance(condition["comparison"], str) and condition["comparison"]


@pytest.mark.parametrize("decoy_name", ["popular_track", "repeat_listener"])
def test_each_decoy_satisfies_and_fails_exactly_the_recorded_conditions(
    decoy_name: str,
):
    recorded = EXPECTED["decoys"][decoy_name]
    processor = processor_over(EVENTS)
    entry = entry_for(processor, recorded["track_id"], recorded["window_start"])
    conditions = entry["conditions"]

    assert sorted(
        name for name, c in conditions.items() if c["satisfied"]
    ) == sorted(recorded["satisfies_conditions"])
    assert sorted(
        name for name, c in conditions.items() if not c["satisfied"]
    ) == sorted(recorded["fails_conditions"])
    assert recorded["track_id"] not in flagged_track_ids(processor)


def test_every_condition_is_satisfiable_by_a_bucket_correctly_not_flagged():
    """"All three must hold together" -- demonstrated, not asserted.

    Between the two decoys every one of the three conditions is shown satisfiable
    by a bucket that is correctly NOT flagged. That is the difference between the
    rule being a conjunction and the rule merely being described as one.
    """
    processor = processor_over(EVENTS)
    satisfied_by_an_unflagged_bucket: Set[str] = set()
    for recorded in EXPECTED["decoys"].values():
        entry = entry_for(processor, recorded["track_id"], recorded["window_start"])
        assert entry["flagged"] is False, "a decoy must never be flagged"
        satisfied_by_an_unflagged_bucket |= {
            name for name, c in entry["conditions"].items() if c["satisfied"]
        }
    assert satisfied_by_an_unflagged_bucket == set(ALL_CONDITIONS)


def test_every_bucket_reports_all_three_conditions_flagged_or_not():
    """Not only the decoys: no bucket anywhere is missing a measured number."""
    processor = processor_over(EVENTS)
    entries = processor.evaluate_all()
    assert entries, "the fixture must produce at least one bucket"
    for entry in entries:
        assert set(entry["conditions"]) == ALL_CONDITIONS, (
            f"{entry['track_id']} @ {entry['window_start']} is missing a condition"
        )
        assert entry["flagged"] == all(
            c["satisfied"] for c in entry["conditions"].values()
        )


# --------------------------------------------------------------------------------
# Dedup: the FALSE-NEGATIVE regression, scaled as a fraction of the burst
#
# WHY A FRACTION AND NOT A SINGLE DUPLICATE. At fixture scale one duplicate is
# already 12.5% of an 8-event burst and breaks the ratio ceiling on its own, so a
# single-duplicate test looks decisive here and is INERT exactly where the risk is
# largest. At full scale the burst is 901 plays across 901 listeners at a ratio of
# 1.00, so crossing 1.1 takes about 91 duplicates. The documented full-scale run
# uses `--commit-every 200`, and Phase 3's finding F3 shows a crash can leave that
# many duplicates behind: 1101 over 901 is 1.22, which silently UNFLAGS the real
# fraud. Scaling the injected volume as a fraction of the burst keeps this test's
# teeth at both scales.
#
# The fractions are derived from configuration, never typed: the headroom between
# a real burst's ratio of 1.0 and the configured ceiling is
# `topology_b_max_plays_per_listener - 1.0`, and each case injects a multiple of it.
# --------------------------------------------------------------------------------
RATIO_HEADROOM = FIXTURE_THRESHOLDS.topology_b_max_plays_per_listener - 1.0
DUPLICATE_FRACTIONS = (
    RATIO_HEADROOM * 1.5,
    RATIO_HEADROOM * 3,
    RATIO_HEADROOM * 6,
    1.0,  # a full second copy of the burst
)


def duplicate_count(fraction: float) -> int:
    return max(1, math.ceil(fraction * len(burst_events())))


@pytest.mark.parametrize("fraction", DUPLICATE_FRACTIONS)
def test_duplicates_do_not_inflate_the_ratio_and_unflag_real_fraud(fraction: float):
    recorded = EXPECTED["topology_b"]
    burst = burst_events()
    n_duplicates = duplicate_count(fraction)

    # THE POSITIVE CONTROL, ASSERTED FIRST. Without dedup the play count would be
    # `len(burst) + n_duplicates` over the same unique listeners, and that ratio
    # must break the ceiling -- otherwise this case would pass whether or not the
    # module deduplicates anything.
    undeduped_ratio = (len(burst) + n_duplicates) / recorded["unique_listeners"]
    assert undeduped_ratio > FIXTURE_THRESHOLDS.topology_b_max_plays_per_listener, (
        f"{n_duplicates} duplicate(s) would not break the ratio ceiling, so this "
        "case cannot catch a missing dedup"
    )

    processor = processor_over(burst + burst[:n_duplicates])
    entry = entry_for(processor, recorded["track_id"], recorded["window_start"])

    assert entry["total_plays"] == recorded["total_plays"]
    assert entry["plays_per_listener"] == pytest.approx(
        recorded["plays_per_listener"]
    )
    assert entry["band_share"] == pytest.approx(recorded["band_share"])
    assert entry["flagged"] is True, (
        "duplicates inflated the ratio past the ceiling and the real fraud stopped "
        "being detected -- the false-negative direction"
    )


@pytest.mark.parametrize("fraction", DUPLICATE_FRACTIONS)
def test_the_dropped_duplicates_are_reported_not_silently_absorbed(fraction: float):
    burst = burst_events()
    n_duplicates = duplicate_count(fraction)
    processor = processor_over(burst + burst[:n_duplicates])
    counts = processor.counts()

    assert counts["duplicate_event_id"] == n_duplicates
    assert counts["records_seen"] == len(burst) + n_duplicates
    assert counts["counted"] == counts["records_seen"] - n_duplicates


def test_a_duplicate_is_refused_before_any_bucket_is_touched():
    """Dedup lives in `decide`, not after counting.

    `decide` is pure, so calling it twice on the same bytes must return a
    duplicate decision the second time only if the FIRST copy was already
    counted -- which is what "before any bucket is touched" means operationally.
    """
    burst = burst_events()
    processor = Stage2Processor(FIXTURE_THRESHOLDS)
    key, value = wire_records(burst)[0]

    first = processor.decide(key, value)
    assert first.count is True
    # `decide` alone must not have counted anything.
    assert processor.decide(key, value).count is True
    processor.count_event(first.event)
    second = processor.decide(key, value)
    assert second.count is False
    assert second.drop_reason == DUPLICATE_EVENT_ID


# --------------------------------------------------------------------------------
# Order-independence
# --------------------------------------------------------------------------------
SHUFFLE_SEEDS = (1, 2, 3, 5, 8)
MAX_RESEEDS = 50


def baseline_events() -> List[PlayEventV1]:
    """The fixture in a canonical, deterministic order."""
    return sorted(EVENTS, key=lambda e: (e.event_time, e.event_id))


def burst_subsequence(events: Sequence[PlayEventV1]) -> List[str]:
    """The `event_id`s of the oracle's Topology B bucket, in arrival order."""
    recorded = EXPECTED["topology_b"]
    hour_prefix = recorded["window_start"][: len("YYYY-MM-DDTHH")]
    return [
        event.event_id
        for event in events
        if event.track_id == recorded["track_id"]
        and event.event_time.startswith(hour_prefix)
    ]


def shuffle_disturbing_the_burst(
    seed: int, base: Sequence[PlayEventV1]
) -> Tuple[List[PlayEventV1], int]:
    """A shuffle that really reorders the Topology B bucket's own events.

    A seed that reorders the stream while leaving the burst's own events in
    sequence tests nothing, because the property under test is per bucket. Such a
    seed is re-rolled rather than silently accepted.
    """
    reference = burst_subsequence(base)
    for offset in range(MAX_RESEEDS):
        shuffled = list(base)
        random.Random(seed + offset * 1000).shuffle(shuffled)
        if burst_subsequence(shuffled) != reference:
            return shuffled, seed + offset * 1000
    raise AssertionError(
        f"no seed derived from {seed} disturbed the Topology B bucket's own order "
        f"within {MAX_RESEEDS} attempts"
    )


def test_the_review_document_does_not_depend_on_arrival_order():
    """WHY THIS MATTERS: Phase 3 measured a watermarked implementation's answer
    moving between 493 and 900 unique listeners across interleaves, because its
    lateness rejection dropped a different set of events each time. This test is
    what keeps that from becoming true again.

    WHAT IT DOES NOT PROVE: this is order-independence of the AGGREGATION over a
    fixed input, not of the pipeline end to end. PROF-02 -- two real replays
    through a real broker, compared -- is Phase 5's to run, and this test must not
    be cited as having discharged it.

    WHY IT LOOKS TAUTOLOGICAL: given a listener set and two integer counters the
    property nearly falls out of the data structures, which is exactly the point.
    Its value is as a TRIPWIRE that goes red the moment someone reintroduces a
    watermarked accumulator, not as a hard-won proof. Saying so keeps the test
    honest about its own strength.

    The whole serialized document is compared rather than just the flagged set,
    because that also pins the deterministic entry ordering and the
    earliest/latest timestamps Phase 5's PROF-02 will compare.
    """
    base = baseline_events()
    baseline_doc = json.dumps(
        processor_over(base).review_document(), sort_keys=True
    )

    # CONTROL 1: the baseline must actually flag something, or every comparison
    # below is between two empty documents.
    assert processor_over(base).flagged_buckets(), (
        "the baseline flags nothing; this test would compare two empty answers"
    )

    for seed in SHUFFLE_SEEDS:
        shuffled, used_seed = shuffle_disturbing_the_burst(seed, base)

        # CONTROL 2: a shuffle, not a filter.
        assert sorted(e.event_id for e in shuffled) == sorted(
            e.event_id for e in base
        ), f"seed {used_seed} changed the multiset of events, not just their order"

        # CONTROL 3: THE ORDER CHANGED INSIDE THE TOPOLOGY B BUCKET, not merely
        # somewhere across the stream. A global-order check would accept a shuffle
        # that left the burst's own events in sequence and test nothing.
        assert sorted(burst_subsequence(shuffled)) == sorted(
            burst_subsequence(base)
        )
        assert burst_subsequence(shuffled) != burst_subsequence(base), (
            f"seed {used_seed} did not disturb the Topology B bucket's own order"
        )

        assert (
            json.dumps(processor_over(shuffled).review_document(), sort_keys=True)
            == baseline_doc
        ), f"arrival order changed the review document (seed {used_seed})"


# --------------------------------------------------------------------------------
# The window-size guard
# --------------------------------------------------------------------------------
def test_a_window_other_than_one_hour_raises_rather_than_being_ignored():
    """Silently ignoring the field would produce a confidently wrong number."""
    wider = FIXTURE_THRESHOLDS.model_copy(
        update={
            "topology_b_window_hours": (
                FIXTURE_THRESHOLDS.topology_b_window_hours + 1
            )
        }
    )
    with pytest.raises(ValueError) as excinfo:
        Stage2Processor(wider)
    assert "topology_b_window_hours" in str(excinfo.value)
    assert str(wider.topology_b_window_hours) in str(excinfo.value)


def test_the_configured_one_hour_window_constructs_cleanly():
    assert FIXTURE_THRESHOLDS.topology_b_window_hours == (
        PRODUCTION_THRESHOLDS.topology_b_window_hours
    )
    assert Stage2Processor(FIXTURE_THRESHOLDS) is not None
    assert Stage2Processor(PRODUCTION_THRESHOLDS) is not None


# --------------------------------------------------------------------------------
# The rolling accumulator is not used -- an AST scan, not a text grep
#
# The scan PARSES rather than greps precisely so that `src/consumer_stage2.py`'s
# own docstring can name the class and explain at length why it is the wrong tool
# there without invalidating the check. A text grep would flag the very
# documentation the design depends on.
# --------------------------------------------------------------------------------
FORBIDDEN_WINDOWING_NAMES = frozenset({"RollingHourlyWindows", "topology_b_windows"})
REQUIRED_WINDOWING_NAME = "hour_bucket"


def referenced_identifiers(source: str) -> Set[str]:
    """Every identifier `source` imports, binds by import, names or reaches for.

    BOTH `alias.name` AND `alias.asname` are collected. They are different
    identifiers -- the imported name and the bound name -- and an aliased import
    hides the forbidden one behind the other, which is exactly the spelling a
    hurried edit would actually produce.
    """
    found: Set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                found.add(alias.name)
                found.update(alias.name.split("."))
                if alias.asname:
                    found.add(alias.asname)
        elif isinstance(node, ast.Name):
            found.add(node.id)
        elif isinstance(node, ast.Attribute):
            found.add(node.attr)
    return found


def test_consumer_stage2_never_reaches_for_the_rolling_accumulator():
    """CD-5 / F1: the accumulator's lateness rejection would drop 57% of the input."""
    identifiers = referenced_identifiers(
        CONSUMER_SOURCE_PATH.read_text(encoding="utf-8")
    )
    assert FORBIDDEN_WINDOWING_NAMES.isdisjoint(identifiers), (
        "src/consumer_stage2.py reaches for the rolling accumulator: "
        f"{sorted(FORBIDDEN_WINDOWING_NAMES & identifiers)}"
    )
    assert REQUIRED_WINDOWING_NAME in identifiers, (
        "src/consumer_stage2.py no longer imports the pure bucketing function"
    )


def _forbidden_snippets() -> List[Tuple[str, str]]:
    """One snippet per branch the scan relies on, for each forbidden name."""
    cases: List[Tuple[str, str]] = []
    for name in sorted(FORBIDDEN_WINDOWING_NAMES):
        cases.extend(
            [
                (f"plain-import[{name}]", f"from src.windowing import {name}\n"),
                (f"bare-call[{name}]", f"x = {name}(t)\n"),
                (
                    f"attribute-reach[{name}]",
                    f"import src.windowing as w\nx = w.{name}(t)\n",
                ),
                (
                    f"aliased-import[{name}]",
                    f"from src.windowing import {name} as _w\nx = _w(t)\n",
                ),
            ]
        )
    return cases


@pytest.mark.parametrize(
    "snippet", [case[1] for case in _forbidden_snippets()],
    ids=[case[0] for case in _forbidden_snippets()],
)
def test_the_scanner_catches_every_spelling_it_claims_to(snippet: str):
    """The positive control, four ways per forbidden name.

    Without the ALIASED case the branch deciding `alias.name` versus
    `alias.asname` is itself unguarded -- which is how a scan passes while being
    blind to the one spelling a hurried edit would produce.
    """
    assert not FORBIDDEN_WINDOWING_NAMES.isdisjoint(
        referenced_identifiers(snippet)
    ), f"the scanner missed a forbidden reference in:\n{snippet}"


def test_the_scanner_does_not_fire_on_the_sanctioned_import():
    """It discriminates: the pure function is not caught by the same net."""
    identifiers = referenced_identifiers(
        f"from src.windowing import {REQUIRED_WINDOWING_NAME}\n"
        f"x = {REQUIRED_WINDOWING_NAME}(t)\n"
    )
    assert FORBIDDEN_WINDOWING_NAMES.isdisjoint(identifiers)
    assert REQUIRED_WINDOWING_NAME in identifiers


# --------------------------------------------------------------------------------
# CTRT-04: identical code, identical data, different config file, different answer
# --------------------------------------------------------------------------------
def test_the_same_fixture_under_production_thresholds_flags_nothing():
    processor = processor_over(EVENTS, PRODUCTION_THRESHOLDS)
    assert processor.flagged_buckets() == []
    assert processor.evaluate_all(), "the fixture must still produce buckets"
    assert processor.review_document()["thresholds_source_path"] == (
        PRODUCTION_THRESHOLDS.source_path
    )


def test_the_two_configs_really_are_different_files_with_different_numbers():
    """Guards the test above from passing vacuously on two identical configs."""
    assert FIXTURE_THRESHOLDS.source_path != PRODUCTION_THRESHOLDS.source_path
    assert (
        FIXTURE_THRESHOLDS.topology_b_min_unique_listeners
        < PRODUCTION_THRESHOLDS.topology_b_min_unique_listeners
    )
