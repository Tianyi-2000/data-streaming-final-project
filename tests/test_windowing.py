"""The event-time windowing module does the arithmetic Phases 3 and 4 depend on.

This file is the evidence behind `src/windowing.py`'s claims. Three kinds of test
live here and they prove different things, so the distinction is worth keeping
straight when reading:

1. **Oracle cross-checks against the checked-in fixture.** The module reproduces
   the hand-checked numbers in `tests/fixtures/expected_flags.json`. Every
   expected value is READ from that file or from a loaded `Thresholds` -- no
   threshold literal and no oracle literal is typed into an assertion here, for
   the same reason `tests/test_fixture_trips_rules.py` refuses to.

2. **Synthetic boundary tests.** The fixture pins no window boundary: FA01's 12
   plays sit ~12 hours inside a 24-hour window, so half-open and closed
   conventions both return 12 there. The conventions are therefore pinned with
   in-test synthetic events, and every such test's docstring names the value the
   OTHER convention would produce -- so a reader can see the assertion
   discriminates between conventions rather than merely being true.

3. **A source scan.** Nothing in the module may read the system clock.

NOTHING HERE REGENERATES THE FIXTURE. `expected_flags.json` is the baseline four
later phases are graded against; a boundary the fixture cannot express is proven
with synthetic events instead.
"""

from __future__ import annotations

import ast
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.play_event_v1 import PlayEventV1, parse_event_time  # noqa: E402
from src.config import load_thresholds  # noqa: E402
from src.windowing import (  # noqa: E402
    HOUR,
    RollingHourlyWindows,
    hour_bucket,
    topology_a_windows,
    topology_b_windows,
    window_bucket_starts,
)

FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "play_events_fixture.jsonl"
EXPECTED_FLAGS_PATH = REPO_ROOT / "tests" / "fixtures" / "expected_flags.json"

EXPECTED: Dict[str, Any] = json.loads(EXPECTED_FLAGS_PATH.read_text(encoding="utf-8"))
EVENTS: List[PlayEventV1] = [
    PlayEventV1.model_validate_json(line)
    for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
    if line.strip()
]

FIXTURE_THRESHOLDS = load_thresholds(EXPECTED["thresholds_path"])


# --------------------------------------------------------------------------------
# Streaming helpers -- the fixture goes through the module the way a consumer would
# --------------------------------------------------------------------------------
def stream_by_listener() -> Tuple[RollingHourlyWindows, Dict[str, int]]:
    """Replay the fixture keyed by listener, keeping each listener's best `add`.

    `add` returns the key's rolling count as of the watermark hour, so the
    maximum value it ever returns for a listener IS that listener's peak
    bucket-aligned rolling count: the count can only fall as the window slides
    past an empty hour, so its maximum is always attained at an hour the
    listener actually played in -- which is exactly an hour `add` was called on.
    """
    accumulator = topology_a_windows(FIXTURE_THRESHOLDS)
    best: Dict[str, int] = {}
    for event in EVENTS:
        score = accumulator.add(event.listener_id, event.event_time, event)
        best[event.listener_id] = max(best.get(event.listener_id, 0), score)
    return accumulator, best


def last_event_time(listener_id: str) -> datetime:
    return max(
        parse_event_time(e.event_time) for e in EVENTS if e.listener_id == listener_id
    )


# --------------------------------------------------------------------------------
# hour_bucket -- the truncation every other function is built on
# --------------------------------------------------------------------------------
def test_hour_bucket_truncates_a_contract_timestamp_to_its_hour():
    bucket = hour_bucket(EXPECTED["topology_a"]["window_start"])
    assert bucket == datetime(2026, 8, 8, 0, 0, tzinfo=timezone.utc)
    assert bucket.tzinfo == timezone.utc
    assert (bucket.minute, bucket.second, bucket.microsecond) == (0, 0, 0)


def test_hour_bucket_converts_a_non_utc_datetime_before_truncating():
    """The bucket must follow the instant, not the wall reading.

    23:30 at +05:00 is 18:30 UTC, so the bucket is hour 18 of the same UTC day.
    Truncating first and converting afterwards would give hour 23 -- a different
    hour, on a different day at some offsets. The order of operations is the
    whole test.
    """
    aware = datetime(2026, 8, 8, 23, 30, tzinfo=timezone(timedelta(hours=5)))
    assert hour_bucket(aware) == datetime(2026, 8, 8, 18, 0, tzinfo=timezone.utc)


def test_hour_bucket_refuses_a_naive_datetime_rather_than_localising_it():
    """A naive datetime is the silent-localisation hazard; it must raise."""
    with pytest.raises(ValueError) as exc:
        hour_bucket(datetime(2026, 8, 8, 0, 3))
    message = str(exc.value)
    assert "timezone" in message.lower()
    assert "2026-08-08 00:03:00" in message


# --------------------------------------------------------------------------------
# window_bucket_starts -- the N hour-starts a rolling count sums over
# --------------------------------------------------------------------------------
def test_window_bucket_starts_returns_exactly_window_hours_starts_oldest_first():
    window_hours = FIXTURE_THRESHOLDS.topology_a_window_hours
    starts = window_bucket_starts("2026-08-08T11:51:00Z", window_hours)

    assert len(starts) == window_hours
    assert starts == sorted(starts)
    assert all(later - earlier == HOUR for earlier, later in zip(starts, starts[1:]))
    assert starts[-1] == hour_bucket("2026-08-08T11:51:00Z")
    # 24 buckets ending at hour 11 begin at hour 12 of the PREVIOUS day.
    assert starts[0] == datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)


def test_window_bucket_starts_rejects_a_window_shorter_than_one_hour():
    with pytest.raises(ValueError):
        window_bucket_starts("2026-08-08T11:51:00Z", 0)


# --------------------------------------------------------------------------------
# The Phase 1 oracle, reproduced through the module
# --------------------------------------------------------------------------------
def test_streaming_the_fixture_reproduces_the_oracles_topology_a_count():
    """CONTEXT success criterion 1, against the hand-checked number.

    The 12 is read from `expected_flags.json`, never typed here: if the oracle
    is ever re-baselined this test follows it instead of silently disagreeing.
    """
    recorded = EXPECTED["topology_a"]
    _, best = stream_by_listener()
    assert best[recorded["listener_id"]] == recorded["plays_in_window"]


def test_the_rolling_count_is_visibly_the_sum_of_its_hourly_buckets():
    """CONTEXT success criterion 2: the caller can obtain the 24 numbers.

    WNDW-02 makes the rolling figure inspectable rather than a black box, so
    this asserts both halves: exactly `window_hours` (hour_start, count) pairs
    come back, and their sum IS the rolling count.
    """
    recorded = EXPECTED["topology_a"]
    listener = recorded["listener_id"]
    accumulator, _ = stream_by_listener()
    as_of = last_event_time(listener)

    counts = accumulator.bucket_counts(listener, as_of=as_of)
    starts = [start for start, _ in counts]

    assert len(counts) == FIXTURE_THRESHOLDS.topology_a_window_hours
    assert starts == sorted(starts)
    assert all(later - earlier == HOUR for earlier, later in zip(starts, starts[1:]))
    assert starts[-1] == hour_bucket(as_of)
    assert sum(count for _, count in counts) == recorded["plays_in_window"]
    assert accumulator.rolling_count(listener, as_of=as_of) == recorded["plays_in_window"]


# --------------------------------------------------------------------------------
# Both window sizes come from Thresholds, not from literals
# --------------------------------------------------------------------------------
def test_both_factories_take_their_window_size_from_the_loaded_thresholds():
    """CONTEXT success criterion 5, and the code path Phase 1's W4 found missing."""
    assert (
        topology_a_windows(FIXTURE_THRESHOLDS).window_hours
        == FIXTURE_THRESHOLDS.topology_a_window_hours
    )
    assert (
        topology_b_windows(FIXTURE_THRESHOLDS).window_hours
        == FIXTURE_THRESHOLDS.topology_b_window_hours
    )


# --------------------------------------------------------------------------------
# The boundary convention, pinned with synthetic events
#
# The fixture cannot pin this and MUST NOT be regenerated to try: FA01's 12 plays
# sit about 12 hours inside the 24-hour window, so the half-open and the closed
# convention both return 12 there. Everything below is built in-test instead, and
# every docstring names the number the OTHER convention would produce -- which is
# what makes the assertion discriminating rather than merely true.
# --------------------------------------------------------------------------------
def test_the_hour_edge_is_half_open():
    """A bucket is [hour, hour + 1h). 20:59:59 and 21:00:00 are different hours.

    A CLOSED bucket [hour, hour + 1h] would put 21:00:00 in hour 20 as well as
    hour 21, and this rolling count would be 2. It is 1.
    """
    accumulator = topology_b_windows(FIXTURE_THRESHOLDS)
    assert accumulator.window_hours == 1, "this test needs the 1-hour window"

    accumulator.add("T", parse_event_time("2026-03-01T20:59:59Z"))
    accumulator.add("T", parse_event_time("2026-03-01T21:00:00Z"))

    assert accumulator.rolling_count("T", as_of="2026-03-01T20:00:00Z") == 1
    assert accumulator.rolling_count("T", as_of="2026-03-01T21:00:00Z") == 1


def test_the_window_edge_is_half_open():
    """CONTEXT success criterion 3, the half the fixture cannot supply.

    Two events exactly 24 hours apart, read through a 24-hour window ending at
    the second one's hour. The 24 buckets run from day-1 hour 1 to day-2 hour 0,
    so the day-1 hour-0 event is outside and the count is 1.

    A CLOSED reading spans 25 buckets -- day-1 hour 0 through day-2 hour 0 --
    and would return 2. This assertion fails under that convention, which is the
    point of it.
    """
    accumulator = topology_a_windows(FIXTURE_THRESHOLDS)
    assert accumulator.window_hours == 24, "this test needs the 24-hour window"

    accumulator.add("L", parse_event_time("2026-01-01T00:00:00Z"))
    accumulator.add("L", parse_event_time("2026-01-02T00:00:00Z"))

    assert accumulator.rolling_count("L", as_of="2026-01-02T00:00:00Z") == 1
    # And the excluded event really is one bucket outside, not absent entirely:
    # ending one hour earlier brings it back.
    assert accumulator.rolling_count("L", as_of="2026-01-01T23:00:00Z") == 1
    assert accumulator.rolling_count("L", as_of="2026-01-01T00:00:00Z") == 1


# --------------------------------------------------------------------------------
# Both window sizes are honoured BEHAVIOURALLY, not merely read
# --------------------------------------------------------------------------------
THREE_CONSECUTIVE_HOURS = (
    "2026-03-01T10:15:00Z",
    "2026-03-01T11:15:00Z",
    "2026-03-01T12:15:00Z",
)


def test_changing_a_configured_window_size_changes_the_modules_answer():
    """Phase 1 warning W4, killed: `topology_b_window_hours` now has behaviour.

    One identical three-hour sequence, three accumulators. The 24-hour window
    sums all three hours; the 1-hour window sums only the last; and an in-memory
    Thresholds with `topology_b_window_hours` retuned to 3 makes the B answer
    move to match A's. Before this test the field could be mutated in the config
    and every assertion in the repo still passed.

    `model_copy` builds the retuned Thresholds in memory. Nothing is written, so
    `config/*.json` is untouched and the phase scope fence holds.
    """
    retuned = FIXTURE_THRESHOLDS.model_copy(update={"topology_b_window_hours": 3})

    def score(accumulator: RollingHourlyWindows) -> int:
        for event_time in THREE_CONSECUTIVE_HOURS:
            accumulator.add("K", parse_event_time(event_time))
        return accumulator.rolling_count("K")

    wide = score(topology_a_windows(FIXTURE_THRESHOLDS))
    narrow = score(topology_b_windows(FIXTURE_THRESHOLDS))
    widened = score(topology_b_windows(retuned))

    assert wide == len(THREE_CONSECUTIVE_HOURS)
    assert narrow == 1
    assert narrow != wide, "the two configured window sizes must differ in effect"
    assert widened == wide
    # The retune stayed in memory.
    assert FIXTURE_THRESHOLDS.topology_b_window_hours != retuned.topology_b_window_hours


# --------------------------------------------------------------------------------
# WNDW-03: one watermark, late arrivals dropped loudly, never buffered
# --------------------------------------------------------------------------------
def test_the_watermark_is_the_greatest_event_time_seen_and_starts_unset():
    accumulator = topology_a_windows(FIXTURE_THRESHOLDS)
    assert accumulator.watermark is None
    assert accumulator.late_dropped == 0

    accumulator.add("K", parse_event_time("2026-03-01T05:40:00Z"))
    assert accumulator.watermark == parse_event_time("2026-03-01T05:40:00Z")

    # An earlier event inside the SAME open hour must not walk it backwards.
    accumulator.add("K", parse_event_time("2026-03-01T05:10:00Z"))
    assert accumulator.watermark == parse_event_time("2026-03-01T05:40:00Z")


def test_out_of_order_inside_the_same_hour_is_not_late():
    """A bucket stays open until the watermark moves PAST it.

    Judging lateness at instant granularity would drop the 05:10 event here and
    under-count the hour. Judging it at bucket granularity -- which is what
    WNDW-03 means by a watermark over hourly buckets -- keeps both.
    """
    accumulator = topology_b_windows(FIXTURE_THRESHOLDS)
    accumulator.add("K", parse_event_time("2026-03-01T05:40:00Z"))
    accumulator.add("K", parse_event_time("2026-03-01T05:10:00Z"))

    assert accumulator.rolling_count("K", as_of="2026-03-01T05:00:00Z") == 2
    assert accumulator.late_dropped == 0


def test_an_event_from_a_closed_hour_is_dropped_counted_and_logged(caplog):
    """WNDW-03: no buffering. The drop is visible in a counter AND in the log.

    A silently discarded event yields a count nobody can reconstruct, which is
    the repudiation threat T-02-04. `late_dropped` non-zero on a real run means
    the producer's nondecreasing-`event_time` guarantee broke.
    """
    accumulator = topology_b_windows(FIXTURE_THRESHOLDS)
    accumulator.add("K", parse_event_time("2026-03-01T05:30:00Z"))
    before = accumulator.rolling_count("K")

    with caplog.at_level(logging.WARNING, logger="src.windowing"):
        returned = accumulator.add("K", parse_event_time("2026-03-01T04:30:00Z"))

    assert accumulator.late_dropped == 1
    assert returned == before, "add must still return the key's rolling count"
    assert accumulator.rolling_count("K") == before
    # The event was dropped, not buffered into its own hour for later.
    assert accumulator.rolling_count("K", as_of="2026-03-01T04:00:00Z") == 0
    # The watermark did not move backwards.
    assert accumulator.watermark == parse_event_time("2026-03-01T05:30:00Z")

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "K" in message
    assert parse_event_time("2026-03-01T04:30:00Z").isoformat() in message
    assert parse_event_time("2026-03-01T05:30:00Z").isoformat() in message


# --------------------------------------------------------------------------------
# Bucket membership: what Phase 4 actually consumes
# --------------------------------------------------------------------------------
def test_bucket_items_returns_the_bucket_containing_a_real_event_time():
    """`hour_start` is truncated, so a caller need not pre-truncate to find it."""
    accumulator = topology_b_windows(FIXTURE_THRESHOLDS)
    accumulator.add("K", parse_event_time("2026-03-01T05:40:00Z"), "first")
    accumulator.add("K", parse_event_time("2026-03-01T05:10:00Z"), "second")

    assert accumulator.bucket_items("K", "2026-03-01T05:00:00Z") == ("first", "second")
    assert accumulator.bucket_items("K", "2026-03-01T05:59:59Z") == ("first", "second")
    assert accumulator.bucket_items("K", "2026-03-01T06:00:00Z") == ()
    assert accumulator.bucket_items("nobody", "2026-03-01T05:00:00Z") == ()


def test_iter_buckets_is_sorted_by_key_then_hour_and_skips_nothing():
    accumulator = topology_b_windows(FIXTURE_THRESHOLDS)
    accumulator.add("B", parse_event_time("2026-03-01T05:00:00Z"), "b5")
    accumulator.add("A", parse_event_time("2026-03-01T06:00:00Z"), "a6")
    accumulator.add("A", parse_event_time("2026-03-01T07:00:00Z"), "a7")

    assert list(accumulator.iter_buckets()) == [
        ("A", hour_bucket("2026-03-01T06:00:00Z"), ("a6",)),
        ("A", hour_bucket("2026-03-01T07:00:00Z"), ("a7",)),
        ("B", hour_bucket("2026-03-01T05:00:00Z"), ("b5",)),
    ]


# --------------------------------------------------------------------------------
# The divergence from Phase 1's sliding maximum, pinned in both directions
#
# `sliding_max_count` below is a deliberately INDEPENDENT reimplementation of the
# shape of `tests/test_fixture_trips_rules.py::rule_a_score`. It is not imported
# from there and that file is not modified.
#
# Phase 1's verification suggested switching that file over to this module. That
# suggestion is visibly CONSIDERED AND DECLINED here, for two reasons. First, its
# docstring forbids extraction: it is a specification of the intended answers,
# and a specification that calls the implementation it is checking proves
# nothing. Second, and more concretely, the two are not the same function -- the
# test below exhibits an input where they differ -- so "switching it over" would
# not be a refactor, it would be a change of meaning.
# --------------------------------------------------------------------------------
def sliding_max_count(event_times: List[datetime], window_hours: int) -> int:
    """Max events in any window anchored at one of the events' own timestamps."""
    times = sorted(event_times)
    window = timedelta(hours=window_hours)
    return max(sum(1 for t in times if start <= t < start + window) for start in times)


def test_bucket_sum_and_sliding_max_are_not_the_same_function():
    """WNDW-02's bucket-aligned count vs the event-anchored sliding maximum.

    Two plays 23h45m apart, straddling a midnight bucket boundary. The sliding
    maximum anchors a 24-hour window at the first play and catches both, so it
    says 2. This module's 24 buckets end at the second play's hour and begin one
    hour after the first play's hour, so it says 1.

    Both are defensible; WNDW-02 is the requirement, so Phase 3 must use this
    module's answer and must not silently assume the other. The divergence is
    one-directional -- every 24-bucket span IS a 24-hour span and the sliding
    maximum maximises over ALL of them -- so this module's count is always <= the
    sliding maximum. It can withhold a flag at the margin; it can never
    manufacture one.
    """
    window_hours = FIXTURE_THRESHOLDS.topology_a_window_hours
    moments = [
        parse_event_time("2026-01-01T00:30:00Z"),
        parse_event_time("2026-01-02T00:15:00Z"),
    ]

    accumulator = topology_a_windows(FIXTURE_THRESHOLDS)
    for moment in moments:
        accumulator.add("L", moment)

    assert accumulator.rolling_count("L", as_of=moments[-1]) == 1
    assert sliding_max_count(moments, window_hours) == 2


def test_bucket_sum_and_sliding_max_agree_on_every_fixture_listener():
    """02-CONTEXT's claim, turned into a regression guard.

    The two semantics agree on this fixture -- which is exactly why the test
    above had to use synthetic events. If a future fixture edit makes them
    diverge, that surfaces here rather than as a changed detection number four
    phases downstream.
    """
    window_hours = FIXTURE_THRESHOLDS.topology_a_window_hours
    _, best = stream_by_listener()

    by_listener: Dict[str, List[datetime]] = {}
    for event in EVENTS:
        by_listener.setdefault(event.listener_id, []).append(
            parse_event_time(event.event_time)
        )

    assert set(best) == set(by_listener)
    disagreements = {
        listener: (best[listener], sliding_max_count(moments, window_hours))
        for listener, moments in by_listener.items()
        if best[listener] != sliding_max_count(moments, window_hours)
    }
    assert not disagreements, f"bucket-sum vs sliding-max diverged: {disagreements}"


# --------------------------------------------------------------------------------
# The oracle, cross-checked through the module for both topologies
# --------------------------------------------------------------------------------
def test_the_rule_a_operator_boundary_survives_this_module():
    """FN01 must still sit EXACTLY on the threshold when scored by this module.

    Phase 3 applies a strict `>` to the number this module produces. If FN01
    scored 9 or 11 here, that phase would have nothing sitting on the line and a
    `>` to `>=` slip would pass its whole suite.
    """
    boundary = EXPECTED["boundaries"]["topology_a_strict_greater_than"]
    _, best = stream_by_listener()

    assert best[boundary["listener_id"]] == boundary["plays_in_window"]
    assert best[boundary["listener_id"]] == FIXTURE_THRESHOLDS.topology_a_plays_over
    assert not best[boundary["listener_id"]] > FIXTURE_THRESHOLDS.topology_a_plays_over


def stream_by_track() -> RollingHourlyWindows:
    accumulator = topology_b_windows(FIXTURE_THRESHOLDS)
    for event in EVENTS:
        accumulator.add(event.track_id, event.event_time, event)
    return accumulator


@pytest.mark.parametrize(
    "recorded_path",
    [
        ("topology_b",),
        ("boundaries", "topology_b_unique_listeners_and_band_share"),
    ],
    ids=["topology_b_burst", "boundary_b_bucket"],
)
def test_the_topology_b_buckets_reproduce_the_oracles_numbers(recorded_path):
    """An ORACLE-AGREEMENT cross-check. Be clear about what it does not prove.

    Both of these tracks have ALL of their fixture events inside the asserted
    hour, so this equality would also hold for an accumulator that simply
    returned everything it ever saw for a key. It cannot, on its own, tell a
    1-hour window from a 24-hour one. The evidence for the window WIDTH is
    `test_the_hour_edge_is_half_open`, where the two conventions give different
    answers.

    What this does prove is that the module reproduces the exact numbers Phase 4
    will consume. The empty-neighbouring-hour assertions close part of the gap
    cheaply.
    """
    recorded: Dict[str, Any] = EXPECTED
    for step in recorded_path:
        recorded = recorded[step]

    # The boundary record states plays-per-listener rather than a play total, so
    # the total is DERIVED from the oracle's own numbers. Still nothing typed.
    expected_plays = recorded.get(
        "total_plays", round(recorded["unique_listeners"] * recorded["plays_per_listener"])
    )

    accumulator = stream_by_track()
    track = recorded["track_id"]
    start = recorded["window_start"]

    items = accumulator.bucket_items(track, start)
    assert len(items) == expected_plays
    assert len({e.listener_id for e in items}) == recorded["unique_listeners"]
    assert accumulator.rolling_count(track, as_of=start) == expected_plays

    # The burst really is confined to that hour: its neighbours are empty.
    assert accumulator.bucket_items(track, hour_bucket(start) - HOUR) == ()
    assert accumulator.bucket_items(track, hour_bucket(start) + HOUR) == ()


def test_the_default_as_of_reads_zero_for_a_key_the_stream_moved_past():
    """Threat T-02-07, pinned so the class docstring's warning cannot rot.

    The watermark is accumulator-wide. After the whole fixture has streamed, it
    sits on 08-10 while the Topology B track's plays sit on 08-09, so the default
    `as_of` returns a plausible 0 rather than raising. With an explicit `as_of`
    the same accumulator returns the oracle's count.

    This is the trap Phase 4 walks into if it streams first and asks afterwards.
    Consumer 2 must pass an explicit `as_of` or iterate with `iter_buckets()`.
    """
    recorded = EXPECTED["topology_b"]
    accumulator = stream_by_track()
    track = recorded["track_id"]

    assert accumulator.watermark == max(
        parse_event_time(e.event_time) for e in EVENTS
    )
    assert accumulator.rolling_count(track) == 0
    assert accumulator.rolling_count(track, as_of=recorded["window_start"]) == (
        recorded["total_plays"]
    )
    # And the events were never lost -- they are still reachable by bucket.
    assert len(accumulator.bucket_items(track, recorded["window_start"])) == (
        recorded["total_plays"]
    )
    assert accumulator.late_dropped == 0, (
        "the fixture is in nondecreasing event_time order (GATE-ORDERING); a "
        "drop here means that guarantee broke"
    )


def test_two_replays_of_the_same_fixture_produce_identical_buckets():
    """Same input, same answer, no hidden state. PROF-02 depends on this."""
    first = [(k, h, tuple(items)) for k, h, items in stream_by_track().iter_buckets()]
    second = [(k, h, tuple(items)) for k, h, items in stream_by_track().iter_buckets()]
    assert first == second
    assert first, "the fixture must produce at least one bucket"


# --------------------------------------------------------------------------------
# CONTEXT success criterion 4: nothing in the module reads the system clock
#
# An AST scan rather than a text grep, and the reason is not fastidiousness: the
# module's own docstring has to DISCUSS the hazard in prose, so a text scan would
# flag the very documentation it depends on. Parsing means only real calls are
# examined -- prose and comments are safe by construction.
# --------------------------------------------------------------------------------
FORBIDDEN_CLOCK_NAMES = frozenset(
    {
        "now",
        "utcnow",
        "today",
        "time",
        "monotonic",
        "perf_counter",
        "time_ns",
        "fromtimestamp",
    }
)


# What a naive "scan the text for clock calls" would look like. Used only to show
# that it is the wrong tool for this file, never as the actual check.
CLOCK_CALL_GREP = re.compile(
    r"\b(" + "|".join(sorted(FORBIDDEN_CLOCK_NAMES)) + r")\s*\("
)


def clock_offenders(source: str) -> List[Tuple[int, str, str]]:
    """(line, kind, name) for every clock read in `source`. Empty means clean."""
    offenders: List[Tuple[int, str, str]] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr in FORBIDDEN_CLOCK_NAMES:
                offenders.append((node.lineno, "attribute-call", func.attr))
            elif isinstance(func, ast.Name) and func.id in FORBIDDEN_CLOCK_NAMES:
                offenders.append((node.lineno, "bare-call", func.id))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "time" or alias.name.startswith("time."):
                    offenders.append((node.lineno, "import", alias.name))
        elif isinstance(node, ast.ImportFrom):
            if node.module == "time":
                offenders.append((node.lineno, "import", "time"))
    return offenders


def test_the_windowing_module_never_reads_the_system_clock():
    """CONTEXT success criterion 4, enforced by a test rather than by inspection."""
    source = (REPO_ROOT / "src" / "windowing.py").read_text(encoding="utf-8")
    assert clock_offenders(source) == []


def test_a_text_grep_would_have_been_the_wrong_tool_here():
    """Why the scan above parses instead of grepping, demonstrated not asserted.

    The module documents the clock hazard by NAMING the spellings it refuses to
    use, so a reader searching for `datetime.now()` lands on the paragraph
    explaining its absence. That is good documentation and it is fatal to a text
    scan: a grep flags the prose it depends on. Parsing sees only real calls.

    If this test ever fails because the grep found nothing, the module stopped
    naming the hazard and the AST scan above became a claim about a file that no
    longer needs it.
    """
    source = (REPO_ROOT / "src" / "windowing.py").read_text(encoding="utf-8")
    grep_hits = CLOCK_CALL_GREP.findall(source)
    assert grep_hits, "the module no longer names the clock hazard in prose"
    assert clock_offenders(source) == []


@pytest.mark.parametrize(
    "snippet, expected_kind",
    [
        ("from datetime import datetime\nx = datetime.now()\n", "attribute-call"),
        ("from datetime import datetime\nx = datetime.utcnow()\n", "attribute-call"),
        ("import time\nx = time.time()\n", "attribute-call"),
        ("from time import time\nx = time()\n", "bare-call"),
    ],
    ids=["datetime.now", "datetime.utcnow", "time.time", "aliased-bare-time"],
)
def test_the_clock_scanner_catches_a_clock_read(snippet: str, expected_kind: str):
    """The positive control, four ways -- without it the scan proves nothing.

    One snippet would only exercise the `ast.Attribute` branch, so a scanner that
    never reached the `ast.Name` branch would still pass while being blind to
    `from time import time`. Asserting the KIND, not merely that something was
    found, is what makes that coverage real rather than inferred.
    """
    kinds = {kind for _, kind, _ in clock_offenders(snippet)}
    assert kinds, "a clock read went unnoticed by the scanner"
    assert expected_kind in kinds
