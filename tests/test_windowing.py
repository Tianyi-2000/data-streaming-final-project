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

import json
import logging
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
