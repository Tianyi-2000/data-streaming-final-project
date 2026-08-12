"""Event-time windowing: hourly buckets and rolling N-hour counts.

This module counts. It does not judge -- every threshold comparison belongs to
Phase 3 (Consumer 1) and Phase 4 (Consumer 2), both of which import from here so
the two sides cannot drift on what "in the window" means.

Four things a later reader needs, in the order they matter.

1. EVERY WINDOW IS MEASURED ON `event_time`, AND NOTHING HERE READS THE SYSTEM
   CLOCK (WNDW-01, CD-7). Replay compresses three simulated days into a few
   seconds, so a wall-clock read does not raise -- it returns a confidently
   wrong number. There is no call anywhere in this file to the current time in
   any of its spellings, and `tests/test_windowing.py` proves it by parsing this
   source and walking the tree, rather than by inspection. Timestamps arrive as
   contract strings or as aware datetimes and are parsed only by
   `contracts.play_event_v1.parse_event_time`.

2. BUCKETS ARE `event_time` TRUNCATED TO THE HOUR, AND A ROLLING N-HOUR COUNT IS
   THE SUM OF THE LAST N HOURLY BUCKETS ending at and including the as-of hour
   (WNDW-02). That is deliberately inspectable: `bucket_counts` hands back the N
   numbers and `rolling_count` is implemented as their sum, so the total can
   never disagree with the parts a caller was shown.

3. INTERVALS ARE HALF-OPEN. A bucket is `[hour, hour + 1h)` and an N-hour window
   is the N buckets spanning `[end - (N-1)h, end + 1h)`. An event landing exactly
   on a far edge belongs to the next bucket, never to both. The choice matters
   because Phase 3's comparison is a strict `>` sitting exactly where a
   one-bucket difference changes who gets accused.

4. THIS IS NOT THE SLIDING MAXIMUM `tests/test_fixture_trips_rules.py::rule_a_score`
   COMPUTES. That helper anchors a window at each event's own timestamp and takes
   the maximum; this module aligns windows to hour boundaries. The two agree on
   every listener in the checked-in fixture and do NOT agree in general. WNDW-02
   is the requirement -- the bucket sum is what makes the number inspectable --
   so Phase 3 must use THIS one and must not assume the other.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Union

# Same import bootstrap `src/config.py` uses, so this module resolves however it
# is invoked and never depends on a conftest.py being present.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.play_event_v1 import parse_event_time  # noqa: E402
from src.config import Thresholds  # noqa: E402

HOUR = timedelta(hours=1)

# What a caller may hand any function here: the contract's wire string, or an
# aware datetime. Never a naive datetime -- see `_as_utc_instant`.
EventTime = Union[str, datetime]


def _as_utc_instant(event_time: EventTime) -> datetime:
    """The aware UTC instant behind a contract string or an aware datetime.

    Raises:
        ValueError: if the value is naive, i.e. carries no timezone. Coercing a
            naive datetime to UTC would be a guess, and a wrong guess produces a
            plausible count rather than an error -- the exact failure mode this
            module exists to make loud.
    """
    moment = parse_event_time(event_time) if isinstance(event_time, str) else event_time
    if not isinstance(moment, datetime):
        raise TypeError(
            f"event_time must be an ISO 8601 string or a datetime; got {moment!r}"
        )
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError(
            f"event_time {moment} has no timezone; naive datetimes are refused "
            "rather than assumed to be UTC"
        )
    return moment.astimezone(timezone.utc)


def hour_bucket(event_time: EventTime) -> datetime:
    """The aware UTC hour a timestamp belongs to.

    Converts to UTC and only then truncates. The order is load-bearing:
    truncating a non-UTC reading first would bucket the wall-clock hour instead
    of the hour the instant actually fell in.
    """
    return _as_utc_instant(event_time).replace(minute=0, second=0, microsecond=0)


def window_bucket_starts(as_of: EventTime, window_hours: int) -> list[datetime]:
    """The `window_hours` hour-starts a rolling count sums over, oldest first.

    The last element is `hour_bucket(as_of)` and the first is `window_hours - 1`
    hours before it. `as_of` is truncated here, so the function is total and no
    caller has to pre-truncate.

    Half-open: for `window_hours=24` ending at hour H, the span covered is
    `[H - 23h, H + 1h)`. An event exactly 24 hours before H falls in bucket
    `H - 24h`, which is outside. A closed reading would return 25 starts and
    include it.
    """
    if window_hours < 1:
        raise ValueError(f"window_hours must be at least 1; got {window_hours}")
    end = hour_bucket(as_of)
    return [end - (window_hours - 1 - i) * HOUR for i in range(window_hours)]


class RollingHourlyWindows:
    """Hourly buckets per key, with a rolling N-bucket count over them.

    Construct one through `topology_a_windows` or `topology_b_windows` so the
    window size comes from configuration rather than from a literal.
    """

    def __init__(self, window_hours: int) -> None:
        if window_hours < 1:
            raise ValueError(f"window_hours must be at least 1; got {window_hours}")
        self._window_hours = window_hours
        # key -> {hour_start: [items]}. A plain dict rather than a defaultdict:
        # every read path below uses `.get`, so inspecting an unknown key can
        # never create an empty entry that later has to be filtered back out.
        self._buckets: dict[str, dict[datetime, list[Any]]] = {}
        self._watermark: Optional[datetime] = None

    @property
    def window_hours(self) -> int:
        """How many hourly buckets a rolling count sums over."""
        return self._window_hours

    def add(self, key: str, event_time: EventTime, item: Any = None) -> int:
        """Record one event under `key` and return that key's rolling count.

        INVARIANT: `add` returns the key's rolling count as of the watermark
        hour, after the add. Always an int, never None.

        `item` is how Phase 4 keeps the events themselves: Consumer 2 recomputes
        unique listeners, plays per listener and band share from a bucket's
        members, so it passes the event. Consumer 1 needs only a count and passes
        nothing. Counts are the bucket's length either way, so a caller that
        passes no item still gets correct numbers.
        """
        moment = _as_utc_instant(event_time)
        bucket = moment.replace(minute=0, second=0, microsecond=0)
        self._buckets.setdefault(key, {}).setdefault(bucket, []).append(item)
        self._watermark = moment
        return self.rolling_count(key)

    def bucket_counts(
        self, key: str, as_of: Optional[EventTime] = None
    ) -> list[tuple[datetime, int]]:
        """The `window_hours` (hour_start, count) pairs behind a rolling count.

        Oldest first, zero-filled for empty hours, ending at the as-of hour.

        THE EMPTY-ACCUMULATOR RULE, which exists because `rolling_count ==
        sum(bucket_counts)` has to hold across it: whenever `as_of` resolves to
        an hour, exactly `window_hours` pairs come back -- including on an
        accumulator nothing has been added to, and including for a key that has
        never been seen. `[]` comes back only when `as_of is None` and there is
        no watermark to resolve it from. Both branches keep the identity true:
        `window_hours` zeros sum to 0, and `[]` sums to 0.
        """
        moment = as_of if as_of is not None else self._watermark
        if moment is None:
            return []
        buckets = self._buckets.get(key, {})
        return [
            (start, len(buckets.get(start, ())))
            for start in window_bucket_starts(moment, self._window_hours)
        ]

    def rolling_count(self, key: str, as_of: Optional[EventTime] = None) -> int:
        """Plays under `key` in the `window_hours` buckets ending at the as-of hour.

        A key whose events all fall outside the window correctly returns 0; that
        is rolling semantics, not a bug.

        Implemented as the sum over `bucket_counts` so the total and the parts a
        caller is shown can never drift apart (WNDW-02).
        """
        return sum(count for _, count in self.bucket_counts(key, as_of))


def topology_a_windows(thresholds: Thresholds) -> RollingHourlyWindows:
    """An accumulator sized by `topology_a_window_hours`. It chooses a window size.

    No verdict lives here: whether a count exceeds `topology_a_plays_over` is
    Phase 3's strict `>`, not this module's business. Consumer 1 constructs its
    accumulator through this rather than passing an integer, so retuning the
    config reaches the running code (CD-4, CTRT-04).
    """
    return RollingHourlyWindows(thresholds.topology_a_window_hours)


def topology_b_windows(thresholds: Thresholds) -> RollingHourlyWindows:
    """An accumulator sized by `topology_b_window_hours`. It chooses a window size.

    No verdict lives here: the three Topology B conditions are Phase 4's. This
    function is also the answer to Phase 1 verification warning W4 --
    `topology_b_window_hours` was config-injected and honoured by nothing; this
    is the code path that reads it (CD-5, CTRT-04).
    """
    return RollingHourlyWindows(thresholds.topology_b_window_hours)
