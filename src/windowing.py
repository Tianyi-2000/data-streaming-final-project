"""Event-time windowing: hourly buckets and rolling N-hour counts.

This module counts. It does not judge -- every threshold comparison belongs to
Phase 3 (Consumer 1) and Phase 4 (Consumer 2), both of which import from here so
the two sides cannot drift on what "in the window" means.

Four things a later reader needs, in the order they matter.

1. EVERY WINDOW IS MEASURED ON `event_time`, AND NOTHING HERE READS THE SYSTEM
   CLOCK (WNDW-01, CD-7). Replay compresses three simulated days into a few
   seconds, so a wall-clock read does not raise -- it returns a confidently
   wrong number. There is no `datetime.now()`, no `datetime.utcnow()`, no
   `time.time()` and no `datetime.fromtimestamp()` anywhere in this file, and
   `test_the_windowing_module_never_reads_the_system_clock` proves it by parsing
   this source and walking the tree rather than by inspection -- naming those
   spellings here is exactly why the check parses instead of grepping.
   Timestamps arrive as contract strings or as aware datetimes and are parsed
   only by `contracts.play_event_v1.parse_event_time`.

2. BUCKETS ARE `event_time` TRUNCATED TO THE HOUR, AND A ROLLING N-HOUR COUNT IS
   THE SUM OF THE LAST N HOURLY BUCKETS ending at and including the as-of hour
   (WNDW-02). That is deliberately inspectable: `bucket_counts` hands back the N
   numbers and `rolling_count` is implemented as their sum, so the total can
   never disagree with the parts a caller was shown.

3. INTERVALS ARE HALF-OPEN. A bucket is `[hour, hour + 1h)` and an N-hour window
   is the N buckets spanning `[end - (N-1)h, end + 1h)`. An event landing exactly
   on a far edge belongs to the next bucket, never to both. The choice matters
   because Phase 3's comparison is a strict `>` sitting exactly where a
   one-bucket difference changes who gets accused. Pinned by
   `test_the_hour_edge_is_half_open` and `test_the_window_edge_is_half_open`,
   whose docstrings record the counts the closed convention would produce -- the
   checked-in fixture has about twelve hours of margin and cannot discriminate.

4. THIS IS NOT THE SLIDING MAXIMUM `tests/test_fixture_trips_rules.py::rule_a_score`
   COMPUTES. That helper anchors a window at each event's own timestamp and takes
   the maximum; this module aligns windows to hour boundaries. WNDW-02 is the
   requirement -- the bucket sum is what makes the number inspectable -- so
   Phase 3 must use THIS one and must not assume the other. Evidence, one step
   away in `tests/test_windowing.py`:
   `test_bucket_sum_and_sliding_max_are_not_the_same_function` exhibits an input
   where this module says 1 and the sliding maximum says 2, and
   `test_bucket_sum_and_sliding_max_agree_on_every_fixture_listener` guards the
   fact that they nonetheless agree across the whole checked-in fixture, so a
   future fixture edit that breaks the agreement reports it there rather than as
   a changed detection number in Phase 3.

   THE DIVERGENCE IS ONE-DIRECTIONAL: bucket-sum <= sliding-max, always. Any run
   of N buckets IS an N-hour span, and the sliding maximum maximises over ALL
   N-hour spans, so this module can under-count relative to a true sliding window
   and can never over-count. Phase 3 is therefore applying a strict `>` to a
   conservative number: at the margin this module can withhold a flag, but it
   cannot manufacture one. That is the right way round for a project whose stated
   ethical failure is accusing an innocent artist, and it is worth saying out loud
   rather than leaving as an accident.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional, Union

# Same import bootstrap `src/config.py` uses, so this module resolves however it
# is invoked and never depends on a conftest.py being present.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.play_event_v1 import parse_event_time  # noqa: E402
from src.config import Thresholds  # noqa: E402

HOUR = timedelta(hours=1)

_LOG = logging.getLogger(__name__)

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

    THE WATERMARK IS PER KEY, NOT ACCUMULATOR-WIDE. This is the single most
    important property of this class, and it was not the original design.

    An accumulator-wide watermark is correct only if events arrive in
    nondecreasing `event_time` order GLOBALLY. The producer does emit that way
    (GATE-ORDERING), but `play-events` has three partitions, and Kafka orders
    events within a partition -- never across them. A consumer polling all three
    receives contiguous runs from each, not a timestamp merge. Measured against
    the real 45,473-event stream, a shared watermark drops 55% of events as
    "late" at batch size 500 and 59% at 1000, because a run from one partition
    walks the watermark past hour boundaries that the other partitions have not
    reached yet. At batch 1000 it still reported the correct Topology A count
    while discarding 59% of the data -- a right answer over a shredded stream,
    which is worse than a wrong one.

    A per-key watermark only ever compares events that share a partition, since
    Kafka routes one key to one partition. The same measurement with per-key
    watermarks drops ZERO events under every consumption order tried.

    Two consequences follow, both of them the ones a caller wants:

    (1) `as_of` DEFAULTS TO THAT KEY'S OWN WATERMARK. Consumer 1 calls `add` per
        event and reads the return value, which is the count as of the event that
        just arrived. Consumer 2 can stream everything and then ask about a track
        whose burst was hours ago and get that track's real count, because the
        default is anchored to the track's last event rather than the stream's.
        Passing an explicit `as_of` is still the way to ask about a specific
        historical window, and `iter_buckets()` still walks everything.

    (2) LATENESS IS JUDGED PER KEY. An event on one key can no longer close
        another key's bucket. `late_dropped` remains an accumulator-wide TOTAL
        because it is a health metric, but a non-zero value now means genuine
        per-key disorder rather than ordinary cross-partition interleaving.

    RETENTION IS UNBOUNDED BY DECISION (threat T-02-06, accepted). Buckets are
    kept for the life of the accumulator, so memory grows with the number of
    distinct `(key, hour)` pairs. Across 45,473 events over three days that is a
    small map and it is correct; an implementation that evicts closed buckets is
    deliberately out of scope for this deadline. Recorded here so Phases 3 and 4
    inherit the property as a stated decision rather than an unexamined default.
    """

    def __init__(self, window_hours: int) -> None:
        if window_hours < 1:
            raise ValueError(f"window_hours must be at least 1; got {window_hours}")
        self._window_hours = window_hours
        # key -> {hour_start: [items]}. A plain dict rather than a defaultdict:
        # every read path below uses `.get`, so inspecting an unknown key can
        # never create an empty entry that later has to be filtered back out.
        self._buckets: dict[str, dict[datetime, list[Any]]] = {}
        # key -> greatest instant seen for THAT key. Per key, not shared: see
        # the class docstring on why a shared watermark loses most of a
        # multi-partition stream.
        self._watermarks: dict[str, datetime] = {}
        self._late_dropped = 0

    @property
    def window_hours(self) -> int:
        """How many hourly buckets a rolling count sums over."""
        return self._window_hours

    @property
    def watermark(self) -> Optional[datetime]:
        """The greatest instant across ALL keys, or None before the first add.

        Observability only. Nothing in this class judges lateness or resolves a
        default `as_of` against it -- both are per key. Use `watermark_for`.
        """
        return max(self._watermarks.values()) if self._watermarks else None

    def watermark_for(self, key: str) -> Optional[datetime]:
        """The greatest instant seen for `key`, or None if it has no events."""
        return self._watermarks.get(key)

    @property
    def late_dropped(self) -> int:
        """How many events were refused for belonging to an already-closed hour.

        An accumulator-wide TOTAL, but lateness is judged per key, so this counts
        genuine PER-KEY disorder only. Cross-partition interleaving no longer
        registers here: measured over the real 45,473-event stream, three-way
        interleaving at batch sizes 50, 500 and 1000 all yield zero.

        Non-zero on a real run therefore means one key's events arrived out of
        `event_time` order within its own partition -- the producer's
        nondecreasing guarantee (GATE-ORDERING) breaking for real, not the
        ordinary consequence of polling several partitions. Counts for the
        affected keys are not trustworthy. Worth surfacing in a consumer's own
        logging rather than leaving it in here.
        """
        return self._late_dropped

    def add(self, key: str, event_time: EventTime, item: Any = None) -> int:
        """Record one event under `key` and return that key's rolling count.

        INVARIANT: `add` returns the key's rolling count as of the watermark
        hour, after the add. Always an int, never None -- on the late path too.

        `item` is how Phase 4 keeps the events themselves: Consumer 2 recomputes
        unique listeners, plays per listener and band share from a bucket's
        members, so it passes the event. Consumer 1 needs only a count and passes
        nothing. Counts are the bucket's length either way, so a caller that
        passes no item still gets correct numbers.

        An event belonging to an hour the watermark has already passed is
        dropped, counted in `late_dropped` and logged once. It is never buffered
        and never re-sorted: WNDW-03 rests on the producer's nondecreasing-
        `event_time` guarantee, so this path should never fire on real data. It
        exists so that if the guarantee ever breaks, the project finds out from a
        counter and a log line rather than from a quietly wrong count.
        """
        moment = _as_utc_instant(event_time)
        bucket = moment.replace(minute=0, second=0, microsecond=0)

        # Lateness is judged at BUCKET granularity, not instant granularity: a
        # bucket stays open until the watermark moves past it. Two events inside
        # the same hour arriving in either order are therefore both legitimate,
        # while an event belonging to an hour already passed is the late arrival
        # WNDW-03 refuses to buffer.
        key_watermark = self._watermarks.get(key)
        if key_watermark is not None and bucket < hour_bucket(key_watermark):
            self._late_dropped += 1
            _LOG.warning(
                "late event dropped (not counted, not buffered): key=%s "
                "event_time=%s watermark=%s",
                key,
                moment.isoformat(),
                key_watermark.isoformat(),
            )
            return self.rolling_count(key)

        self._buckets.setdefault(key, {}).setdefault(bucket, []).append(item)
        # `max` rather than assignment: an earlier event inside the still-open
        # hour must not walk the watermark backwards and retroactively make the
        # next arrival look late.
        self._watermarks[key] = (
            moment if key_watermark is None else max(key_watermark, moment)
        )
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
        moment = as_of if as_of is not None else self._watermarks.get(key)
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

    def bucket_items(self, key: str, hour_start: EventTime) -> tuple[Any, ...]:
        """The items stored in one exact bucket, or an empty tuple.

        `hour_start` is truncated through `hour_bucket`, so a caller passing a
        real event time gets the bucket containing it rather than nothing.

        This is what Phase 4 consumes: unique listeners, plays per listener and
        band share are all computed from a bucket's MEMBERS, not from its count.
        """
        start = hour_bucket(hour_start)
        return tuple(self._buckets.get(key, {}).get(start, ()))

    def iter_buckets(self) -> Iterator[tuple[str, datetime, tuple[Any, ...]]]:
        """Every non-empty bucket as (key, hour_start, items), by key then hour.

        The ordering is required, not incidental: Phase 4 iterates buckets to
        produce `track_review_queue.json` and PROF-02 asserts that two replays of
        the same input produce an identical file.

        Iterating is also the safe way for Consumer 2 to read a stream it has
        already consumed in full -- see the class docstring on why the default
        `as_of` is a trap for that access pattern.
        """
        for key in sorted(self._buckets):
            for start in sorted(self._buckets[key]):
                items = self._buckets[key][start]
                if items:
                    yield key, start, tuple(items)


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
