"""Consumer 1: per-listener state and the re-key (`src/consumer_stage1.py`).

NO THRESHOLD LITERAL AND NO ORACLE LITERAL IS TYPED INTO THIS FILE. Every
expected number comes from `tests/fixtures/expected_flags.json` or from a
`Thresholds` loaded through `src.config.load_thresholds`, following
`tests/test_fixture_trips_rules.py`. That is what makes these tests a proof that
configuration reaches the running detector rather than a restatement of the same
numbers in a second place.

The broker-backed tests create throwaway topics with a uuid4 suffix, so re-runs
never read each other's records and the shared `play-events` / `track-activity`
topics are never written by a test.

Requires the broker for the tests marked as such: docker compose up -d
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.play_event_v1 import PlayEventV1  # noqa: E402
from src import consumer_stage1  # noqa: E402
from src.config import load_thresholds  # noqa: E402
from src.consumer_stage1 import (  # noqa: E402
    DUPLICATE_EVENT_ID,
    INVALID_VALUE,
    KEY_MISMATCH,
    Stage1Processor,
    replay_records,
)

BROKER = "localhost:9092"
REPLAY = REPO_ROOT / "src" / "replay_to_kafka.py"
PARTITIONS = 3

FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "play_events_fixture.jsonl"
EXPECTED_FLAGS_PATH = REPO_ROOT / "tests" / "fixtures" / "expected_flags.json"

EXPECTED: Dict[str, Any] = json.loads(EXPECTED_FLAGS_PATH.read_text(encoding="utf-8"))
FIXTURE_LINES: List[str] = [
    line
    for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
    if line.strip()
]
FIXTURE_THRESHOLDS = load_thresholds(EXPECTED["thresholds_path"])

INVALID_EVENTS_PATH = REPO_ROOT / EXPECTED["invalid_cases_path"]
INVALID_ENVELOPES: List[Dict[str, Any]] = [
    json.loads(line)
    for line in INVALID_EVENTS_PATH.read_text(encoding="utf-8").splitlines()
    if line.strip()
]

# How many envelope-wrapped invalid cases Phase 1 shipped. A structural count of
# the fixture file, not a threshold: it is here so that shrinking the file
# reduces the parametrized test's reach loudly rather than silently.
INVALID_CASE_COUNT = 9

# The one record the tracer sends: `replay_to_kafka.py` strips each line and
# encodes it, so this is exactly the byte string that reaches `play-events`.
FIRST_FIXTURE_BYTES = FIXTURE_LINES[0].strip().encode("utf-8")
FIRST_FIXTURE_RECORD: Dict[str, Any] = json.loads(FIXTURE_LINES[0])


def _reordered_valid_bytes() -> bytes:
    """A valid PlayEventV1 whose JSON *text* a model round-trip would change.

    `event_time` first and `schema_version` last (the model declares them the
    other way round), and a space after every colon and comma (pydantic emits
    none). Field values are lifted from the fixture's first line and the oracle's
    Topology A listener so nothing about the case is invented.
    """
    return (
        '{"event_time": "%s", "event_id": "fx-bytes-001", "event_type": "play", '
        '"listener_id": "%s", "track_id": "%s", "artist_id": "%s", '
        '"played_seconds": %d, "track_duration_seconds": %d, "schema_version": 1}'
        % (
            FIRST_FIXTURE_RECORD["event_time"],
            EXPECTED["topology_a"]["listener_id"],
            FIRST_FIXTURE_RECORD["track_id"],
            FIRST_FIXTURE_RECORD["artist_id"],
            FIRST_FIXTURE_RECORD["played_seconds"],
            FIRST_FIXTURE_RECORD["track_duration_seconds"],
        )
    ).encode("utf-8")


REORDERED_VALID_BYTES = _reordered_valid_bytes()


# --------------------------------------------------------------------------------
# Broker helpers (throwaway topics only -- never the contract's real topics)
# --------------------------------------------------------------------------------
def _admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": BROKER})


def _create_throwaway_topics(*names: str) -> None:
    """Create uuid-suffixed topics at 3 partitions and wait for metadata.

    3 partitions rather than 1 because the offset-commit assertion below is
    per partition, and a single-partition topic would not exercise it.
    """
    admin = _admin()
    futures = admin.create_topics(
        [
            NewTopic(name, num_partitions=PARTITIONS, replication_factor=1)
            for name in names
        ]
    )
    for name, future in futures.items():
        future.result(timeout=30)

    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        metadata = admin.list_topics(timeout=10)
        if all(
            name in metadata.topics and len(metadata.topics[name].partitions) >= PARTITIONS
            for name in names
        ):
            return
        time.sleep(0.3)
    raise RuntimeError(
        f"throwaway topics {list(names)} did not reach {PARTITIONS} partitions in "
        f"cluster metadata at {BROKER} -- is `docker compose up -d` running?"
    )


def _delete_throwaway_topics(*names: str) -> None:
    """Best-effort cleanup so repeated runs do not litter the broker."""
    try:
        for future in _admin().delete_topics(list(names)).values():
            try:
                future.result(timeout=30)
            except Exception:  # noqa: BLE001 - cleanup must never fail a test
                pass
    except Exception:  # noqa: BLE001
        pass


def _drain(topic: str, want: int, timeout: float = 30.0) -> list:
    """Every record on `topic`, read from the start in a throwaway group.

    Polls once more after `want` records arrive, so a caller asserting "exactly
    one" is asserting it rather than assuming it.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": f"drain-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.subscribe([topic])
        messages: list = []
        deadline = time.monotonic() + timeout
        while len(messages) < want and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            messages.append(msg)
        extra = consumer.poll(2.0)
        if extra is not None and not extra.error():
            messages.append(extra)
        return messages
    finally:
        consumer.close()


def _committed_offsets(group: str, topic: str) -> Dict[int, int]:
    """The group's committed offset per partition, as the broker reports it."""
    probe = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": group,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    try:
        committed = probe.committed(
            [TopicPartition(topic, p) for p in range(PARTITIONS)], timeout=20
        )
        return {tp.partition: tp.offset for tp in committed}
    finally:
        probe.close()


def _fixture_records() -> List[tuple]:
    """The 63 fixture lines as `(key_bytes, value_bytes)` pairs, in file order.

    The key is derived the way the shipped producer derives it -- UTF-8
    `listener_id` -- so every pair here is contract-valid on both halves.
    """
    return [
        (
            json.loads(line)["listener_id"].encode("utf-8"),
            line.strip().encode("utf-8"),
        )
        for line in FIXTURE_LINES
    ]


def _invalid_case_record(envelope: Dict[str, Any]) -> tuple:
    """One invalid envelope as a `(key_bytes, value_bytes)` pair.

    `kafka_key` when the envelope carries one -- case 9 is a wholly valid record
    arriving under the wrong key -- otherwise the record's own `listener_id`.
    """
    record = envelope["record"]
    key = envelope.get("kafka_key", record.get("listener_id", ""))
    return key.encode("utf-8"), json.dumps(record).encode("utf-8")


def _record_with(**overrides: Any) -> bytes:
    """The fixture's first record with fields replaced -- valid unless told otherwise."""
    record = dict(FIRST_FIXTURE_RECORD)
    record.update(overrides)
    return json.dumps(record).encode("utf-8")


def _replay_into(topic: str, limit: int, input_path: Path = FIXTURE_PATH):
    """Put `limit` events on `topic` using the SHIPPED producer, not a copy."""
    return subprocess.run(
        [
            sys.executable,
            str(REPLAY),
            "--input",
            str(input_path),
            "--topic",
            topic,
            "--limit",
            str(limit),
            "--broker",
            BROKER,
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=300,
    )


# --------------------------------------------------------------------------------
# Task 1: the tracer -- one event through every layer
# --------------------------------------------------------------------------------
def test_the_forwarded_value_is_the_original_bytes_not_a_model_round_trip():
    """Contract section 4: the value on `track-activity` is the value that arrived.

    THIS TEST HAS TO USE A HAND-BUILT INPUT, AND THE THIRD ASSERTION IS WHY.
    Measured across every line of `play_events_fixture.jsonl` and the head of
    `data/play_events.jsonl`, `model_validate_json(line).model_dump_json()` is
    byte-identical to `line`. So `forwarded == original` over fixture data passes
    even for an implementation that round-trips through the model -- a vacuous
    assertion that proves nothing about section 4. `REORDERED_VALID_BYTES` is a
    record the model accepts but whose text a round-trip normalises, so the same
    assertion discriminates.
    """
    event = PlayEventV1.model_validate_json(REORDERED_VALID_BYTES)

    # Without this the case could be an invalid record in disguise, and the test
    # below would pass for the wrong reason.
    assert event.listener_id == EXPECTED["topology_a"]["listener_id"]

    # The load-bearing line: a round trip really does change these bytes.
    assert REORDERED_VALID_BYTES != event.model_dump_json().encode("utf-8")

    processor = Stage1Processor(FIXTURE_THRESHOLDS)
    decision = processor.decide(
        event.listener_id.encode("utf-8"), REORDERED_VALID_BYTES
    )

    assert decision.forward is True
    assert decision.out_value == REORDERED_VALID_BYTES
    assert decision.out_key == event.track_id.encode("utf-8")


def test_the_tracer_path_rekeys_one_real_event_end_to_end(tmp_path):
    """One real event: play-events -> validate -> count -> track-activity -> commit.

    Needs the broker. The input event is placed by the shipped
    `src/replay_to_kafka.py`, so the producer seam is the real one.
    """
    in_topic = f"tracer-in-{uuid.uuid4().hex}"
    out_topic = f"tracer-out-{uuid.uuid4().hex}"
    _create_throwaway_topics(in_topic, out_topic)
    try:
        proc = _replay_into(in_topic, limit=1)
        assert proc.returncode == 0, (
            f"replay_to_kafka.py --limit 1 exited {proc.returncode}\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )

        group = f"stage1-tracer-{uuid.uuid4().hex}"
        review_path = tmp_path / "listener_review_queue.json"
        consumer_stage1.run(
            broker=BROKER,
            group=group,
            in_topic=in_topic,
            out_topic=out_topic,
            state_dir=tmp_path,
            thresholds=FIXTURE_THRESHOLDS,
            review_path=review_path,
            max_events=1,
        )

        # --- the re-key, on the original bytes ---------------------------------
        out_messages = _drain(out_topic, want=1)
        assert len(out_messages) == 1, (
            f"expected exactly one record on '{out_topic}', got {len(out_messages)}"
        )
        forwarded = out_messages[0]
        event = PlayEventV1.model_validate_json(FIRST_FIXTURE_BYTES)
        assert forwarded.key() == event.track_id.encode("utf-8")
        assert forwarded.value() == FIRST_FIXTURE_BYTES

        # --- the journal ------------------------------------------------------
        journal_path = tmp_path / consumer_stage1.JOURNAL_FILENAME
        assert journal_path.is_file()
        journal_lines = [
            line for line in journal_path.read_text(encoding="utf-8").splitlines() if line
        ]
        assert len(journal_lines) == 1
        assert json.loads(journal_lines[0])["event_id"] == event.event_id

        # --- the review artifact ----------------------------------------------
        assert review_path.is_file()
        review = json.loads(review_path.read_text(encoding="utf-8"))
        # One play cannot exceed a threshold of ten, and the artifact path still
        # has to be proven to run.
        assert review["flagged_listeners"] == []

        # --- the offset, committed only after the produce ---------------------
        in_messages = _drain(in_topic, want=1)
        assert len(in_messages) == 1
        landed_on = in_messages[0].partition()
        offsets = _committed_offsets(group, in_topic)
        assert offsets[landed_on] == 1, (
            f"expected committed offset 1 on partition {landed_on} of '{in_topic}', "
            f"got {offsets}"
        )
    finally:
        _delete_throwaway_topics(in_topic, out_topic)


# --------------------------------------------------------------------------------
# Task 2: the decision layer -- invalid classification and dedup
# --------------------------------------------------------------------------------
def test_the_invalid_case_fixture_still_holds_every_documented_case():
    """A shrunken fixture must fail here, not quietly hollow out the test below."""
    assert len(INVALID_ENVELOPES) == INVALID_CASE_COUNT
    assert sum(1 for e in INVALID_ENVELOPES if e["expect"] == "key_mismatch") == 1


@pytest.mark.parametrize(
    "envelope", INVALID_ENVELOPES, ids=[e["case"] for e in INVALID_ENVELOPES]
)
def test_every_contract_invalid_case_is_dropped_with_its_documented_reason(envelope):
    """Contract section 6: every invalid record is logged and never forwarded.

    Eight cases the shared model rejects; the ninth is a wholly valid record
    arriving under another listener's key, which the model structurally cannot
    catch and `key_matches_listener_id` therefore has to.
    """
    key, value = _invalid_case_record(envelope)
    decision = Stage1Processor(FIXTURE_THRESHOLDS).decide(key, value)

    assert decision.forward is False
    expected_reason = (
        KEY_MISMATCH if envelope["expect"] == "key_mismatch" else INVALID_VALUE
    )
    assert decision.drop_reason == expected_reason

    if envelope["error_contains"] is not None:
        assert envelope["error_contains"] in decision.detail, (
            f"case {envelope['case']}: detail does not name "
            f"{envelope['error_contains']!r}\n{decision.detail}"
        )
    if expected_reason is KEY_MISMATCH:
        # Both sides of the disagreement, so the log line is actionable.
        assert envelope["kafka_key"] in decision.detail
        assert envelope["record"]["listener_id"] in decision.detail


@pytest.mark.parametrize(
    "envelope", INVALID_ENVELOPES, ids=[e["case"] for e in INVALID_ENVELOPES]
)
def test_a_dropped_record_never_produces_an_outgoing_value(envelope):
    """Nothing invalid reaches `track-activity`, key or value."""
    key, value = _invalid_case_record(envelope)
    decision = Stage1Processor(FIXTURE_THRESHOLDS).decide(key, value)
    assert decision.out_value is None
    assert decision.out_key is None


# --------------------------------------------------------------------------------
# Task 2: the Topology A judgment
# --------------------------------------------------------------------------------
def _fixture_processor(thresholds=FIXTURE_THRESHOLDS) -> Stage1Processor:
    processor = Stage1Processor(thresholds)
    replay_records(processor, _fixture_records())
    return processor


def test_the_fixture_flags_exactly_the_oracles_topology_a_listener():
    """The oracle's flagged list, reproduced by the running detector."""
    processor = _fixture_processor()
    flagged = [entry["listener_id"] for entry in processor.flagged_listeners()]

    assert flagged == EXPECTED["expected_flagged_listeners"]
    assert set(flagged).isdisjoint(EXPECTED["expected_unflagged_listeners"])


def test_the_listener_sitting_exactly_on_the_threshold_is_not_flagged():
    """CD-4 is strictly MORE THAN. Flip `>` to `>=` here and FN01 flags.

    Every other listener clears or misses its threshold with margin, so without
    this case the comparison operator itself is untested.
    """
    boundary = EXPECTED["boundaries"]["topology_a_strict_greater_than"]
    processor = _fixture_processor()

    assert processor.peak_for(boundary["listener_id"]) == boundary["plays_in_window"]
    assert boundary["plays_in_window"] == FIXTURE_THRESHOLDS.topology_a_plays_over
    assert boundary["expected_flagged"] is False
    flagged = [entry["listener_id"] for entry in processor.flagged_listeners()]
    assert boundary["listener_id"] not in flagged


def test_a_listener_that_crosses_the_threshold_and_goes_quiet_stays_flagged():
    """The regression this phase exists to avoid.

    `RollingHourlyWindows.add` returns the count as of the event just added, and
    that count FALLS as the 24-bucket window slides past hours the listener did
    not play in. Here QUIET plays twelve times inside one hour, then once two
    days later: `add`'s last return for QUIET is 1, well under the threshold of
    10. An implementation that flags on "the last value `add` returned" reports
    1 and silently un-flags a listener it had already flagged. The flag has to
    come from a per-listener high-water mark.

    Synthetic rather than fixture-driven: every fixture listener's last event is
    inside its own peak window, so the fixture cannot discriminate.
    """
    plays_over = FIXTURE_THRESHOLDS.topology_a_plays_over
    processor = Stage1Processor(FIXTURE_THRESHOLDS)

    burst = plays_over + 2
    last_return = 0
    for i in range(burst):
        last_return = processor.commit_event(
            f"quiet-{i:03d}", "QUIET", f"2026-08-08T00:{i:02d}:00Z"
        )
    assert last_return > plays_over

    # Two days later, the burst has slid out of the window entirely.
    later_return = processor.commit_event("quiet-late", "QUIET", "2026-08-10T00:00:00Z")
    assert later_return < plays_over

    assert processor.peak_for("QUIET") > plays_over
    flagged = [entry["listener_id"] for entry in processor.flagged_listeners()]
    assert "QUIET" in flagged


def test_the_same_event_id_is_counted_once_and_a_different_one_is_not():
    """Dedup is keyed on `event_id` and on nothing else (contract section 5)."""
    listener = FIRST_FIXTURE_RECORD["listener_id"]
    key = listener.encode("utf-8")
    original = _record_with()
    processor = Stage1Processor(FIXTURE_THRESHOLDS)

    first, second = replay_records(processor, [(key, original), (key, original)])
    assert first.forward is True
    assert second.forward is False
    assert second.drop_reason == DUPLICATE_EVENT_ID
    peak_after_duplicate = processor.peak_for(listener)
    assert peak_after_duplicate == 1

    # Identical in every field but `event_id`: a genuinely different play.
    twin = _record_with(event_id=FIRST_FIXTURE_RECORD["event_id"] + "-twin")
    (third,) = replay_records(processor, [(key, twin)])
    assert third.forward is True
    assert processor.peak_for(listener) == peak_after_duplicate + 1


# --------------------------------------------------------------------------------
# Task 2: the review document
# --------------------------------------------------------------------------------
def test_the_review_document_names_the_numbers_behind_every_flag():
    """A flag a reader cannot dispute is an accusation, not a review queue.

    Every entry has to carry the count that produced it, the window it was
    counted over, the threshold it was compared against, and the span of the
    listener's activity -- and the document has to name the config file the
    thresholds came from.
    """
    processor = _fixture_processor()
    document = processor.review_document()

    assert document["flagged_listeners"], "no flags, so this test would be vacuous"
    for entry in document["flagged_listeners"]:
        for field in (
            "listener_id",
            "peak_plays_in_window",
            "window_hours",
            "threshold",
            "first_event_time",
            "last_event_time",
        ):
            assert field in entry, f"review entry is missing {field}"
        assert entry["threshold"] == FIXTURE_THRESHOLDS.topology_a_plays_over
        assert entry["window_hours"] == FIXTURE_THRESHOLDS.topology_a_window_hours
        assert entry["peak_plays_in_window"] > entry["threshold"]

    assert document["thresholds_source_path"] == FIXTURE_THRESHOLDS.source_path


def test_the_review_document_is_byte_stable_across_two_identical_runs():
    """PROF-02 leans on this in Phase 5: same input, byte-identical artifact."""
    first = json.dumps(_fixture_processor().review_document(), indent=2, sort_keys=True)
    second = json.dumps(_fixture_processor().review_document(), indent=2, sort_keys=True)
    assert first.encode("utf-8") == second.encode("utf-8")


def test_the_accumulator_is_built_through_the_sanctioned_factory():
    """Retuning the window in configuration has to reach the running detector.

    A `RollingHourlyWindows(24)` built from a literal passes every other test in
    this file and fails this one (CTRT-04).
    """
    retuned_hours = 3
    assert retuned_hours != FIXTURE_THRESHOLDS.topology_a_window_hours
    retuned = FIXTURE_THRESHOLDS.model_copy(
        update={"topology_a_window_hours": retuned_hours}
    )
    assert Stage1Processor(retuned).window_hours == retuned.topology_a_window_hours


# --------------------------------------------------------------------------------
# Task 3: the run loop -- recovery, offset discipline, late_dropped, the CLI
# --------------------------------------------------------------------------------
def _fixture_listener_ids() -> List[str]:
    return sorted({json.loads(line)["listener_id"] for line in FIXTURE_LINES})


def _peaks(processor: Stage1Processor) -> Dict[str, int]:
    return {
        listener_id: processor.peak_for(listener_id)
        for listener_id in _fixture_listener_ids()
    }


def _journal(path: Path) -> consumer_stage1.Journal:
    return consumer_stage1.Journal(path)


def test_a_restarted_processor_rebuilds_its_state_from_the_journal_alone(tmp_path):
    """The journal is the only persisted state, and it is enough.

    Counts, flags and the dedup set are all derived by replaying it through the
    same `commit_event` the live path uses, so a restart cannot drift from an
    uninterrupted run.
    """
    records = _fixture_records()
    split = 30
    journal_path = tmp_path / consumer_stage1.JOURNAL_FILENAME

    before = Stage1Processor(FIXTURE_THRESHOLDS)
    writer = _journal(journal_path)
    replay_records(before, records[:split], journal=writer)
    writer.close()

    restarted = Stage1Processor(FIXTURE_THRESHOLDS)
    recovered = consumer_stage1.recover_from_journal(restarted, _journal(journal_path))
    assert recovered == before.counts()["forwarded"]
    replay_records(restarted, records[split:])

    uninterrupted = Stage1Processor(FIXTURE_THRESHOLDS)
    replay_records(uninterrupted, records)

    assert uninterrupted.flagged_listeners(), "no flags, so this test would be vacuous"
    assert restarted.flagged_listeners() == uninterrupted.flagged_listeners()
    assert _peaks(restarted) == _peaks(uninterrupted)
    assert restarted.late_dropped == uninterrupted.late_dropped


def test_recovery_survives_a_journal_truncated_mid_line(tmp_path, caplog):
    """A SIGKILL lands mid-write; recovery must not be the thing that breaks."""
    records = _fixture_records()
    journal_path = tmp_path / consumer_stage1.JOURNAL_FILENAME

    writer = _journal(journal_path)
    replay_records(Stage1Processor(FIXTURE_THRESHOLDS), records[:10], journal=writer)
    writer.close()

    lines = journal_path.read_text(encoding="utf-8").splitlines()
    complete, last = lines[:-1], lines[-1]
    half = last[: max(1, len(last) // 2)]
    assert half != last, "the truncation has to actually remove something"
    journal_path.write_text("\n".join(complete) + "\n" + half, encoding="utf-8")

    restarted = Stage1Processor(FIXTURE_THRESHOLDS)
    with caplog.at_level(logging.WARNING):
        recovered = consumer_stage1.recover_from_journal(
            restarted, _journal(journal_path)
        )

    assert recovered == len(complete)
    assert "journal" in caplog.text.lower()


def test_a_recovered_event_id_is_still_deduplicated(tmp_path):
    """Dedup state is derived from the journal, so it cannot drift from it."""
    key = FIRST_FIXTURE_RECORD["listener_id"].encode("utf-8")
    journal_path = tmp_path / consumer_stage1.JOURNAL_FILENAME

    writer = _journal(journal_path)
    replay_records(
        Stage1Processor(FIXTURE_THRESHOLDS),
        [(key, FIRST_FIXTURE_BYTES)],
        journal=writer,
    )
    writer.close()

    restarted = Stage1Processor(FIXTURE_THRESHOLDS)
    assert consumer_stage1.recover_from_journal(restarted, _journal(journal_path)) == 1

    decision = restarted.decide(key, FIRST_FIXTURE_BYTES)
    assert decision.forward is False
    assert decision.drop_reason == DUPLICATE_EVENT_ID


def test_the_run_summary_reports_late_dropped_and_the_counters(tmp_path, capsys):
    """`late_dropped` is surfaced rather than swallowed, in both outputs.

    The final stdout line is machine-readable on purpose: the end-to-end tests
    in later phases parse it instead of running a regex over prose.
    """
    in_topic = f"summary-in-{uuid.uuid4().hex}"
    out_topic = f"summary-out-{uuid.uuid4().hex}"
    _create_throwaway_topics(in_topic, out_topic)
    try:
        limit = 12
        proc = _replay_into(in_topic, limit=limit)
        assert proc.returncode == 0, proc.stderr

        summary = consumer_stage1.run(
            broker=BROKER,
            group=f"stage1-summary-{uuid.uuid4().hex}",
            in_topic=in_topic,
            out_topic=out_topic,
            state_dir=tmp_path,
            thresholds=FIXTURE_THRESHOLDS,
            review_path=tmp_path / "listener_review_queue.json",
            max_events=limit,
            commit_every=4,
        )

        printed = capsys.readouterr().out
        summary_lines = [
            line for line in printed.splitlines() if line.startswith("SUMMARY ")
        ]
        assert len(summary_lines) == 1, (
            f"expected exactly one machine-readable SUMMARY line:\n{printed}"
        )
        reported = json.loads(summary_lines[0][len("SUMMARY ") :])

        for field in (
            "polled",
            "forwarded",
            "invalid_value",
            "key_mismatch",
            "duplicate_event_id",
            "late_dropped",
            "flagged_listeners",
        ):
            assert hasattr(summary, field), f"RunSummary is missing {field}"
            assert field in reported, f"the SUMMARY line is missing {field}"
            assert getattr(summary, field) == reported[field]

        assert summary.polled == limit
        assert summary.forwarded == limit
        assert summary.late_dropped == 0
    finally:
        _delete_throwaway_topics(in_topic, out_topic)

    # And `late_dropped` is READ OFF THE ACCUMULATOR rather than tracked here: a
    # second counter kept alongside could disagree with the number the windowing
    # module actually acted on. Forced per-key disorder, three readings, one value.
    disordered = Stage1Processor(FIXTURE_THRESHOLDS)
    listener = FIRST_FIXTURE_RECORD["listener_id"]
    disordered.commit_event("disorder-late-1", listener, "2026-08-10T00:00:00Z")
    disordered.commit_event("disorder-late-2", listener, "2026-08-08T00:00:00Z")
    assert disordered.late_dropped == 1
    assert disordered.counts()["late_dropped"] == disordered.late_dropped
    assert (
        disordered.review_document()["counts"]["late_dropped"]
        == disordered.late_dropped
    )


def test_the_cli_refuses_to_start_without_a_threshold_config(capsys):
    """No threshold field carries a default, so there is nothing to fall back to."""
    missing = "config/nope.json"
    code = consumer_stage1.main(["--thresholds", missing])
    assert code != 0
    assert "nope.json" in capsys.readouterr().err
