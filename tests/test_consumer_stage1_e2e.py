"""Phase 3's five success criteria, observed against a running broker at 3 partitions.

Plan 01's tests run mostly in-process: they drive `Stage1Processor` and
`replay_records` directly and prove the arithmetic. The guarantees this phase
*claims* are about a running consumer -- real partitioning, real delivery
callbacks, real committed offsets and a real SIGKILL. Those are only true if they
are observed, so every test in this file runs `src/consumer_stage1.py` as a
subprocess and reads what actually crossed the wire.

Three deliberate choices, each of which a later reader would otherwise undo.

1. EVERY TEST CREATES ITS OWN TOPICS. `play-events` accumulates across runs --
   `tests/test_harness_roundtrip.py` records this and asserts `>=` rather than
   `==` because of it. An exact-count assertion on a shared topic is flaky for
   reasons that have nothing to do with correctness, and every criterion here is
   an exact count. So each test gets a fresh input topic and a fresh output topic
   named with a uuid4 suffix, built through `AdminClient` directly:
   `src/create_topics.py::ensure_topics` cannot be reused because it names the
   contract's real topics, which is exactly what must not be touched here.

2. THREE PARTITIONS, NOT ONE. Per-key watermarks are what make the
   multi-partition topology safe (a shared watermark dropped 55% of the real
   stream; per key it drops zero). A one-partition test would not exercise the
   thing that makes the design work, and `late_dropped == 0` would be a much
   weaker claim.

3. THE SHIPPED PRODUCER DRIVES THE VALID RECORDS. `src/replay_to_kafka.py` is
   shelled out to rather than reimplemented, so what is proven is that *that
   script* feeds this consumer. Invalid records are the exception: the producer
   re-validates and refuses to send them, which is correct producer behaviour and
   is precisely why this file bypasses it with a plain `Producer` for those 9.

No count, threshold or listener id is typed into an assertion. Every expected
number comes from `tests/fixtures/expected_flags.json`, from
`config/thresholds.fixture.json`, or from the fixture files on disk.

Requires the broker: docker compose up -d
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src.consumer_stage1 import JOURNAL_FILENAME

REPO_ROOT = Path(__file__).resolve().parents[1]
BROKER = "localhost:9092"

CONSUMER_SCRIPT = REPO_ROOT / "src" / "consumer_stage1.py"
REPLAY_SCRIPT = REPO_ROOT / "src" / "replay_to_kafka.py"

FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "play_events_fixture.jsonl"
INVALID_PATH = REPO_ROOT / "tests" / "fixtures" / "invalid_events.jsonl"
ORACLE_PATH = REPO_ROOT / "tests" / "fixtures" / "expected_flags.json"
FIXTURE_THRESHOLDS = REPO_ROOT / "config" / "thresholds.fixture.json"

# Both topics are created at the same width `src/create_topics.py` uses.
PARTITIONS = 3

# Bounded everywhere: threat T-03-13 is this suite's own wall-clock cost.
DRAIN_TIMEOUT_SECONDS = 30.0
SUBPROCESS_TIMEOUT_SECONDS = 240.0
TOPIC_READY_TIMEOUT_SECONDS = 30.0
# Consecutive empty polls that mean "the topic has no more records for us".
DRAIN_IDLE_POLLS = 3
# A SIGKILLed consumer never leaves its group, so the coordinator has to expire
# its session before a restart can be assigned anything. Measured at 45.3s
# against this broker (librdkafka's `session.timeout.ms` default is 45000).
GROUP_RELEASE_TIMEOUT_SECONDS = 120.0

# The kill test's pace. 63 records at 0.25s each cannot finish in under 15.75s,
# so a kill at 8.0s cannot possibly land after the run completed -- which is the
# failure mode that would make the test pass while proving nothing (T-03-10).
KILL_TEST_THROTTLE_SECONDS = 0.25
KILL_TEST_RUN_SECONDS = 8.0


# --- fixture facts, read rather than typed -------------------------------
def _oracle() -> Dict[str, Any]:
    """The ground-truth file Phase 1 built. Nothing here hardcodes its numbers."""
    return json.loads(ORACLE_PATH.read_text(encoding="utf-8"))


def _fixture_lines() -> List[bytes]:
    """The 63 fixture lines as the exact bytes on disk.

    Byte comparison, not model comparison: contract section 4 requires the value
    on `track-activity` to be the same JSON that arrived, and
    `src/replay_to_kafka.py` sends `line.encode("utf-8")`, so these bytes are
    literally what the consumer should hand back.
    """
    return [
        line.encode("utf-8")
        for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _invalid_envelopes() -> List[Dict[str, Any]]:
    """The 9 envelope-wrapped invalid cases, each with its `record` and reason."""
    return [
        json.loads(line)
        for line in INVALID_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


# --- topics ---------------------------------------------------------------
def _admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": BROKER})


def _throwaway_topics() -> Tuple[str, str]:
    """Create a fresh 3-partition input/output pair and wait for cluster metadata.

    The uuid suffix is what keeps this file from ever reading or writing the
    contract's own `play-events` / `track-activity` (threat T-03-09).
    """
    suffix = uuid.uuid4().hex[:12]
    in_topic = f"e2e-play-events-{suffix}"
    out_topic = f"e2e-track-activity-{suffix}"

    admin = _admin()
    futures = admin.create_topics(
        [
            NewTopic(name, num_partitions=PARTITIONS, replication_factor=1)
            for name in (in_topic, out_topic)
        ]
    )
    for name, future in futures.items():
        future.result(timeout=TOPIC_READY_TIMEOUT_SECONDS)

    deadline = time.monotonic() + TOPIC_READY_TIMEOUT_SECONDS
    while True:
        metadata = admin.list_topics(timeout=10)
        present = [
            name
            for name in (in_topic, out_topic)
            if name in metadata.topics
            and len(metadata.topics[name].partitions) == PARTITIONS
        ]
        if len(present) == 2:
            break
        assert time.monotonic() < deadline, (
            f"topics {in_topic!r}/{out_topic!r} did not reach {PARTITIONS} "
            f"partitions in cluster metadata within "
            f"{TOPIC_READY_TIMEOUT_SECONDS:.0f}s -- is `docker compose up -d` running?"
        )
        time.sleep(0.5)

    return in_topic, out_topic


def _delete_topics(names: Sequence[str]) -> None:
    """Best effort. A cleanup failure must never mask a real assertion."""
    try:
        futures = _admin().delete_topics(list(names))
        for future in futures.values():
            try:
                future.result(timeout=TOPIC_READY_TIMEOUT_SECONDS)
            except Exception:  # noqa: BLE001 -- cleanup only
                pass
    except Exception:  # noqa: BLE001 -- cleanup only
        pass


@pytest.fixture()
def topics():
    """A throwaway 3-partition input/output pair, deleted afterwards."""
    pair = _throwaway_topics()
    try:
        yield pair
    finally:
        _delete_topics(pair)


# --- driving the two kinds of input --------------------------------------
def _summary_number(stdout: str, label: str) -> int:
    """One labelled count out of the producer's summary line.

    Keyed off the label rather than the whole sentence, so a wording change in
    the producer cannot fail these tests for the wrong reason.
    """
    match = re.search(rf"{label}\s+(\d+)", stdout, flags=re.IGNORECASE)
    assert match is not None, (
        f"could not find a '{label} <n>' count in replay output:\n{stdout}"
    )
    return int(match.group(1))


def _replay_fixture(topic: str) -> subprocess.CompletedProcess:
    """Put the 63 valid fixture events on `topic` using the shipped producer.

    It keys by `listener_id` and re-validates every line, so all 63 arrive and 0
    are rejected -- both numbers are asserted rather than assumed, because a
    silent rejection here would make every downstream count wrong for a reason
    that has nothing to do with the consumer.
    """
    proc = subprocess.run(
        [
            sys.executable,
            str(REPLAY_SCRIPT),
            "--input",
            str(FIXTURE_PATH),
            "--topic",
            topic,
            "--broker",
            BROKER,
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
    )
    assert proc.returncode == 0, (
        f"replay_to_kafka.py exited {proc.returncode}\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    total = len(_fixture_lines())
    assert _summary_number(proc.stdout, "queued") == total
    assert _summary_number(proc.stdout, "rejected") == 0
    assert _summary_number(proc.stdout, "delivery failures") == 0
    return proc


def _produce_invalid_records(topic: str) -> List[str]:
    """Put all 9 contract-invalid records on `topic`, bypassing the producer.

    `src/replay_to_kafka.py` re-validates and refuses to send these, which is the
    correct producer behaviour and exactly why this test must not use it: the
    boundary under attack is `play-events` -> Consumer 1, and a record the
    producer would never send is precisely the record Consumer 1 has to reject on
    its own. Returns the `event_id`s so a test can assert none of them arrived.
    """
    envelopes = _invalid_envelopes()
    producer = Producer(
        {"bootstrap.servers": BROKER, "acks": "all", "client.id": "e2e-invalid"}
    )
    failures: List[Any] = []

    def _on_delivery(err, _msg) -> None:
        if err is not None:
            failures.append(err)

    event_ids: List[str] = []
    for envelope in envelopes:
        record = envelope["record"]
        # `kafka_key` when the envelope states one -- that is how the
        # key-vs-value mismatch case declares its wrong key. Otherwise the key
        # the producer would have derived.
        key = envelope.get("kafka_key")
        if key is None:
            key = record.get("listener_id", "")
        producer.produce(
            topic,
            key=str(key).encode("utf-8"),
            value=json.dumps(record).encode("utf-8"),
            on_delivery=_on_delivery,
        )
        producer.poll(0)
        event_ids.append(record.get("event_id"))

    remaining = producer.flush(DRAIN_TIMEOUT_SECONDS)
    assert remaining == 0, f"{remaining} invalid record(s) never left the producer"
    assert not failures, f"delivery failures placing invalid records: {failures}"
    assert len(event_ids) == len(envelopes)
    return event_ids


# --- running the consumer -------------------------------------------------
def _consumer_argv(
    *,
    in_topic: str,
    out_topic: str,
    state_dir: Path,
    review_out: Path,
    group: str,
    commit_every: int,
    idle_timeout: float,
    throttle: float,
) -> List[str]:
    return [
        sys.executable,
        str(CONSUMER_SCRIPT),
        "--broker",
        BROKER,
        "--group",
        group,
        "--in-topic",
        in_topic,
        "--out-topic",
        out_topic,
        "--state-dir",
        str(state_dir),
        "--review-out",
        str(review_out),
        "--thresholds",
        str(FIXTURE_THRESHOLDS),
        "--commit-every",
        str(commit_every),
        "--idle-timeout",
        str(idle_timeout),
        "--throttle",
        str(throttle),
    ]


@dataclass(frozen=True)
class _ConsumerRun:
    """One completed consumer run: its parsed SUMMARY plus both raw streams."""

    summary: Dict[str, Any]
    stdout: str
    stderr: str


def _parse_summary(stdout: str) -> Dict[str, Any]:
    """The last `SUMMARY {json}` line on stdout.

    Tests key off these parsed counts and never off the human-readable prose
    above them, so rewording the report cannot fail a test for the wrong reason.
    """
    lines = [
        line for line in stdout.splitlines() if line.startswith("SUMMARY ")
    ]
    assert lines, f"no 'SUMMARY <json>' line on consumer stdout:\n{stdout}"
    return json.loads(lines[-1][len("SUMMARY "):])


def _run_consumer(
    *,
    in_topic: str,
    out_topic: str,
    state_dir: Path,
    review_out: Path,
    group: Optional[str] = None,
    commit_every: int = 1,
    idle_timeout: float = 6.0,
    throttle: float = 0.0,
    timeout: float = SUBPROCESS_TIMEOUT_SECONDS,
) -> _ConsumerRun:
    """Run `src/consumer_stage1.py` to idle and return its parsed SUMMARY."""
    argv = _consumer_argv(
        in_topic=in_topic,
        out_topic=out_topic,
        state_dir=state_dir,
        review_out=review_out,
        group=group or f"e2e-{uuid.uuid4()}",
        commit_every=commit_every,
        idle_timeout=idle_timeout,
        throttle=throttle,
    )
    proc = subprocess.run(
        argv,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    assert proc.returncode == 0, (
        f"consumer_stage1.py exited {proc.returncode}\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    return _ConsumerRun(
        summary=_parse_summary(proc.stdout),
        stdout=proc.stdout,
        stderr=proc.stderr,
    )


def _start_consumer(
    *,
    in_topic: str,
    out_topic: str,
    state_dir: Path,
    review_out: Path,
    group: str,
    log_dir: Path,
    commit_every: int = 1,
    idle_timeout: float = 6.0,
    throttle: float = 0.0,
) -> Tuple[subprocess.Popen, Path, Path]:
    """Start the consumer and return the live process plus its two log paths.

    stdout and stderr go to FILES, not pipes. A process that is going to be
    SIGKILLed cannot be allowed to block on a full pipe buffer -- the test would
    hang rather than fail, and the hang would look like a broker problem.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    out_path = log_dir / f"stdout-{uuid.uuid4().hex[:8]}.log"
    err_path = log_dir / f"stderr-{uuid.uuid4().hex[:8]}.log"
    argv = _consumer_argv(
        in_topic=in_topic,
        out_topic=out_topic,
        state_dir=state_dir,
        review_out=review_out,
        group=group,
        commit_every=commit_every,
        idle_timeout=idle_timeout,
        throttle=throttle,
    )
    with out_path.open("wb") as out_handle, err_path.open("wb") as err_handle:
        proc = subprocess.Popen(
            argv, cwd=str(REPO_ROOT), stdout=out_handle, stderr=err_handle
        )
    return proc, out_path, err_path


def _wait_for_group_to_release_its_members(
    group: str, timeout: float = GROUP_RELEASE_TIMEOUT_SECONDS
) -> float:
    """Block until the consumer group has no members, and return the wait in seconds.

    WHY THIS EXISTS, AND WHY IT IS NOT PAPERING OVER ANYTHING. A SIGKILLed
    consumer never sends a LeaveGroup, so as far as the group coordinator is
    concerned it is still a member holding all three partitions. A restart on the
    same group is assigned nothing until the coordinator expires that member's
    session, which is `session.timeout.ms` -- 45 seconds by librdkafka's default,
    measured at 45.3s against this broker. That is a property of Kafka consumer
    groups, not of the code under test.

    Waiting for it explicitly is what keeps the restart's own `--idle-timeout`
    short. The alternative is an idle timeout longer than the eviction, which
    would then also be paid again as dead time after the restart finished its
    work, and would turn a legitimate assignment delay into an unexplained
    minute of silence in the test log. A clean shutdown returns from here almost
    immediately, which is itself the visible difference between SIGTERM and
    SIGKILL.
    """
    admin = _admin()
    started = time.monotonic()
    deadline = started + timeout
    while True:
        try:
            description = admin.describe_consumer_groups([group])[group].result(
                timeout=15
            )
            members = len(description.members)
        except Exception as exc:  # noqa: BLE001 -- transient coordinator lookups
            members = -1
            last_error: Optional[str] = repr(exc)
        else:
            last_error = None
        if members == 0:
            return time.monotonic() - started
        assert time.monotonic() < deadline, (
            f"consumer group {group!r} still reports {members} member(s) after "
            f"{timeout:.0f}s; the SIGKILLed member's session should have expired "
            f"long before now (last error: {last_error})"
        )
        time.sleep(1.0)


# --- reading the output topic back ---------------------------------------
@dataclass(frozen=True)
class _Record:
    """One record read off a topic, with its bytes pulled out eagerly.

    Extracted rather than holding the `Message` object, so the records stay
    readable after the draining consumer is closed and a test cannot
    accidentally depend on a live client.
    """

    key: Optional[bytes]
    value: Optional[bytes]
    partition: int
    offset: int


def _drain(
    topic: str,
    expected_at_least: int = 0,
    timeout: float = DRAIN_TIMEOUT_SECONDS,
) -> List[_Record]:
    """Every record on `topic`, in arrival order, from a throwaway earliest group.

    Stops once `expected_at_least` records are in hand AND several consecutive
    polls came back empty, so an exact-count assertion is made against a topic
    that has genuinely stopped producing rather than one that was read too early.
    On a shortfall it keeps polling to the deadline and returns what it has, so
    the failure message shows the real number.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": f"e2e-drain-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    records: List[_Record] = []
    try:
        consumer.subscribe([topic])
        deadline = time.monotonic() + timeout
        idle = 0
        while time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                if len(records) >= expected_at_least and idle >= DRAIN_IDLE_POLLS:
                    break
                continue
            if msg.error():
                continue
            idle = 0
            records.append(
                _Record(
                    key=msg.key(),
                    value=msg.value(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
            )
    finally:
        consumer.close()
    return records


# =========================================================================
# Task 1 -- the forwarding proof
# =========================================================================
@dataclass(frozen=True)
class _ForwardingRun:
    """One consumer run over the 63 valid fixture events plus all 9 invalid ones."""

    run: _ConsumerRun
    review: Dict[str, Any]
    records: List[_Record]
    invalid_event_ids: List[str]


@pytest.fixture(scope="module")
def forwarding_run(tmp_path_factory):
    """The single real run that ROADMAP SC1, SC2 and SC3 are all read off.

    One run rather than four: the criteria are claims about the same execution --
    that the invalid records were dropped *while* the valid ones were forwarded
    *and* the review document written from the resulting state. Splitting it into
    four runs would cost four idle timeouts and prove slightly less.
    """
    in_topic, out_topic = _throwaway_topics()
    try:
        workdir = tmp_path_factory.mktemp("forwarding")
        _replay_fixture(in_topic)
        invalid_event_ids = _produce_invalid_records(in_topic)

        review_out = workdir / "listener_review_queue.json"
        run = _run_consumer(
            in_topic=in_topic,
            out_topic=out_topic,
            state_dir=workdir / "state",
            review_out=review_out,
        )
        records = _drain(out_topic, expected_at_least=len(_fixture_lines()))
        yield _ForwardingRun(
            run=run,
            review=json.loads(review_out.read_text(encoding="utf-8")),
            records=records,
            invalid_event_ids=invalid_event_ids,
        )
    finally:
        _delete_topics([in_topic, out_topic])


def test_no_record_the_fixture_marks_invalid_reaches_track_activity(forwarding_run):
    """ROADMAP SC1: all 9 invalid records logged and dropped, none forwarded.

    The 8 / 1 split between `invalid_value` and `key_mismatch` is asserted, not
    just the total. It matters: a consumer that rejected the key-mismatch record
    as a *model* failure would be dropping the right record for the wrong reason,
    `key_matches_listener_id` would be dead code, and the one contract rule the
    shared model structurally cannot enforce would be unenforced. The record in
    that case is entirely valid JSON -- only the Kafka key is wrong.
    """
    summary = forwarding_run.run.summary
    envelopes = _invalid_envelopes()

    forwarded_ids = {
        json.loads(record.value)["event_id"] for record in forwarding_run.records
    }
    leaked = sorted(set(forwarding_run.invalid_event_ids) & forwarded_ids)
    assert not leaked, (
        f"invalid record(s) reached the output topic: {leaked} -- contract "
        f"section 6 requires them logged and dropped, never forwarded"
    )

    key_mismatch_cases = [
        envelope
        for envelope in envelopes
        if envelope.get("kafka_key") is not None
        and envelope["kafka_key"] != envelope["record"].get("listener_id")
    ]
    expected_key_mismatch = len(key_mismatch_cases)
    expected_invalid_value = len(envelopes) - expected_key_mismatch

    assert summary["key_mismatch"] == expected_key_mismatch, (
        f"expected {expected_key_mismatch} key-vs-value rejection(s), got "
        f"{summary['key_mismatch']}; summary={summary}"
    )
    assert summary["invalid_value"] == expected_invalid_value, (
        f"expected {expected_invalid_value} model rejection(s), got "
        f"{summary['invalid_value']}; summary={summary}"
    )

    stderr = forwarding_run.run.stderr
    drop_lines = [
        line for line in stderr.splitlines() if "dropped record:" in line
    ]
    assert len(drop_lines) == len(envelopes), (
        f"expected one log line per drop ({len(envelopes)}), found "
        f"{len(drop_lines)}:\n" + "\n".join(drop_lines)
    )
    assert (
        sum(1 for line in drop_lines if "reason=key_mismatch" in line)
        == expected_key_mismatch
    )
    assert (
        sum(1 for line in drop_lines if "reason=invalid_value" in line)
        == expected_invalid_value
    )
    # The key-mismatch case is the only one whose record parsed, so it is the
    # only one whose log line can name an event_id. That it does is the evidence
    # a reviewer needs to tell the two rejection paths apart in a real log.
    for envelope in key_mismatch_cases:
        event_id = envelope["record"]["event_id"]
        assert any(
            "reason=key_mismatch" in line and event_id in line
            for line in drop_lines
        ), f"no key_mismatch log line names event_id {event_id!r}"


def test_every_valid_record_is_forwarded_rekeyed_and_byte_identical(forwarding_run):
    """ROADMAP SC2: every valid record on `track-activity`, keyed and unchanged.

    Byte comparison against the lines on disk, not a model comparison. Contract
    section 4 requires the same `PlayEventV1` JSON that was received, and the
    shipped producer sends the raw line, so the correct output bytes are exactly
    the fixture's bytes. The multiset equality is what makes this a bijection
    claim rather than a subset one -- it fails both on a dropped record and on a
    duplicated one.
    """
    oracle = _oracle()
    fixture_lines = _fixture_lines()
    records = forwarding_run.records

    assert len(records) == oracle["total_events"], (
        f"expected {oracle['total_events']} records on the output topic, got "
        f"{len(records)}"
    )
    assert forwarding_run.run.summary["forwarded"] == oracle["total_events"]

    for record in records:
        event = json.loads(record.value)
        assert record.key is not None, (
            f"record at offset {record.offset} arrived unkeyed; the output key is "
            f"the track_id"
        )
        assert record.key.decode("utf-8") == event["track_id"], (
            f"output key {record.key!r} != the track_id inside its own value "
            f"{event['track_id']!r} (partition {record.partition}, offset "
            f"{record.offset})"
        )

    assert sorted(record.value for record in records) == sorted(fixture_lines), (
        "the multiset of forwarded value bytes does not equal the multiset of "
        "fixture lines -- the payload was reformatted, re-serialized, dropped or "
        "duplicated somewhere between the two topics"
    )


def test_the_output_key_is_the_track_id_and_no_longer_the_listener_id(forwarding_run):
    """The re-key is the point of the project (CD-3), so it is asserted directly.

    Without this, a consumer that forwarded every record under the key it
    received would still satisfy a byte-identity test on the value. At least one
    record's outgoing key must differ from its value's `listener_id`, which is
    the observable difference between a repartition and a pointless extra hop.
    """
    rekeyed = [
        record
        for record in forwarding_run.records
        if record.key.decode("utf-8") != json.loads(record.value)["listener_id"]
    ]
    assert rekeyed, (
        "every record on the output topic still carries its listener_id as the "
        "key -- the repartition did not happen"
    )
    # And no record kept the incoming key: the re-key is total, not partial.
    assert len(rekeyed) == len(forwarding_run.records)


def test_the_review_output_from_a_real_run_matches_the_oracle(forwarding_run):
    """ROADMAP SC3: a real run flags FA01 and no other listener, including FN01.

    FN01 sits at exactly the fixture threshold. CD-4 says *more than*, strictly,
    so it must stay unflagged -- flipping `>` to `>=` in the consumer fails this
    test. `late_dropped` is asserted at 0 because per-key watermarks absorb
    three-partition interleaving: a non-zero value here would mean genuine
    per-key `event_time` disorder, and the affected counts would not be
    trustworthy.
    """
    oracle = _oracle()
    review = forwarding_run.review

    flagged = [entry["listener_id"] for entry in review["flagged_listeners"]]
    assert flagged == oracle["expected_flagged_listeners"], (
        f"review flagged {flagged}, oracle expects "
        f"{oracle['expected_flagged_listeners']}"
    )

    boundary = oracle["boundaries"]["topology_a_strict_greater_than"]
    assert boundary["listener_id"] not in flagged, (
        f"{boundary['listener_id']} sits at exactly the threshold "
        f"({boundary['plays_in_window']} vs {boundary['threshold']}) and must not "
        f"be flagged: {boundary['why']}"
    )
    for listener_id in oracle["expected_unflagged_listeners"]:
        assert listener_id not in flagged

    entry = next(
        entry
        for entry in review["flagged_listeners"]
        if entry["listener_id"] == oracle["topology_a"]["listener_id"]
    )
    assert entry["peak_plays_in_window"] == oracle["topology_a"]["plays_in_window"]
    assert entry["window_hours"] == oracle["topology_a"]["window_hours"]

    assert review["counts"]["late_dropped"] == 0, (
        f"{review['counts']['late_dropped']} event(s) were dropped as late; with "
        f"per-key watermarks that means genuine per-key event_time disorder, not "
        f"multi-partition interleaving"
    )


# =========================================================================
# Task 2 -- the idempotence proofs
# =========================================================================
def _event_ids(records: Sequence[_Record]) -> List[str]:
    return [json.loads(record.value)["event_id"] for record in records]


def _fixture_event_ids() -> set:
    return {json.loads(line)["event_id"] for line in _fixture_lines()}


def test_replaying_the_same_input_twice_changes_neither_the_output_nor_the_flags(
    topics, tmp_path, forwarding_run
):
    """ROADMAP SC4: 126 records carrying 63 distinct event_ids, 63 records out.

    `event_id` dedup absorbs the second pass, so the output topic and the flagged
    list are indistinguishable from a single replay -- which is the whole reason
    contract section 5 requires dedup rather than suggesting it.

    WHAT IS COMPARED, AND WHAT DELIBERATELY IS NOT. `flagged_listeners` and the
    content of the output topic are compared against the single-replay run,
    because those are what SC4 claims. The enclosing review document is NOT
    compared: `counts.polled` is 126 here and 72 there, and
    `counts.duplicate_event_id` is 63 here and 0 there. Both differences are
    correct -- they describe the run, not the finding. A byte comparison of the
    whole document would fail for a reason SC4 never claimed, so Phase 5's
    PROF-02 must draw this same line.
    """
    in_topic, out_topic = topics
    oracle = _oracle()
    total = oracle["total_events"]

    _replay_fixture(in_topic)
    _replay_fixture(in_topic)

    review_out = tmp_path / "review-double.json"
    run = _run_consumer(
        in_topic=in_topic,
        out_topic=out_topic,
        state_dir=tmp_path / "state-double",
        review_out=review_out,
    )

    assert run.summary["polled"] == 2 * total, (
        f"expected both replays to be polled ({2 * total}), got "
        f"{run.summary['polled']}"
    )
    assert run.summary["forwarded"] == total
    assert run.summary["duplicate_event_id"] == total, (
        f"expected the second replay's {total} records to be dropped as duplicate "
        f"event_ids, got {run.summary['duplicate_event_id']}"
    )

    records = _drain(out_topic, expected_at_least=total)
    assert len(records) == total, (
        f"a double replay put {len(records)} records on the output topic; dedup "
        f"should have held it to {total}"
    )
    assert sorted(record.value for record in records) == sorted(_fixture_lines())

    review = json.loads(review_out.read_text(encoding="utf-8"))
    assert json.dumps(review["flagged_listeners"], sort_keys=True) == json.dumps(
        forwarding_run.review["flagged_listeners"], sort_keys=True
    ), (
        "the flagged-listener list differs between a single and a double replay, "
        "so dedup let the second pass reach the per-listener counts"
    )
    assert review["counts"]["late_dropped"] == 0


def test_a_second_consumer_run_over_the_same_input_forwards_nothing_new(
    topics, tmp_path
):
    """The replay-identity property PROF-02 will lean on in Phase 5.

    Rerunning the consumer on the same group and the same state directory, after
    the first run consumed everything, forwards nothing: the offsets are
    committed, so there is nothing to poll, and the journal restores the dedup
    set so a redelivery would be absorbed even if there were. Observed once here
    so Phase 5 inherits the property rather than discovering it.
    """
    in_topic, out_topic = topics
    total = _oracle()["total_events"]
    group = f"e2e-rerun-{uuid.uuid4()}"
    state_dir = tmp_path / "state-rerun"

    _replay_fixture(in_topic)

    first = _run_consumer(
        in_topic=in_topic,
        out_topic=out_topic,
        state_dir=state_dir,
        review_out=tmp_path / "review-first.json",
        group=group,
    )
    assert first.summary["forwarded"] == total
    after_first = _drain(out_topic, expected_at_least=total)
    assert len(after_first) == total

    # The first run exited cleanly, so it left the group; this returns at once.
    # It is the same guard the SIGKILL test needs, and the contrast is the point.
    _wait_for_group_to_release_its_members(group)

    second = _run_consumer(
        in_topic=in_topic,
        out_topic=out_topic,
        state_dir=state_dir,
        review_out=tmp_path / "review-second.json",
        group=group,
    )
    assert second.summary["polled"] == 0, (
        f"the second run polled {second.summary['polled']} record(s); every "
        f"offset was committed by the first run, so there was nothing to poll"
    )
    assert second.summary["forwarded"] == 0
    assert second.summary["recovered_from_journal"] == total, (
        f"the second run rebuilt {second.summary['recovered_from_journal']} "
        f"event(s) from the journal, expected {total}"
    )

    after_second = _drain(out_topic, expected_at_least=total)
    assert len(after_second) == len(after_first), (
        f"the second run added {len(after_second) - len(after_first)} record(s) "
        f"to the output topic"
    )
    assert sorted(r.value for r in after_second) == sorted(
        r.value for r in after_first
    )


# =========================================================================
# Task 2 -- the crash proof
# =========================================================================
@dataclass(frozen=True)
class _KilledAndRestarted:
    """One SIGKILL mid-run and one restart on the same group and state directory."""

    partial: List[_Record]
    final: List[_Record]
    restart: _ConsumerRun
    restart_review: Dict[str, Any]
    restart_stderr: str
    state_dir: Path
    eviction_wait_seconds: float


@pytest.fixture(scope="module")
def killed_and_restarted(tmp_path_factory):
    """SIGKILL a real run at `--commit-every 1`, then restart it on the same state.

    Shared by the two tests below rather than performed twice: the SIGKILL costs
    the group coordinator's session-expiry wait (~45s) and both tests are
    assertions about the same single kill.
    """
    in_topic, out_topic = _throwaway_topics()
    try:
        workdir = tmp_path_factory.mktemp("sigkill")
        state_dir = workdir / "state"
        group = f"e2e-sigkill-{uuid.uuid4()}"

        _replay_fixture(in_topic)

        proc, _out_path, _err_path = _start_consumer(
            in_topic=in_topic,
            out_topic=out_topic,
            state_dir=state_dir,
            review_out=workdir / "review-killed.json",
            group=group,
            log_dir=workdir / "logs",
            commit_every=1,
            idle_timeout=6.0,
            throttle=KILL_TEST_THROTTLE_SECONDS,
        )
        # The kill point is wall clock against `--throttle`, not parsed progress
        # output. SIGKILL, not SIGTERM: nothing in the process gets a chance to
        # flush, settle or commit on the way out, which is the only way to
        # exercise the window the journal exists to cover.
        time.sleep(KILL_TEST_RUN_SECONDS)
        proc.kill()
        proc.wait(timeout=30)

        partial = _drain(out_topic)
        eviction_wait = _wait_for_group_to_release_its_members(group)

        restart_review_path = workdir / "review-restart.json"
        restart_proc, restart_out, restart_err = _start_consumer(
            in_topic=in_topic,
            out_topic=out_topic,
            state_dir=state_dir,
            review_out=restart_review_path,
            group=group,
            log_dir=workdir / "logs",
            commit_every=1,
            idle_timeout=12.0,
            throttle=0.0,
        )
        returncode = restart_proc.wait(timeout=SUBPROCESS_TIMEOUT_SECONDS)
        stdout_text = restart_out.read_text(encoding="utf-8", errors="replace")
        stderr_text = restart_err.read_text(encoding="utf-8", errors="replace")
        assert returncode == 0, (
            f"the restarted consumer exited {returncode}\n"
            f"stdout:\n{stdout_text}\nstderr:\n{stderr_text}"
        )

        restart = _ConsumerRun(
            summary=_parse_summary(stdout_text),
            stdout=stdout_text,
            stderr=stderr_text,
        )
        final = _drain(out_topic, expected_at_least=_oracle()["total_events"])

        yield _KilledAndRestarted(
            partial=partial,
            final=final,
            restart=restart,
            restart_review=json.loads(
                restart_review_path.read_text(encoding="utf-8")
            ),
            restart_stderr=stderr_text,
            state_dir=state_dir,
            eviction_wait_seconds=eviction_wait,
        )
    finally:
        _delete_topics([in_topic, out_topic])


def test_a_sigkill_mid_run_followed_by_a_restart_loses_no_event(killed_and_restarted):
    """ROADMAP SC5: nothing lost across a SIGKILL, and at most one duplicate.

    TWO ASSERTIONS POINTING IN OPPOSITE DIRECTIONS, AND NEITHER REPLACES THE
    OTHER. Do not delete one believing the other covers it (threat T-03-11).

    * The set of `event_id`s on the output topic equals the full fixture set.
      THIS IS THE ASSERTION THAT CATCHES AN OFFSET COMMITTED BEFORE ITS PRODUCE
      WAS CONFIRMED. That bug *loses* a record: the offset moves past a record
      that never reached `track-activity`, the restart never sees it again, and
      the only trace is a missing `event_id`. It shows up here and nowhere else
      -- and note that it would sail *under* the duplicate bound below, because
      losing records makes the output smaller, not larger.

    * `len(records) - len(distinct event_ids)` is at most 1. At
      `--commit-every 1` the only SIGKILL window that can duplicate is between
      the delivery callback confirming the produce and the journal line being
      flushed, which is exactly one record wide. Anything above 1 means the
      settle ordering batched more than it claimed to.

    THE THIRD LEG IS RECOVERY, AND ITS STRENGTH IS UNEVEN -- WHICH IS RECORDED
    HERE RATHER THAN GLOSSED, SO NOBODY LATER READS MORE INTO A PASS THAN IT
    EARNS.

    * `recovered_from_journal > 0` and
      `recovered_from_journal + forwarded == total_events` hold whatever the kill
      point: the journal's events are neither recounted nor redelivered, because
      their offsets are committed. Recovery that does not run at all fails these.
    * `listeners_seen` equal to every distinct listener in the fixture is the
      broad rebuild check. The restart can only have met all 32 by replaying the
      journal, since the post-kill records alone do not carry them all.
    * The flagged list and FA01's recovered play count are checked, but are
      deliberately NOT described as the recovery check. FA01's 12 events all live
      in one partition, and how many of them the killed run had reached depends
      on the order librdkafka served the three partitions in -- measured, one
      probe killed at this same point had reached zero of the twelve, and a
      restart would flag FA01 from the post-kill records alone. It is a real
      assertion about the restarted run's output; it is not a reliable detector
      of a recovery that counts entries without rebuilding from them.
    """
    oracle = _oracle()
    total = oracle["total_events"]

    # T-03-10: prove the kill actually interrupted something before trusting
    # anything the restart says. A kill that landed after the run finished would
    # otherwise pass this whole test while exercising nothing.
    partial_count = len(killed_and_restarted.partial)
    assert 0 < partial_count < total, (
        f"the killed run put {partial_count} of {total} records on the output "
        f"topic; the kill has to land strictly inside the run for this test to "
        f"mean anything ({KILL_TEST_RUN_SECONDS}s against a throttle of "
        f"{KILL_TEST_THROTTLE_SECONDS}s/record)"
    )

    final = killed_and_restarted.final
    ids = _event_ids(final)
    distinct = set(ids)

    missing = sorted(_fixture_event_ids() - distinct)
    assert not missing, (
        f"{len(missing)} event(s) never reached the output topic across the "
        f"kill: {missing[:10]}{'...' if len(missing) > 10 else ''} -- an input "
        f"offset was committed before its produce was confirmed"
    )
    assert distinct == _fixture_event_ids()

    duplicates = len(ids) - len(distinct)
    assert duplicates <= 1, (
        f"{duplicates} duplicate record(s) on the output topic; at "
        f"--commit-every 1 the only window that can duplicate is between the "
        f"delivery confirmation and the journal flush, which is one record wide"
    )

    summary = killed_and_restarted.restart.summary
    assert summary["recovered_from_journal"] > 0, (
        "the restart recovered nothing from the journal, so this run proves "
        "nothing about recovery"
    )
    assert summary["recovered_from_journal"] + summary["forwarded"] == total, (
        f"the restart accounted for "
        f"{summary['recovered_from_journal']} + {summary['forwarded']} = "
        f"{summary['recovered_from_journal'] + summary['forwarded']} of {total} "
        f"events; the journal's events are neither recounted nor redelivered, so "
        f"a short sum means the restart began a second, emptier history instead "
        f"of continuing the killed run's"
    )
    every_listener = set(oracle["expected_flagged_listeners"]) | set(
        oracle["expected_unflagged_listeners"]
    )
    assert summary["listeners_seen"] == len(every_listener), (
        f"the restarted run holds state for {summary['listeners_seen']} "
        f"listener(s), expected all {len(every_listener)} in the fixture; the "
        f"post-kill records alone do not carry them all, so a shortfall means the "
        f"journal was counted without being replayed into the per-listener state"
    )
    flagged = [
        entry["listener_id"]
        for entry in killed_and_restarted.restart_review["flagged_listeners"]
    ]
    assert flagged == oracle["expected_flagged_listeners"], (
        f"the restarted run flagged {flagged}, oracle expects "
        f"{oracle['expected_flagged_listeners']} -- the per-listener counts the "
        f"killed process had accumulated were not rebuilt from the journal"
    )
    entry = next(
        candidate
        for candidate in killed_and_restarted.restart_review["flagged_listeners"]
        if candidate["listener_id"] == oracle["topology_a"]["listener_id"]
    )
    assert entry["peak_plays_in_window"] == oracle["topology_a"]["plays_in_window"]
    assert entry["plays_recorded"] == oracle["topology_a"]["plays_in_window"], (
        "the recovered per-listener play count does not match the oracle, so the "
        "journal replay counted a record twice or missed one"
    )


def test_the_journal_left_by_the_killed_run_is_the_only_state_the_restart_needed(
    killed_and_restarted,
):
    """The state directory holds one file, and the restart says what it read from it.

    The journal is the only thing that survives a SIGKILL, so the restart's
    correctness rests entirely on it. Asserting the directory's *exact* contents
    is the part that keeps that claim true later: a second state file added for
    convenience would make recovery depend on something the kill might not have
    left behind, and nothing else in the suite would notice.

    The recovered count is bounded against what the killed run actually put on
    the output topic. It can be one short and never more: the journal line is
    written after the delivery is confirmed, so a kill in that gap leaves the
    record on the topic with no journal entry -- which is the same one-record
    window the duplicate bound describes, seen from the other side.
    """
    state_dir = killed_and_restarted.state_dir
    assert state_dir.is_dir()
    contents = sorted(path.name for path in state_dir.iterdir())
    assert contents == [JOURNAL_FILENAME], (
        f"the state directory holds {contents}; the journal is supposed to be "
        f"the only persisted state"
    )

    journal_lines = [
        line
        for line in (state_dir / JOURNAL_FILENAME).read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    ]
    assert journal_lines, "the killed run left no journal at all"

    match = re.search(
        r"recovered (\d+) event\(s\) from", killed_and_restarted.restart_stderr
    )
    assert match is not None, (
        "the restarted run's stderr never says how many journal entries it "
        f"recovered:\n{killed_and_restarted.restart_stderr[-2000:]}"
    )
    recovered = int(match.group(1))
    assert recovered == killed_and_restarted.restart.summary[
        "recovered_from_journal"
    ], (
        f"the log says {recovered} recovered but the machine-readable summary "
        f"says {killed_and_restarted.restart.summary['recovered_from_journal']}"
    )

    partial_count = len(killed_and_restarted.partial)
    assert partial_count - 1 <= recovered <= partial_count, (
        f"the restart recovered {recovered} entry/entries but the killed run had "
        f"already put {partial_count} record(s) on the output topic; the journal "
        f"may be exactly one short of that and never more"
    )
