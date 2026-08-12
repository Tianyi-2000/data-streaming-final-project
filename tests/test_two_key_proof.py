"""The project's central claim, asserted through the SHIPPED pipeline.

Neither partition key alone catches both fraud topologies. Consumer 1 is keyed by
`listener_id` and finds Topology A; Consumer 2 is keyed by `track_id` and finds
Topology B; NEITHER finds the other's. Everything built in Phases 1-4 is the
apparatus for that sentence. This file is where it stops being a claim in a
document and becomes an assertion that runs.

Five deliberate choices, each of which a later reader would otherwise undo.

1. THIS FILE DRIVES THE SHIPPED PIPELINE AND NEVER RE-IMPLEMENTS A RULE.
   `src/replay_to_kafka.py`, `src/consumer_stage1.py` and `src/consumer_stage2.py`
   run as subprocesses against a real broker, and every assertion below reads a
   review document one of those processes actually wrote.
   `tests/test_fixture_trips_rules.py` already asserts separability at fixture
   scale, but it does so against an INDEPENDENT REIMPLEMENTATION of both rules.
   That proves that file self-consistent; it cannot prove the shipped consumers
   correct, because the model and the code could drift in the same direction and
   every assertion there would stay green. This file exists to close exactly that
   gap, and is what supersedes it for separability.

2. EVERY RUN CREATES ITS OWN TOPICS AND ITS OWN TMP STATE DIRECTORY. Nothing here
   reads or writes the contract's real `play-events` / `track-activity`. That is
   not only the usual isolation argument: the full-scale artifacts this file also
   reads (`output/*.json`) are the products of a run on those very topics, so a
   test that reset the broker would destroy half of its own evidence.

3. A FRESH `--state-dir` PER RUN IS WHAT MAKES A SECOND REPLAY A REAL REPLAY.
   Consumer 1's journal dedups on `event_id` ACROSS runs. A second replay pointed
   at the same state directory forwards nothing, and the PROF-02 comparison would
   then be a real run against an empty one -- which compares equal to nothing and
   passes while proving the opposite of what it claims. Each run therefore gets
   its own `tmp_path` state directory, its own uuid-suffixed topic pair and its
   own consumer groups.

4. NO COUNT, LISTENER ID, TRACK ID OR THRESHOLD IS TYPED INTO AN ASSERTION.
   Fixture-scale values are read from `tests/fixtures/expected_flags.json`;
   full-scale cohort membership is DERIVED by scanning `data/play_events.jsonl`
   and cross-checked against `data/play_events_manifest.json`. A proof whose
   expected values can be edited to match observed behaviour is not a proof.

5. EVERY ABSENCE ASSERTION CARRIES A NON-VACUITY GUARD. "No Topology B listener
   was flagged" is worthless if no Topology B listener was processed. Each
   cross-condition asserts alongside it that the cohort was really seen.

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
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Optional, Sequence, Set, Tuple

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

REPO_ROOT = Path(__file__).resolve().parents[1]
BROKER = "localhost:9092"

REPLAY_SCRIPT = REPO_ROOT / "src" / "replay_to_kafka.py"
STAGE1_SCRIPT = REPO_ROOT / "src" / "consumer_stage1.py"
STAGE2_SCRIPT = REPO_ROOT / "src" / "consumer_stage2.py"

FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "play_events_fixture.jsonl"
ORACLE_PATH = REPO_ROOT / "tests" / "fixtures" / "expected_flags.json"

ORACLE: Dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))
# The oracle names its own threshold file, so the config under test cannot drift
# away from the numbers the oracle was built against without this path breaking.
FIXTURE_THRESHOLDS_PATH = REPO_ROOT / ORACLE["thresholds_path"]

# The same width `src/create_topics.py` uses for the contract's own topics.
PARTITIONS = 3

DRAIN_TIMEOUT_SECONDS = 30.0
SUBPROCESS_TIMEOUT_SECONDS = 240.0
TOPIC_READY_TIMEOUT_SECONDS = 30.0
DRAIN_IDLE_POLLS = 3
IDLE_TIMEOUT_SECONDS = 6.0


# --- fixture facts, read rather than typed --------------------------------
@lru_cache(maxsize=1)
def _fixture_records() -> Tuple[Dict[str, Any], ...]:
    """Every fixture line as a parsed dict, cached for the module's lifetime."""
    return tuple(
        json.loads(line)
        for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    )


def _oracle_listener_roll() -> Set[str]:
    """Every listener the oracle accounts for, flagged and unflagged together.

    This is the non-vacuity yardstick for Consumer 1: if the consumer saw fewer
    listeners than the oracle rolls, some cohort never reached the rule and its
    "not flagged" is an artefact of absence rather than a detection result.
    """
    return set(ORACLE["expected_flagged_listeners"]) | set(
        ORACLE["expected_unflagged_listeners"]
    )


# --- topics ---------------------------------------------------------------
def _admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": BROKER})


def _throwaway_topics(label: str) -> Tuple[str, str]:
    """A fresh 3-partition input/output pair, waited for in cluster metadata.

    The uuid suffix is what keeps this file from ever touching the contract's own
    topics -- and therefore what keeps it from destroying the full-scale
    artifacts its own full-scale section reads.
    """
    suffix = f"{label}-{uuid.uuid4().hex[:12]}"
    in_topic = f"proof-play-events-{suffix}"
    out_topic = f"proof-track-activity-{suffix}"

    admin = _admin()
    futures = admin.create_topics(
        [
            NewTopic(name, num_partitions=PARTITIONS, replication_factor=1)
            for name in (in_topic, out_topic)
        ]
    )
    for future in futures.values():
        future.result(timeout=TOPIC_READY_TIMEOUT_SECONDS)

    deadline = time.monotonic() + TOPIC_READY_TIMEOUT_SECONDS
    while True:
        metadata = admin.list_topics(timeout=10)
        ready = [
            name
            for name in (in_topic, out_topic)
            if name in metadata.topics
            and len(metadata.topics[name].partitions) == PARTITIONS
        ]
        if len(ready) == 2:
            break
        assert time.monotonic() < deadline, (
            f"topics {in_topic!r}/{out_topic!r} did not reach {PARTITIONS} "
            f"partitions in cluster metadata within "
            f"{TOPIC_READY_TIMEOUT_SECONDS:.0f}s -- is `docker compose up -d` "
            "running?"
        )
        time.sleep(0.5)

    return in_topic, out_topic


def _delete_topics(names: Sequence[str]) -> None:
    """Best effort. A cleanup failure must never mask a real assertion."""
    try:
        for future in _admin().delete_topics(list(names)).values():
            try:
                future.result(timeout=TOPIC_READY_TIMEOUT_SECONDS)
            except Exception:  # noqa: BLE001 -- cleanup only
                pass
    except Exception:  # noqa: BLE001 -- cleanup only
        pass


# --- reading a topic back -------------------------------------------------
@dataclass(frozen=True)
class _Record:
    """One record read off a topic, with its bytes pulled out eagerly."""

    key: Optional[bytes]
    value: Optional[bytes]
    partition: int
    offset: int


def _drain(
    topic: str,
    expected_at_least: int = 0,
    timeout: float = DRAIN_TIMEOUT_SECONDS,
) -> List[_Record]:
    """Every record on `topic`, from a throwaway earliest group.

    Stops once `expected_at_least` records are in hand AND several consecutive
    polls came back empty, so an exact-count assertion is made against a topic
    that has genuinely stopped producing rather than one read too early.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": f"proof-drain-{uuid.uuid4()}",
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


# --- driving the shipped scripts ------------------------------------------
def _summary_number(stdout: str, label: str) -> int:
    """One labelled count out of the replay producer's summary line.

    Keyed off the label rather than the whole sentence, so a wording change in
    the producer cannot fail this file for the wrong reason.
    """
    match = re.search(rf"{label}\s+(\d+)", stdout, flags=re.IGNORECASE)
    assert match is not None, (
        f"could not find a '{label} <n>' count in replay output:\n{stdout}"
    )
    return int(match.group(1))


def _parse_summary(stdout: str) -> Dict[str, Any]:
    """The last `SUMMARY {json}` line on a consumer's stdout."""
    lines = [line for line in stdout.splitlines() if line.startswith("SUMMARY ")]
    assert lines, f"no 'SUMMARY <json>' line on consumer stdout:\n{stdout}"
    return json.loads(lines[-1][len("SUMMARY "):])


def _run(argv: List[str], what: str) -> subprocess.CompletedProcess:
    """Run one shipped script to completion, surfacing both streams on failure."""
    proc = subprocess.run(
        argv,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
    )
    assert proc.returncode == 0, (
        f"{what} exited {proc.returncode}\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    return proc


@dataclass(frozen=True)
class PipelineRun:
    """One complete end-to-end replay: producer -> Consumer 1 -> Consumer 2.

    Holds the topics and state directory as well as the outputs, because PROF-02
    must be able to prove the two runs were genuinely independent before it
    compares them.
    """

    in_topic: str
    out_topic: str
    state_dir: Path
    listener_review: Dict[str, Any]
    track_review: Dict[str, Any]
    stage1_summary: Dict[str, Any]
    stage2_summary: Dict[str, Any]
    track_activity_event_ids: FrozenSet[str]


def _run_pipeline(tmp_path: Path, label: str) -> PipelineRun:
    """The 63-event fixture through the SHIPPED pipeline, end to end.

    Throwaway topics, a fresh state directory and fresh consumer groups, so this
    is a complete independent replay and not a continuation of anything.
    """
    in_topic, out_topic = _throwaway_topics(label)
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    listener_review_path = tmp_path / "listener_review_queue.json"
    track_review_path = tmp_path / "track_review_queue.json"

    try:
        # --- the shipped producer, keyed by listener_id ------------------
        replay = _run(
            [
                sys.executable,
                str(REPLAY_SCRIPT),
                "--input",
                str(FIXTURE_PATH),
                "--topic",
                in_topic,
                "--broker",
                BROKER,
            ],
            "replay_to_kafka.py",
        )
        total = len(_fixture_records())
        assert _summary_number(replay.stdout, "queued") == total
        assert _summary_number(replay.stdout, "rejected") == 0
        assert _summary_number(replay.stdout, "delivery failures") == 0

        # --- Consumer 1: listener_id key, Topology A rule ----------------
        stage1 = _run(
            [
                sys.executable,
                str(STAGE1_SCRIPT),
                "--broker",
                BROKER,
                "--group",
                f"proof-c1-{label}-{uuid.uuid4()}",
                "--in-topic",
                in_topic,
                "--out-topic",
                out_topic,
                "--state-dir",
                str(state_dir),
                "--review-out",
                str(listener_review_path),
                "--thresholds",
                str(FIXTURE_THRESHOLDS_PATH),
                "--commit-every",
                "1",
                "--idle-timeout",
                str(IDLE_TIMEOUT_SECONDS),
            ],
            "consumer_stage1.py",
        )

        # --- Consumer 2: track_id key, Topology B rule -------------------
        stage2 = _run(
            [
                sys.executable,
                str(STAGE2_SCRIPT),
                "--broker",
                BROKER,
                "--group",
                f"proof-c2-{label}-{uuid.uuid4()}",
                "--in-topic",
                out_topic,
                "--review-out",
                str(track_review_path),
                "--listener-review",
                str(listener_review_path),
                "--thresholds",
                str(FIXTURE_THRESHOLDS_PATH),
                "--commit-every",
                "1",
                "--idle-timeout",
                str(IDLE_TIMEOUT_SECONDS),
            ],
            "consumer_stage2.py",
        )

        drained = _drain(out_topic, expected_at_least=total)
        event_ids = frozenset(
            json.loads(record.value.decode("utf-8"))["event_id"]
            for record in drained
            if record.value is not None
        )

        assert listener_review_path.is_file(), "Consumer 1 wrote no review queue"
        assert track_review_path.is_file(), "Consumer 2 wrote no review queue"

        return PipelineRun(
            in_topic=in_topic,
            out_topic=out_topic,
            state_dir=state_dir,
            listener_review=json.loads(
                listener_review_path.read_text(encoding="utf-8")
            ),
            track_review=json.loads(track_review_path.read_text(encoding="utf-8")),
            stage1_summary=_parse_summary(stage1.stdout),
            stage2_summary=_parse_summary(stage2.stdout),
            track_activity_event_ids=event_ids,
        )
    finally:
        _delete_topics((in_topic, out_topic))


@pytest.fixture(scope="module")
def run_a(tmp_path_factory):
    """The first end-to-end replay. Module-scoped: this is the expensive object."""
    yield _run_pipeline(tmp_path_factory.mktemp("run-a"), "a")


# --- helpers over a run's documents ---------------------------------------
def _flagged_listener_ids(run: PipelineRun) -> Set[str]:
    return {
        entry["listener_id"] for entry in run.listener_review["flagged_listeners"]
    }


def _flagged_track_ids(run: PipelineRun) -> Set[str]:
    return {entry["track_id"] for entry in run.track_review["flagged_tracks"]}


# =========================================================================
# PROF-01 at fixture scale, through the real consumers
# =========================================================================
def test_both_cohorts_were_really_processed_under_the_declared_thresholds(
    run_a: PipelineRun,
):
    """The non-vacuity guard the two cross-conditions below rest on (PROF-01).

    Every "did not flag" assertion in this file is an absence claim, and an
    absence claim is satisfied for free by an input that never arrived. This test
    is what makes those claims mean something:

    - both review documents name `config/thresholds.fixture.json` as the config
      actually loaded, so the rules ran against the numbers the oracle was built
      against and not some other file;
    - Consumer 1 saw every listener the oracle rolls, so the Topology B cohort
      reached the listener-keyed rule and was NOT flagged, rather than never
      having been offered to it;
    - Consumer 2 counted all 63 fixture events, so Topology A's plays were
      aggregated by the track-keyed stage and simply did not trip Rule B.
    """
    for name, document in (
        ("Consumer 1", run_a.listener_review),
        ("Consumer 2", run_a.track_review),
    ):
        source = Path(document["thresholds_source_path"]).resolve()
        assert source == FIXTURE_THRESHOLDS_PATH.resolve(), (
            f"{name} loaded {source}, not the oracle's own "
            f"{FIXTURE_THRESHOLDS_PATH}"
        )

    roll = _oracle_listener_roll()
    assert run_a.listener_review["counts"]["listeners_seen"] == len(roll), (
        "Consumer 1 did not see every listener the oracle accounts for; the "
        "'no Topology B listener was flagged' assertion below would then be "
        "satisfied by absence rather than by detection"
    )
    assert run_a.track_review["counts"]["counted"] == len(_fixture_records()), (
        "Consumer 2 did not count every fixture event; the 'no Topology A track "
        "was flagged' assertion below would then be satisfied by absence"
    )


def test_the_listener_key_finds_topology_a(run_a: PipelineRun):
    """Rule A, run by the shipped Consumer 1, flags the oracle's listeners.

    The positive half of PROF-01: the listener-keyed rule does its own job. The
    expected set is read from `expected_flags.json`, never typed.
    """
    assert _flagged_listener_ids(run_a) == set(ORACLE["expected_flagged_listeners"])


def test_the_listener_key_does_not_catch_topology_b(run_a: PipelineRun):
    """Rule A flags NO Topology B listener (PROF-01, CD-9).

    Topology B is a burst of many listeners each playing one track once. The
    listener key sees a crowd of single-play listeners and has nothing to
    accumulate, so the rule keyed by `listener_id` cannot see this topology at
    all. That is the first half of "neither key alone catches both".

    The cohort is derived from the fixture by listener-id prefix and asserted
    non-empty before the intersection, so the absence cannot pass vacuously.
    """
    topology_b_cohort = {
        record["listener_id"]
        for record in _fixture_records()
        if record["listener_id"].startswith("FB")
    }
    assert topology_b_cohort, "no Topology B cohort in the fixture to fail to flag"
    assert topology_b_cohort <= set(ORACLE["expected_unflagged_listeners"]), (
        "the derived Topology B cohort is not the one the oracle expects to go "
        "unflagged; the fixture and the oracle have drifted apart"
    )

    flagged = _flagged_listener_ids(run_a)
    assert not (flagged & topology_b_cohort), (
        "the listener-keyed rule flagged a Topology B listener: "
        f"{sorted(flagged & topology_b_cohort)}"
    )


def test_the_track_key_finds_topology_b(run_a: PipelineRun):
    """Rule B, run by the shipped Consumer 2, flags the oracle's tracks.

    The positive half of PROF-01 on the track side, including the boundary case
    the oracle pins exactly on both of its thresholds.
    """
    assert _flagged_track_ids(run_a) == set(ORACLE["expected_flagged_tracks"])


def test_the_track_key_does_not_catch_topology_a(run_a: PipelineRun):
    """Rule B flags NO track the Topology A listener played (PROF-01, CD-9).

    Topology A is one listener playing many DIFFERENT tracks. Its plays spread
    across the catalogue, so no single track-hour bucket accumulates enough
    unique listeners to trip the track-keyed rule. That is the second half of
    "neither key alone catches both".

    The track set is derived from the fixture by the oracle's own Topology A
    listener and cross-checked against the oracle's `distinct_tracks`, so a
    fixture that no longer holds the topology surfaces here.
    """
    topology_a_tracks = {
        record["track_id"]
        for record in _fixture_records()
        if record["listener_id"] == ORACLE["topology_a"]["listener_id"]
    }
    assert len(topology_a_tracks) == ORACLE["topology_a"]["distinct_tracks"], (
        "the Topology A listener no longer plays the number of distinct tracks "
        "the oracle records; the fixture and the oracle have drifted apart"
    )

    flagged = _flagged_track_ids(run_a)
    assert not (flagged & topology_a_tracks), (
        "the track-keyed rule flagged a track the Topology A listener played: "
        f"{sorted(flagged & topology_a_tracks)}"
    )
