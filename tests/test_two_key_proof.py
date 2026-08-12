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


# =========================================================================
# PROF-01 at full scale, from the artifacts already on disk
# =========================================================================
STREAM_PATH = REPO_ROOT / "data" / "play_events.jsonl"
MANIFEST_PATH = REPO_ROOT / "data" / "play_events_manifest.json"
PRODUCTION_THRESHOLDS_PATH = REPO_ROOT / ORACLE["production_thresholds_path"]
FULL_LISTENER_REVIEW = REPO_ROOT / "output" / "listener_review_queue.json"
FULL_TRACK_REVIEW = REPO_ROOT / "output" / "track_review_queue.json"

# Full-scale cohorts are declared by listener-id prefix in `src/generate_events.py`:
# `L` normal, `A` Topology A, `B` Topology B. The prefixes are the derivation rule,
# and the manifest's `counts_by_cohort` is what the derivation is checked against.
COHORT_PREFIXES = {"topology_a": "A", "topology_b": "B"}

REGENERATE_HINT = (
    "the full-scale artifacts under output/ are absent or of unknown provenance. "
    "output/ is gitignored, so this is the normal state of a fresh checkout and "
    "not a failure of the project's claim. Regenerate them with:\n"
    "    docker compose up -d\n"
    "    python3 src/replay_to_kafka.py\n"
    "    python3 src/consumer_stage1.py --thresholds config/thresholds.json\n"
    "    python3 src/consumer_stage2.py --thresholds config/thresholds.json"
)


@lru_cache(maxsize=1)
def _manifest() -> Dict[str, Any]:
    """The generator's own account of the committed stream."""
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def _full_scale_artifacts() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Both full-scale review documents, or a SKIP naming how to regenerate them.

    This SKIPS rather than fails, in three cases, and the distinction matters.
    `output/` is gitignored, so a fresh checkout legitimately has neither file; a
    hard failure here would report the absence of somebody's local run as a
    failure of the project's central claim, which is the one thing these tests
    exist to speak accurately about.

    The other two cases are provenance. A document whose `thresholds_source_path`
    is not `config/thresholds.json`, or whose `counts.records_seen` is not the
    manifest's `total_events`, was produced by a different run against different
    numbers. Such a file cannot prove the claim -- and it cannot disprove it
    either, so it must not fail.
    """
    if not (FULL_LISTENER_REVIEW.is_file() and FULL_TRACK_REVIEW.is_file()):
        pytest.skip(f"{REGENERATE_HINT}\n(reason: one or both files are absent)")

    listener = json.loads(FULL_LISTENER_REVIEW.read_text(encoding="utf-8"))
    track = json.loads(FULL_TRACK_REVIEW.read_text(encoding="utf-8"))
    expected_config = PRODUCTION_THRESHOLDS_PATH.resolve()
    total_events = _manifest()["total_events"]

    for name, document in (("listener", listener), ("track", track)):
        source = Path(document["thresholds_source_path"]).resolve()
        if source != expected_config:
            pytest.skip(
                f"{REGENERATE_HINT}\n(reason: the {name} queue names {source} as "
                f"its threshold config, not {expected_config})"
            )
        if document["counts"]["records_seen"] != total_events:
            pytest.skip(
                f"{REGENERATE_HINT}\n(reason: the {name} queue saw "
                f"{document['counts']['records_seen']} records, not the "
                f"manifest's {total_events})"
            )

    return listener, track


@lru_cache(maxsize=1)
def _full_scale_cohorts() -> Dict[str, FrozenSet[str]]:
    """One scan of the committed stream, cross-checked against the manifest.

    Returns the `A` and `B` listener sets and the distinct tracks each cohort
    played. Every full-scale expected value downstream is derived from this: no
    listener id, track id or count is typed into an assertion.

    The manifest cross-check is the guard that matters. A regenerated stream with
    different cohort sizes would otherwise quietly weaken every assertion below
    -- the cross-conditions would still "pass", against a cohort that no longer
    exists at the size the claim was made about.
    """
    if not STREAM_PATH.is_file():
        pytest.skip(
            f"{STREAM_PATH} is absent; regenerate it with "
            f"`python3 src/generate_events.py --seed {_manifest()['seed']}`"
        )

    listeners: Dict[str, Set[str]] = {"topology_a": set(), "topology_b": set()}
    tracks: Dict[str, Set[str]] = {"topology_a": set(), "topology_b": set()}
    event_counts: Dict[str, int] = {"normal": 0, "topology_a": 0, "topology_b": 0}

    with STREAM_PATH.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            record = json.loads(line)
            listener_id = record["listener_id"]
            for cohort, prefix in COHORT_PREFIXES.items():
                if listener_id.startswith(prefix):
                    listeners[cohort].add(listener_id)
                    tracks[cohort].add(record["track_id"])
                    event_counts[cohort] += 1
                    break
            else:
                event_counts["normal"] += 1

    manifest = _manifest()
    assert event_counts == manifest["counts_by_cohort"], (
        "the cohort event counts recomputed from data/play_events.jsonl do not "
        f"match the manifest: scanned {event_counts}, manifest declares "
        f"{manifest['counts_by_cohort']}"
    )
    assert sum(event_counts.values()) == manifest["total_events"]

    return {
        "topology_a_listeners": frozenset(listeners["topology_a"]),
        "topology_b_listeners": frozenset(listeners["topology_b"]),
        "topology_a_tracks": frozenset(tracks["topology_a"]),
        "topology_b_tracks": frozenset(tracks["topology_b"]),
    }


def _topology_b_target() -> str:
    """The single track the Topology B cohort attacks, derived not typed."""
    targets = _full_scale_cohorts()["topology_b_tracks"]
    assert len(targets) == 1, (
        "the Topology B cohort no longer plays exactly one track; the full-scale "
        f"proof below is written about a single target and found {len(targets)}"
    )
    return next(iter(targets))


def test_the_full_scale_stream_has_the_shape_the_proof_rests_on():
    """The two topologies are structurally opposite in the committed stream.

    Topology B is many listeners on ONE track; Topology A is few listeners across
    MANY tracks. Everything below is an intersection between those two sets, so
    if the stream does not actually have that shape the intersections would be
    empty for uninteresting reasons. Derived from the file, cross-checked against
    the manifest inside `_full_scale_cohorts()`.
    """
    cohorts = _full_scale_cohorts()
    target = _topology_b_target()

    assert cohorts["topology_b_listeners"], "no Topology B cohort in the stream"
    assert cohorts["topology_a_listeners"], "no Topology A cohort in the stream"
    assert len(cohorts["topology_a_tracks"]) > 1, (
        "the Topology A cohort's plays do not spread across the catalogue; the "
        "separability argument depends on exactly that spread"
    )
    assert target in cohorts["topology_a_tracks"], (
        "the Topology B target is not among the tracks the Topology A cohort "
        "played -- which would make the cross-condition below trivially true"
    )


def test_the_listener_key_does_not_catch_topology_b_at_full_scale():
    """Rule A flags every Topology A listener and NO Topology B one (PROF-01).

    45,473 real events. Every one of the 900 Topology B listeners has a single
    play, so the listener-keyed rule has nothing to accumulate for any of them.
    The flagged set is asserted to equal the derived `A` cohort EXACTLY, which is
    both the positive result and the non-vacuity guard: the queue is not empty,
    and it contains nothing but Topology A.
    """
    listener_review, _ = _full_scale_artifacts()
    cohorts = _full_scale_cohorts()

    flagged = {
        entry["listener_id"] for entry in listener_review["flagged_listeners"]
    }
    assert flagged == set(cohorts["topology_a_listeners"]), (
        "the listener queue is not exactly the Topology A cohort: "
        f"missing {sorted(set(cohorts['topology_a_listeners']) - flagged)}, "
        f"unexpected {sorted(flagged - set(cohorts['topology_a_listeners']))}"
    )
    assert not (flagged & cohorts["topology_b_listeners"]), (
        "the listener-keyed rule flagged a Topology B listener at full scale: "
        f"{sorted(flagged & cohorts['topology_b_listeners'])}"
    )


def test_the_track_key_does_not_catch_topology_a_at_full_scale():
    """Rule B flags the Topology B target and NO Topology A track (PROF-01).

    The Topology A cohort's plays reach hundreds of distinct tracks, and no one
    track-hour bucket among them accumulates enough unique listeners in the stop
    band. The comparison set is `topology_a_tracks - {topology_b_target}` --
    the target itself is in the Topology A track set, because the Topology A
    cohort plays across the whole catalogue -- and that difference is asserted
    non-empty first, so the emptiness of the intersection cannot pass vacuously.
    """
    _, track_review = _full_scale_artifacts()
    cohorts = _full_scale_cohorts()
    target = _topology_b_target()

    flagged = {entry["track_id"] for entry in track_review["flagged_tracks"]}
    assert flagged == {target}, (
        f"the track queue is not exactly the Topology B target {target!r}: "
        f"{sorted(flagged)}"
    )

    topology_a_only = set(cohorts["topology_a_tracks"]) - {target}
    assert topology_a_only, (
        "the Topology A cohort played no track other than the Topology B "
        "target; the cross-condition below would then be vacuous"
    )
    assert not (flagged & topology_a_only), (
        "the track-keyed rule flagged a Topology-A-only track at full scale: "
        f"{sorted(flagged & topology_a_only)}"
    )


# =========================================================================
# PROF-02 -- two real end-to-end replays of the same input
# =========================================================================
@pytest.fixture(scope="module")
def run_b(tmp_path_factory):
    """The second end-to-end replay, independent of the first in every respect.

    Its own topics, its own consumer groups, its own state directory. The state
    directory is the one that is easy to get wrong: Consumer 1's journal dedups
    on `event_id` across runs, so a second replay sharing run A's `--state-dir`
    would forward nothing at all and PROF-02 would compare a real run against an
    empty one -- and pass.
    """
    yield _run_pipeline(tmp_path_factory.mktemp("run-b"), "b")


def _detection_content(document: Dict[str, Any]) -> Dict[str, Any]:
    """A review document with its `counts` block removed. THIS IS DELIBERATE.

    PROF-02 claims that replaying the same input reproduces the same DETECTION.
    `counts` does not describe detection: `polled`, `records_seen`,
    `duplicate_event_id` and `late_dropped` describe the state of the topic and
    of the journal the run happened to meet. Comparing them would silently couple
    this proof to conditions the requirement never claimed -- a clean topic and an
    empty journal -- and the first run against a shared broker would fail
    intermittently. That failure would be read as nondeterminism in the
    detection, which is precisely the wrong conclusion and precisely the reason
    this projection exists.

    DO NOT "FIX" THIS INTO A WHOLE-FILE DIFF. What remains after `counts` is
    removed is everything PROF-02 is about: the flagged entries with all their
    measured values, the thresholds, the window bounds, the notes, and the config
    path behind them.

    Exact equality on the float fields (`band_share`, `plays_per_listener`) is
    also deliberate rather than an oversight. Both runs perform identical
    arithmetic over identical integer inputs and `write_review_queue()`
    serializes with `sort_keys=True`, so equality is exact or something drifted.
    An approximate comparison here would hide exactly the drift being looked for.
    """
    return {key: value for key, value in document.items() if key != "counts"}


def test_the_two_replays_were_genuinely_independent(
    run_a: PipelineRun, run_b: PipelineRun
):
    """The precondition every PROF-02 comparison below depends on.

    A run compared against itself is equal unconditionally. This asserts the two
    runs shared no topic, no output topic and no state directory, so what the
    three comparisons demonstrate is reproducibility rather than identity.
    """
    assert run_a.in_topic != run_b.in_topic
    assert run_a.out_topic != run_b.out_topic
    assert run_a.state_dir != run_b.state_dir
    assert run_a.track_review["flagged_tracks"], "run A flagged no track"
    assert run_b.track_review["flagged_tracks"], "run B flagged no track"
    assert run_a.listener_review["flagged_listeners"], "run A flagged no listener"
    assert run_b.listener_review["flagged_listeners"], "run B flagged no listener"


def test_two_replays_produce_the_same_flagged_tracks_and_evidence(
    run_a: PipelineRun, run_b: PipelineRun
):
    """Consumer 2's detection content is identical across two replays (PROF-02).

    Every flagged track with all three measured conditions, their thresholds and
    comparisons, the window bounds, the stop band and the note.

    Phase 4's order-independence test does NOT discharge this. It proves the
    aggregation is order-independent over one fixed input set; it never runs the
    pipeline twice. PROF-02 is two real replays through a live broker, and only
    a second real replay can satisfy it.
    """
    assert _detection_content(run_a.track_review) == _detection_content(
        run_b.track_review
    )


def test_two_replays_produce_the_same_flagged_listeners_and_evidence(
    run_a: PipelineRun, run_b: PipelineRun
):
    """Consumer 1's detection content is identical across two replays (PROF-02).

    Every flagged listener with its peak play count, the threshold it was
    compared against, the window and the first/last event times.

    As above: Phase 4's order-independence test does not discharge this, because
    it never runs the pipeline twice.
    """
    assert _detection_content(run_a.listener_review) == _detection_content(
        run_b.listener_review
    )


def test_two_replays_put_the_same_event_ids_on_track_activity(
    run_a: PipelineRun, run_b: PipelineRun
):
    """The same input yields the same event IDs downstream (PROF-02, check 6).

    This is the contract's own §9 check 6 wording -- "the same event IDs and the
    same logical results" -- with the logical results covered by the two document
    comparisons above and the event IDs covered here. Both sets are compared
    against the fixture's own IDs as well, so an agreement between two equally
    truncated runs cannot pass.

    Phase 4's order-independence test does not discharge this either: it never
    runs the pipeline twice.
    """
    fixture_event_ids = {record["event_id"] for record in _fixture_records()}
    assert run_a.track_activity_event_ids == run_b.track_activity_event_ids
    assert set(run_a.track_activity_event_ids) == fixture_event_ids
