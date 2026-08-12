"""The contract's seven acceptance checks, as a checklist that runs (PROF-03).

`PRODUCER-CONSUMER-CONTRACT.md` section 9 lists seven things "both sides should be
able to verify" before the implementation is considered compatible. Until now that
list was prose: a reader could tick it off by reading, and a check that quietly
stopped being true would leave the document reading exactly as satisfied as
before. This file makes the list legible as a checklist IN RUNNING CODE -- one
test per check, named and numbered to match, each docstring opening with the
check's verbatim wording.

WHAT EACH TEST DOES, AND WHY IT IS NOT ALWAYS AN ASSERTION
----------------------------------------------------------
Four of the seven are already discharged by tests written in Phases 1 and 3, and
one is this phase's own PROF-02. Re-implementing them here would add running time
-- two of them drive a live broker -- and, worse, would create a SECOND place for
the same property to be stated, free to drift from the first. So each test either
asserts its check directly, or CITES the existing test that discharges it by node
id, verified against a real `pytest --collect-only` run.

The citation is not a comment. `_discharged_by()` asserts the cited node id
actually resolves in the collected set, so renaming or deleting a discharging test
breaks the citing check here rather than leaving the checklist silently reading as
satisfied. That the cited test PASSES is proven by the same full-suite run that
runs this file.

Three checks gain something by being asserted here rather than only cited.
Checks 1 and 2 validate the examples PRINTED IN THE CONTRACT ITSELF, which no
other test does -- the fixture tests validate the fixture, not the document. Check
7 corroborates the manifest's cohort claims by recomputing them from the stream,
so the generator's own account is checked rather than taken at its word.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Set

import pytest
from pydantic import ValidationError

from contracts.play_event_v1 import PlayEventV1

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = REPO_ROOT / "PRODUCER-CONSUMER-CONTRACT.md"
STREAM_PATH = REPO_ROOT / "data" / "play_events.jsonl"
MANIFEST_PATH = REPO_ROOT / "data" / "play_events_manifest.json"

COLLECT_TIMEOUT_SECONDS = 180.0

# `src/generate_events.py` declares the cohorts by listener-id prefix.
COHORT_PREFIXES = {"topology_a": "A", "topology_b": "B"}


# --- citations, checked against a real collection -------------------------
@pytest.fixture(scope="module")
def collected_node_ids() -> Set[str]:
    """Every node id pytest collects under `tests/`, from one real collection.

    Run once for the module. This is what turns a citation from a comment into an
    assertion: a cited test that has been renamed or deleted is simply not in
    this set.
    """
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q", "--collect-only"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=COLLECT_TIMEOUT_SECONDS,
    )
    assert proc.returncode == 0, (
        f"pytest --collect-only exited {proc.returncode}\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    ids = {line.strip() for line in proc.stdout.splitlines() if "::" in line}
    assert ids, f"collected no node ids at all:\n{proc.stdout}"
    return ids


def _discharged_by(collected: Set[str], node_id: str) -> None:
    """Assert a cited node id resolves in the collected set.

    Prefix matching, not equality, because a parametrized test collects as
    `name[param]` and would never match its own bare id. A prefix match is what
    lets a citation survive parametrization while still breaking on a rename or a
    deletion -- which is the entire point of citing by node id rather than in
    prose.

    This proves the cited test EXISTS AND IS COLLECTED. That it passes is proven
    by the same full-suite run that runs this file.
    """
    hit = any(
        candidate == node_id or candidate.startswith(node_id + "[")
        for candidate in collected
    )
    assert hit, (
        f"the contract check above cites {node_id!r}, which pytest does not "
        "collect. It has been renamed or deleted, and this check is no longer "
        "discharged by anything."
    )


# --- the contract document's own examples ---------------------------------
def _fenced_json_blocks(markdown: str) -> List[str]:
    """Every ```json fenced block in a slice of the contract document."""
    return re.findall(r"```json\n(.*?)```", markdown, flags=re.DOTALL)


@lru_cache(maxsize=1)
def _contract_text() -> str:
    return CONTRACT_PATH.read_text(encoding="utf-8")


def _contract_valid_example() -> Dict[str, Any]:
    """The json block under the contract's `### Valid example` heading.

    The region is bounded by the next `## ` heading, and exactly one json block
    must be found in it -- the neighbouring ```text block holds the Kafka key, not
    a record. Asserting the count means an edit to the contract surfaces as a
    clear failure here instead of silently selecting a different block.
    """
    text = _contract_text()
    start = text.index("### Valid example")
    end = text.index("\n## ", start)
    blocks = _fenced_json_blocks(text[start:end])
    assert len(blocks) == 1, (
        f"expected exactly one json block under '### Valid example', found "
        f"{len(blocks)}; the contract has been restructured"
    )
    return json.loads(blocks[0])


def _contract_invalid_example() -> Dict[str, Any]:
    """The json block between the contract's `## 6.` and `## 7.` headings."""
    text = _contract_text()
    start = text.index("\n## 6.")
    end = text.index("\n## 7.", start)
    blocks = _fenced_json_blocks(text[start:end])
    assert len(blocks) == 1, (
        f"expected exactly one json block in contract section 6, found "
        f"{len(blocks)}; the contract has been restructured"
    )
    return json.loads(blocks[0])


@lru_cache(maxsize=1)
def _manifest() -> Dict[str, Any]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def _cohort_event_counts() -> Dict[str, int]:
    """Cohort event counts recomputed from the committed stream itself."""
    counts = {"normal": 0, "topology_a": 0, "topology_b": 0}
    with STREAM_PATH.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            listener_id = json.loads(line)["listener_id"]
            for cohort, prefix in COHORT_PREFIXES.items():
                if listener_id.startswith(prefix):
                    counts[cohort] += 1
                    break
            else:
                counts["normal"] += 1
    return counts


# =========================================================================
# The seven checks, in the contract's own order
# =========================================================================
def test_check_1_a_valid_example_passes_the_shared_pydantic_model(
    collected_node_ids: Set[str],
):
    """"A valid example passes the shared Pydantic model."

    Asserted directly, against the example PRINTED IN THE CONTRACT -- which is
    stronger than what existed. `tests/test_contract_model.py` validates every
    fixture record, but nothing validated the block the document itself shows to
    a reader as the canonical valid record. A contract whose own example no
    longer validates is a contract that has drifted from its model.
    """
    event = PlayEventV1.model_validate(_contract_valid_example())
    assert event.event_type == "play"
    assert event.listener_id
    assert event.played_seconds <= event.track_duration_seconds

    _discharged_by(
        collected_node_ids,
        "tests/test_contract_model.py::test_every_valid_fixture_record_is_accepted",
    )


def test_check_2_an_invalid_example_is_rejected_before_production(
    collected_node_ids: Set[str],
):
    """"An invalid example is rejected before production."

    Asserted directly against the contract's own invalid example, and against the
    REASON the document gives for it: `played_seconds` exceeds
    `track_duration_seconds`. Asserting only that something was rejected would
    pass just as happily on an unrelated defect in the example.
    """
    invalid = _contract_invalid_example()
    assert invalid["played_seconds"] > invalid["track_duration_seconds"], (
        "the contract's invalid example no longer violates the rule the document "
        "says it violates"
    )

    with pytest.raises(ValidationError) as excinfo:
        PlayEventV1.model_validate(invalid)

    message = str(excinfo.value)
    assert "played_seconds" in message
    assert "track_duration_seconds" in message

    _discharged_by(
        collected_node_ids,
        "tests/test_contract_model.py"
        "::test_the_model_rejects_the_record_and_names_the_documented_reason",
    )


def test_check_3_the_play_events_key_exactly_matches_listener_id(
    collected_node_ids: Set[str],
):
    """"The `play-events` key exactly matches `listener_id`."

    Asserted directly at the producer's declaration: the committed stream's
    manifest names `listener_id` as the Kafka key field. The behaviour on the
    wire -- that the producer's key function and the consumer's check agree, and
    that a consumed batch really carries that key -- is discharged by the two
    cited tests.
    """
    assert _manifest()["kafka_key_field"] == "listener_id"

    _discharged_by(
        collected_node_ids,
        "tests/test_contract_helpers.py"
        "::test_producer_key_function_and_consumer_check_agree",
    )
    _discharged_by(
        collected_node_ids,
        "tests/test_harness_roundtrip.py"
        "::test_consumed_batch_holds_contract_invariants",
    )


def test_check_4_consumer_1_preserves_event_id_and_the_full_json_value(
    collected_node_ids: Set[str],
):
    """"Consumer 1 preserves `event_id` and the full JSON value."

    Cited, not re-implemented. The discharging test drives a live broker end to
    end and compares the forwarded values BYTE for byte against the fixture
    lines; running a second copy of that here would double its real wall-clock
    cost and create a second place for the same property to drift.
    """
    _discharged_by(
        collected_node_ids,
        "tests/test_consumer_stage1_e2e.py"
        "::test_every_valid_record_is_forwarded_rekeyed_and_byte_identical",
    )


def test_check_5_the_track_activity_key_exactly_matches_track_id(
    collected_node_ids: Set[str],
):
    """"The `track-activity` key exactly matches `track_id`."

    Cited, not re-implemented, for the same reason as check 4: the discharging
    test reads the real output topic and asserts the key is the `track_id` and no
    longer the `listener_id` -- the re-keying that makes the two-key design a
    design at all.
    """
    _discharged_by(
        collected_node_ids,
        "tests/test_consumer_stage1_e2e.py"
        "::test_the_output_key_is_the_track_id_and_no_longer_the_listener_id",
    )


def test_check_6_replaying_the_same_input_reproduces_ids_and_results(
    collected_node_ids: Set[str],
):
    """"Replaying the same input produces the same event IDs and logical results."

    Discharged by this phase's own PROF-02 tests: the event-id comparison covers
    "the same event IDs", the review-document comparison covers "the same logical
    results", and both run two genuinely independent end-to-end replays through a
    live broker.

    PHASE 4'S ORDER-INDEPENDENCE TEST DOES NOT DISCHARGE THIS CHECK and must not
    be cited as if it did. It proves the aggregation is order-independent over
    one fixed input set. It never runs the pipeline twice, and "replaying the
    same input" is a claim about a second replay.
    """
    _discharged_by(
        collected_node_ids,
        "tests/test_two_key_proof.py"
        "::test_two_replays_put_the_same_event_ids_on_track_activity",
    )
    _discharged_by(
        collected_node_ids,
        "tests/test_two_key_proof.py"
        "::test_two_replays_produce_the_same_flagged_tracks_and_evidence",
    )
    _discharged_by(
        collected_node_ids,
        "tests/test_two_key_proof.py"
        "::test_two_replays_produce_the_same_flagged_listeners_and_evidence",
    )
    _discharged_by(
        collected_node_ids,
        "tests/test_two_key_proof.py::test_the_two_replays_were_genuinely_independent",
    )


def test_check_7_the_generator_includes_normal_data_and_both_topologies():
    """"The generator includes normal data and both documented anomaly topologies."

    Asserted twice over, because the manifest is the generator's own account of
    itself. First the manifest is checked for internal consistency: all three
    cohorts present, each positive, summing to `total_events`. Then the same
    three numbers are RECOMPUTED from `data/play_events.jsonl` by listener-id
    prefix, so the claim is corroborated against the stream rather than taken at
    the manifest's word.
    """
    manifest = _manifest()
    by_cohort = manifest["counts_by_cohort"]

    for cohort in ("normal", "topology_a", "topology_b"):
        assert cohort in by_cohort, f"the manifest declares no {cohort!r} cohort"
        assert by_cohort[cohort] > 0, f"the {cohort!r} cohort is empty"
    assert sum(by_cohort.values()) == manifest["total_events"]

    if not STREAM_PATH.is_file():
        pytest.skip(
            f"{STREAM_PATH} is absent, so the manifest's cohort claims cannot be "
            "corroborated against the stream. Regenerate it with "
            f"`python3 src/generate_events.py --seed {manifest['seed']}`"
        )

    assert _cohort_event_counts() == by_cohort, (
        "the cohort counts recomputed from the stream do not match the "
        f"manifest: scanned {_cohort_event_counts()}, manifest declares "
        f"{by_cohort}"
    )
