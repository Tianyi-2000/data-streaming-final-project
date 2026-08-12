"""CTRT-02: the shared model accepts every valid fixture record and rejects every
invalid one, each for its documented reason.

Data-driven on purpose. The valid side reads
`tests/fixtures/play_events_fixture.jsonl`; the invalid side reads
`tests/fixtures/invalid_events.jsonl`, where each line is an *envelope* carrying a
`case` name, an `expect`, a plain-English `reason` quoting the contract rule it
violates, an `error_contains` substring the rejection must include, an optional
`kafka_key`, and the offending `record` nested inside. Adding a tenth case requires
no new test code.

WHY THE INVALID CASES LIVE IN THEIR OWN FILE
--------------------------------------------
They cannot sit in `play_events_fixture.jsonl`, because that file is replayable and
`src/replay_to_kafka.py` validates and drops before sending -- an invalid line there
would silently vanish and test nothing.

Be precise about what the separate file actually buys, because the overclaim is
tempting. Nothing prevents someone pointing `--input` at it; that flag takes any
path. What holds is that every line fails `PlayEventV1.model_validate_json` at the
*envelope's top level*, so the replay producer rejects all of them and places no
record on `play-events`. The file is structurally incapable of contaminating the
topic, not structurally impossible to open. `test_no_invalid_line_validates_as_a_bare_event`
is what actually holds that claim up.

ASSERTING THE REASON, NOT MERELY THE FAILURE
--------------------------------------------
Every rejection test checks the stringified `ValidationError` against the case's own
`error_contains`. Asserting only that *something* went wrong would pass on a typo in
the test fixture just as happily as on the rule it claims to prove.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest
from pydantic import ValidationError

# Same REPO_ROOT bootstrap the repo's other modules use, so this file resolves with
# or without the root conftest.py and with or without PYTHONPATH set.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.play_event_v1 import PlayEventV1, key_matches_listener_id  # noqa: E402

FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "play_events_fixture.jsonl"
INVALID_CASES_PATH = REPO_ROOT / "tests" / "fixtures" / "invalid_events.jsonl"

# The rules PRODUCER-CONSUMER-CONTRACT.md section 6 enumerates. Every one of them
# must be exercised by at least one case in the invalid-cases file. The eighth entry
# is not a section 6 bullet -- it is the model's own `extra="forbid"` guard, kept in
# the same file because a stray field is the same class of defect.
CONTRACT_SECTION_6_RULES = {
    "required_field_missing",
    "kafka_key_not_equal_listener_id",
    "event_type_not_play",
    "id_is_empty",
    "played_seconds_negative",
    "played_seconds_exceeds_duration",
    "track_duration_not_positive",
    "event_time_not_valid_utc",
}

EXPECTED_CASE_COUNT = 9


def _read_lines(path: Path) -> List[str]:
    if not path.is_file():
        return []
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _load_invalid_cases() -> List[Dict[str, Any]]:
    return [json.loads(line) for line in _read_lines(INVALID_CASES_PATH)]


VALID_LINES = _read_lines(FIXTURE_PATH)
INVALID_LINES = _read_lines(INVALID_CASES_PATH)
INVALID_CASES = _load_invalid_cases()
MODEL_REJECT_CASES = [c for c in INVALID_CASES if c["expect"] == "model_reject"]
KEY_MISMATCH_CASES = [c for c in INVALID_CASES if c["expect"] == "key_mismatch"]


def _case_id(case: Dict[str, Any]) -> str:
    return case["case"]


# --------------------------------------------------------------------------------
# The accept half of CTRT-02
# --------------------------------------------------------------------------------
def test_the_valid_fixture_holds_all_63_records():
    assert len(VALID_LINES) == 63, (
        f"expected 63 fixture records, found {len(VALID_LINES)} in {FIXTURE_PATH}"
    )


@pytest.mark.parametrize("line", VALID_LINES, ids=lambda l: json.loads(l)["event_id"])
def test_every_valid_fixture_record_is_accepted(line: str):
    """CTRT-02's accept half -- what makes the fixture trustworthy downstream."""
    event = PlayEventV1.model_validate_json(line)
    assert event.event_type == "play"
    assert event.schema_version == 1


def test_the_two_boundary_records_are_accepted_not_merely_present():
    """CTRT-03 names both boundaries; both are valid and both must be ACCEPTED."""
    by_id = {
        e.event_id: e for e in (PlayEventV1.model_validate_json(l) for l in VALID_LINES)
    }
    assert by_id["fx-n-001"].played_seconds == 0
    zero_length = by_id["fx-n-003"]
    assert zero_length.played_seconds == zero_length.track_duration_seconds


# --------------------------------------------------------------------------------
# The reject half of CTRT-02
# --------------------------------------------------------------------------------
def test_the_invalid_cases_file_exists_and_holds_every_case():
    assert INVALID_CASES_PATH.is_file(), f"missing invalid-cases file: {INVALID_CASES_PATH}"
    assert len(INVALID_CASES) == EXPECTED_CASE_COUNT, (
        f"expected {EXPECTED_CASE_COUNT} invalid cases, found {len(INVALID_CASES)}"
    )


@pytest.mark.parametrize("case", MODEL_REJECT_CASES, ids=_case_id)
def test_the_model_rejects_the_record_and_names_the_documented_reason(case: Dict[str, Any]):
    with pytest.raises(ValidationError) as excinfo:
        PlayEventV1.model_validate(case["record"])
    message = str(excinfo.value)
    assert case["error_contains"] in message, (
        f"case {case['case']}: rejection message did not name "
        f"{case['error_contains']!r}.\nreason: {case['reason']}\ngot: {message}"
    )


@pytest.mark.parametrize("case", KEY_MISMATCH_CASES, ids=_case_id)
def test_a_key_mismatch_passes_the_model_and_is_caught_only_by_the_helper(
    case: Dict[str, Any],
):
    """The one section 6 rule with no owner inside the model.

    `PlayEventV1` validates the Kafka *value* and never sees the key, so this record
    is structurally valid and the model is right to accept it. `key_matches_listener_id`
    is where the rule lives -- which is why plan 01-01 added it.
    """
    event = PlayEventV1.model_validate(case["record"])  # valid: the model is not wrong
    assert event.listener_id == case["record"]["listener_id"]
    assert key_matches_listener_id(case["kafka_key"], event) is False, case["reason"]
    # And the same record with the right key passes, so the helper is not just
    # returning False for everything.
    assert key_matches_listener_id(event.listener_id, event) is True


def test_every_model_reject_case_has_exactly_one_defect():
    """Each case must be a realistic record broken in exactly one documented way.

    Without this, a case could pass for a reason other than the one it names -- two
    defects, and `error_contains` proves nothing about which rule fired.
    """
    for case in MODEL_REJECT_CASES:
        with pytest.raises(ValidationError) as excinfo:
            PlayEventV1.model_validate(case["record"])
        assert len(excinfo.value.errors()) == 1, (
            f"case {case['case']} raised {len(excinfo.value.errors())} errors; "
            f"a case with more than one defect cannot prove which rule rejected it"
        )


# --------------------------------------------------------------------------------
# Structural guarantees about the invalid-cases file itself
# --------------------------------------------------------------------------------
def test_every_invalid_line_is_an_envelope_carrying_case_and_record():
    assert INVALID_LINES, "no invalid-case lines found"
    for line in INVALID_LINES:
        envelope = json.loads(line)
        assert isinstance(envelope, dict)
        for key in ("case", "expect", "reason", "record", "contract_rule"):
            assert key in envelope, f"envelope missing {key!r}: {line}"
        assert envelope["expect"] in ("model_reject", "key_mismatch")
        assert isinstance(envelope["record"], dict)


def test_no_invalid_line_validates_as_a_bare_event():
    """This is the assertion that holds up the "cannot reach play-events" claim.

    `src/replay_to_kafka.py` calls `PlayEventV1.model_validate_json` on each line and
    drops what fails. Every line here is an envelope, not an event, so every line
    fails at the top level and none of it could be produced -- even if someone points
    `--input` at this file, which nothing prevents.
    """
    for line in INVALID_LINES:
        with pytest.raises(ValidationError):
            PlayEventV1.model_validate_json(line)


def test_the_cases_cover_every_rule_in_contract_section_6():
    covered = {case["contract_rule"] for case in INVALID_CASES}
    missing = CONTRACT_SECTION_6_RULES - covered
    assert not missing, f"contract section 6 rules with no case: {sorted(missing)}"


def test_case_names_are_unique():
    names = [case["case"] for case in INVALID_CASES]
    assert len(names) == len(set(names)), f"duplicate case names: {names}"


def test_the_ground_truth_file_points_at_the_invalid_cases():
    """The oracle names where the invalid cases live, so the two cannot drift apart."""
    expected_flags = json.loads(
        (REPO_ROOT / "tests" / "fixtures" / "expected_flags.json").read_text(
            encoding="utf-8"
        )
    )
    assert (
        REPO_ROOT / expected_flags["invalid_cases_path"]
    ).resolve() == INVALID_CASES_PATH.resolve()
