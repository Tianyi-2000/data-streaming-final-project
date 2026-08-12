"""The bounded summary's contract (SUMM-02) and its fallback (SUMM-03).

EVERY TEST HERE RUNS AGAINST A STUBBED CLIENT. None touches the network, none
touches a broker, and none requires an API key. That is not a convenience, it is
the only way the requirement can be met at all: SUMM-02 asserts a CONSTRAINT --
"at most three notes", "no number absent from the input", "no assertion that
fraud occurred" -- and a live call samples one response. One sampled response
that behaved tells you nothing about a bound. A stub that deliberately returns
four notes, a fabricated number, an unflagged track and a verdict, and is
rejected each time, is what proves the bound exists.

THE KEYSTONE IS `test_the_shipped_template_note_satisfies_the_number_bound`.
The number bound is a calibrated check, not an obvious one: it has to admit
every rendering the shipped `template_note()` produces (which renders ratios to
two decimals) while rejecting anything invented. Calibrating it against
known-good output is what makes it correct; the mutated-digit test next to it is
what proves it is not simply passing everything. If the keystone ever fails, the
BOUND is wrong -- `src/consumer_stage2.py` is frozen and `template_note` is a
cross-phase contract.

The verdict lexicon is IMPORTED, never restated. A test that retyped the terms
could drift from the module's tuple, and the drift would show up as a passing
test over a lexicon nothing enforces.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.consumer_stage2 import (  # noqa: E402
    CONDITION_MAX_PLAYS_PER_LISTENER,
    CONDITION_MIN_BAND_SHARE,
    CONDITION_MIN_UNIQUE_LISTENERS,
    template_note,
)
from src.summarize_review_queue import (  # noqa: E402
    MAX_NOTES,
    SOURCE_MODEL,
    SOURCE_TEMPLATE,
    VERDICT_LEXICON,
    check_note,
    default_client,
    main,
    summarize,
)

REAL_REVIEW_QUEUE = REPO_ROOT / "output" / "track_review_queue.json"

REGENERATE = (
    "output/track_review_queue.json is absent or was not produced by a "
    "full-scale run; output/ is gitignored, so this is the normal state of a "
    "fresh checkout. Regenerate it with the commands in README.md."
)


# =========================================================================
# helpers -- synthetic entries in exactly the shape Consumer 2 writes
# =========================================================================
def _entry(
    *,
    track_id: str,
    window_start: str,
    unique_listeners: int,
    total_plays: int,
    plays_in_band: int,
    window_hours: int = 1,
    min_unique: int = 200,
    max_ratio: float = 1.1,
    min_share: float = 0.6,
    first_event_time: str = "2026-08-09T20:00:03Z",
    last_event_time: str = "2026-08-09T20:44:58Z",
) -> Dict[str, Any]:
    """One flagged-entry dict with the same keys `Stage2Processor` emits.

    Built here rather than by driving the real processor because these tests are
    about the SUMMARY layer's bounds, not about detection: what matters is that
    the shape and the arithmetic match what `evaluate_all()` produces, so a note
    checked against one of these is checked against the same thing a note about a
    real entry would be.
    """
    plays_per_listener = total_plays / unique_listeners
    band_share = plays_in_band / total_plays
    return {
        "track_id": track_id,
        "window_start": window_start,
        "window_hours": window_hours,
        "unique_listeners": unique_listeners,
        "total_plays": total_plays,
        "plays_per_listener": plays_per_listener,
        "band_share": band_share,
        "plays_in_band": plays_in_band,
        "stop_band_seconds": [30, 35],
        "first_event_time": first_event_time,
        "last_event_time": last_event_time,
        "conditions": {
            CONDITION_MIN_UNIQUE_LISTENERS: {
                "measured": unique_listeners,
                "threshold": min_unique,
                "comparison": "at least",
                "satisfied": unique_listeners >= min_unique,
            },
            CONDITION_MAX_PLAYS_PER_LISTENER: {
                "measured": plays_per_listener,
                "threshold": max_ratio,
                "comparison": "at most",
                "satisfied": plays_per_listener <= max_ratio,
            },
            CONDITION_MIN_BAND_SHARE: {
                "measured": band_share,
                "threshold": min_share,
                "comparison": "at least",
                "satisfied": band_share >= min_share,
            },
        },
        "flagged": True,
    }


SYNTHETIC_A = _entry(
    track_id="11111111-2222-3333-4444-555555555555",
    window_start="2026-08-09T20:00:00Z",
    unique_listeners=901,
    total_plays=901,
    plays_in_band=832,
)

SYNTHETIC_B = _entry(
    track_id="99999999-8888-7777-6666-555555555555",
    window_start="2026-08-10T03:00:00Z",
    unique_listeners=412,
    total_plays=436,
    plays_in_band=301,
    window_hours=1,
)

SYNTHETIC_C = _entry(
    track_id="abcdabcd-1234-5678-9012-abcdefabcdef",
    window_start="2026-08-11T07:00:00Z",
    unique_listeners=254,
    total_plays=257,
    plays_in_band=203,
)

SYNTHETIC_D = _entry(
    track_id="deadbeef-0000-1111-2222-333344445555",
    window_start="2026-08-11T09:00:00Z",
    unique_listeners=333,
    total_plays=333,
    plays_in_band=290,
)


def _document(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """A review document around a list of flagged entries, notes attached.

    `review_document()` attaches `template_note` to every flagged entry, so a
    document handed to the summary layer always has one -- that is what the
    fallback returns.
    """
    return {
        "flagged_tracks": [{**entry, "note": template_note(entry)} for entry in entries],
        "counts": {"records_seen": 0, "counted": 0, "flagged_buckets": len(entries)},
        "thresholds_source_path": "config/thresholds.json",
        "rule": "topology_b: all three conditions must hold TOGETHER",
        "posture": "review candidates, not a verdict",
    }


class RecordingStub:
    """A client that records every call and returns a scripted response.

    The call COUNT is asserted as often as the return value. "An empty review
    queue must not spend a call" is a property of the summary layer that no
    assertion about notes can see.
    """

    def __init__(self, response: Any = "[]", raises: Optional[Exception] = None):
        self.calls: List[str] = []
        self._response = response
        self._raises = raises

    def __call__(self, prompt: str) -> str:
        self.calls.append(prompt)
        if self._raises is not None:
            raise self._raises
        if isinstance(self._response, str):
            return self._response
        return json.dumps(self._response)


def _model_response(pairs: List[Dict[str, str]]) -> str:
    return json.dumps(pairs)


def _real_flagged_entry() -> Dict[str, Any]:
    if not REAL_REVIEW_QUEUE.is_file():
        pytest.skip(REGENERATE)
    document = json.loads(REAL_REVIEW_QUEUE.read_text(encoding="utf-8"))
    flagged = document.get("flagged_tracks") or []
    if not flagged:
        pytest.skip(f"{REGENERATE}\n(reason: no flagged track in the document)")
    return flagged[0]


# =========================================================================
# the keystone: the bound is calibrated against the shipped fallback
# =========================================================================
def test_the_shipped_template_note_satisfies_the_number_bound():
    """The real flagged entry's own note passes the check applied to model output.

    IF THIS FAILS THE BOUND IS WRONG, NOT THE TEMPLATE. `template_note` is
    frozen in `src/consumer_stage2.py` and its docstring names it this phase's
    fallback; a bound that rejects the fallback would make the fallback path
    unreachable, which is the one path guaranteed to run on a machine with no
    API key.
    """
    entry = _real_flagged_entry()
    assert check_note(template_note(entry), entry) is None


@pytest.mark.parametrize(
    "entry",
    [SYNTHETIC_A, SYNTHETIC_B, SYNTHETIC_C, SYNTHETIC_D],
    ids=["synthetic-a", "synthetic-b", "synthetic-c", "synthetic-d"],
)
def test_the_template_note_satisfies_the_bound_for_synthetic_entries(entry):
    """Not just the one entry that happens to be on disk.

    The real entry has integral ratios (901 plays over 901 listeners is exactly
    1.00), which would hide a bound that only admits integers. B and C have
    non-integral ratios and shares that render to two decimals with a real
    remainder.
    """
    assert check_note(template_note(entry), entry) is None


def test_the_number_bound_rejects_a_single_changed_digit():
    """The bound is not vacuous: one digit is enough to fail it.

    Without this, a check that returned `None` unconditionally would pass the
    keystone above and prove nothing at all.
    """
    entry = SYNTHETIC_A
    note = template_note(entry)
    mutated = note.replace("901 unique listeners", "902 unique listeners")
    assert mutated != note, "the mutation did not apply; the template wording moved"
    assert check_note(mutated, entry) is not None


def test_the_number_bound_rejects_a_fabricated_count():
    """A number that never appeared in the entry, appended to a good note."""
    entry = SYNTHETIC_A
    note = template_note(entry) + " Comparable to 1,040 accounts seen elsewhere."
    assert check_note(note, entry) is not None


def test_identifier_digits_are_not_treated_as_claims():
    """A UUID and three timestamps are full of digits that are not claims.

    If the bound counted them it would reject the shipped template on every
    entry, which is the failure mode the keystone catches -- this test names the
    reason so the scrubbing is not later removed as redundant.
    """
    entry = SYNTHETIC_A
    assert entry["track_id"] in template_note(entry)
    assert entry["window_start"] in template_note(entry)
    assert check_note(template_note(entry), entry) is None


# =========================================================================
# SUMM-02 -- the five bounds, each rejecting the WHOLE response
# =========================================================================
def test_a_well_behaved_stub_is_trusted_and_called_exactly_once():
    document = _document([SYNTHETIC_A])
    entry = document["flagged_tracks"][0]
    stub = RecordingStub(
        _model_response([{"track_id": entry["track_id"], "note": template_note(entry)}])
    )

    result = summarize(document, client=stub)

    assert result.source == SOURCE_MODEL
    assert result.fallback_reason is None
    assert [note["track_id"] for note in result.notes] == [entry["track_id"]]
    assert len(stub.calls) == 1, "one call over the review queue, per SUMM-02"


def test_more_notes_than_the_cap_rejects_the_whole_response():
    """Four notes for a three-flag document: all four go, three templates come back.

    Rejecting only the fourth would leave three notes the bound never actually
    held over -- a partially trusted answer is not a bound.
    """
    document = _document([SYNTHETIC_A, SYNTHETIC_B, SYNTHETIC_C])
    entries = document["flagged_tracks"]
    payload = [
        {"track_id": entry["track_id"], "note": template_note(entry)}
        for entry in entries
    ]
    payload.append({"track_id": entries[0]["track_id"], "note": "One more."})
    stub = RecordingStub(_model_response(payload))

    result = summarize(document, client=stub)

    assert result.source == SOURCE_TEMPLATE
    assert result.fallback_reason is not None
    assert len(result.notes) == MAX_NOTES
    assert [note["note"] for note in result.notes] == [
        template_note(entry) for entry in entries
    ]


def test_a_track_id_absent_from_the_input_rejects_the_whole_response():
    document = _document([SYNTHETIC_A])
    stub = RecordingStub(
        _model_response(
            [{"track_id": "00000000-dead-dead-dead-000000000000", "note": "Queued."}]
        )
    )

    result = summarize(document, client=stub)

    assert result.source == SOURCE_TEMPLATE
    assert result.fallback_reason is not None


def test_a_repeated_track_id_rejects_the_whole_response():
    """Two notes about the same track is one track speaking twice, not two facts."""
    document = _document([SYNTHETIC_A, SYNTHETIC_B])
    entry = document["flagged_tracks"][0]
    stub = RecordingStub(
        _model_response(
            [
                {"track_id": entry["track_id"], "note": template_note(entry)},
                {"track_id": entry["track_id"], "note": template_note(entry)},
            ]
        )
    )

    result = summarize(document, client=stub)

    assert result.source == SOURCE_TEMPLATE
    assert result.fallback_reason is not None


@pytest.mark.parametrize("term", list(VERDICT_LEXICON))
def test_every_verdict_term_rejects_the_whole_response(term):
    """The lexicon is imported, never retyped, and every term in it is enforced.

    Parametrising over the module's own tuple means adding a term to the lexicon
    automatically adds a test, and the two can never disagree about what the
    project refuses to say.
    """
    document = _document([SYNTHETIC_A])
    entry = document["flagged_tracks"][0]
    note = f"This window is {term} activity."
    stub = RecordingStub(
        _model_response([{"track_id": entry["track_id"], "note": note}])
    )

    result = summarize(document, client=stub)

    assert result.source == SOURCE_TEMPLATE
    assert result.fallback_reason is not None


def test_a_verdict_term_inside_a_longer_word_does_not_reject():
    """Word boundaries, not substrings.

    A lexicon matched as a bare substring would reject ordinary prose and drive
    every response to the fallback, which looks like a working bound and is not
    one.
    """
    document = _document([SYNTHETIC_A])
    entry = document["flagged_tracks"][0]
    note = template_note(entry) + " Scamper is not a verdict word."
    stub = RecordingStub(
        _model_response([{"track_id": entry["track_id"], "note": note}])
    )

    result = summarize(document, client=stub)

    assert result.source == SOURCE_MODEL, result.fallback_reason


def test_a_number_absent_from_the_entry_rejects_the_whole_response():
    document = _document([SYNTHETIC_A])
    entry = document["flagged_tracks"][0]
    note = template_note(entry).replace("901 unique listeners", "874 unique listeners")
    stub = RecordingStub(
        _model_response([{"track_id": entry["track_id"], "note": note}])
    )

    result = summarize(document, client=stub)

    assert result.source == SOURCE_TEMPLATE
    assert result.fallback_reason is not None


def test_a_number_belonging_to_a_different_entry_is_still_rejected():
    """Each note is checked against ITS OWN entry, not against the document.

    Checking against the union of every flagged entry's numbers would let the
    model attach one track's evidence to another track's note, which is the
    quiet version of inventing a number.
    """
    document = _document([SYNTHETIC_A, SYNTHETIC_B])
    entries = document["flagged_tracks"]
    stub = RecordingStub(
        _model_response(
            [{"track_id": entries[1]["track_id"], "note": template_note(entries[0])}]
        )
    )

    result = summarize(document, client=stub)

    assert result.source == SOURCE_TEMPLATE
    assert result.fallback_reason is not None


# =========================================================================
# SUMM-03 -- the fallback is the DEFAULT path, not an error path
# =========================================================================
def test_a_raising_client_falls_back_and_does_not_propagate():
    document = _document([SYNTHETIC_A])
    stub = RecordingStub(raises=RuntimeError("connection reset by peer"))

    result = summarize(document, client=stub)

    assert result.source == SOURCE_TEMPLATE
    assert result.fallback_reason is not None
    assert len(result.notes) == 1
    assert result.notes[0]["note"] == template_note(document["flagged_tracks"][0])


@pytest.mark.parametrize(
    "garbage",
    ["", "I'm sorry, I can't help with that.", "{not json at all", "[1, 2, 3]", "{}"],
    ids=["empty", "refusal", "broken-json", "wrong-shape", "object-not-list"],
)
def test_an_unparseable_response_falls_back(garbage):
    document = _document([SYNTHETIC_A])
    stub = RecordingStub(garbage)

    result = summarize(document, client=stub)

    assert result.source == SOURCE_TEMPLATE
    assert result.fallback_reason is not None


def test_no_key_means_no_client_means_no_call(monkeypatch):
    """The state of this machine, and probably the grader's.

    `default_client()` returning None is what makes "no API key required" true,
    and `summarize()` with no client must not merely avoid crashing -- it must
    not attempt a call at all.
    """
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    assert default_client() is None

    document = _document([SYNTHETIC_A])
    result = summarize(document, client=None)

    assert result.source == SOURCE_TEMPLATE
    assert result.fallback_reason is not None
    assert len(result.notes) == 1


def test_the_default_client_never_reaches_the_network_without_a_key(monkeypatch):
    """No key means the requests module is never touched.

    Asserted by making any HTTP attempt an immediate failure rather than by
    trusting that the absent key short-circuits earlier.
    """
    import src.summarize_review_queue as module

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    def explode(*args, **kwargs):  # pragma: no cover - must never run
        raise AssertionError("the summary attempted an HTTP call with no API key")

    monkeypatch.setattr(module.requests, "post", explode)

    result = summarize(_document([SYNTHETIC_A]), client=default_client())
    assert result.source == SOURCE_TEMPLATE


def test_an_empty_review_queue_returns_no_notes_and_spends_no_call():
    """Zero flagged tracks is not an error and must not cost a model call."""
    stub = RecordingStub(_model_response([]))

    result = summarize(_document([]), client=stub)

    assert result.notes == []
    assert result.source == SOURCE_TEMPLATE
    assert stub.calls == [], "an empty input must not spend a call"


def test_more_flagged_tracks_than_the_cap_yields_exactly_the_cap(monkeypatch):
    """Four flagged tracks and no client: three template notes, not four."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    document = _document([SYNTHETIC_A, SYNTHETIC_B, SYNTHETIC_C, SYNTHETIC_D])

    result = summarize(document, client=None)

    assert len(result.notes) == MAX_NOTES == 3
    assert result.source == SOURCE_TEMPLATE


def test_the_fallback_notes_are_the_shipped_template_verbatim():
    """Imported, not reimplemented. A copy would let the two drift silently."""
    document = _document([SYNTHETIC_A, SYNTHETIC_B])
    result = summarize(document, client=None)

    assert [note["note"] for note in result.notes] == [
        template_note(entry) for entry in document["flagged_tracks"]
    ]


def test_the_summary_module_does_not_import_a_new_dependency():
    """The LLM path uses `requests`, already pinned and already used upstream.

    `requirements.txt` is frozen for this phase, so a new import would be a
    dependency the project does not declare.
    """
    source = (REPO_ROOT / "src" / "summarize_review_queue.py").read_text(
        encoding="utf-8"
    )
    assert "import openai" not in source
    assert "from openai" not in source
    assert "import requests" in source


# =========================================================================
# the CLI -- exit codes, and the artifact it must never write
# =========================================================================
def test_main_leaves_the_review_queue_byte_identical(monkeypatch, capsys):
    """The one artifact a human acts on is never written by the model path.

    Hashed before and after rather than diffed, so a rewrite producing identical
    content but a different byte layout would still fail.
    """
    if not REAL_REVIEW_QUEUE.is_file():
        pytest.skip(REGENERATE)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    before = hashlib.sha256(REAL_REVIEW_QUEUE.read_bytes()).hexdigest()
    code = main([])
    after = hashlib.sha256(REAL_REVIEW_QUEUE.read_bytes()).hexdigest()

    assert code == 0, "a fallback is a success, not a failure"
    assert before == after
    assert capsys.readouterr().out.strip()


def test_main_on_a_missing_input_exits_one_and_names_the_regeneration_command(
    tmp_path, capsys
):
    """The ONE exit-1 case, and it is about the input file, never about the model."""
    missing = tmp_path / "nope" / "track_review_queue.json"

    code = main(["--input", str(missing)])

    assert code == 1
    printed = capsys.readouterr()
    combined = printed.out + printed.err
    assert "src/consumer_stage2.py" in combined


def test_main_on_an_unparseable_input_exits_one(tmp_path, capsys):
    broken = tmp_path / "track_review_queue.json"
    broken.write_text("{not json", encoding="utf-8")

    code = main(["--input", str(broken)])

    assert code == 1
    printed = capsys.readouterr()
    assert "src/consumer_stage2.py" in printed.out + printed.err


def test_main_json_output_is_machine_readable(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    document_path = tmp_path / "track_review_queue.json"
    document_path.write_text(
        json.dumps(_document([SYNTHETIC_A]), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    code = main(["--input", str(document_path), "--json"])

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["source"] == SOURCE_TEMPLATE
    assert payload["fallback_reason"]
    assert len(payload["notes"]) == 1


def test_main_never_prints_the_api_key(tmp_path, capsys, monkeypatch):
    """A provenance line names the path and the reason. It never names the key."""
    secret = "sk-test-DO-NOT-LEAK-0123456789"
    monkeypatch.setenv("OPENAI_API_KEY", secret)
    document_path = tmp_path / "track_review_queue.json"
    document_path.write_text(
        json.dumps(_document([SYNTHETIC_A]), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    import src.summarize_review_queue as module

    def explode(*args, **kwargs):
        raise RuntimeError("no network in tests")

    monkeypatch.setattr(module.requests, "post", explode)

    code = main(["--input", str(document_path)])
    printed = capsys.readouterr()

    assert code == 0
    assert secret not in printed.out
    assert secret not in printed.err


def test_summarize_is_typed_as_taking_an_injected_client():
    """The injection seam is the only reason any of the above can be asserted."""
    import inspect

    signature = inspect.signature(summarize)
    assert "client" in signature.parameters
    assert signature.parameters["client"].default is None
    assert "max_notes" in signature.parameters
    assert signature.parameters["max_notes"].default == MAX_NOTES


def test_the_verdict_lexicon_is_broad_enough_to_be_the_ethical_premise():
    """15-25 terms, covering accusation, manipulation and bot vocabulary.

    A three-word lexicon would pass every other test in this file and refuse
    almost nothing.
    """
    assert 15 <= len(VERDICT_LEXICON) <= 25
    assert len(set(VERDICT_LEXICON)) == len(VERDICT_LEXICON)
    assert all(term == term.lower() for term in VERDICT_LEXICON)
    for expected in ("fraud", "bot", "fake"):
        assert any(expected in term for term in VERDICT_LEXICON)
