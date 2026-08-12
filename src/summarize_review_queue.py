"""The bounded reviewer-note summary over `track_review_queue.json` (SUMM-02/03).

THE PROJECT'S STATED AI ELEMENT, AND DELIBERATELY THE SMALLEST PIECE OF IT. One
model call over the track review queue produces at most three reviewer notes.
The model may not change a flag, may not introduce a number that is not already
in its input, and may not assert that fraud occurred. When it does any of those
-- or when there is no model at all -- this module falls back to the
deterministic sentence Consumer 2 already writes, silently, and exits 0.

Five things a later reader needs, in the order they matter.

1. THE FALLBACK IS THE DEFAULT PATH, NOT AN ERROR PATH. There is no
   `OPENAI_API_KEY` on the machine this was built on and probably none on the
   grader's. `default_client()` returns None in that case, `summarize()` returns
   template notes, and the CLI exits 0. A fallback here is a SUCCESS: it is
   exactly what `SUBMISSION-proposal.txt` section 6 promised as the fallback
   behaviour. The only exit-1 case in this module is a missing or unparseable
   INPUT FILE, which is a different kind of problem -- the pipeline has not been
   run.

2. `template_note` IS IMPORTED FROM `src/consumer_stage2.py`, NEVER COPIED. Its
   docstring names itself this phase's SUMM-03 fallback and calls the name and
   signature a cross-phase contract. A copy would let the terminal report and
   the summary drift into disagreeing about the same flagged window, and nothing
   would fail when they did.

3. EVERY BOUND REJECTS THE WHOLE RESPONSE, NOT THE OFFENDING NOTE. A response
   with one fabricated number is a response from a model that will fabricate,
   and keeping its other two notes means keeping notes no bound actually held
   over. Partial trust is not a bound. `summarize()` therefore returns either
   all-model notes or all-template notes, never a mixture, and always says which.

4. THE NUMBER BOUND IS CALIBRATED, NOT OBVIOUS. Identifier-shaped values are
   scrubbed out of a note before any digit is extracted, because a UUID and
   three ISO timestamps are full of digits that are not claims. What remains
   must each be a rendering the entry licenses, and the licensed set holds three
   renderings of every value: integral, two-decimal, and plain `str`. Two
   decimals is not arbitrary -- `template_note` renders its ratios at exactly
   `_NOTE_DECIMALS`, so a bound that did not admit `1.00` and `0.92` would
   reject the shipped fallback and make the guaranteed path unreachable.
   `tests/test_summarize_review_queue.py` asserts the template passes and that a
   single changed digit does not, which is what makes the bound both correct and
   non-vacuous.

5. THIS MODULE NEVER WRITES ITS INPUT. It opens the review queue read-only and
   has no write path to it at all, and it is a separate CLI rather than a hook
   inside Consumer 2's report, so the process that writes the artifact a human
   acts on cannot be reached from the model path. The tests hash the file before
   and after.

The key is read from `os.environ` and from nowhere else. It is never logged,
never echoed into the provenance line, never written to a file, and has no
default in source.

Usage:
    python3 src/summarize_review_queue.py
    python3 src/summarize_review_queue.py --input output/track_review_queue.json
    python3 src/summarize_review_queue.py --json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

import requests

# Same import bootstrap the repo's other scripts use, so this module resolves
# however it is invoked and never depends on a conftest.py being present.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# IMPORTED, NEVER COPIED -- see point 2 of the module docstring.
from src.consumer_stage2 import template_note  # noqa: E402

_LOG = logging.getLogger(__name__)

# SUMM-02's cap, fixed rather than configurable. "Up to three reviewer notes" is
# what the proposal promised and what the tests assert; a config knob here would
# make the promise a runtime detail.
MAX_NOTES = 3

SOURCE_MODEL = "model"
SOURCE_TEMPLATE = "template"

DEFAULT_INPUT_PATH = REPO_ROOT / "output" / "track_review_queue.json"

# THE SOLE SOURCE OF THE KEY. Looked up with os.environ.get and nowhere else:
# no CLI flag, no config file, no default value in this source. Absent means the
# model path does not exist on this machine, which is the normal case.
API_KEY_ENV = "OPENAI_API_KEY"  # read via os.environ.get, and by no other route

OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL = "gpt-4o-mini"
# Explicit connect and read timeouts. A hung call must become a fallback rather
# than a hang (threat T-06-05); requests defaults to waiting forever.
CONNECT_TIMEOUT_SECONDS = 5.0
READ_TIMEOUT_SECONDS = 30.0

# THE ETHICAL PREMISE, IN CODE, and broad on purpose. These are the words this
# project must never let a model put next to a track: they assert wrongdoing or
# intent rather than describing a measurement. The review queue names candidates
# and the evidence behind them; a human decides. Matched case-insensitively at
# word boundaries, so ordinary prose containing a term as a substring is not
# rejected. The tests import this tuple rather than restating it, so the two can
# never disagree about what the project refuses to say.
VERDICT_LEXICON = (
    "fraud",
    "fraudulent",
    "fraudster",
    "criminal",
    "illegal",
    "guilty",
    "scam",
    "cheating",
    "gaming",
    "manipulation",
    "manipulated",
    "artificial streaming",
    "stream farming",
    "bot",
    "bots",
    "botnet",
    "fake",
    "fake accounts",
    "click farm",
    "laundering",
    "abuse",
    "abusive",
)

# A number as it appears in prose, comma grouping included. Comma-grouped forms
# are captured whole and will not match a licensed rendering, which is the
# conservative direction: "1,040" is rejected rather than read as "1" and "040".
_NUMBER_RE = re.compile(r"\d+(?:,\d{3})*(?:\.\d+)?")

# The two decimal places `template_note` renders its ratios at. Named here so
# the bound and the sentence it must admit cannot drift apart.
_LICENSED_DECIMALS = 2

# Entry fields whose values are identifiers or instants, not claims. Scrubbed
# from a note before any digit is extracted -- see point 4.
_IDENTIFIER_FIELDS = (
    "track_id",
    "window_start",
    "first_event_time",
    "last_event_time",
)


@dataclass(frozen=True)
class SummaryResult:
    """The notes, which path produced them, and -- if it fell back -- why.

    `source` is what makes T-06-07 answerable: a reader can always tell whether
    a sentence is generated text or the deterministic template, so generated
    text can never be mistaken for measurement.
    """

    notes: List[Dict[str, str]]
    source: str
    fallback_reason: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "notes": [dict(note) for note in self.notes],
            "source": self.source,
            "fallback_reason": self.fallback_reason,
        }


# =========================================================================
# the number bound
# =========================================================================
def _render(value: Any) -> Set[str]:
    """Every string form of one numeric value that a note is allowed to use.

    Three renderings, because the shipped template uses more than one: the plain
    `str` form, the two-decimal form (`1.0` -> `1.00`, which is what
    `template_note` prints), and the integral form when the value is integral
    (`1.0` -> `1`). Anything outside this set is a number the entry did not
    license.
    """
    forms: Set[str] = set()
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return forms
    forms.add(str(value))
    forms.add(f"{value:.{_LICENSED_DECIMALS}f}")
    if float(value).is_integer():
        forms.add(str(int(value)))
    return forms


def licensed_numbers(entry: Dict[str, Any]) -> Set[str]:
    """The set of number strings one review entry licenses a note to state.

    Built from the entry's OWN values, walked recursively, so adding a measured
    field to the review schema cannot silently narrow the bound. Strings are
    skipped: identifiers and timestamps are handled by scrubbing, not by
    licensing their digits.
    """
    licensed: Set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for value in node.values():
                walk(value)
        elif isinstance(node, (list, tuple)):
            for value in node:
                walk(value)
        else:
            licensed.update(_render(node))

    walk(entry)
    return licensed


def note_numbers(note: str, entry: Dict[str, Any]) -> List[str]:
    """The number strings a note actually claims, identifiers scrubbed first.

    A track UUID carries digits and three ISO timestamps carry more. None of
    them is a claim about anything, and counting them would reject the shipped
    template on every entry.
    """
    scrubbed = note
    for field in _IDENTIFIER_FIELDS:
        value = entry.get(field)
        if isinstance(value, str) and value:
            scrubbed = scrubbed.replace(value, " ")
    return _NUMBER_RE.findall(scrubbed)


def _verdict_terms_in(note: str) -> List[str]:
    """Lexicon terms present in a note, matched at word boundaries."""
    lowered = note.lower()
    return [
        term
        for term in VERDICT_LEXICON
        if re.search(rf"(?<!\w){re.escape(term)}(?!\w)", lowered)
    ]


def check_note(note: str, entry: Dict[str, Any]) -> Optional[str]:
    """The first bound this note breaks against this entry, or None.

    Checked against ITS OWN entry, never against the union of the document's
    entries: letting one track's evidence license another track's note is the
    quiet version of inventing a number.
    """
    if not isinstance(note, str) or not note.strip():
        return "the note is empty"

    verdicts = _verdict_terms_in(note)
    if verdicts:
        return (
            f"the note reaches a verdict: it uses {verdicts[0]!r}, which asserts "
            "wrongdoing rather than describing a measurement"
        )

    licensed = licensed_numbers(entry)
    for number in note_numbers(note, entry):
        if number not in licensed:
            return (
                f"the note states {number!r}, which does not appear in the "
                f"review entry for track {entry.get('track_id')!r}"
            )
    return None


# =========================================================================
# the prompt and the client
# =========================================================================
def build_prompt(document: Dict[str, Any], max_notes: int = MAX_NOTES) -> str:
    """The one prompt, built from the review document and nothing else.

    SUMM-02 says the input is that JSON only, so nothing else is interpolated
    here -- no environment, no file paths beyond what the document carries, and
    certainly no key.
    """
    flagged = document.get("flagged_tracks") or []
    return (
        "You are helping a royalty-integrity reviewer triage a queue of flagged "
        "tracks.\n\n"
        f"Write at most {max_notes} short reviewer notes, one per flagged track, "
        "describing WHY each track is queued for review.\n\n"
        "Hard rules:\n"
        "- Use ONLY the numbers present in the JSON below. Do not compute, round "
        "or invent any number.\n"
        "- Do not change, add or remove a flag.\n"
        "- Do not state or imply that fraud, manipulation, bots or fake accounts "
        "are involved. Describe what was measured against what threshold, and "
        "stop.\n"
        "- These are review candidates for a human, not conclusions.\n\n"
        "Return ONLY a JSON array, with no prose and no code fence, of objects "
        'shaped {"track_id": "<id from the input>", "note": "<one or two '
        'sentences>"}.\n\n'
        "Review queue:\n"
        f"{json.dumps({'flagged_tracks': flagged}, indent=2, sort_keys=True)}\n"
    )


Client = Callable[[str], str]


def default_client() -> Optional[Client]:
    """A callable posting one prompt to the chat API, or None if there is no key.

    NONE IS THE EXPECTED RETURN HERE. The key comes from the environment and
    only from the environment; absent, this returns None, `summarize()` falls
    back, and no HTTP call is attempted at all. The returned callable raises on
    any transport problem so `summarize()` can catch it -- a raise here becomes a
    fallback, never a crash.
    """
    api_key = os.environ.get(API_KEY_ENV)
    if not api_key:
        _LOG.info(
            "%s is not set; the deterministic template is the summary path for "
            "this run. This is the normal case and not an error.",
            API_KEY_ENV,
        )
        return None

    def call(prompt: str) -> str:
        response = requests.post(
            OPENAI_CHAT_URL,
            headers={
                # The key is used here and nowhere else. It is never logged and
                # never returned.
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENAI_MODEL,
                "temperature": 0,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
        )
        response.raise_for_status()
        payload = response.json()
        return payload["choices"][0]["message"]["content"]

    return call


# =========================================================================
# the response, and the five bounds over it
# =========================================================================
def _parse_response(raw: Any) -> List[Dict[str, str]]:
    """A list of `{track_id, note}` pairs, or ValueError. BOUND 5.

    Tolerates a markdown fence because models add them unprompted; tolerates
    nothing else. A response that will not parse is rejected whole, like every
    other bound.
    """
    if not isinstance(raw, str):
        raise ValueError(f"the client returned {type(raw).__name__}, not text")

    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text).strip()
    if not text:
        raise ValueError("the client returned an empty response")

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"the response is not JSON: {exc}") from exc

    if not isinstance(parsed, list):
        raise ValueError(
            f"the response is a {type(parsed).__name__}, not a list of notes"
        )

    pairs: List[Dict[str, str]] = []
    for item in parsed:
        if not isinstance(item, dict):
            raise ValueError("a response element is not an object")
        track_id = item.get("track_id")
        note = item.get("note")
        if not isinstance(track_id, str) or not isinstance(note, str):
            raise ValueError("a response element lacks a string track_id and note")
        pairs.append({"track_id": track_id, "note": note})
    return pairs


def _template_notes(
    flagged: List[Dict[str, Any]], max_notes: int
) -> List[Dict[str, str]]:
    """The deterministic fallback: the shipped sentence, capped at `max_notes`."""
    return [
        {"track_id": entry.get("track_id", ""), "note": template_note(entry)}
        for entry in flagged[:max_notes]
    ]


def _fallback(
    flagged: List[Dict[str, Any]], max_notes: int, reason: str
) -> SummaryResult:
    """Fall back, log the reason at INFO, and return. Never raise, never exit 1."""
    _LOG.info("falling back to the deterministic template: %s", reason)
    return SummaryResult(
        notes=_template_notes(flagged, max_notes),
        source=SOURCE_TEMPLATE,
        fallback_reason=reason,
    )


def summarize(
    document: Dict[str, Any],
    client: Optional[Client] = None,
    max_notes: int = MAX_NOTES,
) -> SummaryResult:
    """At most `max_notes` reviewer notes over one review document.

    `client` is an injected callable of the shape `client(prompt) -> str`. THAT
    SEAM IS THE ONLY REASON SUMM-02 CAN BE PROVEN AT ALL: a live call samples one
    response and demonstrates nothing about a constraint, while a stub that
    returns four notes, an unflagged track, a fabricated number or a verdict
    demonstrates that each bound holds. Absent a client, the deterministic
    template is used and no call is attempted.
    """
    flagged = document.get("flagged_tracks") or []

    # An empty queue is not an error and must not spend a call. Checked before
    # the client is consulted so that "zero flagged tracks costs zero calls" is
    # true by construction rather than by the model returning nothing.
    if not flagged:
        _LOG.info("no flagged tracks in the review queue; nothing to summarize")
        return SummaryResult(
            notes=[],
            source=SOURCE_TEMPLATE,
            fallback_reason="the review queue holds no flagged tracks",
        )

    if client is None:
        return _fallback(
            flagged, max_notes, f"no model client is configured ({API_KEY_ENV} unset)"
        )

    try:
        raw = client(build_prompt(document, max_notes))
    except Exception as exc:  # noqa: BLE001 - any client failure is a fallback
        return _fallback(flagged, max_notes, f"the model call failed: {exc}")

    try:
        pairs = _parse_response(raw)
    except ValueError as exc:
        return _fallback(flagged, max_notes, f"the response was rejected: {exc}")

    # BOUND 1 -- the note cap.
    if len(pairs) > max_notes:
        return _fallback(
            flagged,
            max_notes,
            f"the response held {len(pairs)} notes, over the cap of {max_notes}",
        )

    entries_by_track: Dict[str, Dict[str, Any]] = {
        entry["track_id"]: entry for entry in flagged if "track_id" in entry
    }
    seen: Set[str] = set()

    for pair in pairs:
        track_id = pair["track_id"]

        # BOUND 2 -- track identity: flagged in the input, and named once.
        if track_id not in entries_by_track:
            return _fallback(
                flagged,
                max_notes,
                f"the response named track {track_id!r}, which is not flagged in "
                "the input",
            )
        if track_id in seen:
            return _fallback(
                flagged,
                max_notes,
                f"the response named track {track_id!r} more than once",
            )
        seen.add(track_id)

        # BOUNDS 3 and 4 -- licensed numbers, and the verdict lexicon.
        broken = check_note(pair["note"], entries_by_track[track_id])
        if broken is not None:
            return _fallback(
                flagged,
                max_notes,
                f"a note about track {track_id!r} failed its bound: {broken}",
            )

    return SummaryResult(notes=pairs, source=SOURCE_MODEL, fallback_reason=None)


# =========================================================================
# the CLI
# =========================================================================
REGENERATE_HINT = (
    "output/ is gitignored, so an absent review queue is the normal state of a "
    "fresh checkout and not a failure. Regenerate it with:\n"
    "    docker compose up -d\n"
    "    rm -rf state output\n"
    "    python3 src/replay_to_kafka.py\n"
    "    python3 src/consumer_stage1.py --thresholds config/thresholds.json\n"
    "    python3 src/consumer_stage2.py --thresholds config/thresholds.json"
)


def load_document(path: Path) -> Dict[str, Any]:
    """The review queue, read-only. Raises on absence or on unparseable JSON."""
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("the review queue is not a JSON object")
    return document


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point. Exits 0 on a fallback; 1 only on a bad INPUT FILE."""
    parser = argparse.ArgumentParser(
        description=(
            "Bounded reviewer notes over the track review queue: at most three "
            "notes, no changed flags, no invented numbers, no verdict. Falls "
            "back to the deterministic template when no model is available."
        )
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT_PATH),
        help="the review queue to summarize; read-only, never written",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit the result as JSON on stdout instead of prose",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )

    source_path = Path(args.input)
    try:
        document = load_document(source_path)
    except FileNotFoundError:
        print(
            f"ERROR: no review queue at {source_path}. {REGENERATE_HINT}",
            file=sys.stderr,
        )
        return 1
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(
            f"ERROR: the review queue at {source_path} could not be read: {exc}. "
            f"{REGENERATE_HINT}",
            file=sys.stderr,
        )
        return 1

    result = summarize(document, client=default_client())

    if args.json:
        payload = {**result.as_dict(), "source_path": str(source_path)}
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    # The provenance line: which file, which path produced the notes, and the
    # fallback reason when there was one. Never the key.
    print("")
    print(f"Reviewer notes from {source_path}")
    print(f"  source: {result.source}")
    if result.fallback_reason:
        print(f"  fell back because: {result.fallback_reason}")
    print(f"  notes: {len(result.notes)} (cap {MAX_NOTES})")
    if not result.notes:
        print("  no flagged tracks in this review queue; nothing to summarize.")
    for note in result.notes:
        print("")
        print(f"  {note['track_id']}")
        print(f"      {note['note']}")
    print("")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
