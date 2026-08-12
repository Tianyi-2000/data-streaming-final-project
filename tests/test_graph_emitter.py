"""The graph projection's shaping and decide logic, with no broker.

Three properties this file exists to hold, all of which fail silently in
production if they are wrong.

1. THE `_from` / `_to` PREFIXES. ArangoDB does not validate that an edge's
   endpoints resolve, so a wrong prefix produces a graph whose traversals return
   nothing while erroring on nothing. The prefixes are asserted against the
   COLLECTION NAME CONSTANTS rather than against string literals, so this file
   proves they agree rather than restating them in a second place where they
   could drift.

2. THE STOP-BAND BOOLEAN COMES FROM THE CONTRACT. Both edges of the 30-35 band
   are checked inclusive, because Phase 1 measured that an exclusive upper edge
   drops a sixth of the signal and swings the band share from 0.923 to 0.762
   while raising nothing anywhere.

3. THE VERTEX EMIT-ONCE SET CHANGES NO EDGE. It is a volume compression, and the
   test that proves it is the one that runs the same events twice and compares
   the edge stream, not the vertex stream.

No broker and no database. Live-graph assertions belong to
`tests/test_graph_queries.py`.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.play_event_v1 import (  # noqa: E402
    STOP_BAND_HIGH_SECONDS,
    STOP_BAND_LOW_SECONDS,
    PlayEventV1,
    in_stop_band,
)
from src.graph_emitter import (  # noqa: E402
    INVALID_VALUE,
    KEY_MISMATCH,
    LISTENER_PREFIX,
    LISTENERS_COLLECTION,
    PLAYED_COLLECTION,
    TOPIC_GRAPH_LISTENERS,
    TOPIC_GRAPH_PLAYED,
    TOPIC_GRAPH_TRACKS,
    TRACK_PREFIX,
    TRACKS_COLLECTION,
    GraphProjector,
    listener_document,
    played_document,
    replay_records,
    track_document,
)


def make_event(
    *,
    event_id: str = "e-1",
    listener_id: str = "L001",
    track_id: str = "T001",
    artist_id: str = "AR001",
    played_seconds: int = 32,
    track_duration_seconds: int = 210,
    event_time: str = "2026-08-09T20:00:00Z",
) -> PlayEventV1:
    return PlayEventV1(
        event_id=event_id,
        listener_id=listener_id,
        track_id=track_id,
        artist_id=artist_id,
        played_seconds=played_seconds,
        track_duration_seconds=track_duration_seconds,
        event_time=event_time,
    )


def wire(event: PlayEventV1) -> Tuple[bytes, bytes]:
    """The `(key, value)` bytes this event arrives as on `track-activity`.

    Keyed by `track_id`, because that is Consumer 1's output key. Driving the
    tests through the wire form rather than past it means they exercise the same
    decide path the poll loop uses.
    """
    return event.track_id.encode("utf-8"), event.model_dump_json().encode("utf-8")


# --- Document shapes ---------------------------------------------------------


def test_listener_document_is_keyed_by_listener_id():
    event = make_event(listener_id="A000")
    doc = listener_document(event)
    assert doc["_key"] == "A000"
    assert doc["listener_id"] == "A000"


def test_track_document_is_keyed_by_track_id_and_carries_the_artist():
    event = make_event(track_id="T042", artist_id="AR9")
    doc = track_document(event)
    assert doc["_key"] == "T042"
    assert doc["track_id"] == "T042"
    # The artist is a property of the track, not a third vertex collection:
    # GRPH-01 names two vertex collections and adding a third is out of scope.
    assert doc["artist_id"] == "AR9"


def test_edge_document_is_keyed_by_event_id_and_carries_the_payload():
    event = make_event(
        event_id="evt-7",
        played_seconds=33,
        track_duration_seconds=200,
        event_time="2026-08-09T20:15:00Z",
    )
    doc = played_document(event)
    assert doc["_key"] == "evt-7"
    assert doc["event_time"] == "2026-08-09T20:15:00Z"
    assert doc["played_seconds"] == 33
    assert doc["track_duration_seconds"] == 200
    assert doc["artist_id"] == event.artist_id


def test_edge_endpoints_use_the_collection_name_constants():
    """PROPERTY 1. The prefixes are checked against the COLLECTION NAMES.

    Asserting `_from == "listeners/A000"` would restate the prefix in a second
    place and could not catch the two drifting apart, which is the failure this
    check exists for. Building the expectation out of the same constant the
    loader imports means a rename either updates both or fails here.
    """
    event = make_event(listener_id="A000", track_id="T001")
    doc = played_document(event)

    assert doc["_from"] == f"{LISTENERS_COLLECTION}/A000"
    assert doc["_to"] == f"{TRACKS_COLLECTION}/T001"
    assert doc["_from"] == LISTENER_PREFIX + event.listener_id
    assert doc["_to"] == TRACK_PREFIX + event.track_id
    # A prefix must be the collection name and a slash, and nothing else.
    assert LISTENER_PREFIX == LISTENERS_COLLECTION + "/"
    assert TRACK_PREFIX == TRACKS_COLLECTION + "/"


def test_the_loader_imports_the_same_collection_names_rather_than_restating_them():
    """One place a prefix typo can live. Proven by identity, not by reading."""
    from src import graph_loader

    assert graph_loader.LISTENERS_COLLECTION is LISTENERS_COLLECTION
    assert graph_loader.TRACKS_COLLECTION is TRACKS_COLLECTION
    assert graph_loader.PLAYED_COLLECTION is PLAYED_COLLECTION


# --- Property 2: the stop band comes from the contract -----------------------


@pytest.mark.parametrize(
    "played_seconds, expected",
    [
        (STOP_BAND_LOW_SECONDS - 1, False),  # 29
        (STOP_BAND_LOW_SECONDS, True),       # 30, inclusive
        (STOP_BAND_HIGH_SECONDS, True),      # 35, inclusive
        (STOP_BAND_HIGH_SECONDS + 1, False),  # 36
    ],
)
def test_stopped_in_band_matches_the_contract_helper_at_both_edges(
    played_seconds: int, expected: bool
):
    event = make_event(played_seconds=played_seconds, track_duration_seconds=300)
    doc = played_document(event)
    assert doc["stopped_in_band"] is expected
    # And it IS the contract helper's answer, not a local comparison that happens
    # to agree today.
    assert doc["stopped_in_band"] is in_stop_band(played_seconds)


def test_stop_band_boolean_tracks_the_contract_helper_across_the_whole_range():
    """If a local `30 <= s <= 35` were substituted, this would still pass today.

    So the check is deliberately exhaustive rather than illustrative: it pins the
    edge document to `in_stop_band` for every value a play can take around the
    band, which is what makes a future contract change show up here.
    """
    for seconds in range(0, 60):
        event = make_event(played_seconds=seconds, track_duration_seconds=300)
        assert played_document(event)["stopped_in_band"] is in_stop_band(seconds)


# --- Decide: drops rather than raises ----------------------------------------


def test_null_value_is_dropped_as_invalid_value():
    projector = GraphProjector()
    decision = projector.decide(b"T001", None)
    assert decision.project is False
    assert decision.drop_reason == INVALID_VALUE


def test_unparseable_value_is_dropped_as_invalid_value():
    projector = GraphProjector()
    decision = projector.decide(b"T001", b"{not json")
    assert decision.project is False
    assert decision.drop_reason == INVALID_VALUE


def test_key_that_does_not_equal_the_values_track_id_is_dropped():
    projector = GraphProjector()
    event = make_event(track_id="T001")
    _, value = wire(event)
    decision = projector.decide(b"T999", value)
    assert decision.project is False
    assert decision.drop_reason == KEY_MISMATCH


def test_missing_key_is_a_mismatch_not_a_pass():
    projector = GraphProjector()
    _, value = wire(make_event())
    decision = projector.decide(None, value)
    assert decision.project is False
    assert decision.drop_reason == KEY_MISMATCH


def test_one_malformed_record_does_not_stop_the_stream():
    """A drop is a returned decision, never an exception. Threat T-04-08's shape."""
    projector = GraphProjector()
    good = make_event(event_id="e-good")
    records: List[Tuple[Optional[bytes], Optional[bytes]]] = [
        (b"T001", None),
        (b"T001", b"{not json"),
        (b"WRONG", wire(good)[1]),
        wire(good),
    ]
    out = replay_records(projector, records)
    assert [d.project for d, _ in out] == [False, False, False, True]
    counts = projector.counts()
    assert counts["records_seen"] == 4
    assert counts["projected"] == 1
    assert counts["invalid_value"] == 2
    assert counts["key_mismatch"] == 1


def test_drop_vocabulary_matches_the_other_stages():
    """The three stages' counters must mean the same thing read side by side."""
    from src import consumer_stage2

    assert INVALID_VALUE == consumer_stage2.INVALID_VALUE
    assert KEY_MISMATCH == consumer_stage2.KEY_MISMATCH


def test_there_is_no_event_id_dedup_set():
    """A duplicate event is projected AGAIN, and that is correct here.

    Consumer 2 keeps a dedup set because a duplicate inflates its ratio toward a
    false negative. A graph upserted by key is immune: the redelivered event is
    the same edge `_key` and overwrites itself. So this stage must NOT suppress
    it -- suppressing would be harmless but would misrepresent where idempotency
    actually comes from, which is the write path.
    """
    projector = GraphProjector()
    event = make_event(event_id="e-dup")
    out = replay_records(projector, [wire(event), wire(event)])
    assert [d.project for d, _ in out] == [True, True]

    first_edges = [r for r in out[0][1] if r[0] == TOPIC_GRAPH_PLAYED]
    second_edges = [r for r in out[1][1] if r[0] == TOPIC_GRAPH_PLAYED]
    assert len(first_edges) == len(second_edges) == 1
    # Same key, same bytes: the second write replaces the first.
    assert first_edges[0][1] == second_edges[0][1]
    assert first_edges[0][2] == second_edges[0][2]


# --- Property 3: the emit-once set is a volume compression -------------------


def test_vertex_records_are_emitted_once_per_run():
    projector = GraphProjector()
    events = [
        make_event(event_id="e-1", listener_id="L1", track_id="T1"),
        make_event(event_id="e-2", listener_id="L1", track_id="T1"),
        make_event(event_id="e-3", listener_id="L1", track_id="T2"),
        make_event(event_id="e-4", listener_id="L2", track_id="T1"),
    ]
    replay_records(projector, [wire(e) for e in events])
    counts = projector.counts()
    assert counts["listener_records"] == 2   # L1, L2
    assert counts["track_records"] == 2      # T1, T2
    assert counts["edge_records"] == 4       # every event is an edge


def test_suppressing_repeat_vertices_changes_no_edge():
    """PROPERTY 3. The edge stream is identical with and without the set.

    A projector primed so every vertex is already emitted produces the SAME edges
    as a fresh one -- which is what makes the set a volume compression rather
    than a correctness mechanism, and why deleting it would change the topic
    sizes and nothing else.
    """
    events = [
        make_event(event_id=f"e-{i}", listener_id="L1", track_id="T1")
        for i in range(5)
    ]

    fresh = GraphProjector()
    fresh_out = replay_records(fresh, [wire(e) for e in events])
    fresh_edges = [
        r for _, produced in fresh_out for r in produced if r[0] == TOPIC_GRAPH_PLAYED
    ]

    primed = GraphProjector()
    replay_records(primed, [wire(events[0])])  # burn the vertices
    primed_out = replay_records(primed, [wire(e) for e in events])
    primed_edges = [
        r for _, produced in primed_out for r in produced if r[0] == TOPIC_GRAPH_PLAYED
    ]

    assert fresh_edges == primed_edges
    # And the vertex records really were suppressed the second time.
    primed_vertices = [
        r
        for _, produced in primed_out
        for r in produced
        if r[0] in (TOPIC_GRAPH_LISTENERS, TOPIC_GRAPH_TRACKS)
    ]
    assert primed_vertices == []


def test_kafka_keys_match_each_documents_own_key():
    """The wire key is the UTF-8 `_key`, so partitioning follows the graph key."""
    projector = GraphProjector()
    event = make_event(event_id="e-9", listener_id="A000", track_id="T7")
    produced = replay_records(projector, [wire(event)])[0][1]

    by_topic = {topic: (key, value) for topic, key, value in produced}
    assert set(by_topic) == {
        TOPIC_GRAPH_LISTENERS,
        TOPIC_GRAPH_TRACKS,
        TOPIC_GRAPH_PLAYED,
    }
    for topic, (key, value) in by_topic.items():
        document = json.loads(value)
        assert key.decode("utf-8") == document["_key"], topic

    assert by_topic[TOPIC_GRAPH_LISTENERS][0] == b"A000"
    assert by_topic[TOPIC_GRAPH_TRACKS][0] == b"T7"
    assert by_topic[TOPIC_GRAPH_PLAYED][0] == b"e-9"


def test_values_are_deterministic_json():
    """Two runs over the same input must produce byte-identical values."""
    first = replay_records(GraphProjector(), [wire(make_event())])[0][1]
    second = replay_records(GraphProjector(), [wire(make_event())])[0][1]
    assert first == second


# --- Every emitted id must be a legal Arango _key ----------------------------

# ArangoDB's permitted `_key` characters. An id outside this set is rejected by
# the server at write time, which would be an exception rather than a silent
# wrong answer -- but it would be an exception in the middle of a load, so it is
# cheaper to hold the property here.
_LEGAL_ARANGO_KEY = re.compile(r"^[A-Za-z0-9_\-:.@()+,=;$!*'%]{1,254}$")


@pytest.mark.parametrize(
    "listener_id, track_id, event_id",
    [
        ("A000", "6fe6c7a3-2f67-4fa5-9ee3-724f5c57e836", "e-1"),
        ("B0123", "T-42", "3f2504e0-4f89-11d3-9a0c-0305e82c3301"),
    ],
)
def test_every_emitted_key_is_a_legal_arango_key(
    listener_id: str, track_id: str, event_id: str
):
    projector = GraphProjector()
    event = make_event(
        event_id=event_id, listener_id=listener_id, track_id=track_id
    )
    produced = replay_records(projector, [wire(event)])[0][1]
    assert produced, "expected three records"
    for topic, key, value in produced:
        document = json.loads(value)
        assert _LEGAL_ARANGO_KEY.match(document["_key"]), (topic, document["_key"])


def test_the_shipped_stream_uses_only_legal_arango_keys():
    """The real ids, not invented ones. Reads the committed event file if present."""
    events_path = REPO_ROOT / "data" / "play_events.jsonl"
    if not events_path.exists():
        pytest.skip(
            f"no committed event stream at {events_path}; "
            "run python src/generate_events.py to produce it"
        )

    checked = 0
    with events_path.open(encoding="utf-8") as handle:
        for line in handle:
            if checked >= 5000:
                break
            record = json.loads(line)
            for field_name in ("listener_id", "track_id", "event_id"):
                assert _LEGAL_ARANGO_KEY.match(record[field_name]), record[field_name]
            checked += 1
    assert checked > 0


# --- The three topics are the interface --------------------------------------


def test_the_three_topic_names_are_the_ones_the_requirement_names():
    assert TOPIC_GRAPH_LISTENERS == "graph-listeners"
    assert TOPIC_GRAPH_TRACKS == "graph-tracks"
    assert TOPIC_GRAPH_PLAYED == "graph-played"


def test_the_emitter_reads_track_activity_through_the_contract():
    """The input topic name comes from the frozen contract, never a literal."""
    import src.graph_emitter as emitter
    from contracts.play_event_v1 import TOPIC_TRACK_ACTIVITY

    assert emitter.TOPIC_TRACK_ACTIVITY is TOPIC_TRACK_ACTIVITY


def test_consumer_stage2_is_not_imported_or_modified_by_the_projection():
    """GRPH-02 is amended, not obeyed literally: Consumer 2 is untouched.

    The projection must not reach into Consumer 2's processor -- that is what
    makes it zero-risk to five phases of verification. Importing the module for a
    shared drop-reason constant is fine; using its detector is not.
    """
    import src.graph_emitter as emitter

    assert not hasattr(emitter, "Stage2Processor")
    source = (REPO_ROOT / "src" / "graph_emitter.py").read_text(encoding="utf-8")
    assert "Stage2Processor" not in source
    assert "consumer_stage2" not in source.split('"""', 2)[-1]
