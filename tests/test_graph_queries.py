"""The two cohort queries: construction without a database, results with one.

WHAT THIS FILE IS GUARDING, because both failures return plausible numbers.

FIRST, `UNIQUE` ON BOTH SIDES OF THE CO-LISTENING QUERY. The graph holds 45,473
edges over 13,985 distinct (listener, track) pairs -- a mean of 3.25 plays per
pair. A query that groups peers and takes `COUNT` is counting EDGES, so every
overlap comes back roughly threefold: a real 438 reads as ~1,400, which looks
like a perfectly reasonable answer and is wrong. The construction test asserts
`UNIQUE` is present rather than trusting a reviewer to notice its absence, and
the live thresholds are calibrated on measured DISTINCT-track overlaps so an
inflated count fails rather than passes.

SECOND, THE SEEDS ARE READ, NEVER TYPED. Both come from the shipped review-queue
artifacts, so the graph layer and the keyed pipeline cannot disagree about which
listener and which track were flagged. The tests prove this by pointing the
readers at temporary queues naming DIFFERENT ids and asserting the seeds follow.

THIRD, 928 IS NOT A CONTRADICTION OF 901 AND MUST NOT BE "FIXED" INTO ONE. The
review queue's 901 is one event-time hour, because Consumer 2's rule is a
one-hour bucket. The graph is unwindowed and covers the whole stream, so it sees
928 distinct inbound listeners: 900 Topology B, 20 normal, 8 Topology A. The test
asserts CONTAINMENT -- every one of the 900 single-play listeners is inbound to
the flagged track -- rather than equality of two totals that measure different
things. Asserting equality would force someone to break a correct query.

FOURTH, NO VERDICT LANGUAGE. `VERDICT_LEXICON` is IMPORTED from
`src/summarize_review_queue.py`, never restated, so this layer inherits the bound
the review queue and the summary already carry. The report names 900 accounts at
once; it is the most dangerous artifact this project produces.

Live-graph tests SKIP with a named regeneration command when the database is
absent, the pattern this project already uses for full-scale artifacts.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.graph_emitter import (  # noqa: E402
    LISTENERS_COLLECTION,
    PLAYED_COLLECTION,
    TRACKS_COLLECTION,
)
from src.graph_queries import (  # noqa: E402
    REGENERATE_HINT,
    build_baseline_query,
    build_co_listening_query,
    build_fan_in_query,
    read_seed_listener,
    read_seed_track,
    serialize_report,
    verdict_terms_in,
)
from src.summarize_review_queue import VERDICT_LEXICON  # noqa: E402

# The eight flagged listeners, read from the artifact rather than typed, so this
# file does not hardcode an identifier either.
LISTENER_QUEUE = REPO_ROOT / "output" / "listener_review_queue.json"
TRACK_QUEUE = REPO_ROOT / "output" / "track_review_queue.json"


# --- Query construction, no database -----------------------------------------


def test_co_listening_counts_distinct_tracks_not_edges():
    """THE CORRECTNESS OF THE WHOLE QUERY. A plain COUNT inflates ~3.25x."""
    query, _ = build_co_listening_query(seed="X", min_shared=100)
    normalized = " ".join(query.split())

    # UNIQUE on the counting side: the peer's shared total is the SIZE OF THE
    # UNIQUE SET of tracks, not the number of edges in its group.
    assert "UNIQUE" in normalized
    assert re.search(r"(LENGTH|COUNT)\s*\(\s*UNIQUE\s*\(", normalized), normalized

    # And a bare COLLECT ... WITH COUNT INTO would be counting edges.
    assert "WITH COUNT INTO" not in normalized, (
        "a COLLECT ... WITH COUNT INTO counts EDGES, which inflates every "
        "overlap by roughly 3.25x and still returns a plausible number"
    )


def test_co_listening_uses_unique_on_the_seed_side_too():
    """Both sides. The seed's own track set must be distinct tracks as well."""
    query, _ = build_co_listening_query(seed="X", min_shared=100)
    assert len(re.findall(r"UNIQUE", query)) >= 2, query


def test_fan_in_counts_distinct_tracks_for_each_inbound_listener():
    query, _ = build_fan_in_query(seed="T")
    normalized = " ".join(query.split())
    assert "UNIQUE" in normalized
    # The cohort's signature is one play AND one DISTINCT track in the whole
    # graph, so the per-listener track count cannot be an edge count.
    assert re.search(r"(LENGTH|COUNT)\s*\(\s*UNIQUE\s*\(", normalized), normalized


def test_both_queries_pass_their_seeds_as_bind_parameters():
    """A seed interpolated into the query string fails this test."""
    co_query, co_binds = build_co_listening_query(seed="SEED_L", min_shared=100)
    assert "SEED_L" not in co_query
    assert "SEED_L" in json.dumps(co_binds)

    fan_query, fan_binds = build_fan_in_query(seed="SEED_T")
    assert "SEED_T" not in fan_query
    assert "SEED_T" in json.dumps(fan_binds)


def test_the_threshold_is_a_bind_parameter_too():
    _, binds = build_co_listening_query(seed="X", min_shared=137)
    assert 137 in binds.values()


def test_seed_vertex_ids_are_built_from_the_collection_constants():
    """A `_from` prefix typo here returns an empty cohort and errors on nothing."""
    _, binds = build_co_listening_query(seed="A000", min_shared=100)
    assert f"{LISTENERS_COLLECTION}/A000" in json.dumps(binds)

    _, fan_binds = build_fan_in_query(seed="T1")
    assert f"{TRACKS_COLLECTION}/T1" in json.dumps(fan_binds)


def test_queries_traverse_the_played_edge_collection():
    for query, _ in (
        build_co_listening_query(seed="X", min_shared=1),
        build_fan_in_query(seed="T"),
        build_baseline_query(sample_size=10, exclude=["A000"]),
    ):
        assert PLAYED_COLLECTION in query


# --- Seeds are read, never typed ---------------------------------------------


def test_seeds_are_read_from_the_shipped_artifacts():
    if not LISTENER_QUEUE.exists() or not TRACK_QUEUE.exists():
        pytest.skip(f"review queues absent. {REGENERATE_HINT}")

    listener = read_seed_listener(LISTENER_QUEUE)
    track = read_seed_track(TRACK_QUEUE)

    expected_listener = json.loads(LISTENER_QUEUE.read_text())["flagged_listeners"][0][
        "listener_id"
    ]
    expected_track = json.loads(TRACK_QUEUE.read_text())["flagged_tracks"][0]["track_id"]
    assert listener == expected_listener
    assert track == expected_track


def test_a_different_listener_queue_changes_the_seed(tmp_path):
    """Proves the seed is READ. A hardcoded id would ignore this file."""
    queue = tmp_path / "listener_review_queue.json"
    queue.write_text(
        json.dumps({"flagged_listeners": [{"listener_id": "ZZZ-not-a-real-id"}]})
    )
    assert read_seed_listener(queue) == "ZZZ-not-a-real-id"


def test_a_different_track_queue_changes_the_seed(tmp_path):
    queue = tmp_path / "track_review_queue.json"
    queue.write_text(
        json.dumps({"flagged_tracks": [{"track_id": "QQQ-not-a-real-track"}]})
    )
    assert read_seed_track(queue) == "QQQ-not-a-real-track"


def test_no_seed_identifier_is_hardcoded_in_the_module():
    """An AST scan, so the docstring may discuss the ids without failing this."""
    import ast

    source = (REPO_ROOT / "src" / "graph_queries.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    literals = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]
    real_listener = json.loads(LISTENER_QUEUE.read_text())["flagged_listeners"][0][
        "listener_id"
    ]
    real_track = json.loads(TRACK_QUEUE.read_text())["flagged_tracks"][0]["track_id"]

    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.ClassDef)):
            doc = ast.get_docstring(node)
            if doc:
                docstrings.add(doc)

    for literal in literals:
        if literal in docstrings:
            continue
        assert literal.strip() != real_listener, "seed listener is hardcoded"
        assert literal.strip() != real_track, "seed track is hardcoded"


def test_a_missing_review_queue_names_the_regenerating_command(tmp_path):
    missing = tmp_path / "nope.json"
    with pytest.raises(FileNotFoundError) as caught:
        read_seed_listener(missing)
    assert REGENERATE_HINT in str(caught.value)


def test_an_unparseable_review_queue_names_the_regenerating_command(tmp_path):
    broken = tmp_path / "broken.json"
    broken.write_text("{not json")
    with pytest.raises(ValueError) as caught:
        read_seed_track(broken)
    assert REGENERATE_HINT in str(caught.value)


def test_an_empty_review_queue_is_an_error_not_a_silent_none(tmp_path):
    empty = tmp_path / "empty.json"
    empty.write_text(json.dumps({"flagged_listeners": []}))
    with pytest.raises(ValueError):
        read_seed_listener(empty)


# --- Determinism and the no-verdict bound ------------------------------------


def test_the_report_serializer_is_deterministic_and_sorted():
    doc = {"b": 2, "a": {"z": 1, "y": [3, 2, 1]}}
    first = serialize_report(doc)
    second = serialize_report(doc)
    assert first == second
    assert first.endswith("\n")
    # Sorted keys, matching write_review_queue's idiom.
    assert first.index('"a"') < first.index('"b"')
    assert json.loads(first) == doc


def test_the_lexicon_is_imported_rather_than_restated():
    """The three layers must not be able to drift about what they refuse to say."""
    import src.graph_queries as gq
    from src import summarize_review_queue

    assert gq.VERDICT_LEXICON is summarize_review_queue.VERDICT_LEXICON


def test_a_clean_report_carries_no_verdict_term():
    doc = {
        "posture": "these are review candidates carrying the numbers behind them",
        "cohort": [{"listener": "A000", "shared_distinct_tracks": 452}],
    }
    assert verdict_terms_in(doc) == []


@pytest.mark.parametrize("term", ["fraud", "bots", "fake", "manipulation"])
def test_the_lexicon_check_is_not_vacuous(term):
    """A deliberately constructed report containing a term IS rejected.

    Without this, a check that silently matched nothing would pass forever.
    """
    assert term in VERDICT_LEXICON
    doc = {"note": f"this cohort is the result of {term}"}
    assert verdict_terms_in(doc) == [term]


def test_the_lexicon_check_matches_at_word_boundaries():
    """Ordinary prose containing a term as a substring is not rejected.

    'gaming' is in the lexicon; 'programming' must not trip it.
    """
    assert "gaming" in VERDICT_LEXICON
    assert verdict_terms_in({"note": "the programming interface"}) == []


# --- Live graph. Skipped, with a named command, when absent ------------------


def _graph_or_skip():
    """The live database, or a skip naming exactly how to rebuild it."""
    try:
        from arango import ArangoClient
    except ImportError:  # pragma: no cover
        pytest.skip("python-arango is not installed")

    from src.graph_loader import DEFAULT_DATABASE, DEFAULT_ENDPOINT, credentials

    username, password = credentials()
    try:
        client = ArangoClient(hosts=DEFAULT_ENDPOINT)
        db = client.db(DEFAULT_DATABASE, username=username, password=password)
        db.collection(PLAYED_COLLECTION).count()
    except Exception as exc:  # noqa: BLE001 - any failure means "not available"
        pytest.skip(
            f"no graph at {DEFAULT_ENDPOINT}/{DEFAULT_DATABASE} ({exc}). "
            "Rebuild it with:\n"
            "    docker compose --profile graph up -d\n"
            "    python3 src/graph_emitter.py --group graph-emitter-full\n"
            "    python3 src/graph_loader.py --drop --group graph-loader-full"
        )
    return db


@pytest.fixture(scope="module")
def graph():
    return _graph_or_skip()


@pytest.fixture(scope="module")
def seed_listener():
    if not LISTENER_QUEUE.exists():
        pytest.skip(f"listener review queue absent. {REGENERATE_HINT}")
    return read_seed_listener(LISTENER_QUEUE)


@pytest.fixture(scope="module")
def flagged_listeners():
    if not LISTENER_QUEUE.exists():
        pytest.skip(f"listener review queue absent. {REGENERATE_HINT}")
    doc = json.loads(LISTENER_QUEUE.read_text())
    return [entry["listener_id"] for entry in doc["flagged_listeners"]]


@pytest.fixture(scope="module")
def seed_track():
    if not TRACK_QUEUE.exists():
        pytest.skip(f"track review queue absent. {REGENERATE_HINT}")
    return read_seed_track(TRACK_QUEUE)


def test_the_graph_holds_the_whole_stream(graph):
    assert graph.collection(LISTENERS_COLLECTION).count() == 1308
    assert graph.collection(TRACKS_COLLECTION).count() == 464
    assert graph.collection(PLAYED_COLLECTION).count() == 45473


def test_no_edge_endpoint_dangles(graph):
    """Arango validates nothing here, so 0 is asserted rather than assumed."""
    from src.graph_loader import count_dangling_edges

    assert count_dangling_edges(graph) == 0


def test_co_listening_returns_exactly_the_other_seven_flagged_listeners(
    graph, seed_listener, flagged_listeners
):
    query, binds = build_co_listening_query(seed=seed_listener, min_shared=100)
    rows = list(graph.aql.execute(query, bind_vars=binds))

    peers = [row["listener_id"] for row in rows]
    assert len(peers) == 7, f"expected 7 peers over the threshold, got {peers}"
    assert set(peers) == set(flagged_listeners) - {seed_listener}


def test_each_flagged_peer_shares_at_least_438_distinct_tracks(graph, seed_listener):
    """438 is the MEASURED pairwise floor. ~1,400 would mean UNIQUE went missing."""
    query, binds = build_co_listening_query(seed=seed_listener, min_shared=100)
    rows = list(graph.aql.execute(query, bind_vars=binds))

    for row in rows:
        assert row["shared_distinct_tracks"] >= 438, row
        # And an upper bound, because an edge count would sail past this while
        # still clearing the floor above.
        assert row["shared_distinct_tracks"] <= 464, (
            f"{row} exceeds the total number of tracks in the graph -- this is "
            "what counting edges instead of distinct tracks looks like"
        )


def test_no_listener_outside_the_flagged_set_comes_close(
    graph, seed_listener, flagged_listeners
):
    """THE ASSERTION THAT MAKES THIS A MEASUREMENT RATHER THAN A THRESHOLD CHOICE.

    Any cut in (25, 438] separates the cohort perfectly, so the separation is a
    property of the data rather than of a number someone picked.
    """
    query, binds = build_co_listening_query(seed=seed_listener, min_shared=1)
    rows = list(graph.aql.execute(query, bind_vars=binds))

    outside = [r for r in rows if r["listener_id"] not in flagged_listeners]
    assert outside, "expected some normal listeners to share at least one track"
    worst = max(r["shared_distinct_tracks"] for r in outside)
    assert worst <= 25, f"a non-flagged listener shares {worst} distinct tracks"


def test_fan_in_returns_928_inbound_listeners(graph, seed_track):
    """928, NOT 901. The graph is unwindowed; 901 is one event-time hour."""
    query, binds = build_fan_in_query(seed=seed_track)
    rows = list(graph.aql.execute(query, bind_vars=binds))
    assert len(rows) == 928


def test_exactly_900_inbound_listeners_have_one_play_and_one_track(graph, seed_track):
    """The Topology B signature is the ABSENCE of other activity, not overlap."""
    query, binds = build_fan_in_query(seed=seed_track)
    rows = list(graph.aql.execute(query, bind_vars=binds))

    cohort = [
        r for r in rows if r["total_plays"] == 1 and r["distinct_tracks"] == 1
    ]
    assert len(cohort) == 900


def test_the_900_are_contained_in_the_928_rather_than_equal_to_901(
    graph, seed_track
):
    """CONTAINMENT, NOT EQUALITY -- see point three of the module docstring.

    Asserting 928 == 901 would be asserting that a correct query is broken, and
    would push someone into "fixing" it until it returned a wrong number.
    """
    query, binds = build_fan_in_query(seed=seed_track)
    rows = list(graph.aql.execute(query, bind_vars=binds))

    inbound = {r["listener_id"] for r in rows}
    cohort = {
        r["listener_id"]
        for r in rows
        if r["total_plays"] == 1 and r["distinct_tracks"] == 1
    }
    assert cohort <= inbound
    assert len(cohort) == 900
    assert len(inbound) == 928

    windowed = json.loads(TRACK_QUEUE.read_text())["flagged_tracks"][0]
    assert windowed["unique_listeners"] == 901
    # The unwindowed graph sees strictly more than the one-hour bucket did.
    assert len(inbound) > windowed["unique_listeners"]


def test_the_normal_pairwise_baseline_is_far_below_the_cohort(
    graph, flagged_listeners
):
    """A cohort with no baseline is a threshold choice rather than a finding."""
    query, binds = build_baseline_query(sample_size=60, exclude=flagged_listeners)
    rows = list(graph.aql.execute(query, bind_vars=binds))
    assert rows, "baseline sample returned nothing"

    overlaps = [r["shared_distinct_tracks"] for r in rows]
    mean = sum(overlaps) / len(overlaps)
    assert mean < 5, f"normal pairwise mean {mean} is not a normal baseline"
    assert max(overlaps) <= 25
