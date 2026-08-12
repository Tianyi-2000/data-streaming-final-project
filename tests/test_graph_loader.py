"""The loader's contract with the driver, proven without a database.

THE CENTRAL TEST IN THIS FILE IS THE `raise_on_document_error` ONE, and it is
worth saying why in the file rather than only in the module it guards. In
python-arango 8.3.3, `insert_many` takes `raise_on_document_error=False` BY
DEFAULT and returns per-document failures as error OBJECTS inside the list it
hands back. A loader written the obvious way -- call `insert_many`, do not look
at the return -- therefore completes with no exception, reports a clean run, and
may have written nothing at all. A failed load and a successful load are
indistinguishable from the outside.

That is a repudiation threat rather than a bug: the artifact would claim work
that never happened. So two things are asserted here. That the keyword is passed
with the value that makes failures raise, and that a document the driver rejects
propagates as an EXCEPTION rather than as an element of a list. The first alone
would pass if a future driver changed the keyword's meaning; the second alone
would pass against a fake that raises for its own reasons. Together they pin the
behaviour the loader depends on.

The default itself is asserted too, against the real installed driver. If a later
version changes it, this file says so immediately rather than leaving the
loader's explicit `True` looking like redundant belt-and-braces that someone
might tidy away.

No broker and no database anywhere in this file.
"""

from __future__ import annotations

import inspect
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
    TOPIC_GRAPH_LISTENERS,
    TOPIC_GRAPH_PLAYED,
    TOPIC_GRAPH_TRACKS,
    TRACKS_COLLECTION,
)
from src.graph_loader import (  # noqa: E402
    COLLECTION_FOR_TOPIC,
    DANGLING_EDGES_AQL,
    DEFAULT_ENDPOINT,
    GRAPH_NAME,
    _decode,
    count_dangling_edges,
    graph_state,
    upsert_documents,
)


class FakeCollection:
    """Records the keyword arguments `insert_many` was called with.

    Deliberately NOT a mock with a loose signature: the point of the test is the
    exact keywords, so the fake takes them explicitly and would fail on a call
    that omitted one.
    """

    def __init__(self, name: str = "listeners", reject: bool = False):
        self.name = name
        self.calls: List[Dict[str, Any]] = []
        self._reject = reject

    def insert_many(
        self,
        documents,
        overwrite_mode=None,
        raise_on_document_error=False,
        **kwargs,
    ):
        self.calls.append(
            {
                "documents": list(documents),
                "overwrite_mode": overwrite_mode,
                "raise_on_document_error": raise_on_document_error,
                **kwargs,
            }
        )
        if self._reject:
            # This fake models the DRIVER's own behaviour: with the flag on it
            # raises; with the flag off it hands the failure back as an object
            # in the result list. Both branches are exercised below.
            error = RuntimeError("unique constraint violated for _key 'L1'")
            if raise_on_document_error:
                raise error
            return [error]
        return [{"_key": d["_key"]} for d in documents]


DOCS = [{"_key": "L1", "listener_id": "L1"}, {"_key": "L2", "listener_id": "L2"}]


# --- The two load-bearing keywords -------------------------------------------


def test_insert_many_is_called_with_replace_semantics():
    """GRPH-04's mechanism. A redelivered record must overwrite, not duplicate."""
    collection = FakeCollection()
    written = upsert_documents(collection, DOCS)

    assert written == len(DOCS)
    assert len(collection.calls) == 1
    assert collection.calls[0]["overwrite_mode"] == "replace"


def test_insert_many_is_called_with_document_errors_raising():
    collection = FakeCollection()
    upsert_documents(collection, DOCS)
    assert collection.calls[0]["raise_on_document_error"] is True


def test_a_rejected_document_propagates_as_an_exception():
    """THE POINT OF THIS FILE. Not returned in a list -- raised."""
    collection = FakeCollection(reject=True)
    with pytest.raises(Exception) as caught:
        upsert_documents(collection, DOCS)
    assert "unique constraint" in str(caught.value)


def test_the_check_is_not_vacuous_a_driver_returning_errors_still_fails():
    """Belt and braces: if a future driver ignored the keyword, this still fails.

    The fake here accepts the keyword and returns the error object anyway --
    which is what a driver whose contract changed would do. The loader inspects
    the return value as well as passing the keyword, so a silent partial write is
    caught either way.
    """

    class IgnoresTheKeyword(FakeCollection):
        def insert_many(self, documents, **kwargs):
            super().insert_many(documents, **kwargs)
            return [RuntimeError("write failed for _key 'L1'")]

    with pytest.raises(RuntimeError) as caught:
        upsert_documents(IgnoresTheKeyword(), DOCS)
    assert "error objects rather than raised" in str(caught.value)


def test_the_drivers_default_really_is_false():
    """The reason the loader passes the keyword explicitly, asserted not assumed.

    If a later python-arango flips this default, the loader's explicit `True`
    stops being load-bearing and this test is where that is noticed -- rather
    than someone deleting the keyword as redundant.
    """
    from arango.collection import StandardCollection

    signature = inspect.signature(StandardCollection.insert_many)
    assert signature.parameters["raise_on_document_error"].default is False
    # And `overwrite_mode` has no default either, so replace semantics cannot be
    # inherited by accident.
    assert signature.parameters["overwrite_mode"].default is None


def test_an_empty_batch_is_not_a_write():
    collection = FakeCollection()
    assert upsert_documents(collection, []) == 0
    assert collection.calls == []


# --- The dangling-edge check -------------------------------------------------


def test_dangling_edge_query_counts_unresolvable_endpoints_on_both_sides():
    """ArangoDB validates nothing here, so the query has to check both ends."""
    normalized = " ".join(DANGLING_EDGES_AQL.split())

    assert f"FOR edge IN {PLAYED_COLLECTION}" in normalized
    assert "DOCUMENT(edge._from) == null" in normalized
    assert "DOCUMENT(edge._to) == null" in normalized
    # OR, not AND: an edge with one bad endpoint is dangling.
    assert re.search(r"_from\) == null OR DOCUMENT\(edge\._to\) == null", normalized)
    assert "COLLECT WITH COUNT INTO" in normalized


def test_dangling_edge_query_names_the_edge_collection_constant():
    """Built from the imported constant, so a rename cannot leave it behind."""
    assert PLAYED_COLLECTION in DANGLING_EDGES_AQL


def test_count_dangling_edges_returns_the_number_the_query_produced():
    class FakeAQL:
        def __init__(self, rows):
            self.rows = rows
            self.executed = None

        def execute(self, query, **kwargs):
            self.executed = query
            return iter(self.rows)

    class FakeDB:
        def __init__(self, rows):
            self.aql = FakeAQL(rows)

    db = FakeDB([7])
    assert count_dangling_edges(db) == 7
    assert "DOCUMENT(edge._from)" in db.aql.executed

    # A graph with no edges at all returns an empty cursor, which is zero
    # dangling edges rather than an IndexError.
    assert count_dangling_edges(FakeDB([])) == 0


# --- The SUMMARY line's two halves -------------------------------------------


def test_graph_state_reads_the_three_collections_and_the_dangling_count():
    class FakeCounted:
        def __init__(self, n):
            self.n = n

        def count(self):
            return self.n

    class FakeAQL:
        def execute(self, query, **kwargs):
            return iter([0])

    class FakeDB:
        def __init__(self):
            self.aql = FakeAQL()
            self._counts = {
                LISTENERS_COLLECTION: 1308,
                TRACKS_COLLECTION: 464,
                PLAYED_COLLECTION: 45473,
            }

        def collection(self, name):
            return FakeCounted(self._counts[name])

    state = graph_state(FakeDB())
    assert state == {
        LISTENERS_COLLECTION: 1308,
        TRACKS_COLLECTION: 464,
        PLAYED_COLLECTION: 45473,
        "dangling_edges": 0,
    }


def test_summary_separates_replay_invariant_graph_state_from_run_state():
    """Point 4 of the module docstring, as an assertion.

    A second load over topics that now hold the records twice consumes twice as
    many -- so run counters legitimately move while the graph does not. If the
    two halves shared one object, GRPH-04's diff would fail on a correct run.
    """
    from src.graph_loader import RunSummary

    summary = RunSummary(
        state={
            LISTENERS_COLLECTION: 1308,
            TRACKS_COLLECTION: 464,
            PLAYED_COLLECTION: 45473,
            "dangling_edges": 0,
        },
        records_consumed=47245,
        documents_upserted=47245,
        invalid_value=0,
    )
    doc = summary.as_dict()

    assert set(doc) == {"graph_state", "run_state"}
    assert doc["graph_state"][PLAYED_COLLECTION] == 45473
    assert doc["graph_state"]["dangling_edges"] == 0
    # The run half must NOT carry the collection counts, or a diff of the whole
    # line would fail on a legitimate replay.
    assert PLAYED_COLLECTION not in doc["run_state"]
    assert doc["run_state"]["records_consumed"] == 47245


def test_exactly_one_object_in_the_summary_carries_the_graph_counts():
    """The extraction contract the phase's verify script relies on."""
    from src.graph_loader import RunSummary

    doc = RunSummary(
        state={
            LISTENERS_COLLECTION: 1,
            TRACKS_COLLECTION: 1,
            PLAYED_COLLECTION: 1,
            "dangling_edges": 0,
        },
        records_consumed=3,
        documents_upserted=3,
        invalid_value=0,
    ).as_dict()

    carriers = [
        v for v in doc.values() if isinstance(v, dict) and PLAYED_COLLECTION in v
    ]
    assert len(carriers) == 1

    # And it survives a JSON round trip with sorted keys, since that is how the
    # SUMMARY line is written and read back.
    assert json.loads(json.dumps(doc, sort_keys=True)) == doc


# --- Topic-to-collection mapping and record decoding -------------------------


def test_each_graph_topic_maps_to_its_collection():
    assert COLLECTION_FOR_TOPIC == {
        TOPIC_GRAPH_LISTENERS: LISTENERS_COLLECTION,
        TOPIC_GRAPH_TRACKS: TRACKS_COLLECTION,
        TOPIC_GRAPH_PLAYED: PLAYED_COLLECTION,
    }


@pytest.mark.parametrize(
    "value",
    [None, b"{not json", b"[]", b'"a string"', b'{"listener_id": "L1"}'],
)
def test_a_record_that_is_not_a_keyed_document_is_dropped_not_raised(value):
    """A document with no `_key` cannot be upserted by key, so it is invalid."""
    assert _decode(value) is None


def test_a_well_formed_document_decodes():
    payload = json.dumps({"_key": "L1", "listener_id": "L1"}).encode("utf-8")
    assert _decode(payload) == {"_key": "L1", "listener_id": "L1"}


# --- The graph really is a graph ---------------------------------------------


def test_the_loader_creates_played_as_an_edge_collection_in_a_named_graph():
    """`played` must be an EDGE collection, or traversals return nothing.

    A document collection cannot take part in an edge definition, and the failure
    is silent in exactly the way this phase keeps guarding against.
    """
    source = (REPO_ROOT / "src" / "graph_loader.py").read_text(encoding="utf-8")
    assert "edge=True" in source
    assert "create_graph" in source
    assert "from_vertex_collections" in source
    assert GRAPH_NAME


def test_the_default_endpoint_is_8531_and_not_the_arango_default():
    """8529 and 8530 belong to two unrelated projects on this machine."""
    assert "8531" in DEFAULT_ENDPOINT
    assert "8529" not in DEFAULT_ENDPOINT
    assert "8530" not in DEFAULT_ENDPOINT


def test_the_password_is_read_from_the_environment_not_a_cli_flag(monkeypatch):
    """A credential passed as a flag lands in shell history."""
    from src import graph_loader

    monkeypatch.setenv(graph_loader.ARANGO_PASSWORD_ENV, "from-the-env")
    assert graph_loader.credentials() == ("root", "from-the-env")

    source = (REPO_ROOT / "src" / "graph_loader.py").read_text(encoding="utf-8")
    assert "--password" not in source
