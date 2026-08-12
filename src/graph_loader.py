"""The direct `python-arango` write path: three graph topics -> one named graph.

This is the module GRPH-04 exists for. The Kafka Connect sink that this project
originally planned was going to supply replay idempotency through its REPSERT
semantics; when the connector was descoped, that obligation quietly went with it
and no shipped document carried it any more. The direct writer supplies it
explicitly, and the phase proves it by RUNNING the load twice and comparing
counts rather than by citing a driver's documentation.

Four things a later reader needs, in the order they matter.

1. `insert_many` IS CALLED WITH BOTH `overwrite_mode="replace"` AND
   `raise_on_document_error=True`, AND BOTH ARE LOAD-BEARING. The first is the
   idempotency guarantee: a redelivered record carries the same natural id, so it
   becomes the same `_key` and overwrites itself instead of either raising on a
   duplicate or duplicating the edge.

   The second is the subtler one and it is the reason this paragraph is long. In
   python-arango 8.3.3 `raise_on_document_error` DEFAULTS TO FALSE, and with that
   default a per-document failure is not raised -- it is returned as an error
   OBJECT inside the list `insert_many` hands back. A loader that ignores the
   return value therefore completes without an exception, reports a clean run,
   and has written nothing. A failed load and a successful one look identical
   from the outside. Setting it True converts that into the exception it should
   always have been, and `tests/test_graph_loader.py` asserts both that the
   keyword is passed and that a rejected document propagates as an exception
   rather than as a list element.

2. EDGES MAY ARRIVE BEFORE THEIR VERTICES, AND THAT IS HARMLESS -- BUT ONLY
   BECAUSE THE DANGLING COUNT IS CHECKED. The three topics have no ordering
   relationship to one another, so `graph-played` can be read ahead of
   `graph-listeners`. ArangoDB does not validate that an edge's endpoints resolve
   to real documents, so nothing breaks on the way in. That same property is what
   makes a wrong collection prefix silently fatal: the traversals simply return
   nothing, and nothing errors. So this loader does not try to order anything.
   Instead, after its final settle it counts the edges whose `_from` or `_to`
   fails to resolve, and a non-zero count is a HARD FAILURE with a non-zero exit
   rather than a warning.

3. THE COLLECTION NAMES ARE IMPORTED FROM `src/graph_emitter.py`, NEVER RESTATED.
   The emitter builds `_from` and `_to` out of those same names. One typo in one
   prefix string yields a graph whose traversals return nothing while erroring on
   nothing, so there is exactly one place that typo can live.

4. THE SUMMARY LINE SEPARATES GRAPH STATE FROM RUN STATE, AND THE SEPARATION IS
   THE WHOLE REASON THE LINE IS DIFFABLE. `graph_state` -- the three collection
   counts and `dangling_edges`, read back out of the database at report time --
   is replay-invariant, and it is what GRPH-04 compares across two runs. The run
   counters are NOT replay-invariant: a second load reads topics that now hold
   the records twice, so records consumed legitimately doubles while the graph
   does not move at all. Phase 5 learned this exact lesson when PROF-02 had to
   stop comparing whole review documents because the counts block described topic
   and journal state rather than detection. Extract `graph_state` and diff that.

Usage:
    # broker up, ArangoDB up on 8531, and src/graph_emitter.py having run:
    python src/graph_loader.py
    python src/graph_loader.py --drop              # rebuild from scratch
    python src/graph_loader.py --group graph-loader-full
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaException

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# POINT 3: imported, never restated.
from src.graph_emitter import (  # noqa: E402
    LISTENERS_COLLECTION,
    PLAYED_COLLECTION,
    TOPIC_GRAPH_LISTENERS,
    TOPIC_GRAPH_PLAYED,
    TOPIC_GRAPH_TRACKS,
    TRACKS_COLLECTION,
)

_LOG = logging.getLogger(__name__)

DEFAULT_ENDPOINT = "http://localhost:8531"
DEFAULT_DATABASE = "streaming_fraud_graph"

# The named graph. It is not decoration: it is what makes three collections a
# GRAPH rather than three tables, it is what GRPH-01 names, and it is what lets a
# traversal be written against an edge definition instead of against raw
# collections.
GRAPH_NAME = "listening"
EDGE_DEFINITION_NAME = PLAYED_COLLECTION

# Root credentials. The password comes from the environment with a default that
# matches `docker-compose.yml`'s -- a local-development credential for a
# throwaway container on a non-default port holding synthetic data only.
ARANGO_USERNAME_ENV = "ARANGO_USERNAME"
ARANGO_PASSWORD_ENV = "ARANGO_ROOT_PASSWORD"
DEFAULT_USERNAME = "root"
DEFAULT_PASSWORD = "streamingfraud"

# Which topic feeds which collection. The one place the mapping is written.
COLLECTION_FOR_TOPIC = {
    TOPIC_GRAPH_LISTENERS: LISTENERS_COLLECTION,
    TOPIC_GRAPH_TRACKS: TRACKS_COLLECTION,
    TOPIC_GRAPH_PLAYED: PLAYED_COLLECTION,
}

INVALID_VALUE = "invalid_value"

# POINT 2. `DOCUMENT()` returns null for an id that does not resolve, so this
# counts exactly the edges whose endpoints are not really there. Kept as a module
# constant so `tests/test_graph_loader.py` can assert its construction without a
# database.
DANGLING_EDGES_AQL = f"""
FOR edge IN {PLAYED_COLLECTION}
    FILTER DOCUMENT(edge._from) == null OR DOCUMENT(edge._to) == null
    COLLECT WITH COUNT INTO dangling
    RETURN dangling
"""


def credentials() -> Tuple[str, str]:
    """The ArangoDB username and password, from the environment.

    Read here and nowhere else, so there is one route to the credential and it is
    never a CLI flag that would land in a shell history file.
    """
    return (
        os.environ.get(ARANGO_USERNAME_ENV, DEFAULT_USERNAME),
        os.environ.get(ARANGO_PASSWORD_ENV, DEFAULT_PASSWORD),
    )


def ensure_graph(
    client: Any,
    database: str = DEFAULT_DATABASE,
    drop: bool = False,
) -> Any:
    """Create the database, the three collections and the named graph if absent.

    `played` is created as an EDGE collection, not a document collection. That
    distinction is structural rather than cosmetic: a document collection cannot
    participate in an edge definition, and a traversal over one returns nothing
    while erroring on nothing -- point 2's failure mode again.
    """
    username, password = credentials()
    system = client.db("_system", username=username, password=password)

    if drop and system.has_database(database):
        system.delete_database(database)
        _LOG.info("dropped database %s", database)
    if not system.has_database(database):
        system.create_database(database)
        _LOG.info("created database %s", database)

    db = client.db(database, username=username, password=password)

    for name in (LISTENERS_COLLECTION, TRACKS_COLLECTION):
        if not db.has_collection(name):
            db.create_collection(name)
            _LOG.info("created vertex collection %s", name)
    if not db.has_collection(PLAYED_COLLECTION):
        db.create_collection(PLAYED_COLLECTION, edge=True)
        _LOG.info("created EDGE collection %s", PLAYED_COLLECTION)

    if not db.has_graph(GRAPH_NAME):
        db.create_graph(
            GRAPH_NAME,
            edge_definitions=[
                {
                    "edge_collection": PLAYED_COLLECTION,
                    "from_vertex_collections": [LISTENERS_COLLECTION],
                    "to_vertex_collections": [TRACKS_COLLECTION],
                }
            ],
        )
        _LOG.info(
            "created named graph %s (%s: %s -> %s)",
            GRAPH_NAME,
            PLAYED_COLLECTION,
            LISTENERS_COLLECTION,
            TRACKS_COLLECTION,
        )

    return db


def upsert_documents(collection: Any, documents: List[Dict[str, Any]]) -> int:
    """Write one batch with replace semantics and document errors RAISING.

    POINT 1 IN ONE CALL. Both keywords are passed explicitly and neither is left
    to the driver's defaults -- `raise_on_document_error` defaults to False in
    python-arango 8.3.3, which would turn a failed write into a list of error
    objects that a caller ignoring the return value never sees.

    The return value is checked anyway, belt and braces: if a future driver
    version changes what the keyword does, an error object in the result is still
    caught here rather than counted as a successful write.
    """
    if not documents:
        return 0

    result = collection.insert_many(
        documents,
        overwrite_mode="replace",
        raise_on_document_error=True,
    )

    # Defence in depth. With `raise_on_document_error=True` this loop should
    # never find anything; if it does, the driver's contract changed and a silent
    # partial write is exactly what this module exists to prevent.
    if isinstance(result, list):
        failures = [item for item in result if isinstance(item, Exception)]
        if failures:
            raise RuntimeError(
                f"{len(failures)} document(s) failed to write to "
                f"'{collection.name}' and were returned as error objects rather "
                f"than raised; first: {failures[0]}"
            )
    return len(documents)


def count_dangling_edges(db: Any) -> int:
    """Edges in `played` whose `_from` or `_to` does not resolve. Must be 0."""
    cursor = db.aql.execute(DANGLING_EDGES_AQL)
    rows = list(cursor)
    return int(rows[0]) if rows else 0


def graph_state(db: Any) -> Dict[str, int]:
    """The replay-invariant numbers, read out of the database. Point 4."""
    return {
        LISTENERS_COLLECTION: db.collection(LISTENERS_COLLECTION).count(),
        TRACKS_COLLECTION: db.collection(TRACKS_COLLECTION).count(),
        PLAYED_COLLECTION: db.collection(PLAYED_COLLECTION).count(),
        "dangling_edges": count_dangling_edges(db),
    }


@dataclass
class _Batch:
    """Documents waiting to be written, and the offsets waiting on that write."""

    buffers: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    pending: List[Any] = field(default_factory=list)

    def add(self, collection: str, document: Dict[str, Any]) -> None:
        self.buffers.setdefault(collection, []).append(document)

    def size(self) -> int:
        return sum(len(v) for v in self.buffers.values())


def _build_consumer(broker: str, group: str) -> Consumer:
    """Neither auto-commits nor auto-stores -- the discipline the pipeline uses."""
    return Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )


def _settle(consumer: Consumer, db: Any, batch: _Batch) -> int:
    """Write every buffered collection, THEN store, THEN commit -- in that order.

    The same ordering Consumer 1 uses between a produce and a commit. An offset
    is committed only after the write it depends on has RETURNED successfully, so
    a crash between the two redelivers the record -- which is harmless here
    precisely because the write is an upsert by key.
    """
    written = 0
    if batch.buffers:
        for name, documents in batch.buffers.items():
            written += upsert_documents(db.collection(name), documents)
        batch.buffers.clear()

    if batch.pending:
        for msg in batch.pending:
            consumer.store_offsets(message=msg)
        consumer.commit(asynchronous=False)
        batch.pending.clear()
    return written


@dataclass(frozen=True)
class RunSummary:
    """Graph state and run state, deliberately kept apart. Point 4."""

    state: Dict[str, int]
    records_consumed: int
    documents_upserted: int
    invalid_value: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            # THE REPLAY-INVARIANT HALF. This is the object GRPH-04 diffs.
            "graph_state": dict(self.state),
            # THE RUN HALF, which is NOT replay-invariant: a second load reads
            # topics that now hold the records twice, so these legitimately move
            # while `graph_state` must not.
            "run_state": {
                "records_consumed": self.records_consumed,
                "documents_upserted": self.documents_upserted,
                "invalid_value": self.invalid_value,
            },
        }


def run(
    *,
    broker: str,
    group: str,
    endpoint: str,
    database: str,
    topics: List[str],
    drop: bool = False,
    max_records: int = 0,
    batch_size: int = 500,
    idle_timeout: float = 10.0,
) -> RunSummary:
    """Consume the three graph topics and upsert them into the named graph."""
    from arango import ArangoClient

    client = ArangoClient(hosts=endpoint)
    db = ensure_graph(client, database=database, drop=drop)

    consumer = _build_consumer(broker, group)
    consumer.subscribe(topics)

    batch = _Batch()
    records_consumed = 0
    documents_upserted = 0
    invalid_value = 0

    try:
        idle_since = time.monotonic()
        while True:
            if max_records and records_consumed >= max_records:
                break

            msg = consumer.poll(1.0)
            if msg is None:
                documents_upserted += _settle(consumer, db, batch)
                if time.monotonic() - idle_since >= idle_timeout:
                    break
                continue
            if msg.error():
                _LOG.warning("consumer error: %s", msg.error())
                continue

            idle_since = time.monotonic()
            records_consumed += 1

            collection = COLLECTION_FOR_TOPIC.get(msg.topic())
            if collection is None:
                # Not a graph topic. Nothing to write, but the offset still has
                # to advance or the loop would re-read it forever.
                _LOG.warning("record from unexpected topic %s", msg.topic())
                batch.pending.append(msg)
                continue

            document = _decode(msg.value())
            if document is None:
                invalid_value += 1
                _LOG.warning(
                    "dropped record: reason=%s topic=%s partition=%s offset=%s",
                    INVALID_VALUE,
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
            else:
                batch.add(collection, document)

            batch.pending.append(msg)
            if batch.size() >= batch_size:
                documents_upserted += _settle(consumer, db, batch)

        documents_upserted += _settle(consumer, db, batch)
    finally:
        consumer.close()

    # POINT 2: the dangling-edge check runs after the FINAL settle, never before.
    state = graph_state(db)
    summary = RunSummary(
        state=state,
        records_consumed=records_consumed,
        documents_upserted=documents_upserted,
        invalid_value=invalid_value,
    )
    _report(summary, endpoint, database)
    return summary


def _decode(value: Optional[bytes]) -> Optional[Dict[str, Any]]:
    """Parse one record, returning None rather than raising on a bad one."""
    if value is None:
        return None
    try:
        document = json.loads(value)
    except (ValueError, TypeError):
        return None
    if not isinstance(document, dict) or "_key" not in document:
        return None
    return document


def _report(summary: RunSummary, endpoint: str, database: str) -> None:
    """A human-readable block, then one machine-readable line, on stdout."""
    state = summary.state
    print("")
    print(f"Graph loader done against {endpoint} database '{database}'.")
    print("  GRAPH STATE (replay-invariant; this is what GRPH-04 compares)")
    print(f"    {LISTENERS_COLLECTION:<14}: {state[LISTENERS_COLLECTION]}")
    print(f"    {TRACKS_COLLECTION:<14}: {state[TRACKS_COLLECTION]}")
    print(f"    {PLAYED_COLLECTION:<14}: {state[PLAYED_COLLECTION]}")
    print(f"    {'dangling_edges':<14}: {state['dangling_edges']}")
    print("  RUN STATE (NOT replay-invariant; a second load reads more records)")
    print(f"    records consumed  : {summary.records_consumed}")
    print(f"    documents upserted: {summary.documents_upserted}")
    print(f"    dropped, invalid  : {summary.invalid_value}")
    print("SUMMARY " + json.dumps(summary.as_dict(), sort_keys=True))


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Consume the three graph topics and upsert them into an ArangoDB "
            "named graph, with replace semantics and document errors raising."
        )
    )
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--group", default="graph-loader")
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
        help=(
            "ArangoDB HTTP endpoint. 8531, not the default 8529: two ArangoDB "
            "instances belonging to other projects hold this machine's 8529 "
            "and 8530."
        ),
    )
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument(
        "--drop",
        action="store_true",
        help="drop and recreate the database first, so a rebuild is deterministic",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="documents buffered before a write-then-commit settle",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=10.0,
        help="seconds of empty polling before settling and exiting cleanly",
    )
    parser.add_argument("--max-records", type=int, default=0, help="0 = unlimited")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )

    try:
        summary = run(
            broker=args.broker,
            group=args.group,
            endpoint=args.endpoint,
            database=args.database,
            topics=[TOPIC_GRAPH_LISTENERS, TOPIC_GRAPH_TRACKS, TOPIC_GRAPH_PLAYED],
            drop=args.drop,
            max_records=args.max_records,
            batch_size=args.batch_size,
            idle_timeout=args.idle_timeout,
        )
    except (RuntimeError, KafkaException, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    # POINT 2: a non-zero dangling count is a HARD FAILURE, not a warning. A
    # graph whose edges point at nothing returns empty traversals and errors on
    # nothing, so it has to fail here or it will not fail anywhere.
    dangling = summary.state["dangling_edges"]
    if dangling:
        print(
            f"ERROR: {dangling} edge(s) in '{PLAYED_COLLECTION}' have a _from or "
            "_to that does not resolve to a document. ArangoDB does not validate "
            "edge endpoints, so this would show up as empty traversals rather "
            "than as an error. Check the collection-name prefixes in "
            "src/graph_emitter.py and re-run with --drop.",
            file=sys.stderr,
        )
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
