"""The two cohort queries, and the deterministic cohort report.

TWO QUERIES, BECAUSE THE TWO TOPOLOGIES HAVE OPPOSITE SHAPES AND ONE QUERY
CANNOT FIND BOTH. This is the whole argument for the graph layer, so it is
stated here rather than left implicit in two function names.

  - CO-LISTENING OVERLAP finds a DENSE BIPARTITE BLOCK: a set of accounts that
    touched a near-identical set of tracks. This is the FRAUDAR / CopyCatch
    dense-subgraph pattern the research describes, and it is not expressible as
    a keyed aggregation -- density is a property of the relationships, not of
    any one key, so neither Consumer 1 (keyed by listener) nor Consumer 2 (keyed
    by track) can see it at all.
  - FAN-IN WITH NO OTHER ACTIVITY finds a MAXIMAL FAN-IN STAR, whose signature
    is the ABSENCE of any other activity rather than overlap. These listeners
    exist in the dataset for exactly one reason and their pairwise overlap with
    each other is near zero, so the first query cannot find them.

Writing only the first would miss the case the review queue actually flagged;
writing only the second would miss the more interesting structure.

FOUR THINGS THAT WILL RETURN A PLAUSIBLE WRONG ANSWER IF GOT WRONG.

1. `UNIQUE` ON BOTH SIDES, ALWAYS. The graph holds 45,473 edges over 13,985
   distinct (listener, track) pairs -- a mean of 3.25 plays per pair, maximum 14.
   A query that groups peers and takes `COUNT` counts EDGES, so every overlap
   comes back roughly threefold. A true 438 reads as about 1,400, which is a
   number a reviewer would accept. Every count in this module is the size of a
   UNIQUE set, and `tests/test_graph_queries.py` rejects a built query that
   would count edges rather than relying on anyone noticing.

2. THE SEEDS ARE READ FROM THE SHIPPED ARTIFACTS, NEVER TYPED. The flagged
   listener comes from `output/listener_review_queue.json` and the flagged track
   from `output/track_review_queue.json`, so the graph layer and the keyed
   pipeline cannot disagree about which listener and which track were flagged.
   No identifier appears as a literal in this module, and a test enforces that
   with an AST scan.

3. THE FAN-IN QUERY RETURNS 928, NOT 901, AND 928 IS CORRECT. The review queue's
   901 is measured inside ONE event-time hour, because Consumer 2's rule is a
   one-hour bucket. This graph is unwindowed and covers the whole stream, so it
   sees every listener who ever touched the track: 900 from the fan-in cohort,
   plus normal listeners, plus flagged ones. Both numbers are right and they
   measure different things. The report says so in as many words, because a
   reader who sees only one of them will try to reconcile it and conclude
   something is broken. Do not "fix" this query until it returns 901.

4. A COHORT WITH NO BASELINE IS A THRESHOLD CHOICE, NOT A FINDING. The report
   carries the measured normal pairwise overlap next to the cohort's, so the
   separation is visible as a property of the data. The baseline pool
   deliberately EXCLUDES single-track listeners: pairwise overlap among accounts
   that played one track each is trivially near zero, and including them would
   drive the baseline down and make the separation look larger than it is. The
   conservative choice is the honest one here.

THE REPORT REACHES NO VERDICT, AND THAT BOUND IS INHERITED RATHER THAN RESTATED.
`VERDICT_LEXICON` is imported from `src/summarize_review_queue.py`, so this layer
carries the same bound the review queue and the LLM summary already carry and the
three cannot drift. This is the most dangerous artifact this project produces --
it names hundreds of accounts at once, on evidence a reviewer will not
independently recompute -- and a false positive withholds money from an innocent
artist.

Usage:
    # after src/graph_emitter.py and src/graph_loader.py have run:
    python3 src/graph_queries.py
    python3 src/graph_queries.py --min-shared 100 --sample-size 60
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.graph_emitter import (  # noqa: E402
    LISTENER_PREFIX,
    LISTENERS_COLLECTION,
    PLAYED_COLLECTION,
    TRACK_PREFIX,
    TRACKS_COLLECTION,
)
from src.graph_loader import (  # noqa: E402
    DEFAULT_DATABASE,
    DEFAULT_ENDPOINT,
    count_dangling_edges,
    credentials,
    graph_state,
)

# IMPORTED, NEVER COPIED. Point on the no-verdict bound above.
from src.summarize_review_queue import VERDICT_LEXICON  # noqa: E402

_LOG = logging.getLogger(__name__)

DEFAULT_LISTENER_QUEUE = REPO_ROOT / "output" / "listener_review_queue.json"
DEFAULT_TRACK_QUEUE = REPO_ROOT / "output" / "track_review_queue.json"
DEFAULT_REPORT_PATH = REPO_ROOT / "output" / "cohort_report.json"

# The same shape `src/summarize_review_queue.py` uses: `output/` is gitignored,
# so an absent review queue is the normal state of a fresh checkout rather than a
# failure, and the error names the command that rebuilds it.
REGENERATE_HINT = (
    "output/ is gitignored, so an absent review queue is the normal state of a "
    "fresh checkout and not a failure. Regenerate it with:\n"
    "    docker compose up -d\n"
    "    rm -rf state output\n"
    "    python3 src/replay_to_kafka.py\n"
    "    python3 src/consumer_stage1.py --thresholds config/thresholds.json\n"
    "    python3 src/consumer_stage2.py --thresholds config/thresholds.json"
)

# Above this many shared DISTINCT tracks, a peer joins the reported cohort. The
# measured separation is enormous -- the cohort sits in the 400s and the nearest
# non-cohort pair at 25 -- so ANY cut between those two numbers gives the same
# answer. That is what makes this a measurement rather than a tuned threshold,
# and it is why the report carries the baseline next to the cohort.
DEFAULT_MIN_SHARED = 100

# Pairs sampled for the normal baseline. 60 listeners is 1,770 pairs, which is
# plenty for a mean and costs under a second.
DEFAULT_SAMPLE_SIZE = 60

# Point 4: a listener with one track has near-zero overlap with everyone, so
# including the fan-in cohort in the baseline pool would understate normal
# overlap and overstate the separation.
BASELINE_MIN_TRACKS = 2


# --- Reading the seeds -------------------------------------------------------


def _load_queue(path: Union[str, Path]) -> Dict[str, Any]:
    target = Path(path)
    try:
        raw = target.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"no review queue at {target}. {REGENERATE_HINT}"
        ) from exc
    try:
        return json.loads(raw)
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"the review queue at {target} could not be parsed: {exc}. "
            f"{REGENERATE_HINT}"
        ) from exc


def read_seed_listener(path: Union[str, Path] = DEFAULT_LISTENER_QUEUE) -> str:
    """The flagged listener the co-listening query starts from.

    READ, never typed -- point 2. Takes the first flagged entry; the CLI can
    override which listener is used, but not where the list comes from.
    """
    doc = _load_queue(path)
    entries = doc.get("flagged_listeners") or []
    if not entries:
        raise ValueError(
            f"the review queue at {path} names no flagged listeners, so there is "
            f"no seed to start from. {REGENERATE_HINT}"
        )
    return entries[0]["listener_id"]


def read_seed_track(path: Union[str, Path] = DEFAULT_TRACK_QUEUE) -> str:
    """The flagged track the fan-in query starts from. READ, never typed."""
    doc = _load_queue(path)
    entries = doc.get("flagged_tracks") or []
    if not entries:
        raise ValueError(
            f"the review queue at {path} names no flagged tracks, so there is no "
            f"seed to start from. {REGENERATE_HINT}"
        )
    return entries[0]["track_id"]


def flagged_listener_ids(path: Union[str, Path] = DEFAULT_LISTENER_QUEUE) -> List[str]:
    """Every flagged listener, used to exclude the cohort from the baseline."""
    doc = _load_queue(path)
    return [entry["listener_id"] for entry in doc.get("flagged_listeners") or []]


# --- Query one: co-listening overlap (the dense bipartite block) -------------

# Two steps. First the seed's own DISTINCT tracks; then every edge landing on one
# of them, grouped by its source listener, where the shared count is the size of
# the UNIQUE set of tracks in that group. Both UNIQUEs are point 1: without the
# second, this counts edges and inflates every overlap by roughly 3.25x.
_CO_LISTENING_AQL = f"""
LET seed_tracks = UNIQUE(
    FOR edge IN {PLAYED_COLLECTION}
        FILTER edge._from == @seed_id
        RETURN edge._to
)

FOR edge IN {PLAYED_COLLECTION}
    FILTER edge._to IN seed_tracks
    FILTER edge._from != @seed_id
    COLLECT peer = edge._from INTO shared_tracks = edge._to

    LET shared = LENGTH(UNIQUE(shared_tracks))
    FILTER shared >= @min_shared

    LET peer_total = LENGTH(UNIQUE(
        FOR peer_edge IN {PLAYED_COLLECTION}
            FILTER peer_edge._from == peer
            RETURN peer_edge._to
    ))

    SORT shared DESC, peer ASC
    RETURN {{
        listener_id: PARSE_IDENTIFIER(peer).key,
        shared_distinct_tracks: shared,
        peer_distinct_tracks: peer_total
    }}
"""


def build_co_listening_query(
    *, seed: str, min_shared: int = DEFAULT_MIN_SHARED
) -> Tuple[str, Dict[str, Any]]:
    """The dense-subgraph query, with its seed and threshold as bind parameters.

    The seed arrives as a bare `listener_id` and becomes a vertex id here, built
    from the imported prefix constant so the graph layer cannot disagree with the
    emitter about what a listener id looks like.
    """
    return _CO_LISTENING_AQL, {
        "seed_id": LISTENER_PREFIX + seed,
        "min_shared": min_shared,
    }


# --- Query two: fan-in with no other activity (the maximal star) -------------

# From the track, every distinct inbound listener; then, for each, its activity
# across the ENTIRE graph rather than just this track. The cohort is the subset
# with exactly one play and exactly one distinct track -- an account that exists
# in this dataset for one reason. `total_plays` is an edge count ON PURPOSE here
# (a play IS an edge); `distinct_tracks` is a UNIQUE set, which is what point 1
# is about.
_FAN_IN_AQL = f"""
FOR edge IN {PLAYED_COLLECTION}
    FILTER edge._to == @seed_id
    COLLECT listener = edge._from

    LET listener_edges = (
        FOR listener_edge IN {PLAYED_COLLECTION}
            FILTER listener_edge._from == listener
            RETURN listener_edge._to
    )

    SORT LENGTH(listener_edges) ASC, listener ASC
    RETURN {{
        listener_id: PARSE_IDENTIFIER(listener).key,
        total_plays: LENGTH(listener_edges),
        distinct_tracks: LENGTH(UNIQUE(listener_edges))
    }}
"""


def build_fan_in_query(*, seed: str) -> Tuple[str, Dict[str, Any]]:
    """Every listener inbound to the flagged track, with its whole-graph activity."""
    return _FAN_IN_AQL, {"seed_id": TRACK_PREFIX + seed}


# --- The baseline the separation is measured against -------------------------

_BASELINE_AQL = f"""
LET pool = (
    FOR listener IN {LISTENERS_COLLECTION}
        FILTER listener._key NOT IN @exclude
        LET tracks = UNIQUE(
            FOR edge IN {PLAYED_COLLECTION}
                FILTER edge._from == listener._id
                RETURN edge._to
        )
        FILTER LENGTH(tracks) >= @min_tracks
        SORT listener._key ASC
        LIMIT @sample_size
        RETURN {{ id: listener._key, tracks: tracks }}
)

FOR left IN pool
    FOR right IN pool
        FILTER right.id > left.id
        RETURN {{
            pair: [left.id, right.id],
            shared_distinct_tracks: LENGTH(INTERSECTION(left.tracks, right.tracks))
        }}
"""


def build_baseline_query(
    *,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    exclude: Sequence[str] = (),
    min_tracks: int = BASELINE_MIN_TRACKS,
) -> Tuple[str, Dict[str, Any]]:
    """Pairwise DISTINCT-track overlap among listeners outside the flagged set.

    `min_tracks` is point 4: single-track listeners overlap with nobody, so
    leaving them in would drag the baseline toward zero and flatter the finding.
    `SORT` before `LIMIT` makes the sample deterministic across runs.
    """
    return _BASELINE_AQL, {
        "sample_size": sample_size,
        "exclude": list(exclude),
        "min_tracks": min_tracks,
    }


# --- The no-verdict bound ----------------------------------------------------


def verdict_terms_in(document: Any) -> List[str]:
    """Which lexicon terms appear in a serialized document, at word boundaries.

    Word boundaries so ordinary prose containing a term as a substring is not
    rejected -- 'programming' must not trip 'gaming'. Returns the terms rather
    than a boolean so a failure can name what it found.
    """
    blob = json.dumps(document, sort_keys=True, default=str).lower()
    return [
        term
        for term in VERDICT_LEXICON
        if re.search(r"\b" + re.escape(term.lower()) + r"\b", blob)
    ]


def serialize_report(document: Dict[str, Any]) -> str:
    """Deterministic serialization, matching `write_review_queue`'s idiom.

    Sorted keys and a fixed indent are not cosmetic: two runs over the same graph
    must produce a byte-identical file, or diffing the artifact stops being a
    usable check.
    """
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


def write_report(document: Dict[str, Any], path: Union[str, Path]) -> Dict[str, Any]:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(serialize_report(document), encoding="utf-8")
    return document


# --- Running the queries -----------------------------------------------------


def _mean(values: Sequence[float]) -> float:
    return round(sum(values) / len(values), 4) if values else 0.0


def run_queries(
    db: Any,
    *,
    seed_listener: str,
    seed_track: str,
    flagged_listeners: Sequence[str],
    min_shared: int = DEFAULT_MIN_SHARED,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> Dict[str, Any]:
    """Both queries and the baseline, as the report's body."""
    co_query, co_binds = build_co_listening_query(
        seed=seed_listener, min_shared=min_shared
    )
    cohort = list(db.aql.execute(co_query, bind_vars=co_binds))

    fan_query, fan_binds = build_fan_in_query(seed=seed_track)
    inbound = list(db.aql.execute(fan_query, bind_vars=fan_binds))
    star = [
        row for row in inbound if row["total_plays"] == 1 and row["distinct_tracks"] == 1
    ]

    base_query, base_binds = build_baseline_query(
        sample_size=sample_size, exclude=list(flagged_listeners)
    )
    baseline_rows = list(db.aql.execute(base_query, bind_vars=base_binds))
    overlaps = [row["shared_distinct_tracks"] for row in baseline_rows]

    # The largest overlap between the seed and a listener OUTSIDE the flagged
    # set. This is the number that makes the separation a measurement: any cut
    # between it and the cohort's floor gives the same answer.
    wide_query, wide_binds = build_co_listening_query(seed=seed_listener, min_shared=1)
    all_peers = list(db.aql.execute(wide_query, bind_vars=wide_binds))
    outside = [
        row for row in all_peers if row["listener_id"] not in set(flagged_listeners)
    ]
    nearest_outside = max(
        (row["shared_distinct_tracks"] for row in outside), default=0
    )
    cohort_floor = min((row["shared_distinct_tracks"] for row in cohort), default=0)

    return {
        "co_listening": {
            "shape": (
                "a dense bipartite block: a set of listeners that touched a "
                "near-identical set of tracks. Density is a property of the "
                "relationships rather than of any one key, so neither keyed "
                "stage can see it"
            ),
            "seed_listener": seed_listener,
            "min_shared_distinct_tracks": min_shared,
            "counts_distinct_tracks_not_plays": True,
            "peers_over_threshold": len(cohort),
            "peers": cohort,
            "cohort_lowest_shared_distinct_tracks": cohort_floor,
            "highest_shared_distinct_tracks_outside_flagged_set": nearest_outside,
            "separation_note": (
                f"every cohort member shares at least {cohort_floor} distinct "
                f"tracks with the seed; the highest outside the flagged set is "
                f"{nearest_outside}. Any threshold between those two values "
                "produces the same membership, so this separation is a property "
                "of the data rather than of the threshold chosen"
            ),
        },
        "fan_in": {
            "shape": (
                "a maximal fan-in star, whose signature is the ABSENCE of any "
                "other activity rather than overlap. These listeners have no "
                "pairwise overlap with each other, so the co-listening query "
                "cannot find them"
            ),
            "seed_track": seed_track,
            "inbound_distinct_listeners": len(inbound),
            "single_play_single_track_listeners": len(star),
            "cohort": star,
            "windowed_comparison_note": (
                f"{len(inbound)} is the number of DISTINCT listeners inbound to "
                "this track across the WHOLE stream. The track review queue "
                "reports a different, smaller number for the same track because "
                "it measures one event-time hour, which is the window its rule "
                "uses. This graph is unwindowed. Both numbers are correct and "
                "they measure different things; neither is a correction of the "
                "other"
            ),
        },
        "baseline": {
            "description": (
                "pairwise DISTINCT-track overlap among listeners outside the "
                "flagged set, sampled deterministically. Listeners with fewer "
                f"than {BASELINE_MIN_TRACKS} distinct tracks are excluded: they "
                "overlap with nobody, and including them would push the baseline "
                "toward zero and make the separation look larger than it is"
            ),
            "sample_size_listeners": sample_size,
            "pairs_compared": len(baseline_rows),
            "mean_shared_distinct_tracks": _mean(overlaps),
            "max_shared_distinct_tracks": max(overlaps) if overlaps else 0,
        },
    }


def build_report(
    db: Any,
    *,
    seed_listener: str,
    seed_track: str,
    flagged_listeners: Sequence[str],
    listener_queue: Union[str, Path],
    track_queue: Union[str, Path],
    endpoint: str,
    database: str,
    min_shared: int = DEFAULT_MIN_SHARED,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> Dict[str, Any]:
    """The whole report: both results, the graph they came from, and the posture."""
    body = run_queries(
        db,
        seed_listener=seed_listener,
        seed_track=seed_track,
        flagged_listeners=flagged_listeners,
        min_shared=min_shared,
        sample_size=sample_size,
    )

    document: Dict[str, Any] = {
        "graph": {
            "endpoint": endpoint,
            "database": database,
            **graph_state(db),
        },
        "seeds": {
            "listener": seed_listener,
            "listener_read_from": str(listener_queue),
            "track": seed_track,
            "track_read_from": str(track_queue),
            "note": (
                "both seeds are READ from the shipped review-queue artifacts and "
                "never typed into the query module, so the graph layer and the "
                "keyed pipeline cannot disagree about which listener and which "
                "track were flagged"
            ),
        },
        "posture": (
            "these are review candidates carrying the numbers behind them, not a "
            "verdict: nothing here concludes that any wrongdoing occurred. This "
            "report names many accounts at once on evidence a reviewer will not "
            "independently recompute, and a false positive withholds money from "
            "an innocent artist, so every entry is a candidate for human review "
            "and states no conclusion"
        ),
        "method": (
            "every count in this report is the size of a set of DISTINCT tracks, "
            "never a count of plays. The graph holds 45,473 plays over 13,985 "
            "distinct listener-track pairs, so counting plays would inflate each "
            "overlap roughly threefold and still return a plausible number"
        ),
        **body,
    }
    return document


# --- CLI ---------------------------------------------------------------------


def _report_lines(document: Dict[str, Any]) -> None:
    graph = document["graph"]
    co = document["co_listening"]
    fan = document["fan_in"]
    base = document["baseline"]

    print("")
    print(f"Cohort queries against {graph['endpoint']} database '{graph['database']}'.")
    print(
        f"  graph: {graph[LISTENERS_COLLECTION]} listeners, "
        f"{graph[TRACKS_COLLECTION]} tracks, {graph[PLAYED_COLLECTION]} plays, "
        f"{graph['dangling_edges']} dangling"
    )
    print("")
    print(f"  QUERY 1 co-listening overlap, seeded on {co['seed_listener']}")
    print(
        f"    peers at or above {co['min_shared_distinct_tracks']} shared "
        f"distinct tracks: {co['peers_over_threshold']}"
    )
    for row in co["peers"]:
        print(
            f"      {row['listener_id']}: {row['shared_distinct_tracks']} shared "
            f"of {row['peer_distinct_tracks']} distinct tracks"
        )
    print(
        f"    cohort floor {co['cohort_lowest_shared_distinct_tracks']} vs "
        f"highest outside the flagged set "
        f"{co['highest_shared_distinct_tracks_outside_flagged_set']}"
    )
    print(
        f"    normal pairwise baseline: mean "
        f"{base['mean_shared_distinct_tracks']}, max "
        f"{base['max_shared_distinct_tracks']} over {base['pairs_compared']} pairs"
    )
    print("")
    print(f"  QUERY 2 fan-in with no other activity, seeded on {fan['seed_track']}")
    print(f"    distinct inbound listeners       : {fan['inbound_distinct_listeners']}")
    print(
        f"    of those, one play and one track : "
        f"{fan['single_play_single_track_listeners']}"
    )
    print("")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run the two cohort queries against the graph and write "
            "output/cohort_report.json."
        )
    )
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--listener-queue", default=str(DEFAULT_LISTENER_QUEUE))
    parser.add_argument("--track-queue", default=str(DEFAULT_TRACK_QUEUE))
    parser.add_argument(
        "--seed-listener",
        default=None,
        help="override the seed listener; the default is read from the queue",
    )
    parser.add_argument(
        "--seed-track",
        default=None,
        help="override the seed track; the default is read from the queue",
    )
    parser.add_argument("--min-shared", type=int, default=DEFAULT_MIN_SHARED)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument("--out", default=str(DEFAULT_REPORT_PATH))
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )

    try:
        seed_listener = args.seed_listener or read_seed_listener(args.listener_queue)
        seed_track = args.seed_track or read_seed_track(args.track_queue)
        flagged = flagged_listener_ids(args.listener_queue)
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    from arango import ArangoClient

    username, password = credentials()
    db = ArangoClient(hosts=args.endpoint).db(
        args.database, username=username, password=password
    )

    document = build_report(
        db,
        seed_listener=seed_listener,
        seed_track=seed_track,
        flagged_listeners=flagged,
        listener_queue=args.listener_queue,
        track_queue=args.track_queue,
        endpoint=args.endpoint,
        database=args.database,
        min_shared=args.min_shared,
        sample_size=args.sample_size,
    )

    # THE BOUND, ENFORCED RATHER THAN INTENDED. If a verdict term ever reaches
    # this document the run fails instead of writing it.
    offending = verdict_terms_in(document)
    if offending:
        print(
            f"ERROR: the cohort report contains verdict language {offending}. "
            "This artifact names candidates and the numbers behind them; it "
            "states no conclusion. Refusing to write it.",
            file=sys.stderr,
        )
        return 4

    write_report(document, args.out)
    _report_lines(document)
    print(f"  report: {args.out}")
    print(
        "SUMMARY "
        + json.dumps(
            {
                "graph_state": {
                    k: v for k, v in document["graph"].items() if isinstance(v, int)
                },
                "co_listening_peers": document["co_listening"]["peers_over_threshold"],
                "cohort_floor": document["co_listening"][
                    "cohort_lowest_shared_distinct_tracks"
                ],
                "nearest_outside": document["co_listening"][
                    "highest_shared_distinct_tracks_outside_flagged_set"
                ],
                "baseline_mean": document["baseline"]["mean_shared_distinct_tracks"],
                "fan_in_inbound": document["fan_in"]["inbound_distinct_listeners"],
                "fan_in_cohort": document["fan_in"][
                    "single_play_single_track_listeners"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
