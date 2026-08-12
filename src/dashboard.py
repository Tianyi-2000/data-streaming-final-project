"""Streaming-fraud dashboard (Streamlit).

Reads the consumers' review-queue outputs and the event stream, and visualizes
the two fraud topologies the pipeline detected. Read-only: it shows what the
detectors already decided, it does not re-implement the rules.

Run:
    pip install -r requirements.txt
    # after a pipeline run has produced output/*.json:
    streamlit run src/dashboard.py
"""

from __future__ import annotations

import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

REPO = Path(__file__).resolve().parents[1]

# Colorblind-safe pair (Tableau 10): blue = normal, orange = fraud/flagged.
NORMAL = "#4E79A7"
FRAUD = "#F28E2B"

st.set_page_config(page_title="Streaming Fraud Monitor", layout="wide")


@st.cache_data
def load_events() -> pd.DataFrame:
    rows = []
    with (REPO / "data" / "play_events.jsonl").open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    df = pd.DataFrame(rows)
    df["date"] = df["event_time"].str[:10]
    return df


@st.cache_data
def load_output(name: str):
    p = REPO / "output" / name
    if not p.exists():
        return None
    return json.loads(p.read_text())


events = load_events()
listener_q = load_output("listener_review_queue.json")
track_q = load_output("track_review_queue.json")

st.title("🎧 Artificial Streaming-Fraud Monitor")
st.caption(
    "Real-time Kafka pipeline over a MusicBrainz-grounded event stream. "
    "This view reads the detectors' output review queues — the decisions, not the raw rules."
)

if listener_q is None or track_q is None:
    st.warning(
        "No detector output found in `output/`. Run the pipeline first:\n\n"
        "`docker compose up -d && python src/create_topics.py && "
        "python src/replay_to_kafka.py && python src/consumer_stage1.py && "
        "python src/consumer_stage2.py`"
    )
    st.stop()

flagged_listeners = listener_q["flagged_listeners"]
flagged_ids = {f["listener_id"] for f in flagged_listeners}
flagged_track = track_q["flagged_tracks"][0] if track_q["flagged_tracks"] else None

# ---------------------------------------------------------------- headline row
c1, c2, c3, c4 = st.columns(4)
c1.metric("Events processed", f"{len(events):,}")
c2.metric("Listeners seen", f"{listener_q['counts']['listeners_seen']:,}")
c3.metric("Flagged bots (Topology A)", len(flagged_listeners))
c4.metric("Viral-fraud windows (Topology B)", len(track_q["flagged_tracks"]))

st.divider()

# ============================================================ TOPOLOGY A
st.header("Topology A — high-volume bots")
st.caption(
    "Rule: more than **300 plays in any rolling 24h** window. "
    "Normal listeners never come close; bots run 24/7 with no sleep gap."
)

# Peak plays-per-day for every listener (consistent method for all listeners).
daily = events.groupby(["listener_id", "date"]).size().reset_index(name="plays")
peak = daily.groupby("listener_id")["plays"].max().reset_index(name="peak_day_plays")
peak["Detection"] = peak["listener_id"].apply(
    lambda x: "Flagged bot" if x in flagged_ids else "Normal listener"
)
normal_max = int(peak.loc[peak["Detection"] == "Normal listener", "peak_day_plays"].max())

left, right = st.columns([3, 2])
with left:
    strip = (
        alt.Chart(peak)
        .mark_circle(size=90, opacity=0.65)
        .encode(
            x=alt.X("Detection:N", title=None),
            y=alt.Y("peak_day_plays:Q", title="Peak plays in a single day"),
            color=alt.Color(
                "Detection:N",
                scale=alt.Scale(
                    domain=["Normal listener", "Flagged bot"], range=[NORMAL, FRAUD]
                ),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=["listener_id", "peak_day_plays", "Detection"],
        )
    )
    threshold = (
        alt.Chart(pd.DataFrame({"y": [300]}))
        .mark_rule(color="#E15759", strokeDash=[6, 4], size=2)
        .encode(y="y:Q")
    )
    st.altair_chart(
        (strip + threshold).properties(height=380, title="Every listener's busiest day"),
        use_container_width=True,
    )
    st.caption(
        f"Dashed line = the 300-play/24h threshold. Normal listeners peak at "
        f"**{normal_max}** plays/day; all {len(flagged_listeners)} bots sit far above the line."
    )
with right:
    st.subheader("Flagged bots")
    df_bots = pd.DataFrame(flagged_listeners)[
        ["listener_id", "peak_plays_in_window", "plays_recorded"]
    ].rename(
        columns={
            "listener_id": "Listener",
            "peak_plays_in_window": "Peak / 24h",
            "plays_recorded": "Total plays",
        }
    )
    st.dataframe(df_bots, hide_index=True, use_container_width=True)

st.divider()

# ============================================================ TOPOLOGY B
st.header("Topology B — coordinated viral fraud")
st.caption(
    "Rule: within **1 hour**, a single track with **≥200 unique listeners**, "
    "**≤1.1 plays per listener**, and **≥60% of plays stopping in the 30–35s band** "
    "(gaming the royalty threshold)."
)

if flagged_track:
    m1, m2, m3 = st.columns(3)
    m1.metric("Unique listeners", f"{flagged_track['unique_listeners']:,}", "≥ 200")
    m2.metric("Plays per listener", f"{flagged_track['plays_per_listener']:.2f}", "≤ 1.1")
    m3.metric("Stops in 30–35s band", f"{flagged_track['band_share']:.0%}", "≥ 60%")

    target_id = flagged_track["track_id"]
    tgt = events[events["track_id"] == target_id].copy()

    # A typical track for contrast: the track most played by normal listeners.
    normals = events[~events["listener_id"].isin(flagged_ids)]
    normals = normals[normals["listener_id"].str.startswith("L")]
    typical_id = normals["track_id"].value_counts().idxmax()
    typ = events[events["track_id"] == typical_id].copy()

    tgt["Track"] = "Flagged track"
    typ["Track"] = "Typical track"
    both = pd.concat([tgt, typ])

    hist = (
        alt.Chart(both)
        .mark_bar(opacity=0.85)
        .encode(
            x=alt.X(
                "played_seconds:Q",
                bin=alt.Bin(step=5),
                title="Seconds played before stopping",
            ),
            y=alt.Y("count():Q", title="Number of plays", stack=None),
            color=alt.Color(
                "Track:N",
                scale=alt.Scale(
                    domain=["Typical track", "Flagged track"], range=[NORMAL, FRAUD]
                ),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=["Track", alt.Tooltip("count():Q", title="plays")],
        )
        .properties(height=360, title="Where listeners stopped: flagged vs. typical track")
    )
    st.altair_chart(hist, use_container_width=True)
    st.caption(
        "The flagged track spikes hard in the 30–35s band (artificial early stops); "
        "a normal track's stops spread from 35s to full length."
    )

    with st.expander("Analyst review note (auto-generated, states no conclusion)"):
        st.write(flagged_track["note"])

st.divider()
st.caption(
    "Data source: MusicBrainz (real catalog) + synthetic play events. "
    "Pipeline: producer → Kafka `play-events` → Consumer 1 (Topology A) → "
    "`track-activity` → Consumer 2 (Topology B) → these review queues."
)
