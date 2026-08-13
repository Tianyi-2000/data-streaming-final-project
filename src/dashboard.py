"""Live streaming-fraud dashboard (Streamlit, dark ops-console theme).

Reads the consumers' review-queue outputs and the event stream, and visualizes
the two fraud topologies the pipeline detected. Read-only: it shows what the
detectors already decided, it does not re-implement the rules.

Run:
    pip install -r requirements.txt
    # after a pipeline run has produced output/*.json:
    streamlit run src/dashboard.py
Opens live at http://localhost:8501 .
"""

from __future__ import annotations

import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

REPO = Path(__file__).resolve().parents[1]

# Dark ops-console palette
BG, CARD, INK, MUTED, GRID = "#0B0E14", "#141A24", "#E6EDF3", "#8B97A8", "#222A38"
GREEN, CYAN, PINK = "#00E5A0", "#34D3FF", "#FF5C7A"
NORMAL, FRAUD = CYAN, PINK  # chart series

st.set_page_config(page_title="Streaming Fraud Monitor", layout="wide")

st.markdown(
    f"""
<style>
  .stApp {{ background:{BG};
    background-image:radial-gradient(900px 500px at 82% -8%, rgba(0,229,160,.10), transparent),
      radial-gradient(700px 480px at 0% 0%, rgba(52,211,255,.07), transparent); }}
  #MainMenu, header, footer {{ visibility:hidden; }}
  .block-container {{ padding-top:2.4rem; max-width:1120px; }}
  .eyebrow {{ color:{GREEN}; letter-spacing:.18em; font-size:12px; text-transform:uppercase;
    font-family:ui-monospace,Menlo,monospace; }}
  .hero {{ font-size:34px; line-height:1.15; font-weight:800; margin:8px 0 6px; color:{INK}; }}
  .hero .g {{ color:{GREEN}; }} .hero .c {{ color:{CYAN}; }}
  .sub {{ color:{MUTED}; font-size:15px; max-width:780px; }}
  .tiles {{ display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin:26px 0 8px; }}
  .tile {{ background:{CARD}; border:1px solid {GRID}; border-radius:14px; padding:16px 18px;
    position:relative; overflow:hidden; }}
  .tile::before {{ content:""; position:absolute; left:0; top:0; height:3px; width:100%;
    background:linear-gradient(90deg,{GREEN},{CYAN}); }}
  .tile.alert::before {{ background:linear-gradient(90deg,{PINK},#B23A6B); }}
  .tile .k {{ color:{MUTED}; font-size:12.5px; }}
  .tile .v {{ font-size:32px; font-weight:800; margin-top:4px;
    font-family:ui-monospace,Menlo,monospace; text-shadow:0 0 22px rgba(0,229,160,.25); }}
  .tile.alert .v {{ color:{PINK}; text-shadow:0 0 22px rgba(255,92,122,.30); }}
  .stag {{ display:flex; align-items:center; gap:10px; margin-top:8px; }}
  .stag .dot {{ width:10px; height:10px; border-radius:50%; box-shadow:0 0 12px currentColor; }}
  .stag h2 {{ font-size:19px; margin:0; font-weight:700; color:{INK}; }}
  .rule {{ color:{MUTED}; font-size:13.5px; margin:6px 0 14px; }} .rule b {{ color:{INK}; }}
  .cond {{ display:grid; grid-template-columns:repeat(3,1fr); gap:14px; margin:6px 0 8px; }}
  .cond .c {{ background:{CARD}; border:1px solid {GRID}; border-radius:12px; padding:14px; }}
  .cond .cv {{ font-size:26px; font-weight:800; font-family:ui-monospace,Menlo,monospace; color:{GREEN}; }}
  .cond .ck {{ color:{MUTED}; font-size:12px; }} .cond .ct {{ color:{PINK}; font-size:11px; margin-top:3px; }}
  .note {{ background:{CARD}; border-left:3px solid {CYAN}; border-radius:8px; padding:13px 16px;
    color:{MUTED}; font-size:13px; line-height:1.55; }}
  .cap {{ color:{MUTED}; font-size:12.5px; }}
  hr {{ border-color:{GRID}; }}
</style>
""",
    unsafe_allow_html=True,
)


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
    return json.loads(p.read_text()) if p.exists() else None


def chart_theme(chart):
    return (
        chart.configure_view(strokeWidth=0)
        .configure(background="transparent")
        .configure_axis(
            labelColor=MUTED, titleColor=MUTED, gridColor=GRID, domainColor=GRID, tickColor=GRID
        )
        .configure_legend(labelColor=INK, titleColor=MUTED)
        .configure_title(color=INK)
    )


events = load_events()
listener_q = load_output("listener_review_queue.json")
track_q = load_output("track_review_queue.json")

if listener_q is None or track_q is None:
    st.markdown('<div class="eyebrow">Streaming Fraud Monitor</div>', unsafe_allow_html=True)
    st.warning(
        "No detector output found in `output/`. Run the pipeline first:\n\n"
        "`docker compose up -d && python src/create_topics.py && "
        "python src/replay_to_kafka.py && python src/consumer_stage1.py && "
        "python src/consumer_stage2.py`"
    )
    st.stop()

bots = listener_q["flagged_listeners"]
flagged_ids = {b["listener_id"] for b in bots}
ft = track_q["flagged_tracks"][0] if track_q["flagged_tracks"] else None
n_events, n_listeners = len(events), listener_q["counts"]["listeners_seen"]

# ---------------------------------------------------------------- hero + tiles
st.markdown('<div class="eyebrow">Real-time Kafka pipeline &middot; MusicBrainz-grounded stream</div>', unsafe_allow_html=True)
st.markdown(
    f'<div class="hero">Caught <span class="g">{len(bots)} bot accounts</span> + '
    f'<span class="c">{len(track_q["flagged_tracks"])} coordinated ring</span> '
    f'across {n_events:,} plays.</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="sub">Two event-time detectors watch the stream on different keys and write a '
    'review queue for a human analyst. This view reads their output — the decisions, not the raw rules.</div>',
    unsafe_allow_html=True,
)
st.markdown(
    f"""<div class="tiles">
      <div class="tile"><div class="k">Events processed</div><div class="v">{n_events:,}</div></div>
      <div class="tile"><div class="k">Listeners seen</div><div class="v">{n_listeners:,}</div></div>
      <div class="tile alert"><div class="k">Flagged bots · Topology A</div><div class="v">{len(bots)}</div></div>
      <div class="tile alert"><div class="k">Viral rings · Topology B</div><div class="v">{len(track_q['flagged_tracks'])}</div></div>
    </div>""",
    unsafe_allow_html=True,
)
st.write("")

# ============================================================ TOPOLOGY A
st.markdown(f'<div class="stag"><span class="dot" style="color:{PINK}"></span><h2>Topology A — high-volume bots</h2></div>', unsafe_allow_html=True)
st.markdown('<div class="rule">Rule: more than <b>300 plays in any rolling 24h</b> window. Bots run 24/7 with no sleep gap; real listeners never come close.</div>', unsafe_allow_html=True)

daily = events.groupby(["listener_id", "date"]).size().reset_index(name="plays")
peak = daily.groupby("listener_id")["plays"].max().reset_index(name="peak_day_plays")
peak["Detection"] = peak["listener_id"].apply(lambda x: "Flagged bot" if x in flagged_ids else "Normal listener")
normal_max = int(peak.loc[peak["Detection"] == "Normal listener", "peak_day_plays"].max())

left, right = st.columns([3, 2])
with left:
    strip = (
        alt.Chart(peak)
        .mark_circle(size=95, opacity=0.7)
        .encode(
            x=alt.X("Detection:N", title=None),
            y=alt.Y("peak_day_plays:Q", title="Peak plays in a single day"),
            color=alt.Color(
                "Detection:N",
                scale=alt.Scale(domain=["Normal listener", "Flagged bot"], range=[NORMAL, FRAUD]),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=["listener_id", "peak_day_plays", "Detection"],
        )
    )
    threshold = alt.Chart(pd.DataFrame({"y": [300]})).mark_rule(color=PINK, strokeDash=[6, 4], size=2).encode(y="y:Q")
    st.altair_chart(chart_theme((strip + threshold).properties(height=380)), use_container_width=True)
    st.markdown(f'<div class="cap">Dashed line = the 300-play/24h threshold. Normal listeners peak at <b style="color:{INK}">{normal_max}</b>/day; all {len(bots)} bots sit far above it.</div>', unsafe_allow_html=True)
with right:
    df_bots = pd.DataFrame(bots)[["listener_id", "peak_plays_in_window", "plays_recorded"]].rename(
        columns={"listener_id": "Listener", "peak_plays_in_window": "Peak / 24h", "plays_recorded": "Total plays"}
    )
    st.dataframe(df_bots, hide_index=True, use_container_width=True)

st.markdown("<hr>", unsafe_allow_html=True)

# ============================================================ TOPOLOGY B
st.markdown(f'<div class="stag"><span class="dot" style="color:{CYAN}"></span><h2>Topology B — coordinated viral fraud</h2></div>', unsafe_allow_html=True)
st.markdown('<div class="rule">Rule: within <b>1h</b>, one track with <b>≥200 unique listeners</b>, <b>≤1.1 plays/listener</b>, and <b>≥60% of stops in the 30–35s band</b>.</div>', unsafe_allow_html=True)

if ft:
    st.markdown(
        f"""<div class="cond">
          <div class="c"><div class="cv">{ft['unique_listeners']:,}</div><div class="ck">unique listeners</div><div class="ct">≥ 200 required</div></div>
          <div class="c"><div class="cv">{ft['plays_per_listener']:.2f}</div><div class="ck">plays per listener</div><div class="ct">≤ 1.1 required</div></div>
          <div class="c"><div class="cv">{ft['band_share']:.0%}</div><div class="ck">stops in 30–35s band</div><div class="ct">≥ 60% required</div></div>
        </div>""",
        unsafe_allow_html=True,
    )

    target_id = ft["track_id"]
    tgt = events[events["track_id"] == target_id].copy()
    normals = events[(~events["listener_id"].isin(flagged_ids)) & (events["listener_id"].str.startswith("L"))]
    typical_id = normals["track_id"].value_counts().idxmax()
    typ = events[events["track_id"] == typical_id].copy()
    tgt["Track"], typ["Track"] = "Flagged track", "Typical track"
    both = pd.concat([tgt, typ])

    hist = (
        alt.Chart(both)
        .mark_bar(opacity=0.85)
        .encode(
            x=alt.X("played_seconds:Q", bin=alt.Bin(step=5), title="Seconds played before stopping"),
            y=alt.Y("count():Q", title="Number of plays", stack=None),
            color=alt.Color(
                "Track:N",
                scale=alt.Scale(domain=["Typical track", "Flagged track"], range=[NORMAL, FRAUD]),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=["Track", alt.Tooltip("count():Q", title="plays")],
        )
        .properties(height=340)
    )
    st.altair_chart(chart_theme(hist), use_container_width=True)
    st.markdown('<div class="cap">The flagged track spikes hard in the 30–35s band (artificial early stops); a typical track\'s stops spread from 35s to full length.</div>', unsafe_allow_html=True)
    st.write("")
    st.markdown(f'<div class="note">{ft["note"]}</div>', unsafe_allow_html=True)
