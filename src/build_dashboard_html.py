"""Build a self-contained, presentation-grade fraud dashboard: dashboard.html.

Reads the detectors' review-queue outputs + the event stream, computes the
chart data, and writes ONE static HTML file with the data and SVG charts baked
in. No server, no dependencies — anyone can open dashboard.html in a browser.

Run (after a pipeline run has produced output/*.json):
    python src/build_dashboard_html.py
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# Dark ops-console palette
BG = "#0B0E14"
CARD = "#141A24"
INK = "#E6EDF3"
MUTED = "#8B97A8"
GREEN = "#00E5A0"
CYAN = "#34D3FF"
PINK = "#FF5C7A"
GRID = "#222A38"

BIN_STEP = 5
BIN_MAX = 300


def load_events():
    rows = []
    with (REPO / "data" / "play_events.jsonl").open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def histogram(events_for_track):
    n = BIN_MAX // BIN_STEP
    counts = [0] * n
    for e in events_for_track:
        b = min(e["played_seconds"] // BIN_STEP, n - 1)
        counts[b] += 1
    edges = [i * BIN_STEP for i in range(n)]
    return counts, edges


def bots_chart_svg(bots, normal_max, threshold=300, width=680):
    maxscale = 720
    label_w, val_w, top = 56, 64, 8
    bar_w = width - label_w - val_w
    row_h = 30
    height = top + len(bots) * row_h + 34
    parts = [f'<svg viewBox="0 0 {width} {height}" width="100%" role="img">']
    # threshold + normal reference verticals
    tx = label_w + threshold / maxscale * bar_w
    nx = label_w + normal_max / maxscale * bar_w
    plot_bottom = top + len(bots) * row_h
    parts.append(
        f'<line x1="{tx:.1f}" y1="{top-2}" x2="{tx:.1f}" y2="{plot_bottom}" '
        f'stroke="{PINK}" stroke-width="1.5" stroke-dasharray="5 4" opacity="0.8"/>'
    )
    parts.append(
        f'<text x="{tx:.1f}" y="{plot_bottom+14}" fill="{PINK}" font-size="11" '
        f'text-anchor="middle" font-family="ui-monospace,Menlo,monospace">300 threshold</text>'
    )
    parts.append(
        f'<line x1="{nx:.1f}" y1="{top-2}" x2="{nx:.1f}" y2="{plot_bottom}" '
        f'stroke="{GREEN}" stroke-width="1.5" opacity="0.55"/>'
    )
    parts.append(
        f'<text x="{nx:.1f}" y="{plot_bottom+28}" fill="{GREEN}" font-size="11" '
        f'text-anchor="middle" font-family="ui-monospace,Menlo,monospace">normal max {normal_max}</text>'
    )
    for i, b in enumerate(bots):
        y = top + i * row_h + 4
        peak = b["peak_plays_in_window"]
        w = peak / maxscale * bar_w
        parts.append(
            f'<text x="0" y="{y+15:.0f}" fill="{MUTED}" font-size="12" '
            f'font-family="ui-monospace,Menlo,monospace">{b["listener_id"]}</text>'
        )
        parts.append(
            f'<rect x="{label_w}" y="{y:.0f}" width="{w:.1f}" height="20" rx="4" '
            f'fill="{PINK}"/>'
        )
        parts.append(
            f'<text x="{label_w + w + 8:.1f}" y="{y+15:.0f}" fill="{INK}" font-size="12" '
            f'font-weight="600" font-family="ui-monospace,Menlo,monospace">{peak}</text>'
        )
    parts.append("</svg>")
    return "".join(parts)


def hist_chart_svg(counts, edges, color, title, highlight_band=None, width=380, height=200):
    n = len(counts)
    maxc = max(counts) or 1
    pad_l, pad_r, pad_t, pad_b = 34, 8, 26, 26
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    bw = plot_w / n
    parts = [f'<svg viewBox="0 0 {width} {height}" width="100%" role="img">']
    parts.append(
        f'<text x="{pad_l}" y="14" fill="{INK}" font-size="12.5" font-weight="600">{title}</text>'
    )
    # baseline
    by = pad_t + plot_h
    parts.append(
        f'<line x1="{pad_l}" y1="{by}" x2="{width-pad_r}" y2="{by}" stroke="{GRID}" stroke-width="1"/>'
    )
    for i, c in enumerate(counts):
        h = c / maxc * plot_h
        x = pad_l + i * bw
        y = pad_t + plot_h - h
        in_band = highlight_band and highlight_band[0] <= edges[i] < highlight_band[1]
        col = PINK if in_band else color
        op = "1" if in_band else "0.85"
        parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{max(bw-0.8,0.8):.1f}" '
            f'height="{h:.1f}" fill="{col}" opacity="{op}" rx="0.5"/>'
        )
    # x ticks
    for sec in (0, 30, 60, 120, 180, 240, 300):
        x = pad_l + (sec / BIN_MAX) * plot_w
        parts.append(
            f'<text x="{x:.1f}" y="{height-8}" fill="{MUTED}" font-size="10" '
            f'text-anchor="middle" font-family="ui-monospace,Menlo,monospace">{sec}</text>'
        )
    if highlight_band:
        hx = pad_l + (highlight_band[0] / BIN_MAX) * plot_w
        parts.append(
            f'<text x="{hx:.1f}" y="{pad_t-4}" fill="{PINK}" font-size="10" '
            f'font-family="ui-monospace,Menlo,monospace">30–35s band</text>'
        )
    parts.append("</svg>")
    return "".join(parts)


def main() -> int:
    listener_q = json.loads((REPO / "output" / "listener_review_queue.json").read_text())
    track_q = json.loads((REPO / "output" / "track_review_queue.json").read_text())
    events = load_events()

    bots = listener_q["flagged_listeners"]
    flagged_ids = {b["listener_id"] for b in bots}
    ft = track_q["flagged_tracks"][0]

    # normal listeners' peak daily plays (for the reference line)
    daily = defaultdict(Counter)
    by_track = defaultdict(list)
    for e in events:
        daily[e["listener_id"]][e["event_time"][:10]] += 1
        by_track[e["track_id"]].append(e)
    normal_max = max(
        max(c.values())
        for lid, c in daily.items()
        if lid not in flagged_ids and lid.startswith("L")
    )

    flagged_track_id = ft["track_id"]
    normal_counts = Counter(
        e["track_id"] for e in events
        if e["listener_id"].startswith("L") and e["listener_id"] not in flagged_ids
    )
    typical_track_id = normal_counts.most_common(1)[0][0]

    f_counts, edges = histogram(by_track[flagged_track_id])
    t_counts, _ = histogram(by_track[typical_track_id])

    bots_svg = bots_chart_svg(bots, normal_max)
    flagged_hist = hist_chart_svg(f_counts, edges, PINK, "Flagged track", highlight_band=(30, 35))
    typical_hist = hist_chart_svg(t_counts, edges, CYAN, "Typical track")

    rows = "".join(
        f"<tr><td>{b['listener_id']}</td><td class='num'>{b['peak_plays_in_window']}</td>"
        f"<td class='num'>{b['plays_recorded']:,}</td></tr>"
        for b in bots
    )

    total_events = len(events)
    listeners_seen = listener_q["counts"]["listeners_seen"]

    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Streaming Fraud Monitor</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ margin:0; background:{BG}; color:{INK};
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
    background-image:radial-gradient(900px 500px at 80% -10%, rgba(0,229,160,.10), transparent),
      radial-gradient(700px 500px at 0% 0%, rgba(52,211,255,.07), transparent); }}
  .wrap {{ max-width:1080px; margin:0 auto; padding:40px 28px 60px; }}
  .mono {{ font-family:ui-monospace,"SF Mono",Menlo,monospace; }}
  .eyebrow {{ color:{GREEN}; letter-spacing:.18em; font-size:12px; text-transform:uppercase;
    font-family:ui-monospace,Menlo,monospace; }}
  h1 {{ font-size:34px; line-height:1.15; margin:10px 0 6px; font-weight:750; }}
  h1 .hi {{ color:{GREEN}; }} h1 .hi2 {{ color:{CYAN}; }}
  .sub {{ color:{MUTED}; font-size:15px; max-width:760px; }}
  .tiles {{ display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin:28px 0; }}
  .tile {{ background:{CARD}; border:1px solid {GRID}; border-radius:14px; padding:18px 18px 16px;
    position:relative; overflow:hidden; }}
  .tile::before {{ content:""; position:absolute; left:0; top:0; height:3px; width:100%;
    background:linear-gradient(90deg,{GREEN},{CYAN}); opacity:.9; }}
  .tile .k {{ color:{MUTED}; font-size:12.5px; }}
  .tile .v {{ font-size:34px; font-weight:750; margin-top:6px;
    font-family:ui-monospace,Menlo,monospace; text-shadow:0 0 22px rgba(0,229,160,.25); }}
  .tile.alert .v {{ color:{PINK}; text-shadow:0 0 22px rgba(255,92,122,.30); }}
  section {{ background:{CARD}; border:1px solid {GRID}; border-radius:16px; padding:22px 24px;
    margin-top:20px; }}
  .stag {{ display:flex; align-items:center; gap:10px; }}
  .stag .dot {{ width:10px; height:10px; border-radius:50%; box-shadow:0 0 12px currentColor; }}
  h2 {{ font-size:19px; margin:0; font-weight:700; }}
  .rule {{ color:{MUTED}; font-size:13.5px; margin:6px 0 18px; }}
  .rule b {{ color:{INK}; }}
  .grid2 {{ display:grid; grid-template-columns:1.5fr 1fr; gap:26px; align-items:start; }}
  .grid2b {{ display:grid; grid-template-columns:1fr 1fr; gap:22px; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  th {{ text-align:left; color:{MUTED}; font-weight:600; padding:6px 8px; border-bottom:1px solid {GRID}; }}
  td {{ padding:6px 8px; border-bottom:1px solid rgba(34,42,56,.5); }}
  td.num, th.num {{ text-align:right; font-family:ui-monospace,Menlo,monospace; }}
  .cap {{ color:{MUTED}; font-size:12.5px; margin-top:10px; }}
  .cond {{ display:grid; grid-template-columns:repeat(3,1fr); gap:14px; margin-bottom:18px; }}
  .cond .c {{ background:{BG}; border:1px solid {GRID}; border-radius:12px; padding:14px; }}
  .cond .cv {{ font-size:26px; font-weight:750; font-family:ui-monospace,Menlo,monospace; color:{GREEN}; }}
  .cond .ck {{ color:{MUTED}; font-size:12px; }} .cond .ct {{ color:{PINK}; font-size:11px; margin-top:4px; }}
  .note {{ background:{BG}; border-left:3px solid {CYAN}; border-radius:8px; padding:14px 16px;
    color:{MUTED}; font-size:13px; line-height:1.55; margin-top:16px; }}
  footer {{ color:{MUTED}; font-size:12px; margin-top:26px; text-align:center; line-height:1.6; }}
  @media (max-width:760px) {{ .tiles{{grid-template-columns:repeat(2,1fr);}} .grid2,.grid2b{{grid-template-columns:1fr;}} }}
</style></head>
<body><div class="wrap">
  <div class="eyebrow">Real-time Kafka pipeline &middot; MusicBrainz-grounded stream</div>
  <h1>Caught <span class="hi">8 bot accounts</span> + <span class="hi2">1 coordinated ring</span><br>across {total_events:,} plays.</h1>
  <div class="sub">Two event-time detectors watch the stream on different keys and write a review
    queue for a human analyst. This view reads their output &mdash; the decisions, not the raw rules.</div>

  <div class="tiles">
    <div class="tile"><div class="k">Events processed</div><div class="v">{total_events:,}</div></div>
    <div class="tile"><div class="k">Listeners seen</div><div class="v">{listeners_seen:,}</div></div>
    <div class="tile alert"><div class="k">Flagged bots &middot; Topology A</div><div class="v">{len(bots)}</div></div>
    <div class="tile alert"><div class="k">Viral rings &middot; Topology B</div><div class="v">{len(track_q['flagged_tracks'])}</div></div>
  </div>

  <section>
    <div class="stag"><span class="dot" style="color:{PINK}"></span><h2>Topology A &mdash; high-volume bots</h2></div>
    <div class="rule">Rule: more than <b>300 plays in any rolling 24&#8202;h</b> window. Bots run 24/7 with no sleep gap; real listeners never come close.</div>
    <div class="grid2">
      <div>{bots_svg}</div>
      <div>
        <table><thead><tr><th>Listener</th><th class="num">Peak/24h</th><th class="num">Total</th></tr></thead>
        <tbody>{rows}</tbody></table>
      </div>
    </div>
    <div class="cap">Every flagged bot sustains ~10&times; the busiest normal listener ({normal_max} plays/day). The gap isn't subtle.</div>
  </section>

  <section>
    <div class="stag"><span class="dot" style="color:{CYAN}"></span><h2>Topology B &mdash; coordinated viral fraud</h2></div>
    <div class="rule">Rule: within <b>1&#8202;h</b>, one track with <b>&ge;200 unique listeners</b>, <b>&le;1.1 plays/listener</b>, and <b>&ge;60% of stops in the 30&ndash;35&#8202;s band</b>.</div>
    <div class="cond">
      <div class="c"><div class="cv">{ft['unique_listeners']:,}</div><div class="ck">unique listeners</div><div class="ct">&ge; 200 required</div></div>
      <div class="c"><div class="cv">{ft['plays_per_listener']:.2f}</div><div class="ck">plays per listener</div><div class="ct">&le; 1.1 required</div></div>
      <div class="c"><div class="cv">{ft['band_share']:.0%}</div><div class="ck">stops in 30&ndash;35s band</div><div class="ct">&ge; 60% required</div></div>
    </div>
    <div class="grid2b">{flagged_hist}{typical_hist}</div>
    <div class="cap">The flagged track spikes in the 30&ndash;35&#8202;s band (artificial early stops just past the royalty line); a typical track's stops spread from 35&#8202;s to full length.</div>
    <div class="note">{ft['note']}</div>
  </section>

  <footer>Data: MusicBrainz catalog + synthetic play events &middot;
    producer &rarr; Kafka <span class="mono">play-events</span> &rarr; Consumer&#8201;1 (Topology&#8201;A) &rarr;
    <span class="mono">track-activity</span> &rarr; Consumer&#8201;2 (Topology&#8201;B) &rarr; review queues<br>
    Static export of <span class="mono">output/*.json</span> &middot; rebuild with <span class="mono">python src/build_dashboard_html.py</span></footer>
</div></body></html>"""

    out = REPO / "dashboard.html"
    out.write_text(html)
    print(f"Wrote {out}  ({len(html):,} bytes)")
    print(f"  bots={len(bots)}  flagged_track={flagged_track_id[:12]}  typical_track={typical_track_id[:12]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
