# Research — How Artificial Streaming Fraud Actually Works

> Background research for [Option 4 — Artificial Streaming Anomaly Monitor](04-streaming-fraud-anomaly-monitor.md).
> Researched 2026-08-04. Not a proposal deliverable; supporting evidence for the
> design decisions in that proposal, and a record of one significant correction to it.

## Why this document exists

The first draft of Option 4 detected anomalies **per listener account**, using a
`plays_per_hour` rate rule keyed on `listener_id`. That premise was challenged, and
the research does not support it. Per-account rate detection catches only the
laziest fraud. This document records what artificial streaming operations actually
look like, which signals actually detect them, and what that means for the
project's Kafka topology.

## Headline findings

1. **Fraud is designed to be invisible per account.** Farms distribute activity so
   that no single account, and often no single track, looks anomalous in isolation.
2. **There are at least two opposite topologies**, and a single partitioning key
   cannot catch both.
3. **Camouflage is a first-class design principle** of these operations, not an
   afterthought — which is why naive anomaly detection fails.
4. **The signals that work are distributional (per track) or structural (cross-account
   graph)**, not per-account rate checks.
5. Enforcement in industry happens **primarily at the track level**, with account
   history informing whether enforcement escalates.

## Scale of the problem

- Industry estimates put fraudulent streams near **10% of all streams**; the IFPI
  figure for diverted royalties is close to **$2B/year**. Beatdapp, the dominant
  cross-platform detection vendor, analyzed over **2 trillion streams and 20 trillion
  data points** in 2023 alone.
- Deezer reports logging roughly **75,000 fully-AI-generated tracks per day**, and
  found that up to **85% of streams on fully-AI tracks were fraudulent** in 2025.
- The Music Fights Fraud Alliance (Amazon Music, Spotify, and others) exists
  specifically to coordinate cross-platform response.

## The two topologies

This is the core finding, and the reason the original design was wrong.

### Topology A — few accounts, many plays each, spread thin across content

The *United States v. Michael Smith* case (Southern District of New York; guilty
plea to conspiracy to commit wire fraud, March 19 2026 — the first US criminal
prosecution for AI-assisted streaming fraud):

| Fact | Value |
|---|---|
| Bot accounts | **1,040** (52 cloud service accounts × 20 bot profiles), later scaling toward ~10,000 |
| Fake streams per day | **661,440** across Spotify, Apple Music, Amazon Music, YouTube Music |
| Implied rate | **~636 plays per account per day** |
| Content catalogue | Hundreds of thousands of AI-generated tracks, sourced at up to 10,000/month |
| Duration of operation | **2017 – 2024 (seven years)** |
| Royalties obtained | **>$8M** (charged as >$10M) |

The DOJ filing is explicit about the evasion strategy: Smith **"spread his automated
streams across thousands of songs to avoid anomalous streaming as to any single
song."** The evasion targets *per-track* detection.

Note what the implied rate means for a per-account rule. At ~636 plays/day of
~31 seconds each, a bot account produces roughly 5.5 hours of daily "listening" —
high, but within reach of a genuine heavy user. And the operation ran for seven
years. A `plays_per_hour` threshold is not what caught it.

### Topology B — many accounts, one play each, concentrated on a track

Beatdapp's published analysis of a single bot farm: **more than 53,000 distinct user
accounts that each listened to a particular track exactly once.**

A per-account rate rule flags **nothing** here. Every account made a single play.
There is no per-account anomaly in existence to detect. The signal lives entirely in
the aggregate: an implausible influx of unique listeners to one track, with no
corresponding engagement.

### Why this matters for a keyed stream

Topology A evades track-level detection by spreading across content.
Topology B evades account-level detection by spreading across accounts.
They are near-inverses. **The choice of Kafka partition key determines which family
of fraud the pipeline is even capable of seeing.**

## How the operations are built

**Infrastructure.** Botnets built on residential proxy pools and VPNs, driving
browser automation (Selenium/Puppeteer) and anti-detect browsers, so each account
signs up and streams from a distinct residential IP in a plausible geography.
Account creation is deliberately spread across many residential IPs to stay under
per-IP rate limits and reputation systems.

**Camouflage.** Farms deliberately stream large volumes of *unrelated* artists to
bury the target's inflation. One published estimate: adding 100,000 fake streams to
one artist involves generating roughly **900,000 streams across other artists** to
hide the spike. The consequence is that fraudulent accounts look mostly legitimate
by volume — which is precisely the condition that defeats threshold-based anomaly
detection, and precisely what the camouflage-resistant graph literature was built
for (see FRAUDAR below).

**Supply side.** Cheap AI music generation is what makes spreading economical.
Smith's arrangement for up to 10,000 tracks/month is the enabling half of the
scheme; the bot network is only the demand half. Deezer's 75,000 AI tracks/day
figure suggests this is now the dominant shape of the problem.

**Account acquisition.** Not all accounts are fabricated. Beatdapp describes
detecting compromised real accounts — e.g. an account with five devices, two in
different locations from the other three, all listening to the same material as
10,000 other devices. Premium accounts obtained with stolen credentials or
fraudulent cards raise the per-stream payout, and distort premium-to-free ratios in
royalty data.

## Signals that actually detect it

| Signal | Detection level | Why it works |
|---|---|---|
| Streams-to-unique-listeners ratio | Track | 200K streams from 4K monthly listeners (50:1) is arithmetically implausible |
| Saves / follows / playlist-adds per stream | Track | Bots don't save. "100,000 streams and 12 saves is structurally suspicious" |
| Listen-duration **distribution** | Track | Bots cluster tightly at **30–31s** — the royalty trigger. Humans skip messily, producing a spread-out distribution |
| Geographic concentration vs. promotion | Track | e.g. 50K streams from one emerging-market metro in 48h with no social signal |
| Spike-then-collapse with no promo correlation | Track | Organic growth correlates with press, playlisting, or social activity |
| Off-peak / implausible listening hours | Track / account | Volume concentrated when human activity in that region is minimal |
| Shared device fingerprints across accounts | Cross-account | Same hardware driving many accounts |
| Residential-proxy / VPN attribution | Cross-account | Known bot infrastructure ranges |
| Signup cohorts, premium-to-free ratio | Cross-account | Bulk account creation; stolen-card premium mix |
| **Co-listening cohorts / lockstep behavior** | **Graph** | Sets of accounts streaming near-identical sets of tracks |
| Cross-platform correlation | Cross-platform | Beatdapp analyzes patterns across thousands of unrelated artist accounts simultaneously to find shared infrastructure |

Note the distribution of that middle column: almost nothing useful is per-account.

## The structural / graph layer

The strongest detector is structural, and it is a graph computation rather than a
keyed aggregation.

Model listeners and tracks as a **bipartite graph** (listener —played→ track). A
coordinated farm shows up as an unusually **dense subgraph** — a block in the
adjacency matrix — because a set of accounts touched a near-identical set of tracks.
The relevant prior work:

- **CopyCatch** (Facebook) — detects "lockstep" Page-Like behavior using only the
  bipartite user↔Page graph and edge creation times. Deployed in production across
  a billion-user graph.
- **FRAUDAR** (KDD 2016 best paper) — dense-subgraph detection that is explicitly
  **camouflage-resistant** and provides upper bounds on how much fraudsters can
  achieve while hiding. Directly targets the 900K-cover-streams tactic described
  above.
- Broader bipartite dense-subgraph and graph-clustering literature, including
  linear-time algorithms and hard-link/soft-link clustering approaches (device
  fingerprints as soft links, shared identity attributes as hard links).

Beatdapp's own described heuristic — an account "listening to the same thing as
10,000 other devices" — is a co-listening cohort detection, i.e. this exact pattern.

Production fraud architectures generally pair the two: streaming velocity/windowed
features in Kafka + Flink/ksqlDB for the fast path, and an entity graph
(accounts, devices, IPs, payment methods) with community-detection algorithms for
the ring-detection path.

## Out of scope for this project

Real detection stacks include layers that synthetic play events cannot support, and
the proposal should not claim them:

- Device fingerprinting and anti-detect browser detection
- Residential-proxy / VPN attribution
- Payment fraud and stolen-credential analysis
- Audio-level AI-generation detection (Deezer's classifiers on generator
  signatures; ~99.8% reported accuracy on real-vs-reconstructed audio)

## Implications for Option 4

The redesign below is now implemented in the proposal, and explained in plain terms in
[DESIGN-two-key-pipeline.md](DESIGN-two-key-pipeline.md). It is a **two-stage pipeline
with a repartition**:

```text
seeded_play_events.jsonl
   → producer
   → Stage 1 topic: play-events        (key = listener_id)
        per-account sessionization and behavioral features
   → re-key / repartition
   → Stage 2 topic: track-activity     (key = track_id)
        per-track distributional features:
          unique listeners, streams:listeners ratio,
          30–35s duration histogram, saves:streams ratio,
          geographic spread
   → review queue + artist exposure rollup
   → batch layer: listener↔track bipartite graph
        dense co-listening cohort detection
```

Three specific consequences:

1. **The repartition is the most instructive part of the project.** "We keyed by
   listener, found the fraud isn't visible per listener, and re-keyed by track" is a
   stronger and more honest demonstration of stream design than a single-key
   aggregation — and re-keying between stages is the standard production pattern.
2. **The 30-second rule survives but moves.** It is a per-track duration
   *distribution* signal, not a per-listener one; you need many plays of one track
   before the clustering is visible. It belongs in stage 2.
3. **Non-play event types should be added to the synthetic schema** (`save`,
   `follow`, `playlist_add`). Cheap to generate, and the engagement-ratio signal is
   among the strongest available.

The graph layer also converts the previously-optional graph store into a
load-bearing component: dense-bipartite-subgraph search over accumulated state is
what a graph database is for, and it is not naturally expressible as a keyed
streaming aggregation.

**The ethics framing gets stronger, not weaker.** Camouflage means individual
accounts look legitimate by construction, so confident automated judgment is
unavailable *in principle* rather than merely unwise. A review queue rather than a
verdict is the technically correct output, and the artist-side rollup should report
exposure rather than guilt — a false positive withholds money from an artist who did
nothing wrong, and independent artists have the least recourse when that happens.

## Source quality caveat

Platforms deliberately do not publish detection internals. Treat these tiers
differently:

- **Solid:** the DOJ filing (court record), the academic graph-fraud literature
  (CopyCatch, FRAUDAR, bipartite dense-subgraph work), Spotify's official policy
  statements, Deezer's published figures.
- **Directional:** industry and trade press reporting of vendor analysis (Beatdapp
  figures via Rolling Stone, Music Week).
- **Illustrative only:** specific numeric thresholds (50:1 streams-to-listeners,
  100K:12 streams-to-saves, 50K streams per metro in 48h). These come from
  industry commentary and describe the *shape* of a signal. They are **not**
  published platform thresholds and should not be presented as such.

## Sources

**Court record and case reporting**

- [DOJ / SDNY — North Carolina man pleads guilty to music streaming fraud aided by AI](https://www.justice.gov/usao-sdny/pr/north-carolina-man-pleads-guilty-music-streaming-fraud-aided-artificial-intelligence-0)
- [Help Net Security — fake AI songs streamed billions of times](https://www.helpnetsecurity.com/2026/03/20/ai-music-streaming-fraud-guilty-plea/)
- [AI Incident Database — incident 779](https://incidentdatabase.ai/cite/779/)

**Industry mechanics and detection**

- [Rolling Stone — Inside the Rise of Bots and Streaming Fraud in Music](https://www.rollingstone.com/music/music-features/bots-streaming-fraud-music-protection-1235537602/) (Beatdapp's 53,000-account farm; compromised-account heuristics)
- [Chartlex — Streaming Fraud Crackdown 2026](https://www.chartlex.com/blog/business/music-streaming-fraud-crackdown-2026) (detection layers, ratio signals, 30–31s clustering)
- [limbo — Streaming Fraud & Artificial Streaming Explained](https://www.limbomusic.com/blog-posts/streaming-fraud-artificial-streaming) (geographic, device, engagement signals)
- [HUMAN Security — AI-Powered Streaming Fraud](https://www.humansecurity.com/learn/blog/ai-powered-streaming-fraud/) (botnet infrastructure, residential proxies, Selenium/Puppeteer)
- [Dark Reading — Streaming Fraud Campaigns Rely on AI Tools, Bots](https://www.darkreading.com/threat-intelligence/streaming-fraud-campaigns-rely-on-ai-tools-bots)
- [artist.tools — What Is Streaming Fraud?](https://www.artist.tools/post/what-is-streaming-fraud) (camouflage ratio: 100K target vs 900K cover streams)

**Platform policy and published figures**

- [Spotify for Artists — Artificial Streaming](https://artists.spotify.com/artificial-streaming)
- [Spotify — Track monetization eligibility](https://support.spotify.com/us/artists/article/track-monetization-eligibility/) (30-second stream threshold; 1,000-stream and unique-listener minimums)
- [Deezer newsroom — 85% of AI-track streams confirmed fraudulent](https://newsroom-deezer.com/2026/01/deezer-confirms-demonetization-of-ai-music/)
- [Music Week — inside the industry's streaming fraud response](https://www.musicweek.com/digital/read/inside-the-industry-s-streaming-fraud-response-as-deezer-now-logs-75-000-ai-based-tracks-per-day/093982)

**Graph and algorithmic literature**

- [FRAUDAR: Bounding Graph Fraud in the Face of Camouflage (KDD 2016)](https://dl.acm.org/doi/10.1145/2939672.2939747)
- [On Finding Dense Subgraphs in Bipartite Graphs: Linear Algorithms with Applications to Fraud Detection](https://arxiv.org/pdf/1810.06809)
- [Fraud Detection Through Large-Scale Graph Clustering with Heterogeneous Link Transformation](https://arxiv.org/abs/2512.19061)
- [safe-graph/graph-fraud-detection-papers — curated bibliography](https://github.com/safe-graph/graph-fraud-detection-papers)
- [Detecting music deepfakes is easy but actually hard (Deezer)](https://arxiv.org/pdf/2405.04181)

**Streaming architecture reference**

- [Confluent — Real-Time Streaming Architecture Examples and Patterns](https://www.confluent.io/learn/real-time-streaming-architecture-examples/)
- [Conduktor — Real-Time Fraud Detection with Streaming](https://www.conduktor.io/glossary/real-time-fraud-detection-with-streaming)
