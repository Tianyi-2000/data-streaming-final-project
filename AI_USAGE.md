# AI_USAGE.md

## Summary
Our project satisfies the "one bounded AI component **or AI-assisted workflow**"
requirement through a **disclosed AI-assisted development workflow**. We used
AI coding assistants (Anthropic's Claude, via Claude Code) as pair programmers
to help design and implement the pipeline. AI did **not** run inside the data
pipeline at inference time — every runtime component is plain, inspectable
Python that we can run and explain ourselves. AI accelerated the engineering;
it did not replace our ownership of the design, code, testing, or explanation.

Both teammates used an AI assistant on their own half:
- **Tianyi (producer):** MusicBrainz catalog fetch, synthetic event generator,
  the shared `PlayEventV1` contract, the Kafka replay script, and the local
  Redpanda/Docker setup.
- **PJ (consumer):** event-time windowing, the two detection consumers, the
  threshold config, and the test suite.

## The task AI handled
- Turning natural-language requirements + the frozen contract into working code
  (schema model, generators, replay, consumers, tests, Docker config).
- Explaining trade-offs (e.g., why Redpanda for a free local Kafka) and drafting
  documentation (`README`, `DATA_SOURCE.md`, this file).

## Representative input / output
**Input (our request, paraphrased):**
> "Generate ~45k synthetic `PlayEventV1` events on the real MusicBrainz catalog,
> with three cohorts (normal, a >300/24h volume bot, and a viral one-play-per-
> account burst), validate every event against the contract, and write them in
> nondecreasing event_time order."

**Output (accepted after review):** `src/generate_events.py`, which produces a
reproducible `data/play_events.jsonl`. Verified end-to-end: the live consumers
flagged exactly the 8 planted bots (A000–A007) and the 1 planted viral window
(901 accounts, 92% early-stop) — matching what we designed.

## What we accepted vs. rejected / corrected
**Accepted:** the three-cohort generator design; Redpanda-in-Docker as the free
local Kafka; the shared Pydantic contract module; keeping PJ's consumer as-is
(it was correct and matched the contract — we deliberately did not over-build).

**Rejected / corrected (human review caught these):**
- **Reproducibility bug:** the AI's first generator seeded its RNG with Python's
  built-in `hash()`, which is randomized per process — this broke the contract's
  "stable across replay" rule. We caught it (two runs produced different files)
  and switched to a stable SHA-256 seed. Now byte-identical per `--seed`.
- **Kafka partition mistake:** an early manual step created the topics with 1
  partition; PJ's design needs 3 (his per-key watermarks are built for multi-
  partition). The test suite caught it; we recreated topics with `create_topics.py`.

## Verification methods applied
- **Automated tests:** the full suite passes (271 passed, 2 skipped), including
  contract, windowing, two-key separation, and end-to-end consumer tests.
- **Empirical rule checks:** confirmed normal listeners peak at 60 plays/day
  (never trip the 300/24h rule) and that Topology A never cross-triggers the
  Topology B rule.
- **Reproducibility:** re-ran the generator and diffed output (byte-identical).
- **Live end-to-end run:** replayed 45,473 events through real Kafka and
  inspected the produced `output/*.json` review queues by hand.

## Known limitations & fallback
- **Limitation:** AI can introduce subtle, plausible-looking bugs (the `hash()`
  reproducibility bug above is a real example). We treat all AI output as a draft
  to be reviewed, tested, and run — never trusted blindly.
- **Fallback:** every component is standard Python with pinned dependencies and
  tests. The project runs, and we can read, explain, and modify it, entirely
  without any AI assistant.
