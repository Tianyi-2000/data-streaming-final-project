"""Detection threshold configuration (CTRT-04).

Every detection threshold in this project is a value in a JSON file, loaded
through the one schema below. The same schema serves the full-scale numbers in
`config/thresholds.json` and the fixture-scale numbers in
`config/thresholds.fixture.json`, so a four-dozen-event fixture exercises the
same code paths as the 45,473-event stream with different numbers.

Two design points worth keeping:

1. **No threshold field carries a default.** That is the enforcement mechanism
   for "never hardcoded": a detector cannot silently fall back to a built-in
   number when the config is absent or partial, because no built-in number
   exists. `tests/test_thresholds_config.py` proves it by constructing the
   model with no arguments and expecting a validation error.

2. **The stop-band edges are deliberately not here.** The band *share* is a
   tunable threshold; the band *definition* (`30 <= played_seconds <= 35`) is a
   contract semantic shared with the producer, so it lives in
   `contracts/play_event_v1.py`. Splitting them the other way is how the two
   sides silently diverge on a number that never fails a test.

Usage:
    from src.config import load_thresholds

    t = load_thresholds()                               # config/thresholds.json
    t = load_thresholds("config/thresholds.fixture.json")
    # or: THRESHOLDS_PATH=config/thresholds.fixture.json
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Optional, Union

from pydantic import BaseModel, ConfigDict, Field

# Same import bootstrap the repo's other scripts use, so this module resolves
# however it is invoked and never depends on a conftest.py being present.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

THRESHOLDS_PATH_ENV = "THRESHOLDS_PATH"
DEFAULT_THRESHOLDS_PATH = REPO_ROOT / "config" / "thresholds.json"


class Thresholds(BaseModel):
    """The detection thresholds for both topologies, loaded from JSON.

    Range constraints are on every field so a mistyped config fails at load
    time rather than at judgment time — a threshold set absurdly low would flag
    innocent artists, which is this project's stated ethical failure mode.
    """

    # An unknown or misspelled key must surface immediately rather than being
    # silently ignored while the threshold it was meant to set goes missing.
    model_config = ConfigDict(extra="forbid")

    # --- Topology A (CD-4): more than 300 plays in a rolling 24h window ------
    # The `_over` suffix is load-bearing: CD-4 says *more than*, a strict
    # comparison. A field named `min_plays` would invite an off-by-one that
    # quietly changes who gets flagged.
    topology_a_window_hours: int = Field(gt=0)
    topology_a_plays_over: int = Field(gt=0)

    # --- Topology B (CD-5): three conditions together, in a 1h window --------
    topology_b_window_hours: int = Field(gt=0)
    topology_b_min_unique_listeners: int = Field(gt=0)
    # Fewer than one play per unique listener is arithmetically impossible.
    topology_b_max_plays_per_listener: float = Field(ge=1.0)
    topology_b_min_band_share: float = Field(gt=0.0, le=1.0)

    # --- Non-threshold bookkeeping (these may carry defaults) ---------------
    # Provenance note carried in the file itself.
    comment: str = ""
    # Set by the loader after parsing, not read from the file. Phase 4 writes
    # flags into a review queue that must state the counts behind each flag;
    # carrying the config path means a flag can name the thresholds that
    # produced it, which is what makes a disputed flag auditable later.
    source_path: str = ""


def _resolve_path(path: Optional[Union[str, Path]]) -> Path:
    """Explicit argument, then THRESHOLDS_PATH, then the production config."""
    candidate = path if path is not None else os.environ.get(THRESHOLDS_PATH_ENV)
    if candidate is None:
        return DEFAULT_THRESHOLDS_PATH

    resolved = Path(candidate)
    if resolved.is_absolute():
        return resolved
    # Relative paths resolve against the caller's cwd when that works, and
    # against the repo root otherwise, so the loader behaves the same whether
    # it is called from the repo root or from a test's temp directory.
    if resolved.exists():
        return resolved
    return REPO_ROOT / resolved


def load_thresholds(path: Optional[Union[str, Path]] = None) -> Thresholds:
    """Load and validate the detection thresholds.

    Raises:
        FileNotFoundError: if the resolved config path does not exist.
        pydantic.ValidationError: if a threshold is missing, unknown, or out of
            its sane range.
    """
    resolved = _resolve_path(path)
    if not resolved.is_file():
        raise FileNotFoundError(
            f"threshold config not found: {resolved} "
            f"(set {THRESHOLDS_PATH_ENV} or pass an explicit path)"
        )

    data = json.loads(resolved.read_text(encoding="utf-8"))
    thresholds = Thresholds.model_validate(data)
    # The loader is authoritative for provenance; a value in the file cannot
    # spoof which config a run actually used.
    return thresholds.model_copy(update={"source_path": str(resolved)})
