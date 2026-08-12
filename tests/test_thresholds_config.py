"""Threshold configuration tests (CTRT-04).

Run with:

    PYTHONPATH=. python3 -m pytest tests/test_thresholds_config.py -q

These tests are deliberately self-sufficient: they do not rely on a root
`conftest.py` existing, because the fixture plan creates that file in the same
wave and it may not be present yet. The sys.path bootstrap below is the same
pattern `src/replay_to_kafka.py` already uses.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config import Thresholds, load_thresholds  # noqa: E402

PROD_CONFIG = REPO_ROOT / "config" / "thresholds.json"
FIXTURE_CONFIG = REPO_ROOT / "config" / "thresholds.fixture.json"


def _prod_payload() -> dict:
    """The production config as raw JSON, for mutation in rejection tests."""
    return json.loads(PROD_CONFIG.read_text(encoding="utf-8"))


def _write(tmp_path: Path, payload: dict) -> Path:
    path = tmp_path / "thresholds.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


# --- The production numbers (CD-4, CD-5) -------------------------------------


def test_production_config_carries_the_cd4_topology_a_numbers():
    t = load_thresholds(str(PROD_CONFIG))
    assert t.topology_a_plays_over == 300
    assert t.topology_a_window_hours == 24


def test_production_config_carries_the_cd5_topology_b_numbers():
    t = load_thresholds(str(PROD_CONFIG))
    assert t.topology_b_min_unique_listeners == 200
    assert t.topology_b_max_plays_per_listener == pytest.approx(1.1)
    assert t.topology_b_min_band_share == pytest.approx(0.60)
    assert t.topology_b_window_hours == 1


def test_loader_records_which_config_produced_the_run():
    t = load_thresholds(str(PROD_CONFIG))
    assert t.source_path.endswith("config/thresholds.json")


# --- One schema, two scales ---------------------------------------------------


def test_fixture_config_parses_under_the_identical_schema():
    t = load_thresholds(str(FIXTURE_CONFIG))
    assert isinstance(t, Thresholds)


def test_fixture_config_shrinks_only_the_volume_thresholds():
    prod = load_thresholds(str(PROD_CONFIG))
    fixture = load_thresholds(str(FIXTURE_CONFIG))

    # Volume shrinks, because the mini cohorts are shrunk for readability.
    assert fixture.topology_a_plays_over < prod.topology_a_plays_over
    assert fixture.topology_b_min_unique_listeners < prod.topology_b_min_unique_listeners

    # The *shape* of fraud does not change with cohort size, so these do not.
    assert fixture.topology_a_window_hours == prod.topology_a_window_hours
    assert fixture.topology_b_window_hours == prod.topology_b_window_hours
    assert fixture.topology_b_max_plays_per_listener == prod.topology_b_max_plays_per_listener
    assert fixture.topology_b_min_band_share == prod.topology_b_min_band_share


# --- No hardcoded fallbacks ---------------------------------------------------


def test_constructing_thresholds_with_no_arguments_raises():
    """The enforcement mechanism for 'never hardcoded'.

    No threshold field carries a default, so a detector cannot silently fall
    back to a built-in number when the config is absent or partial.
    """
    with pytest.raises(ValidationError):
        Thresholds()


def test_every_threshold_field_is_required():
    required = {
        name
        for name, field in Thresholds.model_fields.items()
        if field.is_required()
    }
    assert required == {
        "topology_a_window_hours",
        "topology_a_plays_over",
        "topology_b_window_hours",
        "topology_b_min_unique_listeners",
        "topology_b_max_plays_per_listener",
        "topology_b_min_band_share",
    }


# --- Out-of-range and unknown-key configs are rejected at load time -----------


def test_band_share_above_one_is_rejected(tmp_path):
    payload = _prod_payload()
    payload["topology_b_min_band_share"] = 1.5
    with pytest.raises(ValidationError):
        load_thresholds(str(_write(tmp_path, payload)))


def test_zero_topology_a_play_threshold_is_rejected(tmp_path):
    payload = _prod_payload()
    payload["topology_a_plays_over"] = 0
    with pytest.raises(ValidationError):
        load_thresholds(str(_write(tmp_path, payload)))


def test_plays_per_listener_below_one_is_rejected(tmp_path):
    """Fewer than one play per unique listener is arithmetically impossible."""
    payload = _prod_payload()
    payload["topology_b_max_plays_per_listener"] = 0.5
    with pytest.raises(ValidationError):
        load_thresholds(str(_write(tmp_path, payload)))


def test_unknown_key_is_rejected(tmp_path):
    payload = _prod_payload()
    payload["topology_b_min_band_shar"] = 0.6  # typo, must not be ignored
    with pytest.raises(ValidationError):
        load_thresholds(str(_write(tmp_path, payload)))


def test_missing_threshold_key_is_rejected(tmp_path):
    payload = _prod_payload()
    del payload["topology_a_plays_over"]
    with pytest.raises(ValidationError):
        load_thresholds(str(_write(tmp_path, payload)))


# --- Path resolution ----------------------------------------------------------


def test_env_var_selects_the_config_when_called_with_no_argument(monkeypatch):
    monkeypatch.setenv("THRESHOLDS_PATH", str(FIXTURE_CONFIG))
    assert load_thresholds().topology_a_plays_over == 10


def test_no_argument_and_no_env_var_falls_back_to_the_production_config(monkeypatch):
    monkeypatch.delenv("THRESHOLDS_PATH", raising=False)
    assert load_thresholds().topology_a_plays_over == 300


def test_missing_config_path_raises_naming_the_path():
    with pytest.raises(FileNotFoundError) as excinfo:
        load_thresholds("config/no-such-thresholds.json")
    assert "no-such-thresholds.json" in str(excinfo.value)


# --- The stop-band edges belong to the contract, not to this config ----------


def test_stop_band_edges_are_absent_from_the_threshold_model():
    """The band *share* is tunable; the band *definition* is not.

    30 <= played_seconds <= 35 is a contract semantic shared with the producer
    and lives in contracts/play_event_v1.py. If it were duplicated here, the
    two sides could silently diverge on a number that never fails a test, only
    reports a wrong percentage.
    """
    fields = set(Thresholds.model_fields)
    assert {f for f in fields if "band" in f} == {"topology_b_min_band_share"}
    for forbidden in (
        "stop_band_min_seconds",
        "stop_band_max_seconds",
        "band_low_seconds",
        "band_high_seconds",
        "stop_band_start",
        "stop_band_end",
    ):
        assert forbidden not in fields


def test_a_config_declaring_band_edges_is_rejected(tmp_path):
    payload = _prod_payload()
    payload["stop_band_min_seconds"] = 30
    payload["stop_band_max_seconds"] = 35
    with pytest.raises(ValidationError):
        load_thresholds(str(_write(tmp_path, payload)))
