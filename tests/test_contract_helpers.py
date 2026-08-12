"""Unit tests for the shared contract's consumer-side helpers.

These pin two semantics both sides of the seam must agree on identically:
the inclusive 30-35s stop band, and key-equals-listener_id.
"""

from __future__ import annotations

import pytest

from contracts.play_event_v1 import (
    STOP_BAND_HIGH_SECONDS,
    STOP_BAND_LOW_SECONDS,
    PlayEventV1,
    in_stop_band,
    key_matches_listener_id,
    play_events_key,
)


@pytest.fixture
def event_for_L0007() -> PlayEventV1:
    return PlayEventV1(
        event_id="e-0001",
        listener_id="L0007",
        track_id="T0042",
        artist_id="A0003",
        played_seconds=33,
        track_duration_seconds=210,
        event_time="2026-08-08T20:30:00Z",
    )


# --- the 30-35 second stop band, inclusive at both ends ----------------------


@pytest.mark.parametrize("played_seconds", [30, 32, 35])
def test_in_stop_band_true_inside_and_on_both_edges(played_seconds):
    # 35 is the whole point of the fix: the generator spreads Topology B stop
    # times evenly across 30..35, so an exclusive upper edge drops a sixth of
    # the signal (0.762 measured exclusive vs 0.923 inclusive).
    assert in_stop_band(played_seconds) is True


@pytest.mark.parametrize("played_seconds", [0, 29, 36])
def test_in_stop_band_false_outside(played_seconds):
    assert in_stop_band(played_seconds) is False


def test_stop_band_constants_are_the_documented_edges():
    assert STOP_BAND_LOW_SECONDS == 30
    assert STOP_BAND_HIGH_SECONDS == 35


# --- key vs. the value's listener_id ----------------------------------------


def test_key_matches_listener_id_accepts_bytes_key(event_for_L0007):
    assert key_matches_listener_id(b"L0007", event_for_L0007) is True


def test_key_matches_listener_id_accepts_str_key(event_for_L0007):
    # A str key behaves exactly like the bytes key.
    assert key_matches_listener_id("L0007", event_for_L0007) is True


def test_key_matches_listener_id_rejects_a_different_listener(event_for_L0007):
    assert key_matches_listener_id(b"L0008", event_for_L0007) is False


def test_key_matches_listener_id_rejects_missing_key(event_for_L0007):
    assert key_matches_listener_id(None, event_for_L0007) is False


def test_producer_key_function_and_consumer_check_agree(event_for_L0007):
    # The producer's key function fed straight back into the consumer's check
    # is True by construction -- the two sides cannot drift.
    assert key_matches_listener_id(play_events_key(event_for_L0007), event_for_L0007)
