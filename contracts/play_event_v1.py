"""Shared PlayEventV1 contract (producer + consumers).

This is the single source of truth for the event schema, per
CONTRACT-DECISIONS.md section 1/2. Both the producer (Tianyi) and the
consumers (PJ) import this module so the two sides can never drift.

Do not add fields here without a contract amendment. v1 deliberately
excludes country_code / save / follow / playlist_add.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# --- Kafka wiring facts, kept next to the schema so both sides agree ----------
TOPIC_PLAY_EVENTS = "play-events"        # producer output
TOPIC_TRACK_ACTIVITY = "track-activity"  # Consumer 1 output


def play_events_key(event: "PlayEventV1") -> bytes:
    """Kafka message key for the play-events topic: UTF-8 listener_id."""
    return event.listener_id.encode("utf-8")


def key_matches_listener_id(key: "bytes | str | None", event: "PlayEventV1") -> bool:
    """True when the Kafka message key equals the value's listener_id.

    PRODUCER-CONSUMER-CONTRACT.md section 6 lists "the Kafka key does not equal
    the value's listener_id" as an invalidity rule, but PlayEventV1 never sees
    the Kafka key -- key and value are independent byte strings on the wire --
    so the model structurally cannot enforce it. This helper is where that rule
    lives instead.

    Intended caller: Consumer 1 (Phase 3), which must assert this explicitly on
    every record it reads rather than trusting the producer to have set it.

    A missing key (None) is False: an unkeyed record cannot satisfy the rule.
    """
    if key is None:
        return False
    if isinstance(key, (bytes, bytearray)):
        try:
            key = bytes(key).decode("utf-8")
        except UnicodeDecodeError:
            return False
    return key == event.listener_id


class PlayEventV1(BaseModel):
    """A single validated 'play' event.

    Field names, types and constraints match PRODUCER-CONSUMER-CONTRACT.md.
    Construction raises pydantic.ValidationError on any violation, so
    `PlayEventV1.model_validate(d)` doubles as the reject-before-send gate.
    """

    # Reject unknown/extra fields outright so typos surface immediately.
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    event_id: str = Field(min_length=1)
    event_type: Literal["play"] = "play"
    listener_id: str = Field(min_length=1)
    track_id: str = Field(min_length=1)
    artist_id: str = Field(min_length=1)
    played_seconds: int = Field(ge=0)
    track_duration_seconds: int = Field(gt=0)
    event_time: str  # UTC ISO 8601, e.g. "2026-08-08T20:30:00Z"

    @field_validator("event_time")
    @classmethod
    def _event_time_is_utc_iso8601(cls, v: str) -> str:
        # Must end in 'Z' (UTC) and parse cleanly. We keep it as a string on
        # the model because the wire format is a string, but we prove it valid.
        if not v.endswith("Z"):
            raise ValueError("event_time must be UTC ISO 8601 ending in 'Z'")
        try:
            parsed = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"event_time is not valid ISO 8601: {v!r}") from exc
        if parsed.utcoffset() != timezone.utc.utcoffset(None):
            raise ValueError("event_time must be UTC")
        return v

    @model_validator(mode="after")
    def _played_within_duration(self) -> "PlayEventV1":
        if self.played_seconds > self.track_duration_seconds:
            raise ValueError(
                "played_seconds "
                f"({self.played_seconds}) exceeds track_duration_seconds "
                f"({self.track_duration_seconds})"
            )
        return self


def parse_event_time(event_time: str) -> datetime:
    """Convenience for consumers doing event-time windowing (aware UTC)."""
    return datetime.fromisoformat(event_time.replace("Z", "+00:00"))


def format_event_time(dt: datetime) -> str:
    """Render an aware UTC datetime back to the contract's 'Z' string form."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- The Topology B stop-time band, inclusive at both ends --------------------
# These are contract constants, not tunable configuration.
#
# The *share* of plays inside the band is a threshold Consumer 2 tunes, and that
# lives in config. The *definition* of the band is a semantic the producer and
# both consumers must agree on identically, or the metric silently diverges --
# so it lives here, next to the schema, where one edit reaches every side.
STOP_BAND_LOW_SECONDS = 30
STOP_BAND_HIGH_SECONDS = 35


def in_stop_band(played_seconds: int) -> bool:
    """True when played_seconds falls in the 30-35s band, inclusive at both ends.

    Inclusive is deliberate and load-bearing. The generator spreads Topology B
    stop times evenly across 30, 31, 32, 33, 34 and 35 -- it emits
    `rng.randint(30, min(35, dur))`, which includes 35 -- so an exclusive upper
    edge drops one sixth of the signal. Measured on the real stream, the band
    share on the fraud window is 0.762 exclusive versus 0.923 inclusive. Both
    clear the 0.60 threshold today, so flipping this raises no error anywhere;
    the cost of getting it wrong shows up as a quietly wrong number.

    This helper matches shipped producer behaviour rather than changing it.
    """
    return STOP_BAND_LOW_SECONDS <= played_seconds <= STOP_BAND_HIGH_SECONDS
