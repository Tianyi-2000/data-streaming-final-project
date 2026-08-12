"""Fetch a real MusicBrainz catalog -> data/catalog.json (run once).

Per CONTRACT-DECISIONS.md section 6 the producer must NOT call MusicBrainz
live. Instead we fetch ~500 recordings across ~60 artists ONE time and cache
them locally with real MusicBrainz IDs and real durations. The generator then
reads only this file.

MusicBrainz etiquette (https://musicbrainz.org/doc/MusicBrainz_API):
  * Max ~1 request/second  -> we sleep between calls.
  * A descriptive User-Agent WITH a contact is required -> pass --contact.

Usage:
    python src/fetch_musicbrainz_catalog.py --contact you@usfca.edu

Output: data/catalog.json
    {
      "source": "musicbrainz",
      "artist_count": 60,
      "track_count": 500,
      "tracks": [
        {"track_id": "<recording-mbid>", "artist_id": "<artist-mbid>",
         "artist_name": "...", "title": "...", "track_duration_seconds": 214},
        ...
      ]
    }
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import requests

API = "https://musicbrainz.org/ws/2"
RATE_LIMIT_SECONDS = 1.1          # stay just under 1 req/sec
TARGET_TRACKS = 500
TARGET_ARTISTS = 60
MAX_PER_ARTIST = 9                # ~500 tracks / ~60 artists (contract target)

# ~70 well-known artists (buffer over the 60 target in case some don't resolve
# or lack recordings with durations). Spread across genres/eras for variety.
SEED_ARTISTS = [
    "Radiohead", "Taylor Swift", "Beyonce", "Drake", "Kendrick Lamar",
    "Adele", "Coldplay", "Daft Punk", "The Beatles", "Pink Floyd",
    "Nirvana", "Queen", "Michael Jackson", "Madonna", "David Bowie",
    "Fleetwood Mac", "Led Zeppelin", "The Rolling Stones", "U2", "Metallica",
    "Eminem", "Kanye West", "Rihanna", "Lady Gaga", "Bruno Mars",
    "Ed Sheeran", "Billie Eilish", "The Weeknd", "Ariana Grande", "Post Malone",
    "Foo Fighters", "Red Hot Chili Peppers", "Arctic Monkeys", "Muse",
    "The Killers", "Green Day", "Linkin Park", "Bon Iver", "Tame Impala",
    "Frank Ocean", "SZA", "Tyler, the Creator", "Childish Gambino", "Jay-Z",
    "Nas", "Snoop Dogg", "Dr. Dre", "OutKast", "Lauryn Hill",
    "Amy Winehouse", "Norah Jones", "John Mayer", "Jack Johnson", "Bob Dylan",
    "Johnny Cash", "Willie Nelson", "Dolly Parton", "Stevie Wonder",
    "Marvin Gaye", "Prince", "Elton John", "Paul Simon", "Sting",
    "Depeche Mode", "The Cure", "New Order", "Gorillaz", "Massive Attack",
    "Portishead", "Bjork", "Sigur Ros", "Aphex Twin",
]


def build_session(contact: str) -> requests.Session:
    s = requests.Session()
    # A real, descriptive User-Agent with contact is required by MusicBrainz.
    s.headers.update(
        {"User-Agent": f"msds682-final-project/1.0 ( {contact} )"}
    )
    return s


def get_json(session: requests.Session, url: str, params: dict) -> dict:
    """GET with fmt=json, basic 503/backoff handling and rate limiting."""
    params = {**params, "fmt": "json"}
    for attempt in range(4):
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 503:  # rate limited / busy -> back off
            wait = RATE_LIMIT_SECONDS * (attempt + 2)
            print(f"  503 from MusicBrainz, backing off {wait:.1f}s ...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        time.sleep(RATE_LIMIT_SECONDS)
        return resp.json()
    resp.raise_for_status()  # exhausted retries
    return {}


def resolve_artist_id(session: requests.Session, name: str) -> tuple[str, str] | None:
    data = get_json(session, f"{API}/artist", {"query": name, "limit": 1})
    artists = data.get("artists") or []
    if not artists:
        return None
    top = artists[0]
    return top["id"], top.get("name", name)


def fetch_artist_recordings(
    session: requests.Session, artist_id: str, want: int
) -> list[dict]:
    """Return up to `want` recordings that have a usable duration."""
    data = get_json(
        session,
        f"{API}/recording",
        {"artist": artist_id, "limit": 100},
    )
    recordings = data.get("recordings") or []
    # Deterministic order: sort by title, then MBID, so re-runs are stable.
    recordings.sort(key=lambda r: (r.get("title", ""), r.get("id", "")))

    picked: list[dict] = []
    seen_titles: set[str] = set()
    for rec in recordings:
        length_ms = rec.get("length")
        title = (rec.get("title") or "").strip()
        if not length_ms or length_ms <= 0 or not title:
            continue  # skip recordings with no real duration
        # De-dupe near-identical titles (live/remaster variants) per artist.
        key = title.lower()
        if key in seen_titles:
            continue
        seen_titles.add(key)
        picked.append(
            {
                "track_id": rec["id"],
                "title": title,
                "track_duration_seconds": round(length_ms / 1000),
            }
        )
        if len(picked) >= want:
            break
    return picked


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch MusicBrainz catalog cache.")
    parser.add_argument(
        "--contact",
        required=True,
        help="Contact email/URL for the required MusicBrainz User-Agent.",
    )
    parser.add_argument(
        "--out",
        default=str(Path(__file__).resolve().parents[1] / "data" / "catalog.json"),
        help="Output catalog path (default: data/catalog.json).",
    )
    args = parser.parse_args()

    session = build_session(args.contact)
    per_artist = -(-TARGET_TRACKS // TARGET_ARTISTS)  # ceil -> ~9

    tracks: list[dict] = []
    artists_used = 0
    for name in SEED_ARTISTS:
        if artists_used >= TARGET_ARTISTS or len(tracks) >= TARGET_TRACKS:
            break
        print(f"[{artists_used + 1}/{TARGET_ARTISTS}] {name} ...", flush=True)
        try:
            resolved = resolve_artist_id(session, name)
            if resolved is None:
                print(f"  skip: no artist match for {name!r}")
                continue
            artist_id, artist_name = resolved
            want = min(MAX_PER_ARTIST, TARGET_TRACKS - len(tracks))
            recs = fetch_artist_recordings(session, artist_id, want)
        except requests.RequestException as exc:
            print(f"  skip: network error for {name!r}: {exc}")
            continue
        if not recs:
            print(f"  skip: no recordings with durations for {name!r}")
            continue
        for r in recs:
            r["artist_id"] = artist_id
            r["artist_name"] = artist_name
        tracks.extend(recs)
        artists_used += 1
        print(f"  +{len(recs)} tracks (total {len(tracks)})")

    tracks = tracks[:TARGET_TRACKS]
    if len(tracks) < TARGET_TRACKS:
        print(
            f"\nWARNING: only collected {len(tracks)} tracks "
            f"(target {TARGET_TRACKS}). Add more SEED_ARTISTS or re-run.",
            file=sys.stderr,
        )

    distinct_artists = len({t["artist_id"] for t in tracks})
    catalog = {
        "source": "musicbrainz",
        "artist_count": distinct_artists,
        "track_count": len(tracks),
        "tracks": tracks,
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(catalog, indent=2, ensure_ascii=False))
    print(
        f"\nWrote {len(tracks)} tracks / {distinct_artists} artists -> {out_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
