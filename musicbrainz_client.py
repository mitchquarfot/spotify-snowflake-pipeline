"""MusicBrainz API client for enriching Spotify artist data with metadata.

Uses musicbrainzngs to look up artists by ISRC or name, retrieving MBID,
tags, area/country, type (person/group), and active years.

Target table: SPOTIFY_ANALYTICS.RAW_DATA.MUSICBRAINZ_ARTIST_METADATA
Join keys: artist_name (fuzzy), mbid (exact when known), isrc (recording-level)
"""

import time
from typing import Dict, List, Optional
from datetime import datetime, timezone
import structlog
import musicbrainzngs

from config import settings

logger = structlog.get_logger(__name__)

# Table shape for SPOTIFY_ANALYTICS.RAW_DATA.MUSICBRAINZ_ARTIST_METADATA:
# CREATE TABLE IF NOT EXISTS SPOTIFY_ANALYTICS.RAW_DATA.MUSICBRAINZ_ARTIST_METADATA (
#     mbid              VARCHAR(64) NOT NULL,
#     artist_name       VARCHAR(512),
#     sort_name         VARCHAR(512),
#     artist_type       VARCHAR(64),       -- Person, Group, Orchestra, etc.
#     country           VARCHAR(8),
#     area              VARCHAR(256),
#     begin_date        VARCHAR(16),
#     end_date          VARCHAR(16),
#     disambiguation    VARCHAR(512),
#     tags              VARIANT,           -- JSON array of {name, count} objects
#     isrc_lookup_key   VARCHAR(16),       -- ISRC used to find this artist (if any)
#     source_artist_id  VARCHAR(64),       -- Spotify artist_id that triggered lookup
#     enriched_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
#     PRIMARY KEY (mbid)
# );


class MusicBrainzClient:
    """Rate-limited MusicBrainz client using musicbrainzngs."""

    def __init__(
        self,
        user_agent: Optional[str] = None,
        rate_limit_per_sec: Optional[float] = None,
    ):
        ua = user_agent or settings.musicbrainz_user_agent
        self.rate_limit_per_sec = rate_limit_per_sec or settings.musicbrainz_rate_limit_per_sec
        self._min_interval = 1.0 / self.rate_limit_per_sec  # MusicBrainz requires 1 req/s
        self._last_request_time: float = 0.0

        # Configure musicbrainzngs
        app_name, app_version_contact = ua.rsplit("/", 1) if "/" in ua else (ua, "1.0")
        musicbrainzngs.set_useragent(
            app_name.strip(),
            "1.0",
            contact=app_version_contact.strip("() ") if "(" in ua else None,
        )
        logger.info("MusicBrainzClient initialized", user_agent=ua)

    def _rate_limit(self) -> None:
        """Enforce minimum interval between requests (1 req/s per MusicBrainz policy)."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    def lookup_artist_by_name(self, artist_name: str) -> Optional[Dict]:
        """Search for an artist by name and return metadata for the best match.

        Returns a row dict matching MUSICBRAINZ_ARTIST_METADATA table shape, or None.
        """
        self._rate_limit()
        try:
            result = musicbrainzngs.search_artists(artist=artist_name, limit=1)
        except musicbrainzngs.WebServiceError as e:
            logger.error("MusicBrainz search failed", artist=artist_name, error=str(e))
            return None

        artists = result.get("artist-list", [])
        if not artists:
            logger.debug("No MusicBrainz match", artist=artist_name)
            return None

        return self._parse_artist(artists[0])

    def lookup_artist_by_isrc(self, isrc: str) -> Optional[Dict]:
        """Look up the artist of a recording by ISRC.

        Returns artist metadata dict or None.
        """
        self._rate_limit()
        try:
            result = musicbrainzngs.get_recordings_by_isrc(isrc, includes=["artists"])
        except musicbrainzngs.WebServiceError as e:
            logger.error("MusicBrainz ISRC lookup failed", isrc=isrc, error=str(e))
            return None

        recordings = result.get("isrc", {}).get("recording-list", [])
        if not recordings:
            return None

        # Take the first recording's first artist
        artist_credits = recordings[0].get("artist-credit", [])
        if not artist_credits:
            return None

        artist_ref = artist_credits[0].get("artist", {})
        mbid = artist_ref.get("id")
        if not mbid:
            return None

        return self._lookup_artist_by_mbid(mbid, isrc_lookup_key=isrc)

    def _lookup_artist_by_mbid(self, mbid: str, isrc_lookup_key: Optional[str] = None) -> Optional[Dict]:
        """Fetch full artist details by MBID including tags."""
        self._rate_limit()
        try:
            result = musicbrainzngs.get_artist_by_id(mbid, includes=["tags"])
        except musicbrainzngs.WebServiceError as e:
            logger.error("MusicBrainz artist lookup failed", mbid=mbid, error=str(e))
            return None

        artist = result.get("artist")
        if not artist:
            return None

        row = self._parse_artist(artist)
        if row and isrc_lookup_key:
            row["isrc_lookup_key"] = isrc_lookup_key
        return row

    def _parse_artist(self, artist: Dict) -> Optional[Dict]:
        """Parse a MusicBrainz artist dict into a table row."""
        mbid = artist.get("id")
        if not mbid:
            return None

        life_span = artist.get("life-span", {})
        tags_raw = artist.get("tag-list", [])
        tags = [
            {"name": t.get("name"), "count": int(t.get("count", 0))}
            for t in tags_raw
            if t.get("name")
        ]

        area = artist.get("area", {})

        return {
            "mbid": mbid,
            "artist_name": artist.get("name"),
            "sort_name": artist.get("sort-name"),
            "artist_type": artist.get("type"),
            "country": artist.get("country"),
            "area": area.get("name"),
            "begin_date": life_span.get("begin"),
            "end_date": life_span.get("end"),
            "disambiguation": artist.get("disambiguation"),
            "tags": tags,
            "isrc_lookup_key": None,
            "source_artist_id": None,
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        }

    def enrich_artist(
        self, artist_name: str, isrc: Optional[str] = None, source_artist_id: Optional[str] = None
    ) -> Optional[Dict]:
        """Enrich a single artist: prefer ISRC lookup, fallback to name search.

        Returns a row dict or None.
        """
        row = None
        if isrc:
            row = self.lookup_artist_by_isrc(isrc)

        if not row:
            row = self.lookup_artist_by_name(artist_name)

        if row and source_artist_id:
            row["source_artist_id"] = source_artist_id

        return row

    def enrich_batch(self, artists: List[Dict], batch_size: int = 100) -> List[Dict]:
        """Enrich a batch of artists.

        Args:
            artists: list of dicts with 'artist_name'; optionally 'artist_id' and 'isrc'.

        Returns:
            List of enrichment row dicts ready for upload.
        """
        results = []
        total = min(len(artists), batch_size)
        for i, artist in enumerate(artists[:total]):
            artist_name = artist.get("artist_name")
            if not artist_name:
                continue

            logger.debug(
                "Enriching artist via MusicBrainz", artist=artist_name, progress=f"{i+1}/{total}"
            )
            row = self.enrich_artist(
                artist_name,
                isrc=artist.get("isrc"),
                source_artist_id=artist.get("artist_id"),
            )
            if row:
                results.append(row)

        logger.info("MusicBrainz batch enrichment complete", requested=total, enriched=len(results))
        return results
