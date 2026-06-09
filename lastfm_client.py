"""Last.fm API client for enriching Spotify artist data with play counts and tags.

Provides artist popularity metrics (listeners, playcount) and community-curated
tags as a replacement for the removed Spotify popularity/followers fields.

Target table: SPOTIFY_ANALYTICS.RAW_DATA.LASTFM_ENRICHMENT
Join keys: artist_name (primary), mbid (secondary when available)
"""

import time
import requests
from typing import Dict, List, Optional
from datetime import datetime, timezone
import structlog

from config import settings

logger = structlog.get_logger(__name__)

# Table shape for SPOTIFY_ANALYTICS.RAW_DATA.LASTFM_ENRICHMENT:
# CREATE TABLE IF NOT EXISTS SPOTIFY_ANALYTICS.RAW_DATA.LASTFM_ENRICHMENT (
#     artist_name       VARCHAR(512) NOT NULL,
#     mbid              VARCHAR(64),
#     listeners         INTEGER,
#     playcount         BIGINT,
#     top_tags          VARIANT,       -- JSON array of {name, count} objects
#     similar_artists   VARIANT,       -- JSON array of {name, mbid, match} objects
#     bio_summary       VARCHAR,
#     lastfm_url        VARCHAR(1024),
#     enriched_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
#     source_artist_id  VARCHAR(64),   -- Spotify artist_id used to trigger lookup
#     PRIMARY KEY (artist_name)
# );

LASTFM_API_BASE = "http://ws.audioscrobbler.com/2.0/"


class LastfmClient:
    """Rate-limited Last.fm API client for artist enrichment."""

    def __init__(self, api_key: Optional[str] = None, rate_limit_per_sec: Optional[float] = None):
        self.api_key = api_key or settings.lastfm_api_key
        if not self.api_key:
            raise ValueError("LASTFM_API_KEY is required. Set it in .env or environment.")
        self.rate_limit_per_sec = rate_limit_per_sec or settings.lastfm_rate_limit_per_sec
        self._min_interval = 1.0 / min(self.rate_limit_per_sec, 5.0)  # cap at 5 req/s max
        self._last_request_time: float = 0.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "SpotifySnowflakePipeline/1.0"})
        logger.info("LastfmClient initialized", rate_limit=self.rate_limit_per_sec)

    def _rate_limit(self) -> None:
        """Enforce minimum interval between requests (~3 req/s effective)."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    def _request(self, method: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make a rate-limited request to the Last.fm API."""
        self._rate_limit()
        request_params = {
            "method": method,
            "api_key": self.api_key,
            "format": "json",
        }
        if params:
            request_params.update(params)

        try:
            response = self.session.get(LASTFM_API_BASE, params=request_params, timeout=15)
            response.raise_for_status()
            data = response.json()
            if "error" in data:
                logger.warning(
                    "Last.fm API error",
                    method=method,
                    error_code=data.get("error"),
                    message=data.get("message"),
                )
                return None
            return data
        except requests.exceptions.HTTPError as e:
            logger.error("Last.fm HTTP error", method=method, status=e.response.status_code)
            return None
        except requests.exceptions.RequestException as e:
            logger.error("Last.fm request failed", method=method, error=str(e))
            return None

    def get_artist_info(self, artist_name: str) -> Optional[Dict]:
        """Fetch artist info including listeners, playcount, tags, and bio.

        Returns a row dict matching the LASTFM_ENRICHMENT table shape, or None on failure.
        """
        data = self._request("artist.getinfo", {"artist": artist_name, "autocorrect": "1"})
        if not data or "artist" not in data:
            return None

        artist = data["artist"]
        stats = artist.get("stats", {})
        tags_raw = artist.get("tags", {}).get("tag", [])
        bio = artist.get("bio", {})

        top_tags = [{"name": t.get("name"), "count": int(t.get("count", 0))} for t in tags_raw]

        return {
            "artist_name": artist.get("name", artist_name),
            "mbid": artist.get("mbid") or None,
            "listeners": int(stats.get("listeners", 0)),
            "playcount": int(stats.get("playcount", 0)),
            "top_tags": top_tags,
            "similar_artists": None,  # populated separately via get_similar_artists
            "bio_summary": (bio.get("summary") or "")[:2000],
            "lastfm_url": artist.get("url"),
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        }

    def get_similar_artists(self, artist_name: str, limit: int = 10) -> List[Dict]:
        """Fetch similar artists for a given artist.

        Returns list of {name, mbid, match} dicts (match is a 0-1 similarity float).
        """
        data = self._request(
            "artist.getsimilar", {"artist": artist_name, "limit": str(limit), "autocorrect": "1"}
        )
        if not data or "similarartists" not in data:
            return []

        similar_raw = data["similarartists"].get("artist", [])
        return [
            {
                "name": s.get("name"),
                "mbid": s.get("mbid") or None,
                "match": float(s.get("match", 0)),
            }
            for s in similar_raw
        ]

    def enrich_artist(self, artist_name: str, source_artist_id: Optional[str] = None) -> Optional[Dict]:
        """Full enrichment for one artist: info + similar artists combined.

        Returns a complete row dict for the LASTFM_ENRICHMENT table.
        """
        info = self.get_artist_info(artist_name)
        if not info:
            return None

        similar = self.get_similar_artists(artist_name)
        info["similar_artists"] = similar
        info["source_artist_id"] = source_artist_id
        return info

    def enrich_batch(
        self, artists: List[Dict], batch_size: int = 100
    ) -> List[Dict]:
        """Enrich a batch of artists.

        Args:
            artists: list of dicts with at minimum 'artist_name'; optionally 'artist_id'.

        Returns:
            List of enrichment row dicts ready for upload.
        """
        results = []
        total = min(len(artists), batch_size)
        for i, artist in enumerate(artists[:total]):
            artist_name = artist.get("artist_name")
            artist_id = artist.get("artist_id")
            if not artist_name:
                continue

            logger.debug("Enriching artist via Last.fm", artist=artist_name, progress=f"{i+1}/{total}")
            row = self.enrich_artist(artist_name, source_artist_id=artist_id)
            if row:
                results.append(row)

        logger.info("Last.fm batch enrichment complete", requested=total, enriched=len(results))
        return results
