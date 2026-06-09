"""Enrichment pipeline orchestrator for Last.fm and MusicBrainz data hydration.

Mirrors the batch-processing pattern of artist_genre_processor.py: loads a list
of artists needing enrichment, processes them in rate-limited batches via the
Last.fm and MusicBrainz clients, and uploads results to S3 for Snowpipe ingestion
into RAW_DATA.LASTFM_ENRICHMENT and RAW_DATA.MUSICBRAINZ_ARTIST_METADATA.
"""

import json
import gzip
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
import structlog

from config import settings
from lastfm_client import LastfmClient
from musicbrainz_client import MusicBrainzClient
from s3_client import S3Client

logger = structlog.get_logger(__name__)


class EnrichmentPipeline:
    """Orchestrates external data enrichment for Spotify artists."""

    def __init__(
        self,
        lastfm_client: Optional[LastfmClient] = None,
        musicbrainz_client: Optional[MusicBrainzClient] = None,
        s3_client: Optional[S3Client] = None,
    ):
        self.lastfm = lastfm_client or LastfmClient()
        self.musicbrainz = musicbrainz_client or MusicBrainzClient()
        self.s3_client = s3_client or S3Client()
        # TODO: Migrate enrichment state to Snowflake PIPELINE_STATE table for
        # consistency with pipeline.py (currently uses a local JSON file).
        self.state_file = "enrichment_state.json"
        self.processed_lastfm: Set[str] = set()
        self.processed_musicbrainz: Set[str] = set()
        self._load_state()
        logger.info(
            "EnrichmentPipeline initialized",
            lastfm_processed=len(self.processed_lastfm),
            musicbrainz_processed=len(self.processed_musicbrainz),
        )

    def _load_state(self) -> None:
        """Load previously enriched artist names to avoid duplicates."""
        try:
            with open(self.state_file, "r") as f:
                state = json.load(f)
            self.processed_lastfm = set(state.get("processed_lastfm", []))
            self.processed_musicbrainz = set(state.get("processed_musicbrainz", []))
            logger.info("Loaded enrichment state", file=self.state_file)
        except FileNotFoundError:
            logger.info("No existing enrichment state file, starting fresh")
        except Exception as e:
            logger.error("Failed to load enrichment state", error=str(e))

    def _save_state(self) -> None:
        """Persist enrichment state to disk."""
        try:
            state = {
                "processed_lastfm": list(self.processed_lastfm),
                "processed_musicbrainz": list(self.processed_musicbrainz),
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error("Failed to save enrichment state", error=str(e))

    def _upload_to_s3(self, records: List[Dict], prefix: str, label: str) -> Optional[str]:
        """Compress and upload a batch of JSON records to S3."""
        if not records:
            return None

        timestamp = datetime.now(timezone.utc)
        date_path = timestamp.strftime(settings.pipeline.date_partition_format)
        ts_str = timestamp.strftime("%Y%m%d_%H%M%S")
        s3_key = f"{prefix}{date_path}/{label}_{ts_str}.json.gz"

        jsonl = "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in records)
        compressed = gzip.compress(jsonl.encode("utf-8"))

        try:
            self.s3_client.s3_client.put_object(
                Bucket=self.s3_client.bucket_name,
                Key=s3_key,
                Body=compressed,
                ContentType="application/gzip",
                ContentEncoding="gzip",
                Metadata={
                    "source": label,
                    "record_count": str(len(records)),
                    "format": "jsonl",
                    "ingestion_timestamp": timestamp.isoformat(),
                },
            )
            logger.info("Uploaded enrichment batch to S3", s3_key=s3_key, records=len(records))
            return s3_key
        except Exception as e:
            logger.error("S3 upload failed", s3_key=s3_key, error=str(e))
            return None

    def run_lastfm_enrichment(self, artists: List[Dict], batch_size: int = 100) -> Dict:
        """Run Last.fm enrichment for a list of artists.

        Args:
            artists: list of dicts with 'artist_name' and optionally 'artist_id'.
            batch_size: max artists to process in this run.

        Returns:
            Summary dict with counts.
        """
        logger.info("Starting Last.fm enrichment", candidates=len(artists))
        new_artists = [
            a for a in artists if a.get("artist_name") not in self.processed_lastfm
        ][:batch_size]

        if not new_artists:
            logger.info("No new artists to enrich via Last.fm")
            return {"source": "lastfm", "requested": 0, "enriched": 0, "uploaded": False}

        results = self.lastfm.enrich_batch(new_artists, batch_size=batch_size)

        s3_key = self._upload_to_s3(results, "lastfm_enrichment/", "lastfm_artists")

        for r in results:
            self.processed_lastfm.add(r["artist_name"])
        self._save_state()

        summary = {
            "source": "lastfm",
            "requested": len(new_artists),
            "enriched": len(results),
            "uploaded": s3_key is not None,
            "s3_key": s3_key,
        }
        logger.info("Last.fm enrichment complete", **summary)
        return summary

    def run_musicbrainz_enrichment(self, artists: List[Dict], batch_size: int = 100) -> Dict:
        """Run MusicBrainz enrichment for a list of artists.

        Args:
            artists: list of dicts with 'artist_name'; optionally 'artist_id', 'isrc'.
            batch_size: max artists to process in this run.

        Returns:
            Summary dict with counts.
        """
        logger.info("Starting MusicBrainz enrichment", candidates=len(artists))
        new_artists = [
            a for a in artists if a.get("artist_name") not in self.processed_musicbrainz
        ][:batch_size]

        if not new_artists:
            logger.info("No new artists to enrich via MusicBrainz")
            return {"source": "musicbrainz", "requested": 0, "enriched": 0, "uploaded": False}

        results = self.musicbrainz.enrich_batch(new_artists, batch_size=batch_size)

        s3_key = self._upload_to_s3(results, "musicbrainz_enrichment/", "mb_artists")

        for r in results:
            name = r.get("artist_name")
            if name:
                self.processed_musicbrainz.add(name)
        self._save_state()

        summary = {
            "source": "musicbrainz",
            "requested": len(new_artists),
            "enriched": len(results),
            "uploaded": s3_key is not None,
            "s3_key": s3_key,
        }
        logger.info("MusicBrainz enrichment complete", **summary)
        return summary

    def run_full_enrichment(self, artists: List[Dict], batch_size: int = 100) -> Dict:
        """Run both Last.fm and MusicBrainz enrichment sequentially.

        Args:
            artists: list of dicts with 'artist_name' and optionally 'artist_id', 'isrc'.
            batch_size: max artists per source per run.

        Returns:
            Combined summary.
        """
        logger.info("Starting full enrichment pipeline", total_candidates=len(artists))
        start_time = time.time()

        lastfm_summary = self.run_lastfm_enrichment(artists, batch_size=batch_size)
        musicbrainz_summary = self.run_musicbrainz_enrichment(artists, batch_size=batch_size)

        duration_sec = round(time.time() - start_time, 1)
        combined = {
            "lastfm": lastfm_summary,
            "musicbrainz": musicbrainz_summary,
            "duration_sec": duration_sec,
            "total_enriched": lastfm_summary["enriched"] + musicbrainz_summary["enriched"],
        }
        logger.info("Full enrichment pipeline complete", **combined)
        return combined

    def get_stats(self) -> Dict:
        """Return current enrichment statistics."""
        return {
            "lastfm_processed": len(self.processed_lastfm),
            "musicbrainz_processed": len(self.processed_musicbrainz),
            "state_file": self.state_file,
        }
