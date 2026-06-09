"""Main pipeline orchestrator for Spotify to S3 data flow."""

import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple
import schedule
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from spotify_client import SpotifyClient
from s3_client import S3Client
from artist_genre_processor import ArtistGenreProcessor
from config import settings

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Snowflake connection helper
# ---------------------------------------------------------------------------

def _get_snowflake_connection():
    """Create a Snowflake connection using config settings."""
    import snowflake.connector

    connect_params: Dict = {
        "account": settings.snowflake.account,
        "user": settings.snowflake.user,
        "warehouse": settings.snowflake.warehouse,
        "database": settings.snowflake.database,
        "schema": "RAW_DATA",
    }
    if settings.snowflake.role:
        connect_params["role"] = settings.snowflake.role

    # Prefer key-pair auth, fall back to password
    if settings.snowflake.private_key_path or settings.snowflake.private_key:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend
        import os

        if settings.snowflake.private_key_path:
            with open(settings.snowflake.private_key_path, "rb") as key_file:
                key_data = key_file.read()
        else:
            key_data = settings.snowflake.private_key.encode()

        passphrase = (
            settings.snowflake.private_key_passphrase.encode()
            if settings.snowflake.private_key_passphrase
            else None
        )
        private_key = serialization.load_pem_private_key(
            key_data, password=passphrase, backend=default_backend()
        )
        pkb = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        connect_params["private_key"] = pkb
    elif settings.snowflake.password:
        connect_params["password"] = settings.snowflake.password

    return snowflake.connector.connect(**connect_params)


class SpotifyDataPipeline:
    """Main pipeline for streaming Spotify data to S3."""

    def __init__(self, enable_artist_genre_processing: bool = False):
        """Initialize pipeline with clients."""
        self.spotify_client = SpotifyClient()
        self.s3_client = S3Client()
        self.last_processed_timestamp = None

        # Optional artist-genre processing
        self.enable_artist_genre_processing = enable_artist_genre_processing
        if self.enable_artist_genre_processing:
            self.artist_genre_processor = ArtistGenreProcessor(
                spotify_client=self.spotify_client,
                s3_client=self.s3_client
            )
            logger.info("Pipeline initialized with artist-genre processing enabled")
        else:
            self.artist_genre_processor = None
            logger.info("Pipeline initialized")

        logger.info(
            "Pipeline configuration",
            s3_bucket=self.s3_client.bucket_name,
            batch_size=settings.pipeline.batch_size,
            fetch_interval_minutes=settings.pipeline.fetch_interval_minutes,
            max_runtime_minutes=getattr(settings.pipeline, "max_runtime_minutes", None),
            spotify_refresh_token_configured=bool(settings.spotify.refresh_token)
        )
        print(
            "Pipeline configuration: "
            f"S3 bucket='{self.s3_client.bucket_name}', "
            f"batch_size={settings.pipeline.batch_size}, "
            f"fetch_interval={settings.pipeline.fetch_interval_minutes}m, "
            f"max_runtime={getattr(settings.pipeline, 'max_runtime_minutes', None)}m, "
            f"has_refresh_token={'yes' if settings.spotify.refresh_token else 'no'}",
            flush=True
        )

        if not settings.spotify.refresh_token:
            warning_msg = (
                "WARNING: SPOTIFY_REFRESH_TOKEN is missing. GitHub Actions runs require a refresh token. "
                "Generate a fresh token locally and add it to the repository secrets."
            )
            logger.warning("Spotify refresh token missing")
            print(warning_msg, flush=True)

    # ------------------------------------------------------------------
    # State management (Snowflake-backed)
    # ------------------------------------------------------------------

    def load_state(self) -> Dict:
        """Load pipeline state from Snowflake PIPELINE_STATE table."""
        try:
            conn = _get_snowflake_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT state_key, state_value FROM SPOTIFY_ANALYTICS.RAW_DATA.PIPELINE_STATE"
                )
                rows = cursor.fetchall()
                state: Dict = {}
                for key, value in rows:
                    state[key] = json.loads(value) if isinstance(value, str) else value
                logger.info("Loaded pipeline state from Snowflake", keys=list(state.keys()))
                return state
            finally:
                conn.close()
        except Exception as e:
            logger.warning("Failed to load state from Snowflake, returning empty state", error=str(e))
            return {}

    def save_state(self, state: Dict):
        """Save pipeline state to Snowflake PIPELINE_STATE table (MERGE upsert)."""
        try:
            conn = _get_snowflake_connection()
            try:
                cursor = conn.cursor()
                for key, value in state.items():
                    cursor.execute(
                        """
                        MERGE INTO SPOTIFY_ANALYTICS.RAW_DATA.PIPELINE_STATE AS target
                        USING (SELECT %s AS state_key, PARSE_JSON(%s) AS state_value) AS source
                        ON target.state_key = source.state_key
                        WHEN MATCHED THEN
                            UPDATE SET state_value = source.state_value, updated_at = CURRENT_TIMESTAMP()
                        WHEN NOT MATCHED THEN
                            INSERT (state_key, state_value, updated_at)
                            VALUES (source.state_key, source.state_value, CURRENT_TIMESTAMP())
                        """,
                        (key, json.dumps(value)),
                    )
                logger.info("Saved pipeline state to Snowflake", keys=list(state.keys()))
            finally:
                conn.close()
        except Exception as e:
            logger.error("Failed to save state to Snowflake", error=str(e))

    def get_last_processed_timestamp(self) -> Optional[int]:
        """Get the timestamp of the last processed track."""
        state = self.load_state()
        timestamp = state.get("last_processed_timestamp")
        if timestamp:
            logger.info("Last processed timestamp", timestamp=timestamp)
        return timestamp

    def update_last_processed_timestamp(self, timestamp: int):
        """Update the last processed timestamp."""
        state = self.load_state()
        state["last_processed_timestamp"] = timestamp
        state["last_updated"] = datetime.now(timezone.utc).isoformat()
        self.save_state(state)
        self.last_processed_timestamp = timestamp

    # ------------------------------------------------------------------
    # Dead-letter queue (DLQ): write failed records to PIPELINE_ERRORS
    # ------------------------------------------------------------------

    def _write_errors_to_dlq(self, errors: List[Dict], run_id: str):
        """Write failed records to the PIPELINE_ERRORS table."""
        if not errors:
            return
        try:
            conn = _get_snowflake_connection()
            try:
                cursor = conn.cursor()
                for err in errors:
                    cursor.execute(
                        """
                        INSERT INTO SPOTIFY_ANALYTICS.RAW_DATA.PIPELINE_ERRORS
                            (error_id, run_id, error_timestamp, error_type, error_message, record_payload)
                        VALUES (%s, %s, CURRENT_TIMESTAMP(), %s, %s, PARSE_JSON(%s))
                        """,
                        (
                            str(uuid.uuid4()),
                            run_id,
                            err["error_type"],
                            err["error_message"][:4096],
                            json.dumps(err.get("record_payload")),
                        ),
                    )
                logger.info("Wrote errors to DLQ", count=len(errors), run_id=run_id)
            finally:
                conn.close()
        except Exception as e:
            logger.error("Failed to write errors to DLQ", error=str(e))

    # ------------------------------------------------------------------
    # Validation layer
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_record(record: Dict) -> Optional[str]:
        """Validate a transformed record. Returns error message or None if valid."""
        if not record.get("track_id"):
            return "Missing required field: track_id"
        if not record.get("played_at"):
            return "Missing required field: played_at"
        return None

    # ------------------------------------------------------------------
    # Run metrics
    # ------------------------------------------------------------------

    def _emit_run_metrics(self, run_id: str, metrics: Dict):
        """Write pipeline run metrics to PIPELINE_MONITORING table.

        The existing table uses a key/value schema:
            CHECK_TIMESTAMP, METRIC_NAME, METRIC_VALUE, METRIC_DESCRIPTION
        We write one row per metric with run_id embedded in the description.
        """
        metric_rows = [
            ("tracks_fetched", metrics.get("tracks_fetched", 0), f"run={run_id} | Tracks fetched from Spotify API"),
            ("tracks_transformed", metrics.get("tracks_transformed", 0), f"run={run_id} | Tracks that entered transform stage"),
            ("tracks_uploaded", metrics.get("tracks_uploaded", 0), f"run={run_id} | Tracks successfully uploaded to S3"),
            ("errors", metrics.get("errors", 0), f"run={run_id} | Records that failed validation/transform"),
            ("duration_seconds", metrics.get("duration_seconds", 0), f"run={run_id} | Total pipeline run duration"),
            ("run_status", 1 if metrics.get("status") == "success" else 0, f"run={run_id} | status={metrics.get('status', 'unknown')}"),
        ]
        try:
            conn = _get_snowflake_connection()
            try:
                cursor = conn.cursor()
                for metric_name, metric_value, description in metric_rows:
                    cursor.execute(
                        """
                        INSERT INTO SPOTIFY_ANALYTICS.RAW_DATA.PIPELINE_MONITORING
                            (CHECK_TIMESTAMP, METRIC_NAME, METRIC_VALUE, METRIC_DESCRIPTION)
                        VALUES (CURRENT_TIMESTAMP(), %s, %s, %s)
                        """,
                        (metric_name, metric_value, description),
                    )
                logger.info("Emitted run metrics", run_id=run_id, metrics=metrics)
            finally:
                conn.close()
        except Exception as e:
            logger.error("Failed to emit run metrics", error=str(e))

    # ------------------------------------------------------------------
    # Batch processing (with per-record error handling)
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def process_batch(self, tracks: List[Dict], run_id: str = "") -> Tuple[int, int]:
        """
        Process a batch of tracks: transform, validate, and upload to S3.

        Returns:
            Tuple of (successfully_uploaded_count, error_count)
        """
        if not tracks:
            return (0, 0)

        transformed_tracks: List[Dict] = []
        batch_errors: List[Dict] = []

        # Per-record transform with error isolation
        for track_item in tracks:
            try:
                transformed = self.spotify_client.transform_track_data(track_item)

                # Validation layer: reject null track_id / played_at
                validation_error = self._validate_record(transformed)
                if validation_error:
                    batch_errors.append({
                        "error_type": "validation_error",
                        "error_message": validation_error,
                        "record_payload": track_item,
                    })
                    continue

                transformed_tracks.append(transformed)
            except Exception as e:
                batch_errors.append({
                    "error_type": "transform_error",
                    "error_message": str(e),
                    "record_payload": track_item,
                })

        # Write errors to DLQ
        if batch_errors:
            self._write_errors_to_dlq(batch_errors, run_id)

        if not transformed_tracks:
            return (0, len(batch_errors))

        # Upload valid tracks to S3
        s3_key = self.s3_client.upload_tracks_batch(transformed_tracks)

        # Process artist-genre data if enabled
        if self.enable_artist_genre_processing and self.artist_genre_processor:
            try:
                self.artist_genre_processor.process_new_artists_from_tracks(tracks)
            except Exception as e:
                logger.error("Failed to process artist-genre data", error=str(e))

        if s3_key:
            # Update last processed timestamp
            last_track = tracks[-1]
            last_timestamp = int(
                datetime.fromisoformat(
                    last_track["played_at"].replace("Z", "+00:00")
                ).timestamp() * 1000
            )
            self.update_last_processed_timestamp(last_timestamp)

            logger.info(
                "Successfully processed batch",
                uploaded=len(transformed_tracks),
                errors=len(batch_errors),
                s3_key=s3_key,
            )
            return (len(transformed_tracks), len(batch_errors))
        else:
            logger.error("Failed to upload batch to S3")
            raise RuntimeError("S3 upload failed")

    def fetch_and_process_new_tracks(self) -> int:
        """
        Fetch new tracks since last processed timestamp and upload to S3.

        Returns:
            Number of tracks processed
        """
        run_id = str(uuid.uuid4())
        run_start = time.time()
        total_fetched = 0
        total_uploaded = 0
        total_errors = 0

        logger.info("Starting track fetch and process cycle", run_id=run_id)
        print("Starting track fetch & process cycle", flush=True)

        # Get starting timestamp
        last_timestamp = self.get_last_processed_timestamp()
        if not last_timestamp:
            # Safe first-run default: 24 hours ago
            start_time = datetime.now(timezone.utc) - timedelta(hours=24)
            last_timestamp = int(start_time.timestamp() * 1000)
            logger.info("No previous state, starting from 24 hours ago", timestamp=last_timestamp)

        batch: List[Dict] = []
        start_time_dt = datetime.now(timezone.utc)
        max_runtime = None
        if getattr(settings.pipeline, "max_runtime_minutes", None):
            if settings.pipeline.max_runtime_minutes > 0:
                max_runtime = timedelta(minutes=settings.pipeline.max_runtime_minutes)

        try:
            for track in self.spotify_client.get_all_recent_tracks_since(last_timestamp):
                batch.append(track)
                total_fetched += 1

                if max_runtime and datetime.now(timezone.utc) - start_time_dt > max_runtime:
                    logger.error("Track fetch exceeded max runtime", total_fetched=total_fetched)
                    raise TimeoutError("Track fetch exceeded configured max runtime")

                if len(batch) >= settings.pipeline.batch_size:
                    uploaded, errors = self.process_batch(batch, run_id=run_id)
                    total_uploaded += uploaded
                    total_errors += errors
                    batch = []
                    print(f"Processed batch, total tracks uploaded so far: {total_uploaded}", flush=True)

            # Process remaining tracks
            if batch:
                uploaded, errors = self.process_batch(batch, run_id=run_id)
                total_uploaded += uploaded
                total_errors += errors
                print(f"Processed final batch, total tracks uploaded: {total_uploaded}", flush=True)

        except Exception as e:
            logger.error("Error during track fetch and process", error=str(e))
            if batch:
                try:
                    uploaded, errors = self.process_batch(batch, run_id=run_id)
                    total_uploaded += uploaded
                    total_errors += errors
                except Exception as batch_error:
                    logger.error("Failed to process final batch", error=str(batch_error))

        duration = time.time() - run_start
        status = "success" if total_errors == 0 else "partial_success" if total_uploaded > 0 else "failure"

        # Emit run metrics
        self._emit_run_metrics(run_id, {
            "tracks_fetched": total_fetched,
            "tracks_transformed": total_uploaded + total_errors,
            "tracks_uploaded": total_uploaded,
            "errors": total_errors,
            "duration_seconds": round(duration, 2),
            "status": status,
        })

        logger.info(
            "Completed track fetch and process cycle",
            run_id=run_id,
            fetched=total_fetched,
            uploaded=total_uploaded,
            errors=total_errors,
            duration_seconds=round(duration, 2),
        )
        print(
            f"Completed: fetched={total_fetched}, uploaded={total_uploaded}, "
            f"errors={total_errors}, duration={round(duration, 1)}s",
            flush=True,
        )
        return total_uploaded

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    def health_check(self) -> Dict:
        """Check pipeline health by querying state and monitoring tables."""
        result: Dict = {"healthy": False, "checks": {}}
        try:
            conn = _get_snowflake_connection()
            try:
                cursor = conn.cursor()

                # Check state table reachable
                cursor.execute(
                    "SELECT MAX(updated_at) FROM SPOTIFY_ANALYTICS.RAW_DATA.PIPELINE_STATE"
                )
                row = cursor.fetchone()
                last_state_update = row[0] if row else None
                result["checks"]["state_table"] = {
                    "reachable": True,
                    "last_updated": str(last_state_update) if last_state_update else None,
                }

                # Check most recent run in monitoring
                cursor.execute(
                    """
                    SELECT METRIC_NAME, METRIC_VALUE, METRIC_DESCRIPTION, CHECK_TIMESTAMP
                    FROM SPOTIFY_ANALYTICS.RAW_DATA.PIPELINE_MONITORING
                    WHERE METRIC_NAME = 'run_status'
                    ORDER BY CHECK_TIMESTAMP DESC
                    LIMIT 1
                    """
                )
                mon_row = cursor.fetchone()
                if mon_row:
                    result["checks"]["last_run"] = {
                        "metric": mon_row[0],
                        "value": mon_row[1],
                        "description": mon_row[2],
                        "timestamp": str(mon_row[3]),
                    }
                else:
                    result["checks"]["last_run"] = None

                # Determine overall health: healthy if state is < 48h old
                if last_state_update:
                    age = datetime.now(timezone.utc) - last_state_update.replace(tzinfo=timezone.utc)
                    result["healthy"] = age < timedelta(hours=48)
                    result["checks"]["state_age_hours"] = round(age.total_seconds() / 3600, 1)
                else:
                    result["healthy"] = False

            finally:
                conn.close()
        except Exception as e:
            result["checks"]["error"] = str(e)
            logger.error("Health check failed", error=str(e))

        return result

    # ------------------------------------------------------------------
    # Top-level run commands
    # ------------------------------------------------------------------

    def run_once(self) -> bool:
        """Run the pipeline once."""
        try:
            logger.info("Starting pipeline run")
            print("Starting Spotify pipeline run", flush=True)

            # Authenticate with Spotify
            print("Authenticating with Spotify...", flush=True)
            if not self.spotify_client.authenticate():
                logger.error("Failed to authenticate with Spotify")
                print("Spotify authentication failed", flush=True)
                return False
            print("Spotify authentication succeeded", flush=True)

            # Ensure S3 bucket exists
            print(f"Ensuring S3 bucket '{self.s3_client.bucket_name}' exists...", flush=True)
            if not self.s3_client.ensure_bucket_exists():
                logger.error("S3 bucket not accessible")
                print("S3 bucket not accessible", flush=True)
                return False
            print("S3 bucket verified", flush=True)

            # Fetch and process new tracks
            print("Fetching and processing new tracks...", flush=True)
            processed_count = self.fetch_and_process_new_tracks()

            logger.info("Pipeline run completed", processed_count=processed_count)
            print(f"Pipeline run completed. Processed tracks: {processed_count}", flush=True)
            return True

        except Exception as e:
            logger.error("Pipeline run failed", error=str(e))
            print(f"Pipeline run failed: {e}", flush=True)
            return False

    def run_continuous(self):
        """Run the pipeline continuously on a schedule."""
        logger.info(
            "Starting continuous pipeline",
            interval_minutes=settings.pipeline.fetch_interval_minutes
        )

        schedule.every(settings.pipeline.fetch_interval_minutes).minutes.do(self.run_once)

        # Run once immediately
        self.run_once()

        while True:
            try:
                schedule.run_pending()
                time.sleep(60)
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, stopping pipeline")
                break
            except Exception as e:
                logger.error("Error in continuous run", error=str(e))
                time.sleep(60)

    def backfill_historical_data(self, days: int = 30):
        """Backfill historical data for a specified number of days."""
        logger.info("Starting historical data backfill", days=days)

        if days > 50:
            logger.warning("Spotify API only provides ~50 days of history, limiting to 50 days")
            days = 50

        start_time = datetime.now(timezone.utc) - timedelta(days=days)
        start_timestamp = int(start_time.timestamp() * 1000)

        original_timestamp = self.get_last_processed_timestamp()

        try:
            processed_count = 0
            batch: List[Dict] = []
            run_id = str(uuid.uuid4())

            for track in self.spotify_client.get_all_recent_tracks_since(start_timestamp):
                batch.append(track)

                if len(batch) >= settings.pipeline.batch_size:
                    uploaded, _ = self.process_batch(batch, run_id=run_id)
                    processed_count += uploaded
                    batch = []

            if batch:
                uploaded, _ = self.process_batch(batch, run_id=run_id)
                processed_count += uploaded

            logger.info("Historical backfill completed", processed_count=processed_count)

        except Exception as e:
            logger.error("Historical backfill failed", error=str(e))

        # Restore original timestamp if it was higher
        if original_timestamp and self.last_processed_timestamp and original_timestamp > self.last_processed_timestamp:
            self.update_last_processed_timestamp(original_timestamp)

    def backfill_artist_genre_data(self, days: int = 30):
        """Backfill artist-genre data by processing artists from historical listening data."""
        if not self.enable_artist_genre_processing or not self.artist_genre_processor:
            self.artist_genre_processor = ArtistGenreProcessor(
                spotify_client=self.spotify_client,
                s3_client=self.s3_client
            )
            temp_enabled = True
            logger.info("Temporarily enabled artist-genre processing for backfill")
        else:
            temp_enabled = False

        logger.info("Starting artist-genre data backfill", days=days)

        if days > 50:
            logger.warning("Spotify API only provides ~50 days of history, limiting to 50 days")
            days = 50

        start_time = datetime.now(timezone.utc) - timedelta(days=days)
        start_timestamp = int(start_time.timestamp() * 1000)

        try:
            all_tracks: List[Dict] = []
            batch_count = 0

            logger.info("Collecting tracks from listening history for artist extraction")
            for track in self.spotify_client.get_all_recent_tracks_since(start_timestamp):
                all_tracks.append(track)

                if len(all_tracks) >= settings.pipeline.batch_size * 5:
                    artist_count = self.artist_genre_processor.process_new_artists_from_tracks(all_tracks)
                    logger.info(f"Processed batch {batch_count + 1}, new artists: {artist_count}")
                    all_tracks = []
                    batch_count += 1
                    time.sleep(2.0)

            if all_tracks:
                artist_count = self.artist_genre_processor.process_new_artists_from_tracks(all_tracks)
                logger.info(f"Processed final batch, new artists: {artist_count}")

            final_stats = self.artist_genre_processor.get_stats()
            logger.info("Artist-genre backfill completed",
                        total_processed_artists=final_stats["total_processed_artists"])

        except Exception as e:
            logger.error("Artist-genre backfill failed", error=str(e))

        if temp_enabled:
            self.artist_genre_processor = None
            logger.info("Disabled temporary artist-genre processing")

    def get_pipeline_stats(self) -> Dict:
        """Get pipeline statistics and status."""
        try:
            state = self.load_state()
            recent_files = self.s3_client.list_recent_files(days=7)

            stats = {
                "last_processed_timestamp": state.get("last_processed_timestamp"),
                "last_updated": state.get("last_updated"),
                "recent_files_count": len(recent_files),
                "total_recent_size_bytes": sum(f["size"] for f in recent_files),
                "bucket_name": self.s3_client.bucket_name,
                "config": {
                    "fetch_interval_minutes": settings.pipeline.fetch_interval_minutes,
                    "batch_size": settings.pipeline.batch_size,
                    "s3_prefix": settings.pipeline.snowflake_stage_prefix
                }
            }

            if self.enable_artist_genre_processing and self.artist_genre_processor:
                artist_stats = self.artist_genre_processor.get_stats()
                stats["artist_genre_processing"] = {
                    "enabled": True,
                    "total_processed_artists": artist_stats["total_processed_artists"],
                    "artist_s3_prefix": artist_stats["artist_s3_prefix"],
                    "state_file": artist_stats["state_file"]
                }
            else:
                stats["artist_genre_processing"] = {"enabled": False}

            logger.info("Generated pipeline stats", stats=stats)
            return stats

        except Exception as e:
            logger.error("Failed to get pipeline stats", error=str(e))
            return {}
