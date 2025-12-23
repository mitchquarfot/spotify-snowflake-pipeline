"""Main pipeline orchestrator for Spotify to S3 data flow."""

import json
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import schedule
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from spotify_client import SpotifyClient
from s3_client import S3Client
from artist_genre_processor import ArtistGenreProcessor
from config import settings

logger = structlog.get_logger(__name__)


class SpotifyDataPipeline:
    """Main pipeline for streaming Spotify data to S3."""
    
    def __init__(self, enable_artist_genre_processing: bool = False):
        """Initialize pipeline with clients."""
        self.spotify_client = SpotifyClient()
        self.s3_client = S3Client()
        self.state_file = "pipeline_state.json"
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
    
    def load_state(self) -> Dict:
        """Load pipeline state from file."""
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
            logger.info("Loaded pipeline state", state=state)
            return state
        except FileNotFoundError:
            logger.info("No existing state file found, starting fresh")
            return {}
        except Exception as e:
            logger.error("Failed to load state", error=str(e))
            return {}
    
    def save_state(self, state: Dict):
        """Save pipeline state to file."""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.info("Saved pipeline state", state=state)
        except Exception as e:
            logger.error("Failed to save state", error=str(e))
    
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
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def process_batch(self, tracks: List[Dict]) -> bool:
        """
        Process a batch of tracks: transform and upload to S3.
        
        Args:
            tracks: List of raw track data from Spotify
            
        Returns:
            True if successful, False otherwise
        """
        if not tracks:
            return True
        
        try:
            # Transform tracks
            transformed_tracks = []
            for track_item in tracks:
                transformed = self.spotify_client.transform_track_data(track_item)
                transformed_tracks.append(transformed)
            
            # Upload tracks to S3
            s3_key = self.s3_client.upload_tracks_batch(transformed_tracks)
            
            # Process artist-genre data if enabled
            artist_processed_count = 0
            if self.enable_artist_genre_processing and self.artist_genre_processor:
                try:
                    artist_processed_count = self.artist_genre_processor.process_new_artists_from_tracks(tracks)
                    logger.info("Processed artist-genre data", new_artists=artist_processed_count)
                except Exception as e:
                    logger.error("Failed to process artist-genre data", error=str(e))
                    # Don't fail the entire batch if artist processing fails
            
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
                    track_count=len(tracks),
                    s3_key=s3_key,
                    last_timestamp=last_timestamp,
                    new_artists_processed=artist_processed_count
                )
                return True
            else:
                logger.error("Failed to upload batch to S3")
                return False
                
        except Exception as e:
            logger.error("Failed to process batch", error=str(e))
            raise
    
    def fetch_and_process_new_tracks(self) -> int:
        """
        Fetch new tracks since last processed timestamp and upload to S3.
        
        Returns:
            Number of tracks processed
        """
        logger.info("Starting track fetch and process cycle")
        print("🎧 Starting track fetch & process cycle", flush=True)
        
        # Get starting timestamp
        last_timestamp = self.get_last_processed_timestamp()
        if not last_timestamp:
            # Start from 24 hours ago if no previous state
            start_time = datetime.now(timezone.utc) - timedelta(hours=24)
            last_timestamp = int(start_time.timestamp() * 1000)
            logger.info("No previous state, starting from 24 hours ago", timestamp=last_timestamp)
        
        processed_count = 0
        batch = []
        start_time = datetime.now(timezone.utc)
        max_runtime = None
        if getattr(settings.pipeline, "max_runtime_minutes", None):
            if settings.pipeline.max_runtime_minutes > 0:
                max_runtime = timedelta(minutes=settings.pipeline.max_runtime_minutes)
        
        try:
            # Fetch tracks in batches
            for track in self.spotify_client.get_all_recent_tracks_since(last_timestamp):
                batch.append(track)
                
                if max_runtime and datetime.now(timezone.utc) - start_time > max_runtime:
                    logger.error(
                        "Track fetch exceeded max runtime",
                        processed_count=processed_count,
                        max_runtime_minutes=settings.pipeline.max_runtime_minutes
                    )
                    raise TimeoutError("Track fetch exceeded configured max runtime")
                
                # Process batch when it reaches configured size
                if len(batch) >= settings.pipeline.batch_size:
                    if self.process_batch(batch):
                        processed_count += len(batch)
                        batch = []
                        print(f"📦 Processed batch, total tracks so far: {processed_count}", flush=True)
                    else:
                        logger.error("Failed to process batch, stopping")
                        break
            
            # Process remaining tracks
            if batch:
                if self.process_batch(batch):
                    processed_count += len(batch)
                    print(f"📦 Processed final partial batch, total tracks: {processed_count}", flush=True)
        
        except Exception as e:
            logger.error("Error during track fetch and process", error=str(e))
            # Process any remaining batch on error
            if batch:
                try:
                    self.process_batch(batch)
                    processed_count += len(batch)
                except Exception as batch_error:
                    logger.error("Failed to process final batch", error=str(batch_error))
        
        logger.info("Completed track fetch and process cycle", processed_count=processed_count)
        print(f"✅ Completed track fetch & process cycle. Tracks processed: {processed_count}", flush=True)
        return processed_count
    
    def run_once(self) -> bool:
        """Run the pipeline once."""
        try:
            logger.info("Starting pipeline run")
            print("🚀 Starting Spotify pipeline run", flush=True)
            
            # Authenticate with Spotify
            print("🔐 Authenticating with Spotify...", flush=True)
            if not self.spotify_client.authenticate():
                logger.error("Failed to authenticate with Spotify")
                print("❌ Spotify authentication failed", flush=True)
                return False
            print("✅ Spotify authentication succeeded", flush=True)
            
            # Ensure S3 bucket exists
            print(f"🪣 Ensuring S3 bucket '{self.s3_client.bucket_name}' exists...", flush=True)
            if not self.s3_client.ensure_bucket_exists():
                logger.error("S3 bucket not accessible")
                print("❌ S3 bucket not accessible", flush=True)
                return False
            print("✅ S3 bucket verified", flush=True)
            
            # Fetch and process new tracks
            print("🎶 Fetching and processing new tracks...", flush=True)
            processed_count = self.fetch_and_process_new_tracks()
            
            logger.info("Pipeline run completed", processed_count=processed_count)
            print(f"🏁 Pipeline run completed. Processed tracks: {processed_count}", flush=True)
            return True
            
        except Exception as e:
            logger.error("Pipeline run failed", error=str(e))
            print(f"❌ Pipeline run failed: {e}", flush=True)
            return False
    
    def run_continuous(self):
        """Run the pipeline continuously on a schedule."""
        logger.info(
            "Starting continuous pipeline",
            interval_minutes=settings.pipeline.fetch_interval_minutes
        )
        
        # Schedule the pipeline to run at intervals
        schedule.every(settings.pipeline.fetch_interval_minutes).minutes.do(self.run_once)
        
        # Run once immediately
        self.run_once()
        
        # Keep running scheduled jobs
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, stopping pipeline")
                break
            except Exception as e:
                logger.error("Error in continuous run", error=str(e))
                time.sleep(60)  # Wait before retrying
    
    def backfill_historical_data(self, days: int = 30):
        """
        Backfill historical data for a specified number of days.
        
        Args:
            days: Number of days to backfill (max ~50 due to Spotify API limits)
        """
        logger.info("Starting historical data backfill", days=days)
        
        if days > 50:
            logger.warning("Spotify API only provides ~50 days of history, limiting to 50 days")
            days = 50
        
        # Calculate start timestamp
        start_time = datetime.now(timezone.utc) - timedelta(days=days)
        start_timestamp = int(start_time.timestamp() * 1000)
        
        # Temporarily override last processed timestamp
        original_timestamp = self.get_last_processed_timestamp()
        
        try:
            # Process all tracks since start_timestamp
            processed_count = 0
            batch = []
            
            for track in self.spotify_client.get_all_recent_tracks_since(start_timestamp):
                batch.append(track)
                
                if len(batch) >= settings.pipeline.batch_size:
                    if self.process_batch(batch):
                        processed_count += len(batch)
                        batch = []
                    else:
                        break
            
            # Process remaining tracks
            if batch:
                if self.process_batch(batch):
                    processed_count += len(batch)
            
            logger.info("Historical backfill completed", processed_count=processed_count)
            
        except Exception as e:
            logger.error("Historical backfill failed", error=str(e))
        
        # Restore original timestamp if it was higher
        if original_timestamp and original_timestamp > self.last_processed_timestamp:
            self.update_last_processed_timestamp(original_timestamp)
    
    def backfill_artist_genre_data(self, days: int = 30):
        """
        Backfill artist-genre data by processing artists from historical listening data.
        
        Args:
            days: Number of days of listening history to extract artists from
        """
        if not self.enable_artist_genre_processing or not self.artist_genre_processor:
            # Temporarily enable artist processing for this operation
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
        
        # Calculate start timestamp
        start_time = datetime.now(timezone.utc) - timedelta(days=days)
        start_timestamp = int(start_time.timestamp() * 1000)
        
        try:
            # Collect all tracks from the specified period
            all_tracks = []
            batch_count = 0
            
            logger.info("Collecting tracks from listening history for artist extraction")
            for track in self.spotify_client.get_all_recent_tracks_since(start_timestamp):
                all_tracks.append(track)
                
                # Process in smaller batches to avoid memory issues
                if len(all_tracks) >= settings.pipeline.batch_size * 5:  # 5x normal batch size
                    artist_count = self.artist_genre_processor.process_new_artists_from_tracks(all_tracks)
                    logger.info(f"Processed batch {batch_count + 1}, new artists: {artist_count}")
                    all_tracks = []  # Reset for next batch
                    batch_count += 1
                    
                    # Rate limiting for large backfills
                    time.sleep(2.0)
            
            # Process remaining tracks
            if all_tracks:
                artist_count = self.artist_genre_processor.process_new_artists_from_tracks(all_tracks)
                logger.info(f"Processed final batch, new artists: {artist_count}")
            
            # Get final stats
            final_stats = self.artist_genre_processor.get_stats()
            logger.info("Artist-genre backfill completed", 
                       total_processed_artists=final_stats["total_processed_artists"])
            
        except Exception as e:
            logger.error("Artist-genre backfill failed", error=str(e))
        
        # Clean up temporary processor if needed
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
            
            # Add artist-genre processing stats if enabled
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