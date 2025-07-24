"""Artist Genre Processor for extracting and enriching artist data with genres."""

import json
import time
from datetime import datetime, timezone
from typing import List, Dict, Set, Optional
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from spotify_client import SpotifyClient
from s3_client import S3Client
from config import settings

logger = structlog.get_logger(__name__)


class ArtistGenreProcessor:
    """Processor for extracting unique artists and their genres from listening data."""
    
    def __init__(self, spotify_client: Optional[SpotifyClient] = None, s3_client: Optional[S3Client] = None):
        """Initialize the processor with clients."""
        self.spotify_client = spotify_client or SpotifyClient()
        self.s3_client = s3_client or S3Client()
        self.state_file = "artist_genre_state.json"
        self.processed_artists: Set[str] = set()
        
        # Load previously processed artists to avoid duplicates
        self._load_processed_artists()
        
        logger.info("Artist Genre Processor initialized")
    
    def _load_processed_artists(self):
        """Load list of previously processed artists."""
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
            self.processed_artists = set(state.get("processed_artists", []))
            logger.info("Loaded processed artists state", count=len(self.processed_artists))
        except FileNotFoundError:
            logger.info("No existing artist state file found, starting fresh")
            self.processed_artists = set()
        except Exception as e:
            logger.error("Failed to load artist state", error=str(e))
            self.processed_artists = set()
    
    def _save_processed_artists(self):
        """Save list of processed artists to state file."""
        try:
            state = {
                "processed_artists": list(self.processed_artists),
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "total_count": len(self.processed_artists)
            }
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.debug("Saved processed artists state", count=len(self.processed_artists))
        except Exception as e:
            logger.error("Failed to save artist state", error=str(e))
    
    def extract_new_artists_from_tracks(self, track_items: List[Dict]) -> List[str]:
        """
        Extract artist IDs that haven't been processed yet.
        
        Args:
            track_items: List of raw track items from Spotify API
            
        Returns:
            List of new artist IDs to process
        """
        all_artists = self.spotify_client.extract_artists_from_tracks(track_items)
        new_artist_ids = []
        
        for artist in all_artists:
            artist_id = artist.get("id")
            if artist_id and artist_id not in self.processed_artists:
                new_artist_ids.append(artist_id)
        
        logger.info(
            "Extracted new artists from tracks",
            total_artists=len(all_artists),
            new_artists=len(new_artist_ids),
            already_processed=len(all_artists) - len(new_artist_ids)
        )
        
        return new_artist_ids
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def process_artist_batch(self, artist_ids: List[str]) -> List[Dict]:
        """
        Process a batch of artist IDs to get their details and genres.
        
        Args:
            artist_ids: List of Spotify artist IDs
            
        Returns:
            List of transformed artist data dictionaries
        """
        if not artist_ids:
            return []
        
        try:
            # Fetch artist details from Spotify
            raw_artists = self.spotify_client.get_multiple_artists(artist_ids)
            
            # Transform to Snowflake-friendly format
            transformed_artists = []
            for artist in raw_artists:
                if artist:  # Skip None results
                    transformed = self.spotify_client.transform_artist_data(artist)
                    transformed_artists.append(transformed)
                    
                    # Mark as processed
                    artist_id = artist.get("id")
                    if artist_id:
                        self.processed_artists.add(artist_id)
            
            # Save updated state
            self._save_processed_artists()
            
            logger.info(
                "Processed artist batch",
                requested_count=len(artist_ids),
                successfully_processed=len(transformed_artists)
            )
            
            return transformed_artists
            
        except Exception as e:
            logger.error("Failed to process artist batch", error=str(e))
            raise
    
    def generate_artist_s3_key(self, timestamp: datetime, file_suffix: str = "") -> str:
        """
        Generate S3 key for artist data with date partitioning.
        
        Args:
            timestamp: Timestamp for partitioning
            file_suffix: Additional suffix for the filename
            
        Returns:
            S3 key path for artist data
        """
        date_partition = timestamp.strftime(settings.pipeline.date_partition_format)
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
        
        filename = f"spotify_artists_{timestamp_str}"
        if file_suffix:
            filename += f"_{file_suffix}"
        filename += ".json.gz"
        
        # Use a separate prefix for artist data
        artist_prefix = settings.pipeline.snowflake_stage_prefix.replace("listening_history", "artist_genres")
        s3_key = f"{artist_prefix}{date_partition}/{filename}"
        return s3_key
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def upload_artists_to_s3(
        self, 
        artists: List[Dict], 
        timestamp: Optional[datetime] = None
    ) -> str:
        """
        Upload artist data to S3 using the same pattern as track data.
        
        Args:
            artists: List of transformed artist dictionaries
            timestamp: Timestamp for partitioning (defaults to now)
            
        Returns:
            S3 key of uploaded file
        """
        if not artists:
            logger.warning("No artists to upload")
            return ""
        
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        s3_key = self.generate_artist_s3_key(timestamp, f"batch_{len(artists)}")
        
        try:
            # Convert artists to JSONL format (one JSON object per line)
            jsonl_data = "\n".join(json.dumps(artist, ensure_ascii=False) for artist in artists)
            
            # Compress the data
            import gzip
            compressed_data = gzip.compress(jsonl_data.encode('utf-8'))
            
            # Upload to S3
            self.s3_client.s3_client.put_object(
                Bucket=self.s3_client.bucket_name,
                Key=s3_key,
                Body=compressed_data,
                ContentType='application/gzip',
                ContentEncoding='gzip',
                Metadata={
                    'source': 'spotify-artist-api',
                    'artist_count': str(len(artists)),
                    'format': 'jsonl',
                    'ingestion_timestamp': timestamp.isoformat()
                }
            )
            
            logger.info(
                "Successfully uploaded artist batch",
                s3_key=s3_key,
                artist_count=len(artists),
                compressed_size=len(compressed_data)
            )
            
            return s3_key
            
        except Exception as e:
            logger.error(
                "Failed to upload artist batch",
                s3_key=s3_key,
                artist_count=len(artists),
                error=str(e)
            )
            raise
    
    def process_new_artists_from_tracks(self, track_items: List[Dict]) -> int:
        """
        Process new artists found in track data and upload to S3.
        
        Args:
            track_items: List of raw track items from Spotify API
            
        Returns:
            Number of new artists processed and uploaded
        """
        if not track_items:
            return 0
        
        logger.info("Starting artist genre processing from tracks")
        
        # Extract new artist IDs
        new_artist_ids = self.extract_new_artists_from_tracks(track_items)
        
        if not new_artist_ids:
            logger.info("No new artists to process")
            return 0
        
        processed_count = 0
        batch_size = min(50, settings.pipeline.batch_size)  # Spotify API limit
        
        try:
            # Process artists in batches
            for i in range(0, len(new_artist_ids), batch_size):
                batch_ids = new_artist_ids[i:i + batch_size]
                
                # Process batch
                transformed_artists = self.process_artist_batch(batch_ids)
                
                if transformed_artists:
                    # Upload to S3
                    s3_key = self.upload_artists_to_s3(transformed_artists)
                    if s3_key:
                        processed_count += len(transformed_artists)
                        logger.info(
                            "Successfully processed and uploaded artist batch",
                            batch_number=i // batch_size + 1,
                            batch_size=len(batch_ids),
                            uploaded_count=len(transformed_artists),
                            s3_key=s3_key
                        )
                    else:
                        logger.error("Failed to upload artist batch to S3")
                else:
                    logger.warning("No artists processed in batch")
                
                # Rate limiting between batches
                time.sleep(1.0)
        
        except Exception as e:
            logger.error("Error during artist processing", error=str(e))
        
        logger.info(
            "Completed artist genre processing",
            total_new_artists=len(new_artist_ids),
            successfully_processed=processed_count
        )
        
        return processed_count
    
    def get_stats(self) -> Dict:
        """Get statistics about processed artists."""
        return {
            "total_processed_artists": len(self.processed_artists),
            "state_file": self.state_file,
            "artist_s3_prefix": settings.pipeline.snowflake_stage_prefix.replace("listening_history", "artist_genres")
        } 