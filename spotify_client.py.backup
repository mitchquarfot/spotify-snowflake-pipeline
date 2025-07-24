"""Spotify API client for fetching listening history."""

import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Generator
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from config import settings

logger = structlog.get_logger(__name__)


class SpotifyClient:
    """Client for interacting with Spotify Web API."""
    
    def __init__(self):
        """Initialize Spotify client with OAuth."""
        self.auth_manager = SpotifyOAuth(
            client_id=settings.spotify.client_id,
            client_secret=settings.spotify.client_secret,
            redirect_uri=settings.spotify.redirect_uri,
            scope="user-read-recently-played user-read-playback-state",
            cache_path=".spotify_cache"
        )
        self.sp = spotipy.Spotify(auth_manager=self.auth_manager)
        logger.info("Spotify client initialized")
    
    def authenticate(self) -> bool:
        """Ensure user is authenticated."""
        try:
            # This will trigger OAuth flow if needed
            self.sp.current_user()
            logger.info("Successfully authenticated with Spotify")
            return True
        except Exception as e:
            logger.error("Failed to authenticate with Spotify", error=str(e))
            return False
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def get_recent_tracks(
        self, 
        limit: int = 50, 
        after: Optional[int] = None,
        before: Optional[int] = None
    ) -> List[Dict]:
        """
        Fetch recently played tracks.
        
        Args:
            limit: Number of tracks to fetch (max 50)
            after: Unix timestamp to fetch tracks after
            before: Unix timestamp to fetch tracks before
            
        Returns:
            List of recently played track objects
        """
        try:
            params = {"limit": min(limit, 50)}
            if after:
                params["after"] = after
            if before:
                params["before"] = before
                
            results = self.sp.current_user_recently_played(**params)
            tracks = results.get("items", [])
            
            logger.info(
                "Fetched recent tracks",
                count=len(tracks),
                after=after,
                before=before
            )
            return tracks
            
        except Exception as e:
            logger.error("Failed to fetch recent tracks", error=str(e))
            raise
    
    def get_all_recent_tracks_since(self, since_timestamp: int) -> Generator[Dict, None, None]:
        """
        Fetch all tracks since a given timestamp using pagination.
        
        Args:
            since_timestamp: Unix timestamp to start fetching from
            
        Yields:
            Individual track objects
        """
        after = since_timestamp
        
        while True:
            tracks = self.get_recent_tracks(
                limit=settings.pipeline.batch_size,
                after=after
            )
            
            if not tracks:
                logger.info("No more tracks to fetch")
                break
                
            for track in tracks:
                yield track
                
            # Update 'after' to the timestamp of the last track
            last_track_time = tracks[-1]["played_at"]
            last_timestamp = int(
                datetime.fromisoformat(
                    last_track_time.replace("Z", "+00:00")
                ).timestamp() * 1000
            )
            
            # If we didn't get a full batch, we've reached the end
            if len(tracks) < settings.pipeline.batch_size:
                logger.info("Reached end of available tracks")
                break
                
            after = last_timestamp
            
            # Rate limiting - Spotify allows 100 requests per minute
            time.sleep(0.6)  # ~60 requests per minute to be safe
    
    def transform_track_data(self, track_item: Dict) -> Dict:
        """
        Transform Spotify track data into Snowflake-friendly format.
        
        Args:
            track_item: Raw track item from Spotify API
            
        Returns:
            Transformed track data
        """
        track = track_item.get("track", {})
        played_at = track_item.get("played_at")
        context = track_item.get("context", {})
        
        # Parse timestamp
        played_at_dt = datetime.fromisoformat(
            played_at.replace("Z", "+00:00")
        )
        
        return {
            # Listening metadata
            "played_at": played_at,
            "played_at_timestamp": int(played_at_dt.timestamp()),
            "played_at_date": played_at_dt.strftime("%Y-%m-%d"),
            "played_at_hour": played_at_dt.hour,
            
            # Track information
            "track_id": track.get("id"),
            "track_name": track.get("name"),
            "track_duration_ms": track.get("duration_ms"),
            "track_popularity": track.get("popularity"),
            "track_explicit": track.get("explicit"),
            "track_preview_url": track.get("preview_url"),
            "track_external_urls": json.dumps(track.get("external_urls", {})),
            
            # Artist information
            "artists": json.dumps([
                {
                    "id": artist.get("id"),
                    "name": artist.get("name"),
                    "uri": artist.get("uri"),
                    "external_urls": artist.get("external_urls", {})
                }
                for artist in track.get("artists", [])
            ]),
            "primary_artist_id": track.get("artists", [{}])[0].get("id"),
            "primary_artist_name": track.get("artists", [{}])[0].get("name"),
            
            # Album information
            "album_id": track.get("album", {}).get("id"),
            "album_name": track.get("album", {}).get("name"),
            "album_type": track.get("album", {}).get("album_type"),
            "album_release_date": track.get("album", {}).get("release_date"),
            "album_total_tracks": track.get("album", {}).get("total_tracks"),
            "album_images": json.dumps(track.get("album", {}).get("images", [])),
            
            # Context (playlist, artist, album, etc.)
            "context_type": context.get("type") if context else None,
            "context_uri": context.get("uri") if context else None,
            "context_external_urls": json.dumps(context.get("external_urls", {})) if context else None,
            
            # Audio features (to be enriched separately if needed)
            "track_uri": track.get("uri"),
            
            # Pipeline metadata
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "data_source": "spotify_recently_played_api"
        } 