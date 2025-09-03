"""Spotify API client for fetching listening history."""

import json
import time
import os
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
        
        # If we have a refresh token, use it
        if settings.spotify.refresh_token:
            self._setup_with_refresh_token()
        
        self.sp = spotipy.Spotify(auth_manager=self.auth_manager)
        logger.info("Spotify client initialized")
    
    def _setup_with_refresh_token(self):
        """Set up auth manager with stored refresh token."""
        try:
            # Create token info dict with refresh token
            token_info = {
                'refresh_token': settings.spotify.refresh_token,
                'access_token': None,
                'expires_at': 0,  # Force refresh
                'scope': 'user-read-recently-played user-read-playback-state',
                'token_type': 'Bearer'
            }
            
            # Save token info to cache
            self.auth_manager.cache_handler.save_token_to_cache(token_info)
            logger.info("Loaded refresh token from settings")
            
        except Exception as e:
            logger.warning("Failed to set up refresh token", error=str(e))
    
    def authenticate(self) -> bool:
        """Ensure user is authenticated."""
        try:
            # This will use cached token or refresh if needed
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
    
    def extract_artists_from_tracks(self, track_items: List[Dict]) -> List[Dict]:
        """
        Extract unique artists from a list of track items.
        
        Args:
            track_items: List of raw track items from Spotify API
            
        Returns:
            List of unique artist dictionaries with id, name, and uri
        """
        unique_artists = {}
        
        for track_item in track_items:
            track = track_item.get("track", {})
            artists = track.get("artists", [])
            
            for artist in artists:
                artist_id = artist.get("id")
                if artist_id and artist_id not in unique_artists:
                    unique_artists[artist_id] = {
                        "id": artist_id,
                        "name": artist.get("name"),
                        "uri": artist.get("uri"),
                        "external_urls": artist.get("external_urls", {})
                    }
        
        return list(unique_artists.values())
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def get_artist_details(self, artist_id: str) -> Optional[Dict]:
        """
        Get detailed information about an artist including genres.
        
        Args:
            artist_id: Spotify artist ID
            
        Returns:
            Artist details including genres, or None if not found
        """
        try:
            artist = self.sp.artist(artist_id)
            logger.debug("Fetched artist details", artist_id=artist_id, name=artist.get("name"))
            return artist
        except Exception as e:
            logger.warning("Failed to fetch artist details", artist_id=artist_id, error=str(e))
            return None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def get_multiple_artists(self, artist_ids: List[str], batch_size: int = 50) -> List[Dict]:
        """
        Get details for multiple artists in batches.
        
        Args:
            artist_ids: List of Spotify artist IDs
            batch_size: Number of artists to fetch per batch (max 50)
            
        Returns:
            List of artist detail dictionaries
        """
        all_artists = []
        
        # Process in batches due to API limitations
        for i in range(0, len(artist_ids), batch_size):
            batch_ids = artist_ids[i:i + batch_size]
            
            try:
                # Remove any None values
                batch_ids = [aid for aid in batch_ids if aid]
                if not batch_ids:
                    continue
                
                artists = self.sp.artists(batch_ids)
                batch_artists = artists.get("artists", [])
                all_artists.extend([artist for artist in batch_artists if artist])
                
                logger.info(
                    "Fetched artist batch",
                    batch_number=i // batch_size + 1,
                    batch_size=len(batch_ids),
                    fetched_count=len(batch_artists)
                )
                
                # Rate limiting - be conservative with batch requests
                time.sleep(1.0)
                
            except Exception as e:
                logger.error(
                    "Failed to fetch artist batch",
                    batch_ids=batch_ids,
                    error=str(e)
                )
                continue
        
        logger.info("Completed artist batch fetching", total_fetched=len(all_artists))
        return all_artists
    
    def transform_artist_data(self, artist: Dict, enhance_empty_genres: bool = True) -> Dict:
        """
        Transform artist data into Snowflake-friendly format.
        
        Args:
            artist: Raw artist data from Spotify API
            enhance_empty_genres: Whether to enhance artists with empty genres
            
        Returns:
            Transformed artist data with genres
        """
        genres = artist.get("genres", [])
        followers = artist.get("followers", {})
        
        # Enhanced genre processing for empty genres
        data_source = "spotify_artist_api"
        genre_inference_methods = None
        original_genres_empty = False
        
        if enhance_empty_genres and not genres:
            # Try to infer genres for artists with empty genre arrays
            enhanced_artist = self._enhance_empty_genres(artist)
            genres = enhanced_artist.get("genres", [])
            genre_inference_methods = enhanced_artist.get("genre_inference_methods")
            original_genres_empty = enhanced_artist.get("original_genres_empty", False)
            if genre_inference_methods:
                data_source = f"spotify_artist_api_enhanced_{'+'.join(genre_inference_methods)}"
        
        result = {
            "artist_id": artist.get("id"),
            "artist_name": artist.get("name"),
            "artist_uri": artist.get("uri"),
            "genres": json.dumps(genres),
            "genres_list": genres,  # For easier querying
            "primary_genre": genres[0] if genres else None,  # Most relevant genre (safe access)
            "genre_count": len(genres),
            "popularity": artist.get("popularity"),
            "followers_total": followers.get("total") if followers else None,
            "external_urls": json.dumps(artist.get("external_urls", {})),
            "images": json.dumps(artist.get("images", [])),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "data_source": data_source
        }
        
        # Add enhancement metadata if genres were inferred
        if original_genres_empty:
            result["original_genres_empty"] = True
            if genre_inference_methods:
                result["genre_inference_methods"] = json.dumps(genre_inference_methods)
        
        return result
    
    def _enhance_empty_genres(self, artist: Dict) -> Dict:
        """
        Enhance artist with inferred genres when original genres are empty.
        
        Args:
            artist: Raw artist data from Spotify API
            
        Returns:
            Enhanced artist data with inferred genres
        """
        original_genres = artist.get("genres", [])
        
        # If we already have genres, return as-is
        if original_genres:
            return artist
        
        # Try to infer genres using various strategies
        inferred_genres = []
        inference_methods = []
        
        # Strategy 1: Name-based inference
        name_genre = self._infer_genre_from_name(artist.get("name", ""))
        if name_genre:
            inferred_genres.append(name_genre)
            inference_methods.append("name_pattern")
        
        # Strategy 2: Popularity-based inference
        popularity = artist.get("popularity")
        followers = artist.get("followers", {}).get("total")
        popularity_genre = self._infer_genre_from_popularity(popularity, followers)
        if popularity_genre and popularity_genre not in inferred_genres:
            inferred_genres.append(popularity_genre)
            inference_methods.append("popularity_analysis")
        
        # If we inferred any genres, update the artist data
        if inferred_genres:
            enhanced_data = artist.copy()
            enhanced_data["genres"] = inferred_genres
            enhanced_data["genre_inference_methods"] = inference_methods
            enhanced_data["original_genres_empty"] = True
            
            logger.info("Enhanced artist with inferred genres",
                       artist_name=artist.get("name"),
                       artist_id=artist.get("id"),
                       inferred_genres=inferred_genres,
                       methods=inference_methods)
            
            return enhanced_data
        
        # If no genres could be inferred, add a generic classification
        enhanced_data = artist.copy()
        enhanced_data["genres"] = ["unclassified"]
        enhanced_data["genre_inference_methods"] = ["fallback"]
        enhanced_data["original_genres_empty"] = True
        
        logger.warning("Could not infer genres for artist",
                      artist_name=artist.get("name"),
                      artist_id=artist.get("id"),
                      popularity=popularity,
                      followers=followers)
        
        return enhanced_data
    
    def _infer_genre_from_name(self, artist_name: str) -> Optional[str]:
        """Infer genre from artist name patterns."""
        if not artist_name:
            return None
            
        name_lower = artist_name.lower()
        
        # Common genre mappings based on artist name patterns
        genre_patterns = {
            'electronic': ['dj ', 'dj_', 'electronic', 'edm', 'house', 'techno', 'trance'],
            'hip hop': ['lil ', 'young ', 'big ', 'rapper', 'mc ', 'hip hop', 'rap'],
            'rock': ['band', 'rock', 'metal', 'punk'],
            'pop': ['pop', 'mainstream'],
            'indie': ['indie', 'alternative'],
            'country': ['country', 'nashville'],
            'jazz': ['jazz', 'blues'],
            'classical': ['orchestra', 'symphony', 'classical'],
            'latin': ['latin', 'spanish', 'reggaeton'],
            'r&b': ['r&b', 'soul', 'rnb']
        }
        
        # Check for common patterns
        for genre, patterns in genre_patterns.items():
            for pattern in patterns:
                if pattern in name_lower:
                    return genre
        
        return None
    
    def _infer_genre_from_popularity(self, popularity: int, followers: int) -> Optional[str]:
        """Infer genre based on popularity and follower count."""
        if popularity is None:
            return None
            
        # Popularity-based genre inference
        if popularity >= 80:
            return 'mainstream pop'
        elif popularity >= 60:
            return 'pop'
        elif popularity >= 40:
            return 'alternative'
        elif popularity >= 20:
            return 'indie'
        else:
            return 'underground'