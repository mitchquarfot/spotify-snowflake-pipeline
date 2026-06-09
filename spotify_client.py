"""Spotify API client for fetching listening history."""

import json
import time
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Generator
import spotipy
from spotipy.oauth2 import SpotifyOAuth, SpotifyOauthError
import structlog
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from config import settings

logger = structlog.get_logger(__name__)


class AdaptiveRateLimiter:
    """Adaptive rate limiter that respects Retry-After headers."""

    def __init__(self):
        self._consecutive_429s = 0
        self._last_request_time = 0.0
        self._min_interval = 60.0 / settings.rate_limit.requests_per_minute

    def wait(self):
        """Wait the appropriate amount between requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.monotonic()

    def handle_rate_limit(self, retry_after: Optional[float] = None):
        """Handle a 429 response, sleeping for the Retry-After duration."""
        self._consecutive_429s += 1

        if self._consecutive_429s >= settings.rate_limit.circuit_breaker_threshold:
            pause = settings.rate_limit.circuit_breaker_pause_sec
            logger.warning(
                "Circuit breaker triggered — pausing",
                consecutive_429s=self._consecutive_429s,
                pause_sec=pause,
            )
            time.sleep(pause)
            self._consecutive_429s = 0
            return

        if retry_after is not None:
            sleep_time = min(retry_after, settings.rate_limit.max_backoff_sec)
        else:
            sleep_time = min(
                settings.rate_limit.default_sleep_sec
                * (settings.rate_limit.backoff_multiplier ** self._consecutive_429s),
                settings.rate_limit.max_backoff_sec,
            )

        logger.info(
            "Rate limited — sleeping",
            retry_after=retry_after,
            sleep_sec=sleep_time,
            consecutive_429s=self._consecutive_429s,
        )
        time.sleep(sleep_time)

    def reset(self):
        """Reset consecutive 429 counter on a successful request."""
        self._consecutive_429s = 0


class SpotifyClient:
    """Client for interacting with Spotify Web API."""
    
    def __init__(self):
        """Initialize Spotify client with OAuth."""
        self.auth_manager = SpotifyOAuth(
            client_id=settings.spotify.client_id,
            client_secret=settings.spotify.client_secret,
            redirect_uri=settings.spotify.redirect_uri,
            scope="user-read-recently-played user-read-playback-state user-library-read user-top-read",
            cache_path=".spotify_cache",
            open_browser=False
        )
        
        # Track token expiry for proactive refresh
        self._token_expires_at: Optional[float] = None
        
        # If we have a refresh token, use it
        if settings.spotify.refresh_token:
            self._setup_with_refresh_token()
        
        self.sp = spotipy.Spotify(auth_manager=self.auth_manager)
        self._rate_limiter = AdaptiveRateLimiter()
        logger.info("Spotify client initialized")
    
    def _setup_with_refresh_token(self):
        """Set up auth manager with stored refresh token."""
        try:
            token_info = self.auth_manager.refresh_access_token(
                settings.spotify.refresh_token
            )
            if not token_info or "access_token" not in token_info:
                raise SpotifyOauthError("Failed to refresh access token")
            self.auth_manager.cache_handler.save_token_to_cache(token_info)
            self._token_expires_at = token_info.get("expires_at")
            logger.info("Refreshed Spotify access token using provided refresh token")
        except SpotifyOauthError as e:
            error_desc = getattr(e, "error_description", str(e))
            is_revoked = "revoked" in error_desc.lower() or "invalid_grant" in str(
                getattr(e, "error", "")
            ).lower()
            if is_revoked:
                logger.error(
                    "Spotify refresh token has been REVOKED — re-authorization required",
                    error=str(e),
                )
            else:
                logger.error(
                    "Recoverable token refresh failure — token may have expired",
                    error=str(e),
                    error_code=getattr(e, "error", None),
                    error_description=getattr(e, "error_description", None),
                )
            raise
        except Exception as e:
            logger.warning("Failed to set up refresh token", error=str(e))

    def _ensure_token_fresh(self):
        """Proactively refresh token if within the margin of expiry."""
        if self._token_expires_at is None:
            return
        remaining = self._token_expires_at - time.time()
        if remaining > settings.rate_limit.token_refresh_margin_sec:
            return
        logger.info(
            "Token nearing expiry — proactively refreshing",
            remaining_sec=remaining,
        )
        try:
            token_info = self.auth_manager.refresh_access_token(
                settings.spotify.refresh_token
            )
            if token_info and "access_token" in token_info:
                self.auth_manager.cache_handler.save_token_to_cache(token_info)
                self._token_expires_at = token_info.get("expires_at")
                logger.info("Proactive token refresh succeeded")
        except SpotifyOauthError as e:
            error_desc = getattr(e, "error_description", str(e))
            is_revoked = "revoked" in error_desc.lower() or "invalid_grant" in str(
                getattr(e, "error", "")
            ).lower()
            if is_revoked:
                logger.error(
                    "Token REVOKED during proactive refresh — re-authorization required",
                    error=str(e),
                )
                raise
            logger.warning(
                "Proactive token refresh failed (recoverable) — will retry on next call",
                error=str(e),
            )
        except Exception as e:
            logger.warning("Proactive token refresh failed unexpectedly", error=str(e))

    def _rate_limited_call(self, func, *args, **kwargs):
        """Execute a Spotify API call with adaptive rate limiting and token refresh."""
        self._ensure_token_fresh()
        self._rate_limiter.wait()
        try:
            result = func(*args, **kwargs)
            self._rate_limiter.reset()
            return result
        except spotipy.exceptions.SpotifyException as e:
            if e.http_status == 429:
                retry_after = None
                if hasattr(e, "headers") and e.headers:
                    ra_header = e.headers.get("Retry-After")
                    if ra_header is not None:
                        try:
                            retry_after = float(ra_header)
                        except (ValueError, TypeError):
                            pass
                self._rate_limiter.handle_rate_limit(retry_after)
                raise
            raise

    def authenticate(self) -> bool:
        """Ensure user is authenticated."""
        try:
            self.sp.current_user()
            logger.info("Successfully authenticated with Spotify")
            return True
        except SpotifyOauthError as e:
            logger.error(
                "Failed to authenticate with Spotify",
                error=str(e),
                error_code=getattr(e, "error", None),
                error_description=getattr(e, "error_description", None)
            )
            print("❌ Spotify authentication error. Check refresh token and client credentials.", flush=True)
            return False
        except Exception as e:
            logger.error("Failed to authenticate with Spotify", error=str(e))
            print(f"❌ Spotify authentication exception: {e}", flush=True)
            return False
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(spotipy.exceptions.SpotifyException),
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
                
            results = self._rate_limited_call(
                self.sp.current_user_recently_played, **params
            )
            tracks = results.get("items", []) if results else []
            
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
        safety_counter = 0
        max_iterations = 500

        while True:
            safety_counter += 1
            if safety_counter > max_iterations:
                logger.error(
                    "Pagination aborted due to exceeding safety limit",
                    since_timestamp=since_timestamp,
                    last_after=after,
                    max_iterations=max_iterations
                )
                break
            
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
            next_after = last_timestamp + 1
            
            if len(tracks) < settings.pipeline.batch_size:
                logger.info("Reached end of available tracks")
                break
                
            after = next_after
    
    def transform_track_data(self, track_item: Dict) -> Dict:
        """
        Transform Spotify track data into Snowflake-friendly format.
        
        Args:
            track_item: Raw track item from Spotify API
            
        Returns:
            Transformed track data
        """
        track = track_item.get("track") or {}
        played_at = track_item.get("played_at")
        context = track_item.get("context") or {}
        
        # Parse timestamp
        played_at_dt = datetime.fromisoformat(
            played_at.replace("Z", "+00:00")
        ) if played_at else datetime.now(timezone.utc)

        artists = track.get("artists") or []
        album = track.get("album") or {}
        first_artist = artists[0] if artists else {}
        
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
            "track_explicit": track.get("explicit"),
            "track_preview_url": track.get("preview_url"),
            "track_external_urls": json.dumps(track.get("external_urls") or {}),
            "track_isrc": (track.get("external_ids") or {}).get("isrc"),
            
            # Artist information
            "artists": json.dumps([
                {
                    "id": artist.get("id"),
                    "name": artist.get("name"),
                    "uri": artist.get("uri"),
                    "external_urls": artist.get("external_urls") or {}
                }
                for artist in artists
            ]),
            "primary_artist_id": first_artist.get("id"),
            "primary_artist_name": first_artist.get("name"),
            
            # Album information
            "album_id": album.get("id"),
            "album_name": album.get("name"),
            "album_type": album.get("album_type"),
            "album_release_date": album.get("release_date"),
            "album_total_tracks": album.get("total_tracks"),
            "album_images": json.dumps(album.get("images") or []),
            
            # Context (playlist, artist, album, etc.)
            "context_type": context.get("type") if context else None,
            "context_uri": context.get("uri") if context else None,
            "context_external_urls": json.dumps(context.get("external_urls") or {}) if context else None,
            
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
            track = track_item.get("track") or {}
            artists = track.get("artists") or []
            
            for artist in artists:
                artist_id = artist.get("id")
                if artist_id and artist_id not in unique_artists:
                    unique_artists[artist_id] = {
                        "id": artist_id,
                        "name": artist.get("name"),
                        "uri": artist.get("uri"),
                        "external_urls": artist.get("external_urls") or {}
                    }
        
        return list(unique_artists.values())
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(spotipy.exceptions.SpotifyException),
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
            artist = self._rate_limited_call(self.sp.artist, artist_id)
            logger.debug("Fetched artist details", artist_id=artist_id, name=artist.get("name"))
            return artist
        except Exception as e:
            logger.warning("Failed to fetch artist details", artist_id=artist_id, error=str(e))
            return None
    
    def get_multiple_artists(self, artist_ids: List[str], batch_size: int = 50) -> List[Dict]:
        """
        Get details for multiple artists using individual API calls.
        
        Note: As of February 2026, Spotify removed the batch GET /artists endpoint.
        This method now fetches artists individually via GET /artists/{id}.
        
        Args:
            artist_ids: List of Spotify artist IDs
            batch_size: Number of artists per logging batch (for progress tracking)
            
        Returns:
            List of artist detail dictionaries
        """
        all_artists = []
        
        # Remove any None/empty values
        artist_ids = [aid for aid in artist_ids if aid]
        if not artist_ids:
            return all_artists
        
        for i, artist_id in enumerate(artist_ids):
            try:
                artist = self.get_artist_details(artist_id)
                if artist:
                    all_artists.append(artist)
                
                # Log progress at batch intervals
                if (i + 1) % batch_size == 0:
                    logger.info(
                        "Artist fetch progress",
                        fetched=i + 1,
                        total=len(artist_ids),
                        success_count=len(all_artists)
                    )
                
            except Exception as e:
                logger.warning(
                    "Failed to fetch artist",
                    artist_id=artist_id,
                    error=str(e)
                )
                continue
        
        logger.info("Completed artist fetching", total_fetched=len(all_artists), total_requested=len(artist_ids))
        return all_artists
    
    def transform_artist_data(self, artist: Dict, enhance_empty_genres: bool = True) -> Dict:
        """
        Transform artist data into Snowflake-friendly format.
        
        Note: As of February 2026, Spotify removed 'popularity' and 'followers' 
        fields from artist objects.
        
        Args:
            artist: Raw artist data from Spotify API
            enhance_empty_genres: Whether to enhance artists with empty genres
            
        Returns:
            Transformed artist data with genres
        """
        genres = artist.get("genres") or []
        
        data_source = "spotify_artist_api"
        genre_inference_methods = None
        original_genres_empty = False
        
        if enhance_empty_genres and not genres:
            enhanced_artist = self._enhance_empty_genres(artist)
            genres = enhanced_artist.get("genres") or []
            genre_inference_methods = enhanced_artist.get("genre_inference_methods")
            original_genres_empty = enhanced_artist.get("original_genres_empty", False)
            if genre_inference_methods:
                data_source = f"spotify_artist_api_enhanced_{'+'.join(genre_inference_methods)}"
        
        result = {
            "artist_id": artist.get("id"),
            "artist_name": artist.get("name"),
            "artist_uri": artist.get("uri"),
            "genres": json.dumps(genres),
            "genres_list": genres,
            "primary_genre": genres[0] if genres else None,
            "genre_count": len(genres),
            "external_urls": json.dumps(artist.get("external_urls") or {}),
            "images": json.dumps(artist.get("images") or []),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "data_source": data_source
        }
        
        if original_genres_empty:
            result["original_genres_empty"] = True
            if genre_inference_methods:
                result["genre_inference_methods"] = json.dumps(genre_inference_methods)
        
        return result
    
    def _enhance_empty_genres(self, artist: Dict) -> Dict:
        """
        Enhance artist with inferred genres when original genres are empty.
        
        Note: As of February 2026, popularity/followers fields are no longer available,
        so genre inference relies solely on name-based patterns.
        
        Args:
            artist: Raw artist data from Spotify API
            
        Returns:
            Enhanced artist data with inferred genres
        """
        original_genres = artist.get("genres") or []
        
        if original_genres:
            return artist
        
        inferred_genres = []
        inference_methods = []
        
        name_genre = self._infer_genre_from_name(artist.get("name") or "")
        if name_genre:
            inferred_genres.append(name_genre)
            inference_methods.append("name_pattern")
        
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
        
        enhanced_data = artist.copy()
        enhanced_data["genres"] = ["unclassified"]
        enhanced_data["genre_inference_methods"] = ["fallback"]
        enhanced_data["original_genres_empty"] = True
        
        logger.warning("Could not infer genres for artist",
                      artist_name=artist.get("name"),
                      artist_id=artist.get("id"))
        
        return enhanced_data
    
    def _infer_genre_from_name(self, artist_name: str) -> Optional[str]:
        """Infer genre from artist name patterns."""
        if not artist_name:
            return None
            
        name_lower = artist_name.lower()
        
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
        
        for genre, patterns in genre_patterns.items():
            for pattern in patterns:
                if pattern in name_lower:
                    return genre
        
        return None
    
    def search(self, query: str, search_type: str = 'track', limit: int = 10) -> Dict:
        """
        Search Spotify catalog.
        
        Note: As of February 2026, Spotify reduced search limits from 50 to 10 max,
        and default from 20 to 5.
        
        Args:
            query: Search query string
            search_type: Type of search ('track', 'artist', 'album', etc.)
            limit: Maximum number of results (max 10 as of Feb 2026)
            
        Returns:
            Search results from Spotify API
        """
        try:
            effective_limit = min(limit, 10)
            if limit > 10:
                logger.warning(
                    "Search limit capped at 10 per Spotify API Feb 2026 changes",
                    requested=limit,
                    effective=effective_limit
                )
            logger.info(f"Searching Spotify: {query} (type: {search_type}, limit: {effective_limit})")
            results = self._rate_limited_call(
                self.sp.search, q=query, type=search_type, limit=effective_limit
            )
            return results or {}
        except Exception as e:
            logger.error(f"Error searching Spotify: {e}")
            return {}
    
    def get_recommendations(self, **kwargs) -> Dict:
        """
        DEPRECATED: The Spotify /recommendations endpoint was removed in February 2026.

        This method is retained for backward compatibility with existing callers
        (e.g., spotify_discovery_system.py) and will be cleaned up in Workstream C.
        It will always return an empty tracks list and log a deprecation warning.

        Args:
            **kwargs: Legacy recommendation parameters (ignored).

        Returns:
            Dict with empty 'tracks' list.
        """
        logger.warning(
            "get_recommendations called but the Spotify /recommendations endpoint "
            "was removed in February 2026. Returning empty results."
        )
        return {'tracks': []}
