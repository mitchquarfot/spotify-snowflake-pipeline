"""Tests for spotify_client.py: AdaptiveRateLimiter, transform_track_data, OAuth."""

import time
from unittest.mock import patch, MagicMock, PropertyMock

import pytest


# ---------------------------------------------------------------------------
# Patch config.settings BEFORE importing spotify_client so that Settings()
# doesn't fail due to missing env vars.
# ---------------------------------------------------------------------------

_MOCK_SETTINGS = MagicMock()
_MOCK_SETTINGS.spotify.client_id = "fake_id"
_MOCK_SETTINGS.spotify.client_secret = "fake_secret"
_MOCK_SETTINGS.spotify.redirect_uri = "http://localhost:8080/callback"
_MOCK_SETTINGS.spotify.refresh_token = None
_MOCK_SETTINGS.rate_limit.requests_per_minute = 100
_MOCK_SETTINGS.rate_limit.default_sleep_sec = 0.6
_MOCK_SETTINGS.rate_limit.backoff_multiplier = 1.5
_MOCK_SETTINGS.rate_limit.max_backoff_sec = 120.0
_MOCK_SETTINGS.rate_limit.circuit_breaker_threshold = 5
_MOCK_SETTINGS.rate_limit.circuit_breaker_pause_sec = 60.0
_MOCK_SETTINGS.rate_limit.token_refresh_margin_sec = 300
_MOCK_SETTINGS.pipeline.batch_size = 50


# ---------------------------------------------------------------------------
# AdaptiveRateLimiter tests
# ---------------------------------------------------------------------------

class TestAdaptiveRateLimiter:
    """Tests for Retry-After parsing and adaptive backoff logic."""

    def _make_limiter(self):
        with patch("spotify_client.settings", _MOCK_SETTINGS):
            from spotify_client import AdaptiveRateLimiter
            return AdaptiveRateLimiter()

    def test_handle_rate_limit_with_retry_after(self):
        """Should sleep exactly the Retry-After duration (capped by max_backoff)."""
        limiter = self._make_limiter()
        with patch("spotify_client.time.sleep") as mock_sleep:
            limiter.handle_rate_limit(retry_after=3.0)
            mock_sleep.assert_called_once_with(3.0)

    def test_handle_rate_limit_without_retry_after_uses_exponential_backoff(self):
        """Without Retry-After, should use exponential backoff based on consecutive 429s."""
        limiter = self._make_limiter()
        with patch("spotify_client.time.sleep") as mock_sleep:
            # First 429: default_sleep * backoff^1 = 0.6 * 1.5^1 = 0.9
            limiter.handle_rate_limit(retry_after=None)
            assert mock_sleep.call_count == 1
            slept = mock_sleep.call_args[0][0]
            expected = 0.6 * (1.5 ** 1)
            assert abs(slept - expected) < 0.01

    def test_handle_rate_limit_caps_at_max_backoff(self):
        """Retry-After values above max_backoff should be capped."""
        limiter = self._make_limiter()
        with patch("spotify_client.time.sleep") as mock_sleep:
            limiter.handle_rate_limit(retry_after=9999.0)
            mock_sleep.assert_called_once_with(120.0)

    def test_circuit_breaker_triggers_after_threshold(self):
        """After N consecutive 429s, circuit breaker should pause for longer."""
        limiter = self._make_limiter()
        # Simulate consecutive 429s up to threshold
        with patch("spotify_client.time.sleep") as mock_sleep:
            for _ in range(4):
                limiter.handle_rate_limit(retry_after=1.0)

            # The 5th call should trigger circuit breaker (threshold=5)
            limiter.handle_rate_limit(retry_after=1.0)
            # Last call should be the circuit breaker pause (60s)
            assert mock_sleep.call_args[0][0] == 60.0

    def test_reset_clears_consecutive_counter(self):
        """reset() should zero the consecutive 429 counter."""
        limiter = self._make_limiter()
        with patch("spotify_client.time.sleep"):
            limiter.handle_rate_limit(retry_after=1.0)
            limiter.handle_rate_limit(retry_after=1.0)
        assert limiter._consecutive_429s == 2
        limiter.reset()
        assert limiter._consecutive_429s == 0

    def test_wait_sleeps_minimum_interval(self):
        """wait() should enforce minimum interval between requests."""
        limiter = self._make_limiter()
        limiter._last_request_time = time.monotonic()  # just called
        with patch("spotify_client.time.sleep") as mock_sleep:
            with patch("spotify_client.time.monotonic", return_value=limiter._last_request_time + 0.1):
                limiter.wait()
            # Should sleep the remaining interval
            assert mock_sleep.called


# ---------------------------------------------------------------------------
# transform_track_data tests
# ---------------------------------------------------------------------------

class TestTransformTrackData:
    """Tests for SpotifyClient.transform_track_data."""

    def _make_client(self):
        """Create a SpotifyClient with mocked auth (no network)."""
        with patch("spotify_client.settings", _MOCK_SETTINGS):
            with patch("spotify_client.SpotifyOAuth") as mock_oauth:
                mock_oauth.return_value = MagicMock()
                with patch("spotify_client.spotipy.Spotify"):
                    from spotify_client import SpotifyClient
                    client = SpotifyClient()
                    return client

    def test_extracts_isrc_from_external_ids(self, sample_track_item):
        """Should extract track_isrc from external_ids.isrc."""
        client = self._make_client()
        result = client.transform_track_data(sample_track_item)
        assert result["track_isrc"] == "GBARL1700417"

    def test_tolerates_none_external_ids(self, sample_track_item_no_external_ids):
        """Should return track_isrc=None when external_ids is None."""
        client = self._make_client()
        result = client.transform_track_data(sample_track_item_no_external_ids)
        assert result["track_isrc"] is None

    def test_tolerates_missing_external_ids_key(self):
        """Should handle track with no external_ids key at all."""
        client = self._make_client()
        track_item = {
            "track": {
                "id": "abc123",
                "name": "Test",
                "duration_ms": 180000,
                "explicit": False,
                "preview_url": None,
                "external_urls": {},
                "uri": "spotify:track:abc123",
                "artists": [],
                "album": {},
            },
            "played_at": "2026-06-01T12:00:00Z",
            "context": None,
        }
        result = client.transform_track_data(track_item)
        assert result["track_isrc"] is None
        assert result["track_id"] == "abc123"

    def test_required_fields_present(self, sample_track_item):
        """All critical output fields should be populated."""
        client = self._make_client()
        result = client.transform_track_data(sample_track_item)
        assert result["track_id"] == "4iV5W9uYEdYUVa79Axb7Rh"
        assert result["played_at"] == "2026-06-08T14:30:00Z"
        assert result["primary_artist_name"] == "Liam Gallagher"
        assert result["album_name"] == "As You Were"


class TestTransformArtistData:
    """Tests for transform_artist_data with absent popularity/followers."""

    def _make_client(self):
        with patch("spotify_client.settings", _MOCK_SETTINGS):
            with patch("spotify_client.SpotifyOAuth") as mock_oauth:
                mock_oauth.return_value = MagicMock()
                with patch("spotify_client.spotipy.Spotify"):
                    from spotify_client import SpotifyClient
                    return SpotifyClient()

    def test_handles_no_popularity_no_followers(self, sample_artist_payload):
        """Feb 2026: popularity and followers are absent; transform should not crash."""
        client = self._make_client()
        # Explicitly verify these fields are absent (simulates the API removal)
        assert "popularity" not in sample_artist_payload
        assert "followers" not in sample_artist_payload
        result = client.transform_artist_data(sample_artist_payload, enhance_empty_genres=False)
        assert result["artist_id"] == "2DaxqgrOhkeH0fpeiQq2f4"
        assert result["artist_name"] == "Liam Gallagher"
        assert result["genre_count"] == 2


# ---------------------------------------------------------------------------
# OAuth scope tests
# ---------------------------------------------------------------------------

class TestOAuthScope:
    """Verify required scopes are set on the auth manager."""

    def test_scope_includes_user_top_read(self):
        """The OAuth scope string must include user-top-read."""
        with patch("spotify_client.settings", _MOCK_SETTINGS):
            with patch("spotify_client.SpotifyOAuth") as mock_oauth:
                mock_oauth.return_value = MagicMock()
                with patch("spotify_client.spotipy.Spotify"):
                    from spotify_client import SpotifyClient
                    SpotifyClient()
                    call_kwargs = mock_oauth.call_args[1]
                    scope_str = call_kwargs["scope"]
                    assert "user-top-read" in scope_str

    def test_scope_includes_recently_played(self):
        """The OAuth scope string must include user-read-recently-played."""
        with patch("spotify_client.settings", _MOCK_SETTINGS):
            with patch("spotify_client.SpotifyOAuth") as mock_oauth:
                mock_oauth.return_value = MagicMock()
                with patch("spotify_client.spotipy.Spotify"):
                    from spotify_client import SpotifyClient
                    SpotifyClient()
                    call_kwargs = mock_oauth.call_args[1]
                    scope_str = call_kwargs["scope"]
                    assert "user-read-recently-played" in scope_str


# ---------------------------------------------------------------------------
# Token refresh tests
# ---------------------------------------------------------------------------

class TestTokenRefresh:
    """Test proactive token refresh and revoked-vs-expired distinction."""

    def test_proactive_refresh_before_expiry(self):
        """Token should be refreshed when remaining time < margin."""
        with patch("spotify_client.settings", _MOCK_SETTINGS):
            with patch("spotify_client.SpotifyOAuth") as mock_oauth:
                mock_instance = MagicMock()
                mock_instance.refresh_access_token.return_value = {
                    "access_token": "new_token",
                    "expires_at": time.time() + 3600,
                }
                mock_instance.cache_handler = MagicMock()
                mock_oauth.return_value = mock_instance
                with patch("spotify_client.spotipy.Spotify"):
                    from spotify_client import SpotifyClient
                    client = SpotifyClient()
                    client._token_expires_at = time.time() + 100  # < 300s margin
                    client._ensure_token_fresh()
                    # Should have called refresh
                    mock_instance.refresh_access_token.assert_called()

    def test_no_refresh_when_token_far_from_expiry(self):
        """Token should NOT be refreshed when remaining time > margin."""
        with patch("spotify_client.settings", _MOCK_SETTINGS):
            with patch("spotify_client.SpotifyOAuth") as mock_oauth:
                mock_instance = MagicMock()
                mock_instance.cache_handler = MagicMock()
                mock_oauth.return_value = mock_instance
                with patch("spotify_client.spotipy.Spotify"):
                    from spotify_client import SpotifyClient
                    client = SpotifyClient()
                    client._token_expires_at = time.time() + 3600  # plenty of time
                    client._ensure_token_fresh()
                    mock_instance.refresh_access_token.assert_not_called()

    def test_revoked_token_raises(self):
        """A revoked token should raise SpotifyOauthError with revoked indication."""
        from spotipy.oauth2 import SpotifyOauthError

        with patch("spotify_client.settings", _MOCK_SETTINGS):
            with patch("spotify_client.SpotifyOAuth") as mock_oauth:
                err = SpotifyOauthError("token revoked")
                err.error_description = "Token has been revoked"
                err.error = "invalid_grant"
                mock_instance = MagicMock()
                mock_instance.refresh_access_token.side_effect = err
                mock_instance.cache_handler = MagicMock()
                mock_oauth.return_value = mock_instance
                with patch("spotify_client.spotipy.Spotify"):
                    from spotify_client import SpotifyClient
                    client = SpotifyClient()
                    client._token_expires_at = time.time() + 100
                    with pytest.raises(SpotifyOauthError):
                        client._ensure_token_fresh()

    def test_expired_token_does_not_raise(self):
        """A non-revoked refresh failure should be swallowed (recoverable)."""
        from spotipy.oauth2 import SpotifyOauthError

        with patch("spotify_client.settings", _MOCK_SETTINGS):
            with patch("spotify_client.SpotifyOAuth") as mock_oauth:
                err = SpotifyOauthError("temporary error")
                err.error_description = "Service temporarily unavailable"
                err.error = "server_error"
                mock_instance = MagicMock()
                mock_instance.refresh_access_token.side_effect = err
                mock_instance.cache_handler = MagicMock()
                mock_oauth.return_value = mock_instance
                with patch("spotify_client.spotipy.Spotify"):
                    from spotify_client import SpotifyClient
                    client = SpotifyClient()
                    client._token_expires_at = time.time() + 100
                    # Should NOT raise (recoverable)
                    client._ensure_token_fresh()
