"""Tests for lastfm_client.py and musicbrainz_client.py: HTTP fixture replay."""

from unittest.mock import patch, MagicMock

import pytest
import responses

# ---------------------------------------------------------------------------
# Mock settings before imports
# ---------------------------------------------------------------------------

_MOCK_SETTINGS = MagicMock()
_MOCK_SETTINGS.lastfm_api_key = "fake_lastfm_key"
_MOCK_SETTINGS.lastfm_rate_limit_per_sec = 5.0
_MOCK_SETTINGS.musicbrainz_user_agent = "TestApp/1.0 (test@test.com)"
_MOCK_SETTINGS.musicbrainz_rate_limit_per_sec = 1.0


# ---------------------------------------------------------------------------
# Last.fm client tests
# ---------------------------------------------------------------------------

class TestLastfmClientGetArtistInfo:
    """Test LastfmClient.get_artist_info parses fixture JSON correctly."""

    @responses.activate
    def test_parses_artist_info(self, lastfm_artist_info_response):
        """Should return correctly shaped row from a valid response."""
        responses.add(
            responses.GET,
            "http://ws.audioscrobbler.com/2.0/",
            json=lastfm_artist_info_response,
            status=200,
        )
        with patch("lastfm_client.settings", _MOCK_SETTINGS):
            from lastfm_client import LastfmClient
            client = LastfmClient(api_key="test_key")
            result = client.get_artist_info("Liam Gallagher")

        assert result is not None
        assert result["artist_name"] == "Liam Gallagher"
        assert result["mbid"] == "fd857293-5ab8-40de-b29e-55a69d4e4d0f"
        assert result["listeners"] == 1350000
        assert result["playcount"] == 28000000
        assert len(result["top_tags"]) == 3
        assert result["top_tags"][0]["name"] == "britpop"
        assert "lastfm_url" in result
        assert result["enriched_at"] is not None

    @responses.activate
    def test_returns_none_on_api_error(self):
        """Should return None when Last.fm returns an error response."""
        responses.add(
            responses.GET,
            "http://ws.audioscrobbler.com/2.0/",
            json={"error": 6, "message": "Artist not found"},
            status=200,
        )
        with patch("lastfm_client.settings", _MOCK_SETTINGS):
            from lastfm_client import LastfmClient
            client = LastfmClient(api_key="test_key")
            result = client.get_artist_info("Nonexistent Artist")

        assert result is None

    @responses.activate
    def test_returns_none_on_http_error(self):
        """Should return None on HTTP 500 without crashing."""
        responses.add(
            responses.GET,
            "http://ws.audioscrobbler.com/2.0/",
            json={},
            status=500,
        )
        with patch("lastfm_client.settings", _MOCK_SETTINGS):
            from lastfm_client import LastfmClient
            client = LastfmClient(api_key="test_key")
            result = client.get_artist_info("Any Artist")

        assert result is None


class TestLastfmClientGetSimilarArtists:
    """Test LastfmClient.get_similar_artists."""

    @responses.activate
    def test_parses_similar_artists(self, lastfm_similar_artists_response):
        """Should return list of {name, mbid, match} dicts."""
        responses.add(
            responses.GET,
            "http://ws.audioscrobbler.com/2.0/",
            json=lastfm_similar_artists_response,
            status=200,
        )
        with patch("lastfm_client.settings", _MOCK_SETTINGS):
            from lastfm_client import LastfmClient
            client = LastfmClient(api_key="test_key")
            result = client.get_similar_artists("Liam Gallagher")

        assert len(result) == 2
        assert result[0]["name"] == "Noel Gallagher"
        assert result[0]["match"] == 0.95
        assert result[1]["mbid"] == "some-mbid-2"


class TestLastfmEnrichArtist:
    """Test full enrichment pipeline combining info + similar."""

    @responses.activate
    def test_enrich_artist_combines_info_and_similar(
        self, lastfm_artist_info_response, lastfm_similar_artists_response
    ):
        """enrich_artist should combine getinfo and getsimilar into one row."""
        # Two sequential HTTP calls: first getinfo, then getsimilar
        responses.add(
            responses.GET,
            "http://ws.audioscrobbler.com/2.0/",
            json=lastfm_artist_info_response,
            status=200,
        )
        responses.add(
            responses.GET,
            "http://ws.audioscrobbler.com/2.0/",
            json=lastfm_similar_artists_response,
            status=200,
        )
        with patch("lastfm_client.settings", _MOCK_SETTINGS):
            from lastfm_client import LastfmClient
            client = LastfmClient(api_key="test_key")
            result = client.enrich_artist("Liam Gallagher", source_artist_id="spotify-id-123")

        assert result is not None
        assert result["artist_name"] == "Liam Gallagher"
        assert result["source_artist_id"] == "spotify-id-123"
        assert result["similar_artists"] is not None
        assert len(result["similar_artists"]) == 2


# ---------------------------------------------------------------------------
# MusicBrainz client tests
# ---------------------------------------------------------------------------

class TestMusicBrainzClientLookupByName:
    """Test MusicBrainzClient.lookup_artist_by_name."""

    def test_parses_search_result(self, musicbrainz_search_response):
        """Should return a row dict with expected fields from search_artists."""
        with patch("musicbrainz_client.settings", _MOCK_SETTINGS), \
             patch("musicbrainz_client.musicbrainzngs") as mock_mb:
            mock_mb.search_artists.return_value = musicbrainz_search_response
            from musicbrainz_client import MusicBrainzClient
            client = MusicBrainzClient(user_agent="Test/1.0")
            result = client.lookup_artist_by_name("Liam Gallagher")

        assert result is not None
        assert result["mbid"] == "fd857293-5ab8-40de-b29e-55a69d4e4d0f"
        assert result["artist_name"] == "Liam Gallagher"
        assert result["sort_name"] == "Gallagher, Liam"
        assert result["artist_type"] == "Person"
        assert result["country"] == "GB"
        assert result["area"] == "Manchester"
        assert result["begin_date"] == "1972-09-21"
        assert result["disambiguation"] == "Oasis singer"
        assert len(result["tags"]) == 2
        assert result["tags"][0]["name"] == "britpop"

    def test_returns_none_when_no_match(self):
        """Should return None if no artists match."""
        with patch("musicbrainz_client.settings", _MOCK_SETTINGS), \
             patch("musicbrainz_client.musicbrainzngs") as mock_mb:
            mock_mb.search_artists.return_value = {"artist-list": []}
            from musicbrainz_client import MusicBrainzClient
            client = MusicBrainzClient(user_agent="Test/1.0")
            result = client.lookup_artist_by_name("zzzzNonexistent")

        assert result is None


class TestMusicBrainzClientLookupByISRC:
    """Test MusicBrainzClient.lookup_artist_by_isrc."""

    def test_isrc_lookup_returns_artist(
        self, musicbrainz_isrc_response, musicbrainz_artist_by_id_response
    ):
        """Should chain ISRC → recording → artist MBID → full artist."""
        with patch("musicbrainz_client.settings", _MOCK_SETTINGS), \
             patch("musicbrainz_client.musicbrainzngs") as mock_mb:
            mock_mb.get_recordings_by_isrc.return_value = musicbrainz_isrc_response
            mock_mb.get_artist_by_id.return_value = musicbrainz_artist_by_id_response
            from musicbrainz_client import MusicBrainzClient
            client = MusicBrainzClient(user_agent="Test/1.0")
            result = client.lookup_artist_by_isrc("GBARL1700417")

        assert result is not None
        assert result["mbid"] == "fd857293-5ab8-40de-b29e-55a69d4e4d0f"
        assert result["isrc_lookup_key"] == "GBARL1700417"
        assert result["artist_name"] == "Liam Gallagher"

    def test_isrc_lookup_returns_none_when_no_recordings(self):
        """Should return None if ISRC has no recordings."""
        with patch("musicbrainz_client.settings", _MOCK_SETTINGS), \
             patch("musicbrainz_client.musicbrainzngs") as mock_mb:
            mock_mb.get_recordings_by_isrc.return_value = {"isrc": {"recording-list": []}}
            from musicbrainz_client import MusicBrainzClient
            client = MusicBrainzClient(user_agent="Test/1.0")
            result = client.lookup_artist_by_isrc("USXXX0000000")

        assert result is None


class TestMusicBrainzEnrichArtist:
    """Test enrich_artist preferring ISRC, falling back to name."""

    def test_prefers_isrc_over_name(
        self, musicbrainz_isrc_response, musicbrainz_artist_by_id_response
    ):
        """If ISRC is provided, should use ISRC path first."""
        with patch("musicbrainz_client.settings", _MOCK_SETTINGS), \
             patch("musicbrainz_client.musicbrainzngs") as mock_mb:
            mock_mb.get_recordings_by_isrc.return_value = musicbrainz_isrc_response
            mock_mb.get_artist_by_id.return_value = musicbrainz_artist_by_id_response
            from musicbrainz_client import MusicBrainzClient
            client = MusicBrainzClient(user_agent="Test/1.0")
            result = client.enrich_artist(
                "Liam Gallagher", isrc="GBARL1700417", source_artist_id="sp-123"
            )

        assert result is not None
        assert result["source_artist_id"] == "sp-123"
        # search_artists should NOT have been called since ISRC succeeded
        mock_mb.search_artists.assert_not_called()

    def test_falls_back_to_name_when_isrc_fails(self, musicbrainz_search_response):
        """If ISRC lookup returns None, should fall back to name search."""
        with patch("musicbrainz_client.settings", _MOCK_SETTINGS), \
             patch("musicbrainz_client.musicbrainzngs") as mock_mb:
            mock_mb.get_recordings_by_isrc.return_value = {"isrc": {"recording-list": []}}
            mock_mb.search_artists.return_value = musicbrainz_search_response
            from musicbrainz_client import MusicBrainzClient
            client = MusicBrainzClient(user_agent="Test/1.0")
            result = client.enrich_artist("Liam Gallagher", isrc="BAD_ISRC")

        assert result is not None
        assert result["artist_name"] == "Liam Gallagher"
