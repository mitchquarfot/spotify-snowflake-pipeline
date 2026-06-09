"""Tests for pipeline.py: Snowflake-backed state, DLQ routing, validation."""

import json
from unittest.mock import patch, MagicMock

import pytest


# Mock settings before importing pipeline
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
_MOCK_SETTINGS.pipeline.fetch_interval_minutes = 30
_MOCK_SETTINGS.pipeline.max_runtime_minutes = 30
_MOCK_SETTINGS.pipeline.snowflake_stage_prefix = "spotify/"
_MOCK_SETTINGS.snowflake.account = "test_account"
_MOCK_SETTINGS.snowflake.user = "test_user"
_MOCK_SETTINGS.snowflake.password = "test_pass"
_MOCK_SETTINGS.snowflake.warehouse = "test_wh"
_MOCK_SETTINGS.snowflake.database = "SPOTIFY_ANALYTICS"
_MOCK_SETTINGS.snowflake.role = None
_MOCK_SETTINGS.snowflake.private_key_path = None
_MOCK_SETTINGS.snowflake.private_key = None
_MOCK_SETTINGS.snowflake.private_key_passphrase = None


def _make_pipeline():
    """Create a SpotifyDataPipeline with fully mocked dependencies."""
    with patch("pipeline.settings", _MOCK_SETTINGS), \
         patch("spotify_client.settings", _MOCK_SETTINGS), \
         patch("pipeline.SpotifyClient") as mock_sp, \
         patch("pipeline.S3Client") as mock_s3, \
         patch("pipeline.ArtistGenreProcessor"):
        mock_s3.return_value.bucket_name = "test-bucket"
        mock_sp.return_value.authenticate.return_value = True
        from pipeline import SpotifyDataPipeline
        pipeline = SpotifyDataPipeline(enable_artist_genre_processing=False)
        return pipeline


# ---------------------------------------------------------------------------
# load_state / save_state tests
# ---------------------------------------------------------------------------

class TestLoadState:
    """Test load_state from mocked Snowflake connection."""

    def test_returns_empty_dict_on_first_run(self):
        """When table has no rows, load_state should return {}."""
        pipeline = _make_pipeline()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []  # no rows
        mock_conn.cursor.return_value = mock_cursor

        with patch("pipeline._get_snowflake_connection", return_value=mock_conn):
            state = pipeline.load_state()
        assert state == {}

    def test_returns_parsed_state_dict(self):
        """Should parse JSON state_value from Snowflake rows."""
        pipeline = _make_pipeline()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("last_processed_timestamp", "1717850000000"),
            ("last_updated", '"2026-06-08T15:00:00+00:00"'),
        ]
        mock_conn.cursor.return_value = mock_cursor

        with patch("pipeline._get_snowflake_connection", return_value=mock_conn):
            state = pipeline.load_state()
        assert state["last_processed_timestamp"] == 1717850000000
        assert "2026-06-08" in state["last_updated"]

    def test_returns_empty_on_connection_failure(self):
        """If Snowflake connection fails, should return {} (safe default)."""
        pipeline = _make_pipeline()
        with patch("pipeline._get_snowflake_connection", side_effect=Exception("conn failed")):
            state = pipeline.load_state()
        assert state == {}


class TestSaveState:
    """Test save_state writes to Snowflake via MERGE."""

    def test_executes_merge_for_each_key(self):
        """Should execute one MERGE per state key."""
        pipeline = _make_pipeline()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("pipeline._get_snowflake_connection", return_value=mock_conn):
            pipeline.save_state({"key1": "val1", "key2": 42})

        assert mock_cursor.execute.call_count == 2
        # Verify MERGE SQL is used
        first_call_sql = mock_cursor.execute.call_args_list[0][0][0]
        assert "MERGE" in first_call_sql

    def test_round_trip_state(self):
        """State saved and loaded back should match (mocked cursor)."""
        pipeline = _make_pipeline()
        state_to_save = {"last_processed_timestamp": 1717850000000}

        # Mock save
        save_conn = MagicMock()
        save_cursor = MagicMock()
        save_conn.cursor.return_value = save_cursor

        with patch("pipeline._get_snowflake_connection", return_value=save_conn):
            pipeline.save_state(state_to_save)

        # Mock load — simulate what Snowflake would return after the MERGE
        load_conn = MagicMock()
        load_cursor = MagicMock()
        load_cursor.fetchall.return_value = [
            ("last_processed_timestamp", json.dumps(1717850000000)),
        ]
        load_conn.cursor.return_value = load_cursor

        with patch("pipeline._get_snowflake_connection", return_value=load_conn):
            loaded = pipeline.load_state()

        assert loaded["last_processed_timestamp"] == state_to_save["last_processed_timestamp"]


# ---------------------------------------------------------------------------
# DLQ / per-record error handling tests
# ---------------------------------------------------------------------------

class TestDLQRouting:
    """A bad record should go to DLQ without dropping the good ones."""

    def test_bad_record_routes_to_dlq(self, sample_track_item):
        """A record with null track_id should land in errors, rest uploaded."""
        pipeline = _make_pipeline()

        # Create a bad track item (track.id is None)
        bad_item = {
            "track": {
                "id": None,
                "name": "Bad Track",
                "duration_ms": 100,
                "explicit": False,
                "preview_url": None,
                "external_urls": {},
                "uri": None,
                "artists": [],
                "album": {},
            },
            "played_at": "2026-06-08T12:00:00Z",
            "context": None,
        }

        # Mock S3 upload to succeed
        pipeline.s3_client.upload_tracks_batch = MagicMock(return_value="s3://bucket/key.json")
        # Mock DLQ write
        with patch("pipeline._get_snowflake_connection") as mock_conn_fn:
            mock_conn = MagicMock()
            mock_conn_fn.return_value = mock_conn
            mock_conn.cursor.return_value = MagicMock()

            # Patch transform on the client to just pass-through our fixture
            pipeline.spotify_client.transform_track_data = lambda item: {
                "track_id": item["track"]["id"],
                "played_at": item["played_at"],
                "track_name": item["track"]["name"],
            }

            uploaded, errors = pipeline.process_batch(
                [sample_track_item, bad_item], run_id="test-run"
            )

        assert uploaded == 1  # good record uploaded
        assert errors == 1  # bad record counted as error

    def test_all_good_records_no_errors(self, sample_track_item):
        """When all records are valid, error count should be 0."""
        pipeline = _make_pipeline()
        pipeline.s3_client.upload_tracks_batch = MagicMock(return_value="s3://bucket/key.json")

        with patch("pipeline._get_snowflake_connection") as mock_conn_fn:
            mock_conn = MagicMock()
            mock_conn_fn.return_value = mock_conn
            mock_conn.cursor.return_value = MagicMock()

            # Normal transform
            pipeline.spotify_client.transform_track_data = lambda item: {
                "track_id": item["track"]["id"],
                "played_at": item["played_at"],
                "track_name": item["track"]["name"],
            }

            uploaded, errors = pipeline.process_batch(
                [sample_track_item], run_id="test-run"
            )

        assert uploaded == 1
        assert errors == 0


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------

class TestValidation:
    """Test _validate_record rejects null track_id / played_at."""

    def test_rejects_null_track_id(self):
        """Should return error message for missing track_id."""
        pipeline = _make_pipeline()
        result = pipeline._validate_record({"track_id": None, "played_at": "2026-01-01T00:00:00Z"})
        assert result is not None
        assert "track_id" in result

    def test_rejects_null_played_at(self):
        """Should return error message for missing played_at."""
        pipeline = _make_pipeline()
        result = pipeline._validate_record({"track_id": "abc", "played_at": None})
        assert result is not None
        assert "played_at" in result

    def test_rejects_empty_track_id(self):
        """Should return error for empty-string track_id."""
        pipeline = _make_pipeline()
        result = pipeline._validate_record({"track_id": "", "played_at": "2026-01-01T00:00:00Z"})
        assert result is not None

    def test_accepts_valid_record(self):
        """Valid record should return None (no error)."""
        pipeline = _make_pipeline()
        result = pipeline._validate_record({"track_id": "abc123", "played_at": "2026-06-08T10:00:00Z"})
        assert result is None
