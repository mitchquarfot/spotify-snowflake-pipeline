"""Configuration management for Spotify to S3 pipeline."""

import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Main application settings with all configurations."""
    
    # Spotify API configuration
    spotify_client_id: str = Field(..., env="SPOTIFY_CLIENT_ID")
    spotify_client_secret: str = Field(..., env="SPOTIFY_CLIENT_SECRET") 
    spotify_redirect_uri: str = Field("http://localhost:8080/callback", env="SPOTIFY_REDIRECT_URI")
    spotify_refresh_token: Optional[str] = Field(None, env="SPOTIFY_REFRESH_TOKEN")
    
    # AWS configuration
    aws_access_key_id: str = Field(..., env="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = Field(..., env="AWS_SECRET_ACCESS_KEY")
    aws_region: str = Field("us-west-2", env="AWS_REGION")
    s3_bucket_name: str = Field(..., env="S3_BUCKET_NAME")
    
    # Pipeline configuration
    fetch_interval_minutes: int = Field(30, env="FETCH_INTERVAL_MINUTES")
    batch_size: int = Field(50, env="BATCH_SIZE")
    max_retries: int = Field(3, env="MAX_RETRIES")
    max_runtime_minutes: int = Field(30, env="MAX_RUNTIME_MINUTES")
    snowflake_stage_prefix: str = Field("spotify_listening_history/", env="SNOWFLAKE_STAGE_PREFIX")
    date_partition_format: str = Field("%Y/%m/%d", env="DATE_PARTITION_FORMAT")

    # Snowflake configuration
    snowflake_account: Optional[str] = Field(None, env="SNOWFLAKE_ACCOUNT")
    snowflake_user: Optional[str] = Field(None, env="SNOWFLAKE_USER")
    snowflake_password: Optional[str] = Field(None, env="SNOWFLAKE_PASSWORD")
    snowflake_private_key_path: Optional[str] = Field(None, env="SNOWFLAKE_PRIVATE_KEY_PATH")
    snowflake_private_key: Optional[str] = Field(None, env="SNOWFLAKE_PRIVATE_KEY")
    snowflake_private_key_passphrase: Optional[str] = Field(None, env="SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    snowflake_warehouse: Optional[str] = Field(None, env="SNOWFLAKE_WAREHOUSE")
    snowflake_database: str = Field("SPOTIFY_ANALYTICS", env="SNOWFLAKE_DATABASE")
    snowflake_schema: str = Field("ANALYTICS", env="SNOWFLAKE_SCHEMA")
    snowflake_role: Optional[str] = Field(None, env="SNOWFLAKE_ROLE")

    # Snowflake pipeline state settings (for state table backing)
    snowflake_state_database: str = Field("SPOTIFY_ANALYTICS", env="SNOWFLAKE_STATE_DATABASE")
    snowflake_state_schema: str = Field("RAW_DATA", env="SNOWFLAKE_STATE_SCHEMA")
    snowflake_state_table: str = Field("PIPELINE_STATE", env="SNOWFLAKE_STATE_TABLE")

    # Last.fm API configuration
    lastfm_api_key: Optional[str] = Field(None, env="LASTFM_API_KEY")
    lastfm_api_secret: Optional[str] = Field(None, env="LASTFM_API_SECRET")
    lastfm_rate_limit_per_sec: float = Field(5.0, env="LASTFM_RATE_LIMIT_PER_SEC")

    # MusicBrainz configuration
    musicbrainz_user_agent: str = Field(
        "SpotifySnowflakePipeline/1.0 (mitch.quarfot@snowflake.com)",
        env="MUSICBRAINZ_USER_AGENT",
    )
    musicbrainz_rate_limit_per_sec: float = Field(1.0, env="MUSICBRAINZ_RATE_LIMIT_PER_SEC")

    # Rate limiting and backoff tunables
    rate_limit_requests_per_minute: int = Field(100, env="RATE_LIMIT_REQUESTS_PER_MINUTE")
    rate_limit_default_sleep_sec: float = Field(0.6, env="RATE_LIMIT_DEFAULT_SLEEP_SEC")
    rate_limit_backoff_multiplier: float = Field(1.5, env="RATE_LIMIT_BACKOFF_MULTIPLIER")
    rate_limit_max_backoff_sec: float = Field(120.0, env="RATE_LIMIT_MAX_BACKOFF_SEC")
    rate_limit_circuit_breaker_threshold: int = Field(5, env="RATE_LIMIT_CIRCUIT_BREAKER_THRESHOLD")
    rate_limit_circuit_breaker_pause_sec: float = Field(60.0, env="RATE_LIMIT_CIRCUIT_BREAKER_PAUSE_SEC")
    token_refresh_margin_sec: int = Field(300, env="TOKEN_REFRESH_MARGIN_SEC")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    # Convenience properties to maintain backward compatibility
    @property
    def spotify(self):
        """Spotify configuration namespace."""
        class SpotifyConfig:
            def __init__(self, settings):
                self.client_id = settings.spotify_client_id
                self.client_secret = settings.spotify_client_secret
                self.redirect_uri = settings.spotify_redirect_uri
                self.refresh_token = settings.spotify_refresh_token
        return SpotifyConfig(self)
    
    @property 
    def aws(self):
        """AWS configuration namespace."""
        class AWSConfig:
            def __init__(self, settings):
                self.access_key_id = settings.aws_access_key_id
                self.secret_access_key = settings.aws_secret_access_key
                self.region = settings.aws_region
                self.s3_bucket_name = settings.s3_bucket_name
        return AWSConfig(self)
        
    @property
    def pipeline(self):
        """Pipeline configuration namespace."""
        class PipelineConfig:
            def __init__(self, settings):
                self.fetch_interval_minutes = settings.fetch_interval_minutes
                self.batch_size = settings.batch_size
                self.max_retries = settings.max_retries
                self.max_runtime_minutes = settings.max_runtime_minutes
                self.snowflake_stage_prefix = settings.snowflake_stage_prefix
                self.date_partition_format = settings.date_partition_format
        return PipelineConfig(self)

    @property
    def snowflake(self):
        class SnowflakeConfig:
            def __init__(self, settings):
                self.account = settings.snowflake_account
                self.user = settings.snowflake_user
                self.password = settings.snowflake_password
                self.private_key_path = settings.snowflake_private_key_path
                self.private_key = settings.snowflake_private_key
                self.private_key_passphrase = settings.snowflake_private_key_passphrase
                self.warehouse = settings.snowflake_warehouse
                self.database = settings.snowflake_database
                self.schema = settings.snowflake_schema
                self.role = settings.snowflake_role
        return SnowflakeConfig(self)

    @property
    def rate_limit(self):
        """Rate limiting configuration namespace."""
        class RateLimitConfig:
            def __init__(self, settings):
                self.requests_per_minute = settings.rate_limit_requests_per_minute
                self.default_sleep_sec = settings.rate_limit_default_sleep_sec
                self.backoff_multiplier = settings.rate_limit_backoff_multiplier
                self.max_backoff_sec = settings.rate_limit_max_backoff_sec
                self.circuit_breaker_threshold = settings.rate_limit_circuit_breaker_threshold
                self.circuit_breaker_pause_sec = settings.rate_limit_circuit_breaker_pause_sec
                self.token_refresh_margin_sec = settings.token_refresh_margin_sec
        return RateLimitConfig(self)

    @property
    def lastfm(self):
        """Last.fm configuration namespace."""
        class LastfmConfig:
            def __init__(self, settings):
                self.api_key = settings.lastfm_api_key
                self.api_secret = settings.lastfm_api_secret
                self.rate_limit_per_sec = settings.lastfm_rate_limit_per_sec
        return LastfmConfig(self)

    @property
    def musicbrainz(self):
        """MusicBrainz configuration namespace."""
        class MusicBrainzConfig:
            def __init__(self, settings):
                self.user_agent = settings.musicbrainz_user_agent
                self.rate_limit_per_sec = settings.musicbrainz_rate_limit_per_sec
        return MusicBrainzConfig(self)


# Global settings instance
settings = Settings()
