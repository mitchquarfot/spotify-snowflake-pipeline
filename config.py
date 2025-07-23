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
    
    # AWS configuration
    aws_access_key_id: str = Field(..., env="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = Field(..., env="AWS_SECRET_ACCESS_KEY")
    aws_region: str = Field("us-west-2", env="AWS_REGION")
    s3_bucket_name: str = Field(..., env="S3_BUCKET_NAME")
    
    # Pipeline configuration
    fetch_interval_minutes: int = Field(30, env="FETCH_INTERVAL_MINUTES")
    batch_size: int = Field(50, env="BATCH_SIZE")
    max_retries: int = Field(3, env="MAX_RETRIES")
    snowflake_stage_prefix: str = Field("spotify_listening_history/", env="SNOWFLAKE_STAGE_PREFIX")
    date_partition_format: str = Field("%Y/%m/%d", env="DATE_PARTITION_FORMAT")

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
                self.snowflake_stage_prefix = settings.snowflake_stage_prefix
                self.date_partition_format = settings.date_partition_format
        return PipelineConfig(self)


# Global settings instance
settings = Settings()
