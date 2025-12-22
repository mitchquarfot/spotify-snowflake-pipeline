-- Snowflake Setup Script for Spotify Listening History Pipeline
-- Replace placeholder values with your actual configuration

-- 1. Create Database and Schema
CREATE DATABASE IF NOT EXISTS spotify_analytics;
USE DATABASE spotify_analytics;
CREATE SCHEMA IF NOT EXISTS raw_data;
USE SCHEMA raw_data;

-- 2. Create the main listening history table
CREATE OR REPLACE TABLE spotify_listening_history (
    -- Listening metadata
    played_at TIMESTAMP_NTZ,
    played_at_timestamp NUMBER(38,0),
    played_at_date DATE,
    played_at_hour NUMBER(2,0),
    
    -- Track information
    track_id STRING(22),
    track_name STRING(500),
    track_duration_ms NUMBER(38,0),
    track_popularity NUMBER(3,0),
    track_explicit BOOLEAN,
    track_preview_url STRING(500),
    track_external_urls VARIANT,
    track_uri STRING(50),
    
    -- Artist information  
    artists VARIANT, -- Full artist array
    primary_artist_id STRING(22),
    primary_artist_name STRING(500),
    
    -- Album information
    album_id STRING(22),
    album_name STRING(500),
    album_type STRING(20),
    album_release_date STRING(10),
    album_total_tracks NUMBER(3,0),
    album_images VARIANT,
    
    -- Context (playlist, artist, album, etc.)
    context_type STRING(20),
    context_uri STRING(100),
    context_external_urls VARIANT,
    
    -- Pipeline metadata
    ingested_at TIMESTAMP_NTZ,
    data_source STRING(50) DEFAULT 'spotify_recently_played_api'
)
CLUSTER BY (played_at_date, primary_artist_name);

-- 3. Create the artist-genre table
CREATE OR REPLACE TABLE spotify_artist_genres (
    -- Artist identification
    artist_id STRING(22) PRIMARY KEY,
    artist_name STRING(500),
    artist_uri STRING(50),
    
    -- Genre information
    genres VARIANT, -- Full genres array as JSON
    genres_list ARRAY, -- Genres as native array for easier querying
    primary_genre STRING(100), -- Most relevant/first genre
    genre_count NUMBER(3,0), -- Number of genres for this artist
    
    -- Artist metrics
    popularity NUMBER(3,0), -- Artist popularity score (0-100)
    followers_total NUMBER(38,0), -- Total follower count
    
    -- Additional metadata
    external_urls VARIANT, -- External URLs (Spotify, etc.)
    images VARIANT, -- Artist image URLs and metadata
    
    -- Pipeline metadata
    ingested_at TIMESTAMP_NTZ,
    data_source STRING(50) DEFAULT 'spotify_artist_api'
)
CLUSTER BY (primary_genre, popularity DESC);

-- 4. Create S3 Stage for listening history
-- Replace YOUR_BUCKET_NAME, YOUR_ACCESS_KEY, YOUR_SECRET_KEY with actual values
CREATE OR REPLACE STAGE spotify_s3_stage
URL = 's3://YOUR_BUCKET_NAME/spotify_listening_history/'
CREDENTIALS = (
    AWS_KEY_ID = 'YOUR_ACCESS_KEY'
    AWS_SECRET_KEY = 'YOUR_SECRET_KEY'
)
FILE_FORMAT = (
    TYPE = JSON
    COMPRESSION = GZIP
    STRIP_OUTER_ARRAY = FALSE
    IGNORE_UTF8_ERRORS = TRUE
);

-- 5. Create S3 Stage for artist-genre data
CREATE OR REPLACE STAGE spotify_artist_s3_stage
URL = 's3://YOUR_BUCKET_NAME/spotify_artist_genres/'
CREDENTIALS = (
    AWS_KEY_ID = 'YOUR_ACCESS_KEY'
    AWS_SECRET_KEY = 'YOUR_SECRET_KEY'
)
FILE_FORMAT = (
    TYPE = JSON
    COMPRESSION = GZIP
    STRIP_OUTER_ARRAY = FALSE
    IGNORE_UTF8_ERRORS = TRUE
);

-- 6. Test the stages (optional)
-- LIST @spotify_s3_stage;
-- LIST @spotify_artist_s3_stage;

-- 7. Create Snowpipe for automatic listening history ingestion
CREATE OR REPLACE PIPE spotify_ingestion_pipe
AUTO_INGEST = TRUE
AS
COPY INTO spotify_listening_history
FROM @spotify_s3_stage
FILE_FORMAT = (
    TYPE = JSON 
    COMPRESSION = GZIP
    STRIP_OUTER_ARRAY = FALSE
    IGNORE_UTF8_ERRORS = TRUE
)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'SKIP_FILE';

-- 8. Create Snowpipe for automatic artist-genre ingestion
CREATE OR REPLACE PIPE spotify_artist_ingestion_pipe
AUTO_INGEST = TRUE
AS
COPY INTO spotify_artist_genres
FROM @spotify_artist_s3_stage
FILE_FORMAT = (
    TYPE = JSON 
    COMPRESSION = GZIP
    STRIP_OUTER_ARRAY = FALSE
    IGNORE_UTF8_ERRORS = TRUE
)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'SKIP_FILE';

-- 9. Show pipe details (copy the SQS queue URLs for S3 notification setup)
SHOW PIPES LIKE 'spotify_%_pipe';

-- 10. Create useful views for analytics

-- Daily listening summary view
CREATE OR REPLACE VIEW daily_listening_summary AS
SELECT 
    played_at_date,
    COUNT(*) AS total_tracks,
    COUNT(DISTINCT track_id) AS unique_tracks,
    COUNT(DISTINCT primary_artist_name) AS unique_artists,
    COUNT(DISTINCT album_id) AS unique_albums,
    SUM(track_duration_ms) / 1000 / 60 AS total_minutes_listened,
    AVG(track_duration_ms) / 1000 AS avg_track_length_seconds,
    MODE(primary_artist_name) AS most_played_artist
FROM spotify_listening_history
GROUP BY played_at_date
ORDER BY played_at_date DESC;

-- Artist statistics view with genre information
CREATE OR REPLACE VIEW artist_stats_with_genres AS
SELECT 
    h.primary_artist_name,
    h.primary_artist_id,
    ag.primary_genre,
    ag.genres_list,
    ag.genre_count,
    ag.popularity AS artist_popularity,
    ag.followers_total,
    COUNT(*) AS total_plays,
    COUNT(DISTINCT h.track_id) AS unique_tracks,
    COUNT(DISTINCT h.album_id) AS unique_albums,
    SUM(h.track_duration_ms) / 1000 / 60 AS total_minutes,
    AVG(h.track_popularity) AS avg_track_popularity,
    MIN(h.played_at) AS first_played,
    MAX(h.played_at) AS last_played
FROM spotify_listening_history h
LEFT JOIN spotify_artist_genres ag ON h.primary_artist_id = ag.artist_id
WHERE h.primary_artist_name IS NOT NULL
GROUP BY h.primary_artist_name, h.primary_artist_id, ag.primary_genre, 
         ag.genres_list, ag.genre_count, ag.popularity, ag.followers_total
ORDER BY total_plays DESC;

-- Genre analysis view
CREATE OR REPLACE VIEW genre_listening_analysis AS
SELECT 
    ag.primary_genre,
    COUNT(DISTINCT h.primary_artist_id) AS unique_artists,
    COUNT(*) AS total_plays,
    COUNT(DISTINCT h.track_id) AS unique_tracks,
    SUM(h.track_duration_ms) / 1000 / 60 AS total_minutes,
    AVG(h.track_popularity) AS avg_track_popularity,
    AVG(ag.popularity) AS avg_artist_popularity,
    AVG(ag.followers_total) AS avg_artist_followers
FROM spotify_listening_history h
JOIN spotify_artist_genres ag ON h.primary_artist_id = ag.artist_id
WHERE ag.primary_genre IS NOT NULL
GROUP BY ag.primary_genre
ORDER BY total_plays DESC;

-- Multi-genre artist analysis (artists with multiple genres)
CREATE OR REPLACE VIEW multi_genre_artists AS
SELECT 
    artist_id,
    artist_name,
    genres_list,
    genre_count,
    popularity,
    followers_total
FROM spotify_artist_genres
WHERE genre_count > 1
ORDER BY genre_count DESC, popularity DESC;

-- Hourly listening patterns view
CREATE OR REPLACE VIEW hourly_patterns AS
SELECT 
    played_at_hour,
    COUNT(*) AS total_tracks,
    AVG(track_duration_ms) / 1000 / 60 AS avg_minutes_per_hour,
    COUNT(DISTINCT played_at_date) AS days_active,
    ROUND(COUNT(*) / COUNT(DISTINCT played_at_date), 2) AS avg_tracks_per_day
FROM spotify_listening_history
GROUP BY played_at_hour
ORDER BY played_at_hour;

-- Context analysis view (how you listen: playlists vs albums vs artists)
CREATE OR REPLACE VIEW listening_context AS
SELECT 
    context_type,
    COUNT(*) AS total_plays,
    COUNT(DISTINCT track_id) AS unique_tracks,
    SUM(track_duration_ms) / 1000 / 60 AS total_minutes,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_listening
FROM spotify_listening_history
WHERE context_type IS NOT NULL
GROUP BY context_type
ORDER BY total_plays DESC;

-- 11. Create monitoring table for pipeline health
CREATE OR REPLACE TABLE pipeline_monitoring (
    check_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    metric_name STRING(100),
    metric_value NUMBER(38,2),
    metric_description STRING(500)
);

-- 12. Create a procedure to update monitoring metrics
CREATE OR REPLACE PROCEDURE update_pipeline_metrics()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Clear old metrics
    DELETE FROM pipeline_monitoring WHERE check_timestamp < DATEADD(day, -7, CURRENT_TIMESTAMP());
    
    -- Insert current metrics for listening history
    INSERT INTO pipeline_monitoring (metric_name, metric_value, metric_description)
    SELECT 
        'total_tracks_ingested',
        COUNT(*),
        'Total number of tracks in the database'
    FROM spotify_listening_history;
    
    INSERT INTO pipeline_monitoring (metric_name, metric_value, metric_description)
    SELECT 
        'data_freshness_hours',
        DATEDIFF('hour', MAX(played_at), CURRENT_TIMESTAMP()),
        'Hours since most recent track was played'
    FROM spotify_listening_history;
    
    INSERT INTO pipeline_monitoring (metric_name, metric_value, metric_description)
    SELECT 
        'tracks_last_24h',
        COUNT(*),
        'Number of tracks ingested in the last 24 hours'
    FROM spotify_listening_history 
    WHERE ingested_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP());
    
    -- Insert current metrics for artist-genre data
    INSERT INTO pipeline_monitoring (metric_name, metric_value, metric_description)
    SELECT 
        'total_artists_processed',
        COUNT(*),
        'Total number of unique artists with genre data'
    FROM spotify_artist_genres;
    
    INSERT INTO pipeline_monitoring (metric_name, metric_value, metric_description)
    SELECT 
        'artists_with_genres',
        COUNT(*),
        'Number of artists that have at least one genre'
    FROM spotify_artist_genres
    WHERE genre_count > 0;
    
    INSERT INTO pipeline_monitoring (metric_name, metric_value, metric_description)
    SELECT 
        'avg_genres_per_artist',
        AVG(genre_count),
        'Average number of genres per artist'
    FROM spotify_artist_genres
    WHERE genre_count > 0;
    
    RETURN 'Metrics updated successfully';
END;
$$;

-- 13. Sample queries to verify everything works

-- Test listening history data
-- SELECT * FROM spotify_listening_history LIMIT 10;

-- Test artist-genre data
-- SELECT * FROM spotify_artist_genres LIMIT 10;

-- Test artist stats with genres
-- SELECT * FROM artist_stats_with_genres LIMIT 10;

-- Test genre analysis
-- SELECT * FROM genre_listening_analysis LIMIT 10;

-- Update and check monitoring metrics
-- CALL update_pipeline_metrics();
-- SELECT * FROM pipeline_monitoring ORDER BY check_timestamp DESC; 