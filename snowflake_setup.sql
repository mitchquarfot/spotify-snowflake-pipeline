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

-- 3. Create S3 Stage
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

-- 4. Test the stage (optional)
-- LIST @spotify_s3_stage;

-- 5. Create Snowpipe for automatic ingestion
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

-- 6. Show pipe details (copy the SQS queue URL for S3 notification setup)
SHOW PIPES LIKE 'spotify_ingestion_pipe';

-- 7. Create useful views for analytics

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

-- Artist statistics view
CREATE OR REPLACE VIEW artist_stats AS
SELECT 
    primary_artist_name,
    COUNT(*) AS total_plays,
    COUNT(DISTINCT track_id) AS unique_tracks,
    COUNT(DISTINCT album_id) AS unique_albums,
    SUM(track_duration_ms) / 1000 / 60 AS total_minutes,
    AVG(track_popularity) AS avg_track_popularity,
    MIN(played_at) AS first_played,
    MAX(played_at) AS last_played
FROM spotify_listening_history
WHERE primary_artist_name IS NOT NULL
GROUP BY primary_artist_name
ORDER BY total_plays DESC;

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

-- 8. Create monitoring table for pipeline health
CREATE OR REPLACE TABLE pipeline_monitoring (
    check_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    metric_name STRING(100),
    metric_value NUMBER(38,2),
    metric_description STRING(500)
);

-- 9. Create a procedure to update monitoring metrics
CREATE OR REPLACE PROCEDURE update_pipeline_metrics()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Clear old metrics
    DELETE FROM pipeline_monitoring WHERE check_timestamp < DATEADD(day, -7, CURRENT_TIMESTAMP());
    
    -- Insert current metrics
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
    
    RETURN 'Metrics updated successfully';
END;
$$;

-- 10. Sample queries to verify everything works

-- Test query: Recent listening activity
SELECT 
    played_at,
    track_name,
    primary_artist_name,
    album_name,
    context_type
FROM spotify_listening_history 
ORDER BY played_at DESC 
LIMIT 10;

-- Test query: Daily summary for last week
SELECT * FROM daily_listening_summary 
WHERE played_at_date >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY played_at_date DESC;

-- 11. Set up task to run monitoring procedure daily
CREATE OR REPLACE TASK update_metrics_task
WAREHOUSE = 'COMPUTE_WH'  -- Replace with your warehouse name
SCHEDULE = 'USING CRON 0 6 * * * UTC'  -- Daily at 6 AM UTC
AS
CALL update_pipeline_metrics();

-- Start the task (uncomment when ready)
-- ALTER TASK update_metrics_task RESUME;

-- 12. Grant permissions (adjust as needed for your security model)
-- GRANT USAGE ON DATABASE spotify_analytics TO ROLE analyst_role;
-- GRANT USAGE ON SCHEMA spotify_analytics.raw_data TO ROLE analyst_role;
-- GRANT SELECT ON ALL TABLES IN SCHEMA spotify_analytics.raw_data TO ROLE analyst_role;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA spotify_analytics.raw_data TO ROLE analyst_role;

-- Setup complete!
-- Next steps:
-- 1. Update the stage credentials with your actual AWS details
-- 2. Copy the Snowpipe SQS queue URL from SHOW PIPES output
-- 3. Configure S3 bucket notifications to send to that SQS queue
-- 4. Run your Python pipeline to start ingesting data 