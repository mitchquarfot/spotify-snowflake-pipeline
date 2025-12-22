-- Setup Snowpipe for Auto-Ingesting ML-Powered Spotify Discoveries from S3
-- This creates dedicated infrastructure for ML recommendation pipeline
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- 1. Create raw ML discovery table (with additional ML fields)
CREATE OR REPLACE TABLE raw_spotify_ml_discoveries (
    track_id VARCHAR(100),
    track_name VARCHAR(500),
    primary_artist_name VARCHAR(500),
    primary_artist_id VARCHAR(100),
    album_name VARCHAR(500),
    album_release_date DATE,
    track_popularity INTEGER,
    track_duration_ms INTEGER,
    preview_url VARCHAR(1000),
    discovery_strategy VARCHAR(100),
    -- ML-specific fields
    ml_recommendation_score FLOAT,
    ml_strategies_used VARCHAR(1000),
    ml_weighted_score FLOAT,
    seed_track VARCHAR(1000),
    seed_genre VARCHAR(100),
    genre_novelty_score FLOAT,
    artist_novelty_score FLOAT,
    discovered_at TIMESTAMP_LTZ,
    -- Ingestion metadata
    ingested_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    file_name VARCHAR(500),
    s3_key VARCHAR(1000)
);

-- 2. Create file format for ML JSON discovery files  
CREATE OR REPLACE FILE FORMAT ml_discovery_json_format
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = FALSE
    STRIP_OUTER_ARRAY = FALSE
    STRIP_NULL_VALUES = FALSE
    IGNORE_UTF8_ERRORS = FALSE;

-- 3. Create external stage for ML discovery files
-- ✅ CONFIGURED FOR YOUR SETUP (bucket: mquarfot-dev)
CREATE OR REPLACE STAGE ml_discovery_s3_stage
    URL = 's3://mquarfot-dev/spotify_ml_discoveries/'
    CREDENTIALS = (
        AWS_KEY_ID = '*************************'
        AWS_SECRET_KEY = '***************************************'
    )
    FILE_FORMAT = ml_discovery_json_format;

-- 4. Create the ML Snowpipe for automatic ingestion
CREATE OR REPLACE PIPE ml_discovery_snowpipe
    AUTO_INGEST = TRUE
    AS 
    COPY INTO raw_spotify_ml_discoveries (
        track_id,
        track_name,
        primary_artist_name,
        primary_artist_id,
        album_name,
        album_release_date,
        track_popularity,
        track_duration_ms,
        preview_url,
        discovery_strategy,
        ml_recommendation_score,
        ml_strategies_used,
        ml_weighted_score,
        seed_track,
        seed_genre,
        genre_novelty_score,
        artist_novelty_score,
        discovered_at,
        file_name,
        s3_key
    )
    FROM (
        SELECT 
            $1:track_id::VARCHAR(100) AS track_id,
            $1:track_name::VARCHAR(500) AS track_name,
            $1:primary_artist_name::VARCHAR(500) AS primary_artist_name,
            $1:primary_artist_id::VARCHAR(100) AS primary_artist_id,
            $1:album_name::VARCHAR(500) AS album_name,
            TRY_TO_DATE($1:album_release_date::VARCHAR) AS album_release_date,
            $1:track_popularity::INTEGER AS track_popularity,
            $1:track_duration_ms::INTEGER AS track_duration_ms,
            $1:preview_url::VARCHAR(1000) AS preview_url,
            $1:discovery_strategy::VARCHAR(100) AS discovery_strategy,
            $1:ml_recommendation_score::FLOAT AS ml_recommendation_score,
            $1:ml_strategies_used::VARCHAR(1000) AS ml_strategies_used,
            $1:ml_weighted_score::FLOAT AS ml_weighted_score,
            $1:seed_track::VARCHAR(1000) AS seed_track,
            $1:seed_genre::VARCHAR(100) AS seed_genre,
            $1:genre_novelty_score::FLOAT AS genre_novelty_score,
            $1:artist_novelty_score::FLOAT AS artist_novelty_score,
            TRY_TO_TIMESTAMP($1:discovered_at::VARCHAR) AS discovered_at,
            METADATA$FILENAME AS file_name,
            METADATA$FILE_ROW_NUMBER AS s3_key
        FROM @ml_discovery_s3_stage
    );

-- 5. Create stream to track new ML discoveries
CREATE OR REPLACE STREAM ml_processed_discoveries_stream ON TABLE raw_spotify_ml_discoveries;

-- 6. Create processed ML discoveries table (cleaned and deduplicated)
CREATE OR REPLACE TABLE ml_spotify_ml_discoveries (
    track_id VARCHAR(100) PRIMARY KEY,
    track_name VARCHAR(500),
    primary_artist_name VARCHAR(500),
    primary_artist_id VARCHAR(100),
    album_name VARCHAR(500),
    album_release_date DATE,
    track_popularity INTEGER,
    track_duration_ms INTEGER,
    preview_url VARCHAR(1000),
    discovery_strategy VARCHAR(100),
    ml_recommendation_score FLOAT,
    ml_strategies_used VARCHAR(1000),
    ml_weighted_score FLOAT,
    seed_track VARCHAR(1000),
    seed_genre VARCHAR(100),
    genre_novelty_score FLOAT,
    artist_novelty_score FLOAT,
    first_discovered_at TIMESTAMP_LTZ,
    discovery_count INTEGER DEFAULT 1,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 7. Create task to process ML discoveries from stream
CREATE OR REPLACE TASK process_new_ml_discoveries
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0,15,30,45 * * * * UTC'  -- Every 15 minutes
    AS
    MERGE INTO ml_spotify_ml_discoveries AS target
    USING ml_processed_discoveries_stream AS source
    ON target.track_id = source.track_id
    WHEN MATCHED THEN 
        UPDATE SET 
            discovery_count = target.discovery_count + 1,
            updated_at = CURRENT_TIMESTAMP(),
            -- Update ML scores if newer discovery has better score
            ml_recommendation_score = CASE 
                WHEN source.ml_recommendation_score > target.ml_recommendation_score 
                THEN source.ml_recommendation_score 
                ELSE target.ml_recommendation_score 
            END,
            ml_weighted_score = CASE 
                WHEN source.ml_weighted_score > target.ml_weighted_score 
                THEN source.ml_weighted_score 
                ELSE target.ml_weighted_score 
            END
    WHEN NOT MATCHED THEN 
        INSERT (
            track_id, track_name, primary_artist_name, primary_artist_id,
            album_name, album_release_date, track_popularity, track_duration_ms,
            preview_url, discovery_strategy, ml_recommendation_score, 
            ml_strategies_used, ml_weighted_score, seed_track, seed_genre,
            genre_novelty_score, artist_novelty_score,
            first_discovered_at, created_at, updated_at
        )
        VALUES (
            source.track_id, source.track_name, source.primary_artist_name, 
            source.primary_artist_id, source.album_name, source.album_release_date,
            source.track_popularity, source.track_duration_ms, source.preview_url,
            source.discovery_strategy, source.ml_recommendation_score,
            source.ml_strategies_used, source.ml_weighted_score, source.seed_track,
            source.seed_genre, source.genre_novelty_score, source.artist_novelty_score,
            source.discovered_at, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        );

-- Resume the task
ALTER TASK process_new_ml_discoveries RESUME;

-- 8. Create comparison view to analyze both pipelines
CREATE OR REPLACE VIEW discovery_pipeline_comparison AS
SELECT 
    'Smart Search' AS pipeline_type,
    COUNT(*) AS total_discoveries,
    AVG(track_popularity) AS avg_popularity,
    COUNT(DISTINCT primary_artist_name) AS unique_artists,
    COUNT(DISTINCT seed_genre) AS unique_genres,  -- Updated to use seed_genre
    MAX(created_at) AS latest_discovery
FROM ml_spotify_discoveries
UNION ALL
SELECT 
    'ML Hybrid' AS pipeline_type,
    COUNT(*) AS total_discoveries,
    AVG(track_popularity) AS avg_popularity,
    COUNT(DISTINCT primary_artist_name) AS unique_artists,
    COUNT(DISTINCT seed_genre) AS unique_genres,
    MAX(created_at) AS latest_discovery  
FROM ml_spotify_ml_discoveries;

-- 9. Show setup completion status
SELECT 
    '🚀 ML DISCOVERY INFRASTRUCTURE DEPLOYED!' AS status,
    'Smart Search Path: s3://mquarfot-dev/spotify_discoveries/' AS path_a,
    'ML Hybrid Path: s3://mquarfot-dev/spotify_ml_discoveries/' AS path_b,
    'Ready for A/B testing!' AS next_step;

-- 10. Verify pipes status
SELECT 
    'Discovery Pipes Status' AS check_type,
    pipe_name,
    is_autoingest_enabled,
    notification_channel_name
FROM SNOWFLAKE.INFORMATION_SCHEMA.PIPES 
WHERE pipe_name IN ('DISCOVERY_SNOWPIPE', 'ML_DISCOVERY_SNOWPIPE')
ORDER BY pipe_name;

-- 11. Test both stages
LIST @discovery_s3_stage;
LIST @ml_discovery_s3_stage;
