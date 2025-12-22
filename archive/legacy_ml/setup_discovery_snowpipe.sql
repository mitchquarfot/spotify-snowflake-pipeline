-- Setup Snowpipe for Auto-Ingesting Spotify Discoveries from S3
-- This creates the infrastructure to automatically ingest discovered tracks
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- 1. Create raw discovery table (similar to your listening history structure)
CREATE OR REPLACE TABLE raw_spotify_discoveries (
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
    seed_artist VARCHAR(500),
    seed_genre VARCHAR(100),
    preference_score FLOAT,
    discovered_at TIMESTAMP_LTZ,
    batch_timestamp VARCHAR(50),
    -- Ingestion metadata (similar to listening history)
    ingested_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    file_name VARCHAR(500),
    s3_key VARCHAR(1000)
);

-- 2. Create file format for JSON discovery files
CREATE OR REPLACE FILE FORMAT discovery_json_format
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = FALSE
    STRIP_OUTER_ARRAY = FALSE
    STRIP_NULL_VALUES = FALSE
    IGNORE_UTF8_ERRORS = FALSE;

-- 3. Create external stage for discovery files
-- ✅ CONFIGURED FOR YOUR SETUP (bucket: mquarfot-dev)
-- ⚠️  UPDATE YOUR_ACCESS_KEY and YOUR_SECRET_KEY with your actual AWS credentials
CREATE OR REPLACE STAGE discovery_s3_stage
    URL = 's3://mquarfot-dev/spotify_discoveries/'
    CREDENTIALS = (
        AWS_KEY_ID = '****************'
        AWS_SECRET_KEY = '***********************************'
    )
    FILE_FORMAT = discovery_json_format;

-- 4. Create the Snowpipe for automatic ingestion
CREATE OR REPLACE PIPE discovery_snowpipe
    AUTO_INGEST = TRUE
    AS 
    COPY INTO raw_spotify_discoveries (
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
        seed_artist,
        seed_genre,
        preference_score,
        discovered_at,
        batch_timestamp,
        file_name,
        s3_key
    )
    FROM (
        SELECT 
            $1:track_id::VARCHAR(100),
            $1:track_name::VARCHAR(500),
            $1:primary_artist_name::VARCHAR(500),
            $1:primary_artist_id::VARCHAR(100),
            $1:album_name::VARCHAR(500),
            TRY_TO_DATE($1:album_release_date::VARCHAR),
            $1:track_popularity::INTEGER,
            $1:track_duration_ms::INTEGER,
            $1:preview_url::VARCHAR(1000),
            $1:discovery_strategy::VARCHAR(100),
            $1:seed_artist::VARCHAR(500),
            $1:seed_genre::VARCHAR(100),
            $1:preference_score::FLOAT,
            TRY_TO_TIMESTAMP($1:discovered_at::VARCHAR),
            $1:batch_timestamp::VARCHAR(50),
            METADATA$FILENAME,
            METADATA$FILE_ROW_NUMBER
        FROM @discovery_s3_stage
    );

-- 5. Show the Snowpipe's SQS queue ARN for S3 event notification setup
SELECT SYSTEM$PIPE_STATUS('discovery_snowpipe');

-- 6. Create processed discoveries table (cleaned and deduplicated)
CREATE OR REPLACE TABLE ml_spotify_discoveries (
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
    seed_artist VARCHAR(500),
    seed_genre VARCHAR(100),
    preference_score FLOAT,
    discovered_at TIMESTAMP_LTZ,
    recommendation_score FLOAT DEFAULT 0,
    is_recommended BOOLEAN DEFAULT FALSE,
    user_feedback VARCHAR(100) DEFAULT NULL,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 7. Create view to automatically process raw discoveries into clean table
CREATE OR REPLACE VIEW processed_discoveries_stream AS
SELECT DISTINCT
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
    seed_artist,
    seed_genre,
    preference_score,
    discovered_at,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
FROM raw_spotify_discoveries
WHERE track_id IS NOT NULL
AND track_name IS NOT NULL
AND primary_artist_name IS NOT NULL
AND ingested_at >= DATEADD('days', -7, CURRENT_TIMESTAMP()); -- Only recent discoveries

-- 8. Create task to periodically merge new discoveries (runs every 15 minutes)
-- ⚠️  UPDATE YOUR_WAREHOUSE_NAME to match your existing warehouse
CREATE OR REPLACE TASK process_new_discoveries
    WAREHOUSE = 'SPOTIFY_WH'  -- Update with your warehouse name (same as existing setup)
    SCHEDULE = 'USING CRON 0,15,30,45 * * * * UTC'  -- Every 15 minutes
    AS
    MERGE INTO ml_spotify_discoveries AS target
    USING processed_discoveries_stream AS source
    ON target.track_id = source.track_id
    WHEN NOT MATCHED THEN 
        INSERT (
            track_id, track_name, primary_artist_name, primary_artist_id,
            album_name, album_release_date, track_popularity, track_duration_ms,
            preview_url, discovery_strategy, seed_artist, seed_genre,
            preference_score, discovered_at, created_at, updated_at
        )
        VALUES (
            source.track_id, source.track_name, source.primary_artist_name, source.primary_artist_id,
            source.album_name, source.album_release_date, source.track_popularity, source.track_duration_ms,
            source.preview_url, source.discovery_strategy, source.seed_artist, source.seed_genre,
            source.preference_score, source.discovered_at, source.created_at, source.updated_at
        )
    WHEN MATCHED THEN
        UPDATE SET
            recommendation_score = COALESCE(target.recommendation_score, 0),
            updated_at = CURRENT_TIMESTAMP();

-- 9. Start the processing task
ALTER TASK process_new_discoveries RESUME;

-- 10. Test queries to verify setup
SELECT '=== SNOWPIPE SETUP VERIFICATION ===' as section;

-- Check if tables exist
SELECT 'Tables Created' as check_type, 
       CASE WHEN COUNT(*) = 2 THEN '✅ SUCCESS' ELSE '❌ MISSING' END as status
FROM SPOTIFY_ANALYTICS.INFORMATION_SCHEMA.TABLES 
WHERE table_name IN ('RAW_SPOTIFY_DISCOVERIES', 'ML_SPOTIFY_DISCOVERIES')
AND table_schema = 'ANALYTICS';

-- Check if stage exists
SELECT 'Stage Created' as check_type,
       CASE WHEN COUNT(*) >= 1 THEN '✅ SUCCESS' ELSE '❌ MISSING' END as status
FROM SPOTIFY_ANALYTICS.INFORMATION_SCHEMA.STAGES 
WHERE stage_name = 'DISCOVERY_S3_STAGE';

-- Check if pipe exists  
SELECT 'Snowpipe Created' as check_type,
       CASE WHEN COUNT(*) >= 1 THEN '✅ SUCCESS' ELSE '❌ MISSING' END as status
FROM SPOTIFY_ANALYTICS.INFORMATION_SCHEMA.PIPES 
WHERE pipe_name = 'DISCOVERY_SNOWPIPE';

-- Check if task exists
SELECT 'Processing Task' as check_type,
       CASE WHEN COUNT(*) >= 1 THEN '✅ SUCCESS' ELSE '❌ MISSING' END as status
FROM SNOWFLAKE.INFORMATION_SCHEMA.TASKS 
WHERE task_name = 'PROCESS_NEW_DISCOVERIES';

-- Show next steps
SELECT 
'NEXT STEPS:

1. 🔧 UPDATE STAGE CONFIGURATION:
   - Replace "your-bucket-name" with your actual S3 bucket
   - Replace AWS credentials with your actual keys
   - Or use IAM role authentication (recommended)

2. 📡 SETUP S3 EVENT NOTIFICATIONS:
   - Get SQS queue ARN: SELECT SYSTEM$PIPE_STATUS(''discovery_snowpipe'');
   - Configure S3 bucket to send events to this SQS queue
   - Set path filter: spotify_discoveries/

3. 🧪 TEST THE PIPELINE:
   - Run: python spotify_discovery_system.py
   - Check: SELECT * FROM raw_spotify_discoveries;
   - Verify: SELECT * FROM ml_spotify_discoveries;

4. 🎵 USE RECOMMENDATIONS:
   - Run discovery_recommendation_views.sql
   - Query: SELECT * FROM ml_top_discovery_recommendations;

✅ Your discovery pipeline is ready!
' as instructions;

desc pipe discovery_snowpipe;