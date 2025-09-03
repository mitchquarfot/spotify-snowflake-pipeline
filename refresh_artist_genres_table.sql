-- Refresh Artist Genres Table to Ingest Enhanced S3 Data
-- Run this script in Snowflake to pull in the processed artist genre data

USE ROLE SPOTIFY_ANALYST_ROLE;
USE WAREHOUSE SPOTIFY_WH;
USE DATABASE spotify_analytics;
USE SCHEMA raw_data;

-- =============================================================================
-- CHECK CURRENT STATE
-- =============================================================================

SELECT 'CURRENT ARTIST GENRES TABLE STATUS' as report_section;

-- Check current counts and empty genres
SELECT 
    COUNT(*) as total_artists,
    COUNT(CASE WHEN genres IS NULL THEN 1 END) as null_genres,
    COUNT(CASE WHEN genres = '[]' THEN 1 END) as empty_array_genres,
    COUNT(CASE WHEN genre_count = 0 THEN 1 END) as zero_genre_count,
    COUNT(CASE WHEN genre_count > 0 THEN 1 END) as artists_with_genres,
    MAX(ingested_at) as latest_ingestion
FROM spotify_artist_genres;

-- Sample of artists with empty genres
SELECT 'SAMPLE EMPTY GENRE ARTISTS (BEFORE)' as report_section;

SELECT 
    artist_id,
    artist_name,
    genres,
    genre_count,
    popularity,
    followers_total,
    data_source,
    ingested_at
FROM spotify_artist_genres
WHERE genre_count = 0 OR genres = '[]'
ORDER BY popularity DESC NULLS LAST
LIMIT 10;

-- =============================================================================
-- CHECK SNOWPIPE STATUS
-- =============================================================================

SELECT 'SNOWPIPE STATUS CHECK' as report_section;

-- Check if Snowpipe is running and when it last processed files
SHOW PIPES LIKE 'SPOTIFY_ARTIST_INGESTION_PIPE';

-- Check recent copy history for artist data
SELECT 
    file_name,
    stage_location,
    first_error_message,
    first_error_line_number,
    first_error_character_position,
    rows_parsed,
    rows_loaded,
    error_count,
    error_limit,
    load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    table_name => 'SPOTIFY_ARTIST_GENRES',
    start_time => DATEADD('hours', -24, CURRENT_TIMESTAMP())
))
ORDER BY load_time DESC
LIMIT 10;

-- =============================================================================
-- MANUAL REFRESH OPTIONS
-- =============================================================================

SELECT 'MANUAL REFRESH PROCEDURES' as report_section;

-- Option 1: Refresh Snowpipe (if it's stuck)
-- ALTER PIPE spotify_artist_ingestion_pipe REFRESH;

-- Option 2: Manual copy from stage (if Snowpipe isn't working)
/*
COPY INTO spotify_artist_genres
FROM @spotify_artist_s3_stage
FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'GZIP')
PATTERN = '.*spotify_artist.*\.json\.gz'
ON_ERROR = 'CONTINUE';
*/

-- Option 3: Check what files are in the stage
LIST @spotify_artist_s3_stage;

-- =============================================================================
-- FORCE SNOWPIPE REFRESH
-- =============================================================================

SELECT 'FORCING SNOWPIPE REFRESH' as report_section;

-- Force refresh the Snowpipe to pick up any missed files
ALTER PIPE spotify_artist_ingestion_pipe REFRESH;

-- Wait a moment for processing, then check again
SELECT 'Snowpipe refresh initiated. Check status in a few minutes.' as status_message;

-- =============================================================================
-- VERIFICATION AFTER REFRESH
-- =============================================================================

-- Wait 2-3 minutes, then run this section to verify the refresh worked

/*
SELECT 'VERIFICATION: Updated counts after refresh' as report_section;

SELECT 
    COUNT(*) as total_artists,
    COUNT(CASE WHEN genre_count = 0 THEN 1 END) as empty_genre_count,
    COUNT(CASE WHEN genre_count > 0 THEN 1 END) as artists_with_genres,
    ROUND(100.0 * COUNT(CASE WHEN genre_count > 0 THEN 1 END) / COUNT(*), 2) as genre_coverage_percentage,
    MAX(ingested_at) as latest_ingestion
FROM spotify_artist_genres;

-- Sample of recently enhanced artists
SELECT 'SAMPLE ENHANCED ARTISTS (AFTER)' as report_section;

SELECT 
    artist_id,
    artist_name,
    genres,
    genre_count,
    primary_genre,
    popularity,
    data_source,
    ingested_at
FROM spotify_artist_genres
WHERE data_source LIKE '%enhanced%'
   OR data_source LIKE '%popularity_analysis%'
   OR data_source LIKE '%name_pattern%'
ORDER BY ingested_at DESC
LIMIT 10;
*/

SELECT '✅ Artist genres table refresh initiated!' as completion_message;
