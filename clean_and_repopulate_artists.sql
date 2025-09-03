-- Clean and Repopulate Artist Genres Strategy
-- Remove artists with empty/null genres, then repopulate with enhanced data

USE ROLE SPOTIFY_ANALYST_ROLE;
USE WAREHOUSE SPOTIFY_WH;
USE DATABASE spotify_analytics;
USE SCHEMA raw_data;

-- =============================================================================
-- STEP 1: ANALYZE CURRENT STATE
-- =============================================================================

SELECT 'CURRENT STATE ANALYSIS' as report_section;

-- Get detailed breakdown of genre status
SELECT 
    COUNT(*) as total_artists,
    COUNT(CASE WHEN genres IS NULL THEN 1 END) as null_genres,
    COUNT(CASE WHEN genres = '[]' THEN 1 END) as empty_array_genres,
    COUNT(CASE WHEN genre_count = 0 THEN 1 END) as zero_genre_count,
    COUNT(CASE WHEN genre_count > 0 THEN 1 END) as artists_with_genres,
    ROUND(100.0 * COUNT(CASE WHEN genre_count > 0 THEN 1 END) / COUNT(*), 2) as current_coverage_percentage
FROM spotify_artist_genres;

-- Show sample of artists to be removed
SELECT 'ARTISTS TO BE REMOVED (SAMPLE)' as report_section;

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
WHERE genres IS NULL 
   OR genres = '[]' 
   OR genre_count = 0
ORDER BY popularity DESC NULLS LAST
LIMIT 20;

-- Count artists to be removed
SELECT 
    COUNT(*) as artists_to_remove,
    COUNT(DISTINCT artist_id) as unique_artist_ids_to_remove
FROM spotify_artist_genres
WHERE genres IS NULL 
   OR genres = '[]' 
   OR genre_count = 0;

-- =============================================================================
-- STEP 2: BACKUP STRATEGY (OPTIONAL)
-- =============================================================================

SELECT 'CREATING BACKUP TABLE' as report_section;

-- Create backup of current table
CREATE OR REPLACE TABLE spotify_artist_genres_backup AS
SELECT * FROM spotify_artist_genres;

SELECT 'Backup created: spotify_artist_genres_backup' as backup_status;

-- =============================================================================
-- STEP 3: REMOVE ARTISTS WITH EMPTY/NULL GENRES
-- =============================================================================

SELECT 'REMOVING ARTISTS WITH EMPTY/NULL GENRES' as report_section;

-- Delete artists with empty or null genres
DELETE FROM spotify_artist_genres
WHERE genres IS NULL 
   OR genres = '[]' 
   OR genre_count = 0;

-- Show results of deletion
SELECT 
    COUNT(*) as remaining_artists,
    COUNT(CASE WHEN genre_count > 0 THEN 1 END) as artists_with_genres,
    ROUND(100.0 * COUNT(CASE WHEN genre_count > 0 THEN 1 END) / COUNT(*), 2) as coverage_after_cleanup
FROM spotify_artist_genres;

-- =============================================================================
-- STEP 4: FORCE SNOWPIPE REFRESH TO INGEST NEW DATA
-- =============================================================================

SELECT 'FORCING SNOWPIPE REFRESH' as report_section;

-- Refresh the Snowpipe to pick up enhanced data from S3
ALTER PIPE spotify_artist_ingestion_pipe REFRESH;

-- Check what files are available in the stage
SELECT 'FILES IN S3 STAGE' as report_section;
LIST @spotify_artist_s3_stage;

-- Manual copy if needed (uncomment if Snowpipe is slow)
/*
COPY INTO spotify_artist_genres
FROM @spotify_artist_s3_stage
FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'GZIP')
PATTERN = '.*spotify_artist.*\.json\.gz'
ON_ERROR = 'CONTINUE';
*/

SELECT 'Snowpipe refresh initiated. Wait 2-3 minutes for processing.' as refresh_status;

-- =============================================================================
-- STEP 5: VERIFICATION QUERIES (RUN AFTER WAITING)
-- =============================================================================

-- Wait 2-3 minutes, then run this section

/*
SELECT 'VERIFICATION: Results after repopulation' as report_section;

-- Check final counts
SELECT 
    COUNT(*) as total_artists,
    COUNT(CASE WHEN genre_count = 0 THEN 1 END) as empty_genre_count,
    COUNT(CASE WHEN genre_count > 0 THEN 1 END) as artists_with_genres,
    ROUND(100.0 * COUNT(CASE WHEN genre_count > 0 THEN 1 END) / COUNT(*), 2) as final_coverage_percentage,
    MAX(ingested_at) as latest_ingestion
FROM spotify_artist_genres;

-- Show sample of enhanced artists
SELECT 'SAMPLE ENHANCED ARTISTS' as report_section;

SELECT 
    artist_id,
    artist_name,
    genres,
    genre_count,
    primary_genre,
    popularity,
    followers_total,
    data_source,
    ingested_at
FROM spotify_artist_genres
WHERE data_source LIKE '%enhanced%'
   OR data_source LIKE '%popularity_analysis%'
   OR data_source LIKE '%name_pattern%'
ORDER BY ingested_at DESC
LIMIT 15;

-- Check for any remaining empty genres
SELECT 'REMAINING EMPTY GENRE ARTISTS' as report_section;

SELECT 
    COUNT(*) as remaining_empty_count,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ No empty genres remaining!'
        ELSE '⚠️ Some empty genres still exist'
    END as status
FROM spotify_artist_genres
WHERE genres IS NULL 
   OR genres = '[]' 
   OR genre_count = 0;

-- Show data source breakdown
SELECT 'DATA SOURCE BREAKDOWN' as report_section;

SELECT 
    data_source,
    COUNT(*) as artist_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM spotify_artist_genres
GROUP BY data_source
ORDER BY artist_count DESC;
*/

SELECT '✅ Clean and repopulate process initiated!' as completion_message;
SELECT 'Wait 2-3 minutes, then run the verification queries (uncomment them)' as next_steps;
