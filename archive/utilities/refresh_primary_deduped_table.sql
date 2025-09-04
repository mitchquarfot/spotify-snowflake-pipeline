-- Refresh Primary Deduped Dynamic Table
-- Run this script in Snowflake to refresh the spotify_mt_listening_deduped table
-- This is the foundation table that feeds into your medallion architecture

-- Context setup
USE ROLE SPOTIFY_ANALYST_ROLE;
USE WAREHOUSE SPOTIFY_WH;
USE DATABASE spotify_analytics;
USE SCHEMA raw_data;

-- =============================================================================
-- REFRESH PRIMARY DEDUPED TABLE
-- =============================================================================

SELECT 'Refreshing primary deduped listening table...' as status;

-- Refresh the main deduped table (this pulls from raw spotify_listening_history)
ALTER DYNAMIC TABLE spotify_mt_listening_deduped REFRESH;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================

-- Check refresh status
SELECT 'VERIFICATION: Dynamic Table Status' as check_type;

SELECT 
    table_name,
    target_lag,
    refresh_mode,
    last_refresh_time,
    next_refresh_time,
    refresh_status,
    refresh_error
FROM INFORMATION_SCHEMA.DYNAMIC_TABLES 
WHERE table_name = 'SPOTIFY_MT_LISTENING_DEDUPED'
  AND table_schema = 'RAW_DATA';

-- Check row count and data freshness
SELECT 'VERIFICATION: Data Freshness and Counts' as check_type;

SELECT 
    COUNT(*) as total_plays,
    COUNT(DISTINCT unique_play) as unique_plays,
    COUNT(DISTINCT primary_artist_id) as unique_artists,
    COUNT(DISTINCT track_id) as unique_tracks,
    MIN(denver_ts) as earliest_play,
    MAX(denver_ts) as latest_play,
    MAX(ingested_at) as latest_ingestion,
    DATEDIFF('day', MIN(denver_ts), MAX(denver_ts)) as days_of_data
FROM spotify_mt_listening_deduped;

-- Check recent activity (last 24 hours)
SELECT 'VERIFICATION: Recent Activity (Last 24 Hours)' as check_type;

SELECT 
    DATE(denver_ts) as play_date,
    COUNT(*) as plays_count,
    COUNT(DISTINCT primary_artist_id) as unique_artists,
    COUNT(DISTINCT track_id) as unique_tracks,
    MIN(denver_ts) as first_play,
    MAX(denver_ts) as last_play
FROM spotify_mt_listening_deduped
WHERE denver_ts >= DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY DATE(denver_ts)
ORDER BY play_date DESC;

-- Sample recent plays
SELECT 'VERIFICATION: Recent Plays Sample' as check_type;

SELECT 
    denver_ts,
    track_name,
    primary_artist_name,
    album_name,
    context_type,
    ingested_at
FROM spotify_mt_listening_deduped
ORDER BY denver_ts DESC
LIMIT 10;

-- Check for any duplicates (should be 0)
SELECT 'VERIFICATION: Duplicate Check' as check_type;

SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT unique_play) as unique_rows,
    (COUNT(*) - COUNT(DISTINCT unique_play)) as duplicate_count,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT unique_play) THEN '✅ No duplicates'
        ELSE '⚠️ Duplicates found!'
    END as duplicate_status
FROM spotify_mt_listening_deduped;

-- Check ingestion timeline (last 7 days)
SELECT 'VERIFICATION: Ingestion Timeline (Last 7 Days)' as check_type;

SELECT 
    DATE(ingested_at) as ingestion_date,
    COUNT(*) as plays_ingested,
    COUNT(DISTINCT primary_artist_id) as artists_ingested,
    MIN(ingested_at) as first_ingestion,
    MAX(ingested_at) as last_ingestion
FROM spotify_mt_listening_deduped
WHERE ingested_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY DATE(ingested_at)
ORDER BY ingestion_date DESC;

-- Check timezone conversion accuracy
SELECT 'VERIFICATION: Timezone Conversion Sample' as check_type;

SELECT 
    played_at as utc_timestamp,
    denver_ts as mountain_time,
    EXTRACT(HOUR FROM played_at) as utc_hour,
    EXTRACT(HOUR FROM denver_ts) as denver_hour,
    (EXTRACT(HOUR FROM denver_ts) - EXTRACT(HOUR FROM played_at)) as hour_offset
FROM spotify_mt_listening_deduped
ORDER BY played_at DESC
LIMIT 5;

SELECT '✅ Primary deduped table refreshed and verified!' as completion_status;
