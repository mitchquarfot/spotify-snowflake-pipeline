-- Refresh All Dynamic Tables in Medallion Architecture
-- Run this script in Snowflake to manually refresh all dynamic tables
-- Location: Denver, CO (Mountain Time)

-- Context setup
USE ROLE SPOTIFY_ANALYST_ROLE;
USE WAREHOUSE SPOTIFY_WH;
USE DATABASE spotify_analytics;
USE SCHEMA medallion_arch;

-- =============================================================================
-- REFRESH DYNAMIC TABLES IN DEPENDENCY ORDER
-- =============================================================================

-- Step 1: Refresh Bronze Layer (depends on raw data)
SELECT 'Refreshing Bronze Layer...' as status;

ALTER DYNAMIC TABLE bronze_artist_genres REFRESH;

-- Step 2: Refresh Silver Layer (depends on Bronze + raw data)
SELECT 'Refreshing Silver Layer...' as status;

ALTER DYNAMIC TABLE silver_listening_enriched REFRESH;
ALTER DYNAMIC TABLE silver_artist_summary REFRESH;

-- Step 3: Refresh Gold Layer (depends on Silver)
SELECT 'Refreshing Gold Layer...' as status;

ALTER DYNAMIC TABLE gold_daily_listening_summary REFRESH;
ALTER DYNAMIC TABLE gold_genre_analysis REFRESH;
ALTER DYNAMIC TABLE gold_monthly_insights REFRESH;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================

-- Check refresh status and row counts
SELECT 'VERIFICATION: Dynamic Table Status' as check_type;

SELECT 
    table_name,
    target_lag,
    refresh_mode,
    last_refresh_time,
    next_refresh_time,
    refresh_status
FROM INFORMATION_SCHEMA.DYNAMIC_TABLES 
WHERE table_schema = 'MEDALLION_ARCH'
ORDER BY table_name;

-- Check row counts in each layer
SELECT 'VERIFICATION: Row Counts by Layer' as check_type;

SELECT 'Bronze Layer' as layer, 'bronze_artist_genres' as table_name, COUNT(*) as row_count FROM bronze_artist_genres
UNION ALL
SELECT 'Silver Layer' as layer, 'silver_listening_enriched' as table_name, COUNT(*) as row_count FROM silver_listening_enriched
UNION ALL
SELECT 'Silver Layer' as layer, 'silver_artist_summary' as table_name, COUNT(*) as row_count FROM silver_artist_summary
UNION ALL
SELECT 'Gold Layer' as layer, 'gold_daily_listening_summary' as table_name, COUNT(*) as row_count FROM gold_daily_listening_summary
UNION ALL
SELECT 'Gold Layer' as layer, 'gold_genre_analysis' as table_name, COUNT(*) as row_count FROM gold_genre_analysis
UNION ALL
SELECT 'Gold Layer' as layer, 'gold_monthly_insights' as table_name, COUNT(*) as row_count FROM gold_monthly_insights
ORDER BY layer, table_name;

-- Check data freshness
SELECT 'VERIFICATION: Data Freshness' as check_type;

SELECT 
    'silver_listening_enriched' as table_name,
    MAX(denver_timestamp) as latest_listening_event,
    MAX(ingested_at) as latest_ingestion,
    COUNT(*) as total_events
FROM silver_listening_enriched
UNION ALL
SELECT 
    'bronze_artist_genres' as table_name,
    NULL as latest_listening_event,
    MAX(ingested_at) as latest_ingestion,
    COUNT(*) as total_events
FROM bronze_artist_genres
ORDER BY table_name;

-- Sample recent data
SELECT 'VERIFICATION: Recent Listening Sample' as check_type;

SELECT 
    denver_timestamp,
    track_name,
    primary_artist_name,
    primary_genre,
    time_of_day_category
FROM silver_listening_enriched 
ORDER BY denver_timestamp DESC 
LIMIT 10;

-- Genre distribution
SELECT 'VERIFICATION: Genre Distribution' as check_type;

SELECT 
    primary_genre,
    COUNT(*) as play_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM silver_listening_enriched 
WHERE primary_genre IS NOT NULL
GROUP BY primary_genre
ORDER BY play_count DESC
LIMIT 10;

SELECT 'All dynamic tables refreshed successfully! 🎉' as completion_status;
