-- Enhanced query to find missing artists and prepare for population
-- Run this in Snowflake to identify missing artists and get their IDs

USE ROLE SPOTIFY_ANALYST_ROLE;
USE WAREHOUSE SPOTIFY_WH;
USE DATABASE spotify_analytics;
USE SCHEMA raw_data;

-- =============================================================================
-- STEP 1: Find missing artists with detailed stats
-- =============================================================================

-- Get comprehensive missing artist information
CREATE OR REPLACE TEMPORARY TABLE missing_artists_analysis AS
SELECT 
    h.primary_artist_id,
    h.primary_artist_name,
    COUNT(DISTINCT h.unique_play) as play_count,
    COUNT(DISTINCT h.denver_date) as days_active,
    MIN(h.denver_ts) as first_played,
    MAX(h.denver_ts) as last_played,
    COUNT(DISTINCT h.track_id) as unique_tracks,
    CASE WHEN ag.artist_id IS NULL THEN 'MISSING' ELSE 'PRESENT' END as genre_status
FROM spotify_mt_listening_deduped h
LEFT JOIN spotify_artist_genres ag ON h.primary_artist_id = ag.artist_id
WHERE ag.artist_id IS NULL  -- Only missing artists
GROUP BY h.primary_artist_id, h.primary_artist_name, ag.artist_id
ORDER BY play_count DESC;

-- Display the analysis
SELECT 'MISSING ARTISTS ANALYSIS' as report_section;

SELECT 
    primary_artist_id,
    primary_artist_name,
    play_count,
    days_active,
    first_played,
    last_played,
    unique_tracks
FROM missing_artists_analysis
ORDER BY play_count DESC
LIMIT 50;

-- Summary statistics
SELECT 'MISSING ARTISTS SUMMARY' as report_section;

SELECT 
    COUNT(*) as total_missing_artists,
    SUM(play_count) as total_missing_plays,
    ROUND(100.0 * SUM(play_count) / (
        SELECT COUNT(DISTINCT unique_play) 
        FROM spotify_mt_listening_deduped
    ), 2) as percentage_of_total_plays,
    MAX(play_count) as most_plays_by_missing_artist,
    AVG(play_count) as avg_plays_per_missing_artist
FROM missing_artists_analysis;

-- =============================================================================
-- STEP 2: Generate artist ID list for processing
-- =============================================================================

-- Get comma-separated list of top missing artist IDs (for manual processing)
SELECT 'TOP MISSING ARTIST IDS (for manual processing)' as report_section;

SELECT 
    LISTAGG(primary_artist_id, ',') WITHIN GROUP (ORDER BY play_count DESC) as artist_ids_csv
FROM (
    SELECT primary_artist_id, play_count
    FROM missing_artists_analysis
    ORDER BY play_count DESC
    LIMIT 50  -- Process top 50 missing artists
);

-- Get individual artist IDs (for copy-paste)
SELECT 'INDIVIDUAL ARTIST IDS (copy these)' as report_section;

SELECT 
    primary_artist_id,
    primary_artist_name,
    play_count,
    '''' || primary_artist_id || ''',' as formatted_id
FROM missing_artists_analysis
ORDER BY play_count DESC
LIMIT 20;

-- =============================================================================
-- STEP 3: Check current genre coverage
-- =============================================================================

-- Overall coverage statistics
SELECT 'GENRE COVERAGE STATISTICS' as report_section;

WITH coverage_stats AS (
    SELECT 
        COUNT(DISTINCT h.primary_artist_id) as total_artists,
        COUNT(DISTINCT CASE WHEN ag.artist_id IS NOT NULL THEN h.primary_artist_id END) as artists_with_genres,
        COUNT(DISTINCT h.unique_play) as total_plays,
        COUNT(DISTINCT CASE WHEN ag.artist_id IS NOT NULL THEN h.unique_play END) as plays_with_genres
    FROM spotify_mt_listening_deduped h
    LEFT JOIN spotify_artist_genres ag ON h.primary_artist_id = ag.artist_id
)
SELECT 
    total_artists,
    artists_with_genres,
    (total_artists - artists_with_genres) as missing_artists,
    ROUND(100.0 * artists_with_genres / total_artists, 2) as artist_coverage_percentage,
    total_plays,
    plays_with_genres,
    (total_plays - plays_with_genres) as plays_missing_genres,
    ROUND(100.0 * plays_with_genres / total_plays, 2) as play_coverage_percentage
FROM coverage_stats;

-- =============================================================================
-- STEP 4: Prepare for external processing
-- =============================================================================

-- Create a JSON-like output for the Python script
SELECT 'JSON FORMAT FOR PYTHON SCRIPT' as report_section;

SELECT 
    '[' || LISTAGG(
        '{"PRIMARY_ARTIST_ID":"' || primary_artist_id || '","PRIMARY_ARTIST_NAME":"' || 
        REPLACE(primary_artist_name, '"', '\\"') || '","PLAY_COUNT":' || play_count || '}', 
        ','
    ) WITHIN GROUP (ORDER BY play_count DESC) || ']' as json_output
FROM (
    SELECT primary_artist_id, primary_artist_name, play_count
    FROM missing_artists_analysis
    ORDER BY play_count DESC
    LIMIT 50
);

-- Clean up
DROP TABLE missing_artists_analysis;

SELECT '✅ Analysis complete! Use the artist IDs above with the Python script.' as completion_message;
