-- Test script to verify window functions work without nesting issues
-- Run this to test the fixed window function logic

USE DATABASE spotify_analytics;
USE SCHEMA medallion_arch;

-- Test the fixed window function pattern
-- This should work now that we're using aliases instead of nested window functions
WITH test_window_functions AS (
    SELECT 
        TRACK_ID,
        TRACK_NAME,
        PRIMARY_GENRE,
        TRACK_POPULARITY,
        
        -- User engagement with this track (these should work)
        COUNT(*) OVER (PARTITION BY TRACK_ID) AS user_play_count,
        MAX(DENVER_DATE) OVER (PARTITION BY TRACK_ID) AS last_played_date
        
    FROM silver_listening_enriched
    WHERE DENVER_DATE >= DATEADD('days', -30, CURRENT_DATE)
),
ranked_tracks AS (
    SELECT *,
        -- Genre rank for this track (FIXED: using alias instead of nested function)
        ROW_NUMBER() OVER (
            PARTITION BY PRIMARY_GENRE 
            ORDER BY TRACK_POPULARITY DESC, user_play_count DESC
        ) AS genre_rank_TEST
        
    FROM test_window_functions
)
SELECT 
    PRIMARY_GENRE,
    TRACK_NAME,
    user_play_count,
    genre_rank_TEST
FROM ranked_tracks
ORDER BY PRIMARY_GENRE, genre_rank_TEST
LIMIT 20;

-- If this query runs without errors, the window function nesting issue is fixed!
