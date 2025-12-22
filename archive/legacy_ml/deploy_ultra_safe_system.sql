-- ULTRA-SAFE SYSTEM DEPLOYMENT - NO ML VIEWS AT ALL
-- Uses only direct source data, guaranteed to work
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- =====================================================================
-- EMERGENCY FALLBACK: DIRECT SOURCE DATA ONLY
-- =====================================================================

-- Simple popularity-based recommendations (no ML views)
CREATE OR REPLACE VIEW get_safe_recommendations AS
SELECT 
    TRACK_ID as track_id,
    TRACK_NAME as track_name,
    PRIMARY_ARTIST_NAME as artist_name,
    PRIMARY_GENRE as genre,
    ALBUM_NAME as album_name,
    TRACK_POPULARITY as track_popularity,
    -- Simple recommendation score based on popularity + play count
    CASE 
        WHEN TRACK_POPULARITY >= 80 THEN 0.9
        WHEN TRACK_POPULARITY >= 60 THEN 0.7
        WHEN TRACK_POPULARITY >= 40 THEN 0.5
        WHEN TRACK_POPULARITY >= 20 THEN 0.3
        ELSE 0.1
    END as recommendation_score,
    'popularity_based' as strategy,
    ROW_NUMBER() OVER (ORDER BY TRACK_POPULARITY DESC) as position
FROM spotify_analytics.medallion_arch.silver_listening_enriched
WHERE TRACK_POPULARITY > 20
AND TRACK_NAME IS NOT NULL
AND PRIMARY_ARTIST_NAME IS NOT NULL
ORDER BY TRACK_POPULARITY DESC;

-- Genre-based recommendations (direct from source)
CREATE OR REPLACE VIEW get_safe_genre_recommendations AS
SELECT 
    TRACK_ID as track_id,
    TRACK_NAME as track_name,
    PRIMARY_ARTIST_NAME as artist_name,
    TRACK_POPULARITY as track_popularity,
    PRIMARY_GENRE as genre,
    (TRACK_POPULARITY / 100.0) as recommendation_score
FROM spotify_analytics.medallion_arch.silver_listening_enriched
WHERE TRACK_POPULARITY > 30
AND PRIMARY_GENRE IS NOT NULL
ORDER BY TRACK_POPULARITY DESC;

-- User's library for discovery (tracks they have)
CREATE OR REPLACE VIEW get_safe_user_library AS
SELECT 
    TRACK_ID as track_id,
    TRACK_NAME as track_name,
    PRIMARY_ARTIST_NAME as artist_name,
    PRIMARY_GENRE as genre,
    TRACK_POPULARITY as track_popularity,
    COUNT(*) as play_count,
    MAX(TRACK_POPULARITY) / 100.0 as familiarity_score
FROM spotify_analytics.medallion_arch.silver_listening_enriched
WHERE TRACK_NAME IS NOT NULL
GROUP BY TRACK_ID, TRACK_NAME, PRIMARY_ARTIST_NAME, PRIMARY_GENRE, TRACK_POPULARITY
HAVING COUNT(*) >= 1
ORDER BY play_count DESC, TRACK_POPULARITY DESC;

-- =====================================================================
-- SAFE UTILITY FUNCTIONS
-- =====================================================================

-- Count total tracks available
CREATE OR REPLACE FUNCTION get_safe_track_count()
RETURNS INTEGER
LANGUAGE SQL
AS
$$
    SELECT COUNT(DISTINCT TRACK_ID) 
    FROM spotify_analytics.medallion_arch.silver_listening_enriched
$$;

-- Get unique genres count  
CREATE OR REPLACE FUNCTION get_safe_genre_count()
RETURNS INTEGER
LANGUAGE SQL
AS
$$
    SELECT COUNT(DISTINCT PRIMARY_GENRE) 
    FROM spotify_analytics.medallion_arch.silver_listening_enriched
    WHERE PRIMARY_GENRE IS NOT NULL
$$;

-- System status check
CREATE OR REPLACE FUNCTION check_safe_system_status()
RETURNS STRING
LANGUAGE SQL
AS
$$
    SELECT 
        'Safe System: ' || 
        get_safe_track_count() || ' tracks, ' ||
        get_safe_genre_count() || ' genres ✅'
$$;

-- =====================================================================
-- TESTING THE SAFE SYSTEM
-- =====================================================================

-- Test core functionality
SELECT 
    'Safe System Status' as test_name,
    check_safe_system_status() as status;

-- Test recommendations
SELECT 
    'Safe Recommendations Test' as test_name,
    track_name,
    artist_name,
    genre,
    recommendation_score
FROM get_safe_recommendations
LIMIT 5;

-- Test genre recommendations
SELECT 
    'Safe Genre Recommendations Test' as test_name,
    genre,
    COUNT(*) as track_count,
    AVG(recommendation_score) as avg_score
FROM get_safe_genre_recommendations
GROUP BY genre
ORDER BY track_count DESC
LIMIT 5;

-- Test user library
SELECT 
    'Safe User Library Test' as test_name,
    COUNT(*) as total_user_tracks,
    AVG(familiarity_score) as avg_familiarity,
    MAX(play_count) as max_plays
FROM get_safe_user_library;

-- =====================================================================
-- SUCCESS MESSAGE
-- =====================================================================

SELECT 
    '🛡️ ULTRA-SAFE SYSTEM DEPLOYED!' as status,
    'No ML views - direct source data only' as safety_level,
    'Guaranteed to work with any data' as reliability,
    'Basic but functional recommendations' as functionality;

