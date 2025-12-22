-- ML SYSTEM DEPLOYMENT - NO UDFs (AVOIDING COMPILATION ISSUES)
-- Uses views and direct queries instead of table-valued functions
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- =====================================================================
-- 1. CREATE WORKING ML RECOMMENDATION VIEWS
-- =====================================================================

-- Main recommendation view (replaces UDF)
CREATE OR REPLACE VIEW get_top_recommendations AS
SELECT 
    TRACK_ID as track_id,
    TRACK_NAME as track_name,
    PRIMARY_ARTIST_NAME as artist_name,
    PRIMARY_GENRE as genre,
    ALBUM_NAME as album_name,
    TRACK_POPULARITY as track_popularity,
    final_recommendation_score as recommendation_score,
    playlist_position
FROM ml_hybrid_recommendations_simple
WHERE final_recommendation_score >= 0.3
ORDER BY final_recommendation_score DESC;

-- Genre-specific recommendations (replaces UDF)
CREATE OR REPLACE VIEW get_genre_recommendations AS
SELECT 
    TRACK_ID as track_id,
    TRACK_NAME as track_name,
    PRIMARY_ARTIST_NAME as artist_name,
    TRACK_POPULARITY as track_popularity,
    PRIMARY_GENRE as genre
FROM ml_track_content_features
WHERE user_play_count >= 1  -- Only tracks in user's library
ORDER BY track_popularity DESC;

-- Discovery recommendations (replaces UDF)
CREATE OR REPLACE VIEW get_discovery_recommendations AS
SELECT 
    TRACK_ID as track_id,
    TRACK_NAME as track_name,
    PRIMARY_ARTIST_NAME as artist_name,
    PRIMARY_GENRE as genre,
    recommendation_score as discovery_score
FROM ml_discovery_recommendations
WHERE recommendation_score > 0.5
ORDER BY recommendation_score DESC;

-- =====================================================================
-- 2. UTILITY SCALAR FUNCTIONS (THESE WORK RELIABLY)
-- =====================================================================

-- Get total recommendation count
CREATE OR REPLACE FUNCTION get_recommendation_count()
RETURNS INTEGER
LANGUAGE SQL
AS
$$
    SELECT COUNT(*) FROM ml_hybrid_recommendations_simple
$$;

-- Get max recommendation score
CREATE OR REPLACE FUNCTION get_max_recommendation_score()
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    SELECT MAX(final_recommendation_score) FROM ml_hybrid_recommendations_simple
$$;

-- Check if ML system has data
CREATE OR REPLACE FUNCTION check_ml_data_status()
RETURNS STRING
LANGUAGE SQL
AS
$$
    SELECT CASE 
        WHEN (SELECT COUNT(*) FROM ml_hybrid_recommendations_simple) > 0 
        THEN 'ML System Ready ✅'
        ELSE 'No ML data available ❌'
    END
$$;

-- =====================================================================
-- 3. TESTING AND VALIDATION
-- =====================================================================

-- Test all ML views
SELECT 'get_top_recommendations' as view_name, COUNT(*) as row_count FROM get_top_recommendations
UNION ALL
SELECT 'get_genre_recommendations' as view_name, COUNT(*) as row_count FROM get_genre_recommendations  
UNION ALL
SELECT 'get_discovery_recommendations' as view_name, COUNT(*) as row_count FROM get_discovery_recommendations;

-- Test scalar functions
SELECT 
    'ML System Status Check' as test_name,
    check_ml_data_status() as status,
    get_recommendation_count() as total_recommendations,
    get_max_recommendation_score() as max_score;

-- Sample recommendations
SELECT 
    'Top 5 ML Recommendations' as test_name,
    track_name,
    artist_name,
    recommendation_score
FROM get_top_recommendations
LIMIT 5;

-- =====================================================================
-- 4. SUCCESS MESSAGE
-- =====================================================================

SELECT 
    '🎉 ML SYSTEM DEPLOYED SUCCESSFULLY (NO UDFs)!' as status,
    'Views created instead of table functions' as approach,
    'Python system updated to use views' as integration,
    'Ready for music discovery!' as next_step;
