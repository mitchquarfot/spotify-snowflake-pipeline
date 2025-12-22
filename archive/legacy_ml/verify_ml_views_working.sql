-- WORKING ML VIEWS VERIFICATION (FIXED FOR SUBQUERY ISSUES)
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- Check if ML views exist in the ANALYTICS schema (correct location)
SELECT 
    'ML Views in ANALYTICS Schema' AS check_type,
    table_name,
    table_type,
    'Found in correct schema ✅' AS status
FROM information_schema.tables
WHERE table_schema = 'ANALYTICS' 
AND table_name LIKE 'ML_%'
ORDER BY table_name;

-- Test the simplified ML view first (this should work)
SELECT 
    'ML_HYBRID_RECOMMENDATIONS_SIMPLE Data Check' AS test_name,
    COUNT(*) AS row_count,
    CASE 
        WHEN COUNT(*) > 0 THEN 'Has data ✅'
        WHEN COUNT(*) = 0 THEN 'Empty - check data pipeline ⚠️'
        ELSE 'Error'
    END AS status
FROM ml_hybrid_recommendations_simple;

-- Test simple query on the working view
SELECT 
    'Sample ML Recommendations (Simple)' AS test_name,
    track_name,
    primary_artist_name,
    track_popularity,
    final_recommendation_score AS recommendation_score
FROM ml_hybrid_recommendations_simple
ORDER BY final_recommendation_score DESC
LIMIT 5;

-- Test individual component views (should work without subquery issues)
SELECT 
    'ML_TRACK_CONTENT_FEATURES Data Check' AS test_name,
    COUNT(*) AS row_count,
    CASE 
        WHEN COUNT(*) > 0 THEN 'Has data ✅'
        ELSE 'No data ⚠️'
    END AS status
FROM ml_track_content_features
LIMIT 1;

-- Test collaborative filtering view
SELECT 
    'ML_COLLABORATIVE_RECOMMENDATIONS Data Check' AS test_name,
    COUNT(*) AS row_count,
    CASE 
        WHEN COUNT(*) > 0 THEN 'Has data ✅'
        ELSE 'No data ⚠️'
    END AS status
FROM ml_collaborative_recommendations
LIMIT 1;

-- Test if we can access basic data from content features
SELECT 
    'Sample Content Features' AS test_name,
    track_name,
    primary_artist_name,
    primary_genre,
    track_popularity
FROM ml_track_content_features
WHERE user_play_count > 0
ORDER BY track_popularity DESC
LIMIT 5;
