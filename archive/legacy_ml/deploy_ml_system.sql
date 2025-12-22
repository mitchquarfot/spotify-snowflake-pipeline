-- =====================================================================
-- SPOTIFY ML SYSTEM DEPLOYMENT SCRIPT
-- Quick deployment script for Snowflake Native Streamlit
-- Run this AFTER deploying the main SQL files
-- =====================================================================

USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- =====================================================================
-- 1. VERIFY PREREQUISITES
-- =====================================================================

-- Check data availability
SELECT 
    'Data Validation' as check_type,
    COUNT(*) as total_tracks,
    COUNT(DISTINCT primary_genre) as unique_genres,
    COUNT(DISTINCT primary_artist_id) as unique_artists,
    CASE 
        WHEN COUNT(*) >= 50 AND COUNT(DISTINCT primary_genre) >= 5 AND COUNT(DISTINCT primary_artist_id) >= 20 
        THEN '✅ Ready for ML' 
        ELSE '❌ Need more data' 
    END as status
FROM spotify_analytics.medallion_arch.silver_listening_enriched
WHERE denver_date >= DATEADD('days', -90, CURRENT_DATE);

-- =====================================================================
-- 2. INITIALIZE ML SYSTEM
-- =====================================================================

-- Initialize the automation system (creates tracking tables)
CALL initialize_ml_automation();

-- =====================================================================
-- 3. VERIFY ML COMPONENTS
-- =====================================================================

-- Check ML views are populated
SELECT 'ML Views Check' as check_type, 'ml_user_genre_interactions' as component, COUNT(*) as rows FROM ml_user_genre_interactions
UNION ALL
SELECT 'ML Views Check', 'ml_track_content_features', COUNT(*) FROM ml_track_content_features
UNION ALL  
SELECT 'ML Views Check', 'ml_temporal_patterns', COUNT(*) FROM ml_temporal_patterns
UNION ALL
SELECT 'ML Views Check', 'ml_genre_similarity_matrix', COUNT(*) FROM ml_genre_similarity_matrix
UNION ALL
SELECT 'ML Views Check', 'ml_hybrid_recommendations', COUNT(*) FROM ml_hybrid_recommendations;

-- Check ML functions exist
SELECT 'ML Functions Check' as check_type, 'Functions Created' as component, COUNT(*) as count
FROM (
    SHOW FUNCTIONS LIKE 'get_spotify%'
);

-- =====================================================================
-- 4. TEST RECOMMENDATION GENERATION
-- =====================================================================

-- Test core recommendation functions
SELECT 'Function Test' as test_type, 'Hybrid Recommendations' as function_name, COUNT(*) as results 
FROM TABLE(get_spotify_recommendations(5))
UNION ALL
SELECT 'Function Test', 'Discovery Recommendations', COUNT(*) 
FROM TABLE(get_discovery_recommendations('balanced', 5, 70))
UNION ALL
SELECT 'Function Test', 'Time-Based Recommendations', COUNT(*) 
FROM TABLE(get_time_based_recommendations(HOUR(CURRENT_TIMESTAMP()), DAYOFWEEK(CURRENT_DATE()) IN (0,6), 5));

-- =====================================================================
-- 5. SAMPLE RECOMMENDATIONS PREVIEW
-- =====================================================================

-- Show sample recommendations to verify quality
SELECT 
    '🎵 Sample Hybrid Recommendations' as section,
    track_name,
    artist_name,
    genre,
    ROUND(recommendation_score, 3) as score,
    recommendation_strategies as strategies
FROM TABLE(get_spotify_recommendations(3))
ORDER BY recommendation_score DESC;

-- Show sample discovery recommendations
SELECT 
    '🔍 Sample Discovery Recommendations' as section,
    track_name,
    artist_name,
    genre,
    ROUND(discovery_score, 3) as score,
    discovery_reason as reason
FROM TABLE(get_discovery_recommendations('balanced', 3, 70))
ORDER BY discovery_score DESC;

-- =====================================================================
-- 6. SYSTEM STATUS SUMMARY
-- =====================================================================

-- Overall system status
SELECT 
    'DEPLOYMENT STATUS' as summary_type,
    CASE 
        WHEN (SELECT COUNT(*) FROM ml_user_genre_interactions) > 0
         AND (SELECT COUNT(*) FROM ml_hybrid_recommendations) > 0
         AND (SELECT COUNT(*) FROM TABLE(get_spotify_recommendations(1))) > 0
        THEN '✅ ML SYSTEM READY FOR STREAMLIT'
        ELSE '❌ DEPLOYMENT INCOMPLETE'
    END as status,
    CURRENT_TIMESTAMP() as check_time;

-- =====================================================================
-- 7. OPTIONAL: START AUTOMATION (UNCOMMENT TO ENABLE)
-- =====================================================================

-- Uncomment these lines to start automated monitoring and retraining:
-- CALL start_ml_automation();
-- SELECT '🤖 Automated ML system started' as automation_status;

-- =====================================================================
-- DEPLOYMENT COMPLETE
-- =====================================================================

SELECT 
    '🎉 DEPLOYMENT SUMMARY' as final_status,
    'ML recommendation system deployed successfully!' as message,
    'Next: Update your Streamlit app with the new ML tab' as next_step;

-- =====================================================================
-- NEXT STEPS:
-- 1. Update your Snowflake Native Streamlit app with spotify_analytics_streamlit_app.py
-- 2. Test the new "🤖 ML Recommendations" tab
-- 3. Generate your first AI-powered playlist!
-- =====================================================================
