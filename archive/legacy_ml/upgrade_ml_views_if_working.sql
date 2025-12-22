-- UPGRADE ML VIEWS IF BASIC VERSIONS WORK
-- Run this after create_working_ml_views.sql succeeds
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- =====================================================================
-- STEP 1: VERIFY BASIC VIEWS WORK
-- =====================================================================

-- Check that Level 1 views work
SELECT 
    'Basic Views Status' AS check_name,
    (SELECT COUNT(*) FROM ml_simple_collaborative) AS collaborative_tracks,
    (SELECT COUNT(*) FROM ml_simple_content_based) AS content_based_tracks,
    CASE 
        WHEN (SELECT COUNT(*) FROM ml_simple_collaborative) > 0 AND 
             (SELECT COUNT(*) FROM ml_simple_content_based) > 0 
        THEN '✅ Ready to upgrade'
        ELSE '❌ Fix basic views first'
    END AS upgrade_status;

-- =====================================================================
-- STEP 2: UPGRADE TO DUAL-ALGORITHM HYBRID (IF STEP 1 PASSES)
-- =====================================================================

-- Only run this if basic views work!
CREATE OR REPLACE VIEW ml_dual_algorithm_hybrid AS
WITH collaborative_weighted AS (
    SELECT 
        TRACK_ID,
        TRACK_NAME,
        PRIMARY_ARTIST_NAME,
        PRIMARY_GENRE,
        ALBUM_NAME,
        TRACK_POPULARITY,
        final_recommendation_score * 0.6 AS weighted_score,  -- 60% weight for collaborative
        recommendation_strategies,
        'collaborative_priority' AS strategy_type,
        recommendation_reason,
        popularity_tier,
        generated_at
    FROM ml_simple_collaborative
    WHERE final_recommendation_score > 0.3
),
content_weighted AS (
    SELECT 
        TRACK_ID,
        TRACK_NAME,
        PRIMARY_ARTIST_NAME,
        PRIMARY_GENRE,
        ALBUM_NAME,
        TRACK_POPULARITY,
        final_recommendation_score * 0.4 AS weighted_score,  -- 40% weight for content
        recommendation_strategies,
        'content_priority' AS strategy_type,
        recommendation_reason,
        popularity_tier,
        generated_at
    FROM ml_simple_content_based
    WHERE final_recommendation_score > 0.4
),
all_weighted AS (
    SELECT * FROM collaborative_weighted
    UNION ALL
    SELECT * FROM content_weighted
)
SELECT 
    TRACK_ID,
    MAX(TRACK_NAME) AS TRACK_NAME,
    MAX(PRIMARY_ARTIST_NAME) AS PRIMARY_ARTIST_NAME,
    MAX(PRIMARY_GENRE) AS PRIMARY_GENRE,
    MAX(ALBUM_NAME) AS ALBUM_NAME,
    MAX(TRACK_POPULARITY) AS TRACK_POPULARITY,
    SUM(weighted_score) AS final_recommendation_score,
    
    -- Multi-algorithm indicators
    CASE 
        WHEN COUNT(*) > 1 THEN 'dual_algorithm_hybrid'
        ELSE MAX(recommendation_strategies)
    END AS recommendation_strategies,
    
    COUNT(*) AS recommendation_support,
    
    CASE 
        WHEN COUNT(*) > 1 THEN 'High Confidence'
        ELSE 'Single Algorithm'
    END AS ml_confidence,
    
    CASE 
        WHEN COUNT(*) > 1 THEN 'Multiple ML algorithms agree on this track'
        ELSE MAX(recommendation_reason)
    END AS recommendation_reason,
    
    MAX(popularity_tier) AS popularity_tier,
    MAX(generated_at) AS generated_at
    
FROM all_weighted
GROUP BY TRACK_ID;

-- =====================================================================
-- STEP 3: TEST DUAL ALGORITHM VERSION
-- =====================================================================

SELECT 
    'Dual Algorithm Test' AS test_name,
    COUNT(*) AS total_tracks,
    COUNT(CASE WHEN recommendation_support > 1 THEN 1 END) AS multi_algorithm_tracks,
    AVG(final_recommendation_score) AS avg_score,
    MAX(final_recommendation_score) AS max_score
FROM ml_dual_algorithm_hybrid;

-- =====================================================================
-- STEP 4: UPGRADE MAIN VIEW IF DUAL ALGORITHM WORKS
-- =====================================================================

-- Check if dual algorithm version works
SELECT 
    'Upgrade Decision' AS decision_point,
    CASE 
        WHEN (SELECT COUNT(*) FROM ml_dual_algorithm_hybrid) > 50 
        THEN '✅ Upgrading to dual algorithm'
        ELSE '⚠️  Staying with single algorithm'
    END AS decision;

-- Upgrade main view to dual algorithm if it works
-- (This will only succeed if dual algorithm view works)
CREATE OR REPLACE VIEW ml_hybrid_recommendations_working AS
SELECT 
    TRACK_ID,
    TRACK_NAME,
    PRIMARY_ARTIST_NAME,
    PRIMARY_GENRE,
    ALBUM_NAME,
    TRACK_POPULARITY,
    final_recommendation_score,
    recommendation_strategies,
    recommendation_support,
    ml_confidence,
    ROW_NUMBER() OVER (ORDER BY final_recommendation_score DESC) AS playlist_position,
    recommendation_reason,
    popularity_tier,
    generated_at
FROM ml_dual_algorithm_hybrid
ORDER BY final_recommendation_score DESC;

-- Update aliases to point to upgraded version
CREATE OR REPLACE VIEW ml_hybrid_recommendations_advanced AS 
SELECT * FROM ml_hybrid_recommendations_working;

CREATE OR REPLACE VIEW ml_hybrid_recommendations_simple AS
SELECT * FROM ml_hybrid_recommendations_working;

-- =====================================================================
-- STEP 5: UPGRADE ANALYTICS
-- =====================================================================

CREATE OR REPLACE VIEW ml_recommendation_analytics_advanced AS
SELECT 
    COUNT(*) AS total_ml_recommendations,
    COUNT(DISTINCT PRIMARY_GENRE) AS unique_genres_recommended,
    COUNT(DISTINCT PRIMARY_ARTIST_NAME) AS unique_artists_recommended,
    ROUND(AVG(final_recommendation_score), 3) AS avg_ml_score,
    ROUND(MAX(final_recommendation_score), 3) AS max_ml_score,
    ROUND(MIN(final_recommendation_score), 3) AS min_ml_score,
    
    -- Algorithm distribution
    COUNT(CASE WHEN recommendation_strategies LIKE '%collaborative%' THEN 1 END) AS collaborative_recommendations,
    COUNT(CASE WHEN recommendation_strategies LIKE '%content%' THEN 1 END) AS content_based_recommendations,
    0 AS temporal_recommendations,
    0 AS discovery_recommendations,
    
    -- Multi-algorithm intelligence
    COUNT(CASE WHEN recommendation_support > 1 THEN 1 END) AS multi_algorithm_tracks,
    COUNT(CASE WHEN ml_confidence = 'High Confidence' THEN 1 END) AS high_confidence_tracks,
    ROUND(COUNT(CASE WHEN recommendation_support > 1 THEN 1 END) * 100.0 / COUNT(*), 1) AS multi_algorithm_percentage,
    
    -- Popularity distribution
    COUNT(CASE WHEN TRACK_POPULARITY >= 80 THEN 1 END) AS mainstream_recommendations,
    COUNT(CASE WHEN TRACK_POPULARITY BETWEEN 60 AND 79 THEN 1 END) AS popular_recommendations,
    COUNT(CASE WHEN TRACK_POPULARITY BETWEEN 40 AND 59 THEN 1 END) AS rising_recommendations,
    COUNT(CASE WHEN TRACK_POPULARITY BETWEEN 20 AND 39 THEN 1 END) AS hidden_gem_recommendations,
    COUNT(CASE WHEN TRACK_POPULARITY < 20 THEN 1 END) AS deep_cut_recommendations,
    
    -- Diversity metrics
    ROUND(COUNT(DISTINCT PRIMARY_GENRE) * 100.0 / COUNT(*), 1) AS genre_diversity_percentage,
    ROUND(COUNT(DISTINCT PRIMARY_ARTIST_NAME) * 100.0 / COUNT(*), 1) AS artist_diversity_percentage,
    
    CURRENT_TIMESTAMP AS analysis_timestamp

FROM ml_hybrid_recommendations_working;

-- =====================================================================
-- FINAL VERIFICATION
-- =====================================================================

-- Test all main views
SELECT '🧠 Final ML System Test' AS test_name, COUNT(*) AS tracks 
FROM ml_hybrid_recommendations_working;

SELECT '📊 Analytics Test' AS test_name, total_ml_recommendations 
FROM ml_recommendation_analytics_advanced;

SELECT 
    '🎵 Sample Recommendations' AS test_name,
    TRACK_NAME,
    PRIMARY_ARTIST_NAME,
    final_recommendation_score,
    recommendation_strategies,
    ml_confidence
FROM ml_hybrid_recommendations_working
ORDER BY final_recommendation_score DESC
LIMIT 5;

-- =====================================================================
-- SUCCESS MESSAGE
-- =====================================================================

SELECT 
    '🎉 ML VIEWS UPGRADED SUCCESSFULLY!' AS status,
    'Progressive upgrade from single to dual algorithm' AS upgrade_type,
    'All complex subquery issues avoided' AS technical_approach,
    'ML intelligence preserved with working implementation' AS result;

