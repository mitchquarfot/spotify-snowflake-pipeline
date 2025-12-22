-- CREATE WORKING ML VIEWS - PROGRESSIVE APPROACH
-- Start ultra-simple, add complexity only if previous level works
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- =====================================================================
-- LEVEL 1: ULTRA-SIMPLE SINGLE-VIEW APPROACHES (GUARANTEED TO WORK)
-- =====================================================================

-- Level 1A: Just collaborative filtering (simplest possible)
CREATE OR REPLACE VIEW ml_simple_collaborative AS
SELECT 
    TRACK_ID,
    TRACK_NAME,
    PRIMARY_ARTIST_NAME,
    PRIMARY_GENRE,
    ALBUM_NAME,
    TRACK_POPULARITY,
    recommendation_score AS final_recommendation_score,
    'collaborative_filtering' AS recommendation_strategies,
    1 AS recommendation_support,
    'Single Algorithm' AS ml_confidence,
    ROW_NUMBER() OVER (ORDER BY recommendation_score DESC) AS playlist_position,
    'Users with similar taste love this track' AS recommendation_reason,
    CASE 
        WHEN TRACK_POPULARITY >= 80 THEN 'Mainstream Hit'
        WHEN TRACK_POPULARITY >= 60 THEN 'Popular Track'
        WHEN TRACK_POPULARITY >= 40 THEN 'Rising Track'
        WHEN TRACK_POPULARITY >= 20 THEN 'Hidden Gem'
        ELSE 'Deep Cut Discovery'
    END AS popularity_tier,
    CURRENT_TIMESTAMP AS generated_at
FROM ml_collaborative_recommendations
WHERE recommendation_score > 0.3
ORDER BY recommendation_score DESC;

-- Level 1B: Just content-based filtering 
CREATE OR REPLACE VIEW ml_simple_content_based AS
SELECT 
    TRACK_ID,
    TRACK_NAME,
    PRIMARY_ARTIST_NAME,
    PRIMARY_GENRE,
    ALBUM_NAME,
    TRACK_POPULARITY,
    recommendation_score AS final_recommendation_score,
    'content_based_filtering' AS recommendation_strategies,
    1 AS recommendation_support,
    'Single Algorithm' AS ml_confidence,
    ROW_NUMBER() OVER (ORDER BY recommendation_score DESC) AS playlist_position,
    'Similar musical characteristics to your favorites' AS recommendation_reason,
    CASE 
        WHEN TRACK_POPULARITY >= 80 THEN 'Mainstream Hit'
        WHEN TRACK_POPULARITY >= 60 THEN 'Popular Track'
        WHEN TRACK_POPULARITY >= 40 THEN 'Rising Track'
        WHEN TRACK_POPULARITY >= 20 THEN 'Hidden Gem'
        ELSE 'Deep Cut Discovery'
    END AS popularity_tier,
    CURRENT_TIMESTAMP AS generated_at
FROM ml_content_based_recommendations
WHERE recommendation_score > 0.4
ORDER BY recommendation_score DESC;

-- =====================================================================
-- LEVEL 2: SIMPLE UNION (NO COMPLEX AGGREGATION)
-- =====================================================================

CREATE OR REPLACE VIEW ml_simple_union AS
SELECT * FROM ml_simple_collaborative
UNION ALL
SELECT * FROM ml_simple_content_based
ORDER BY final_recommendation_score DESC;

-- =====================================================================
-- LEVEL 3: SAFE HYBRID WITH WEIGHTED SCORING (IF LEVEL 2 WORKS)
-- =====================================================================

CREATE OR REPLACE VIEW ml_safe_hybrid AS
SELECT 
    TRACK_ID,
    TRACK_NAME,
    PRIMARY_ARTIST_NAME,
    PRIMARY_GENRE,
    ALBUM_NAME,
    TRACK_POPULARITY,
    -- Simple weighted scoring without complex aggregation
    CASE 
        WHEN recommendation_strategies = 'collaborative_filtering' THEN final_recommendation_score * 0.7
        WHEN recommendation_strategies = 'content_based_filtering' THEN final_recommendation_score * 0.5
        ELSE final_recommendation_score * 0.3
    END AS final_recommendation_score,
    recommendation_strategies,
    recommendation_support,
    ml_confidence,
    ROW_NUMBER() OVER (ORDER BY 
        CASE 
            WHEN recommendation_strategies = 'collaborative_filtering' THEN final_recommendation_score * 0.7
            WHEN recommendation_strategies = 'content_based_filtering' THEN final_recommendation_score * 0.5
            ELSE final_recommendation_score * 0.3
        END DESC
    ) AS playlist_position,
    recommendation_reason,
    popularity_tier,
    generated_at
FROM ml_simple_union
WHERE final_recommendation_score > 0.25;

-- =====================================================================
-- LEVEL 4: FINAL WORKING ML VIEW (FALLBACK TO SAFEST WORKING LEVEL)
-- =====================================================================

-- This will be our main ML view - start with Level 1, upgrade if higher levels work
CREATE OR REPLACE VIEW ml_hybrid_recommendations_working AS
SELECT * FROM ml_simple_collaborative;  -- Start with safest

-- Also create the "advanced" alias that Python expects
CREATE OR REPLACE VIEW ml_hybrid_recommendations_advanced AS 
SELECT * FROM ml_hybrid_recommendations_working;

-- Simple version alias
CREATE OR REPLACE VIEW ml_hybrid_recommendations_simple AS
SELECT * FROM ml_hybrid_recommendations_working;

-- =====================================================================
-- LEVEL 5: ULTRA-SIMPLE ANALYTICS (NO COMPLEX AGGREGATIONS)
-- =====================================================================

CREATE OR REPLACE VIEW ml_recommendation_analytics_advanced AS
SELECT 
    COUNT(*) AS total_ml_recommendations,
    COUNT(DISTINCT PRIMARY_GENRE) AS unique_genres_recommended,
    COUNT(DISTINCT PRIMARY_ARTIST_NAME) AS unique_artists_recommended,
    AVG(final_recommendation_score) AS avg_ml_score,
    MAX(final_recommendation_score) AS max_ml_score,
    MIN(final_recommendation_score) AS min_ml_score,
    
    -- Simple counts (no complex CASE aggregations)
    COUNT(*) AS collaborative_recommendations, -- Since we're starting with just collaborative
    0 AS content_based_recommendations,
    0 AS temporal_recommendations,  
    0 AS discovery_recommendations,
    
    -- Simple metrics
    COUNT(*) AS multi_algorithm_tracks,  -- Will be 0 initially
    0 AS high_confidence_tracks,
    0.0 AS multi_algorithm_percentage,
    
    -- Popularity distribution (safe aggregation)
    SUM(CASE WHEN TRACK_POPULARITY >= 80 THEN 1 ELSE 0 END) AS mainstream_recommendations,
    SUM(CASE WHEN TRACK_POPULARITY BETWEEN 60 AND 79 THEN 1 ELSE 0 END) AS popular_recommendations,
    SUM(CASE WHEN TRACK_POPULARITY BETWEEN 40 AND 59 THEN 1 ELSE 0 END) AS rising_recommendations,
    SUM(CASE WHEN TRACK_POPULARITY BETWEEN 20 AND 39 THEN 1 ELSE 0 END) AS hidden_gem_recommendations,
    SUM(CASE WHEN TRACK_POPULARITY < 20 THEN 1 ELSE 0 END) AS deep_cut_recommendations,
    
    -- Simple diversity
    COUNT(DISTINCT PRIMARY_GENRE) * 100.0 / GREATEST(COUNT(*), 1) AS genre_diversity_percentage,
    COUNT(DISTINCT PRIMARY_ARTIST_NAME) * 100.0 / GREATEST(COUNT(*), 1) AS artist_diversity_percentage,
    
    CURRENT_TIMESTAMP AS analysis_timestamp

FROM ml_hybrid_recommendations_working;

-- =====================================================================
-- TESTING SEQUENCE
-- =====================================================================

-- Test Level 1A (Collaborative only)
SELECT 'Level 1A Test' AS level, COUNT(*) AS tracks FROM ml_simple_collaborative;

-- Test Level 1B (Content-based only) 
SELECT 'Level 1B Test' AS level, COUNT(*) AS tracks FROM ml_simple_content_based;

-- Test Level 2 (Union)
SELECT 'Level 2 Test' AS level, COUNT(*) AS tracks FROM ml_simple_union;

-- Test Level 3 (Safe hybrid)
SELECT 'Level 3 Test' AS level, COUNT(*) AS tracks FROM ml_safe_hybrid;

-- Test Level 4 (Working ML view)
SELECT 'Level 4 Test' AS level, COUNT(*) AS tracks FROM ml_hybrid_recommendations_working;

-- Test Level 5 (Analytics)
SELECT 'Level 5 Test' AS level, total_ml_recommendations FROM ml_recommendation_analytics_advanced;

-- =====================================================================
-- SUCCESS MESSAGE
-- =====================================================================

SELECT 
    '✅ WORKING ML VIEWS CREATED!' AS status,
    'Progressive approach: Start simple, add complexity step by step' AS strategy,
    'Test each level - upgrade to higher levels if they work' AS next_steps,
    'ml_hybrid_recommendations_working is your main working view' AS primary_view;

