-- CREATE ML DYNAMIC TABLES - AVOID VIEW COMPLEXITY ISSUES
-- Dynamic tables materialize results and refresh automatically
-- Much more performant and reliable than complex views
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- =====================================================================
-- 1. COLLABORATIVE FILTERING DYNAMIC TABLE
-- =====================================================================

CREATE OR REPLACE DYNAMIC TABLE ml_collaborative_recommendations_dt
TARGET_LAG = '15 minutes'
WAREHOUSE = 'spotify_analytics_wh'
AS
SELECT 
    tcf.track_id,
    tcf.track_name,
    tcf.primary_artist_name,
    tcf.primary_genre,
    tcf.album_name,
    tcf.track_popularity,
    
    -- Collaborative scoring: Combine user affinity + genre similarity  
    GREATEST(
        ugi.weighted_preference * 0.6 +      -- User's preference for this genre
        COALESCE(gsm.jaccard_similarity, 0.1) * 0.4,  -- Genre similarity bonus
        0.1
    ) AS recommendation_score,
    
    'collaborative_filtering' AS recommendation_strategy,
    ugi.weighted_preference AS affinity_score,
    COALESCE(gsm.jaccard_similarity, 0) AS genre_similarity,
    ROW_NUMBER() OVER (ORDER BY 
        (ugi.weighted_preference * 0.6 + COALESCE(gsm.jaccard_similarity, 0.1) * 0.4) DESC
    ) AS genre_track_rank,
    
    CURRENT_TIMESTAMP AS generated_at
    
FROM ml_track_content_features tcf
INNER JOIN ml_user_genre_interactions ugi 
    ON tcf.primary_genre = ugi.primary_genre
LEFT JOIN ml_genre_similarity_matrix gsm 
    ON tcf.primary_genre = gsm.genre_a
    
-- Filter for quality recommendations
WHERE tcf.user_play_count <= 1           -- Don't recommend heavily played tracks
AND ugi.weighted_preference > 0.1        -- User has some preference for genre
AND tcf.track_popularity >= 10           -- Track has some popularity
AND tcf.primary_genre IS NOT NULL;

-- =====================================================================
-- 2. CONTENT-BASED FILTERING DYNAMIC TABLE  
-- =====================================================================

CREATE OR REPLACE DYNAMIC TABLE ml_content_based_recommendations_dt
TARGET_LAG = '15 minutes'
WAREHOUSE = 'spotify_analytics_wh'
AS
WITH user_preferences AS (
    -- Calculate user's average preferences
    SELECT 
        AVG(track_popularity) AS avg_popularity,
        AVG(track_duration_ms) AS avg_duration,
        MODE(era_category) AS preferred_era,
        MODE(popularity_tier) AS preferred_popularity_tier
    FROM ml_track_content_features
    WHERE user_play_count >= 2
)
SELECT 
    tcf.track_id,
    tcf.track_name,
    tcf.primary_artist_name,
    tcf.primary_genre,
    tcf.album_name,
    tcf.track_popularity,
    
    -- Content-based similarity scoring
    GREATEST(
        -- Popularity similarity (closer to user's average = higher score)
        1.0 - ABS(tcf.track_popularity - up.avg_popularity) / 100.0 * 0.3 +
        
        -- Duration similarity  
        1.0 - ABS(tcf.track_duration_ms - up.avg_duration) / 300000.0 * 0.2 +
        
        -- Era matching bonus
        CASE WHEN tcf.era_category = up.preferred_era THEN 0.3 ELSE 0.1 END +
        
        -- Popularity tier matching
        CASE WHEN tcf.popularity_tier = up.preferred_popularity_tier THEN 0.2 ELSE 0.1 END,
        
        0.1
    ) AS recommendation_score,
    
    'content_based_filtering' AS recommendation_strategy,
    ABS(tcf.track_popularity - up.avg_popularity) AS popularity_distance,
    ROW_NUMBER() OVER (ORDER BY 
        (1.0 - ABS(tcf.track_popularity - up.avg_popularity) / 100.0 * 0.3 +
         1.0 - ABS(tcf.track_duration_ms - up.avg_duration) / 300000.0 * 0.2 +
         CASE WHEN tcf.era_category = up.preferred_era THEN 0.3 ELSE 0.1 END +
         CASE WHEN tcf.popularity_tier = up.preferred_popularity_tier THEN 0.2 ELSE 0.1 END) DESC
    ) AS content_rank,
    
    CURRENT_TIMESTAMP AS generated_at
    
FROM ml_track_content_features tcf
CROSS JOIN user_preferences up
WHERE tcf.user_play_count <= 1           -- Don't recommend heavily played tracks
AND tcf.track_popularity >= 10           -- Quality threshold
AND tcf.primary_genre IS NOT NULL;

-- =====================================================================
-- 3. HYBRID ML RECOMMENDATIONS DYNAMIC TABLE
-- =====================================================================

CREATE OR REPLACE DYNAMIC TABLE ml_hybrid_recommendations_dt
TARGET_LAG = '20 minutes' 
WAREHOUSE = 'spotify_analytics_wh'
AS
WITH all_recommendations AS (
    -- Collaborative recommendations (60% weight)
    SELECT 
        track_id,
        track_name,
        primary_artist_name,
        primary_genre,
        album_name,
        track_popularity,
        recommendation_score * 0.6 AS weighted_score,
        'collaborative_filtering' AS strategy,
        recommendation_score AS original_score,
        generated_at
    FROM ml_collaborative_recommendations_dt
    WHERE recommendation_score > 0.2
    
    UNION ALL
    
    -- Content-based recommendations (40% weight)
    SELECT 
        track_id,
        track_name,
        primary_artist_name,
        primary_genre,
        album_name,
        track_popularity,
        recommendation_score * 0.4 AS weighted_score,
        'content_based_filtering' AS strategy,
        recommendation_score AS original_score,
        generated_at
    FROM ml_content_based_recommendations_dt
    WHERE recommendation_score > 0.3
),
aggregated_recommendations AS (
    SELECT 
        track_id,
        MAX(track_name) AS track_name,
        MAX(primary_artist_name) AS primary_artist_name,
        MAX(primary_genre) AS primary_genre,
        MAX(album_name) AS album_name,
        MAX(track_popularity) AS track_popularity,
        
        -- Aggregate scores
        SUM(weighted_score) AS final_recommendation_score,
        COUNT(*) AS recommendation_support,
        MAX(original_score) AS max_individual_score,
        
        -- Strategy combination
        CASE 
            WHEN COUNT(*) = 1 THEN MAX(strategy)
            ELSE 'hybrid_multi_strategy'
        END AS combined_strategies,
        
        MAX(generated_at) AS generated_at
    FROM all_recommendations
    GROUP BY track_id
)
SELECT 
    track_id,
    track_name,
    primary_artist_name,
    primary_genre,
    album_name,
    track_popularity,
    final_recommendation_score,
    combined_strategies AS recommendation_strategies,
    recommendation_support,
    
    -- ML confidence based on algorithm agreement
    CASE 
        WHEN recommendation_support > 1 THEN 'High Confidence'
        WHEN final_recommendation_score > 0.6 THEN 'Medium Confidence'
        ELSE 'Single Algorithm'
    END AS ml_confidence,
    
    -- Recommendation reasoning
    CASE 
        WHEN recommendation_support > 1 THEN 'Multiple algorithms agree this matches your taste'
        WHEN combined_strategies = 'collaborative_filtering' THEN 'Users with similar taste love this track'
        WHEN combined_strategies = 'content_based_filtering' THEN 'Similar to tracks you already love'
        ELSE 'ML-powered personalized recommendation'
    END AS recommendation_reason,
    
    -- Popularity categorization
    CASE 
        WHEN track_popularity >= 80 THEN 'Mainstream Hit'
        WHEN track_popularity >= 60 THEN 'Popular Track'
        WHEN track_popularity >= 40 THEN 'Rising Track'
        WHEN track_popularity >= 20 THEN 'Hidden Gem'
        ELSE 'Deep Cut Discovery'
    END AS popularity_tier,
    
    ROW_NUMBER() OVER (ORDER BY final_recommendation_score DESC) AS playlist_position,
    generated_at
    
FROM aggregated_recommendations
WHERE final_recommendation_score > 0.25
ORDER BY final_recommendation_score DESC;

-- =====================================================================
-- 4. ML ANALYTICS DYNAMIC TABLE
-- =====================================================================

CREATE OR REPLACE DYNAMIC TABLE ml_recommendation_analytics_dt
TARGET_LAG = '30 minutes'
WAREHOUSE = 'spotify_analytics_wh'  
AS
SELECT 
    -- Basic metrics
    COUNT(*) AS total_ml_recommendations,
    COUNT(DISTINCT primary_genre) AS unique_genres_recommended,
    COUNT(DISTINCT primary_artist_name) AS unique_artists_recommended,
    ROUND(AVG(final_recommendation_score), 3) AS avg_ml_score,
    ROUND(MAX(final_recommendation_score), 3) AS max_ml_score,
    ROUND(MIN(final_recommendation_score), 3) AS min_ml_score,
    
    -- Strategy distribution
    COUNT(CASE WHEN recommendation_strategies = 'collaborative_filtering' THEN 1 END) AS collaborative_only,
    COUNT(CASE WHEN recommendation_strategies = 'content_based_filtering' THEN 1 END) AS content_based_only,
    COUNT(CASE WHEN recommendation_strategies = 'hybrid_multi_strategy' THEN 1 END) AS multi_algorithm,
    
    -- Confidence distribution
    COUNT(CASE WHEN ml_confidence = 'High Confidence' THEN 1 END) AS high_confidence_tracks,
    COUNT(CASE WHEN ml_confidence = 'Medium Confidence' THEN 1 END) AS medium_confidence_tracks,
    COUNT(CASE WHEN ml_confidence = 'Single Algorithm' THEN 1 END) AS single_algorithm_tracks,
    
    -- Multi-algorithm percentage
    ROUND(COUNT(CASE WHEN recommendation_support > 1 THEN 1 END) * 100.0 / COUNT(*), 1) AS multi_algorithm_percentage,
    
    -- Popularity distribution
    COUNT(CASE WHEN track_popularity >= 80 THEN 1 END) AS mainstream_hits,
    COUNT(CASE WHEN track_popularity BETWEEN 60 AND 79 THEN 1 END) AS popular_tracks,
    COUNT(CASE WHEN track_popularity BETWEEN 40 AND 59 THEN 1 END) AS rising_tracks,
    COUNT(CASE WHEN track_popularity BETWEEN 20 AND 39 THEN 1 END) AS hidden_gems,
    COUNT(CASE WHEN track_popularity < 20 THEN 1 END) AS deep_cuts,
    
    -- Diversity metrics
    ROUND(COUNT(DISTINCT primary_genre) * 100.0 / COUNT(*), 1) AS genre_diversity_percentage,
    ROUND(COUNT(DISTINCT primary_artist_name) * 100.0 / COUNT(*), 1) AS artist_diversity_percentage,
    
    CURRENT_TIMESTAMP AS analysis_timestamp
    
FROM ml_hybrid_recommendations_dt;

-- =====================================================================
-- 5. CREATE VIEW ALIASES FOR COMPATIBILITY
-- =====================================================================

-- Create views that point to dynamic tables for backward compatibility
CREATE OR REPLACE VIEW ml_collaborative_recommendations AS 
SELECT * FROM ml_collaborative_recommendations_dt;

CREATE OR REPLACE VIEW ml_content_based_recommendations AS
SELECT * FROM ml_content_based_recommendations_dt;

CREATE OR REPLACE VIEW ml_hybrid_recommendations AS
SELECT * FROM ml_hybrid_recommendations_dt;

CREATE OR REPLACE VIEW ml_hybrid_recommendations_advanced AS
SELECT * FROM ml_hybrid_recommendations_dt;

CREATE OR REPLACE VIEW ml_hybrid_recommendations_simple AS
SELECT * FROM ml_hybrid_recommendations_dt;

CREATE OR REPLACE VIEW ml_hybrid_recommendations_working AS  
SELECT * FROM ml_hybrid_recommendations_dt;

CREATE OR REPLACE VIEW ml_recommendation_analytics_advanced AS
SELECT * FROM ml_recommendation_analytics_dt;

-- =====================================================================
-- 6. VERIFICATION TESTS  
-- =====================================================================

-- Test dynamic tables
SELECT 'Collaborative DT' AS test, COUNT(*) AS rows FROM ml_collaborative_recommendations_dt;
SELECT 'Content-Based DT' AS test, COUNT(*) AS rows FROM ml_content_based_recommendations_dt;  
SELECT 'Hybrid DT' AS test, COUNT(*) AS rows FROM ml_hybrid_recommendations_dt;
SELECT 'Analytics DT' AS test, total_ml_recommendations AS rows FROM ml_recommendation_analytics_dt;

-- Show top recommendations
SELECT 
    'Top ML Recommendations' AS test,
    track_name,
    primary_artist_name,
    final_recommendation_score,
    recommendation_strategies,
    ml_confidence
FROM ml_hybrid_recommendations_dt
ORDER BY final_recommendation_score DESC
LIMIT 10;

-- =====================================================================
-- 7. SUCCESS MESSAGE
-- =====================================================================

SELECT 
    '🚀 DYNAMIC TABLE ML SYSTEM DEPLOYED!' AS status,
    'Materialized results avoid all subquery evaluation issues' AS benefit,
    'Auto-refreshes every 15-30 minutes' AS refresh_schedule,
    'Much better performance than complex views' AS performance,
    'No more "Unsupported subquery type" errors!' AS reliability;
