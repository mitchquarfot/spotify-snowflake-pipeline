-- CREATE ML REGULAR TABLES - BACKUP APPROACH IF DYNAMIC TABLES HAVE ISSUES
-- Regular tables with manual refresh via stored procedures
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- =====================================================================
-- 1. CREATE REGULAR TABLES FOR ML RECOMMENDATIONS
-- =====================================================================

-- Collaborative filtering results table
CREATE OR REPLACE TABLE ml_collaborative_recommendations_tbl (
    track_id VARCHAR,
    track_name VARCHAR,
    primary_artist_name VARCHAR,
    primary_genre VARCHAR,
    album_name VARCHAR,
    track_popularity INTEGER,
    recommendation_score FLOAT,
    recommendation_strategy VARCHAR,
    affinity_score FLOAT,
    genre_similarity FLOAT,
    genre_track_rank INTEGER,
    generated_at TIMESTAMP
);

-- Content-based filtering results table  
CREATE OR REPLACE TABLE ml_content_based_recommendations_tbl (
    track_id VARCHAR,
    track_name VARCHAR,
    primary_artist_name VARCHAR,
    primary_genre VARCHAR,
    album_name VARCHAR,
    track_popularity INTEGER,
    recommendation_score FLOAT,
    recommendation_strategy VARCHAR,
    popularity_distance FLOAT,
    content_rank INTEGER,
    generated_at TIMESTAMP
);

-- Hybrid recommendations results table
CREATE OR REPLACE TABLE ml_hybrid_recommendations_tbl (
    track_id VARCHAR,
    track_name VARCHAR,
    primary_artist_name VARCHAR,
    primary_genre VARCHAR,
    album_name VARCHAR,
    track_popularity INTEGER,
    final_recommendation_score FLOAT,
    recommendation_strategies VARCHAR,
    recommendation_support INTEGER,
    ml_confidence VARCHAR,
    recommendation_reason VARCHAR,
    popularity_tier VARCHAR,
    playlist_position INTEGER,
    generated_at TIMESTAMP
);

-- Analytics table
CREATE OR REPLACE TABLE ml_recommendation_analytics_tbl (
    total_ml_recommendations INTEGER,
    unique_genres_recommended INTEGER,
    unique_artists_recommended INTEGER,
    avg_ml_score FLOAT,
    max_ml_score FLOAT,
    min_ml_score FLOAT,
    collaborative_only INTEGER,
    content_based_only INTEGER,
    multi_algorithm INTEGER,
    high_confidence_tracks INTEGER,
    medium_confidence_tracks INTEGER,
    single_algorithm_tracks INTEGER,
    multi_algorithm_percentage FLOAT,
    mainstream_hits INTEGER,
    popular_tracks INTEGER,
    rising_tracks INTEGER,
    hidden_gems INTEGER,
    deep_cuts INTEGER,
    genre_diversity_percentage FLOAT,
    artist_diversity_percentage FLOAT,
    analysis_timestamp TIMESTAMP
);

-- =====================================================================
-- 2. STORED PROCEDURE TO POPULATE COLLABORATIVE RECOMMENDATIONS
-- =====================================================================

CREATE OR REPLACE PROCEDURE refresh_collaborative_recommendations()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Clear existing data
    DELETE FROM ml_collaborative_recommendations_tbl;
    
    -- Populate with fresh recommendations
    INSERT INTO ml_collaborative_recommendations_tbl
    SELECT 
        tcf.track_id,
        tcf.track_name,
        tcf.primary_artist_name,
        tcf.primary_genre,
        tcf.album_name,
        tcf.track_popularity,
        
        -- Collaborative scoring: Combine user affinity + genre similarity  
        GREATEST(
            ugi.weighted_preference * 0.6 +     
            COALESCE(gsm.jaccard_similarity, 0.1) * 0.4,  
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
        
    WHERE tcf.user_play_count <= 1           
    AND ugi.weighted_preference > 0.1             
    AND tcf.track_popularity >= 10           
    AND tcf.primary_genre IS NOT NULL;
    
    RETURN 'Collaborative recommendations refreshed: ' || (SELECT COUNT(*) FROM ml_collaborative_recommendations_tbl) || ' tracks';
END;
$$;

-- =====================================================================
-- 3. STORED PROCEDURE TO POPULATE CONTENT-BASED RECOMMENDATIONS
-- =====================================================================

CREATE OR REPLACE PROCEDURE refresh_content_based_recommendations()
RETURNS STRING  
LANGUAGE SQL
AS
$$
BEGIN
    -- Clear existing data
    DELETE FROM ml_content_based_recommendations_tbl;
    
    -- Get user preferences first
    CREATE OR REPLACE TEMPORARY TABLE user_prefs AS
    SELECT 
        AVG(track_popularity) AS avg_popularity,
        AVG(track_duration_ms) AS avg_duration,
        MODE(era_category) AS preferred_era,
        MODE(popularity_tier) AS preferred_popularity_tier
    FROM ml_track_content_features
    WHERE user_play_count >= 2;
    
    -- Populate content-based recommendations
    INSERT INTO ml_content_based_recommendations_tbl  
    SELECT 
        tcf.track_id,
        tcf.track_name,
        tcf.primary_artist_name,
        tcf.primary_genre,
        tcf.album_name,
        tcf.track_popularity,
        
        -- Content-based similarity scoring
        GREATEST(
            1.0 - ABS(tcf.track_popularity - up.avg_popularity) / 100.0 * 0.3 +
            1.0 - ABS(tcf.track_duration_ms - up.avg_duration) / 300000.0 * 0.2 +
            CASE WHEN tcf.era_category = up.preferred_era THEN 0.3 ELSE 0.1 END +
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
    CROSS JOIN user_prefs up
    WHERE tcf.user_play_count <= 1          
    AND tcf.track_popularity >= 10           
    AND tcf.primary_genre IS NOT NULL;
    
    DROP TABLE user_prefs;
    
    RETURN 'Content-based recommendations refreshed: ' || (SELECT COUNT(*) FROM ml_content_based_recommendations_tbl) || ' tracks';
END;
$$;

-- =====================================================================
-- 4. STORED PROCEDURE TO CREATE HYBRID RECOMMENDATIONS
-- =====================================================================

CREATE OR REPLACE PROCEDURE refresh_hybrid_recommendations()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Clear existing data
    DELETE FROM ml_hybrid_recommendations_tbl;
    
    -- Create temporary combined recommendations
    CREATE OR REPLACE TEMPORARY TABLE all_recs AS
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
    FROM ml_collaborative_recommendations_tbl
    WHERE recommendation_score > 0.2
    
    UNION ALL
    
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
    FROM ml_content_based_recommendations_tbl
    WHERE recommendation_score > 0.3;
    
    -- Populate hybrid recommendations
    INSERT INTO ml_hybrid_recommendations_tbl
    SELECT 
        track_id,
        MAX(track_name) AS track_name,
        MAX(primary_artist_name) AS primary_artist_name,
        MAX(primary_genre) AS primary_genre,
        MAX(album_name) AS album_name,
        MAX(track_popularity) AS track_popularity,
        
        SUM(weighted_score) AS final_recommendation_score,
        CASE 
            WHEN COUNT(*) = 1 THEN MAX(strategy)
            ELSE 'hybrid_multi_strategy'
        END AS recommendation_strategies,
        COUNT(*) AS recommendation_support,
        
        CASE 
            WHEN COUNT(*) > 1 THEN 'High Confidence'
            WHEN SUM(weighted_score) > 0.6 THEN 'Medium Confidence'
            ELSE 'Single Algorithm'
        END AS ml_confidence,
        
        CASE 
            WHEN COUNT(*) > 1 THEN 'Multiple algorithms agree this matches your taste'
            WHEN MAX(strategy) = 'collaborative_filtering' THEN 'Users with similar taste love this track'
            WHEN MAX(strategy) = 'content_based_filtering' THEN 'Similar to tracks you already love'
            ELSE 'ML-powered personalized recommendation'
        END AS recommendation_reason,
        
        CASE 
            WHEN MAX(track_popularity) >= 80 THEN 'Mainstream Hit'
            WHEN MAX(track_popularity) >= 60 THEN 'Popular Track'
            WHEN MAX(track_popularity) >= 40 THEN 'Rising Track'
            WHEN MAX(track_popularity) >= 20 THEN 'Hidden Gem'
            ELSE 'Deep Cut Discovery'
        END AS popularity_tier,
        
        ROW_NUMBER() OVER (ORDER BY SUM(weighted_score) DESC) AS playlist_position,
        MAX(generated_at) AS generated_at
        
    FROM all_recs
    GROUP BY track_id
    HAVING SUM(weighted_score) > 0.25
    ORDER BY SUM(weighted_score) DESC;
    
    DROP TABLE all_recs;
    
    RETURN 'Hybrid recommendations refreshed: ' || (SELECT COUNT(*) FROM ml_hybrid_recommendations_tbl) || ' tracks';
END;
$$;

-- =====================================================================
-- 5. MASTER REFRESH PROCEDURE
-- =====================================================================

CREATE OR REPLACE PROCEDURE refresh_all_ml_recommendations()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    collab_result STRING;
    content_result STRING;  
    hybrid_result STRING;
BEGIN
    -- Refresh in sequence
    CALL refresh_collaborative_recommendations() INTO collab_result;
    CALL refresh_content_based_recommendations() INTO content_result;
    CALL refresh_hybrid_recommendations() INTO hybrid_result;
    
    -- Update analytics
    DELETE FROM ml_recommendation_analytics_tbl;
    INSERT INTO ml_recommendation_analytics_tbl
    SELECT 
        COUNT(*) AS total_ml_recommendations,
        COUNT(DISTINCT primary_genre) AS unique_genres_recommended,
        COUNT(DISTINCT primary_artist_name) AS unique_artists_recommended,
        ROUND(AVG(final_recommendation_score), 3) AS avg_ml_score,
        ROUND(MAX(final_recommendation_score), 3) AS max_ml_score,
        ROUND(MIN(final_recommendation_score), 3) AS min_ml_score,
        COUNT(CASE WHEN recommendation_strategies = 'collaborative_filtering' THEN 1 END) AS collaborative_only,
        COUNT(CASE WHEN recommendation_strategies = 'content_based_filtering' THEN 1 END) AS content_based_only,
        COUNT(CASE WHEN recommendation_strategies = 'hybrid_multi_strategy' THEN 1 END) AS multi_algorithm,
        COUNT(CASE WHEN ml_confidence = 'High Confidence' THEN 1 END) AS high_confidence_tracks,
        COUNT(CASE WHEN ml_confidence = 'Medium Confidence' THEN 1 END) AS medium_confidence_tracks,
        COUNT(CASE WHEN ml_confidence = 'Single Algorithm' THEN 1 END) AS single_algorithm_tracks,
        ROUND(COUNT(CASE WHEN recommendation_support > 1 THEN 1 END) * 100.0 / COUNT(*), 1) AS multi_algorithm_percentage,
        COUNT(CASE WHEN track_popularity >= 80 THEN 1 END) AS mainstream_hits,
        COUNT(CASE WHEN track_popularity BETWEEN 60 AND 79 THEN 1 END) AS popular_tracks,
        COUNT(CASE WHEN track_popularity BETWEEN 40 AND 59 THEN 1 END) AS rising_tracks,
        COUNT(CASE WHEN track_popularity BETWEEN 20 AND 39 THEN 1 END) AS hidden_gems,
        COUNT(CASE WHEN track_popularity < 20 THEN 1 END) AS deep_cuts,
        ROUND(COUNT(DISTINCT primary_genre) * 100.0 / COUNT(*), 1) AS genre_diversity_percentage,
        ROUND(COUNT(DISTINCT primary_artist_name) * 100.0 / COUNT(*), 1) AS artist_diversity_percentage,
        CURRENT_TIMESTAMP AS analysis_timestamp
    FROM ml_hybrid_recommendations_tbl;
    
    RETURN collab_result || '; ' || content_result || '; ' || hybrid_result;
END;
$$;

-- =====================================================================
-- 6. CREATE VIEW ALIASES POINTING TO TABLES
-- =====================================================================

CREATE OR REPLACE VIEW ml_collaborative_recommendations AS 
SELECT * FROM ml_collaborative_recommendations_tbl;

CREATE OR REPLACE VIEW ml_content_based_recommendations AS
SELECT * FROM ml_content_based_recommendations_tbl;

CREATE OR REPLACE VIEW ml_hybrid_recommendations AS
SELECT * FROM ml_hybrid_recommendations_tbl;

CREATE OR REPLACE VIEW ml_hybrid_recommendations_advanced AS
SELECT * FROM ml_hybrid_recommendations_tbl;

CREATE OR REPLACE VIEW ml_hybrid_recommendations_simple AS
SELECT * FROM ml_hybrid_recommendations_tbl;

CREATE OR REPLACE VIEW ml_hybrid_recommendations_working AS  
SELECT * FROM ml_hybrid_recommendations_tbl;

CREATE OR REPLACE VIEW ml_recommendation_analytics_advanced AS
SELECT * FROM ml_recommendation_analytics_tbl;

-- =====================================================================
-- 7. INITIAL POPULATION
-- =====================================================================

-- Populate all tables initially
CALL refresh_all_ml_recommendations();

-- =====================================================================
-- 8. VERIFICATION
-- =====================================================================

SELECT 'Table-Based ML System' AS approach, COUNT(*) AS recommendations 
FROM ml_hybrid_recommendations_tbl;

SELECT 'Sample Recommendations' AS test,
       track_name, primary_artist_name, final_recommendation_score, ml_confidence
FROM ml_hybrid_recommendations_tbl  
ORDER BY final_recommendation_score DESC
LIMIT 5;

-- =====================================================================
-- 9. SUCCESS MESSAGE
-- =====================================================================

SELECT 
    '📊 TABLE-BASED ML SYSTEM DEPLOYED!' AS status,
    'Uses regular tables with stored procedure refresh' AS approach,
    'No subquery evaluation issues' AS reliability,
    'Call refresh_all_ml_recommendations() to update' AS refresh_method;
