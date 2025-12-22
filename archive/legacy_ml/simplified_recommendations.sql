-- Simplified recommendation views with relaxed filtering
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- Simplified Collaborative Filtering (relaxed thresholds)
CREATE OR REPLACE VIEW ml_collaborative_recommendations_simple AS
WITH user_profile AS (
    SELECT 
        PRIMARY_GENRE,
        weighted_preference,
        preference_strength
    FROM ml_user_genre_interactions
    WHERE weighted_preference > 0.5  -- Lowered from 1.0
),
similar_genres AS (
    SELECT 
        gsm.to_genre AS recommended_genre,
        AVG(gsm.similarity_score * up.weighted_preference) AS collaborative_score,
        COUNT(*) AS supporting_genres
    FROM ml_genre_similarity_matrix gsm
    JOIN user_profile up ON gsm.from_genre = up.PRIMARY_GENRE
    WHERE gsm.similarity_score > 0.1  -- Lowered from 0.3
    AND gsm.to_genre NOT IN (SELECT PRIMARY_GENRE FROM user_profile)
    GROUP BY gsm.to_genre
    HAVING COUNT(*) >= 1  -- Lowered from 2
),
genre_tracks AS (
    SELECT 
        sg.recommended_genre,
        sg.collaborative_score,
        tcf.TRACK_ID,
        tcf.TRACK_NAME,
        tcf.PRIMARY_ARTIST_NAME,
        tcf.ALBUM_NAME,
        tcf.TRACK_POPULARITY,
        tcf.popularity_normalized,
        tcf.freshness_score,
        tcf.user_play_count,
        ROW_NUMBER() OVER (
            PARTITION BY sg.recommended_genre 
            ORDER BY tcf.TRACK_POPULARITY DESC, tcf.freshness_score DESC, RANDOM()
        ) AS genre_track_rank
    FROM similar_genres sg
    JOIN ml_track_content_features tcf ON sg.recommended_genre = tcf.PRIMARY_GENRE
    WHERE tcf.user_play_count <= 1  -- Changed from = 0 to allow some replays
)
SELECT 
    TRACK_ID,
    TRACK_NAME,
    PRIMARY_ARTIST_NAME,
    recommended_genre AS PRIMARY_GENRE,
    ALBUM_NAME,
    TRACK_POPULARITY,
    collaborative_score,
    (collaborative_score * 0.6 + 
     popularity_normalized * 0.25 + 
     freshness_score * 0.15) AS recommendation_score,
    'collaborative_filtering' AS recommendation_strategy,
    genre_track_rank,
    CURRENT_TIMESTAMP AS generated_at
FROM genre_tracks
WHERE genre_track_rank <= 5  -- Increased from 3
ORDER BY recommendation_score DESC
LIMIT 50;

-- Simplified Content-Based Filtering
CREATE OR REPLACE VIEW ml_content_based_recommendations_simple AS
WITH user_favorite_tracks AS (
    SELECT 
        TRACK_ID,
        TRACK_NAME,
        PRIMARY_ARTIST_NAME,
        PRIMARY_GENRE,
        popularity_normalized,
        duration_normalized,
        popularity_tier,
        duration_category,
        era_category,
        user_play_count
    FROM ml_track_content_features
    WHERE user_play_count >= 1  -- Lowered from 2
    ORDER BY user_play_count DESC, popularity_normalized DESC
    LIMIT 20  -- Increased from 10
),
content_candidates AS (
    SELECT 
        cand.TRACK_ID,
        cand.TRACK_NAME,
        cand.PRIMARY_ARTIST_NAME,
        cand.PRIMARY_GENRE,
        cand.ALBUM_NAME,
        cand.TRACK_POPULARITY,
        cand.user_play_count,
        fav.TRACK_ID AS seed_TRACK_ID,
        -- Simplified content similarity
        (
            CASE WHEN cand.PRIMARY_GENRE = fav.PRIMARY_GENRE THEN 1.0 ELSE 0.3 END * 0.4 +
            (1 - ABS(cand.popularity_normalized - fav.popularity_normalized)) * 0.3 +
            (1 - ABS(cand.duration_normalized - fav.duration_normalized)) * 0.3
        ) AS content_similarity_score
    FROM ml_track_content_features cand
    CROSS JOIN user_favorite_tracks fav
    WHERE cand.TRACK_ID != fav.TRACK_ID
    AND cand.user_play_count <= 1  -- Changed from = 0
),
ranked_candidates AS (
    SELECT 
        TRACK_ID,
        TRACK_NAME,
        PRIMARY_ARTIST_NAME,
        PRIMARY_GENRE,
        ALBUM_NAME,
        TRACK_POPULARITY,
        AVG(content_similarity_score) AS avg_similarity,
        MAX(content_similarity_score) AS max_similarity,
        COUNT(*) AS similarity_support,
        (AVG(content_similarity_score) * 0.7 + MAX(content_similarity_score) * 0.3) AS content_score,
        ROW_NUMBER() OVER (ORDER BY 
            (AVG(content_similarity_score) * 0.7 + MAX(content_similarity_score) * 0.3) DESC
        ) AS content_rank
    FROM content_candidates
    WHERE content_similarity_score > 0.3  -- Lowered from 0.5
    GROUP BY TRACK_ID, TRACK_NAME, PRIMARY_ARTIST_NAME, PRIMARY_GENRE, ALBUM_NAME, TRACK_POPULARITY
    HAVING COUNT(*) >= 1  -- Lowered from 2
)
SELECT 
    TRACK_ID,
    TRACK_NAME,
    PRIMARY_ARTIST_NAME,
    PRIMARY_GENRE,
    ALBUM_NAME,
    TRACK_POPULARITY,
    content_score AS recommendation_score,
    'content_based_filtering' AS recommendation_strategy,
    content_rank,
    avg_similarity,
    max_similarity,
    similarity_support,
    CURRENT_TIMESTAMP AS generated_at
FROM ranked_candidates
ORDER BY content_score DESC
LIMIT 50;

-- Simplified Temporal Recommendations  
CREATE OR REPLACE VIEW ml_temporal_recommendations_simple AS
WITH current_context AS (
    SELECT 
        HOUR(CONVERT_TIMEZONE('America/Denver', CURRENT_TIMESTAMP())) AS current_hour,
        CASE WHEN DAYOFWEEK(CURRENT_DATE()) IN (0,6) THEN TRUE ELSE FALSE END AS is_current_weekend
),
relevant_patterns AS (
    SELECT 
        tp.PRIMARY_GENRE,
        tp.denver_hour,
        tp.IS_WEEKEND,
        tp.hour_genre_probability,
        tp.temporal_recommendation_score,
        tp.temporal_strength,
        cc.current_hour,
        cc.is_current_weekend,
        -- More relaxed temporal relevance
        CASE 
            WHEN tp.denver_hour = cc.current_hour AND tp.IS_WEEKEND = cc.is_current_weekend THEN 1.0
            WHEN ABS(tp.denver_hour - cc.current_hour) <= 3 AND tp.IS_WEEKEND = cc.is_current_weekend THEN 0.8  -- Increased from 1
            WHEN tp.IS_WEEKEND = cc.is_current_weekend THEN 0.6
            WHEN ABS(tp.denver_hour - cc.current_hour) <= 4 THEN 0.4  -- Increased from 2
            ELSE 0.2
        END AS temporal_relevance
    FROM ml_temporal_patterns tp
    CROSS JOIN current_context cc
    WHERE tp.temporal_strength IN ('strong', 'moderate', 'weak')  -- Added 'weak'
),
temporal_tracks AS (
    SELECT 
        rp.PRIMARY_GENRE,
        rp.temporal_relevance,
        rp.temporal_recommendation_score,
        tcf.TRACK_ID,
        tcf.TRACK_NAME,
        tcf.PRIMARY_ARTIST_NAME,
        tcf.ALBUM_NAME,
        tcf.TRACK_POPULARITY,
        tcf.freshness_score,
        tcf.user_play_count,
        (rp.temporal_relevance * 0.5 + 
         rp.hour_genre_probability * 0.3 + 
         tcf.popularity_normalized * 0.2) AS temporal_score,
        ROW_NUMBER() OVER (
            PARTITION BY rp.PRIMARY_GENRE 
            ORDER BY 
                (rp.temporal_relevance * 0.5 + 
                 rp.hour_genre_probability * 0.3 + 
                 tcf.popularity_normalized * 0.2) DESC,
                RANDOM()
        ) AS genre_temporal_rank
    FROM relevant_patterns rp
    JOIN ml_track_content_features tcf ON rp.PRIMARY_GENRE = tcf.PRIMARY_GENRE
    WHERE tcf.user_play_count <= 1  -- Changed from = 0
)
SELECT 
    TRACK_ID,
    TRACK_NAME,
    PRIMARY_ARTIST_NAME,
    PRIMARY_GENRE,
    ALBUM_NAME,
    TRACK_POPULARITY,
    temporal_score AS recommendation_score,
    'temporal_patterns' AS recommendation_strategy,
    temporal_relevance,
    genre_temporal_rank,
    CURRENT_TIMESTAMP AS generated_at
FROM temporal_tracks
WHERE genre_temporal_rank <= 5  -- Increased from 2
AND temporal_relevance >= 0.2  -- Lowered from 0.4
ORDER BY temporal_score DESC
LIMIT 50;

-- Simplified Discovery Recommendations (avoids complex subquery)
CREATE OR REPLACE VIEW ml_discovery_recommendations_simple AS
SELECT 
    tcf.TRACK_ID,
    tcf.TRACK_NAME,
    tcf.PRIMARY_ARTIST_NAME,
    tcf.PRIMARY_ARTIST_ID,
    tcf.PRIMARY_GENRE,
    tcf.ALBUM_NAME,
    tcf.TRACK_POPULARITY,
    tcf.era_category,
    
    -- Simple discovery scoring
    CASE 
        WHEN tcf.PRIMARY_GENRE NOT IN (
            SELECT PRIMARY_GENRE FROM ml_user_genre_interactions WHERE weighted_preference > 0.3
        ) THEN 0.8
        ELSE 0.2
    END AS genre_novelty_score,
    
    CASE 
        WHEN tcf.TRACK_POPULARITY BETWEEN 20 AND 70 THEN 
            (70 - ABS(tcf.TRACK_POPULARITY - 45)) / 50.0
        ELSE 0.3
    END AS hidden_gem_score,
    
    -- Combined simple discovery score
    (
        CASE 
            WHEN tcf.PRIMARY_GENRE NOT IN (
                SELECT PRIMARY_GENRE FROM ml_user_genre_interactions WHERE weighted_preference > 0.3
            ) THEN 0.8
            ELSE 0.2
        END * 0.6 +
        CASE 
            WHEN tcf.TRACK_POPULARITY BETWEEN 20 AND 70 THEN 
                (70 - ABS(tcf.TRACK_POPULARITY - 45)) / 50.0
            ELSE 0.3
        END * 0.4
    ) AS discovery_score,
    
    'discovery_exploration' AS recommendation_strategy,
    ROW_NUMBER() OVER (ORDER BY 
        (
            CASE 
                WHEN tcf.PRIMARY_GENRE NOT IN (
                    SELECT PRIMARY_GENRE FROM ml_user_genre_interactions WHERE weighted_preference > 0.3
                ) THEN 0.8
                ELSE 0.2
            END * 0.6 +
            CASE 
                WHEN tcf.TRACK_POPULARITY BETWEEN 20 AND 70 THEN 
                    (70 - ABS(tcf.TRACK_POPULARITY - 45)) / 50.0
                ELSE 0.3
            END * 0.4
        ) DESC,
        RANDOM()
    ) AS discovery_rank,
    CURRENT_TIMESTAMP AS generated_at
    
FROM ml_track_content_features tcf
WHERE tcf.user_play_count = 0
AND tcf.TRACK_POPULARITY > 5  -- Lowered from 10
AND tcf.TRACK_POPULARITY < 90  -- Added upper limit
ORDER BY discovery_score DESC
LIMIT 50;
