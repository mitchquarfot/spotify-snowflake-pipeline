-- =====================================================================
-- SPOTIFY ML RECOMMENDATION ENGINE - SNOWFLAKE SQL IMPLEMENTATION
-- Advanced recommendation system using Snowflake ML capabilities
-- Supports model registry integration and hybrid recommendation strategies
-- =====================================================================

USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- =====================================================================
-- 1. MODEL TRAINING DATA PREPARATION VIEWS
-- =====================================================================

-- User-Genre Interaction Matrix for Collaborative Filtering
CREATE OR REPLACE VIEW ml_user_genre_interactions AS
SELECT
    'user_1' AS user_id,  -- Single user for now, expandable for multi-user
    PRIMARY_GENRE,
    COUNT(*) AS play_count,
    AVG(TRACK_POPULARITY) AS avg_popularity,
    SUM(CASE WHEN DENVER_DATE >= DATEADD('days', -30, CURRENT_DATE) 
             THEN 1 ELSE 0 END) AS recent_plays,
    SUM(CASE WHEN DENVER_DATE >= DATEADD('days', -7, CURRENT_DATE) 
             THEN 1 ELSE 0 END) AS very_recent_plays,
    
    -- Recency weighting using exponential decay
    EXP(-AVG(DATEDIFF('days', DENVER_DATE, CURRENT_DATE)) / 30.0) AS recency_weight,
    
    -- Temporal preferences
    AVG(DENVER_HOUR) AS avg_listening_hour,
    AVG(CASE WHEN IS_WEEKEND THEN 1 ELSE 0 END) AS weekend_preference,
    
    -- Artist diversity in genre
    COUNT(DISTINCT PRIMARY_ARTIST_ID) AS artist_diversity_in_genre,
    
    -- Track characteristics
    AVG(TRACK_DURATION_MS) / 1000.0 / 60.0 AS avg_duration_minutes,
    STDDEV(TRACK_POPULARITY) AS popularity_variance,
    
    -- Engineered preference features
    COUNT(*) * EXP(-AVG(DATEDIFF('days', DENVER_DATE, CURRENT_DATE)) / 30.0) AS weighted_preference,
    CASE WHEN SUM(CASE WHEN DENVER_DATE >= DATEADD('days', -30, CURRENT_DATE) THEN 1 ELSE 0 END) > 0 
         THEN 1 ELSE 0 END AS is_current_preference,
    CASE WHEN SUM(CASE WHEN DENVER_DATE >= DATEADD('days', -7, CURRENT_DATE) THEN 1 ELSE 0 END) > 0 
         THEN 1 ELSE 0 END AS is_very_recent_preference,
    LN(COUNT(*) + 1) AS log_play_count,
    
    -- Preference strength categorization
    CASE 
        WHEN COUNT(*) >= 50 THEN 'high'
        WHEN COUNT(*) >= 20 THEN 'medium'
        WHEN COUNT(*) >= 10 THEN 'low'
        ELSE 'minimal'
    END AS preference_strength
    
FROM spotify_analytics.medallion_arch.silver_listening_enriched
WHERE DENVER_DATE >= DATEADD('days', -180, CURRENT_DATE)  -- 6 months of data
GROUP BY user_id, PRIMARY_GENRE
HAVING COUNT(*) >= 3;  -- Minimum plays for reliability

-- Content Features for Tracks
CREATE OR REPLACE VIEW ml_track_content_features AS
WITH date_parsed_tracks AS (
    SELECT 
        TRACK_ID,
        TRACK_NAME,
        PRIMARY_ARTIST_ID,
        PRIMARY_ARTIST_NAME,
        PRIMARY_GENRE,
        TRACK_POPULARITY,
        TRACK_DURATION_MS,
        ALBUM_RELEASE_DATE,
        ALBUM_NAME,
        ALBUM_TYPE,
        
        -- Robust date parsing: handles both "YYYY" and "YYYY-MM-DD" formats
        COALESCE(
            TRY_CAST(SUBSTRING(ALBUM_RELEASE_DATE, 1, 4) AS INTEGER),
            YEAR(TRY_TO_DATE(ALBUM_RELEASE_DATE, 'YYYY-MM-DD')),
            2000  -- Default fallback for invalid dates
        ) AS release_year
    FROM spotify_analytics.medallion_arch.silver_listening_enriched
),
track_base_features AS (
    SELECT 
        dpt.TRACK_ID,
        dpt.TRACK_NAME,
        dpt.PRIMARY_ARTIST_ID,
        dpt.PRIMARY_ARTIST_NAME,
        dpt.PRIMARY_GENRE,
        dpt.TRACK_POPULARITY,
        dpt.TRACK_DURATION_MS,
        dpt.ALBUM_RELEASE_DATE,
        dpt.ALBUM_NAME,
        dpt.ALBUM_TYPE,
        dpt.release_year,
        
        -- Era categorization based on parsed release year
        CASE 
            WHEN dpt.release_year >= 2020 THEN 'current'
            WHEN dpt.release_year >= 2015 THEN 'recent'
            WHEN dpt.release_year >= 2010 THEN '2010s'
            WHEN dpt.release_year >= 2000 THEN '2000s'
            WHEN dpt.release_year >= 1990 THEN '90s'
            ELSE 'classic'
        END AS era_category,
        
        -- Popularity tiers (0-4 scale)
        CASE 
            WHEN dpt.TRACK_POPULARITY >= 80 THEN 4  -- Mainstream
            WHEN dpt.TRACK_POPULARITY >= 60 THEN 3  -- Popular
            WHEN dpt.TRACK_POPULARITY >= 40 THEN 2  -- Moderate
            WHEN dpt.TRACK_POPULARITY >= 20 THEN 1  -- Niche
            ELSE 0  -- Underground
        END AS popularity_tier,
        
        -- Duration categories (0-5 scale)
        CASE
            WHEN dpt.TRACK_DURATION_MS < 120000 THEN 0  -- Very short (< 2min)
            WHEN dpt.TRACK_DURATION_MS < 180000 THEN 1  -- Short (2-3min)
            WHEN dpt.TRACK_DURATION_MS < 240000 THEN 2  -- Normal (3-4min)
            WHEN dpt.TRACK_DURATION_MS < 300000 THEN 3  -- Long (4-5min)
            WHEN dpt.TRACK_DURATION_MS < 420000 THEN 4  -- Very long (5-7min)
            ELSE 5  -- Extended (>7min)
        END AS duration_category,
        
        -- User engagement with this track
        COUNT(*) OVER (PARTITION BY dpt.TRACK_ID) AS user_play_count,
        MAX(sle.DENVER_DATE) OVER (PARTITION BY dpt.TRACK_ID) AS last_played_date
    FROM date_parsed_tracks dpt
    JOIN spotify_analytics.medallion_arch.silver_listening_enriched sle 
        ON dpt.TRACK_ID = sle.TRACK_ID
    WHERE sle.DENVER_DATE >= DATEADD('days', -90, CURRENT_DATE)
),
track_with_engagement AS (
    SELECT 
        *,
        -- Genre rank for this track (now calculated after window functions)
        ROW_NUMBER() OVER (
            PARTITION BY PRIMARY_GENRE 
            ORDER BY TRACK_POPULARITY DESC, user_play_count DESC
        ) AS genre_rank
    FROM track_base_features
),
artist_features AS (
    SELECT 
        PRIMARY_ARTIST_ID,
        PRIMARY_ARTIST_NAME,
        COUNT(DISTINCT PRIMARY_GENRE) AS artist_genre_diversity,
        AVG(TRACK_POPULARITY) AS artist_avg_popularity,
        COUNT(DISTINCT TRACK_ID) AS artist_track_count,
        AVG(TRACK_DURATION_MS) AS artist_avg_duration,
        COUNT(*) AS artist_total_plays
    FROM spotify_analytics.medallion_arch.silver_listening_enriched
    WHERE DENVER_DATE >= DATEADD('days', -90, CURRENT_DATE)
    GROUP BY PRIMARY_ARTIST_ID, PRIMARY_ARTIST_NAME
),
genre_embeddings AS (
    SELECT 
        PRIMARY_GENRE,
        AVG(TRACK_POPULARITY) AS genre_avg_popularity,
        AVG(TRACK_DURATION_MS) AS genre_avg_duration,
        COUNT(DISTINCT PRIMARY_ARTIST_ID) AS genre_artist_count,
        COUNT(DISTINCT TRACK_ID) AS genre_track_count,
        STDDEV(TRACK_POPULARITY) AS genre_popularity_variance,
        AVG(release_year) AS genre_avg_release_year
    FROM date_parsed_tracks
    WHERE TRACK_ID IN (
        SELECT TRACK_ID FROM spotify_analytics.medallion_arch.silver_listening_enriched 
        WHERE DENVER_DATE >= DATEADD('days', -90, CURRENT_DATE)
    )
    GROUP BY PRIMARY_GENRE
)
SELECT 
    tf.*,
    af.artist_genre_diversity,
    af.artist_avg_popularity,
    af.artist_track_count,
    af.artist_avg_duration,
    af.artist_total_plays,
    ge.genre_avg_popularity,
    ge.genre_avg_duration,
    ge.genre_artist_count,
    ge.genre_track_count,
    ge.genre_popularity_variance,
    ge.genre_avg_release_year,
    
    -- Composite features for ML
    tf.TRACK_POPULARITY - ge.genre_avg_popularity AS popularity_vs_genre_avg,
    tf.TRACK_DURATION_MS - ge.genre_avg_duration AS duration_vs_genre_avg,
    af.artist_avg_popularity - ge.genre_avg_popularity AS artist_vs_genre_popularity,
    
    -- Freshness and engagement scores
    DATEDIFF('days', tf.last_played_date, CURRENT_DATE) AS days_since_last_played,
    EXP(-DATEDIFF('days', tf.last_played_date, CURRENT_DATE) / 30.0) AS freshness_score,
    
    -- Normalized features for ML (0-1 scale)
    tf.TRACK_POPULARITY / 100.0 AS popularity_normalized,
    tf.user_play_count / GREATEST(MAX(tf.user_play_count) OVER(), 1) AS play_count_normalized,
    (tf.TRACK_DURATION_MS - 60000) / (600000 - 60000) AS duration_normalized  -- 1min to 10min scale
    
FROM track_with_engagement tf
JOIN artist_features af ON tf.PRIMARY_ARTIST_ID = af.PRIMARY_ARTIST_ID
JOIN genre_embeddings ge ON tf.PRIMARY_GENRE = ge.PRIMARY_GENRE
WHERE tf.genre_rank <= 50;  -- Top 50 tracks per genre to keep dataset manageable

-- Temporal Listening Patterns for Temporal Recommendations
CREATE OR REPLACE VIEW ml_temporal_patterns AS
WITH temporal_base AS (
    SELECT 
        PRIMARY_GENRE,
        denver_hour,
        IS_WEEKEND,
        COUNT(*) AS play_count,
        AVG(TRACK_POPULARITY) AS avg_popularity,
        COUNT(DISTINCT PRIMARY_ARTIST_ID) AS unique_artists,
        AVG(TRACK_DURATION_MS) / 1000.0 / 60.0 AS avg_duration_minutes,
        
        -- Calculate probability of this genre at this time
        COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY denver_hour, IS_WEEKEND) AS hour_genre_probability,
        
        -- Calculate genre's temporal preference
        COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY PRIMARY_GENRE) AS genre_time_preference
        
    FROM spotify_analytics.medallion_arch.silver_listening_enriched
    WHERE DENVER_DATE >= DATEADD('days', -90, CURRENT_DATE)
    GROUP BY PRIMARY_GENRE, denver_hour, IS_WEEKEND
    HAVING COUNT(*) >= 2  -- Minimum plays for pattern reliability
),
genre_temporal_summary AS (
    SELECT 
        PRIMARY_GENRE,
        AVG(denver_hour) AS avg_listening_hour,
        STDDEV(denver_hour) AS hour_variance,
        AVG(CASE WHEN IS_WEEKEND THEN 1 ELSE 0 END) AS weekend_preference,
        COUNT(DISTINCT denver_hour) AS hour_diversity,
        SUM(play_count) AS total_genre_plays
    FROM temporal_base
    GROUP BY PRIMARY_GENRE
)
SELECT 
    tp.*,
    gts.avg_listening_hour,
    gts.hour_variance,
    gts.weekend_preference,
    gts.hour_diversity,
    gts.total_genre_plays,
    
    -- Temporal strength indicators
    CASE 
        WHEN tp.hour_genre_probability > 0.3 THEN 'strong'
        WHEN tp.hour_genre_probability > 0.1 THEN 'moderate'
        ELSE 'weak'
    END AS temporal_strength,
    
    -- Time-based recommendations score
    tp.hour_genre_probability * tp.play_count AS temporal_recommendation_score
    
FROM temporal_base tp
JOIN genre_temporal_summary gts ON tp.PRIMARY_GENRE = gts.PRIMARY_GENRE
ORDER BY tp.denver_hour, tp.IS_WEEKEND, tp.play_count DESC;

-- =====================================================================
-- 2. SIMILARITY AND RELATIONSHIP MATRICES
-- =====================================================================

-- Genre Similarity Matrix using Co-occurrence Analysis
CREATE OR REPLACE VIEW ml_genre_similarity_matrix AS
WITH user_sessions AS (
    -- Define sessions as tracks played within 1 hour of each other
    SELECT 
        FLOOR(DATEDIFF('minute', '1970-01-01'::TIMESTAMP, DENVER_TIMESTAMP) / 60) AS session_id,
        PRIMARY_GENRE,
        COUNT(*) AS tracks_in_session
    FROM spotify_analytics.medallion_arch.silver_listening_enriched
    WHERE DENVER_DATE >= DATEADD('days', -90, CURRENT_DATE)
    GROUP BY session_id, PRIMARY_GENRE
),
genre_cooccurrence AS (
    SELECT 
        s1.PRIMARY_GENRE AS genre_1,
        s2.PRIMARY_GENRE AS genre_2,
        COUNT(*) AS sessions_together,
        AVG(s1.tracks_in_session + s2.tracks_in_session) AS avg_session_intensity,
        
        -- Jaccard similarity coefficient
        COUNT(*) / (
            COUNT(DISTINCT s1.session_id) + 
            COUNT(DISTINCT s2.session_id) - 
            COUNT(*)
        )::FLOAT AS jaccard_similarity
        
    FROM user_sessions s1
    JOIN user_sessions s2 ON s1.session_id = s2.session_id
    WHERE s1.PRIMARY_GENRE < s2.PRIMARY_GENRE  -- Avoid duplicates and self-pairs
    GROUP BY s1.PRIMARY_GENRE, s2.PRIMARY_GENRE
    HAVING COUNT(*) >= 3  -- Minimum co-occurrences for reliability
),
genre_metrics AS (
    SELECT 
        PRIMARY_GENRE,
        COUNT(DISTINCT session_id) AS total_sessions,
        SUM(tracks_in_session) AS total_tracks,
        AVG(tracks_in_session) AS avg_tracks_per_session
    FROM user_sessions
    GROUP BY PRIMARY_GENRE
),
similarity_calculations AS (
    SELECT 
        gc.genre_1,
        gc.genre_2,
        gc.sessions_together,
        gc.avg_session_intensity,
        gc.jaccard_similarity,
        gm1.total_sessions AS genre_1_sessions,
        gm1.total_tracks AS genre_1_total_tracks,
        gm2.total_sessions AS genre_2_sessions,
        gm2.total_tracks AS genre_2_total_tracks,
        
        -- Normalized similarity scores
        gc.sessions_together / LEAST(gm1.total_sessions, gm2.total_sessions)::FLOAT AS overlap_ratio
        
    FROM genre_cooccurrence gc
    JOIN genre_metrics gm1 ON gc.genre_1 = gm1.PRIMARY_GENRE
    JOIN genre_metrics gm2 ON gc.genre_2 = gm2.PRIMARY_GENRE
    
    UNION ALL
    
    -- Add reverse relationships for symmetric matrix
    SELECT 
        gc.genre_2 AS genre_1,  -- Swapped
        gc.genre_1 AS genre_2,  -- Swapped
        gc.sessions_together,
        gc.avg_session_intensity,
        gc.jaccard_similarity,
        gm2.total_sessions AS genre_1_sessions,
        gm2.total_tracks AS genre_1_total_tracks,
        gm1.total_sessions AS genre_2_sessions,
        gm1.total_tracks AS genre_2_total_tracks,
        
        -- Normalized similarity scores
        gc.sessions_together / LEAST(gm1.total_sessions, gm2.total_sessions)::FLOAT AS overlap_ratio
        
    FROM genre_cooccurrence gc
    JOIN genre_metrics gm1 ON gc.genre_1 = gm1.PRIMARY_GENRE
    JOIN genre_metrics gm2 ON gc.genre_2 = gm2.PRIMARY_GENRE
)
SELECT 
    genre_1,
    genre_2,
    sessions_together,
    avg_session_intensity,
    jaccard_similarity,
    genre_1_sessions,
    genre_1_total_tracks,
    genre_2_sessions,
    genre_2_total_tracks,
    overlap_ratio,
    
    -- Combined similarity score
    (jaccard_similarity * 0.6 + overlap_ratio * 0.4) AS combined_similarity_score,
    
    -- Bidirectional relationships (for easy lookup)
    genre_1 AS from_genre,
    genre_2 AS to_genre,
    (jaccard_similarity * 0.6 + overlap_ratio * 0.4) AS similarity_score
    
FROM similarity_calculations

ORDER BY similarity_score DESC;

-- Artist Similarity Network
CREATE OR REPLACE VIEW ml_artist_similarity_network AS
WITH artist_genre_overlap AS (
    SELECT 
        a1.PRIMARY_ARTIST_ID AS artist_1_id,
        a1.PRIMARY_ARTIST_NAME AS artist_1_name,
        a2.PRIMARY_ARTIST_ID AS artist_2_id,
        a2.PRIMARY_ARTIST_NAME AS artist_2_name,
        
        -- Calculate genre overlap using simplified approach
        CASE 
            WHEN a1.PRIMARY_GENRE = a2.PRIMARY_GENRE THEN ARRAY_CONSTRUCT(a1.PRIMARY_GENRE)
            ELSE ARRAY_CONSTRUCT()
        END AS shared_genres,
        
        COUNT(DISTINCT a1.PRIMARY_GENRE) OVER (PARTITION BY a1.PRIMARY_ARTIST_ID) AS artist_1_genre_count,
        COUNT(DISTINCT a2.PRIMARY_GENRE) OVER (PARTITION BY a2.PRIMARY_ARTIST_ID) AS artist_2_genre_count,
        
        AVG(a1.TRACK_POPULARITY) OVER (PARTITION BY a1.PRIMARY_ARTIST_ID) AS artist_1_avg_popularity,
        AVG(a2.TRACK_POPULARITY) OVER (PARTITION BY a2.PRIMARY_ARTIST_ID) AS artist_2_avg_popularity
        
    FROM spotify_analytics.medallion_arch.silver_listening_enriched a1
    JOIN spotify_analytics.medallion_arch.silver_listening_enriched a2 
        ON a1.PRIMARY_ARTIST_ID < a2.PRIMARY_ARTIST_ID  -- Avoid duplicates
    WHERE a1.DENVER_DATE >= DATEADD('days', -90, CURRENT_DATE)
    AND a2.DENVER_DATE >= DATEADD('days', -90, CURRENT_DATE)
)
SELECT DISTINCT
    artist_1_id,
    artist_1_name,
    artist_2_id,
    artist_2_name,
    shared_genres,
    ARRAY_SIZE(shared_genres) AS shared_genre_count,
    artist_1_genre_count,
    artist_2_genre_count,
    
    -- Jaccard similarity for genres
    ARRAY_SIZE(shared_genres) / 
    (artist_1_genre_count + artist_2_genre_count - ARRAY_SIZE(shared_genres))::FLOAT AS genre_jaccard_similarity,
    
    -- Popularity similarity (1 - normalized difference)
    1 - ABS(artist_1_avg_popularity - artist_2_avg_popularity) / 100.0 AS popularity_similarity,
    
    -- Combined artist similarity score
    (ARRAY_SIZE(shared_genres) / 
     (artist_1_genre_count + artist_2_genre_count - ARRAY_SIZE(shared_genres))::FLOAT * 0.7 +
     (1 - ABS(artist_1_avg_popularity - artist_2_avg_popularity) / 100.0) * 0.3
    ) AS artist_similarity_score
    
FROM artist_genre_overlap
WHERE ARRAY_SIZE(shared_genres) > 0  -- Must have at least one shared genre
ORDER BY artist_similarity_score DESC;

-- =====================================================================
-- 3. RECOMMENDATION GENERATION FUNCTIONS
-- =====================================================================

-- Collaborative Filtering Recommendations
CREATE OR REPLACE VIEW ml_collaborative_recommendations AS
WITH user_profile AS (
    SELECT 
        PRIMARY_GENRE,
        weighted_preference,
        preference_strength,
        is_current_preference,
        recency_weight
    FROM ml_user_genre_interactions
    WHERE weighted_preference > 1.0  -- Only consider significant preferences
),
similar_genres AS (
    SELECT 
        gsm.to_genre AS recommended_genre,
        AVG(gsm.similarity_score * up.weighted_preference) AS collaborative_score,
        COUNT(*) AS supporting_genres
    FROM ml_genre_similarity_matrix gsm
    JOIN user_profile up ON gsm.from_genre = up.PRIMARY_GENRE
    WHERE gsm.similarity_score > 0.3  -- Minimum similarity threshold
    AND gsm.to_genre NOT IN (SELECT PRIMARY_GENRE FROM user_profile)  -- Exclude known genres
    GROUP BY gsm.to_genre
    HAVING COUNT(*) >= 2  -- Must be similar to at least 2 user genres
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
        
        -- Rank tracks within each recommended genre
        ROW_NUMBER() OVER (
            PARTITION BY sg.recommended_genre 
            ORDER BY tcf.TRACK_POPULARITY DESC, tcf.freshness_score DESC, RANDOM()
        ) AS genre_track_rank
    FROM similar_genres sg
    JOIN ml_track_content_features tcf ON sg.recommended_genre = tcf.PRIMARY_GENRE
    WHERE tcf.user_play_count = 0  -- Only recommend new tracks
)
SELECT 
    TRACK_ID,
    TRACK_NAME,
    PRIMARY_ARTIST_NAME,
    recommended_genre AS PRIMARY_GENRE,
    ALBUM_NAME,
    TRACK_POPULARITY,
    collaborative_score,
    
    -- Final recommendation score combining multiple factors
    (collaborative_score * 0.6 + 
     popularity_normalized * 0.25 + 
     freshness_score * 0.15) AS recommendation_score,
    
    'collaborative_filtering' AS recommendation_strategy,
    genre_track_rank,
    CURRENT_TIMESTAMP AS generated_at
    
FROM genre_tracks
WHERE genre_track_rank <= 3  -- Top 3 tracks per recommended genre
ORDER BY recommendation_score DESC;

-- Content-Based Recommendations
CREATE OR REPLACE VIEW ml_content_based_recommendations AS
WITH user_favorite_tracks_base AS (
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
        user_play_count,
        freshness_score,
        -- Rank tracks by engagement to select top favorites
        ROW_NUMBER() OVER (ORDER BY user_play_count DESC, freshness_score DESC) AS favorite_rank
    FROM ml_track_content_features
    WHERE user_play_count >= 2  -- Tracks user has played multiple times
),
user_favorite_tracks AS (
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
    FROM user_favorite_tracks_base
    WHERE favorite_rank <= 10  -- Use top 10 favorite tracks as seeds
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
        fav.TRACK_NAME AS seed_TRACK_NAME,
        
        -- Calculate content similarity (simplified cosine similarity)
        (
            -- Genre similarity (exact match = 1, different = 0)
            CASE WHEN cand.PRIMARY_GENRE = fav.PRIMARY_GENRE THEN 1.0 ELSE 0.0 END * 0.3 +
            
            -- Popularity similarity (1 - normalized difference)
            (1 - ABS(cand.popularity_normalized - fav.popularity_normalized)) * 0.25 +
            
            -- Duration similarity
            (1 - ABS(cand.duration_normalized - fav.duration_normalized)) * 0.2 +
            
            -- Popularity tier similarity
            (1 - ABS(cand.popularity_tier - fav.popularity_tier) / 4.0) * 0.15 +
            
            -- Era similarity
            CASE WHEN cand.era_category = fav.era_category THEN 1.0 ELSE 0.5 END * 0.1
            
        ) AS content_similarity_score
        
    FROM ml_track_content_features cand
    CROSS JOIN user_favorite_tracks fav
    WHERE cand.TRACK_ID != fav.TRACK_ID  -- Don't recommend the same track
    AND cand.user_play_count = 0  -- Only recommend new tracks
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
        
        -- Combined content score
        (AVG(content_similarity_score) * 0.7 + MAX(content_similarity_score) * 0.3) AS content_score,
        
        ROW_NUMBER() OVER (ORDER BY 
            (AVG(content_similarity_score) * 0.7 + MAX(content_similarity_score) * 0.3) DESC
        ) AS content_rank
        
    FROM content_candidates
    WHERE content_similarity_score > 0.5  -- Minimum similarity threshold
    GROUP BY TRACK_ID, TRACK_NAME, PRIMARY_ARTIST_NAME, PRIMARY_GENRE, ALBUM_NAME, TRACK_POPULARITY
    HAVING COUNT(*) >= 2  -- Must be similar to at least 2 favorite tracks
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
ORDER BY content_score DESC;

-- Temporal Pattern Recommendations
CREATE OR REPLACE VIEW ml_temporal_recommendations AS
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
        
        -- Calculate temporal relevance to current time
        CASE 
            WHEN tp.denver_hour = cc.current_hour AND tp.IS_WEEKEND = cc.is_current_weekend THEN 1.0
            WHEN ABS(tp.denver_hour - cc.current_hour) <= 1 AND tp.IS_WEEKEND = cc.is_current_weekend THEN 0.8
            WHEN tp.IS_WEEKEND = cc.is_current_weekend THEN 0.6
            WHEN ABS(tp.denver_hour - cc.current_hour) <= 2 THEN 0.4
            ELSE 0.2
        END AS temporal_relevance
        
    FROM ml_temporal_patterns tp
    CROSS JOIN current_context cc
    WHERE tp.temporal_strength IN ('strong', 'moderate')
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
        
        -- Combine temporal patterns with track quality
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
    WHERE tcf.user_play_count = 0  -- Only recommend new tracks
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
WHERE genre_temporal_rank <= 2  -- Top 2 tracks per temporally relevant genre
AND temporal_relevance >= 0.4  -- Minimum temporal relevance
ORDER BY temporal_score DESC;

-- Discovery Recommendations (Exploration)
CREATE OR REPLACE VIEW ml_discovery_recommendations AS
WITH user_known_genres AS (
    SELECT DISTINCT PRIMARY_GENRE
    FROM ml_user_genre_interactions
    WHERE weighted_preference > 0.5
),
user_known_artists AS (
    SELECT DISTINCT PRIMARY_ARTIST_ID
    FROM spotify_analytics.medallion_arch.silver_listening_enriched
    WHERE DENVER_DATE >= DATEADD('days', -90, CURRENT_DATE)
),
discovery_candidates AS (
    SELECT 
        tcf.TRACK_ID,
        tcf.TRACK_NAME,
        tcf.PRIMARY_ARTIST_NAME,
        tcf.PRIMARY_ARTIST_ID,
        tcf.PRIMARY_GENRE,
        tcf.ALBUM_NAME,
        tcf.TRACK_POPULARITY,
        tcf.era_category,
        tcf.artist_avg_popularity,
        
        -- Discovery scores
        CASE 
            WHEN tcf.PRIMARY_GENRE NOT IN (SELECT PRIMARY_GENRE FROM user_known_genres) THEN 1.0
            ELSE 0.3  -- Lower score for known genres
        END AS genre_novelty_score,
        
        CASE 
            WHEN tcf.PRIMARY_ARTIST_ID NOT IN (SELECT PRIMARY_ARTIST_ID FROM user_known_artists) THEN 1.0
            ELSE 0.0  -- No score for known artists
        END AS artist_novelty_score,
        
        -- Hidden gems: moderate popularity tracks that aren't mainstream
        CASE 
            WHEN tcf.TRACK_POPULARITY BETWEEN 30 AND 70 THEN 
                (70 - ABS(tcf.TRACK_POPULARITY - 50)) / 20.0
            ELSE 0.3
        END AS hidden_gem_score,
        
        -- Era diversity bonus
        CASE 
            WHEN tcf.era_category IN ('current', 'recent') THEN 0.8
            WHEN tcf.era_category IN ('2010s', '2000s') THEN 1.0
            ELSE 1.2  -- Bonus for older music
        END AS era_diversity_score
        
    FROM ml_track_content_features tcf
    WHERE tcf.user_play_count = 0  -- Only recommend completely new tracks
    AND tcf.TRACK_POPULARITY > 10  -- Avoid completely unknown tracks
),
scored_discoveries AS (
    SELECT 
        *,
        -- Combined discovery score
        (genre_novelty_score * 0.3 + 
         artist_novelty_score * 0.3 + 
         hidden_gem_score * 0.25 + 
         era_diversity_score * 0.15) AS discovery_score,
        
        ROW_NUMBER() OVER (ORDER BY 
            (genre_novelty_score * 0.3 + 
             artist_novelty_score * 0.3 + 
             hidden_gem_score * 0.25 + 
             era_diversity_score * 0.15) DESC,
            RANDOM()
        ) AS discovery_rank
        
    FROM discovery_candidates
    WHERE (genre_novelty_score > 0.5 OR artist_novelty_score > 0.5)  -- Must have some novelty
)
SELECT 
    TRACK_ID,
    TRACK_NAME,
    PRIMARY_ARTIST_NAME,
    PRIMARY_GENRE,
    ALBUM_NAME,
    TRACK_POPULARITY,
    discovery_score AS recommendation_score,
    'discovery_exploration' AS recommendation_strategy,
    genre_novelty_score,
    artist_novelty_score,
    hidden_gem_score,
    era_diversity_score,
    discovery_rank,
    CURRENT_TIMESTAMP AS generated_at
    
FROM scored_discoveries
WHERE discovery_rank <= 20  -- Top 20 discovery recommendations
ORDER BY discovery_score DESC;

-- =====================================================================
-- 4. UNIFIED HYBRID RECOMMENDATION SYSTEM
-- =====================================================================

-- Simplified Hybrid Recommendations (avoids complex subquery issues)
CREATE OR REPLACE VIEW ml_hybrid_recommendations_simple AS
SELECT 
    TRACK_ID,
    TRACK_NAME,
    PRIMARY_ARTIST_NAME,
    PRIMARY_GENRE,
    ALBUM_NAME,
    TRACK_POPULARITY,
    recommendation_score * 0.40 AS final_recommendation_score,
    'collaborative_filtering' AS recommendation_strategies,
    1 AS recommendation_support,
    ROW_NUMBER() OVER (ORDER BY recommendation_score DESC) AS playlist_position,
    
    -- Simple metadata
    OBJECT_CONSTRUCT(
        'recommendation_reason', 'Based on similar listening patterns',
        'confidence_score', recommendation_score * 0.40,
        'spotify_url', 'https://open.spotify.com/track/' || TRACK_ID,
        'strategies_used', 'collaborative_filtering',
        'popularity_tier', 
            CASE 
                WHEN TRACK_POPULARITY >= 80 THEN 'Mainstream Hit'
                WHEN TRACK_POPULARITY >= 60 THEN 'Popular'
                WHEN TRACK_POPULARITY >= 40 THEN 'Rising'
                WHEN TRACK_POPULARITY >= 20 THEN 'Hidden Gem'
                ELSE 'Deep Cut'
            END
    ) AS recommendation_metadata,
    
    generated_at AS playlist_generated_at
    
FROM ml_collaborative_recommendations
WHERE recommendation_score > 0.3
ORDER BY recommendation_score DESC
LIMIT 30;

-- Full Hybrid Recommendations (use this if individual views work)
CREATE OR REPLACE VIEW ml_hybrid_recommendations AS
WITH weighted_recommendations AS (
    -- Collaborative Filtering (40% weight)
    SELECT 
        TRACK_ID,
        TRACK_NAME,
        PRIMARY_ARTIST_NAME,
        PRIMARY_GENRE,
        ALBUM_NAME,
        TRACK_POPULARITY,
        recommendation_score,
        recommendation_strategy,
        genre_track_rank,
        generated_at,
        recommendation_score * 0.40 AS weighted_score,
        1 AS strategy_priority
    FROM ml_collaborative_recommendations
    WHERE recommendation_score > 0.3
    
    UNION ALL
    
    -- Content-Based Filtering (30% weight)
    SELECT 
        TRACK_ID,
        TRACK_NAME,
        PRIMARY_ARTIST_NAME,
        PRIMARY_GENRE,
        ALBUM_NAME,
        TRACK_POPULARITY,
        recommendation_score,
        recommendation_strategy,
        content_rank AS genre_track_rank,
        generated_at,
        recommendation_score * 0.30 AS weighted_score,
        2 AS strategy_priority
    FROM ml_content_based_recommendations
    WHERE recommendation_score > 0.4
    
    UNION ALL
    
    -- Temporal Patterns (20% weight)
    SELECT 
        TRACK_ID,
        TRACK_NAME,
        PRIMARY_ARTIST_NAME,
        PRIMARY_GENRE,
        ALBUM_NAME,
        TRACK_POPULARITY,
        recommendation_score,
        recommendation_strategy,
        genre_temporal_rank AS genre_track_rank,
        generated_at,
        recommendation_score * 0.20 AS weighted_score,
        3 AS strategy_priority
    FROM ml_temporal_recommendations
    WHERE recommendation_score > 0.3
    
    UNION ALL
    
    -- Discovery (10% weight)
    SELECT 
        TRACK_ID,
        TRACK_NAME,
        PRIMARY_ARTIST_NAME,
        PRIMARY_GENRE,
        ALBUM_NAME,
        TRACK_POPULARITY,
        recommendation_score,
        recommendation_strategy,
        discovery_rank AS genre_track_rank,
        generated_at,
        recommendation_score * 0.10 AS weighted_score,
        4 AS strategy_priority
    FROM ml_discovery_recommendations
    WHERE recommendation_score > 0.5
),
deduplicated_recommendations AS (
    SELECT 
        TRACK_ID,
        TRACK_NAME,
        PRIMARY_ARTIST_NAME,
        PRIMARY_GENRE,
        ALBUM_NAME,
        TRACK_POPULARITY,
        
        -- Combine scores for tracks recommended by multiple strategies
        MAX(recommendation_score) AS max_individual_score,
        SUM(weighted_score) AS combined_weighted_score,
        COUNT(*) AS recommendation_support,
        
        -- Aggregate strategy information
        LISTAGG(DISTINCT recommendation_strategy, ',') AS combined_strategies,
        MIN(strategy_priority) AS primary_strategy_priority,
        
        MAX(generated_at) AS generated_at
        
    FROM weighted_recommendations
    GROUP BY TRACK_ID, TRACK_NAME, PRIMARY_ARTIST_NAME, PRIMARY_GENRE, ALBUM_NAME, TRACK_POPULARITY
),
final_ranked_recommendations AS (
    SELECT 
        *,
        -- Final hybrid score combining individual performance and multi-strategy support
        (combined_weighted_score + 
         (recommendation_support - 1) * 0.1) AS final_recommendation_score,  -- Bonus for multi-strategy support
        
        ROW_NUMBER() OVER (ORDER BY 
            (combined_weighted_score + (recommendation_support - 1) * 0.1) DESC,
            recommendation_support DESC,
            TRACK_POPULARITY DESC
        ) AS final_rank
        
    FROM deduplicated_recommendations
)
SELECT 
    TRACK_ID,
    TRACK_NAME,
    PRIMARY_ARTIST_NAME,
    PRIMARY_GENRE,
    ALBUM_NAME,
    TRACK_POPULARITY,
    final_recommendation_score,
    combined_strategies AS recommendation_strategies,
    recommendation_support,
    final_rank AS playlist_position,
    
    -- Add recommendation metadata
    OBJECT_CONSTRUCT(
        'recommendation_reason', 
        CASE 
            WHEN POSITION('collaborative', combined_strategies) > 0 AND POSITION('content', combined_strategies) > 0 
                THEN 'Multiple algorithms agree this matches your taste'
            WHEN POSITION('collaborative', combined_strategies) > 0 
                THEN 'People with similar taste also enjoy this'
            WHEN POSITION('content', combined_strategies) > 0 
                THEN 'Similar to tracks you already love'
            WHEN POSITION('temporal', combined_strategies) > 0 
                THEN 'Perfect for this time of day'
            WHEN POSITION('discovery', combined_strategies) > 0 
                THEN 'Discover something new you might love'
            ELSE 'Recommended based on your listening patterns'
        END,
        'confidence_score', final_recommendation_score,
        'spotify_url', 'https://open.spotify.com/track/' || TRACK_ID,
        'strategies_used', combined_strategies,
        'support_level', recommendation_support,
        'popularity_tier', 
            CASE 
                WHEN TRACK_POPULARITY >= 80 THEN 'Mainstream Hit'
                WHEN TRACK_POPULARITY >= 60 THEN 'Popular'
                WHEN TRACK_POPULARITY >= 40 THEN 'Rising'
                WHEN TRACK_POPULARITY >= 20 THEN 'Hidden Gem'
                ELSE 'Deep Cut'
            END
    ) AS recommendation_metadata,
    
    generated_at AS playlist_generated_at
    
FROM final_ranked_recommendations
WHERE final_rank <= 30  -- Top 30 recommendations for playlist
ORDER BY final_rank;

-- =====================================================================
-- 5. PERFORMANCE MONITORING AND ANALYTICS
-- =====================================================================

-- Simple Analytics View (avoids subquery issues)
CREATE OR REPLACE VIEW ml_recommendation_analytics_simple AS
SELECT 
    COUNT(*) AS total_recommendations,
    COUNT(DISTINCT PRIMARY_GENRE) AS unique_genres_recommended,
    COUNT(DISTINCT PRIMARY_ARTIST_NAME) AS unique_artists_recommended,
    AVG(final_recommendation_score) AS avg_recommendation_score,
    MAX(final_recommendation_score) AS max_recommendation_score,
    MIN(final_recommendation_score) AS min_recommendation_score,
    AVG(TRACK_POPULARITY) AS avg_recommended_popularity,
    CURRENT_TIMESTAMP AS analysis_timestamp
FROM ml_hybrid_recommendations_simple;

-- Full Analytics View (commented out due to subquery issues)
/*
CREATE OR REPLACE VIEW ml_recommendation_analytics AS
SELECT 
    COUNT(*) AS total_recommendations,
    COUNT(DISTINCT PRIMARY_GENRE) AS unique_genres_recommended,
    COUNT(DISTINCT PRIMARY_ARTIST_NAME) AS unique_artists_recommended,
    AVG(final_recommendation_score) AS avg_recommendation_score,
    STDDEV(final_recommendation_score) AS recommendation_score_variance,
    
    -- Strategy distribution
    COUNT(CASE WHEN CONTAINS(recommendation_strategies, 'collaborative') THEN 1 END) AS collaborative_recs,
    COUNT(CASE WHEN CONTAINS(recommendation_strategies, 'content') THEN 1 END) AS content_recs,
    COUNT(CASE WHEN CONTAINS(recommendation_strategies, 'temporal') THEN 1 END) AS temporal_recs,
    COUNT(CASE WHEN CONTAINS(recommendation_strategies, 'discovery') THEN 1 END) AS discovery_recs,
    
    -- Multi-strategy recommendations
    COUNT(CASE WHEN recommendation_support > 1 THEN 1 END) AS multi_strategy_recs,
    
    -- Popularity distribution
    COUNT(CASE WHEN TRACK_POPULARITY >= 80 THEN 1 END) AS mainstream_hits,
    COUNT(CASE WHEN TRACK_POPULARITY BETWEEN 60 AND 79 THEN 1 END) AS popular_tracks,
    COUNT(CASE WHEN TRACK_POPULARITY BETWEEN 40 AND 59 THEN 1 END) AS rising_tracks,
    COUNT(CASE WHEN TRACK_POPULARITY BETWEEN 20 AND 39 THEN 1 END) AS hidden_gems,
    COUNT(CASE WHEN TRACK_POPULARITY < 20 THEN 1 END) AS deep_cuts,
    
    -- Quality metrics
    AVG(TRACK_POPULARITY) AS avg_recommended_popularity,
    MAX(final_recommendation_score) AS max_recommendation_score,
    MIN(final_recommendation_score) AS min_recommendation_score,
    
    -- Diversity scores
    COUNT(DISTINCT PRIMARY_GENRE) / GREATEST(COUNT(*), 1)::FLOAT AS genre_diversity_score,
    COUNT(DISTINCT PRIMARY_ARTIST_NAME) / GREATEST(COUNT(*), 1)::FLOAT AS artist_diversity_score,
    
    -- Strategy effectiveness
    COUNT(CASE WHEN recommendation_support > 1 THEN 1 END) / GREATEST(COUNT(*), 1)::FLOAT AS multi_strategy_ratio,
    
    -- Balance metrics
    (COUNT(CASE WHEN TRACK_POPULARITY >= 60 THEN 1 END)) / GREATEST(COUNT(*), 1)::FLOAT AS familiarity_ratio,
    (COUNT(CASE WHEN TRACK_POPULARITY < 40 THEN 1 END)) / GREATEST(COUNT(*), 1)::FLOAT AS discovery_ratio,
    
    CURRENT_TIMESTAMP AS analysis_timestamp
    
FROM ml_hybrid_recommendations;
*/

-- =====================================================================
-- USAGE EXAMPLES AND TESTING QUERIES
-- =====================================================================

-- Example: Get your personalized hybrid recommendations
-- Try simple version first if you get subquery errors:
SELECT * FROM ml_hybrid_recommendations_simple ORDER BY playlist_position;

-- Full hybrid recommendations (use this if simple version works):
-- SELECT * FROM ml_hybrid_recommendations ORDER BY playlist_position;

-- Example: Analyze recommendation system performance  
SELECT * FROM ml_recommendation_analytics_simple;

-- Example: Get collaborative filtering recommendations only
-- SELECT * FROM ml_collaborative_recommendations LIMIT 10;

-- Example: See temporal recommendations for current time
-- SELECT * FROM ml_temporal_recommendations LIMIT 10;

-- Example: Find similar genres to your favorites
-- SELECT to_genre, similarity_score 
-- FROM ml_genre_similarity_matrix 
-- WHERE from_genre = 'indie rock' 
-- ORDER BY similarity_score DESC LIMIT 10;

-- Example: View your music taste profile
-- SELECT * FROM ml_user_genre_interactions ORDER BY weighted_preference DESC;

-- Setup complete! Your comprehensive ML recommendation engine is ready for use with Snowflake Model Registry.
