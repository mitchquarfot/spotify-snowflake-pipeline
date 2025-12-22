-- =====================================================================
-- MODEL INFERENCE FUNCTIONS
-- SQL functions for real-time model inference and recommendation generation
-- =====================================================================

USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- =====================================================================
-- 1. REAL-TIME RECOMMENDATION FUNCTIONS
-- =====================================================================

-- Get personalized recommendations using the hybrid model
CREATE OR REPLACE FUNCTION get_spotify_recommendations(
    num_recommendations INTEGER DEFAULT 30,
    current_hour INTEGER DEFAULT NULL,
    is_weekend BOOLEAN DEFAULT NULL,
    strategy_weights OBJECT DEFAULT NULL,
    min_score FLOAT DEFAULT 0.3
)
RETURNS TABLE (
    track_id STRING,
    track_name STRING,
    artist_name STRING,
    genre STRING,
    album_name STRING,
    track_popularity INTEGER,
    recommendation_score FLOAT,
    recommendation_strategies STRING,
    playlist_position INTEGER,
    spotify_url STRING,
    recommendation_reason STRING
)
LANGUAGE SQL
AS
$$
    WITH contextualized_recommendations AS (
        SELECT 
            hr.*,
            -- Apply temporal context if provided
            CASE 
                WHEN current_hour IS NOT NULL THEN
                    -- Boost score for temporally relevant tracks
                    CASE 
                        WHEN CONTAINS(hr.recommendation_strategies, 'temporal') THEN hr.final_recommendation_score * 1.2
                        ELSE hr.final_recommendation_score
                    END
                ELSE hr.final_recommendation_score
            END AS contextualized_score
        FROM ml_hybrid_recommendations hr
        WHERE hr.final_recommendation_score >= min_score
    ),
    filtered_recommendations AS (
        SELECT 
            track_id,
            track_name,
            primary_artist_name AS artist_name,
            primary_genre AS genre,
            album_name,
            track_popularity,
            contextualized_score AS recommendation_score,
            recommendation_strategies,
            ROW_NUMBER() OVER (ORDER BY contextualized_score DESC) AS playlist_position,
            'https://open.spotify.com/track/' || track_id AS spotify_url,
            recommendation_metadata:recommendation_reason::STRING AS recommendation_reason
        FROM contextualized_recommendations
        ORDER BY contextualized_score DESC
        LIMIT num_recommendations
    )
    SELECT * FROM filtered_recommendations
$$;

-- Get recommendations by specific strategy
CREATE OR REPLACE FUNCTION get_recommendations_by_strategy(
    strategy STRING,
    num_recommendations INTEGER DEFAULT 20
)
RETURNS TABLE (
    track_id STRING,
    track_name STRING,
    artist_name STRING,
    genre STRING,
    recommendation_score FLOAT,
    strategy_rank INTEGER
)
LANGUAGE SQL
AS
$$
    SELECT 
        track_id,
        track_name,
        primary_artist_name AS artist_name,
        primary_genre AS genre,
        recommendation_score,
        ROW_NUMBER() OVER (ORDER BY recommendation_score DESC) AS strategy_rank
    FROM (
        SELECT track_id, track_name, primary_artist_name, primary_genre, recommendation_score, 'collaborative' as strategy_source
        FROM ml_collaborative_recommendations
        WHERE strategy = 'collaborative'
        
        UNION ALL
        
        SELECT track_id, track_name, primary_artist_name, primary_genre, recommendation_score, 'content_based' as strategy_source
        FROM ml_content_based_recommendations  
        WHERE strategy = 'content_based'
        
        UNION ALL
        
        SELECT track_id, track_name, primary_artist_name, primary_genre, recommendation_score, 'temporal' as strategy_source
        FROM ml_temporal_recommendations
        WHERE strategy = 'temporal'
        
        UNION ALL
        
        SELECT track_id, track_name, primary_artist_name, primary_genre, recommendation_score, 'discovery' as strategy_source
        FROM ml_discovery_recommendations
        WHERE strategy = 'discovery'
    )
    WHERE track_id IS NOT NULL
    ORDER BY recommendation_score DESC
    LIMIT num_recommendations
$$;

-- Get similar tracks to a given track
CREATE OR REPLACE FUNCTION get_similar_tracks(
    seed_track_id STRING,
    num_similar INTEGER DEFAULT 10,
    include_same_artist BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
    track_id STRING,
    track_name STRING,
    artist_name STRING,
    genre STRING,
    similarity_score FLOAT,
    similarity_rank INTEGER,
    similarity_reason STRING
)
LANGUAGE SQL
AS
$$
    WITH seed_track_features AS (
        SELECT 
            track_id,
            primary_artist_id,
            primary_genre,
            popularity_normalized,
            duration_normalized,
            era_category,
            popularity_tier
        FROM ml_track_content_features
        WHERE track_id = seed_track_id
    ),
    similarity_candidates AS (
        SELECT 
            cf.track_id,
            cf.track_name,
            cf.primary_artist_name,
            cf.primary_genre,
            
            -- Calculate multi-dimensional similarity
            (
                -- Genre similarity
                CASE WHEN cf.primary_genre = sf.primary_genre THEN 1.0 ELSE 0.3 END * 0.3 +
                
                -- Popularity similarity
                (1 - ABS(cf.popularity_normalized - sf.popularity_normalized)) * 0.25 +
                
                -- Duration similarity
                (1 - ABS(cf.duration_normalized - sf.duration_normalized)) * 0.2 +
                
                -- Era similarity
                CASE WHEN cf.era_category = sf.era_category THEN 1.0 ELSE 0.5 END * 0.15 +
                
                -- Popularity tier similarity
                (1 - ABS(cf.popularity_tier - sf.popularity_tier) / 4.0) * 0.1
                
            ) AS content_similarity,
            
            CASE 
                WHEN cf.primary_genre = sf.primary_genre THEN 'Same genre'
                WHEN cf.era_category = sf.era_category THEN 'Same era'
                WHEN ABS(cf.popularity_normalized - sf.popularity_normalized) < 0.2 THEN 'Similar popularity'
                ELSE 'Musical characteristics'
            END AS similarity_reason
            
        FROM ml_track_content_features cf
        CROSS JOIN seed_track_features sf
        WHERE cf.track_id != sf.track_id
        AND (include_same_artist OR cf.primary_artist_id != sf.primary_artist_id)
        AND cf.user_play_count = 0  -- Only recommend new tracks
    )
    SELECT 
        track_id,
        track_name,
        primary_artist_name AS artist_name,
        primary_genre AS genre,
        content_similarity AS similarity_score,
        ROW_NUMBER() OVER (ORDER BY content_similarity DESC) AS similarity_rank,
        similarity_reason
    FROM similarity_candidates
    WHERE content_similarity > 0.4  -- Minimum similarity threshold
    ORDER BY content_similarity DESC
    LIMIT num_similar
$$;

-- Get genre recommendations based on user preferences
CREATE OR REPLACE FUNCTION get_genre_recommendations(
    target_genre STRING DEFAULT NULL,
    num_tracks INTEGER DEFAULT 15,
    popularity_range ARRAY DEFAULT NULL
)
RETURNS TABLE (
    track_id STRING,
    track_name STRING,
    artist_name STRING,
    genre STRING,
    track_popularity INTEGER,
    genre_relevance_score FLOAT,
    track_rank_in_genre INTEGER
)
LANGUAGE SQL
AS
$$
    WITH user_genre_affinity AS (
        SELECT 
            primary_genre,
            weighted_preference,
            preference_strength
        FROM ml_user_genre_interactions
        WHERE (target_genre IS NULL OR primary_genre = target_genre)
        ORDER BY weighted_preference DESC
        LIMIT CASE WHEN target_genre IS NULL THEN 5 ELSE 1 END
    ),
    genre_tracks AS (
        SELECT 
            tcf.track_id,
            tcf.track_name,
            tcf.primary_artist_name,
            tcf.primary_genre,
            tcf.track_popularity,
            uga.weighted_preference AS genre_relevance_score,
            ROW_NUMBER() OVER (
                PARTITION BY tcf.primary_genre 
                ORDER BY tcf.track_popularity DESC, tcf.freshness_score DESC, RANDOM()
            ) AS track_rank_in_genre
        FROM ml_track_content_features tcf
        JOIN user_genre_affinity uga ON tcf.primary_genre = uga.primary_genre
        WHERE tcf.user_play_count = 0  -- Only new tracks
        AND (
            popularity_range IS NULL OR
            tcf.track_popularity BETWEEN GET(popularity_range, 0) AND GET(popularity_range, 1)
        )
    )
    SELECT 
        track_id,
        track_name,
        primary_artist_name AS artist_name,
        primary_genre AS genre,
        track_popularity,
        genre_relevance_score,
        track_rank_in_genre
    FROM genre_tracks
    WHERE track_rank_in_genre <= (num_tracks / GREATEST((SELECT COUNT(*) FROM user_genre_affinity), 1))
    ORDER BY genre_relevance_score DESC, track_rank_in_genre
    LIMIT num_tracks
$$;

-- =====================================================================
-- 2. TEMPORAL AND CONTEXTUAL RECOMMENDATION FUNCTIONS
-- =====================================================================

-- Get recommendations for specific time context
CREATE OR REPLACE FUNCTION get_time_based_recommendations(
    target_hour INTEGER,
    target_is_weekend BOOLEAN,
    num_recommendations INTEGER DEFAULT 20,
    temporal_flexibility INTEGER DEFAULT 2
)
RETURNS TABLE (
    track_id STRING,
    track_name STRING,
    artist_name STRING,
    genre STRING,
    temporal_score FLOAT,
    hour_relevance STRING,
    recommendation_rank INTEGER
)
LANGUAGE SQL
AS
$$
    WITH temporal_context AS (
        SELECT 
            primary_genre,
            denver_hour,
            is_weekend,
            hour_genre_probability,
            temporal_recommendation_score,
            
            -- Calculate relevance to target time
            CASE 
                WHEN denver_hour = target_hour 
                AND is_weekend = target_is_weekend THEN 1.0
                WHEN ABS(denver_hour - target_hour) <= temporal_flexibility
                AND is_weekend = target_is_weekend THEN 0.8
                WHEN is_weekend = target_is_weekend THEN 0.6
                WHEN ABS(denver_hour - target_hour) <= temporal_flexibility THEN 0.4
                ELSE 0.2
            END AS time_relevance,
            
            CASE 
                WHEN denver_hour = target_hour THEN 'Perfect match'
                WHEN ABS(denver_hour - target_hour) <= 1 THEN 'Close match'
                WHEN ABS(denver_hour - target_hour) <= temporal_flexibility THEN 'Similar time'
                ELSE 'General preference'
            END AS hour_relevance
            
        FROM ml_temporal_patterns
        WHERE temporal_strength IN ('strong', 'moderate')
    ),
    temporal_tracks AS (
        SELECT 
            tcf.track_id,
            tcf.track_name,
            tcf.primary_artist_name,
            tcf.primary_genre,
            tc.temporal_recommendation_score,
            tc.time_relevance,
            tc.hour_relevance,
            
            -- Combined temporal score
            (tc.temporal_recommendation_score * tc.time_relevance) AS temporal_score,
            
            ROW_NUMBER() OVER (
                ORDER BY (tc.temporal_recommendation_score * tc.time_relevance) DESC,
                tcf.track_popularity DESC
            ) AS recommendation_rank
            
        FROM ml_track_content_features tcf
        JOIN temporal_context tc ON tcf.primary_genre = tc.primary_genre
        WHERE tcf.user_play_count = 0
        AND tc.time_relevance >= 0.4
    )
    SELECT 
        track_id,
        track_name,
        primary_artist_name AS artist_name,
        primary_genre AS genre,
        temporal_score,
        hour_relevance,
        recommendation_rank
    FROM temporal_tracks
    ORDER BY temporal_score DESC
    LIMIT num_recommendations
$$;

-- Get mood-based recommendations (simplified)
CREATE OR REPLACE FUNCTION get_mood_recommendations(
    mood STRING,
    num_recommendations INTEGER DEFAULT 15
)
RETURNS TABLE (
    track_id STRING,
    track_name STRING,
    artist_name STRING,
    genre STRING,
    mood_relevance_score FLOAT,
    mood_reason STRING
)
LANGUAGE SQL
AS
$$
    WITH mood_genre_mapping AS (
        SELECT 
            primary_genre,
            CASE 
                WHEN mood = 'energetic' THEN
                    CASE 
                        WHEN primary_genre ILIKE '%electronic%' OR primary_genre ILIKE '%dance%' 
                             OR primary_genre ILIKE '%pop%' OR primary_genre ILIKE '%rock%' THEN 0.9
                        WHEN primary_genre ILIKE '%hip hop%' OR primary_genre ILIKE '%rap%' THEN 0.8
                        ELSE 0.3
                    END
                WHEN mood = 'relaxed' THEN
                    CASE 
                        WHEN primary_genre ILIKE '%ambient%' OR primary_genre ILIKE '%classical%' 
                             OR primary_genre ILIKE '%jazz%' OR primary_genre ILIKE '%folk%' THEN 0.9
                        WHEN primary_genre ILIKE '%indie%' OR primary_genre ILIKE '%alternative%' THEN 0.7
                        ELSE 0.4
                    END
                WHEN mood = 'focused' THEN
                    CASE 
                        WHEN primary_genre ILIKE '%instrumental%' OR primary_genre ILIKE '%classical%' 
                             OR primary_genre ILIKE '%ambient%' THEN 0.9
                        WHEN primary_genre ILIKE '%electronic%' AND NOT primary_genre ILIKE '%dance%' THEN 0.7
                        ELSE 0.3
                    END
                WHEN mood = 'nostalgic' THEN
                    CASE 
                        WHEN primary_genre ILIKE '%classic%' OR primary_genre ILIKE '%oldies%' THEN 0.9
                        WHEN primary_genre ILIKE '%rock%' OR primary_genre ILIKE '%pop%' THEN 0.6
                        ELSE 0.4
                    END
                ELSE 0.5
            END AS mood_score,
            
            CASE 
                WHEN mood = 'energetic' THEN 'High energy genre'
                WHEN mood = 'relaxed' THEN 'Calming genre'
                WHEN mood = 'focused' THEN 'Focus-friendly genre'
                WHEN mood = 'nostalgic' THEN 'Classic genre'
                ELSE 'Mood-appropriate genre'
            END AS mood_reason
            
        FROM (SELECT DISTINCT primary_genre FROM ml_track_content_features) genres
    ),
    mood_tracks AS (
        SELECT 
            tcf.track_id,
            tcf.track_name,
            tcf.primary_artist_name,
            tcf.primary_genre,
            mgm.mood_score,
            mgm.mood_reason,
            
            -- Combine mood score with track characteristics
            (mgm.mood_score * 0.7 + tcf.popularity_normalized * 0.3) AS mood_relevance_score,
            
            ROW_NUMBER() OVER (
                ORDER BY (mgm.mood_score * 0.7 + tcf.popularity_normalized * 0.3) DESC,
                RANDOM()
            ) AS mood_rank
            
        FROM ml_track_content_features tcf
        JOIN mood_genre_mapping mgm ON tcf.primary_genre = mgm.primary_genre
        WHERE tcf.user_play_count = 0
        AND mgm.mood_score > 0.5
    )
    SELECT 
        track_id,
        track_name,
        primary_artist_name AS artist_name,
        primary_genre AS genre,
        mood_relevance_score,
        mood_reason
    FROM mood_tracks
    ORDER BY mood_relevance_score DESC
    LIMIT num_recommendations
$$;

-- =====================================================================
-- 3. DISCOVERY AND EXPLORATION FUNCTIONS
-- =====================================================================

-- Get discovery recommendations for exploring new music
CREATE OR REPLACE FUNCTION get_discovery_recommendations(
    discovery_type STRING DEFAULT 'balanced',
    num_recommendations INTEGER DEFAULT 20,
    max_popularity INTEGER DEFAULT 70
)
RETURNS TABLE (
    track_id STRING,
    track_name STRING,
    artist_name STRING,
    genre STRING,
    discovery_score FLOAT,
    discovery_reason STRING,
    novelty_level STRING
)
LANGUAGE SQL
AS
$$
    WITH user_known_content AS (
        SELECT DISTINCT primary_genre FROM ml_user_genre_interactions
        UNION
        SELECT DISTINCT primary_artist_id FROM spotify_analytics.medallion_arch.silver_listening_enriched 
        WHERE denver_date >= DATEADD('days', -90, CURRENT_DATE)
    ),
    discovery_candidates AS (
        SELECT 
            tcf.track_id,
            tcf.track_name,
            tcf.primary_artist_name,
            tcf.primary_artist_id,
            tcf.primary_genre,
            tcf.track_popularity,
            
            -- Calculate novelty scores
            CASE 
                WHEN tcf.primary_genre NOT IN (SELECT primary_genre FROM ml_user_genre_interactions) THEN 1.0
                ELSE 0.3
            END AS genre_novelty,
            
            CASE 
                WHEN tcf.primary_artist_id NOT IN (
                    SELECT DISTINCT primary_artist_id FROM spotify_analytics.medallion_arch.silver_listening_enriched 
                    WHERE denver_date >= DATEADD('days', -90, CURRENT_DATE)
                ) THEN 1.0
                ELSE 0.0
            END AS artist_novelty,
            
            -- Hidden gems score (moderate popularity)
            CASE 
                WHEN tcf.track_popularity BETWEEN 20 AND max_popularity THEN
                    (max_popularity - ABS(tcf.track_popularity - 45)) / max_popularity
                ELSE 0.3
            END AS hidden_gem_score,
            
            -- Era diversity
            CASE 
                WHEN tcf.era_category IN ('2000s', '90s', '80s', 'classic') THEN 1.2
                WHEN tcf.era_category = '2010s' THEN 1.0
                ELSE 0.8
            END AS era_novelty
            
        FROM ml_track_content_features tcf
        WHERE tcf.user_play_count = 0
        AND tcf.track_popularity <= max_popularity
        AND tcf.track_popularity >= 15  -- Avoid completely unknown tracks
    ),
    scored_discoveries AS (
        SELECT 
            *,
            -- Calculate discovery scores based on type
            CASE 
                WHEN discovery_type = 'genre_exploration' THEN
                    genre_novelty * 0.6 + hidden_gem_score * 0.3 + era_novelty * 0.1
                WHEN discovery_type = 'artist_discovery' THEN
                    artist_novelty * 0.6 + genre_novelty * 0.2 + hidden_gem_score * 0.2
                WHEN discovery_type = 'hidden_gems' THEN
                    hidden_gem_score * 0.5 + genre_novelty * 0.3 + artist_novelty * 0.2
                WHEN discovery_type = 'time_travel' THEN
                    era_novelty * 0.5 + hidden_gem_score * 0.3 + genre_novelty * 0.2
                ELSE -- 'balanced'
                    (genre_novelty + artist_novelty + hidden_gem_score + era_novelty) / 4
            END AS discovery_score,
            
            -- Determine discovery reason
            CASE 
                WHEN genre_novelty = 1.0 AND artist_novelty = 1.0 THEN 'Completely new artist and genre'
                WHEN artist_novelty = 1.0 THEN 'New artist in familiar genre'
                WHEN genre_novelty = 1.0 THEN 'New genre exploration'
                WHEN hidden_gem_score > 0.7 THEN 'Hidden gem discovery'
                WHEN era_novelty > 1.0 THEN 'Vintage music discovery'
                ELSE 'Musical exploration'
            END AS discovery_reason,
            
            -- Novelty level
            CASE 
                WHEN (genre_novelty + artist_novelty) >= 1.5 THEN 'High'
                WHEN (genre_novelty + artist_novelty) >= 0.8 THEN 'Medium'
                ELSE 'Low'
            END AS novelty_level
            
        FROM discovery_candidates
    )
    SELECT 
        track_id,
        track_name,
        primary_artist_name AS artist_name,
        primary_genre AS genre,
        discovery_score,
        discovery_reason,
        novelty_level
    FROM scored_discoveries
    WHERE discovery_score > 0.4  -- Minimum discovery threshold
    AND (genre_novelty > 0.5 OR artist_novelty > 0.5)  -- Must have some novelty
    ORDER BY discovery_score DESC
    LIMIT num_recommendations
$$;

-- Get recommendations for music exploration based on seed preferences
CREATE OR REPLACE FUNCTION explore_from_preferences(
    seed_genres ARRAY DEFAULT NULL,
    seed_artists ARRAY DEFAULT NULL,
    exploration_radius FLOAT DEFAULT 0.7,
    num_recommendations INTEGER DEFAULT 25
)
RETURNS TABLE (
    track_id STRING,
    track_name STRING,
    artist_name STRING,
    genre STRING,
    exploration_score FLOAT,
    connection_path STRING,
    exploration_type STRING
)
LANGUAGE SQL
AS
$$
    WITH seed_context AS (
        SELECT 
            -- Use provided seeds or fall back to user's top preferences
            COALESCE(
                explore_from_preferences.seed_genres,
                ARRAY_AGG(primary_genre) WITHIN GROUP (ORDER BY weighted_preference DESC)
            ) AS genres,
            COALESCE(
                explore_from_preferences.seed_artists,
                ARRAY_CONSTRUCT()  -- Empty array if no artists provided
            ) AS artists
        FROM (SELECT primary_genre, weighted_preference FROM ml_user_genre_interactions ORDER BY weighted_preference DESC LIMIT 5)
    ),
    genre_expansion AS (
        SELECT 
            gsm.to_genre AS explored_genre,
            gsm.from_genre AS seed_genre,
            gsm.similarity_score,
            'Genre similarity to ' || gsm.from_genre AS connection_path,
            'genre_expansion' AS exploration_type
        FROM ml_genre_similarity_matrix gsm
        CROSS JOIN seed_context sc
        WHERE EXISTS (SELECT 1 FROM TABLE(FLATTEN(sc.genres)) WHERE VALUE = gsm.from_genre)
        AND gsm.similarity_score >= explore_from_preferences.exploration_radius
        AND NOT EXISTS (SELECT 1 FROM TABLE(FLATTEN(sc.genres)) WHERE VALUE = gsm.to_genre)
    ),
    artist_expansion AS (
        SELECT 
            asn.artist_2_name AS explored_artist,
            asn.artist_1_name AS seed_artist,
            asn.artist_similarity_score AS similarity_score,
            'Artist similarity to ' || asn.artist_1_name AS connection_path,
            'artist_expansion' AS exploration_type
        FROM ml_artist_similarity_network asn
        CROSS JOIN seed_context sc
        WHERE EXISTS (SELECT 1 FROM TABLE(FLATTEN(sc.artists)) WHERE VALUE = asn.artist_1_name)
        AND asn.artist_similarity_score >= explore_from_preferences.exploration_radius
    ),
    exploration_tracks AS (
        -- Genre-based exploration
        SELECT 
            tcf.track_id,
            tcf.track_name,
            tcf.primary_artist_name,
            tcf.primary_genre,
            ge.similarity_score AS exploration_score,
            ge.connection_path,
            ge.exploration_type
        FROM ml_track_content_features tcf
        JOIN genre_expansion ge ON tcf.primary_genre = ge.explored_genre
        WHERE tcf.user_play_count = 0
        
        UNION ALL
        
        -- Artist-based exploration
        SELECT 
            tcf.track_id,
            tcf.track_name,
            tcf.primary_artist_name,
            tcf.primary_genre,
            ae.similarity_score AS exploration_score,
            ae.connection_path,
            ae.exploration_type
        FROM ml_track_content_features tcf
        JOIN artist_expansion ae ON tcf.primary_artist_name = ae.explored_artist
        WHERE tcf.user_play_count = 0
    ),
    ranked_explorations AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (ORDER BY exploration_score DESC, RANDOM()) AS exploration_rank
        FROM exploration_tracks
    )
    SELECT 
        track_id,
        track_name,
        primary_artist_name AS artist_name,
        primary_genre AS genre,
        exploration_score,
        connection_path,
        exploration_type
    FROM ranked_explorations
    WHERE exploration_rank <= explore_from_preferences.num_recommendations
    ORDER BY exploration_score DESC
$$;

-- =====================================================================
-- 4. UTILITY AND HELPER FUNCTIONS
-- =====================================================================

-- Get user's music taste profile summary
CREATE OR REPLACE FUNCTION get_user_taste_profile()
RETURNS TABLE (
    profile_type STRING,
    item_name STRING,
    preference_score FLOAT,
    preference_rank INTEGER,
    additional_info OBJECT
)
LANGUAGE SQL
AS
$$
    -- Top genres
    SELECT 
        'genre' AS profile_type,
        primary_genre AS item_name,
        weighted_preference AS preference_score,
        ROW_NUMBER() OVER (ORDER BY weighted_preference DESC) AS preference_rank,
        OBJECT_CONSTRUCT(
            'play_count', play_count,
            'avg_popularity', avg_popularity,
            'preference_strength', preference_strength,
            'is_current', is_current_preference
        ) AS additional_info
    FROM ml_user_genre_interactions
    ORDER BY weighted_preference DESC
    LIMIT 10
    
    UNION ALL
    
    -- Top artists
    SELECT 
        'artist' AS profile_type,
        primary_artist_name AS item_name,
        COUNT(*) AS preference_score,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS preference_rank,
        OBJECT_CONSTRUCT(
            'total_plays', COUNT(*),
            'unique_tracks', COUNT(DISTINCT track_id),
            'avg_popularity', AVG(track_popularity),
            'genres', ARRAY_AGG(DISTINCT primary_genre)
        ) AS additional_info
    FROM spotify_analytics.medallion_arch.silver_listening_enriched
    WHERE denver_date >= DATEADD('days', -90, CURRENT_DATE)
    GROUP BY primary_artist_name
    ORDER BY COUNT(*) DESC
    LIMIT 10
    
    UNION ALL
    
    -- Temporal patterns
    SELECT 
        'temporal' AS profile_type,
        'Hour ' || denver_hour || (CASE WHEN is_weekend THEN ' (Weekend)' ELSE ' (Weekday)' END) AS item_name,
        temporal_recommendation_score AS preference_score,
        ROW_NUMBER() OVER (ORDER BY temporal_recommendation_score DESC) AS preference_rank,
        OBJECT_CONSTRUCT(
            'hour', denver_hour,
            'is_weekend', is_weekend,
            'probability', hour_genre_probability,
            'strength', temporal_strength
        ) AS additional_info
    FROM ml_temporal_patterns
    WHERE temporal_strength IN ('strong', 'moderate')
    ORDER BY temporal_recommendation_score DESC
    LIMIT 5
$$;

-- Validate and score recommendation quality
CREATE OR REPLACE FUNCTION validate_recommendation_quality(
    recommended_tracks ARRAY,
    quality_threshold FLOAT DEFAULT 0.5
)
RETURNS TABLE (
    track_id STRING,
    quality_score FLOAT,
    quality_reasons ARRAY,
    passes_threshold BOOLEAN
)
LANGUAGE SQL
AS
$$
    WITH track_quality AS (
        SELECT 
            VALUE::STRING AS track_id,
            tcf.track_name,
            tcf.primary_genre,
            tcf.track_popularity,
            tcf.user_play_count,
            
            -- Quality scoring
            CASE 
                WHEN tcf.user_play_count > 0 THEN 0.0  -- Already played
                WHEN tcf.track_popularity < 10 THEN 0.2  -- Too obscure
                WHEN tcf.track_popularity > 95 THEN 0.6  -- Too mainstream
                ELSE 1.0
            END AS novelty_score,
            
            CASE 
                WHEN tcf.primary_genre IN (SELECT primary_genre FROM ml_user_genre_interactions WHERE weighted_preference > 1.0) THEN 0.8
                WHEN tcf.primary_genre IN (SELECT to_genre FROM ml_genre_similarity_matrix WHERE similarity_score > 0.5) THEN 0.6
                ELSE 0.3
            END AS relevance_score,
            
            CASE 
                WHEN tcf.track_popularity BETWEEN 30 AND 80 THEN 1.0
                WHEN tcf.track_popularity BETWEEN 20 AND 90 THEN 0.8
                ELSE 0.5
            END AS quality_score_component
            
        FROM TABLE(FLATTEN(validate_recommendation_quality.recommended_tracks)) 
        LEFT JOIN ml_track_content_features tcf ON VALUE::STRING = tcf.track_id
    )
    SELECT 
        track_id,
        (novelty_score * 0.4 + relevance_score * 0.4 + quality_score_component * 0.2) AS quality_score,
        ARRAY_CONSTRUCT(
            CASE WHEN novelty_score = 0.0 THEN 'Already played' END,
            CASE WHEN novelty_score = 0.2 THEN 'Very obscure' END,
            CASE WHEN novelty_score = 0.6 THEN 'Very mainstream' END,
            CASE WHEN relevance_score >= 0.8 THEN 'Highly relevant to taste' END,
            CASE WHEN relevance_score >= 0.6 THEN 'Somewhat relevant to taste' END,
            CASE WHEN relevance_score = 0.3 THEN 'New genre exploration' END
        ) AS quality_reasons,
        (novelty_score * 0.4 + relevance_score * 0.4 + quality_score_component * 0.2) >= validate_recommendation_quality.quality_threshold AS passes_threshold
    FROM track_quality
    WHERE track_id IS NOT NULL
$$;

-- =====================================================================
-- USAGE EXAMPLES
-- =====================================================================

-- Get personalized recommendations for current time
-- SELECT * FROM TABLE(get_spotify_recommendations(30, HOUR(CURRENT_TIMESTAMP()), DAYOFWEEK(CURRENT_DATE()) IN (0,6)));

-- Get similar tracks to a specific track
-- SELECT * FROM TABLE(get_similar_tracks('4iV5W9uYEdYUVa79Axb7Rh', 10, FALSE));

-- Get recommendations by strategy
-- SELECT * FROM TABLE(get_recommendations_by_strategy('collaborative', 15));

-- Get mood-based recommendations
-- SELECT * FROM TABLE(get_mood_recommendations('energetic', 20));

-- Get discovery recommendations
-- SELECT * FROM TABLE(get_discovery_recommendations('hidden_gems', 15, 60));

-- Explore from specific preferences
-- SELECT * FROM TABLE(explore_from_preferences(ARRAY_CONSTRUCT('indie rock', 'electronic'), NULL, 0.6, 20));

-- Get user taste profile
-- SELECT * FROM TABLE(get_user_taste_profile());

-- Validate recommendation quality
-- SELECT * FROM TABLE(validate_recommendation_quality(ARRAY_CONSTRUCT('track1', 'track2', 'track3'), 0.6));

-- Setup complete! Your model inference functions are ready for real-time recommendations.
