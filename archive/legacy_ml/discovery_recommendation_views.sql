-- Discovery Recommendation Views: Work with newly discovered Spotify tracks
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- Smart Discovery Recommendations (combines discovered tracks with user preferences)
CREATE OR REPLACE VIEW ml_smart_discovery_recommendations AS
WITH user_preferences AS (
    SELECT 
        primary_genre,
        weighted_preference,
        total_listening_time,
        avg_daily_listens
    FROM ml_user_genre_interactions
    WHERE weighted_preference > 0.1
),
artist_preferences AS (
    SELECT 
        primary_artist_name,
        COUNT(*) as artist_play_count,
        AVG(user_play_count) as avg_track_plays,
        COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () as artist_preference_score
    FROM ml_track_content_features
    GROUP BY primary_artist_name
    ORDER BY artist_preference_score DESC
),
discovery_scoring AS (
    SELECT 
        d.track_id,
        d.track_name,
        d.primary_artist_name,
        d.primary_artist_id,
        d.album_name,
        d.album_release_date,
        d.track_popularity,
        d.track_duration_ms,
        d.preview_url,
        d.discovery_strategy,
        d.seed_artist,
        d.seed_genre,
        d.preference_score as seed_preference_score,
        d.discovered_at,
        
        -- Calculate recommendation score based on multiple factors
        CASE 
            WHEN d.discovery_strategy = 'artist_based' THEN
                -- Artist-based discovery scoring
                COALESCE(ap.artist_preference_score, 0.1) * 0.5 +  -- Artist familiarity
                (d.track_popularity / 100.0) * 0.3 +               -- Track popularity
                d.preference_score * 0.2                           -- Seed preference
            
            WHEN d.discovery_strategy = 'genre_based' THEN  
                -- Genre-based discovery scoring
                COALESCE(up.weighted_preference, 0.1) * 0.4 +      -- Genre preference
                (d.track_popularity / 100.0) * 0.3 +               -- Track popularity  
                d.preference_score * 0.2 +                         -- Seed preference
                (1.0 / (DATEDIFF('days', d.discovered_at, CURRENT_TIMESTAMP) + 1)) * 0.1  -- Freshness bonus
                
            ELSE 0.3  -- Default score
        END AS calculated_recommendation_score,
        
        -- Diversity scoring (prefer variety)
        ROW_NUMBER() OVER (
            PARTITION BY d.primary_artist_name 
            ORDER BY d.track_popularity DESC
        ) AS artist_diversity_rank,
        
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(d.seed_genre, 'unknown')
            ORDER BY d.track_popularity DESC
        ) AS genre_diversity_rank
        
    FROM ml_spotify_discoveries d
    LEFT JOIN user_preferences up ON LOWER(d.seed_genre) = LOWER(up.primary_genre)
    LEFT JOIN artist_preferences ap ON d.primary_artist_name = ap.primary_artist_name
    WHERE d.discovered_at >= DATEADD('days', -7, CURRENT_TIMESTAMP)  -- Recent discoveries only
)
SELECT 
    track_id,
    track_name,
    primary_artist_name,
    primary_artist_id,
    album_name,
    album_release_date,
    track_popularity,
    track_duration_ms,
    preview_url,
    discovery_strategy,
    seed_artist,
    seed_genre,
    seed_preference_score,
    
    -- Final recommendation score with diversity penalty
    calculated_recommendation_score * 
    CASE 
        WHEN artist_diversity_rank = 1 THEN 1.0        -- First track from artist
        WHEN artist_diversity_rank = 2 THEN 0.7        -- Second track penalty
        WHEN artist_diversity_rank = 3 THEN 0.4        -- Third track penalty  
        ELSE 0.2                                       -- Heavy penalty for 4+
    END * 
    CASE
        WHEN genre_diversity_rank <= 3 THEN 1.0       -- Top 3 per genre
        WHEN genre_diversity_rank <= 5 THEN 0.8       -- 4-5 per genre
        ELSE 0.5                                       -- 6+ per genre
    END AS final_recommendation_score,
    
    artist_diversity_rank,
    genre_diversity_rank,
    discovered_at,
    
    ROW_NUMBER() OVER (ORDER BY 
        calculated_recommendation_score * 
        CASE 
            WHEN artist_diversity_rank = 1 THEN 1.0
            WHEN artist_diversity_rank = 2 THEN 0.7
            WHEN artist_diversity_rank = 3 THEN 0.4
            ELSE 0.2
        END * 
        CASE
            WHEN genre_diversity_rank <= 3 THEN 1.0
            WHEN genre_diversity_rank <= 5 THEN 0.8
            ELSE 0.5
        END DESC
    ) AS recommendation_rank
    
FROM discovery_scoring
WHERE calculated_recommendation_score > 0.2  -- Minimum quality threshold
ORDER BY final_recommendation_score DESC;

-- Discovery Analytics View
CREATE OR REPLACE VIEW ml_discovery_analytics AS
SELECT 
    -- Discovery summary stats
    COUNT(*) as total_discoveries,
    COUNT(DISTINCT primary_artist_name) as unique_artists_discovered,
    COUNT(DISTINCT seed_genre) as genres_explored,
    COUNT(DISTINCT discovery_strategy) as strategies_used,
    AVG(track_popularity) as avg_discovery_popularity,
    
    -- Discovery breakdown by strategy
    COUNT(CASE WHEN discovery_strategy = 'artist_based' THEN 1 END) as artist_based_discoveries,
    COUNT(CASE WHEN discovery_strategy = 'genre_based' THEN 1 END) as genre_based_discoveries,
    
    -- Quality metrics
    COUNT(CASE WHEN final_recommendation_score > 0.7 THEN 1 END) as high_quality_discoveries,
    COUNT(CASE WHEN final_recommendation_score BETWEEN 0.4 AND 0.7 THEN 1 END) as medium_quality_discoveries,
    COUNT(CASE WHEN final_recommendation_score < 0.4 THEN 1 END) as low_quality_discoveries,
    
    -- Recency
    MAX(discovered_at) as latest_discovery,
    MIN(discovered_at) as earliest_discovery,
    
    -- Top discoveries
    LISTAGG(
        CASE WHEN recommendation_rank <= 5 
        THEN track_name || ' by ' || primary_artist_name 
        END, '; '
    ) as top_5_recommendations
    
FROM ml_smart_discovery_recommendations;

-- Quick Discovery Test View  
CREATE OR REPLACE VIEW ml_discovery_quick_test AS
SELECT 
    'Total Discovered Tracks' as metric,
    COUNT(*) as value,
    '' as details
FROM ml_spotify_discoveries

UNION ALL

SELECT 
    'Recent High-Quality Recommendations' as metric,
    COUNT(*) as value,
    'Score > 0.6, Discovered in last 7 days' as details
FROM ml_smart_discovery_recommendations
WHERE final_recommendation_score > 0.6

UNION ALL

SELECT 
    'Top Discovery Strategy' as metric,
    0 as value,
    discovery_strategy as details
FROM ml_spotify_discoveries
GROUP BY discovery_strategy
ORDER BY COUNT(*) DESC
LIMIT 1

UNION ALL

SELECT 
    'Most Discovered Artist' as metric,
    COUNT(*) as value,
    primary_artist_name as details  
FROM ml_spotify_discoveries
GROUP BY primary_artist_name
ORDER BY COUNT(*) DESC
LIMIT 1;

-- Sample Discovery Recommendations (Top 10)
CREATE OR REPLACE VIEW ml_top_discovery_recommendations AS
SELECT 
    recommendation_rank,
    track_name,
    primary_artist_name,
    COALESCE(seed_genre, seed_artist) as discovery_seed,
    discovery_strategy,
    track_popularity,
    ROUND(final_recommendation_score, 3) as score,
    preview_url,
    'Listen on Spotify: https://open.spotify.com/track/' || track_id as spotify_link
FROM ml_smart_discovery_recommendations
WHERE recommendation_rank <= 10
ORDER BY recommendation_rank;

-- Test all discovery views
SELECT 'Discovery Analytics Test' as test_name;
SELECT * FROM ml_discovery_analytics;

SELECT 'Quick Discovery Test' as test_name;  
SELECT * FROM ml_discovery_quick_test;

SELECT 'Top 10 Recommendations Test' as test_name;
SELECT * FROM ml_top_discovery_recommendations;
