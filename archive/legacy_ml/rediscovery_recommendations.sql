-- REDISCOVERY SYSTEM: Recommend tracks you've played least from your existing library
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- Rediscovery Candidate Tracks (tracks played 1-2 times)
CREATE OR REPLACE VIEW ml_rediscovery_candidates AS
SELECT 
    track_id,
    track_name,
    primary_artist_name,
    primary_genre,
    album_name,
    track_popularity,
    track_duration_ms,
    album_release_date,
    era_category,
    popularity_tier,
    duration_category,
    artist_avg_popularity,
    genre_avg_popularity,
    user_play_count,
    
    -- Add recency factor (older listens = higher rediscovery potential)
    DATEDIFF('days', last_played_date, CURRENT_DATE) AS days_since_last_played,
    
    CURRENT_TIMESTAMP AS generated_at
    
FROM ml_track_content_features
WHERE user_play_count <= 3  -- Tracks played 3 times or less
AND user_play_count >= 1    -- But at least once (exist in your library)
ORDER BY user_play_count ASC, days_since_last_played DESC;

-- Rediscovery Collaborative Filtering
CREATE OR REPLACE VIEW ml_rediscovery_collaborative AS
SELECT 
    rc.track_id,
    rc.track_name,
    rc.primary_artist_name,
    rc.primary_genre,
    rc.album_name,
    rc.track_popularity,
    
    -- Scoring: Genre preference + Rarity bonus + Recency bonus
    COALESCE(ugi.weighted_preference, 0.1) * 
    (1.0 / rc.user_play_count) *  -- Rarity bonus (less played = higher score)
    (CASE WHEN rc.days_since_last_played > 30 THEN 1.5 ELSE 1.0 END) *  -- Recency bonus
    (rc.track_popularity / 100.0) AS rediscovery_score,
    
    'rediscovery_collaborative' AS recommendation_strategy,
    rc.user_play_count,
    rc.days_since_last_played,
    
    ROW_NUMBER() OVER (ORDER BY 
        COALESCE(ugi.weighted_preference, 0.1) * 
        (1.0 / rc.user_play_count) *
        (CASE WHEN rc.days_since_last_played > 30 THEN 1.5 ELSE 1.0 END) *
        (rc.track_popularity / 100.0) DESC
    ) AS rank,
    
    CURRENT_TIMESTAMP AS generated_at
    
FROM ml_rediscovery_candidates rc
LEFT JOIN ml_user_genre_interactions ugi ON rc.primary_genre = ugi.primary_genre
WHERE rc.track_popularity > 20
ORDER BY rediscovery_score DESC
LIMIT 50;

-- Test rediscovery system
SELECT 'Rediscovery Candidates' as test_type, COUNT(*) as count FROM ml_rediscovery_candidates;
SELECT 'Rediscovery Recommendations' as test_type, COUNT(*) as count FROM ml_rediscovery_collaborative;

-- Sample rediscovery recommendations
SELECT 
    track_name,
    primary_artist_name,
    primary_genre,
    user_play_count,
    days_since_last_played,
    ROUND(rediscovery_score, 4) as score
FROM ml_rediscovery_collaborative
ORDER BY rediscovery_score DESC
LIMIT 10;
