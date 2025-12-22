-- Generate User Music Profile for Discovery System
-- This creates a JSON file with your listening preferences for the Python discovery script
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- Generate comprehensive user profile
WITH user_stats AS (
    SELECT 
        primary_genre,
        primary_artist_name,
        primary_artist_id,
        era_category,
        track_popularity,
        track_duration_ms,
        user_play_count,
        COUNT(*) OVER () as total_listens
    FROM ml_track_content_features
    WHERE user_play_count >= 1
),
genre_preferences AS (
    SELECT 
        primary_genre,
        COUNT(*) as genre_listens,
        AVG(user_play_count) as avg_plays_per_track,
        COUNT(*) * 1.0 / MAX(total_listens) as genre_preference
    FROM user_stats
    GROUP BY primary_genre, total_listens
    ORDER BY genre_preference DESC
    LIMIT 10
),
artist_preferences AS (
    SELECT 
        primary_artist_name,
        COUNT(*) as artist_listens,
        AVG(user_play_count) as avg_plays_per_track,
        COUNT(*) * 1.0 / MAX(total_listens) as artist_preference
    FROM user_stats
    GROUP BY primary_artist_name, total_listens
    ORDER BY artist_preference DESC
    LIMIT 10
),
listening_stats AS (
    SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY track_popularity) as pop_25,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY track_popularity) as pop_75,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY track_duration_ms) as dur_25,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY track_duration_ms) as dur_75,
        (SELECT era_category FROM ml_track_content_features 
         WHERE user_play_count >= 1 
         GROUP BY era_category 
         ORDER BY COUNT(*) DESC 
         LIMIT 1) as preferred_era,
        COUNT(DISTINCT primary_genre) * 1.0 / 50 as discovery_openness  -- Normalized
    FROM ml_track_content_features
    WHERE user_play_count >= 1
),
-- Generate arrays for top genres and artists
top_genres_array AS (
    SELECT ARRAY_AGG(
        OBJECT_CONSTRUCT('name', primary_genre, 'score', genre_preference)
    ) as genres_array
    FROM genre_preferences
),
top_artists_array AS (
    SELECT ARRAY_AGG(
        OBJECT_CONSTRUCT('name', primary_artist_name, 'score', artist_preference)
    ) as artists_array
    FROM artist_preferences
),
profile_json AS (
    SELECT 
        OBJECT_CONSTRUCT(
            'top_genres', g.genres_array,
            'top_artists', a.artists_array,
            'preferred_popularity_range', ARRAY_CONSTRUCT(s.pop_25, s.pop_75),
            'preferred_eras', ARRAY_CONSTRUCT(s.preferred_era),
            'preferred_duration_range', ARRAY_CONSTRUCT(s.dur_25, s.dur_75),
            'discovery_openness', s.discovery_openness,
            'generated_at', CURRENT_TIMESTAMP(),
            'stats', OBJECT_CONSTRUCT(
                'total_genres', (SELECT COUNT(DISTINCT primary_genre) FROM user_stats),
                'total_artists', (SELECT COUNT(DISTINCT primary_artist_name) FROM user_stats),
                'total_tracks', (SELECT COUNT(DISTINCT track_id) FROM ml_track_content_features WHERE user_play_count >= 1),
                'avg_popularity', (SELECT AVG(track_popularity) FROM user_stats),
                'avg_duration_min', (SELECT AVG(track_duration_ms) / 60000.0 FROM user_stats)
            )
        ) as user_profile_json
    FROM listening_stats s
    CROSS JOIN top_genres_array g
    CROSS JOIN top_artists_array a
)
-- ============================================================================
-- COPY THIS JSON TO user_music_profile.json FILE
-- ============================================================================
SELECT 
    '🎵 COPY THIS JSON TO user_music_profile.json 🎵' as instructions,
    user_profile_json
FROM profile_json;