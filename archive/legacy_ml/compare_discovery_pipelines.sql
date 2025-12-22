-- Discovery Pipeline A/B Testing Dashboard
-- Compare Smart Search vs. ML Hybrid discovery approaches
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- 1. Pipeline Performance Comparison
SELECT 
    '📊 PIPELINE PERFORMANCE COMPARISON' AS analysis_type,
    pipeline_type,
    total_discoveries,
    avg_popularity,
    unique_artists,
    unique_genres,
    latest_discovery
FROM discovery_pipeline_comparison
ORDER BY total_discoveries DESC;

-- 2. Quality Analysis - Smart Search vs ML
WITH smart_search_quality AS (
    SELECT 
        'Smart Search' AS pipeline,
        COUNT(*) AS total_tracks,
        AVG(track_popularity) AS avg_popularity,
        COUNT(DISTINCT discovery_strategy) AS strategies_used,
        AVG(COALESCE(preference_score, 0.5)) AS avg_preference_score,
        COUNT(DISTINCT primary_genre) AS genre_diversity,
        MIN(created_at) AS first_discovery,
        MAX(created_at) AS latest_discovery
    FROM ml_spotify_discoveries
    WHERE created_at >= CURRENT_DATE - 7  -- Last 7 days
),
ml_quality AS (
    SELECT 
        'ML Hybrid' AS pipeline,
        COUNT(*) AS total_tracks,
        AVG(track_popularity) AS avg_popularity,
        COUNT(DISTINCT discovery_strategy) AS strategies_used,
        AVG(ml_recommendation_score) AS avg_recommendation_score,
        COUNT(DISTINCT seed_genre) AS genre_diversity,
        MIN(created_at) AS first_discovery,
        MAX(created_at) AS latest_discovery
    FROM ml_spotify_ml_discoveries
    WHERE created_at >= CURRENT_DATE - 7  -- Last 7 days
)
SELECT 
    '🎯 QUALITY METRICS (Last 7 Days)' AS analysis_type,
    pipeline,
    total_tracks,
    ROUND(avg_popularity, 2) AS avg_popularity,
    strategies_used,
    ROUND(avg_recommendation_score, 3) AS avg_score,
    genre_diversity,
    first_discovery,
    latest_discovery
FROM smart_search_quality
UNION ALL  
SELECT 
    '🎯 QUALITY METRICS (Last 7 Days)' AS analysis_type,
    pipeline,
    total_tracks,
    ROUND(avg_popularity, 2) AS avg_popularity,
    strategies_used,
    ROUND(avg_recommendation_score, 3) AS avg_score,
    genre_diversity,
    first_discovery,
    latest_discovery
FROM ml_quality
ORDER BY total_tracks DESC;

-- 3. Diversity and Novelty Analysis
SELECT 
    '🌈 DIVERSITY ANALYSIS' AS analysis_type,
    'Smart Search' AS pipeline_type,
    COUNT(DISTINCT seed_genre) AS genre_diversity,
    COUNT(DISTINCT primary_artist_name) AS artist_diversity,
    ROUND(AVG(track_popularity), 2) AS avg_popularity,
    COUNT(CASE WHEN track_popularity < 30 THEN 1 END) AS underground_tracks,
    COUNT(CASE WHEN track_popularity > 70 THEN 1 END) AS mainstream_tracks
FROM ml_spotify_discoveries
UNION ALL
SELECT 
    '🌈 DIVERSITY ANALYSIS' AS analysis_type,
    'ML Hybrid' AS pipeline_type,
    COUNT(DISTINCT seed_genre) AS genre_diversity,
    COUNT(DISTINCT primary_artist_name) AS artist_diversity,
    ROUND(AVG(track_popularity), 2) AS avg_popularity,
    COUNT(CASE WHEN track_popularity < 30 THEN 1 END) AS underground_tracks,
    COUNT(CASE WHEN track_popularity > 70 THEN 1 END) AS mainstream_tracks
FROM ml_spotify_ml_discoveries
ORDER BY underground_tracks DESC;

-- 4. Top Discoveries by Pipeline
SELECT 
    '🎵 TOP DISCOVERIES - SMART SEARCH' AS analysis_type,
    track_name,
    primary_artist_name,
    seed_genre,
    track_popularity,
    discovery_strategy,
    ROUND(COALESCE(preference_score, 0.5), 3) AS score,
    created_at
FROM ml_spotify_discoveries
ORDER BY COALESCE(preference_score, track_popularity) DESC
LIMIT 10;

SELECT 
    '🧠 TOP DISCOVERIES - ML HYBRID' AS analysis_type,
    track_name,
    primary_artist_name,
    seed_genre,
    track_popularity,
    discovery_strategy,
    ROUND(ml_recommendation_score, 3) AS ml_score,
    ml_strategies_used,
    created_at
FROM ml_spotify_ml_discoveries
ORDER BY ml_recommendation_score DESC
LIMIT 10;

-- 5. Discovery Strategy Breakdown
SELECT 
    '📈 STRATEGY BREAKDOWN - SMART SEARCH' AS analysis_type,
    discovery_strategy,
    COUNT(*) AS track_count,
    ROUND(AVG(track_popularity), 2) AS avg_popularity,
    COUNT(DISTINCT primary_artist_name) AS unique_artists
FROM ml_spotify_discoveries
GROUP BY discovery_strategy
ORDER BY track_count DESC;

SELECT 
    '🤖 STRATEGY BREAKDOWN - ML HYBRID' AS analysis_type,
    discovery_strategy,
    COUNT(*) AS track_count,
    ROUND(AVG(track_popularity), 2) AS avg_popularity,
    ROUND(AVG(ml_recommendation_score), 3) AS avg_ml_score,
    COUNT(DISTINCT primary_artist_name) AS unique_artists
FROM ml_spotify_ml_discoveries
GROUP BY discovery_strategy
ORDER BY track_count DESC;

-- 6. Genre Distribution Comparison
WITH smart_genres AS (
    SELECT 
        'Smart Search' AS pipeline,
        seed_genre,
        COUNT(*) AS track_count,
        ROUND(AVG(track_popularity), 2) AS avg_popularity
    FROM ml_spotify_discoveries
    WHERE seed_genre IS NOT NULL
    GROUP BY seed_genre
),
ml_genres AS (
    SELECT 
        'ML Hybrid' AS pipeline,
        seed_genre,
        COUNT(*) AS track_count,
        ROUND(AVG(track_popularity), 2) AS avg_popularity
    FROM ml_spotify_ml_discoveries
    WHERE seed_genre IS NOT NULL
    GROUP BY seed_genre
)
SELECT 
    '🎭 GENRE DISTRIBUTION COMPARISON' AS analysis_type,
    COALESCE(s.seed_genre, m.seed_genre) AS genre,
    COALESCE(s.track_count, 0) AS smart_search_tracks,
    COALESCE(m.track_count, 0) AS ml_hybrid_tracks,
    COALESCE(s.avg_popularity, 0) AS smart_avg_popularity,
    COALESCE(m.avg_popularity, 0) AS ml_avg_popularity
FROM smart_genres s
FULL OUTER JOIN ml_genres m ON s.seed_genre = m.seed_genre
ORDER BY COALESCE(s.track_count, 0) + COALESCE(m.track_count, 0) DESC
LIMIT 15;

-- 7. Recent Activity Timeline
SELECT 
    '⏰ RECENT ACTIVITY TIMELINE' AS analysis_type,
    DATE_TRUNC('hour', created_at) AS discovery_hour,
    COUNT(CASE WHEN 'Smart Search' = 'Smart Search' THEN 1 END) AS smart_discoveries,
    COUNT(CASE WHEN 'ML Hybrid' = 'ML Hybrid' THEN 1 END) AS ml_discoveries
FROM (
    SELECT created_at, 'Smart Search' AS pipeline FROM ml_spotify_discoveries WHERE created_at >= CURRENT_DATE - 2
    UNION ALL
    SELECT created_at, 'ML Hybrid' AS pipeline FROM ml_spotify_ml_discoveries WHERE created_at >= CURRENT_DATE - 2
) combined
GROUP BY DATE_TRUNC('hour', created_at)
ORDER BY discovery_hour DESC
LIMIT 24;  -- Last 24 hours

-- 8. Recommendation Score Distribution (ML only)
SELECT 
    '📊 ML SCORE DISTRIBUTION' AS analysis_type,
    CASE 
        WHEN ml_recommendation_score >= 0.8 THEN 'Excellent (0.8+)'
        WHEN ml_recommendation_score >= 0.6 THEN 'Good (0.6-0.8)'
        WHEN ml_recommendation_score >= 0.4 THEN 'Fair (0.4-0.6)'
        ELSE 'Low (<0.4)'
    END AS score_range,
    COUNT(*) AS track_count,
    ROUND(AVG(track_popularity), 2) AS avg_popularity
FROM ml_spotify_ml_discoveries
WHERE ml_recommendation_score IS NOT NULL
GROUP BY 
    CASE 
        WHEN ml_recommendation_score >= 0.8 THEN 'Excellent (0.8+)'
        WHEN ml_recommendation_score >= 0.6 THEN 'Good (0.6-0.8)'
        WHEN ml_recommendation_score >= 0.4 THEN 'Fair (0.4-0.6)'
        ELSE 'Low (<0.4)'
    END
ORDER BY 
    CASE 
        WHEN score_range = 'Excellent (0.8+)' THEN 1
        WHEN score_range = 'Good (0.6-0.8)' THEN 2
        WHEN score_range = 'Fair (0.4-0.6)' THEN 3
        ELSE 4
    END;

-- 9. Summary Recommendations
SELECT 
    '🏆 PIPELINE COMPARISON SUMMARY' AS analysis_type,
    '📊 Check total discoveries, diversity, and quality metrics above' AS instruction_1,
    '🎯 Smart Search: Good for broad discovery based on user preferences' AS smart_search_strength,
    '🧠 ML Hybrid: Better for personalized recommendations using listening patterns' AS ml_hybrid_strength,
    '⚖️  Choose based on: discovery count, genre diversity, and recommendation accuracy' AS decision_criteria,
    '🔄 Run both pipelines regularly for comprehensive music discovery' AS recommendation;
