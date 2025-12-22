-- Test Both Discovery Systems: Rediscovery (existing tracks) + True Discovery (new Spotify tracks)
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- ==============================================
-- PART 1: TEST REDISCOVERY SYSTEM (Quick Win)
-- ==============================================

SELECT '=== REDISCOVERY SYSTEM TESTS ===' as test_section;

-- Test 1: Check rediscovery candidates
SELECT 'Rediscovery Candidates (tracks played 1-3 times)' as test_name, COUNT(*) as count
FROM ml_track_content_features
WHERE user_play_count <= 3 AND user_play_count >= 1;

-- Test 2: Sample rediscovery candidates  
SELECT 'Sample Rediscovery Candidates' as test_name;
SELECT 
    track_name,
    primary_artist_name,
    primary_genre,
    user_play_count,
    DATEDIFF('days', last_played_date, CURRENT_DATE) as days_since_played
FROM ml_track_content_features
WHERE user_play_count <= 3 AND user_play_count >= 1
ORDER BY user_play_count ASC, days_since_played DESC
LIMIT 10;

-- Test 3: Check if rediscovery views work (run after deploying rediscovery_recommendations.sql)
-- Uncomment after running rediscovery_recommendations.sql:
/*
SELECT 'Rediscovery Recommendations Count' as test_name, COUNT(*) as count
FROM ml_rediscovery_collaborative;

SELECT 'Top Rediscovery Recommendations' as test_name;
SELECT 
    track_name,
    primary_artist_name,
    primary_genre,
    user_play_count,
    days_since_last_played,
    ROUND(rediscovery_score, 4) as score
FROM ml_rediscovery_collaborative
ORDER BY rediscovery_score DESC
LIMIT 5;
*/

-- ==============================================  
-- PART 2: TEST TRUE DISCOVERY SYSTEM SETUP
-- ==============================================

SELECT '=== TRUE DISCOVERY SYSTEM TESTS ===' as test_section;

-- Test 4: Check if discovery table exists (run after Python discovery)
-- This will fail initially - that's expected
SELECT 'Discovery Table Status' as test_name;
SELECT 
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = 'ML_SPOTIFY_DISCOVERIES' 
            AND table_schema = 'ANALYTICS'
        ) 
        THEN 'EXISTS - Run discovery views!'
        ELSE 'NOT EXISTS - Run Python discovery first!'
    END as status;

-- Test 5: User profile analysis (shows what we'll search for)
SELECT 'User Listening Profile Analysis' as test_name;

-- Top genres for discovery seeds
SELECT 'Top Genres (Discovery Seeds)' as analysis_type;
SELECT 
    primary_genre,
    weighted_preference,
    total_listening_time,
    RANK() OVER (ORDER BY weighted_preference DESC) as genre_rank
FROM ml_user_genre_interactions
WHERE weighted_preference > 0.1
ORDER BY weighted_preference DESC
LIMIT 5;

-- Top artists for discovery seeds  
SELECT 'Top Artists (Discovery Seeds)' as analysis_type;
SELECT 
    primary_artist_name,
    COUNT(*) as track_count,
    SUM(user_play_count) as total_plays,
    AVG(user_play_count) as avg_plays_per_track
FROM ml_track_content_features
GROUP BY primary_artist_name
ORDER BY total_plays DESC
LIMIT 5;

-- User preferences summary
SELECT 'User Preferences Summary' as analysis_type;
SELECT 
    'Popularity Range' as preference_type,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY track_popularity) || ' - ' ||
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY track_popularity) as range_value
FROM ml_track_content_features
WHERE user_play_count >= 1

UNION ALL

SELECT 
    'Duration Range (minutes)' as preference_type,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY track_duration_ms) / 60000, 1) || ' - ' ||
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY track_duration_ms) / 60000, 1) as range_value
FROM ml_track_content_features
WHERE user_play_count >= 1

UNION ALL

SELECT 
    'Preferred Era' as preference_type,
    MODE() WITHIN GROUP (ORDER BY era_category) as range_value
FROM ml_track_content_features
WHERE user_play_count >= 1

UNION ALL

SELECT 
    'Genre Diversity' as preference_type,
    COUNT(DISTINCT primary_genre) || ' different genres' as range_value
FROM ml_track_content_features
WHERE user_play_count >= 1;

-- ==============================================
-- PART 3: SYSTEM READINESS CHECK
-- ==============================================

SELECT '=== SYSTEM READINESS CHECK ===' as test_section;

-- Overall system status
SELECT 'System Component Status' as test_name;

SELECT 
    'ml_user_genre_interactions' as component,
    COUNT(*) as records,
    CASE WHEN COUNT(*) > 0 THEN '✅ READY' ELSE '❌ MISSING' END as status
FROM ml_user_genre_interactions

UNION ALL

SELECT 
    'ml_track_content_features' as component,
    COUNT(*) as records,
    CASE WHEN COUNT(*) > 0 THEN '✅ READY' ELSE '❌ MISSING' END as status  
FROM ml_track_content_features

UNION ALL

SELECT 
    'spotify_client_config' as component,
    1 as records,
    CASE WHEN LENGTH($SPOTIFY_CLIENT_ID) > 10 THEN '✅ READY' ELSE '❌ CHECK CONFIG' END as status

UNION ALL

SELECT 
    'rediscovery_candidates' as component,
    COUNT(*) as records,
    CASE WHEN COUNT(*) >= 10 THEN '✅ READY' ELSE '⚠️ LOW DATA' END as status
FROM ml_track_content_features
WHERE user_play_count <= 3 AND user_play_count >= 1;

-- ==============================================
-- DEPLOYMENT INSTRUCTIONS
-- ============================================== 

SELECT '=== NEXT STEPS ===' as test_section;

SELECT 
'DEPLOYMENT INSTRUCTIONS:

🚀 QUICK WIN - REDISCOVERY SYSTEM:
1. Run: rediscovery_recommendations.sql
2. Test query: SELECT * FROM ml_rediscovery_collaborative LIMIT 10;
3. ✅ Get working recommendations immediately!

🎯 FULL DISCOVERY SYSTEM:
1. Ensure Spotify API credentials in config.py
2. Run: python spotify_discovery_system.py  
3. Run: discovery_recommendation_views.sql
4. Test query: SELECT * FROM ml_top_discovery_recommendations;
5. ✅ Get completely new music from Spotify!

📊 EXPECTED RESULTS:
- Rediscovery: ~50-200 recommendations from existing library
- True Discovery: ~50 new tracks from Spotify catalog
- Combined: Personalized music discovery system!

⚡ IMMEDIATE ACTION:
Run rediscovery_recommendations.sql now for instant results!
' as instructions;
