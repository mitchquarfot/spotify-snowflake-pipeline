-- Test S3-Based Discovery Pipeline End-to-End
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- ==============================================
-- TEST 1: VERIFY REDISCOVERY SYSTEM (Working)
-- ==============================================

SELECT '=== REDISCOVERY SYSTEM STATUS ===' as test_section;

SELECT 
    'Rediscovery Recommendations' as component,
    COUNT(*) as count,
    CASE WHEN COUNT(*) > 0 THEN '✅ WORKING' ELSE '❌ NEEDS DEPLOYMENT' END as status
FROM ml_rediscovery_collaborative;

-- ==============================================
-- TEST 2: CHECK USER PROFILE GENERATION
-- ==============================================

SELECT '=== USER PROFILE GENERATION ===' as test_section;

-- Test profile data availability
SELECT 'User Profile Data Check' as test_name;
SELECT 
    'Total Genres' as metric,
    COUNT(DISTINCT primary_genre) as value,
    'Should be 5-20 for good diversity' as note
FROM ml_track_content_features
WHERE user_play_count >= 1

UNION ALL

SELECT 
    'Total Artists' as metric,
    COUNT(DISTINCT primary_artist_name) as value,
    'Should be 50+ for good recommendations' as note
FROM ml_track_content_features
WHERE user_play_count >= 1

UNION ALL

SELECT 
    'Total Unique Tracks' as metric,
    COUNT(DISTINCT track_id) as value,
    'Base data for preference analysis' as note
FROM ml_track_content_features
WHERE user_play_count >= 1;

-- Show top preference seeds for discovery
SELECT 'Top Genre Seeds for Spotify Search' as test_name;
SELECT 
    primary_genre,
    COUNT(*) as listens,
    ROUND(COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (), 3) as preference_score
FROM ml_track_content_features
WHERE user_play_count >= 1
GROUP BY primary_genre
ORDER BY preference_score DESC
LIMIT 5;

-- ==============================================
-- TEST 3: VERIFY S3/SNOWPIPE INFRASTRUCTURE
-- ==============================================

SELECT '=== S3/SNOWPIPE INFRASTRUCTURE ===' as test_section;

-- Check if discovery tables exist
SELECT 'Discovery Infrastructure Status' as test_name;
SELECT 
    table_name,
    CASE WHEN table_name IS NOT NULL THEN '✅ EXISTS' ELSE '❌ MISSING' END as status,
    'Run setup_discovery_snowpipe.sql' as action_if_missing
FROM information_schema.tables 
WHERE table_name IN ('RAW_SPOTIFY_DISCOVERIES', 'ML_SPOTIFY_DISCOVERIES')
AND table_schema = 'ANALYTICS'

UNION ALL

SELECT 
    'DISCOVERY_S3_STAGE' as table_name,
    CASE WHEN COUNT(*) > 0 THEN '✅ EXISTS' ELSE '❌ MISSING' END as status,
    'Run setup_discovery_snowpipe.sql' as action_if_missing
FROM information_schema.stages 
WHERE stage_name = 'DISCOVERY_S3_STAGE'

UNION ALL

SELECT 
    'DISCOVERY_SNOWPIPE' as table_name,
    CASE WHEN COUNT(*) > 0 THEN '✅ EXISTS' ELSE '❌ MISSING' END as status,
    'Run setup_discovery_snowpipe.sql' as action_if_missing
FROM information_schema.pipes 
WHERE pipe_name = 'DISCOVERY_SNOWPIPE';

-- ==============================================
-- TEST 4: CHECK FOR EXISTING DISCOVERIES
-- ==============================================

SELECT '=== EXISTING SPOTIFY DISCOVERIES ===' as test_section;

-- Check raw discoveries table
SELECT 
    'Raw Discoveries Count' as metric,
    COUNT(*) as count,
    CASE 
        WHEN COUNT(*) > 0 THEN '✅ DATA FOUND'
        ELSE '⚠️ NO DATA YET - Run Python script'
    END as status
FROM raw_spotify_discoveries
WHERE ingested_at >= DATEADD('days', -7, CURRENT_TIMESTAMP());

-- Check processed discoveries table
SELECT 
    'Processed Discoveries Count' as metric,
    COUNT(*) as count,
    CASE 
        WHEN COUNT(*) > 0 THEN '✅ READY FOR RECOMMENDATIONS'
        ELSE '⚠️ NO DATA YET - Run Python script'
    END as status
FROM ml_spotify_discoveries
WHERE created_at >= DATEADD('days', -7, CURRENT_TIMESTAMP());

-- Sample existing discoveries (if any)
SELECT 'Sample Discovered Tracks (Recent)' as test_name;
SELECT 
    track_name,
    primary_artist_name,
    discovery_strategy,
    COALESCE(seed_genre, seed_artist) as discovery_seed,
    track_popularity,
    discovered_at
FROM ml_spotify_discoveries
ORDER BY discovered_at DESC
LIMIT 5;

-- ==============================================
-- TEST 5: DISCOVERY RECOMMENDATION VIEWS
-- ==============================================

SELECT '=== DISCOVERY RECOMMENDATION VIEWS ===' as test_section;

-- Check if discovery recommendation views exist
SELECT 
    'Discovery Views Status' as test_name,
    view_name,
    CASE WHEN view_name IS NOT NULL THEN '✅ EXISTS' ELSE '❌ MISSING' END as status
FROM information_schema.views 
WHERE view_name IN (
    'ML_SMART_DISCOVERY_RECOMMENDATIONS',
    'ML_DISCOVERY_ANALYTICS', 
    'ML_TOP_DISCOVERY_RECOMMENDATIONS'
)
AND table_schema = 'ANALYTICS';

-- Test discovery recommendations (if data exists)
SELECT 'Discovery Recommendations Test' as test_name;
SELECT 
    COUNT(*) as recommendation_count,
    CASE 
        WHEN COUNT(*) > 0 THEN '✅ WORKING - You have new music!'
        WHEN COUNT(*) = 0 AND EXISTS(SELECT 1 FROM ml_spotify_discoveries LIMIT 1) THEN '⚠️ DATA BUT NO RECS - Check filters'
        ELSE '⏳ WAITING FOR DATA - Run Python script first'
    END as status
FROM ml_smart_discovery_recommendations;

-- ==============================================
-- DEPLOYMENT STATUS SUMMARY
-- ==============================================

SELECT '=== DEPLOYMENT STATUS SUMMARY ===' as test_section;

WITH status_check AS (
    SELECT 
        EXISTS(SELECT 1 FROM ml_rediscovery_collaborative LIMIT 1) as rediscovery_working,
        EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'ML_SPOTIFY_DISCOVERIES') as tables_created,
        EXISTS(SELECT 1 FROM information_schema.stages WHERE stage_name = 'DISCOVERY_S3_STAGE') as stage_created,
        EXISTS(SELECT 1 FROM information_schema.pipes WHERE pipe_name = 'DISCOVERY_SNOWPIPE') as pipe_created,
        EXISTS(SELECT 1 FROM ml_spotify_discoveries LIMIT 1) as discoveries_exist,
        EXISTS(SELECT 1 FROM ml_smart_discovery_recommendations LIMIT 1) as recommendations_working
)
SELECT 
    '🎵 REDISCOVERY SYSTEM' as component,
    CASE WHEN rediscovery_working THEN '✅ WORKING' ELSE '❌ DEPLOY FIRST' END as status,
    CASE WHEN NOT rediscovery_working THEN 'Run rediscovery_recommendations.sql' ELSE 'Ready!' END as action
FROM status_check

UNION ALL

SELECT 
    '🏗️ S3/SNOWPIPE INFRASTRUCTURE' as component,
    CASE WHEN tables_created AND stage_created AND pipe_created THEN '✅ READY' ELSE '❌ SETUP NEEDED' END as status,
    CASE WHEN NOT (tables_created AND stage_created AND pipe_created) THEN 'Run setup_discovery_snowpipe.sql' ELSE 'Ready!' END as action
FROM status_check

UNION ALL

SELECT 
    '🐍 PYTHON DISCOVERY SCRIPT' as component,
    CASE WHEN discoveries_exist THEN '✅ DATA FOUND' ELSE '⏳ WAITING' END as status,
    CASE WHEN NOT discoveries_exist THEN 'Run python spotify_discovery_system.py' ELSE 'Data loaded!' END as action
FROM status_check

UNION ALL

SELECT 
    '🎯 DISCOVERY RECOMMENDATIONS' as component,
    CASE WHEN recommendations_working THEN '✅ WORKING' ELSE '⏳ WAITING FOR DATA' END as status,
    CASE WHEN NOT recommendations_working THEN 'Run discovery_recommendation_views.sql after Python script' ELSE 'New music ready!' END as action
FROM status_check;

-- ==============================================
-- NEXT STEPS GUIDE
-- ==============================================

SELECT 
'🚀 YOUR S3 DISCOVERY PIPELINE STATUS:

✅ WORKING NOW:
- Rediscovery System: Find forgotten gems from your library

🔧 SETUP NEEDED:
1. Run setup_discovery_snowpipe.sql (creates S3 ingestion)
2. Update S3 bucket name and credentials in the script
3. Configure S3 event notifications (auto-trigger Snowpipe)

🎵 DISCOVER NEW MUSIC:
1. Run generate_user_profile.sql (analyze your preferences) 
2. Copy JSON output to user_music_profile.json file
3. Run python spotify_discovery_system.py (fetch new tracks)
4. Run discovery_recommendation_views.sql (create recommendation views)
5. Query SELECT * FROM ml_top_discovery_recommendations;

📊 EXPECTED RESULTS:
- Rediscovery: 50-200 tracks from your library
- New Discovery: 50+ completely new tracks from Spotify
- Combined: Endless personalized music discovery!

💡 TIP: Start with rediscovery while setting up the full S3 pipeline!
' as next_steps;
