-- Diagnose Why Discovery Script Found 0 Tracks
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

SELECT '=== DISCOVERY ISSUE DIAGNOSIS ===' as section;

-- 1. Check if core ML views exist (needed for user profile generation)
SELECT 'CORE ML VIEWS STATUS' as check_type;
SELECT 
    table_name as view_name,
    '✅ EXISTS' as status
FROM spotify_analytics.information_schema.views 
WHERE table_name IN (
    'ML_TRACK_CONTENT_FEATURES',
    'ML_USER_GENRE_INTERACTIONS'
)
AND table_schema = 'ANALYTICS'

UNION ALL

SELECT 
    'ML_TRACK_CONTENT_FEATURES' as view_name,
    '❌ MISSING - DEPLOY spotify_ml_recommendation_engine.sql' as status
WHERE NOT EXISTS (
    SELECT 1 FROM spotify_analytics.information_schema.views 
    WHERE table_name = 'ML_TRACK_CONTENT_FEATURES' AND table_schema = 'ANALYTICS'
);

-- 2. Check if medallion architecture exists (needed for ML views)
SELECT 'MEDALLION ARCHITECTURE STATUS' as check_type;
SELECT 
    table_name,
    table_type,
    CASE WHEN table_name IS NOT NULL THEN '✅ EXISTS' ELSE '❌ MISSING' END as status
FROM spotify_analytics.information_schema.tables 
WHERE table_name LIKE '%SILVER%' OR table_name LIKE '%GOLD%' OR table_name LIKE '%BRONZE%'
AND table_schema IN ('ANALYTICS', 'MEDALLION_ARCH');

-- 3. Check medallion silver data (foundation for ML features)
SELECT 'SILVER LAYER DATA STATUS' as check_type;
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT track_id) as unique_tracks,
    COUNT(DISTINCT primary_artist_name) as unique_artists,
    COUNT(DISTINCT primary_genre) as unique_genres,
    MIN(denver_date) as earliest_listen,
    MAX(denver_date) as latest_listen,
    CASE 
        WHEN COUNT(*) > 1000 THEN '✅ SUFFICIENT DATA'
        WHEN COUNT(*) > 100 THEN '⚠️ LIMITED DATA'  
        WHEN COUNT(*) > 0 THEN '❌ VERY LIMITED DATA'
        ELSE '❌ NO DATA'
    END as data_status
FROM spotify_analytics.medallion_arch.silver_listening_enriched;

-- 4. Test user profile generation capability
SELECT 'USER PROFILE GENERATION TEST' as check_type;

-- Try to generate basic profile data using medallion architecture
WITH test_profile AS (
    SELECT 
        COUNT(DISTINCT primary_genre) as genre_count,
        COUNT(DISTINCT primary_artist_name) as artist_count,
        COUNT(*) as total_listens,
        COUNT(DISTINCT track_id) as unique_tracks
    FROM spotify_analytics.medallion_arch.silver_listening_enriched
    WHERE denver_date >= DATEADD('days', -90, CURRENT_DATE)
)
SELECT 
    genre_count,
    artist_count, 
    total_listens,
    unique_tracks,
    CASE 
        WHEN genre_count >= 5 AND artist_count >= 20 THEN '✅ GOOD FOR DISCOVERY'
        WHEN genre_count >= 3 AND artist_count >= 10 THEN '⚠️ LIMITED VARIETY'
        ELSE '❌ INSUFFICIENT VARIETY'
    END as profile_quality
FROM test_profile;

-- 5. Check deployment order requirements
SELECT 'DEPLOYMENT ORDER CHECK' as check_type;

WITH deployment_status AS (
    SELECT 
        EXISTS(SELECT 1 FROM spotify_analytics.information_schema.tables 
               WHERE table_name = 'SILVER_LISTENING_ENRICHED' 
               AND table_schema = 'MEDALLION_ARCH') as silver_data_exists,
        
        EXISTS(SELECT 1 FROM spotify_analytics.information_schema.tables 
               WHERE table_name = 'BRONZE_ARTIST_GENRES' 
               AND table_schema = 'MEDALLION_ARCH') as medallion_exists,
               
        EXISTS(SELECT 1 FROM spotify_analytics.information_schema.views 
               WHERE table_name = 'ML_TRACK_CONTENT_FEATURES' 
               AND table_schema = 'ANALYTICS') as ml_views_exist,
               
        EXISTS(SELECT 1 FROM spotify_analytics.information_schema.views 
               WHERE table_name = 'ML_REDISCOVERY_COLLABORATIVE' 
               AND table_schema = 'ANALYTICS') as rediscovery_exists
)
SELECT 
    CASE WHEN silver_data_exists THEN '✅' ELSE '❌' END || ' Silver Listening Data (Enriched)' as step_1,
    CASE WHEN medallion_exists THEN '✅' ELSE '❌' END || ' Medallion Architecture (Bronze/Gold)' as step_2, 
    CASE WHEN ml_views_exist THEN '✅' ELSE '❌' END || ' ML Recommendation Views' as step_3,
    CASE WHEN rediscovery_exists THEN '✅' ELSE '❌' END || ' Rediscovery System' as step_4,
    '⏳ Discovery System (In Progress)' as step_5
FROM deployment_status;

-- 6. Recommended next actions
SELECT 
CASE 
    WHEN NOT EXISTS(SELECT 1 FROM spotify_analytics.information_schema.tables 
                    WHERE table_name = 'SILVER_LISTENING_ENRICHED' 
                    AND table_schema = 'MEDALLION_ARCH')
    THEN '🚀 NEXT ACTION: Deploy Medallion Architecture

1. Run: medallion_architecture_views.sql
2. Wait for dynamic tables to refresh (~6 hours or force refresh)
3. Run: spotify_ml_recommendation_engine.sql  
4. Run: generate_user_profile.sql
5. Re-run: python spotify_discovery_system.py

This will create the genre-enriched Silver layer needed for discovery!'

    WHEN NOT EXISTS(SELECT 1 FROM spotify_analytics.information_schema.views 
                    WHERE table_name = 'ML_TRACK_CONTENT_FEATURES')
    THEN '🚀 NEXT ACTION: Deploy ML Views

1. Run: spotify_ml_recommendation_engine.sql  
2. Run: generate_user_profile.sql
3. Re-run: python spotify_discovery_system.py

This will give you REAL preferences instead of default values!'

    WHEN NOT EXISTS(SELECT 1 FROM spotify_analytics.medallion_arch.silver_listening_enriched LIMIT 1000)
    THEN '📊 NEXT ACTION: Need More Listening Data

Your discovery needs more listening history data.
- Ensure your main pipeline is running
- Wait for more data to accumulate
- Or adjust discovery parameters for smaller datasets'

    ELSE '🔧 NEXT ACTION: Debug Spotify API

ML views exist but 0 tracks discovered suggests:
1. Spotify API authentication issue
2. Search parameters too restrictive  
3. Network connectivity problem
4. API rate limiting

Check Spotify Developer Dashboard for API status.'

END as recommended_action;
