-- DIAGNOSE EXISTING ML VIEWS - FIND WHAT ACTUALLY WORKS
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- =====================================================================
-- 1. CHECK WHAT BASE ML VIEWS EXIST AND WORK
-- =====================================================================

-- Check which ML views exist
SELECT 
    'ML Views Check' AS test_step,
    table_name,
    CASE WHEN table_name IS NOT NULL THEN '✅ Exists' ELSE '❌ Missing' END AS status
FROM information_schema.tables 
WHERE table_schema = 'ANALYTICS' 
AND table_name LIKE 'ML_%'
ORDER BY table_name;

-- =====================================================================
-- 2. TEST EACH BASE ML VIEW INDIVIDUALLY
-- =====================================================================

-- Test ml_collaborative_recommendations
SELECT '🤝 Collaborative Test' AS test_name, COUNT(*) AS row_count, 'EXISTS' AS status
FROM ml_collaborative_recommendations
LIMIT 1;

-- Test ml_content_based_recommendations  
SELECT '🎵 Content-Based Test' AS test_name, COUNT(*) AS row_count, 'EXISTS' AS status
FROM ml_content_based_recommendations
LIMIT 1;

-- Test ml_temporal_recommendations
SELECT '⏰ Temporal Test' AS test_name, COUNT(*) AS row_count, 'EXISTS' AS status
FROM ml_temporal_recommendations
LIMIT 1;

-- Test ml_discovery_recommendations
SELECT '🔍 Discovery Test' AS test_name, COUNT(*) AS row_count, 'EXISTS' AS status
FROM ml_discovery_recommendations
LIMIT 1;

-- =====================================================================
-- 3. TEST SIMPLE SELECTIONS FROM EACH VIEW
-- =====================================================================

-- Test simple selection from collaborative
SELECT 
    'Simple Collaborative' AS test_type,
    track_name,
    primary_artist_name,
    recommendation_score
FROM ml_collaborative_recommendations
WHERE recommendation_score > 0.3
ORDER BY recommendation_score DESC
LIMIT 5;

-- Test simple selection from content-based
SELECT 
    'Simple Content-Based' AS test_type,
    track_name,
    primary_artist_name,
    recommendation_score
FROM ml_content_based_recommendations
WHERE recommendation_score > 0.4
ORDER BY recommendation_score DESC
LIMIT 5;

-- Test simple selection from temporal
SELECT 
    'Simple Temporal' AS test_type,
    track_name,
    primary_artist_name,
    recommendation_score
FROM ml_temporal_recommendations
WHERE recommendation_score > 0.3
ORDER BY recommendation_score DESC
LIMIT 5;

-- Test simple selection from discovery
SELECT 
    'Simple Discovery' AS test_type,
    track_name,
    primary_artist_name,
    recommendation_score
FROM ml_discovery_recommendations
WHERE recommendation_score > 0.5
ORDER BY recommendation_score DESC
LIMIT 5;

-- =====================================================================
-- 4. IDENTIFY THE PROBLEM SOURCE
-- =====================================================================

SELECT 
    '🚨 DIAGNOSIS COMPLETE' AS status,
    'Check which views return data above' AS next_step,
    'We will build working views from successful base views only' AS strategy;
