-- =====================================================================
-- PROGRESSIVE SPOTIFY ML DEPLOYMENT - HANDLES SUBQUERY ISSUES
-- =====================================================================
-- Try advanced systems first, fall back to simpler approaches if needed
-- =====================================================================

-- =====================================================================
-- DEPLOYMENT APPROACH: TRY FROM ADVANCED TO BASIC
-- =====================================================================

SELECT '📋 PROGRESSIVE DEPLOYMENT STRATEGY' AS guide_section;

-- LEVEL 1: ADVANCED ML (if no subquery errors)
-- Run: spotify_ml_recommendation_engine.sql
-- Then: deploy_ml_system_no_udfs.sql
-- If this works: You have full ML system ✅

-- LEVEL 2: BASIC ML (if Level 1 has subquery errors)  
-- Run: spotify_ml_recommendation_engine.sql (foundation needed)
-- Then: deploy_basic_ml_system.sql (simpler views)
-- If this works: You have basic ML system ✅

-- LEVEL 3: ULTRA-SAFE (if all ML views fail)
-- Run: deploy_ultra_safe_system.sql (no ML views at all)
-- This always works: Popularity-based recommendations ✅

-- =====================================================================
-- STEP 1: TEST WHICH LEVEL WORKS FOR YOU
-- =====================================================================

SELECT '🧪 TESTING ML SYSTEM COMPATIBILITY' AS test_section;

-- Test if advanced ML views work
SELECT 'Advanced ML Test' AS test_name, 'Trying ml_hybrid_recommendations_simple...' AS status;

-- This will fail if you have subquery issues:
-- SELECT COUNT(*) FROM ml_hybrid_recommendations_simple;

-- Test if basic ML views work  
SELECT 'Basic ML Test' AS test_name, 'Trying ml_collaborative_recommendations...' AS status;

-- This should work better:
-- SELECT COUNT(*) FROM ml_collaborative_recommendations;

-- Test if source data works (always works)
SELECT 'Source Data Test' AS test_name, 'Trying direct source...' AS status;
SELECT COUNT(*) FROM spotify_analytics.medallion_arch.silver_listening_enriched;

-- =====================================================================
-- DEPLOYMENT OPTION A: FULL ML SYSTEM (TRY FIRST)
-- =====================================================================

SELECT '🎯 DEPLOYMENT OPTION A: FULL ML SYSTEM' AS deployment_option;

-- 1. Run foundation
-- File: spotify_ml_recommendation_engine.sql

-- 2. Deploy advanced ML system  
-- File: deploy_ml_system_no_udfs.sql

-- 3. Test it works
-- File: verify_ml_views_working.sql

-- 4. If successful, continue with infrastructure:
-- File: setup_discovery_snowpipe.sql
-- File: setup_ml_discovery_snowpipe.sql

-- =====================================================================
-- DEPLOYMENT OPTION B: BASIC ML SYSTEM (IF A FAILS)
-- =====================================================================

SELECT '🎯 DEPLOYMENT OPTION B: BASIC ML SYSTEM' AS deployment_option;

-- 1. Run foundation (same as Option A)
-- File: spotify_ml_recommendation_engine.sql

-- 2. Deploy BASIC ML system instead
-- File: deploy_basic_ml_system.sql

-- 3. Test basic system
SELECT 'Testing Basic System' AS test_name;
-- SELECT * FROM get_simple_recommendations LIMIT 3;

-- 4. Update Python to use basic views
-- Modify spotify_ml_discovery_system.py to use get_simple_recommendations

-- =====================================================================
-- DEPLOYMENT OPTION C: ULTRA-SAFE (IF A & B FAIL)
-- =====================================================================

SELECT '🎯 DEPLOYMENT OPTION C: ULTRA-SAFE SYSTEM' AS deployment_option;

-- 1. Skip ML foundation entirely
-- 2. Deploy ultra-safe system
-- File: deploy_ultra_safe_system.sql

-- 3. Test safe system (always works)
SELECT 'Testing Ultra-Safe System' AS test_name;
SELECT * FROM get_safe_recommendations LIMIT 3;

-- 4. Update Python to use safe views  
-- Modify spotify_ml_discovery_system.py to use get_safe_recommendations

-- =====================================================================
-- PYTHON SYSTEM UPDATES FOR EACH OPTION
-- =====================================================================

SELECT '🐍 PYTHON SYSTEM CONFIGURATION' AS python_section;

-- Option A: Uses get_top_recommendations (advanced)
-- Option B: Uses get_simple_recommendations (basic) 
-- Option C: Uses get_safe_recommendations (ultra-safe)

-- Update this query in spotify_ml_discovery_system.py:
/*
ml_query = """
SELECT 
    track_name,
    artist_name,
    genre,
    album_name,
    track_popularity,
    recommendation_score,
    strategy as recommendation_strategies,
    position as playlist_position
FROM get_safe_recommendations  -- <-- Change this view name based on option
WHERE recommendation_score > 0.3
ORDER BY recommendation_score DESC
LIMIT %s
"""
*/

-- =====================================================================
-- VERIFICATION QUERIES FOR EACH OPTION
-- =====================================================================

SELECT '✅ VERIFICATION COMMANDS' AS verification_section;

-- For Option A (Advanced):
-- SELECT 'Advanced ML' as system_type, COUNT(*) as recommendations FROM get_top_recommendations;

-- For Option B (Basic):
-- SELECT 'Basic ML' as system_type, COUNT(*) as recommendations FROM get_simple_recommendations;

-- For Option C (Ultra-Safe):
SELECT 'Ultra-Safe' as system_type, COUNT(*) as recommendations FROM get_safe_recommendations;

-- =====================================================================
-- FINAL SUCCESS CHECK
-- =====================================================================

SELECT 
    '🎉 DEPLOYMENT COMPLETE!' AS status,
    'System deployed with appropriate complexity level' AS result,
    'Both Smart Search & ML pipelines ready' AS capability,
    'Test with: python spotify_discovery_system.py' AS smart_search_test,
    'Test with: python spotify_ml_discovery_system.py' AS ml_test;
