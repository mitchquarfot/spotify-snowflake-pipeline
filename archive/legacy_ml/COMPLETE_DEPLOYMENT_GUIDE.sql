-- =====================================================================
-- COMPLETE SPOTIFY ML DISCOVERY SYSTEM - DEPLOYMENT GUIDE
-- =====================================================================
-- Follow this step-by-step guide to deploy your dual-pipeline system
-- =====================================================================

-- =====================================================================
-- STEP 1: CREATE ML FOUNDATION (REQUIRED FIRST)
-- =====================================================================
-- Run file: spotify_ml_recommendation_engine.sql
-- This creates all the ML views and recommendation algorithms
-- Expected result: 15+ ML views created in analytics schema

-- Verify Step 1 completed:
SELECT 
    'ML Foundation Check' AS step,
    COUNT(*) AS ml_views_created,
    CASE WHEN COUNT(*) >= 10 THEN '✅ Ready for Step 2' ELSE '❌ Run spotify_ml_recommendation_engine.sql' END AS status
FROM information_schema.tables
WHERE table_schema = 'ANALYTICS' AND table_name LIKE 'ML_%';

-- =====================================================================  
-- STEP 2: DEPLOY ML SYSTEM (NO UDFs)
-- =====================================================================
-- Run file: deploy_ml_system_no_udfs.sql  
-- This creates working views for ML recommendations without UDF issues
-- Expected result: 3 recommendation views + 3 utility functions

-- Verify Step 2 completed:
SELECT 
    'ML System Check' AS step,
    table_name,
    'View created ✅' AS status
FROM information_schema.tables
WHERE table_schema = 'ANALYTICS' 
AND table_name IN ('GET_TOP_RECOMMENDATIONS', 'GET_GENRE_RECOMMENDATIONS', 'GET_DISCOVERY_RECOMMENDATIONS')
ORDER BY table_name;

-- =====================================================================
-- STEP 3: VERIFY ML SYSTEM WORKS  
-- =====================================================================
-- Run file: verify_ml_views_working.sql
-- This tests that all ML components are functional
-- Expected result: All queries return data without errors

-- Quick verification (run after step 3):
SELECT 
    'ML Data Verification' AS step,
    (SELECT COUNT(*) FROM get_top_recommendations) AS top_recs,
    (SELECT COUNT(*) FROM ml_hybrid_recommendations_simple) AS hybrid_recs,
    CASE WHEN (SELECT COUNT(*) FROM get_top_recommendations) > 0 
         THEN '✅ ML System Working' 
         ELSE '❌ Check data pipeline' 
    END AS status;

-- =====================================================================
-- STEP 4: DEPLOY SMART SEARCH INFRASTRUCTURE (PIPELINE A)
-- =====================================================================
-- Run file: setup_discovery_snowpipe.sql
-- This creates Smart Search pipeline infrastructure
-- Expected result: Tables, stages, pipes for s3://mquarfot-dev/spotify_discoveries/

-- Verify Step 4 completed:
SELECT 
    'Smart Search Pipeline Check' AS step,
    table_name,
    'Infrastructure created ✅' AS status
FROM information_schema.tables
WHERE table_schema = 'ANALYTICS' 
AND table_name IN ('RAW_SPOTIFY_DISCOVERIES', 'ML_SPOTIFY_DISCOVERIES')
ORDER BY table_name;

-- Check Smart Search Snowpipe:
SELECT 
    'Smart Search Snowpipe' AS component,
    pipe_name,
    is_autoingest_enabled,
    'Pipe active ✅' AS status
FROM snowflake.information_schema.pipes 
WHERE pipe_name = 'DISCOVERY_SNOWPIPE';

-- =====================================================================
-- STEP 5: DEPLOY ML INFRASTRUCTURE (PIPELINE B)
-- =====================================================================
-- Run file: setup_ml_discovery_snowpipe.sql
-- This creates ML Hybrid pipeline infrastructure  
-- Expected result: Tables, stages, pipes for s3://mquarfot-dev/spotify_ml_discoveries/

-- Verify Step 5 completed:
SELECT 
    'ML Hybrid Pipeline Check' AS step,
    table_name,
    'Infrastructure created ✅' AS status
FROM information_schema.tables
WHERE table_schema = 'ANALYTICS' 
AND table_name IN ('RAW_SPOTIFY_ML_DISCOVERIES', 'ML_SPOTIFY_ML_DISCOVERIES')
ORDER BY table_name;

-- Check ML Hybrid Snowpipe:
SELECT 
    'ML Hybrid Snowpipe' AS component,
    pipe_name,
    is_autoingest_enabled,
    'Pipe active ✅' AS status
FROM snowflake.information_schema.pipes 
WHERE pipe_name = 'ML_DISCOVERY_SNOWPIPE';

-- =====================================================================
-- FINAL VERIFICATION: COMPLETE SYSTEM CHECK
-- =====================================================================
-- Run this after all 5 steps to verify complete deployment

SELECT '🎯 COMPLETE SYSTEM STATUS CHECK' AS verification_type;

-- ML Foundation Check
SELECT 
    'ML Views' AS component,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) >= 10 THEN '✅ Working' ELSE '❌ Missing' END AS status
FROM information_schema.tables
WHERE table_schema = 'ANALYTICS' AND table_name LIKE 'ML_%';

-- Recommendation Views Check  
SELECT 
    'Recommendation Views' AS component,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) = 3 THEN '✅ Working' ELSE '❌ Missing' END AS status
FROM information_schema.tables
WHERE table_schema = 'ANALYTICS' 
AND table_name IN ('GET_TOP_RECOMMENDATIONS', 'GET_GENRE_RECOMMENDATIONS', 'GET_DISCOVERY_RECOMMENDATIONS');

-- Pipeline Infrastructure Check
SELECT 
    'Pipeline Tables' AS component,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) >= 4 THEN '✅ Working' ELSE '❌ Missing' END AS status
FROM information_schema.tables
WHERE table_schema = 'ANALYTICS' 
AND table_name LIKE '%DISCOVERIES';

-- Snowpipe Status Check
SELECT 
    'Snowpipes' AS component,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) >= 2 THEN '✅ Working' ELSE '❌ Missing' END AS status
FROM snowflake.information_schema.pipes 
WHERE pipe_name LIKE '%DISCOVERY%';

-- Sample Data Check
SELECT 
    'Sample ML Recommendations' AS test_type,
    track_name,
    artist_name,
    recommendation_score
FROM get_top_recommendations
LIMIT 3;

-- =====================================================================
-- SUCCESS MESSAGE
-- =====================================================================
SELECT 
    '🎉 DEPLOYMENT COMPLETE!' AS status,
    '✅ Smart Search Pipeline (spotify_discoveries/)' AS pipeline_a,
    '✅ ML Hybrid Pipeline (spotify_ml_discoveries/)' AS pipeline_b,
    '✅ Ready for A/B testing!' AS next_step;

