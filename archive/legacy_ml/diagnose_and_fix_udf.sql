-- COMPREHENSIVE UDF DIAGNOSTIC AND FIX
-- Run this step-by-step to identify and resolve the compilation issue

-- =====================================================================
-- STEP 1: CHECK CURRENT CONTEXT
-- =====================================================================
SELECT CURRENT_DATABASE() AS current_db, CURRENT_SCHEMA() AS current_schema;

-- =====================================================================
-- STEP 2: VERIFY SCHEMA EXISTS AND SET CONTEXT
-- =====================================================================
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- Verify context is set
SELECT CURRENT_DATABASE() AS db_after_use, CURRENT_SCHEMA() AS schema_after_use;

-- =====================================================================
-- STEP 3: CHECK IF REQUIRED VIEWS EXIST
-- =====================================================================
SELECT 
    'Required Views Check' AS check_type,
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema = 'ANALYTICS' 
AND table_name IN (
    'ML_HYBRID_RECOMMENDATIONS',
    'ML_TRACK_CONTENT_FEATURES', 
    'ML_DISCOVERY_RECOMMENDATIONS'
)
ORDER BY table_name;

-- =====================================================================
-- STEP 4: TEST BASIC UDF CREATION (NO DEPENDENCIES)
-- =====================================================================

-- Ultra-simple function (should always work)
CREATE OR REPLACE FUNCTION test_ultra_simple()
RETURNS INTEGER
LANGUAGE SQL
AS
$$
    1
$$;

-- Test it
SELECT test_ultra_simple() AS ultra_simple_test;

-- =====================================================================
-- STEP 5: TEST TABLE FUNCTION (NO DEPENDENCIES)
-- =====================================================================

CREATE OR REPLACE FUNCTION test_simple_table()
RETURNS TABLE(test_result INTEGER, test_message STRING)
LANGUAGE SQL
AS
$$
    SELECT 1 AS test_result, 'Success' AS test_message
$$;

-- Test it
SELECT * FROM TABLE(test_simple_table());

-- =====================================================================
-- STEP 6: ONLY IF VIEWS EXIST - CREATE ML FUNCTIONS
-- =====================================================================

-- Check view data availability first
SELECT 
    'Data Availability Check' AS check_type,
    (SELECT COUNT(*) FROM ml_hybrid_recommendations) AS hybrid_recs_count,
    (SELECT COUNT(*) FROM ml_track_content_features) AS content_features_count;

-- =====================================================================
-- STEP 7: SIMPLE ML FUNCTION (ONLY IF VIEWS HAVE DATA)
-- =====================================================================

CREATE OR REPLACE FUNCTION get_top_recommendations(limit_count INTEGER DEFAULT 10)
RETURNS TABLE(
    track_name STRING,
    artist_name STRING,
    score FLOAT
)
LANGUAGE SQL
AS
$$
    SELECT 
        TRACK_NAME AS track_name,
        PRIMARY_ARTIST_NAME AS artist_name,
        final_recommendation_score AS score
    FROM ml_hybrid_recommendations
    ORDER BY final_recommendation_score DESC
    LIMIT limit_count
$$;

-- Test the ML function
SELECT * FROM TABLE(get_top_recommendations(5));

-- =====================================================================
-- STEP 8: STATUS SUMMARY
-- =====================================================================

SELECT 
    '🎯 DIAGNOSTIC SUMMARY' AS status,
    CASE 
        WHEN (SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ML_HYBRID_RECOMMENDATIONS') = 0 
        THEN '❌ ML Views missing - run spotify_ml_recommendation_engine.sql first'
        WHEN (SELECT COUNT(*) FROM ml_hybrid_recommendations) = 0
        THEN '⚠️  ML Views exist but no data - check data pipeline'
        ELSE '✅ ML system ready for UDF creation'
    END AS recommendation;
