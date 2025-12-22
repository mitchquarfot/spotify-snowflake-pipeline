-- VERIFY CORRECT TABLE REFERENCES IN CODEBASE
-- Ensures all ML scripts reference existing tables correctly

-- =====================================================================
-- 1. VERIFY CORE DATA LAYERS EXIST
-- =====================================================================

-- Bronze layer (raw deduplicated data)
SELECT 'Bronze Layer Check' AS layer, COUNT(*) AS records 
FROM raw_data.spotify_mt_listening_deduped
LIMIT 1;

-- Silver layer (enriched listening data) 
SELECT 'Silver Layer Check' AS layer, COUNT(*) AS records
FROM spotify_analytics.medallion_arch.silver_listening_enriched
LIMIT 1;

-- =====================================================================
-- 2. VERIFY SCHEMA STRUCTURE
-- =====================================================================

-- Check what schemas exist
SELECT 'Available Schemas' AS info, schema_name
FROM information_schema.schemata 
WHERE catalog_name = 'SPOTIFY_ANALYTICS'
ORDER BY schema_name;

-- Check what tables exist in each schema
SELECT 
    'Table Inventory' AS info,
    table_schema,
    table_name,
    table_type,
    CASE WHEN table_type = 'VIEW' THEN '📊 View'
         WHEN table_type = 'BASE TABLE' THEN '💾 Table' 
         WHEN table_type = 'DYNAMIC TABLE' THEN '🔄 Dynamic Table'
         ELSE table_type
    END AS type_icon
FROM information_schema.tables
WHERE table_catalog = 'SPOTIFY_ANALYTICS'
AND (table_schema IN ('MEDALLION_ARCH', 'ANALYTICS', 'RAW_DATA') OR table_name LIKE 'ML_%')
ORDER BY table_schema, table_name;

-- =====================================================================
-- 3. CORRECT TABLE REFERENCES SUMMARY
-- =====================================================================

SELECT 
    '✅ CORRECT TABLE REFERENCES TO USE:' AS guidance,
    'Bronze/Raw Layer: raw_data.spotify_mt_listening_deduped' AS bronze_layer,
    'Silver Layer: spotify_analytics.medallion_arch.silver_listening_enriched' AS silver_layer,
    'ML Views: spotify_analytics.analytics.ml_*' AS ml_layer,
    'Gold Layer: spotify_analytics.medallion_arch.gold_*' AS gold_layer;

-- =====================================================================
-- 4. COMMON MISTAKES TO AVOID
-- =====================================================================

SELECT 
    '❌ INCORRECT REFERENCES TO AVOID:' AS warning,
    'NEVER use: gold_deduplicated_listening (does not exist)' AS mistake_1,
    'NEVER use: spotify_analytics.raw_data.* (wrong database)' AS mistake_2,
    'NEVER use: medallion_arch.ml_* (ML views are in analytics schema)' AS mistake_3;

