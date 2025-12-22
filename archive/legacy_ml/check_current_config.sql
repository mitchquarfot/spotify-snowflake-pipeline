-- Quick Configuration Check: Find your current S3 and Snowflake setup details
-- Run this first to gather the information needed for discovery pipeline setup

USE DATABASE spotify_analytics;

SELECT '=== CURRENT INFRASTRUCTURE DISCOVERY ===' as section;

-- 1. Check existing S3 stages (to get your bucket name)
SELECT 'EXISTING S3 STAGES' as check_type;
SELECT 
    stage_name,
    stage_url,
    'Copy bucket name from URL for discovery setup' as note
FROM information_schema.stages 
WHERE stage_name LIKE '%SPOTIFY%'
   OR stage_url LIKE '%spotify%';

-- 2. Check existing warehouses (to get your warehouse name)  
SELECT 'AVAILABLE WAREHOUSES' as check_type;
SHOW WAREHOUSES;

-- 3. Check existing pipes (to understand your current setup pattern)
SELECT 'EXISTING SNOWPIPES' as check_type;
SELECT 
    pipe_name,
    definition,
    'Pattern to follow for discovery pipe' as note
FROM information_schema.pipes 
WHERE pipe_name LIKE '%SPOTIFY%';

-- 4. Check current database and schema
SELECT 'CURRENT CONTEXT' as check_type;
SELECT 
    CURRENT_DATABASE() as current_database,
    CURRENT_SCHEMA() as current_schema,
    'Use these for discovery setup' as note;

-- 5. Check if medallion architecture exists
SELECT 'MEDALLION ARCHITECTURE STATUS' as check_type;
SELECT 
    table_schema,
    COUNT(*) as table_count,
    LISTAGG(table_name, ', ') as table_names
FROM information_schema.tables 
WHERE table_name LIKE '%SILVER%' OR table_name LIKE '%GOLD%'
   OR table_name LIKE '%BRONZE%' OR table_name LIKE '%MEDALLION%'
GROUP BY table_schema;

-- 6. Check rediscovery system status
SELECT 'REDISCOVERY SYSTEM STATUS' as check_type;
SELECT 
    CASE 
        WHEN EXISTS(SELECT 1 FROM information_schema.views WHERE view_name = 'ML_REDISCOVERY_COLLABORATIVE')
        THEN '✅ DEPLOYED'
        ELSE '❌ NEEDS DEPLOYMENT'
    END as rediscovery_status,
    CASE 
        WHEN EXISTS(SELECT 1 FROM information_schema.views WHERE view_name = 'ML_REDISCOVERY_COLLABORATIVE')
        THEN 'Ready for new discovery setup'
        ELSE 'Run rediscovery_recommendations.sql first'
    END as action_needed;

-- 7. Sample current listening data to verify base system
SELECT 'LISTENING DATA SAMPLE' as check_type;
SELECT 
    COUNT(*) as total_tracks_in_system,
    COUNT(DISTINCT primary_artist_name) as unique_artists,
    COUNT(DISTINCT primary_genre) as unique_genres,
    'Base data for recommendations' as note
FROM ml_track_content_features
WHERE user_play_count >= 1;

SELECT 
'📋 CONFIGURATION CHECKLIST:

1. 🏗️ COPY THESE VALUES FOR SETUP:
   - S3 Bucket: [Copy from EXISTING S3 STAGES above]
   - Warehouse: [Copy from AVAILABLE WAREHOUSES above]  
   - Database: ' || CURRENT_DATABASE() || '
   - Schema: ' || CURRENT_SCHEMA() || '

2. 🔧 NEXT STEPS:
   a) Update setup_discovery_snowpipe.sql with your values
   b) Run the setup script in Snowflake
   c) Configure S3 event notifications  
   d) Test with Python discovery script

3. 📖 FULL GUIDE:
   See COMPLETE_PIPELINE_SETUP_GUIDE.md for detailed instructions

🎯 Ready to build your complete discovery pipeline!
' as instructions;
