-- DIAGNOSE WHY ML VIEWS HAVE NO ROWS - CORRECTED TABLE NAMES
-- Traces actual data pipeline using correct table/schema references
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- =====================================================================
-- 1. CHECK SOURCE DATA EXISTS - CORRECT TABLE NAMES
-- =====================================================================

-- Check raw deduplicated listening data (bronze layer)
SELECT 'Raw Deduped Listening Check' AS step, COUNT(*) AS row_count
FROM raw_data.spotify_mt_listening_deduped
LIMIT 1;

-- Check silver enriched listening data
SELECT 'Silver Enriched Data Check' AS step, COUNT(*) AS row_count
FROM spotify_analytics.medallion_arch.silver_listening_enriched
LIMIT 1;

-- Check if we have track content features (ML foundation)
SELECT 'ML Track Features Check' AS step, COUNT(*) AS row_count
FROM ml_track_content_features
LIMIT 1;

-- =====================================================================
-- 2. CHECK ML PIPELINE DEPENDENCIES
-- =====================================================================

-- Check if genre similarity matrix has data
SELECT 'Genre Similarity Matrix Check' AS step, COUNT(*) AS row_count
FROM ml_genre_similarity_matrix
LIMIT 1;

-- Check if user genre interactions exist
SELECT 'User Genre Interactions Check' AS step, COUNT(*) AS row_count
FROM ml_user_genre_interactions  
LIMIT 1;

-- Check temporal patterns
SELECT 'Temporal Patterns Check' AS step, COUNT(*) AS row_count
FROM ml_temporal_patterns
LIMIT 1;

-- =====================================================================
-- 3. DETAILED DIAGNOSTIC FOR FIRST ML VIEW
-- =====================================================================

-- Debug collaborative recommendations step by step
SELECT 'Step 1: Track Content Features' AS debug_step, COUNT(*) AS tracks
FROM ml_track_content_features
WHERE user_play_count > 0;

SELECT 'Step 2: Genre Interactions' AS debug_step, COUNT(*) AS interactions  
FROM ml_user_genre_interactions
WHERE play_count > 0;

SELECT 'Step 3: Genre Similarity' AS debug_step, COUNT(*) AS similarities
FROM ml_genre_similarity_matrix 
WHERE jaccard_similarity > 0;

-- =====================================================================
-- 4. CHECK SCHEMA AND TABLE EXISTENCE
-- =====================================================================

-- Verify all required views/tables exist
SELECT 
    'View/Table Existence Check' AS check_type,
    t.table_name,
    table_type,
    CASE WHEN row_count = 0 THEN '❌ EMPTY' 
         WHEN row_count > 0 THEN '✅ HAS DATA'
         ELSE '❓ UNKNOWN'
    END AS status
FROM information_schema.tables t
LEFT JOIN (
    -- Get row counts for key tables/views
    SELECT 'ML_TRACK_CONTENT_FEATURES' AS table_name, COUNT(*) AS a.row_count FROM ml_track_content_features a
    UNION ALL
    SELECT 'ML_USER_GENRE_INTERACTIONS' AS table_name, COUNT(*) FROM ml_user_genre_interactions  
    UNION ALL
    SELECT 'ML_GENRE_SIMILARITY_MATRIX' AS table_name, COUNT(*) FROM ml_genre_similarity_matrix
    UNION ALL
    SELECT 'ML_TEMPORAL_PATTERNS' AS table_name, COUNT(*) FROM ml_temporal_patterns
) rc ON t.table_name = rc.table_name
WHERE t.table_schema IN ('ANALYTICS', 'MEDALLION_ARCH')
AND t.table_name LIKE 'ML_%'
ORDER BY t.table_name;

-- =====================================================================
-- 5. CHECK SILVER LAYER DATA QUALITY
-- =====================================================================

-- Check silver layer has recent data
SELECT 
    'Silver Layer Data Quality' AS check_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT track_id) AS unique_tracks,
    COUNT(DISTINCT primary_artist_id) AS unique_artists,
    COUNT(DISTINCT primary_genre) AS unique_genres,
    MIN(denver_date) AS earliest_date,
    MAX(denver_date) AS latest_date,
    COUNT(CASE WHEN denver_date >= DATEADD('days', -30, CURRENT_DATE) THEN 1 END) AS recent_30_days,
    COUNT(CASE WHEN denver_date >= DATEADD('days', -90, CURRENT_DATE) THEN 1 END) AS recent_90_days
FROM spotify_analytics.medallion_arch.silver_listening_enriched;

-- =====================================================================
-- 6. CHECK FILTERING CRITERIA IN ML VIEWS
-- =====================================================================

-- Check if filtering is too restrictive in collaborative recommendations
SELECT 
    'ML View Filtering Analysis' AS analysis,
    
    -- Source data counts
    (SELECT COUNT(*) FROM spotify_analytics.medallion_arch.silver_listening_enriched) AS total_silver_records,
    (SELECT COUNT(DISTINCT track_id) FROM spotify_analytics.medallion_arch.silver_listening_enriched) AS unique_tracks_in_silver,
    (SELECT COUNT(DISTINCT primary_genre) FROM spotify_analytics.medallion_arch.silver_listening_enriched WHERE primary_genre IS NOT NULL) AS unique_genres_in_silver,
    
    -- ML foundation counts
    (SELECT COUNT(*) FROM ml_track_content_features) AS tracks_in_ml_features,
    (SELECT COUNT(*) FROM ml_user_genre_interactions) AS genre_interactions,
    (SELECT COUNT(*) FROM ml_genre_similarity_matrix) AS genre_similarities,
    
    -- Filtering impact
    (SELECT COUNT(*) FROM ml_track_content_features WHERE user_play_count >= 2) AS tracks_with_2plus_plays,
    (SELECT COUNT(*) FROM ml_user_genre_interactions WHERE weighted_preference > 0.1) AS genres_with_preference,
    (SELECT COUNT(*) FROM ml_genre_similarity_matrix WHERE jaccard_similarity > 0) AS genre_pairs_with_similarity;

-- =====================================================================
-- 7. SAMPLE ACTUAL DATA TO VERIFY CONTENT
-- =====================================================================

-- Show sample silver data
SELECT 'Sample Silver Data' AS sample_type,
       track_name, primary_artist_name, primary_genre, track_popularity, denver_date
FROM spotify_analytics.medallion_arch.silver_listening_enriched  
WHERE primary_genre IS NOT NULL
ORDER BY denver_date DESC
LIMIT 10;

-- Show sample track features (if exists)
SELECT 'Sample Track Features' AS sample_type,
       track_name, primary_artist_name, primary_genre, user_play_count, track_popularity
FROM ml_track_content_features  
WHERE user_play_count > 0
ORDER BY user_play_count DESC
LIMIT 10;

-- Show sample genre interactions (if exists)
SELECT 'Sample Genre Interactions' AS sample_type,
       primary_genre, play_count, artist_diversity_in_genre AS unique_artists, weighted_preference
FROM ml_user_genre_interactions
WHERE play_count > 0
ORDER BY weighted_preference DESC  
LIMIT 10;

-- Show sample genre similarities (if exists)
SELECT 'Sample Genre Similarities' AS sample_type,
       genre_a, genre_b, shared_tracks, jaccard_similarity
FROM ml_genre_similarity_matrix
WHERE jaccard_similarity > 0
ORDER BY jaccard_similarity DESC
LIMIT 10;

-- =====================================================================
-- 8. IDENTIFY THE EXACT BOTTLENECK
-- =====================================================================

SELECT 
    '🚨 ROOT CAUSE ANALYSIS' AS diagnosis,
    CASE 
        WHEN (SELECT COUNT(*) FROM raw_data.spotify_mt_listening_deduped) = 0 
        THEN '❌ CRITICAL: No raw deduped data - check Snowpipe ingestion'
        
        WHEN (SELECT COUNT(*) FROM spotify_analytics.medallion_arch.silver_listening_enriched) = 0
        THEN '❌ CRITICAL: No silver enriched data - medallion views not working'
        
        WHEN (SELECT COUNT(*) FROM spotify_analytics.medallion_arch.silver_listening_enriched WHERE denver_date >= DATEADD('days', -90, CURRENT_DATE)) < 100
        THEN '❌ CRITICAL: Very little recent data - need more listening history'
        
        WHEN (SELECT COUNT(DISTINCT primary_genre) FROM spotify_analytics.medallion_arch.silver_listening_enriched WHERE primary_genre IS NOT NULL) < 3
        THEN '❌ CRITICAL: Too few genres - need more diverse listening'
        
        WHEN (SELECT COUNT(*) FROM ml_track_content_features) = 0 
        THEN '❌ ML FOUNDATION MISSING: Run spotify_ml_recommendation_engine.sql to create base ML views'
        
        WHEN (SELECT COUNT(*) FROM ml_user_genre_interactions) = 0
        THEN '❌ ML PIPELINE ISSUE: Genre interactions not calculated - check ML view logic'
        
        WHEN (SELECT COUNT(*) FROM ml_genre_similarity_matrix WHERE jaccard_similarity > 0) = 0
        THEN '❌ ML PIPELINE ISSUE: Genre similarities not calculated - check Jaccard logic'
        
        WHEN (SELECT COUNT(*) FROM ml_collaborative_recommendations) = 0
        THEN '❌ ML FILTERING ISSUE: Collaborative recommendations too restrictive - check filtering criteria'
        
        ELSE '✅ BASE DATA EXISTS: Check individual ML recommendation view queries'
    END AS root_cause,
    
    CASE 
        WHEN (SELECT COUNT(*) FROM raw_data.spotify_mt_listening_deduped) = 0 
        THEN 'Check S3 data ingestion and Snowpipe status'
        
        WHEN (SELECT COUNT(*) FROM spotify_analytics.medallion_arch.silver_listening_enriched) = 0
        THEN 'Run medallion_architecture_views.sql to create silver layer'
        
        WHEN (SELECT COUNT(*) FROM ml_track_content_features) = 0 
        THEN 'Run spotify_ml_recommendation_engine.sql to create ML foundation'
        
        ELSE 'Debug individual ML recommendation view logic and filtering'
    END AS recommended_fix;

-- =====================================================================
-- 9. SUCCESS MESSAGE
-- =====================================================================

SELECT 
    '✅ THOROUGH DIAGNOSTIC COMPLETE' AS status,
    'Using correct table names: raw_data.spotify_mt_listening_deduped & silver_listening_enriched' AS correction,
    'Root cause identified above - follow recommended fix' AS next_step;
