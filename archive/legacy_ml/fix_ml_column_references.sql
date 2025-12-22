-- FIX ML COLUMN REFERENCES - CORRECT ALL COLUMN NAME MISMATCHES
-- Based on actual view definitions in spotify_ml_recommendation_engine.sql

-- =====================================================================
-- IDENTIFY ACTUAL COLUMN NAMES IN ML VIEWS
-- =====================================================================

-- Check ml_user_genre_interactions actual columns
SELECT 'ml_user_genre_interactions Columns' AS view_check;
DESCRIBE VIEW ml_user_genre_interactions;

-- Check ml_genre_similarity_matrix actual columns  
SELECT 'ml_genre_similarity_matrix Columns' AS view_check;
DESCRIBE VIEW ml_genre_similarity_matrix;

-- Check ml_track_content_features actual columns
SELECT 'ml_track_content_features Columns' AS view_check;
DESCRIBE VIEW ml_track_content_features;

-- =====================================================================
-- CORRECT COLUMN MAPPING
-- =====================================================================

SELECT 
    '✅ CORRECT COLUMN NAMES TO USE:' AS guidance,
    'ml_user_genre_interactions.play_count (NOT total_plays)' AS correction_1,
    'ml_user_genre_interactions.weighted_preference (NOT affinity_score)' AS correction_2,
    'ml_genre_similarity_matrix.jaccard_similarity (correct)' AS correction_3,
    'ml_track_content_features.user_play_count (correct)' AS correction_4;

-- =====================================================================
-- TEST CORRECTED QUERIES
-- =====================================================================

-- Test corrected user genre interactions query
SELECT 'Corrected Genre Interactions Test' AS test_type,
       primary_genre, 
       play_count,           -- CORRECTED: was total_plays
       weighted_preference,  -- CORRECTED: was affinity_score
       preference_strength
FROM ml_user_genre_interactions
WHERE play_count > 0       -- CORRECTED: was total_plays > 0
ORDER BY weighted_preference DESC  -- CORRECTED: was affinity_score DESC
LIMIT 10;

-- Test filtering criteria with corrected columns
SELECT 
    'Corrected Filtering Analysis' AS analysis,
    COUNT(*) AS total_genre_interactions,
    COUNT(CASE WHEN play_count >= 2 THEN 1 END) AS genres_with_2plus_plays,
    COUNT(CASE WHEN weighted_preference > 0.1 THEN 1 END) AS genres_with_preference,
    COUNT(CASE WHEN recent_plays > 0 THEN 1 END) AS genres_with_recent_activity
FROM ml_user_genre_interactions;

-- =====================================================================
-- SUCCESS MESSAGE
-- =====================================================================

SELECT 
    '🔧 COLUMN REFERENCE CORRECTIONS IDENTIFIED' AS status,
    'Use play_count and weighted_preference in ml_user_genre_interactions' AS fix_required;

