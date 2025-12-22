-- Test script for simplified recommendation views
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- Test the simplified recommendation views
SELECT 'Simplified Collaborative' as test_name, COUNT(*) as row_count 
FROM ml_collaborative_recommendations_simple;

SELECT 'Simplified Content-Based' as test_name, COUNT(*) as row_count 
FROM ml_content_based_recommendations_simple;

SELECT 'Simplified Temporal' as test_name, COUNT(*) as row_count 
FROM ml_temporal_recommendations_simple;

SELECT 'Simplified Discovery' as test_name, COUNT(*) as row_count 
FROM ml_discovery_recommendations_simple;

-- If any of the above work, test a few sample recommendations
SELECT 'Sample Collaborative Recs' as test_name;
SELECT TRACK_NAME, PRIMARY_ARTIST_NAME, recommendation_score 
FROM ml_collaborative_recommendations_simple 
ORDER BY recommendation_score DESC 
LIMIT 5;

SELECT 'Sample Content-Based Recs' as test_name;
SELECT TRACK_NAME, PRIMARY_ARTIST_NAME, recommendation_score 
FROM ml_content_based_recommendations_simple 
ORDER BY recommendation_score DESC 
LIMIT 5;

SELECT 'Sample Temporal Recs' as test_name;
SELECT TRACK_NAME, PRIMARY_ARTIST_NAME, recommendation_score 
FROM ml_temporal_recommendations_simple 
ORDER BY recommendation_score DESC 
LIMIT 5;

SELECT 'Sample Discovery Recs' as test_name;
SELECT TRACK_NAME, PRIMARY_ARTIST_NAME, discovery_score 
FROM ml_discovery_recommendations_simple 
ORDER BY discovery_score DESC 
LIMIT 5;
