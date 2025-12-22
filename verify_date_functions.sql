-- Quick verification that date functions work properly
-- Run this to test date/time operations before full ML deployment

USE DATABASE spotify_analytics;
USE SCHEMA medallion_arch;

-- Test 1: Verify ALBUM_RELEASE_DATE exists and is date-compatible (VARCHAR to DATE conversion)
SELECT 
    'ALBUM_RELEASE_DATE Test' as test_name,
    COUNT(*) as total_rows,
    COUNT(ALBUM_RELEASE_DATE) as non_null_dates,
    MIN(YEAR(TO_DATE(ALBUM_RELEASE_DATE, 'YYYY-MM-DD'))) as earliest_year,
    MAX(YEAR(TO_DATE(ALBUM_RELEASE_DATE, 'YYYY-MM-DD'))) as latest_year
FROM silver_listening_enriched 
WHERE ALBUM_RELEASE_DATE IS NOT NULL
LIMIT 5;

-- Test 2: Verify DENVER_HOUR is numeric and valid
SELECT 
    'DENVER_HOUR Test' as test_name,
    COUNT(*) as total_rows,
    MIN(DENVER_HOUR) as min_hour,
    MAX(DENVER_HOUR) as max_hour,
    AVG(DENVER_HOUR) as avg_hour
FROM silver_listening_enriched
LIMIT 5;

-- Test 3: Verify TRACK_DURATION_MS is numeric (not used in date functions)
SELECT 
    'TRACK_DURATION_MS Test' as test_name,
    COUNT(*) as total_rows,
    AVG(TRACK_DURATION_MS) / 1000.0 / 60.0 as avg_duration_minutes
FROM silver_listening_enriched
LIMIT 5;

-- If all these run without errors, the date/time functions should work properly
