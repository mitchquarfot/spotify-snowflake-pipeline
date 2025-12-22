-- Test script to validate Snowflake function compatibility fixes
-- Run this BEFORE deploying the full ML system to catch any remaining issues

USE DATABASE spotify_analytics;
USE SCHEMA medallion_arch;

-- Test 1: DATEDIFF with proper timestamp conversion
SELECT 
    'DATEDIFF Test' as test_name,
    FLOOR(DATEDIFF('minute', '1970-01-01'::TIMESTAMP, DENVER_TIMESTAMP) / 60) AS test_session_id,
    COUNT(*) as records
FROM silver_listening_enriched 
WHERE DENVER_DATE >= DATEADD('days', -30, CURRENT_DATE)
GROUP BY test_session_id
ORDER BY test_session_id
LIMIT 5;

-- Test 2: ARRAY_CONSTRUCT functions
SELECT 
    'ARRAY_CONSTRUCT Test' as test_name,
    CASE 
        WHEN PRIMARY_GENRE = 'rock' THEN ARRAY_CONSTRUCT(PRIMARY_GENRE)
        ELSE ARRAY_CONSTRUCT()
    END AS test_array,
    PRIMARY_GENRE
FROM silver_listening_enriched 
LIMIT 5;

-- Test 3: POSITION function for string searching
WITH test_strings AS (
    SELECT 'collaborative+content+temporal' as test_strategies
)
SELECT 
    'POSITION Test' as test_name,
    test_strategies,
    POSITION('collaborative', test_strategies) > 0 as has_collaborative,
    POSITION('content', test_strategies) > 0 as has_content,
    POSITION('temporal', test_strategies) > 0 as has_temporal,
    POSITION('discovery', test_strategies) > 0 as has_discovery
FROM test_strings;

-- Test 4: OBJECT_CONSTRUCT function
SELECT 
    'OBJECT_CONSTRUCT Test' as test_name,
    OBJECT_CONSTRUCT(
        'track_name', TRACK_NAME,
        'artist', PRIMARY_ARTIST_NAME,
        'popularity', TRACK_POPULARITY,
        'spotify_url', 'https://open.spotify.com/track/' || TRACK_ID
    ) AS test_metadata
FROM silver_listening_enriched 
LIMIT 3;

-- Test 5: Date conversion functions
SELECT 
    'Date Conversion Test' as test_name,
    ALBUM_RELEASE_DATE as raw_date,
    TO_DATE(ALBUM_RELEASE_DATE, 'YYYY-MM-DD') as converted_date,
    YEAR(TO_DATE(ALBUM_RELEASE_DATE, 'YYYY-MM-DD')) as extracted_year
FROM silver_listening_enriched 
WHERE ALBUM_RELEASE_DATE IS NOT NULL
LIMIT 5;

-- Test 6: Mathematical functions
SELECT 
    'Math Functions Test' as test_name,
    ABS(TRACK_POPULARITY - 50) as abs_test,
    GREATEST(TRACK_POPULARITY, 1) as greatest_test,
    LN(TRACK_POPULARITY + 1) as ln_test,
    FLOOR(TRACK_POPULARITY / 10.0) as floor_test
FROM silver_listening_enriched 
WHERE TRACK_POPULARITY > 0
LIMIT 5;

-- If ALL these tests pass without errors, the ML system should deploy successfully!
