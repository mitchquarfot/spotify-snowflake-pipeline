-- Test script to verify date conversion works with your VARCHAR(10) dates
-- Run this BEFORE deploying the full ML system

USE DATABASE spotify_analytics;
USE SCHEMA medallion_arch;

-- Test 1: Check the actual format of your ALBUM_RELEASE_DATE strings
SELECT 
    'Date Format Check' as test_name,
    ALBUM_RELEASE_DATE as raw_date_string,
    LENGTH(ALBUM_RELEASE_DATE) as string_length,
    SUBSTR(ALBUM_RELEASE_DATE, 1, 4) as year_part,
    SUBSTR(ALBUM_RELEASE_DATE, 6, 2) as month_part,
    SUBSTR(ALBUM_RELEASE_DATE, 9, 2) as day_part
FROM silver_listening_enriched 
WHERE ALBUM_RELEASE_DATE IS NOT NULL
LIMIT 5;

-- Test 2: Try converting with YYYY-MM-DD format
SELECT 
    'YYYY-MM-DD Conversion Test' as test_name,
    ALBUM_RELEASE_DATE as raw_string,
    TRY_TO_DATE(ALBUM_RELEASE_DATE, 'YYYY-MM-DD') as converted_date,
    YEAR(TRY_TO_DATE(ALBUM_RELEASE_DATE, 'YYYY-MM-DD')) as extracted_year
FROM silver_listening_enriched 
WHERE ALBUM_RELEASE_DATE IS NOT NULL
LIMIT 5;

-- Test 3: Try alternative date formats if YYYY-MM-DD fails
SELECT 
    'Alternative Format Tests' as test_name,
    ALBUM_RELEASE_DATE as raw_string,
    TRY_TO_DATE(ALBUM_RELEASE_DATE, 'YYYY-MM-DD') as format_1,
    TRY_TO_DATE(ALBUM_RELEASE_DATE, 'YYYY-MM') as format_2,
    TRY_TO_DATE(ALBUM_RELEASE_DATE, 'YYYY') as format_3
FROM silver_listening_enriched 
WHERE ALBUM_RELEASE_DATE IS NOT NULL
LIMIT 5;

-- If Test 2 shows NULLs, we need to adjust the format in the ML script
-- If Test 2 shows proper dates, then the ML script should work!
