-- Quick script to verify your current table structure
-- Run this to make sure all expected columns exist in uppercase

USE DATABASE spotify_analytics;
USE SCHEMA medallion_arch;

-- Check the actual column names in your silver table
DESCRIBE TABLE silver_listening_enriched;

-- Quick test to verify the columns the ML system needs exist
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT PRIMARY_GENRE) as unique_genres,
    COUNT(DISTINCT PRIMARY_ARTIST_ID) as unique_artists,
    MIN(DENVER_DATE) as earliest_date,
    MAX(DENVER_DATE) as latest_date
FROM silver_listening_enriched
WHERE DENVER_DATE >= DATEADD('days', -90, CURRENT_DATE);

-- This should return data without errors if your table structure is compatible
