-- Quick fixes for MIN() issues in existing medallion architecture
-- This addresses the "Adam Doleac" problem where MIN() returns alphabetical results

USE ROLE SPOTIFY_ANALYST_ROLE;
USE WAREHOUSE SPOTIFY_WH;
USE DATABASE spotify_analytics;
USE SCHEMA medallion_arch;

-- =============================================================================
-- DEMONSTRATION OF THE PROBLEM
-- =============================================================================

SELECT 'DEMONSTRATING THE MIN() PROBLEM' as report_section;

-- Show how MIN() gives wrong results for "top artist"
WITH genre_stats AS (
    SELECT 
        se.primary_genre,
        se.primary_artist_name,
        COUNT(*) as play_count,
        ROW_NUMBER() OVER (PARTITION BY se.primary_genre ORDER BY COUNT(*) DESC) as actual_rank
    FROM silver_listening_enriched se
    WHERE se.primary_genre = 'country'  -- Focus on country genre
    GROUP BY se.primary_genre, se.primary_artist_name
)
SELECT 
    'country' as genre,
    MIN(primary_artist_name) as min_gives_alphabetical,
    (SELECT primary_artist_name FROM genre_stats WHERE actual_rank = 1) as actual_top_artist,
    (SELECT play_count FROM genre_stats WHERE actual_rank = 1) as actual_top_plays
FROM genre_stats;

-- =============================================================================
-- TEMPORARY FIX: CREATE CORRECTED VIEWS
-- =============================================================================

-- Create a corrected genre analysis view
CREATE OR REPLACE VIEW gold_genre_analysis_corrected AS
WITH genre_artist_stats AS (
    SELECT 
        se.primary_genre,
        se.primary_artist_name,
        COUNT(*) as artist_play_count,
        ROW_NUMBER() OVER (PARTITION BY se.primary_genre ORDER BY COUNT(*) DESC, se.primary_artist_name) as artist_rank
    FROM silver_listening_enriched se
    WHERE se.primary_genre IS NOT NULL
    GROUP BY se.primary_genre, se.primary_artist_name
),
genre_aggregates AS (
    SELECT 
        se.primary_genre,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as genre_rank,
        
        -- Listening metrics
        COUNT(*) as total_plays,
        COUNT(DISTINCT se.primary_artist_id) as unique_artists,
        COUNT(DISTINCT se.track_id) as unique_tracks,
        ROUND(SUM(se.track_duration_minutes), 2) as total_listening_minutes,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage_of_total_listening,
        
        -- Artist diversity
        ROUND(AVG(se.artist_popularity), 1) as average_artist_popularity,
        ROUND(AVG(se.artist_followers), 0) as average_artist_followers,
        
        -- Temporal patterns
        MIN(se.time_of_day_category) as primary_time_of_day,
        MIN(DAYNAME(se.denver_timestamp)) as primary_day_of_week,
        ROUND(AVG(se.denver_hour), 0) as average_listening_hour,
        
        -- Discovery patterns
        MIN(se.denver_timestamp) as first_discovered,
        MAX(se.denver_timestamp) as last_played,
        COUNT(DISTINCT se.denver_date) as days_active,
        
        -- Track characteristics
        ROUND(AVG(se.track_duration_minutes), 2) as average_track_duration_minutes,
        ROUND(AVG(se.track_popularity), 1) as average_track_popularity,
        ROUND(100.0 * SUM(CASE WHEN se.track_explicit THEN 1 ELSE 0 END) / COUNT(*), 1) as explicit_content_percentage,
        
        MAX(se.ingested_at) as last_updated
        
    FROM silver_listening_enriched se
    WHERE se.primary_genre IS NOT NULL
    GROUP BY se.primary_genre
)
SELECT 
    ga.*,
    -- CORRECTED: Get actual top artist based on play count
    gas.primary_artist_name as top_artist,
    gas.artist_play_count as top_artist_plays
FROM genre_aggregates ga
LEFT JOIN genre_artist_stats gas ON ga.primary_genre = gas.primary_genre AND gas.artist_rank = 1
ORDER BY ga.total_plays DESC;

-- =============================================================================
-- SHOW CORRECTED RESULTS
-- =============================================================================

SELECT 'CORRECTED GENRE ANALYSIS RESULTS' as report_section;

-- Show the corrected top artists for each genre
SELECT 
    primary_genre,
    top_artist,
    top_artist_plays,
    total_plays as genre_total_plays,
    ROUND(100.0 * top_artist_plays / total_plays, 2) as top_artist_percentage
FROM gold_genre_analysis_corrected
ORDER BY total_plays DESC
LIMIT 15;

-- Focus on country genre to verify the fix
SELECT 'COUNTRY GENRE VERIFICATION' as report_section;

SELECT 
    primary_genre,
    top_artist,
    top_artist_plays,
    total_plays,
    'Should NOT be Adam Doleac unless he truly has the most plays' as note
FROM gold_genre_analysis_corrected
WHERE primary_genre = 'country';

-- =============================================================================
-- MONTHLY INSIGHTS CORRECTION
-- =============================================================================

-- Create corrected monthly view
CREATE OR REPLACE VIEW gold_monthly_insights_corrected AS
WITH monthly_artist_stats AS (
    SELECT 
        denver_year,
        denver_month,
        primary_artist_name,
        COUNT(*) as monthly_plays,
        ROW_NUMBER() OVER (PARTITION BY denver_year, denver_month ORDER BY COUNT(*) DESC, primary_artist_name) as artist_rank
    FROM silver_listening_enriched
    GROUP BY denver_year, denver_month, primary_artist_name
),
monthly_genre_stats AS (
    SELECT 
        denver_year,
        denver_month,
        primary_genre,
        COUNT(*) as genre_plays,
        ROW_NUMBER() OVER (PARTITION BY denver_year, denver_month ORDER BY COUNT(*) DESC, primary_genre) as genre_rank
    FROM silver_listening_enriched
    WHERE primary_genre IS NOT NULL
    GROUP BY denver_year, denver_month, primary_genre
),
monthly_track_stats AS (
    SELECT 
        denver_year,
        denver_month,
        track_name,
        COUNT(*) as track_plays,
        ROW_NUMBER() OVER (PARTITION BY denver_year, denver_month ORDER BY COUNT(*) DESC, track_name) as track_rank
    FROM silver_listening_enriched
    GROUP BY denver_year, denver_month, track_name
),
monthly_base AS (
    SELECT 
        denver_year as year,
        denver_month as month,
        MONTHNAME(DATE_FROM_PARTS(denver_year, denver_month, 1)) as month_name,
        denver_quarter as quarter,
        COUNT(*) as total_plays,
        ROUND(SUM(track_duration_minutes) / 60, 2) as total_listening_hours,
        ROUND(COUNT(*) / COUNT(DISTINCT denver_date), 2) as daily_average_plays,
        COUNT(DISTINCT primary_artist_id) as unique_artists,
        COUNT(DISTINCT primary_genre) as unique_genres,
        ROUND(100.0 * SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) / COUNT(*), 1) as weekend_listening_percentage,
        MIN(time_of_day_category) as primary_time_of_day,
        ROUND(AVG(track_popularity), 1) as average_track_popularity,
        MAX(ingested_at) as max_ingested_at
    FROM silver_listening_enriched se_outer
    GROUP BY denver_year, denver_month, denver_quarter
)
SELECT 
    mb.*,
    -- CORRECTED: Get actual top items based on play count
    mas.primary_artist_name as top_artist,
    mgs.primary_genre as top_genre,
    mts.track_name as top_track,
    ROUND(unique_artists / total_plays * 100, 2) as artist_discovery_rate,
    ROUND(unique_genres / total_plays * 100, 2) as genre_diversity_score,
    CASE 
        WHEN average_track_popularity > 70 THEN 'High Mainstream'
        WHEN average_track_popularity > 50 THEN 'Moderate Mainstream' 
        ELSE 'Underground/Niche'
    END as mainstream_tendency,
    ROUND(total_listening_hours * 60 / total_plays, 2) as average_session_length_minutes
FROM monthly_base mb
LEFT JOIN monthly_artist_stats mas ON mb.year = mas.denver_year AND mb.month = mas.denver_month AND mas.artist_rank = 1
LEFT JOIN monthly_genre_stats mgs ON mb.year = mgs.denver_year AND mb.month = mgs.denver_month AND mgs.genre_rank = 1
LEFT JOIN monthly_track_stats mts ON mb.year = mts.denver_year AND mb.month = mts.denver_month AND mts.track_rank = 1
ORDER BY mb.year DESC, mb.month DESC;

-- Show corrected monthly results
SELECT 'CORRECTED MONTHLY INSIGHTS SAMPLE' as report_section;

SELECT 
    year,
    month,
    month_name,
    top_artist,
    top_genre,
    top_track,
    total_plays
FROM gold_monthly_insights_corrected
ORDER BY year DESC, month DESC
LIMIT 10;

SELECT '✅ MIN() issues identified and corrected views created!' as completion_message;
SELECT 'Use gold_genre_analysis_corrected and gold_monthly_insights_corrected for accurate results' as recommendation;
