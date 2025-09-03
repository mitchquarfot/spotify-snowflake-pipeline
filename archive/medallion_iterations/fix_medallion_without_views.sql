-- Fix MIN() Issues in Medallion Architecture WITHOUT Helper Views
-- Embed the "top item" logic directly in dynamic tables to avoid view dependency issues

USE ROLE SPOTIFY_ANALYST_ROLE;
USE WAREHOUSE SPOTIFY_WH;
USE DATABASE spotify_analytics;
USE SCHEMA medallion_arch;

-- =============================================================================
-- APPROACH: Use Window Functions Directly in Dynamic Tables
-- =============================================================================

SELECT 'FIXING DYNAMIC TABLES WITH EMBEDDED LOGIC' as report_section;

-- =============================================================================
-- FIX 1: GOLD_GENRE_ANALYSIS with embedded top artist logic
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE gold_genre_analysis(
    PRIMARY_GENRE,
    GENRE_RANK,
    
    -- Listening metrics
    TOTAL_PLAYS,
    UNIQUE_ARTISTS,
    UNIQUE_TRACKS,
    TOTAL_LISTENING_MINUTES,
    PERCENTAGE_OF_TOTAL_LISTENING,
    
    -- Artist diversity
    AVERAGE_ARTIST_POPULARITY,
    AVERAGE_ARTIST_FOLLOWERS,
    TOP_ARTIST,
    TOP_ARTIST_PLAYS,
    
    -- Temporal patterns
    PRIMARY_TIME_OF_DAY,
    PRIMARY_DAY_OF_WEEK,
    AVERAGE_LISTENING_HOUR,
    
    -- Discovery patterns
    FIRST_DISCOVERED,
    LAST_PLAYED,
    DAYS_ACTIVE,
    
    -- Track characteristics
    AVERAGE_TRACK_DURATION_MINUTES,
    AVERAGE_TRACK_POPULARITY,
    EXPLICIT_CONTENT_PERCENTAGE,
    
    LAST_UPDATED
    
) TARGET_LAG = '6 hours'
  REFRESH_MODE = INCREMENTAL 
  INITIALIZE = ON_CREATE 
  WAREHOUSE = SPOTIFY_WH
AS (
    WITH genre_stats AS (
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
    ),
    top_artists_per_genre AS (
        SELECT 
            se.primary_genre,
            se.primary_artist_name,
            COUNT(*) as artist_play_count,
            ROW_NUMBER() OVER (PARTITION BY se.primary_genre ORDER BY COUNT(*) DESC, se.primary_artist_name) as artist_rank
        FROM silver_listening_enriched se
        WHERE se.primary_genre IS NOT NULL
        GROUP BY se.primary_genre, se.primary_artist_name
        QUALIFY artist_rank = 1
    )
    SELECT 
        gs.primary_genre,
        gs.genre_rank,
        gs.total_plays,
        gs.unique_artists,
        gs.unique_tracks,
        gs.total_listening_minutes,
        gs.percentage_of_total_listening,
        gs.average_artist_popularity,
        gs.average_artist_followers,
        -- FIXED: Use actual top artist based on play count
        COALESCE(ta.primary_artist_name, 'Unknown') as top_artist,
        COALESCE(ta.artist_play_count, 0) as top_artist_plays,
        gs.primary_time_of_day,
        gs.primary_day_of_week,
        gs.average_listening_hour,
        gs.first_discovered,
        gs.last_played,
        gs.days_active,
        gs.average_track_duration_minutes,
        gs.average_track_popularity,
        gs.explicit_content_percentage,
        gs.last_updated
    FROM genre_stats gs
    LEFT JOIN top_artists_per_genre ta ON gs.primary_genre = ta.primary_genre
    ORDER BY gs.total_plays DESC
);

-- =============================================================================
-- FIX 2: GOLD_MONTHLY_INSIGHTS with embedded top items logic
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE gold_monthly_insights(
    YEAR,
    MONTH,
    MONTH_NAME,
    QUARTER,
    
    -- Volume metrics
    TOTAL_PLAYS,
    TOTAL_LISTENING_HOURS,
    DAILY_AVERAGE_PLAYS,
    
    -- Diversity metrics
    UNIQUE_ARTISTS,
    UNIQUE_GENRES,
    ARTIST_DISCOVERY_RATE,
    GENRE_DIVERSITY_SCORE,
    
    -- Top items (FIXED)
    TOP_ARTIST,
    TOP_GENRE,
    TOP_TRACK,
    
    -- Behavioral patterns
    WEEKEND_LISTENING_PERCENTAGE,
    PRIMARY_TIME_OF_DAY,
    AVERAGE_SESSION_LENGTH_MINUTES,
    
    -- Quality metrics
    AVERAGE_TRACK_POPULARITY,
    MAINSTREAM_TENDENCY,
    
    -- Growth metrics (month-over-month)
    PLAYS_GROWTH_RATE,
    ARTIST_DISCOVERY_GROWTH,
    
    LAST_UPDATED
    
) TARGET_LAG = '1 day'
  REFRESH_MODE = INCREMENTAL 
  INITIALIZE = ON_CREATE 
  WAREHOUSE = SPOTIFY_WH
AS (
    WITH monthly_base AS (
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
    ),
    top_artists_per_month AS (
        SELECT 
            denver_year,
            denver_month,
            primary_artist_name,
            COUNT(*) as monthly_plays,
            ROW_NUMBER() OVER (PARTITION BY denver_year, denver_month ORDER BY COUNT(*) DESC, primary_artist_name) as artist_rank
        FROM silver_listening_enriched
        GROUP BY denver_year, denver_month, primary_artist_name
        QUALIFY artist_rank = 1
    ),
    top_genres_per_month AS (
        SELECT 
            denver_year,
            denver_month,
            primary_genre,
            COUNT(*) as genre_plays,
            ROW_NUMBER() OVER (PARTITION BY denver_year, denver_month ORDER BY COUNT(*) DESC, primary_genre) as genre_rank
        FROM silver_listening_enriched
        WHERE primary_genre IS NOT NULL
        GROUP BY denver_year, denver_month, primary_genre
        QUALIFY genre_rank = 1
    ),
    top_tracks_per_month AS (
        SELECT 
            denver_year,
            denver_month,
            track_name,
            COUNT(*) as track_plays,
            ROW_NUMBER() OVER (PARTITION BY denver_year, denver_month ORDER BY COUNT(*) DESC, track_name) as track_rank
        FROM silver_listening_enriched
        GROUP BY denver_year, denver_month, track_name
        QUALIFY track_rank = 1
    ),
    monthly_with_growth AS (
        SELECT 
            mb.*,
            -- FIXED: Use actual top items based on play count
            COALESCE(tam.primary_artist_name, 'Unknown') as top_artist,
            COALESCE(tgm.primary_genre, 'Unknown') as top_genre,
            COALESCE(ttm.track_name, 'Unknown') as top_track,
            ROUND(mb.unique_artists / mb.total_plays * 100, 2) as artist_discovery_rate,
            ROUND(mb.unique_genres / mb.total_plays * 100, 2) as genre_diversity_score,
            CASE 
                WHEN mb.average_track_popularity > 70 THEN 'High Mainstream'
                WHEN mb.average_track_popularity > 50 THEN 'Moderate Mainstream' 
                ELSE 'Underground/Niche'
            END as mainstream_tendency,
            ROUND(mb.total_listening_hours * 60 / mb.total_plays, 2) as average_session_length_minutes,
            LAG(mb.total_plays) OVER (ORDER BY mb.year, mb.month) as prev_month_plays,
            LAG(mb.unique_artists) OVER (ORDER BY mb.year, mb.month) as prev_month_artists
        FROM monthly_base mb
        LEFT JOIN top_artists_per_month tam ON mb.year = tam.denver_year AND mb.month = tam.denver_month
        LEFT JOIN top_genres_per_month tgm ON mb.year = tgm.denver_year AND mb.month = tgm.denver_month
        LEFT JOIN top_tracks_per_month ttm ON mb.year = ttm.denver_year AND mb.month = ttm.denver_month
    )
    SELECT 
        year,
        month,
        month_name,
        quarter,
        total_plays,
        total_listening_hours,
        daily_average_plays,
        unique_artists,
        unique_genres,
        artist_discovery_rate,
        genre_diversity_score,
        top_artist,
        top_genre,
        top_track,
        weekend_listening_percentage,
        primary_time_of_day,
        average_session_length_minutes,
        average_track_popularity,
        mainstream_tendency,
        CASE 
            WHEN prev_month_plays IS NOT NULL 
            THEN ROUND(100.0 * (total_plays - prev_month_plays) / prev_month_plays, 2)
            ELSE NULL 
        END as plays_growth_rate,
        CASE 
            WHEN prev_month_artists IS NOT NULL 
            THEN ROUND(100.0 * (unique_artists - prev_month_artists) / prev_month_artists, 2)
            ELSE NULL 
        END as artist_discovery_growth,
        max_ingested_at as last_updated
    FROM monthly_with_growth
    ORDER BY year DESC, month DESC
);

-- =============================================================================
-- VERIFICATION
-- =============================================================================

SELECT 'VERIFICATION: Fixed dynamic tables created successfully' as report_section;

-- Check that the tables were created
SELECT 
    table_name,
    target_lag,
    refresh_mode,
    refresh_status
FROM INFORMATION_SCHEMA.DYNAMIC_TABLES 
WHERE table_schema = 'MEDALLION_ARCH'
  AND table_name IN ('GOLD_GENRE_ANALYSIS', 'GOLD_MONTHLY_INSIGHTS')
ORDER BY table_name;

-- Wait for initial refresh, then check results
SELECT 'Wait 2-3 minutes for initial refresh, then run verification queries' as next_steps;

-- Verification queries to run after refresh:
/*
-- Check genre analysis results
SELECT 
    primary_genre,
    top_artist,
    top_artist_plays,
    total_plays,
    'Should show actual top artists, not alphabetical' as note
FROM gold_genre_analysis
ORDER BY total_plays DESC
LIMIT 10;

-- Check monthly insights results  
SELECT 
    year,
    month,
    top_artist,
    top_genre,
    top_track,
    total_plays
FROM gold_monthly_insights
ORDER BY year DESC, month DESC
LIMIT 10;
*/

SELECT '✅ Dynamic tables recreated with embedded top-item logic!' as completion_message;
SELECT 'No helper views needed - all logic embedded directly in dynamic tables' as solution_note;
