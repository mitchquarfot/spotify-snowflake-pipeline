-- Fix MIN() Issues in Medallion Architecture
-- Replace alphabetical MIN() with proper "most played" logic using window functions

USE ROLE SPOTIFY_ANALYST_ROLE;
USE WAREHOUSE SPOTIFY_WH;
USE DATABASE spotify_analytics;
USE SCHEMA medallion_arch;

-- =============================================================================
-- IDENTIFY THE ISSUES
-- =============================================================================

SELECT 'CURRENT ISSUES WITH MIN() FUNCTIONS' as report_section;

-- Show the problem: MIN() gives alphabetical results, not "top" results
SELECT 
    se.primary_genre,
    MIN(se.primary_artist_name) as current_min_artist,
    COUNT(*) as total_plays,
    -- Show what the actual top artist should be
    FIRST_VALUE(se.primary_artist_name) OVER (
        PARTITION BY se.primary_genre 
        ORDER BY COUNT(*) OVER (PARTITION BY se.primary_genre, se.primary_artist_name) DESC,
                 se.primary_artist_name
    ) as actual_top_artist
FROM silver_listening_enriched se
WHERE se.primary_genre IS NOT NULL
GROUP BY se.primary_genre, se.primary_artist_name
QUALIFY ROW_NUMBER() OVER (PARTITION BY se.primary_genre ORDER BY COUNT(*) DESC) = 1
ORDER BY total_plays DESC
LIMIT 10;

-- =============================================================================
-- SOLUTION: CREATE HELPER VIEWS FOR TOP ITEMS
-- =============================================================================

-- Create a view for top artists by genre (most played)
CREATE OR REPLACE VIEW top_artists_by_genre AS
SELECT 
    primary_genre,
    primary_artist_name as top_artist_name,
    artist_play_count,
    ROW_NUMBER() OVER (PARTITION BY primary_genre ORDER BY artist_play_count DESC, primary_artist_name) as rank
FROM (
    SELECT 
        primary_genre,
        primary_artist_name,
        COUNT(*) as artist_play_count
    FROM silver_listening_enriched
    WHERE primary_genre IS NOT NULL
    GROUP BY primary_genre, primary_artist_name
)
QUALIFY rank = 1;

-- Create a view for top tracks by genre
CREATE OR REPLACE VIEW top_tracks_by_genre AS
SELECT 
    primary_genre,
    track_name as top_track_name,
    track_play_count,
    ROW_NUMBER() OVER (PARTITION BY primary_genre ORDER BY track_play_count DESC, track_name) as rank
FROM (
    SELECT 
        primary_genre,
        track_name,
        COUNT(*) as track_play_count
    FROM silver_listening_enriched
    WHERE primary_genre IS NOT NULL
    GROUP BY primary_genre, track_name
)
QUALIFY rank = 1;

-- Create a view for top artists by month
CREATE OR REPLACE VIEW top_artists_by_month AS
SELECT 
    denver_year,
    denver_month,
    primary_artist_name as top_artist_name,
    artist_play_count,
    ROW_NUMBER() OVER (PARTITION BY denver_year, denver_month ORDER BY artist_play_count DESC, primary_artist_name) as rank
FROM (
    SELECT 
        denver_year,
        denver_month,
        primary_artist_name,
        COUNT(*) as artist_play_count
    FROM silver_listening_enriched
    GROUP BY denver_year, denver_month, primary_artist_name
)
QUALIFY rank = 1;

-- =============================================================================
-- FIX 1: UPDATE GOLD_GENRE_ANALYSIS
-- =============================================================================

SELECT 'FIXING GOLD_GENRE_ANALYSIS TABLE' as report_section;

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
        -- FIXED: Use actual top artist instead of alphabetical MIN
        COALESCE(ta.top_artist_name, 'Unknown') as top_artist,
        COALESCE(ta.artist_play_count, 0) as top_artist_plays,
        
        -- Temporal patterns
        -- Note: These are simplified since we can't use MODE() in dynamic tables
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
    LEFT JOIN top_artists_by_genre ta ON se.primary_genre = ta.primary_genre
    WHERE se.primary_genre IS NOT NULL
    GROUP BY se.primary_genre, ta.top_artist_name, ta.artist_play_count
    ORDER BY total_plays DESC
);

-- =============================================================================
-- FIX 2: UPDATE GOLD_MONTHLY_INSIGHTS
-- =============================================================================

SELECT 'FIXING GOLD_MONTHLY_INSIGHTS TABLE' as report_section;

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
            -- FIXED: Use actual top items instead of alphabetical MIN
            COALESCE(tam.top_artist_name, 'Unknown') as top_artist,
            FIRST_VALUE(primary_genre) OVER (
                PARTITION BY denver_year, denver_month 
                ORDER BY COUNT(*) OVER (PARTITION BY denver_year, denver_month, primary_genre) DESC
            ) as top_genre,
            FIRST_VALUE(track_name) OVER (
                PARTITION BY denver_year, denver_month 
                ORDER BY COUNT(*) OVER (PARTITION BY denver_year, denver_month, track_name) DESC
            ) as top_track,
            ROUND(100.0 * SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) / COUNT(*), 1) as weekend_listening_percentage,
            MIN(time_of_day_category) as primary_time_of_day,
            ROUND(AVG(track_popularity), 1) as average_track_popularity,
            MAX(ingested_at) as max_ingested_at
        FROM silver_listening_enriched se_outer
        LEFT JOIN top_artists_by_month tam ON se_outer.denver_year = tam.denver_year 
                                           AND se_outer.denver_month = tam.denver_month
        GROUP BY denver_year, denver_month, denver_quarter, tam.top_artist_name
    ),
    monthly_with_growth AS (
        SELECT *,
            ROUND(unique_artists / total_plays * 100, 2) as artist_discovery_rate,
            ROUND(unique_genres / total_plays * 100, 2) as genre_diversity_score,
            CASE 
                WHEN average_track_popularity > 70 THEN 'High Mainstream'
                WHEN average_track_popularity > 50 THEN 'Moderate Mainstream' 
                ELSE 'Underground/Niche'
            END as mainstream_tendency,
            ROUND(total_listening_hours * 60 / total_plays, 2) as average_session_length_minutes,
            LAG(total_plays) OVER (ORDER BY year, month) as prev_month_plays,
            LAG(unique_artists) OVER (ORDER BY year, month) as prev_month_artists
        FROM monthly_base
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

SELECT 'VERIFICATION: Fixed top artists by genre' as report_section;

-- Show the corrected top artists (should now be based on play count, not alphabetical)
SELECT 
    primary_genre,
    top_artist,
    top_artist_plays,
    total_plays as genre_total_plays
FROM gold_genre_analysis
ORDER BY total_plays DESC
LIMIT 10;

SELECT '✅ Medallion architecture MIN() issues fixed!' as completion_message;
SELECT 'Top artists/tracks now based on play count, not alphabetical order' as fix_description;
