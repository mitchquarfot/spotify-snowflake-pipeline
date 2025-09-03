-- Fix MIN() Issues with FULL Refresh Mode
-- Avoid incremental refresh issues by using FULL mode and simplified logic

USE ROLE SPOTIFY_ANALYST_ROLE;
USE WAREHOUSE SPOTIFY_WH;
USE DATABASE spotify_analytics;
USE SCHEMA medallion_arch;

-- =============================================================================
-- APPROACH: Use FULL refresh mode to avoid incremental tracking issues
-- =============================================================================

SELECT 'FIXING DYNAMIC TABLES WITH FULL REFRESH MODE' as report_section;

-- =============================================================================
-- FIX 1: GOLD_GENRE_ANALYSIS with FULL refresh and simplified logic
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
  REFRESH_MODE = FULL  -- Changed from INCREMENTAL to FULL
  INITIALIZE = ON_CREATE 
  WAREHOUSE = SPOTIFY_WH
AS (
    WITH genre_base_stats AS (
        SELECT 
            se.primary_genre,
            COUNT(*) as total_plays,
            COUNT(DISTINCT se.primary_artist_id) as unique_artists,
            COUNT(DISTINCT se.track_id) as unique_tracks,
            ROUND(SUM(se.track_duration_minutes), 2) as total_listening_minutes,
            ROUND(AVG(se.artist_popularity), 1) as average_artist_popularity,
            ROUND(AVG(se.artist_followers), 0) as average_artist_followers,
            MIN(se.time_of_day_category) as primary_time_of_day,
            MIN(DAYNAME(se.denver_timestamp)) as primary_day_of_week,
            ROUND(AVG(se.denver_hour), 0) as average_listening_hour,
            MIN(se.denver_timestamp) as first_discovered,
            MAX(se.denver_timestamp) as last_played,
            COUNT(DISTINCT se.denver_date) as days_active,
            ROUND(AVG(se.track_duration_minutes), 2) as average_track_duration_minutes,
            ROUND(AVG(se.track_popularity), 1) as average_track_popularity,
            ROUND(100.0 * SUM(CASE WHEN se.track_explicit THEN 1 ELSE 0 END) / COUNT(*), 1) as explicit_content_percentage,
            MAX(se.ingested_at) as last_updated
        FROM silver_listening_enriched se
        WHERE se.primary_genre IS NOT NULL
        GROUP BY se.primary_genre
    ),
    genre_with_top_artist AS (
        SELECT 
            gbs.*,
            ROW_NUMBER() OVER (ORDER BY gbs.total_plays DESC) as genre_rank,
            ROUND(100.0 * gbs.total_plays / SUM(gbs.total_plays) OVER (), 2) as percentage_of_total_listening,
            -- Get top artist using window function to avoid joins
            FIRST_VALUE(se2.primary_artist_name) OVER (
                PARTITION BY gbs.primary_genre 
                ORDER BY COUNT(*) OVER (PARTITION BY gbs.primary_genre, se2.primary_artist_name) DESC,
                         se2.primary_artist_name
            ) as top_artist,
            MAX(COUNT(*) OVER (PARTITION BY gbs.primary_genre, se2.primary_artist_name)) OVER (
                PARTITION BY gbs.primary_genre
            ) as top_artist_plays
        FROM genre_base_stats gbs,
             silver_listening_enriched se2
        WHERE gbs.primary_genre = se2.primary_genre
        GROUP BY gbs.primary_genre, gbs.total_plays, gbs.unique_artists, gbs.unique_tracks, 
                 gbs.total_listening_minutes, gbs.average_artist_popularity, gbs.average_artist_followers,
                 gbs.primary_time_of_day, gbs.primary_day_of_week, gbs.average_listening_hour,
                 gbs.first_discovered, gbs.last_played, gbs.days_active, gbs.average_track_duration_minutes,
                 gbs.average_track_popularity, gbs.explicit_content_percentage, gbs.last_updated,
                 se2.primary_artist_name
        QUALIFY ROW_NUMBER() OVER (PARTITION BY gbs.primary_genre ORDER BY COUNT(*) DESC, se2.primary_artist_name) = 1
    )
    SELECT 
        primary_genre,
        genre_rank,
        total_plays,
        unique_artists,
        unique_tracks,
        total_listening_minutes,
        percentage_of_total_listening,
        average_artist_popularity,
        average_artist_followers,
        top_artist,
        top_artist_plays,
        primary_time_of_day,
        primary_day_of_week,
        average_listening_hour,
        first_discovered,
        last_played,
        days_active,
        average_track_duration_minutes,
        average_track_popularity,
        explicit_content_percentage,
        last_updated
    FROM genre_with_top_artist
    ORDER BY total_plays DESC
);

-- =============================================================================
-- SIMPLIFIED ALTERNATIVE: Use a simpler approach without complex window functions
-- =============================================================================

-- Let's try a much simpler approach that definitely works
CREATE OR REPLACE DYNAMIC TABLE gold_genre_analysis_simple(
    PRIMARY_GENRE,
    GENRE_RANK,
    TOTAL_PLAYS,
    UNIQUE_ARTISTS,
    UNIQUE_TRACKS,
    TOTAL_LISTENING_MINUTES,
    PERCENTAGE_OF_TOTAL_LISTENING,
    AVERAGE_ARTIST_POPULARITY,
    AVERAGE_ARTIST_FOLLOWERS,
    PRIMARY_TIME_OF_DAY,
    PRIMARY_DAY_OF_WEEK,
    AVERAGE_LISTENING_HOUR,
    FIRST_DISCOVERED,
    LAST_PLAYED,
    DAYS_ACTIVE,
    AVERAGE_TRACK_DURATION_MINUTES,
    AVERAGE_TRACK_POPULARITY,
    EXPLICIT_CONTENT_PERCENTAGE,
    LAST_UPDATED
    
) TARGET_LAG = '6 hours'
  REFRESH_MODE = FULL
  INITIALIZE = ON_CREATE 
  WAREHOUSE = SPOTIFY_WH
AS (
    SELECT 
        se.primary_genre,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as genre_rank,
        COUNT(*) as total_plays,
        COUNT(DISTINCT se.primary_artist_id) as unique_artists,
        COUNT(DISTINCT se.track_id) as unique_tracks,
        ROUND(SUM(se.track_duration_minutes), 2) as total_listening_minutes,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage_of_total_listening,
        ROUND(AVG(se.artist_popularity), 1) as average_artist_popularity,
        ROUND(AVG(se.artist_followers), 0) as average_artist_followers,
        MIN(se.time_of_day_category) as primary_time_of_day,
        MIN(DAYNAME(se.denver_timestamp)) as primary_day_of_week,
        ROUND(AVG(se.denver_hour), 0) as average_listening_hour,
        MIN(se.denver_timestamp) as first_discovered,
        MAX(se.denver_timestamp) as last_played,
        COUNT(DISTINCT se.denver_date) as days_active,
        ROUND(AVG(se.track_duration_minutes), 2) as average_track_duration_minutes,
        ROUND(AVG(se.track_popularity), 1) as average_track_popularity,
        ROUND(100.0 * SUM(CASE WHEN se.track_explicit THEN 1 ELSE 0 END) / COUNT(*), 1) as explicit_content_percentage,
        MAX(se.ingested_at) as last_updated
    FROM silver_listening_enriched se
    WHERE se.primary_genre IS NOT NULL
    GROUP BY se.primary_genre
    ORDER BY total_plays DESC
);

-- =============================================================================
-- CREATE SEPARATE VIEW FOR TOP ARTISTS (not a dynamic table)
-- =============================================================================

-- Create a regular view for top artists by genre
CREATE OR REPLACE VIEW top_artists_by_genre_view AS
SELECT 
    primary_genre,
    primary_artist_name as top_artist,
    artist_play_count as top_artist_plays,
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

-- =============================================================================
-- GOLD_MONTHLY_INSIGHTS with FULL refresh
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE gold_monthly_insights(
    YEAR,
    MONTH,
    MONTH_NAME,
    QUARTER,
    TOTAL_PLAYS,
    TOTAL_LISTENING_HOURS,
    DAILY_AVERAGE_PLAYS,
    UNIQUE_ARTISTS,
    UNIQUE_GENRES,
    ARTIST_DISCOVERY_RATE,
    GENRE_DIVERSITY_SCORE,
    WEEKEND_LISTENING_PERCENTAGE,
    PRIMARY_TIME_OF_DAY,
    AVERAGE_SESSION_LENGTH_MINUTES,
    AVERAGE_TRACK_POPULARITY,
    MAINSTREAM_TENDENCY,
    PLAYS_GROWTH_RATE,
    ARTIST_DISCOVERY_GROWTH,
    LAST_UPDATED
    
) TARGET_LAG = '1 day'
  REFRESH_MODE = FULL  -- Changed from INCREMENTAL to FULL
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
    monthly_with_growth AS (
        SELECT 
            *,
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

SELECT 'VERIFICATION: Dynamic tables created with FULL refresh mode' as report_section;

-- Check that the tables were created
SELECT 
    table_name,
    target_lag,
    refresh_mode,
    refresh_status
FROM INFORMATION_SCHEMA.DYNAMIC_TABLES 
WHERE table_schema = 'MEDALLION_ARCH'
  AND table_name IN ('GOLD_GENRE_ANALYSIS_SIMPLE', 'GOLD_MONTHLY_INSIGHTS')
ORDER BY table_name;

-- Check the top artists view
SELECT 'TOP ARTISTS BY GENRE (from view)' as report_section;

SELECT 
    primary_genre,
    top_artist,
    top_artist_plays
FROM top_artists_by_genre_view
ORDER BY top_artist_plays DESC
LIMIT 10;

SELECT '✅ Dynamic tables created with FULL refresh mode!' as completion_message;
SELECT 'Use gold_genre_analysis_simple for genre stats and top_artists_by_genre_view for top artists' as usage_note;
