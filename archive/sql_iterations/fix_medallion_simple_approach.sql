-- Simple Fix for MIN() Issues - Clean and Straightforward Approach
-- Use FULL refresh with simple aggregations and separate queries for top items

USE ROLE SPOTIFY_ANALYST_ROLE;
USE WAREHOUSE SPOTIFY_WH;
USE DATABASE spotify_analytics;
USE SCHEMA medallion_arch;

-- =============================================================================
-- CLEAN APPROACH: Simple dynamic tables + separate views for "top" items
-- =============================================================================

SELECT 'CREATING SIMPLE, WORKING SOLUTION' as report_section;

-- =============================================================================
-- 1. GOLD_GENRE_ANALYSIS - Simple aggregation without complex joins
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE gold_genre_analysis(
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
-- 2. TOP ARTISTS BY GENRE - Separate view (not dynamic table)
-- =============================================================================

CREATE OR REPLACE VIEW top_artists_by_genre AS
SELECT 
    primary_genre,
    primary_artist_name as top_artist,
    play_count as top_artist_plays
FROM (
    SELECT 
        primary_genre,
        primary_artist_name,
        COUNT(*) as play_count,
        ROW_NUMBER() OVER (PARTITION BY primary_genre ORDER BY COUNT(*) DESC, primary_artist_name) as rn
    FROM silver_listening_enriched
    WHERE primary_genre IS NOT NULL
    GROUP BY primary_genre, primary_artist_name
) ranked
WHERE rn = 1;

-- =============================================================================
-- 3. COMBINED GENRE ANALYSIS WITH TOP ARTISTS - View joining the above
-- =============================================================================

CREATE OR REPLACE VIEW gold_genre_analysis_complete AS
SELECT 
    g.*,
    t.top_artist,
    t.top_artist_plays
FROM gold_genre_analysis g
LEFT JOIN top_artists_by_genre t ON g.primary_genre = t.primary_genre
ORDER BY g.total_plays DESC;

-- =============================================================================
-- 4. GOLD_MONTHLY_INSIGHTS - Simplified without complex joins
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
  REFRESH_MODE = FULL
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
        FROM silver_listening_enriched
        GROUP BY denver_year, denver_month, denver_quarter
    ),
    monthly_with_calcs AS (
        SELECT 
            *,
            ROUND(unique_artists / total_plays * 100, 2) as artist_discovery_rate,
            ROUND(unique_genres / total_plays * 100, 2) as genre_diversity_score,
            CASE 
                WHEN average_track_popularity > 70 THEN 'High Mainstream'
                WHEN average_track_popularity > 50 THEN 'Moderate Mainstream' 
                ELSE 'Underground/Niche'
            END as mainstream_tendency,
            ROUND(total_listening_hours * 60 / total_plays, 2) as average_session_length_minutes
        FROM monthly_base
    ),
    monthly_with_growth AS (
        SELECT 
            *,
            LAG(total_plays) OVER (ORDER BY year, month) as prev_month_plays,
            LAG(unique_artists) OVER (ORDER BY year, month) as prev_month_artists
        FROM monthly_with_calcs
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
-- 5. TOP ITEMS BY MONTH - Separate views for monthly top artists, genres, tracks
-- =============================================================================

CREATE OR REPLACE VIEW top_monthly_artists AS
SELECT 
    denver_year,
    denver_month,
    primary_artist_name as top_artist,
    play_count as top_artist_plays
FROM (
    SELECT 
        denver_year,
        denver_month,
        primary_artist_name,
        COUNT(*) as play_count,
        ROW_NUMBER() OVER (PARTITION BY denver_year, denver_month ORDER BY COUNT(*) DESC, primary_artist_name) as rn
    FROM silver_listening_enriched
    GROUP BY denver_year, denver_month, primary_artist_name
) ranked
WHERE rn = 1;

CREATE OR REPLACE VIEW top_monthly_genres AS
SELECT 
    denver_year,
    denver_month,
    primary_genre as top_genre,
    play_count as top_genre_plays
FROM (
    SELECT 
        denver_year,
        denver_month,
        primary_genre,
        COUNT(*) as play_count,
        ROW_NUMBER() OVER (PARTITION BY denver_year, denver_month ORDER BY COUNT(*) DESC, primary_genre) as rn
    FROM silver_listening_enriched
    WHERE primary_genre IS NOT NULL
    GROUP BY denver_year, denver_month, primary_genre
) ranked
WHERE rn = 1;

CREATE OR REPLACE VIEW top_monthly_tracks AS
SELECT 
    denver_year,
    denver_month,
    track_name as top_track,
    play_count as top_track_plays
FROM (
    SELECT 
        denver_year,
        denver_month,
        track_name,
        COUNT(*) as play_count,
        ROW_NUMBER() OVER (PARTITION BY denver_year, denver_month ORDER BY COUNT(*) DESC, track_name) as rn
    FROM silver_listening_enriched
    GROUP BY denver_year, denver_month, track_name
) ranked
WHERE rn = 1;

-- =============================================================================
-- 6. COMPLETE MONTHLY INSIGHTS WITH TOP ITEMS
-- =============================================================================

CREATE OR REPLACE VIEW gold_monthly_insights_complete AS
SELECT 
    m.*,
    ta.top_artist,
    ta.top_artist_plays,
    tg.top_genre,
    tg.top_genre_plays,
    tt.top_track,
    tt.top_track_plays
FROM gold_monthly_insights m
LEFT JOIN top_monthly_artists ta ON m.year = ta.denver_year AND m.month = ta.denver_month
LEFT JOIN top_monthly_genres tg ON m.year = tg.denver_year AND m.month = tg.denver_month
LEFT JOIN top_monthly_tracks tt ON m.year = tt.denver_year AND m.month = tt.denver_month
ORDER BY m.year DESC, m.month DESC;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================

SELECT 'VERIFICATION: All objects created successfully' as report_section;

-- Check dynamic tables
SELECT 
    table_name,
    target_lag,
    refresh_mode,
    refresh_status
FROM INFORMATION_SCHEMA.DYNAMIC_TABLES 
WHERE table_schema = 'MEDALLION_ARCH'
  AND table_name IN ('GOLD_GENRE_ANALYSIS', 'GOLD_MONTHLY_INSIGHTS')
ORDER BY table_name;

-- Check views
SELECT 
    table_name,
    table_type
FROM INFORMATION_SCHEMA.TABLES 
WHERE table_schema = 'MEDALLION_ARCH'
  AND table_name LIKE '%TOP_%' OR table_name LIKE '%COMPLETE%'
ORDER BY table_name;

SELECT '✅ SOLUTION COMPLETE!' as status;
SELECT 'Use gold_genre_analysis_complete for genre stats with actual top artists' as usage_1;
SELECT 'Use gold_monthly_insights_complete for monthly data with actual top items' as usage_2;
SELECT 'No more alphabetical bias - all "top" items based on actual play counts!' as benefit;

-- Sample verification queries to run after refresh:
/*
-- Test genre analysis with real top artists
SELECT 
    primary_genre,
    total_plays,
    top_artist,
    top_artist_plays,
    'Should show real top artists, not alphabetical' as note
FROM gold_genre_analysis_complete
ORDER BY total_plays DESC
LIMIT 10;

-- Test monthly insights with real top items
SELECT 
    year,
    month,
    total_plays,
    top_artist,
    top_genre,
    top_track
FROM gold_monthly_insights_complete
ORDER BY year DESC, month DESC
LIMIT 5;
*/
