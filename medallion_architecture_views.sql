-- Medallion Architecture for Spotify Analytics with Mountain Time and Genre Integration
-- Bronze → Silver → Gold Data Transformation Pipeline
-- Location: Denver, CO (Mountain Time)

USE DATABASE spotify_analytics;
USE SCHEMA raw_data;

-- =============================================================================
-- BRONZE LAYER: Raw data with minimal processing
-- =============================================================================

-- Bronze: Your existing deduplicated listening history (already created)
-- SPOTIFY_MT_LISTENING_DEDUPED serves as our Bronze layer

-- Bronze: Raw artist genres (minimal processing)
CREATE OR REPLACE DYNAMIC TABLE bronze_artist_genres(
    ARTIST_ID,
    ARTIST_NAME,
    ARTIST_URI,
    GENRES,
    GENRES_LIST,
    PRIMARY_GENRE,
    GENRE_COUNT,
    POPULARITY,
    FOLLOWERS_TOTAL,
    EXTERNAL_URLS,
    IMAGES,
    INGESTED_AT,
    DATA_SOURCE
) TARGET_LAG = '6 hours' 
  REFRESH_MODE = INCREMENTAL 
  INITIALIZE = ON_CREATE 
  WAREHOUSE = SPOTIFY_WH
AS (
    SELECT *
    FROM raw_data.spotify_artist_genres
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY artist_id 
        ORDER BY ingested_at DESC
    ) = 1
);

-- =============================================================================
-- SILVER LAYER: Cleaned, enriched, and business-ready data
-- =============================================================================

-- Silver: Enhanced listening history with genre information and Mountain Time
CREATE OR REPLACE DYNAMIC TABLE silver_listening_enriched(
    -- Core identifiers
    UNIQUE_PLAY,
    LISTENING_EVENT_ID,
    
    -- Mountain Time columns
    DENVER_TIMESTAMP,
    DENVER_DATE,
    DENVER_HOUR,
    DENVER_DAY_OF_WEEK,
    DENVER_MONTH,
    DENVER_YEAR,
    DENVER_QUARTER,
    IS_WEEKEND,
    TIME_OF_DAY_CATEGORY,
    
    -- Original timestamp info
    UTC_PLAYED_AT,
    UTC_TIMESTAMP,
    
    -- Track information  
    TRACK_ID,
    TRACK_NAME,
    TRACK_DURATION_MS,
    TRACK_DURATION_MINUTES,
    TRACK_POPULARITY,
    TRACK_EXPLICIT,
    TRACK_PREVIEW_URL,
    TRACK_URI,
    
    -- Artist information with genres
    PRIMARY_ARTIST_ID,
    PRIMARY_ARTIST_NAME,
    ARTIST_POPULARITY,
    ARTIST_FOLLOWERS,
    PRIMARY_GENRE,
    ALL_GENRES,
    GENRE_COUNT,
    IS_MULTI_GENRE_ARTIST,
    
    -- Album information
    ALBUM_ID,
    ALBUM_NAME,
    ALBUM_TYPE,
    ALBUM_RELEASE_DATE,
    ALBUM_RELEASE_YEAR,
    ALBUM_TOTAL_TRACKS,
    ALBUM_AGE_YEARS,
    IS_SINGLE,
    IS_COMPILATION,
    
    -- Context and metadata
    CONTEXT_TYPE,
    CONTEXT_URI,
    LISTENING_SOURCE,
    INGESTED_AT,
    DATA_SOURCE
    
) TARGET_LAG = '6 hours'
  REFRESH_MODE = INCREMENTAL 
  INITIALIZE = ON_CREATE 
  WAREHOUSE = SPOTIFY_WH
AS (
    SELECT 
        -- Core identifiers
        h.unique_play,
        ROW_NUMBER() OVER (ORDER BY h.denver_ts) as listening_event_id,
        
        -- Mountain Time enrichment
        h.denver_ts as denver_timestamp,
        h.denver_ts::DATE as denver_date,
        EXTRACT(HOUR FROM h.denver_ts) as denver_hour,
        DAYOFWEEK(h.denver_ts) as denver_day_of_week,
        EXTRACT(MONTH FROM h.denver_ts) as denver_month,
        EXTRACT(YEAR FROM h.denver_ts) as denver_year,
        EXTRACT(QUARTER FROM h.denver_ts) as denver_quarter,
        CASE WHEN DAYOFWEEK(h.denver_ts) IN (1, 7) THEN TRUE ELSE FALSE END as is_weekend,
        CASE 
            WHEN EXTRACT(HOUR FROM h.denver_ts) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM h.denver_ts) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM h.denver_ts) BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Night'
        END as time_of_day_category,
        
        -- Original UTC timestamps
        h.played_at as utc_played_at,
        h.played_at_timestamp as utc_timestamp,
        
        -- Track information
        h.track_id,
        h.track_name,
        h.track_duration_ms,
        ROUND(h.track_duration_ms / 60000.0, 2) as track_duration_minutes,
        h.track_popularity,
        h.track_explicit,
        h.track_preview_url,
        h.track_uri,
        
        -- Artist information enriched with genres
        h.primary_artist_id,
        h.primary_artist_name,
        ag.popularity as artist_popularity,
        ag.followers_total as artist_followers,
        ag.primary_genre,
        ag.genres_list as all_genres,
        ag.genre_count,
        CASE WHEN ag.genre_count > 1 THEN TRUE ELSE FALSE END as is_multi_genre_artist,
        
        -- Album information
        h.album_id,
        h.album_name,
        h.album_type,
        h.album_release_date,
        TRY_CAST(SUBSTRING(h.album_release_date, 1, 4) AS INTEGER) as album_release_year,
        h.album_total_tracks,
        DATEDIFF('year', TRY_CAST(h.album_release_date AS DATE), CURRENT_DATE()) as album_age_years,
        CASE WHEN h.album_type = 'single' THEN TRUE ELSE FALSE END as is_single,
        CASE WHEN h.album_type = 'compilation' THEN TRUE ELSE FALSE END as is_compilation,
        
        -- Context and metadata
        h.context_type,
        h.context_uri,
        COALESCE(h.context_type, 'unknown') as listening_source,
        h.ingested_at,
        h.data_source
        
    FROM spotify_mt_listening_deduped h
    LEFT JOIN bronze_artist_genres ag ON h.primary_artist_id = ag.artist_id
);

-- Silver: Artist summary with listening metrics
CREATE OR REPLACE DYNAMIC TABLE silver_artist_summary(
    ARTIST_ID,
    ARTIST_NAME,
    ARTIST_URI,
    
    -- Genre information
    PRIMARY_GENRE,
    ALL_GENRES,
    GENRE_COUNT,
    IS_MULTI_GENRE_ARTIST,
    
    -- Spotify metrics
    ARTIST_POPULARITY,
    ARTIST_FOLLOWERS,
    
    -- Personal listening metrics
    TOTAL_PLAYS,
    UNIQUE_TRACKS_PLAYED,
    UNIQUE_ALBUMS_PLAYED,
    TOTAL_LISTENING_MINUTES,
    AVERAGE_TRACK_POPULARITY,
    FIRST_LISTENED,
    LAST_LISTENED,
    DAYS_SINCE_LAST_PLAY,
    LISTENING_STREAK_DAYS,
    
    -- Temporal patterns
    FAVORITE_TIME_OF_DAY,
    FAVORITE_DAY_OF_WEEK,
    WEEKEND_PLAY_PERCENTAGE,
    
    -- Metadata
    LAST_UPDATED
    
) TARGET_LAG = '6 hours'
  REFRESH_MODE = INCREMENTAL 
  INITIALIZE = ON_CREATE 
  WAREHOUSE = SPOTIFY_WH  
AS (
    SELECT 
        ag.artist_id,
        ag.artist_name,
        ag.artist_uri,
        
        -- Genre information
        ag.primary_genre,
        ag.genres_list as all_genres,
        ag.genre_count,
        CASE WHEN ag.genre_count > 1 THEN TRUE ELSE FALSE END as is_multi_genre_artist,
        
        -- Spotify metrics
        ag.popularity as artist_popularity,
        ag.followers_total as artist_followers,
        
        -- Personal listening metrics
        COUNT(*) as total_plays,
        COUNT(DISTINCT h.track_id) as unique_tracks_played,
        COUNT(DISTINCT h.album_id) as unique_albums_played,
        ROUND(SUM(h.track_duration_ms) / 60000.0, 2) as total_listening_minutes,
        ROUND(AVG(h.track_popularity), 1) as average_track_popularity,
        MIN(h.denver_ts) as first_listened,
        MAX(h.denver_ts) as last_listened,
        DATEDIFF('day', MAX(h.denver_ts), CURRENT_TIMESTAMP()) as days_since_last_play,
        DATEDIFF('day', MIN(h.denver_ts), MAX(h.denver_ts)) + 1 as listening_streak_days,
        
        -- Temporal patterns
        MODE(h.time_of_day_category) as favorite_time_of_day,
        MODE(DAYNAME(h.denver_ts)) as favorite_day_of_week,
        ROUND(100.0 * SUM(CASE WHEN h.is_weekend THEN 1 ELSE 0 END) / COUNT(*), 1) as weekend_play_percentage,
        
        -- Metadata
        CURRENT_TIMESTAMP() as last_updated
        
    FROM bronze_artist_genres ag
    LEFT JOIN silver_listening_enriched h ON ag.artist_id = h.primary_artist_id
    WHERE ag.artist_id IS NOT NULL
    GROUP BY ag.artist_id, ag.artist_name, ag.artist_uri, ag.primary_genre, 
             ag.genres_list, ag.genre_count, ag.popularity, ag.followers_total
);

-- =============================================================================
-- GOLD LAYER: Business metrics and analytics-ready aggregations
-- =============================================================================

-- Gold: Daily listening summary with genre insights
CREATE OR REPLACE DYNAMIC TABLE gold_daily_listening_summary(
    DENVER_DATE,
    DAY_OF_WEEK,
    IS_WEEKEND,
    MONTH_NAME,
    QUARTER,
    YEAR,
    
    -- Listening volume metrics
    TOTAL_PLAYS,
    UNIQUE_TRACKS,
    UNIQUE_ARTISTS,
    UNIQUE_ALBUMS,
    UNIQUE_GENRES,
    TOTAL_LISTENING_MINUTES,
    AVERAGE_TRACK_LENGTH_MINUTES,
    
    -- Genre diversity metrics
    TOP_GENRE,
    GENRE_DIVERSITY_SCORE,
    MULTI_GENRE_ARTIST_PERCENTAGE,
    
    -- Discovery metrics
    NEW_ARTISTS_DISCOVERED,
    NEW_TRACKS_DISCOVERED,
    REPEAT_PLAY_PERCENTAGE,
    
    -- Context analysis
    PLAYLIST_PLAY_PERCENTAGE,
    ALBUM_PLAY_PERCENTAGE,
    ARTIST_PLAY_PERCENTAGE,
    
    -- Popularity metrics
    AVERAGE_TRACK_POPULARITY,
    AVERAGE_ARTIST_POPULARITY,
    MAINSTREAM_SCORE,
    
    -- Temporal patterns
    MOST_ACTIVE_HOUR,
    LISTENING_SESSIONS,
    
    LAST_UPDATED
    
) TARGET_LAG = '6 hours'
  REFRESH_MODE = INCREMENTAL 
  INITIALIZE = ON_CREATE 
  WAREHOUSE = SPOTIFY_WH
AS (
    SELECT 
        denver_date,
        DAYNAME(denver_date) as day_of_week,
        is_weekend,
        MONTHNAME(denver_date) as month_name,
        denver_quarter as quarter,
        denver_year as year,
        
        -- Listening volume metrics
        COUNT(*) as total_plays,
        COUNT(DISTINCT track_id) as unique_tracks,
        COUNT(DISTINCT primary_artist_id) as unique_artists,
        COUNT(DISTINCT album_id) as unique_albums,
        COUNT(DISTINCT primary_genre) as unique_genres,
        ROUND(SUM(track_duration_minutes), 2) as total_listening_minutes,
        ROUND(AVG(track_duration_minutes), 2) as average_track_length_minutes,
        
        -- Genre diversity metrics
        MODE(primary_genre) as top_genre,
        ROUND(COUNT(DISTINCT primary_genre) / COUNT(*) * 100, 2) as genre_diversity_score,
        ROUND(100.0 * SUM(CASE WHEN is_multi_genre_artist THEN 1 ELSE 0 END) / COUNT(*), 1) as multi_genre_artist_percentage,
        
        -- Discovery metrics (simplified for daily view)
        COUNT(DISTINCT primary_artist_id) as new_artists_discovered,
        COUNT(DISTINCT track_id) as new_tracks_discovered,
        ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT unique_play)) / COUNT(*), 1) as repeat_play_percentage,
        
        -- Context analysis
        ROUND(100.0 * SUM(CASE WHEN listening_source = 'playlist' THEN 1 ELSE 0 END) / COUNT(*), 1) as playlist_play_percentage,
        ROUND(100.0 * SUM(CASE WHEN listening_source = 'album' THEN 1 ELSE 0 END) / COUNT(*), 1) as album_play_percentage,
        ROUND(100.0 * SUM(CASE WHEN listening_source = 'artist' THEN 1 ELSE 0 END) / COUNT(*), 1) as artist_play_percentage,
        
        -- Popularity metrics
        ROUND(AVG(track_popularity), 1) as average_track_popularity,
        ROUND(AVG(artist_popularity), 1) as average_artist_popularity,
        ROUND((AVG(track_popularity) + AVG(artist_popularity)) / 2, 1) as mainstream_score,
        
        -- Temporal patterns
        MODE(denver_hour) as most_active_hour,
        COUNT(DISTINCT DATE_TRUNC('hour', denver_timestamp)) as listening_sessions,
        
        CURRENT_TIMESTAMP() as last_updated
        
    FROM silver_listening_enriched
    WHERE denver_date IS NOT NULL
    GROUP BY denver_date, is_weekend, denver_quarter, denver_year
    ORDER BY denver_date DESC
);

-- Gold: Genre analysis and trends
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
    FAVORITE_TIME_OF_DAY,
    FAVORITE_DAY_OF_WEEK,
    PEAK_LISTENING_HOUR,
    
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
        primary_genre,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as genre_rank,
        
        -- Listening metrics
        COUNT(*) as total_plays,
        COUNT(DISTINCT primary_artist_id) as unique_artists,
        COUNT(DISTINCT track_id) as unique_tracks,
        ROUND(SUM(track_duration_minutes), 2) as total_listening_minutes,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage_of_total_listening,
        
        -- Artist diversity
        ROUND(AVG(artist_popularity), 1) as average_artist_popularity,
        ROUND(AVG(artist_followers), 0) as average_artist_followers,
        MODE(primary_artist_name) as top_artist,
        MAX(artist_play_count.play_count) as top_artist_plays,
        
        -- Temporal patterns
        MODE(time_of_day_category) as favorite_time_of_day,
        MODE(DAYNAME(denver_timestamp)) as favorite_day_of_week,
        MODE(denver_hour) as peak_listening_hour,
        
        -- Discovery patterns
        MIN(denver_timestamp) as first_discovered,
        MAX(denver_timestamp) as last_played,
        COUNT(DISTINCT denver_date) as days_active,
        
        -- Track characteristics
        ROUND(AVG(track_duration_minutes), 2) as average_track_duration_minutes,
        ROUND(AVG(track_popularity), 1) as average_track_popularity,
        ROUND(100.0 * SUM(CASE WHEN track_explicit THEN 1 ELSE 0 END) / COUNT(*), 1) as explicit_content_percentage,
        
        CURRENT_TIMESTAMP() as last_updated
        
    FROM silver_listening_enriched se
    LEFT JOIN (
        -- Subquery to get play counts per artist within each genre
        SELECT primary_genre, primary_artist_name, COUNT(*) as play_count
        FROM silver_listening_enriched 
        GROUP BY primary_genre, primary_artist_name
    ) artist_play_count ON se.primary_genre = artist_play_count.primary_genre 
                         AND se.primary_artist_name = artist_play_count.primary_artist_name
    WHERE primary_genre IS NOT NULL
    GROUP BY primary_genre
    ORDER BY total_plays DESC
);

-- Gold: Monthly listening insights with year-over-year comparisons
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
    
    -- Top items
    TOP_ARTIST,
    TOP_GENRE,
    TOP_TRACK,
    
    -- Behavioral patterns
    WEEKEND_LISTENING_PERCENTAGE,
    MOST_ACTIVE_TIME_OF_DAY,
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
            MONTHNAME(denver_date) as month_name,
            denver_quarter as quarter,
            COUNT(*) as total_plays,
            ROUND(SUM(track_duration_minutes) / 60, 2) as total_listening_hours,
            ROUND(COUNT(*) / COUNT(DISTINCT denver_date), 2) as daily_average_plays,
            COUNT(DISTINCT primary_artist_id) as unique_artists,
            COUNT(DISTINCT primary_genre) as unique_genres,
            MODE(primary_artist_name) as top_artist,
            MODE(primary_genre) as top_genre,
            MODE(track_name) as top_track,
            ROUND(100.0 * SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) / COUNT(*), 1) as weekend_listening_percentage,
            MODE(time_of_day_category) as most_active_time_of_day,
            ROUND(AVG(track_popularity), 1) as average_track_popularity
        FROM silver_listening_enriched
        GROUP BY denver_year, denver_month, denver_quarter
    ),
    monthly_with_growth AS (
        SELECT *,
            ROUND(COUNT(DISTINCT primary_artist_id) / COUNT(*) * 100, 2) as artist_discovery_rate,
            ROUND(COUNT(DISTINCT primary_genre) / COUNT(*) * 100, 2) as genre_diversity_score,
            CASE 
                WHEN average_track_popularity > 70 THEN 'High Mainstream'
                WHEN average_track_popularity > 50 THEN 'Moderate Mainstream' 
                ELSE 'Underground/Niche'
            END as mainstream_tendency,
            ROUND(AVG(track_duration_minutes), 2) as average_session_length_minutes,
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
        most_active_time_of_day,
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
        CURRENT_TIMESTAMP() as last_updated
    FROM monthly_with_growth
    ORDER BY year DESC, month DESC
);

-- =============================================================================
-- UTILITY VIEWS: Quick access and monitoring
-- =============================================================================

-- Current month snapshot
CREATE OR REPLACE VIEW current_month_snapshot AS
SELECT * 
FROM gold_monthly_insights 
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE()) 
  AND month = EXTRACT(MONTH FROM CURRENT_DATE());

-- Recent listening activity (last 7 days)
CREATE OR REPLACE VIEW recent_activity AS
SELECT 
    denver_timestamp,
    track_name,
    primary_artist_name,
    primary_genre,
    album_name,
    time_of_day_category,
    listening_source
FROM silver_listening_enriched
WHERE denver_date >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY denver_timestamp DESC
LIMIT 100;

-- Top discoveries this month
CREATE OR REPLACE VIEW monthly_discoveries AS
SELECT 
    primary_artist_name,
    primary_genre,
    COUNT(*) as plays_this_month,
    MIN(denver_timestamp) as first_played,
    MAX(denver_timestamp) as last_played
FROM silver_listening_enriched
WHERE denver_date >= DATE_TRUNC('month', CURRENT_DATE())
  AND primary_artist_id NOT IN (
      SELECT DISTINCT primary_artist_id 
      FROM silver_listening_enriched 
      WHERE denver_date < DATE_TRUNC('month', CURRENT_DATE())
  )
GROUP BY primary_artist_name, primary_genre
ORDER BY plays_this_month DESC;

-- Genre progression over time
CREATE OR REPLACE VIEW genre_timeline AS
SELECT 
    denver_date,
    primary_genre,
    COUNT(*) as daily_plays,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY denver_date), 2) as daily_genre_percentage
FROM silver_listening_enriched
WHERE denver_date >= DATEADD('month', -6, CURRENT_DATE())
  AND primary_genre IS NOT NULL
GROUP BY denver_date, primary_genre
ORDER BY denver_date DESC, daily_plays DESC;

-- =============================================================================
-- SAMPLE QUERIES FOR VALIDATION
-- =============================================================================

-- Test the medallion architecture with sample queries:

-- 1. Current listening trends
-- SELECT * FROM gold_daily_listening_summary WHERE denver_date >= DATEADD('day', -30, CURRENT_DATE()) ORDER BY denver_date DESC;

-- 2. Genre analysis
-- SELECT * FROM gold_genre_analysis ORDER BY total_plays DESC;

-- 3. Artist deep dive
-- SELECT * FROM silver_artist_summary WHERE total_plays >= 10 ORDER BY total_plays DESC;

-- 4. Monthly insights
-- SELECT * FROM gold_monthly_insights ORDER BY year DESC, month DESC;

-- 5. Recent discoveries
-- SELECT * FROM monthly_discoveries;

-- 6. Time-based patterns
-- SELECT time_of_day_category, COUNT(*) as plays, COUNT(DISTINCT primary_genre) as genres 
-- FROM silver_listening_enriched 
-- GROUP BY time_of_day_category 
-- ORDER BY plays DESC;

-- Setup complete! Your medallion architecture is now ready with Mountain Time support and genre integration.
