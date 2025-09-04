-- Identify and Enhance Artists with Empty Genres
-- This script finds artists with empty genre arrays and provides strategies to enhance them

USE ROLE SPOTIFY_ANALYST_ROLE;
USE WAREHOUSE SPOTIFY_WH;
USE DATABASE spotify_analytics;
USE SCHEMA raw_data;

-- =============================================================================
-- STEP 1: Identify artists with empty genres
-- =============================================================================

SELECT 'ARTISTS WITH EMPTY GENRES ANALYSIS' as report_section;

-- Find artists with empty genre arrays (genre_count = 0)
CREATE OR REPLACE TEMPORARY TABLE empty_genre_artists AS
SELECT 
    ag.artist_id,
    ag.artist_name,
    ag.popularity,
    ag.followers_total,
    ag.genre_count,
    ag.data_source,
    ag.ingested_at,
    -- Calculate play statistics for prioritization
    COUNT(DISTINCT h.unique_play) as total_plays,
    COUNT(DISTINCT h.denver_date) as days_active,
    MIN(h.denver_ts) as first_played,
    MAX(h.denver_ts) as last_played
FROM spotify_artist_genres ag
LEFT JOIN spotify_mt_listening_deduped h ON ag.artist_id = h.primary_artist_id
WHERE ag.genre_count = 0  -- Empty genres array
GROUP BY ag.artist_id, ag.artist_name, ag.popularity, ag.followers_total, 
         ag.genre_count, ag.data_source, ag.ingested_at
ORDER BY total_plays DESC;

-- Display the analysis
SELECT 
    artist_id,
    artist_name,
    popularity,
    followers_total,
    total_plays,
    days_active,
    first_played,
    last_played,
    data_source
FROM empty_genre_artists
ORDER BY total_plays DESC
LIMIT 50;

-- Summary statistics
SELECT 'EMPTY GENRES SUMMARY' as report_section;

SELECT 
    COUNT(*) as total_empty_genre_artists,
    SUM(total_plays) as total_plays_affected,
    ROUND(100.0 * SUM(total_plays) / (
        SELECT COUNT(DISTINCT unique_play) 
        FROM spotify_mt_listening_deduped
    ), 2) as percentage_of_total_plays,
    AVG(popularity) as avg_popularity,
    AVG(followers_total) as avg_followers,
    MAX(total_plays) as most_plays_by_empty_genre_artist
FROM empty_genre_artists;

-- =============================================================================
-- STEP 2: Categorize artists by potential genre inference strategies
-- =============================================================================

SELECT 'GENRE INFERENCE OPPORTUNITIES' as report_section;

-- Artists that could benefit from name-based inference
SELECT 'Name-based inference candidates:' as category;

SELECT 
    artist_name,
    artist_id,
    total_plays,
    popularity,
    CASE 
        WHEN LOWER(artist_name) LIKE '%dj %' OR LOWER(artist_name) LIKE '%dj_%' THEN 'electronic'
        WHEN LOWER(artist_name) LIKE '%lil %' OR LOWER(artist_name) LIKE '%young %' OR LOWER(artist_name) LIKE '%big %' THEN 'hip hop'
        WHEN LOWER(artist_name) LIKE '%band%' OR LOWER(artist_name) LIKE '%rock%' THEN 'rock'
        WHEN LOWER(artist_name) LIKE '%pop%' THEN 'pop'
        WHEN LOWER(artist_name) LIKE '%indie%' THEN 'indie'
        WHEN LOWER(artist_name) LIKE '%country%' THEN 'country'
        WHEN LOWER(artist_name) LIKE '%jazz%' OR LOWER(artist_name) LIKE '%blues%' THEN 'jazz'
        WHEN LOWER(artist_name) LIKE '%latin%' OR LOWER(artist_name) LIKE '%spanish%' THEN 'latin'
        ELSE NULL
    END as suggested_genre
FROM empty_genre_artists
WHERE CASE 
    WHEN LOWER(artist_name) LIKE '%dj %' OR LOWER(artist_name) LIKE '%dj_%' THEN 'electronic'
    WHEN LOWER(artist_name) LIKE '%lil %' OR LOWER(artist_name) LIKE '%young %' OR LOWER(artist_name) LIKE '%big %' THEN 'hip hop'
    WHEN LOWER(artist_name) LIKE '%band%' OR LOWER(artist_name) LIKE '%rock%' THEN 'rock'
    WHEN LOWER(artist_name) LIKE '%pop%' THEN 'pop'
    WHEN LOWER(artist_name) LIKE '%indie%' THEN 'indie'
    WHEN LOWER(artist_name) LIKE '%country%' THEN 'country'
    WHEN LOWER(artist_name) LIKE '%jazz%' OR LOWER(artist_name) LIKE '%blues%' THEN 'jazz'
    WHEN LOWER(artist_name) LIKE '%latin%' OR LOWER(artist_name) LIKE '%spanish%' THEN 'latin'
    ELSE NULL
END IS NOT NULL
ORDER BY total_plays DESC
LIMIT 20;

-- Artists that could benefit from popularity-based inference
SELECT 'Popularity-based inference candidates:' as category;

SELECT 
    artist_name,
    artist_id,
    popularity,
    followers_total,
    total_plays,
    CASE 
        WHEN popularity >= 80 THEN 'mainstream pop'
        WHEN popularity >= 60 THEN 'pop'
        WHEN popularity >= 40 THEN 'alternative'
        WHEN popularity >= 20 THEN 'indie'
        ELSE 'underground'
    END as suggested_genre
FROM empty_genre_artists
WHERE popularity IS NOT NULL
ORDER BY total_plays DESC
LIMIT 20;

-- =============================================================================
-- STEP 3: Generate artist IDs for reprocessing
-- =============================================================================

SELECT 'ARTIST IDS FOR REPROCESSING' as report_section;

-- Get comma-separated list of top empty-genre artist IDs
SELECT 
    LISTAGG(artist_id, ',') WITHIN GROUP (ORDER BY total_plays DESC) as artist_ids_csv
FROM (
    SELECT artist_id, total_plays
    FROM empty_genre_artists
    ORDER BY total_plays DESC
    LIMIT 100  -- Process top 100 empty-genre artists
);

-- =============================================================================
-- STEP 4: Create manual genre assignment table (optional)
-- =============================================================================

SELECT 'MANUAL GENRE ASSIGNMENTS' as report_section;

-- Create a table for manual genre assignments
CREATE OR REPLACE TABLE manual_artist_genre_assignments (
    artist_id STRING,
    artist_name STRING,
    assigned_genres ARRAY,
    primary_genre STRING,
    assignment_reason STRING,
    assigned_by STRING,
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Example manual assignments (you can add more)
INSERT INTO manual_artist_genre_assignments 
(artist_id, artist_name, assigned_genres, primary_genre, assignment_reason, assigned_by)
VALUES 
('61S5H9Lxn1PDUvu1TV0kCX', 'Knox', ['pop', 'alternative'], 'pop', 'popularity_and_followers_analysis', 'manual_review');

-- Show example of how to use manual assignments
SELECT 'Example manual genre assignment update:' as example;

/*
-- Example update query (uncomment to use):
UPDATE spotify_artist_genres ag
SET 
    genres = m.assigned_genres,
    genres_list = m.assigned_genres,
    primary_genre = m.primary_genre,
    genre_count = ARRAY_SIZE(m.assigned_genres),
    data_source = 'manual_assignment_' || m.assignment_reason,
    ingested_at = CURRENT_TIMESTAMP()
FROM manual_artist_genre_assignments m
WHERE ag.artist_id = m.artist_id
  AND ag.genre_count = 0;  -- Only update empty genre artists
*/

-- =============================================================================
-- STEP 5: Verification queries
-- =============================================================================

SELECT 'VERIFICATION: Genre Coverage After Enhancement' as report_section;

-- Check overall genre coverage
WITH coverage_stats AS (
    SELECT 
        COUNT(*) as total_artists,
        COUNT(CASE WHEN genre_count > 0 THEN 1 END) as artists_with_genres,
        COUNT(CASE WHEN genre_count = 0 THEN 1 END) as artists_without_genres
    FROM spotify_artist_genres
)
SELECT 
    total_artists,
    artists_with_genres,
    artists_without_genres,
    ROUND(100.0 * artists_with_genres / total_artists, 2) as coverage_percentage
FROM coverage_stats;

-- Clean up
DROP TABLE empty_genre_artists;

SELECT '✅ Empty genre analysis complete!' as completion_message;
