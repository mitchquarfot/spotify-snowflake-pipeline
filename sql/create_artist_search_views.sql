-- =============================================================================
-- Artist Search Views (source objects for Cortex Search services)
--
-- Splits the artist catalog by ROLE on played tracks:
--   lead     -> artist appears as primary_artist_id on >=1 played track
--   featured -> artist appears ONLY as a non-primary artist on played tracks
-- Both groups have been heard. There is no "unheard / recommended" data here;
-- true discovery would require a separate generated feed (e.g. Last.fm getSimilar).
--
-- Sources:
--   MEDALLION_ARCH.BRONZE_ARTIST_GENRES  (deduped artist catalog, dynamic table)
--   RAW_DATA.SPOTIFY_MT_LISTENING_DEDUPED (deduped plays, source of truth)
--   ANALYTICS.ARTIST_AI_GENRES           (Cortex AI genre/mood classification)
-- =============================================================================

CREATE OR REPLACE VIEW SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_SEARCH_360
COMMENT = 'Unified artist search base. artist_role = lead (primary on >=1 played track) or featured (appears only as a non-primary artist on played tracks). Both groups have been heard.'
AS
WITH listen_stats AS (
    SELECT
        primary_artist_id              AS artist_id,
        MAX(primary_artist_name)       AS artist_name,
        COUNT(*)                       AS play_count,
        COUNT(DISTINCT track_id)       AS unique_tracks,
        MIN(denver_ts)                 AS first_listened_at,
        MAX(denver_ts)                 AS last_listened_at
    FROM SPOTIFY_ANALYTICS.RAW_DATA.SPOTIFY_MT_LISTENING_DEDUPED
    WHERE primary_artist_id IS NOT NULL
    GROUP BY primary_artist_id
)
SELECT
    COALESCE(b.artist_id, ls.artist_id)                                   AS artist_id,
    COALESCE(b.artist_name, ls.artist_name)                               AS artist_name,
    b.primary_genre,
    ai.ai_primary_genre,
    ai.ai_secondary_genre,
    ai.ai_mood,
    ai.ai_confidence,
    ai.ai_model,
    COALESCE(ls.play_count, 0)                                            AS play_count,
    COALESCE(ls.unique_tracks, 0)                                         AS unique_tracks,
    ls.first_listened_at,
    ls.last_listened_at,
    CASE WHEN ls.artist_id IS NOT NULL THEN 'lead' ELSE 'featured' END     AS artist_role,
    TRIM(
        COALESCE(b.artist_name, ls.artist_name)
        || ' | genre: ' || COALESCE(b.primary_genre, ai.ai_primary_genre, 'unknown')
        || CASE WHEN ai.ai_secondary_genre IS NOT NULL THEN ' / ' || ai.ai_secondary_genre ELSE '' END
        || CASE WHEN ai.ai_mood IS NOT NULL THEN ' | mood: ' || ai.ai_mood ELSE '' END
        || CASE WHEN b.genres_list IS NOT NULL AND ARRAY_SIZE(b.genres_list) > 0
                THEN ' | tags: ' || ARRAY_TO_STRING(b.genres_list, ', ') ELSE '' END
    )                                                                     AS search_text
FROM SPOTIFY_ANALYTICS.MEDALLION_ARCH.BRONZE_ARTIST_GENRES b
FULL OUTER JOIN listen_stats ls          ON b.artist_id = ls.artist_id
LEFT JOIN SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_AI_GENRES ai ON COALESCE(b.artist_id, ls.artist_id) = ai.artist_id;

-- Lead artists: core listening. Source for the "my listening" Cortex Search service.
CREATE OR REPLACE VIEW SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_SEARCH_LEAD_ARTISTS
COMMENT = 'Lead artists of tracks you played (primary on >=1 play) - core listening. Source for the listening search service.'
AS
SELECT artist_id, artist_name, primary_genre, ai_primary_genre, ai_secondary_genre, ai_mood,
       play_count, unique_tracks, first_listened_at, last_listened_at, search_text
FROM SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_SEARCH_360
WHERE artist_role = 'lead';

-- Featured-only artists: heard via collaborations, never the lead. NOT unheard recommendations.
CREATE OR REPLACE VIEW SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_SEARCH_FEATURED_ARTISTS
COMMENT = 'Featured-only artists on tracks you played (never the lead). Still heard - NOT unheard recommendations.'
AS
SELECT artist_id, artist_name, primary_genre, ai_primary_genre, ai_secondary_genre, ai_mood, search_text
FROM SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_SEARCH_360
WHERE artist_role = 'featured';
