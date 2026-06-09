-- =============================================================================
-- Artist Recommendations ("artists I would like") — AI-generated discovery feed
--
-- Generates genuinely UNHEARD artist recommendations using Cortex AI
-- (claude-sonnet-4-6), seeded from the listener's top LEAD artists, then filters
-- out anything already in the catalog so results are truly new.
--
-- This is the data source for the "artists I would like" Cortex Search service
-- (distinct from ARTIST_SEARCH_LEAD_ARTISTS / ARTIST_SEARCH_FEATURED_ARTISTS,
-- which are both music already heard).
--
-- LIMITATION: LLM-suggested names are not yet validated against a real catalog,
-- so a few may be misspelled or non-existent. is_verified stays FALSE until a
-- future Last.fm / Spotify pass validates names and adds real popularity metrics.
-- =============================================================================

CREATE TABLE IF NOT EXISTS SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_RECOMMENDATIONS (
    rec_id              VARCHAR(36)   NOT NULL DEFAULT UUID_STRING(),
    recommended_artist  VARCHAR(512)  NOT NULL,
    suggested_genre     VARCHAR(128),
    reason              VARCHAR(2048),
    seed_artist         VARCHAR(512),
    is_verified         BOOLEAN       DEFAULT FALSE,
    model               VARCHAR(64)   DEFAULT 'claude-sonnet-4-6',
    generated_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (recommended_artist)
);

-- Generation: 5 recommendations per top-30 lead artist, parsed, filtered against
-- the catalog (so only unheard artists), deduped by artist name. Idempotent via
-- the catalog filter + PK; re-running adds only new unheard names.
INSERT INTO SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_RECOMMENDATIONS
    (recommended_artist, suggested_genre, reason, seed_artist)
WITH seeds AS (
    SELECT artist_name, primary_genre, play_count
    FROM SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_SEARCH_LEAD_ARTISTS
    WHERE primary_genre IS NOT NULL
    ORDER BY play_count DESC
    LIMIT 30
),
gen AS (
    SELECT
        s.artist_name AS seed_artist,
        SNOWFLAKE.CORTEX.COMPLETE(
            'claude-sonnet-4-6',
            CONCAT(
                'A listener loves the artist "', s.artist_name, '" (genre: ', s.primary_genre, '). ',
                'Recommend exactly 5 real, currently-active or well-known music artists they likely have NOT heard but would enjoy. ',
                'Avoid extremely famous mainstream artists and avoid the seed artist itself. ',
                'Return ONLY 5 lines, no numbering, no preamble. Each line EXACTLY: ArtistName~genre~one short reason. ',
                'Use ~ as the field separator.'
            )
        ) AS resp
    FROM seeds s
),
parsed AS (
    SELECT
        g.seed_artist,
        TRIM(SPLIT_PART(line.value, '~', 1)) AS recommended_artist,
        TRIM(SPLIT_PART(line.value, '~', 2)) AS suggested_genre,
        TRIM(SPLIT_PART(line.value, '~', 3)) AS reason
    FROM gen g,
         LATERAL FLATTEN(input => SPLIT(g.resp, '\n')) line
    WHERE TRIM(line.value) <> ''
      AND SPLIT_PART(line.value, '~', 1) <> ''
      AND SPLIT_PART(line.value, '~', 2) <> ''
)
SELECT recommended_artist, suggested_genre, reason, seed_artist
FROM parsed
WHERE UPPER(recommended_artist) NOT IN (
        SELECT UPPER(artist_name) FROM SPOTIFY_ANALYTICS.MEDALLION_ARCH.BRONZE_ARTIST_GENRES
      )
  AND UPPER(recommended_artist) NOT IN (
        SELECT UPPER(recommended_artist) FROM SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_RECOMMENDATIONS
      )
QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(recommended_artist) ORDER BY seed_artist) = 1;

-- Search-ready view: source for the "artists I would like" Cortex Search service.
CREATE OR REPLACE VIEW SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_SEARCH_RECOMMENDED
COMMENT = 'AI-generated unheard artist recommendations (claude-sonnet-4-6), seeded from top lead artists and filtered against the catalog. Source for the "artists I would like" Cortex Search service. is_verified=FALSE until validated against Last.fm/Spotify.'
AS
SELECT
    rec_id,
    recommended_artist AS artist_name,
    suggested_genre,
    seed_artist,
    reason,
    is_verified,
    TRIM(recommended_artist || ' | genre: ' || COALESCE(suggested_genre, 'unknown')
         || ' | like: ' || seed_artist
         || CASE WHEN reason IS NOT NULL THEN ' | why: ' || reason ELSE '' END) AS search_text
FROM SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_RECOMMENDATIONS;
