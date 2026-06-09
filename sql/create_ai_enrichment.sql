-- =============================================================================
-- Cortex AI Enrichment for Spotify Artist Data
-- 
-- Purpose: Use Snowflake Cortex AI functions to enrich artist genre/mood
--          classification and build embedding-based similarity vectors for
--          music recommendations (replacing the deprecated Spotify
--          /recommendations endpoint).
--
-- Objects created:
--   SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_AI_GENRES       (table)
--   SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_EMBEDDINGS      (table)
--   SPOTIFY_ANALYTICS.ANALYTICS.V_ARTIST_SIMILAR_BY_EMBEDDING (view)
--
-- Prerequisites:
--   - SPOTIFY_ANALYTICS.RAW_DATA.SPOTIFY_ARTIST_GENRES must exist
--   - ACCOUNTADMIN or role with CORTEX AI usage privileges
-- =============================================================================

USE DATABASE SPOTIFY_ANALYTICS;
USE SCHEMA ANALYTICS;

-- -----------------------------------------------------------------------------
-- 1. AI_COMPLETE: Genre/Mood Classification for Unclassified Artists
--
-- Targets artists where genres are empty ('[]') or only contain 'unclassified'.
-- Uses claude-sonnet-4-6 to infer primary_genre and mood from the artist name
-- and any partial genre data available.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_AI_GENRES (
    artist_id           VARCHAR(64)   NOT NULL,
    artist_name         VARCHAR(512),
    original_genres     VARCHAR,
    ai_primary_genre    VARCHAR(128),
    ai_secondary_genre  VARCHAR(128),
    ai_mood             VARCHAR(128),
    ai_confidence       FLOAT,
    ai_model            VARCHAR(64)   DEFAULT 'claude-sonnet-4-6',
    classified_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (artist_id)
);

-- Batch classification query: run this as a scheduled task or one-off job.
-- Processes artists with empty/unclassified genres and inserts AI-inferred genres.
INSERT INTO SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_AI_GENRES
    (artist_id, artist_name, original_genres, ai_primary_genre, ai_secondary_genre, ai_mood, ai_confidence, ai_model)
SELECT
    ag.artist_id,
    ag.artist_name,
    ag.genres AS original_genres,
    TRIM(SPLIT_PART(ai_response, '|', 1))                       AS ai_primary_genre,
    TRIM(SPLIT_PART(ai_response, '|', 2))                       AS ai_secondary_genre,
    TRIM(SPLIT_PART(ai_response, '|', 3))                       AS ai_mood,
    TRY_CAST(TRIM(SPLIT_PART(ai_response, '|', 4)) AS FLOAT)   AS ai_confidence,
    'claude-sonnet-4-6'                                          AS ai_model
FROM (
    SELECT
        artist_id,
        artist_name,
        genres,
        SNOWFLAKE.CORTEX.COMPLETE(
            'claude-sonnet-4-6',
            CONCAT(
                'You are a music genre classifier. Given an artist name, infer their most likely ',
                'primary genre, secondary genre, mood, and your confidence (0.0-1.0). ',
                'Respond ONLY with: primary_genre|secondary_genre|mood|confidence. ',
                'Valid genres: rock, pop, hip-hop, electronic, country, r&b, jazz, blues, ',
                'metal, punk, indie, folk, classical, latin, reggae, soul, funk, ambient, world. ',
                'Valid moods: energetic, melancholic, chill, upbeat, dark, dreamy, aggressive, romantic. ',
                'Artist name: "', artist_name, '". ',
                'Known partial genre info: ', COALESCE(genres, '[]'), '. ',
                'Response:'
            )
        ) AS ai_response
    FROM (
        -- Dedupe source by artist_id BEFORE the LLM call. SPOTIFY_ARTIST_GENRES
        -- is not unique on artist_id (~6 rows/artist); without this the COMPLETE
        -- call runs per duplicate and the target table fans out.
        SELECT artist_id, artist_name, genres
        FROM SPOTIFY_ANALYTICS.RAW_DATA.SPOTIFY_ARTIST_GENRES
        WHERE genres IN ('[]', '["unclassified"]')
          AND artist_id NOT IN (
              SELECT artist_id FROM SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_AI_GENRES
          )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY ingested_at DESC) = 1
    )
    -- Full backfill: no LIMIT. Idempotent via NOT IN above.
) ag
WHERE ai_response IS NOT NULL
  AND SPLIT_PART(ai_response, '|', 1) != '';


-- -----------------------------------------------------------------------------
-- 2. EMBED_TEXT_768: Artist Similarity Embeddings
--
-- Creates vector embeddings for each artist based on their name and primary genre.
-- These embeddings enable cosine-similarity-based recommendations as a replacement
-- for the deprecated Spotify /recommendations endpoint.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_EMBEDDINGS (
    artist_id       VARCHAR(64)   NOT NULL,
    artist_name     VARCHAR(512),
    primary_genre   VARCHAR(128),
    embedding       VECTOR(FLOAT, 768),
    embedded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (artist_id)
);

-- Batch embedding generation: builds vectors from artist_name + primary_genre.
-- Uses the AI-classified genre when the original genre is missing.
INSERT INTO SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_EMBEDDINGS
    (artist_id, artist_name, primary_genre, embedding)
SELECT
    ag.artist_id,
    ag.artist_name,
    COALESCE(ai.ai_primary_genre, ag.primary_genre, 'unknown') AS primary_genre,
    SNOWFLAKE.CORTEX.EMBED_TEXT_768(
        'snowflake-arctic-embed-m',
        CONCAT(
            ag.artist_name,
            ' ',
            COALESCE(ai.ai_primary_genre, ag.primary_genre, 'unknown')
        )
    ) AS embedding
FROM (
    -- Dedupe source by artist_id BEFORE embedding (SPOTIFY_ARTIST_GENRES is not
    -- unique on artist_id). Prevents fan-out and duplicate embedding calls.
    SELECT artist_id, artist_name, primary_genre
    FROM SPOTIFY_ANALYTICS.RAW_DATA.SPOTIFY_ARTIST_GENRES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY ingested_at DESC) = 1
) ag
LEFT JOIN SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_AI_GENRES ai
    ON ag.artist_id = ai.artist_id
WHERE ag.artist_id NOT IN (
    SELECT artist_id FROM SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_EMBEDDINGS
);
-- Full backfill: no LIMIT. Idempotent via NOT IN above.


-- -----------------------------------------------------------------------------
-- 3. Similarity Recommendation View & Example Query
--
-- View that exposes the embedding table for similarity lookups.
-- Example query below demonstrates finding the top-N most similar artists
-- using VECTOR_COSINE_SIMILARITY.
-- -----------------------------------------------------------------------------

CREATE VIEW IF NOT EXISTS SPOTIFY_ANALYTICS.ANALYTICS.V_ARTIST_SIMILAR_BY_EMBEDDING AS
SELECT
    e.artist_id,
    e.artist_name,
    e.primary_genre,
    e.embedding,
    e.embedded_at
FROM SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_EMBEDDINGS e;

-- Example: Find the 10 most similar artists to a given artist (by name).
-- Replace 'Radiohead' with any artist_name in the embeddings table.
--
-- SELECT
--     target.artist_name   AS source_artist,
--     similar.artist_name  AS recommended_artist,
--     similar.primary_genre,
--     VECTOR_COSINE_SIMILARITY(target.embedding, similar.embedding) AS similarity_score
-- FROM SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_EMBEDDINGS target
-- CROSS JOIN SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_EMBEDDINGS similar
-- WHERE target.artist_name = 'Radiohead'
--   AND similar.artist_id != target.artist_id
-- ORDER BY similarity_score DESC
-- LIMIT 10;
