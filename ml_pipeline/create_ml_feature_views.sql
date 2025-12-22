USE DATABASE spotify_analytics;
USE SCHEMA analytics;

CREATE OR REPLACE VIEW ml_user_recent_listens AS
SELECT
    r.unique_play,
    r.denver_ts,
    r.played_at,
    r.played_at_timestamp,
    r.played_at_date,
    r.played_at_hour,
    r.track_id,
    r.track_name,
    r.track_duration_ms,
    r.track_popularity,
    r.track_explicit,
    r.track_preview_url,
    r.track_external_urls,
    r.track_uri,
    r.primary_artist_id,
    r.primary_artist_name,
    ag.primary_genre,
    ag.genres_list,
    ag.genre_count,
    ag.popularity        AS artist_popularity,
    ag.followers_total   AS artist_followers,
    r.album_id,
    r.album_name,
    r.album_type,
    r.album_release_date,
    r.album_total_tracks,
    r.context_type,
    r.context_uri,
    r.context_external_urls,
    r.ingested_at,
    r.data_source
FROM raw_data.spotify_mt_listening_deduped r
LEFT JOIN raw_data.spotify_artist_genres ag
  ON r.primary_artist_id = ag.artist_id;

CREATE OR REPLACE VIEW ml_genre_preference_features AS
SELECT
    primary_genre,
    COUNT(*)                        AS play_count,
    COUNT(DISTINCT track_id)        AS unique_tracks,
    COUNT(DISTINCT primary_artist_id) AS unique_artists,
    AVG(track_popularity)           AS avg_track_popularity,
    AVG(track_duration_ms)          AS avg_track_duration_ms,
    MIN(denver_ts)                  AS first_listened_at,
    MAX(denver_ts)                  AS last_listened_at
FROM ml_user_recent_listens
WHERE primary_genre IS NOT NULL
GROUP BY primary_genre;

CREATE OR REPLACE VIEW ml_artist_preference_features AS
SELECT
    primary_artist_id,
    primary_artist_name,
    COUNT(*)                 AS play_count,
    COUNT(DISTINCT track_id) AS unique_tracks,
    AVG(track_popularity)    AS avg_track_popularity,
    MIN(denver_ts)           AS first_listened_at,
    MAX(denver_ts)           AS last_listened_at
FROM ml_user_recent_listens
WHERE primary_artist_id IS NOT NULL
GROUP BY primary_artist_id, primary_artist_name;

CREATE OR REPLACE VIEW ml_track_preference_features AS
SELECT
    track_id,
    track_name,
    primary_artist_id,
    primary_artist_name,
    primary_genre,
    COUNT(*)              AS play_count,
    AVG(track_popularity) AS avg_track_popularity,
    MIN(denver_ts)        AS first_listened_at,
    MAX(denver_ts)        AS last_listened_at
FROM ml_user_recent_listens
WHERE track_id IS NOT NULL
GROUP BY track_id, track_name, primary_artist_id, primary_artist_name, primary_genre;

CREATE OR REPLACE VIEW ml_listened_track_ids AS
SELECT DISTINCT track_id
FROM ml_user_recent_listens
WHERE track_id IS NOT NULL;