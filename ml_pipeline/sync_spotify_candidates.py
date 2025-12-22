#!/usr/bin/env python3
"""
Pulls fresh candidate tracks from Spotify (based on top genres/artists)
and upserts them into Snowflake.analytics.ml_candidate_tracks.
"""

from __future__ import annotations

import json
from typing import Dict, List

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from cryptography.hazmat.primitives import serialization

from config import settings
from spotify_client import SpotifyClient


TOP_GENRES_SQL = """
SELECT primary_genre
FROM analytics.ml_genre_preference_features
WHERE primary_genre IS NOT NULL
ORDER BY play_count DESC
LIMIT %(limit)s
"""

TOP_ARTISTS_SQL = """
SELECT primary_artist_id, primary_artist_name
FROM analytics.ml_artist_preference_features
WHERE primary_artist_id IS NOT NULL
ORDER BY play_count DESC
LIMIT %(limit)s
"""

LISTENED_TRACKS_SQL = """
SELECT track_id
FROM analytics.ml_listened_track_ids
"""

UPSERT_SQL = """
MERGE INTO analytics.ml_candidate_tracks AS target
USING analytics.ml_candidate_tracks_stage AS source
ON target.track_id = source.track_id
WHEN MATCHED THEN UPDATE SET
    track_name = source.track_name,
    primary_artist_id = source.primary_artist_id,
    primary_artist_name = source.primary_artist_name,
    source = source.source,
    seed_value = source.seed_value,
    track_popularity = source.track_popularity,
    album_release_date = source.album_release_date,
    fetched_at = source.fetched_at,
    raw_payload = source.raw_payload
WHEN NOT MATCHED THEN INSERT (
    track_id,
    track_name,
    primary_artist_id,
    primary_artist_name,
    source,
    seed_value,
    track_popularity,
    album_release_date,
    fetched_at,
    raw_payload
) VALUES (
    source.track_id,
    source.track_name,
    source.primary_artist_id,
    source.primary_artist_name,
    source.source,
    source.seed_value,
    source.track_popularity,
    source.album_release_date,
    source.fetched_at,
    source.raw_payload
)
"""

class CandidateSync:
    def __init__(self, genre_limit: int = 5, artist_limit: int = 5, per_seed: int = 30):
        self.genre_limit = genre_limit
        self.artist_limit = artist_limit
        self.per_seed = per_seed

        self.spotify = SpotifyClient()
        conn_params = {
            "account": settings.snowflake.account,
            "user": settings.snowflake.user,
            "warehouse": settings.snowflake.warehouse,
            "database": settings.snowflake.database,
            "schema": settings.snowflake.schema,
            "role": settings.snowflake.role,
        }

        if settings.snowflake.private_key and settings.snowflake.private_key_path:
            raise ValueError("Provide either SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PRIVATE_KEY_PATH, not both")

        if settings.snowflake.private_key:
            conn_params["authenticator"] = "SNOWFLAKE_JWT"
            conn_params["private_key"] = serialization.load_pem_private_key(
                settings.snowflake.private_key.encode("utf-8"),
                password=settings.snowflake.private_key_passphrase.encode("utf-8")
                if settings.snowflake.private_key_passphrase
                else None,
            )
        elif settings.snowflake.private_key_path:
            conn_params["authenticator"] = "SNOWFLAKE_JWT"
            with open(settings.snowflake.private_key_path, "rb") as f:
                key_bytes = f.read()
            conn_params["private_key"] = serialization.load_pem_private_key(
                key_bytes,
                password=settings.snowflake.private_key_passphrase.encode("utf-8")
                if settings.snowflake.private_key_passphrase
                else None,
            )
        else:
            conn_params["password"] = settings.snowflake.password

        self.snowflake_conn = snowflake.connector.connect(**conn_params)

    def _fetch_listened_tracks(self) -> set[str]:
        df = pd.read_sql(LISTENED_TRACKS_SQL, self.snowflake_conn)
        return set(df["TRACK_ID"]) if not df.empty else set()

    def _top_genres(self) -> List[str]:
        df = pd.read_sql(TOP_GENRES_SQL, self.snowflake_conn, params={"limit": self.genre_limit})
        return df["PRIMARY_GENRE"].tolist()

    def _top_artists(self) -> List[Dict[str, str]]:
        df = pd.read_sql(TOP_ARTISTS_SQL, self.snowflake_conn, params={"limit": self.artist_limit})
        return df.to_dict(orient="records")

    def _search_tracks_by_genre(self, genre: str) -> List[Dict]:
        query = f'genre:"{genre}" year:2020-2025'
        results = self.spotify.search(query, search_type="track", limit=self.per_seed)
        return results.get("tracks", {}).get("items", [])

    def _top_tracks_for_artist(self, artist_id: str) -> List[Dict]:
        artist_top = self.spotify.sp.artist_top_tracks(artist_id)
        return artist_top.get("tracks", [])

    def run(self):
        listened = self._fetch_listened_tracks()
        rows = []

        # Genre-based seeds
        for genre in self._top_genres():
            tracks = self._search_tracks_by_genre(genre)
            for track in tracks:
                if track["id"] in listened:
                    continue
                rows.append(self._transform_track(track, source="genre_seed", seed_value=genre))

        # Artist-based seeds
        for artist in self._top_artists():
            tracks = self._top_tracks_for_artist(artist["PRIMARY_ARTIST_ID"])
            for track in tracks:
                if track["id"] in listened:
                    continue
                rows.append(self._transform_track(track, source="artist_seed", seed_value=artist["PRIMARY_ARTIST_NAME"]))

        if not rows:
            print("No new candidates found.")
            return

        df = pd.DataFrame(rows)
        self._upsert(df)
        print(f"✅ Loaded {len(df)} candidate tracks into analytics.ml_candidate_tracks")

    def _transform_track(self, track: Dict, source: str, seed_value: str) -> Dict:
        return {
            "track_id": track["id"],
            "track_name": track["name"],
            "primary_artist_id": track["artists"][0]["id"],
            "primary_artist_name": track["artists"][0]["name"],
            "source": source,
            "seed_value": seed_value,
            "track_popularity": track.get("popularity"),
            "album_release_date": track.get("album", {}).get("release_date"),
            "fetched_at": pd.Timestamp.utcnow(),
            "raw_payload": json.dumps(track),
        }

    def _upsert(self, df: pd.DataFrame):
        cursor = self.snowflake_conn.cursor()
        cursor.execute("CREATE OR REPLACE TEMP TABLE analytics.ml_candidate_tracks_stage AS SELECT * FROM analytics.ml_candidate_tracks WHERE 1=0")
        try:
            insert_sql = """
                INSERT INTO analytics.ml_candidate_tracks_stage (
                    track_id,
                    track_name,
                    primary_artist_id,
                    primary_artist_name,
                    source,
                    seed_value,
                    track_popularity,
                    album_release_date,
                    fetched_at,
                    raw_payload
                ) VALUES (%(track_id)s, %(track_name)s, %(primary_artist_id)s, %(primary_artist_name)s,
                          %(source)s, %(seed_value)s, %(track_popularity)s, %(album_release_date)s,
                          %(fetched_at)s, %(raw_payload)s)
            """
            rows = df.to_dict(orient="records")
            for row in rows:
                if isinstance(row["album_release_date"], pd.Timestamp):
                    row["album_release_date"] = row["album_release_date"].date()
                if isinstance(row["fetched_at"], pd.Timestamp):
                    row["fetched_at"] = row["fetched_at"].to_pydatetime()
                row["raw_payload"] = None
            cursor.executemany(insert_sql, rows)
            cursor.execute(UPSERT_SQL)
        finally:
            cursor.execute("DROP TABLE IF EXISTS analytics.ml_candidate_tracks_stage")
            cursor.close()

if __name__ == "__main__":
    sync = CandidateSync()
    sync.run()