"""Offline recommendation helpers built on Snowflake data.

This module reads listening-history aggregates and pre-fetched candidate tracks
from Snowflake, computes simple scoring features, and returns ranked
recommendations without hitting Spotify's recommendation endpoint.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import settings


@dataclass
class RecommendationCandidate:
    """Container for recommended Spotify track metadata."""

    track_id: str
    track_name: str
    artist_id: str
    artist_name: str
    source: str
    reason: str
    score: float


class SnowflakeFeatureStore:
    """Fetches listening-history aggregates from Snowflake."""

    GENRE_SQL = """
        SELECT
            primary_genre,
            play_count,
            unique_tracks,
            unique_artists,
            avg_track_popularity,
            last_listened_at
        FROM analytics.ml_genre_preference_features
        WHERE play_count >= %(min_plays)s
        ORDER BY play_count DESC
        LIMIT %(limit)s
    """

    ARTIST_SQL = """
        SELECT
            primary_artist_id,
            primary_artist_name,
            play_count,
            unique_tracks,
            avg_track_popularity,
            last_listened_at
        FROM analytics.ml_artist_preference_features
        WHERE play_count >= %(min_plays)s
        ORDER BY play_count DESC
        LIMIT %(limit)s
    """

    TRACK_HISTORY_SQL = """
        SELECT track_id
        FROM analytics.ml_listened_track_ids
    """

    CANDIDATE_SQL = """
        SELECT
            c.track_id,
            c.track_name,
            c.primary_artist_id,
            c.primary_artist_name,
            c.source,
            c.seed_value,
            c.track_popularity,
            c.album_release_date,
            c.fetched_at,
            gp.play_count AS genre_play_count,
            ap.play_count AS artist_play_count
        FROM analytics.ml_candidate_tracks c
        LEFT JOIN analytics.ml_genre_preference_features gp
            ON LOWER(c.seed_value) = LOWER(gp.primary_genre)
        LEFT JOIN analytics.ml_artist_preference_features ap
            ON c.primary_artist_id = ap.primary_artist_id
    """

    def __init__(self, connection_params: Optional[Dict[str, str]] = None):
        if connection_params is None:
            snowflake_cfg = settings.snowflake
            connection_params = {
                "account": os.getenv("SNOWFLAKE_ACCOUNT") or snowflake_cfg.account,
                "user": os.getenv("SNOWFLAKE_USER") or snowflake_cfg.user,
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE") or snowflake_cfg.warehouse,
                "database": os.getenv("SNOWFLAKE_DATABASE") or snowflake_cfg.database,
                "schema": os.getenv("SNOWFLAKE_SCHEMA") or snowflake_cfg.schema,
                "role": os.getenv("SNOWFLAKE_ROLE") or snowflake_cfg.role,
            }

            private_key = os.getenv("SNOWFLAKE_PRIVATE_KEY") or snowflake_cfg.private_key
            private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or snowflake_cfg.private_key_path
            private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or snowflake_cfg.private_key_passphrase
            password = os.getenv("SNOWFLAKE_PASSWORD") or snowflake_cfg.password

            if private_key and private_key_path:
                raise ValueError("Provide either SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PRIVATE_KEY_PATH, not both")

            if private_key:
                key_bytes = private_key.encode("utf-8")
                connection_params["authenticator"] = "SNOWFLAKE_JWT"
                connection_params["private_key"] = serialization.load_pem_private_key(
                    key_bytes,
                    password=private_key_passphrase.encode("utf-8") if private_key_passphrase else None,
                )
            elif private_key_path:
                with open(private_key_path, "rb") as key_file:
                    key_bytes = key_file.read()
                connection_params["authenticator"] = "SNOWFLAKE_JWT"
                connection_params["private_key"] = serialization.load_pem_private_key(
                    key_bytes,
                    password=private_key_passphrase.encode("utf-8") if private_key_passphrase else None,
                )
            else:
                connection_params["password"] = password

        missing = [k for k in ["account", "user", "warehouse", "database", "schema"] if not connection_params.get(k)]
        if missing:
            raise ValueError(f"Missing Snowflake connection parameters: {missing}")

        self.connection_params = connection_params

    def _connect(self):
        return snowflake.connector.connect(**self.connection_params)

    def top_genres(self, min_plays: int = 2, limit: int = 10) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql(self.GENRE_SQL, conn, params={"min_plays": min_plays, "limit": limit})

    def top_artists(self, min_plays: int = 2, limit: int = 10) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql(self.ARTIST_SQL, conn, params={"min_plays": min_plays, "limit": limit})

    def listened_track_ids(self) -> List[str]:
        with self._connect() as conn:
            df = pd.read_sql(self.TRACK_HISTORY_SQL, conn)
        return df["TRACK_ID"].tolist() if not df.empty else []

    def candidate_tracks(self) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql(self.CANDIDATE_SQL, conn)


class SpotifyRecommender:
    """Ranks pre-fetched candidate tracks using simple heuristics."""

    def __init__(self, feature_store: SnowflakeFeatureStore):
        self.feature_store = feature_store

    def recommend(self, total_candidates: int = 30) -> List[RecommendationCandidate]:
        listened = set(self.feature_store.listened_track_ids())
        candidates_df = self.feature_store.candidate_tracks()

        if candidates_df.empty:
            print("No candidate tracks found in analytics.ml_candidate_tracks")
            return []

        candidates_df = candidates_df.rename(columns=str.upper)
        candidates_df = candidates_df[~candidates_df["TRACK_ID"].isin(listened)]

        if candidates_df.empty:
            print("All candidates already exist in listening history")
            return []

        candidates_df["GENRE_PLAY_COUNT"] = candidates_df["GENRE_PLAY_COUNT"].fillna(0)
        candidates_df["ARTIST_PLAY_COUNT"] = candidates_df["ARTIST_PLAY_COUNT"].fillna(0)
        candidates_df["TRACK_POPULARITY"] = candidates_df["TRACK_POPULARITY"].fillna(0)

        candidates_df["genre_rank"] = candidates_df["GENRE_PLAY_COUNT"].rank(method="average", pct=True)
        candidates_df["artist_rank"] = candidates_df["ARTIST_PLAY_COUNT"].rank(method="average", pct=True)
        candidates_df["popularity_norm"] = candidates_df["TRACK_POPULARITY"] / 100.0

        candidates_df["score"] = (
            0.5 * candidates_df["genre_rank"]
            + 0.3 * candidates_df["artist_rank"]
            + 0.2 * candidates_df["popularity_norm"]
        )

        top_df = candidates_df.sort_values("score", ascending=False).head(total_candidates)

        results: List[RecommendationCandidate] = []
        for _, row in top_df.iterrows():
            results.append(
                RecommendationCandidate(
                    track_id=row["TRACK_ID"],
                    track_name=row["TRACK_NAME"],
                    artist_id=row["PRIMARY_ARTIST_ID"],
                    artist_name=row["PRIMARY_ARTIST_NAME"],
                    source=row.get("SOURCE", "candidate_pool"),
                    reason=f"Seed {row.get('SEED_VALUE', 'unknown')} | score {row['score']:.2f}",
                    score=float(row["score"]),
                )
            )

        return results


def main(total_candidates: int = 30):
    feature_store = SnowflakeFeatureStore()
    recommender = SpotifyRecommender(feature_store)
    candidates = recommender.recommend(total_candidates)

    print("🎯 Recommendation Candidates")
    for idx, candidate in enumerate(candidates, 1):
        print(
            f"{idx:02d}. {candidate.track_name} — {candidate.artist_name}"
            f" | source={candidate.source} | score={candidate.score:.2f}"
        )


if __name__ == "__main__":
    main()

