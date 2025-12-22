"""
Spotify ML Recommendation Models for Snowflake Model Registry
Advanced recommendation system using collaborative filtering, content-based, and temporal ML
"""

import snowflake.snowpark as snowpark
from snowflake.ml.registry import ModelRegistry
from snowflake.ml.modeling.preprocessing import StandardScaler
from sklearn.decomposition import NMF, TruncatedSVD
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import structlog
import json

logger = structlog.get_logger(__name__)


class SpotifyCollaborativeFilter:
    """
    Collaborative Filtering model for genre-based recommendations.
    Uses Matrix Factorization (NMF) to find latent genre relationships.
    """
    
    def __init__(self, session: snowpark.Session):
        self.session = session
        self.model_registry = ModelRegistry(session)
        self.model = None
        self.user_features = None
        self.genre_features = None
        self.genre_names = None
        
    def prepare_training_data(self) -> pd.DataFrame:
        """Extract user-genre interaction matrix from Snowflake data."""
        
        logger.info("Preparing collaborative filtering training data")
        
        # Create user-genre interaction matrix with rich features
        query = """
        WITH user_genre_interactions AS (
            SELECT 
                'user_1' AS user_id,  -- Single user for now, expandable for multi-user
                primary_genre,
                COUNT(*) AS play_count,
                AVG(track_popularity) AS avg_popularity,
                SUM(CASE WHEN denver_date >= DATEADD('days', -30, CURRENT_DATE) 
                         THEN 1 ELSE 0 END) AS recent_plays,
                SUM(CASE WHEN denver_date >= DATEADD('days', -7, CURRENT_DATE) 
                         THEN 1 ELSE 0 END) AS very_recent_plays,
                
                -- Recency weighting using exponential decay
                EXP(-AVG(DATEDIFF('days', denver_date, CURRENT_DATE)) / 30.0) AS recency_weight,
                
                -- Temporal preferences
                AVG(denver_hour) AS avg_listening_hour,
                AVG(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS weekend_preference,
                
                -- Artist diversity in genre
                COUNT(DISTINCT primary_artist_id) AS artist_diversity_in_genre,
                
                -- Track characteristics
                AVG(track_duration_ms) / 1000.0 / 60.0 AS avg_duration_minutes,
                STDDEV(track_popularity) AS popularity_variance
                
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE denver_date >= DATEADD('days', -180, CURRENT_DATE)  -- 6 months of data
            GROUP BY user_id, primary_genre
            HAVING play_count >= 3  -- Minimum plays for reliability
        ),
        genre_features AS (
            SELECT 
                primary_genre,
                COUNT(DISTINCT primary_artist_id) AS total_artists_in_genre,
                AVG(track_popularity) AS genre_avg_popularity,
                STDDEV(track_popularity) AS genre_popularity_variance,
                COUNT(DISTINCT YEAR(album_release_date)) AS era_diversity,
                AVG(track_duration_ms) / 1000.0 / 60.0 AS genre_avg_duration
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE denver_date >= DATEADD('days', -180, CURRENT_DATE)
            GROUP BY primary_genre
        )
        SELECT 
            ui.*,
            gf.total_artists_in_genre,
            gf.genre_avg_popularity,
            gf.genre_popularity_variance,
            gf.era_diversity,
            gf.genre_avg_duration,
            
            -- Engineered preference features
            ui.play_count * ui.recency_weight AS weighted_preference,
            CASE WHEN ui.recent_plays > 0 THEN 1 ELSE 0 END AS is_current_preference,
            CASE WHEN ui.very_recent_plays > 0 THEN 1 ELSE 0 END AS is_very_recent_preference,
            LN(ui.play_count + 1) AS log_play_count,
            
            -- Preference strength categorization
            CASE 
                WHEN ui.play_count >= 50 THEN 'high'
                WHEN ui.play_count >= 20 THEN 'medium'
                WHEN ui.play_count >= 10 THEN 'low'
                ELSE 'minimal'
            END AS preference_strength
            
        FROM user_genre_interactions ui
        JOIN genre_features gf ON ui.primary_genre = gf.primary_genre
        ORDER BY weighted_preference DESC
        """
        
        df = self.session.sql(query).to_pandas()
        logger.info(f"Prepared training data with {len(df)} genre interactions")
        
        return df
    
    def train_collaborative_model(self, n_components: int = 20, max_iter: int = 500):
        """
        Train Matrix Factorization model using Non-negative Matrix Factorization.
        
        Args:
            n_components: Number of latent factors
            max_iter: Maximum training iterations
        """
        
        logger.info(f"Training collaborative filtering model with {n_components} components")
        
        # Get training data
        df = self.prepare_training_data()
        
        if df.empty:
            raise ValueError("No training data available. Need more listening history.")
        
        # Create user-item matrix (user-genre in this case)
        user_genre_matrix = df.pivot_table(
            index='user_id',
            columns='primary_genre', 
            values='weighted_preference',
            fill_value=0
        )
        
        logger.info(f"Created user-genre matrix: {user_genre_matrix.shape}")
        
        # Train NMF (Non-negative Matrix Factorization)
        self.model = NMF(
            n_components=n_components,
            init='nndsvd',  # Non-negative double SVD initialization
            max_iter=max_iter,
            random_state=42,
            alpha_W=0.1,    # L1 regularization for user features
            alpha_H=0.1,    # L1 regularization for genre features
            l1_ratio=0.5    # Balance between L1 and L2 regularization
        )
        
        # Fit the model
        self.user_features = self.model.fit_transform(user_genre_matrix.values)
        self.genre_features = self.model.components_
        self.genre_names = user_genre_matrix.columns.tolist()
        
        # Calculate reconstruction error
        reconstructed = np.dot(self.user_features, self.genre_features)
        reconstruction_error = np.mean((user_genre_matrix.values - reconstructed) ** 2)
        
        logger.info(f"Model trained successfully. Reconstruction error: {reconstruction_error:.4f}")
        
        return self
    
    def get_genre_recommendations(self, num_recommendations: int = 10) -> List[Dict]:
        """Generate genre recommendations using the trained model."""
        
        if self.model is None:
            raise ValueError("Model not trained. Call train_collaborative_model() first.")
        
        # Get user preferences from latent factors
        user_vector = self.user_features[0]  # First (and only) user
        
        # Calculate genre scores by matrix multiplication
        genre_scores = np.dot(user_vector, self.genre_features)
        
        # Get top genres
        genre_indices = np.argsort(genre_scores)[::-1][:num_recommendations]
        
        recommendations = []
        for idx in genre_indices:
            genre_name = self.genre_names[idx]
            score = genre_scores[idx]
            
            recommendations.append({
                'genre': genre_name,
                'score': float(score),
                'recommendation_type': 'collaborative_filtering'
            })
        
        return recommendations
    
    def register_model(self, model_name: str = "spotify_collaborative_filter", version: str = "1.0"):
        """Register model in Snowflake Model Registry."""
        
        if self.model is None:
            raise ValueError("Model not trained. Cannot register untrained model.")
        
        # Create comprehensive model metadata
        model_metadata = {
            "model_type": "collaborative_filtering",
            "algorithm": "non_negative_matrix_factorization",
            "n_components": self.model.n_components,
            "training_data_days": 180,
            "min_genre_plays": 3,
            "regularization": {
                "alpha_W": 0.1,
                "alpha_H": 0.1,
                "l1_ratio": 0.5
            },
            "feature_engineering": [
                "recency_weighting",
                "log_transformation", 
                "temporal_preferences",
                "artist_diversity"
            ],
            "genre_count": len(self.genre_names),
            "training_timestamp": str(pd.Timestamp.now())
        }
        
        # Package model with supporting data
        model_package = {
            'nmf_model': self.model,
            'user_features': self.user_features,
            'genre_features': self.genre_features,
            'genre_names': self.genre_names,
            'metadata': model_metadata
        }
        
        # Register in Snowflake Model Registry
        model_ref = self.model_registry.log_model(
            model=model_package,
            model_name=model_name,
            version=version,
            metadata=model_metadata,
            comment=f"Collaborative filtering for Spotify genre recommendations using NMF with {len(self.genre_names)} genres"
        )
        
        logger.info(f"Model registered successfully: {model_ref}")
        return model_ref


class SpotifyContentBasedModel:
    """
    Content-based filtering using track and artist features.
    Uses cosine similarity on engineered content features.
    """
    
    def __init__(self, session: snowpark.Session):
        self.session = session
        self.model_registry = ModelRegistry(session)
        self.similarity_model = None
        self.track_features_df = None
        
    def prepare_content_features(self) -> pd.DataFrame:
        """Extract and engineer content features for tracks."""
        
        logger.info("Preparing content-based features")
        
        query = """
        WITH track_features AS (
            SELECT 
                track_id,
                track_name,
                primary_artist_id,
                primary_artist_name,
                primary_genre,
                track_popularity,
                track_duration_ms,
                album_release_date,
                album_name,
                album_type,
                
                -- Engineered temporal features
                YEAR(album_release_date) AS release_year,
                CASE 
                    WHEN YEAR(album_release_date) >= 2020 THEN 'current'
                    WHEN YEAR(album_release_date) >= 2015 THEN 'recent'
                    WHEN YEAR(album_release_date) >= 2010 THEN '2010s'
                    WHEN YEAR(album_release_date) >= 2000 THEN '2000s'
                    WHEN YEAR(album_release_date) >= 1990 THEN '90s'
                    ELSE 'classic'
                END AS era_category,
                
                -- Popularity tiers
                CASE 
                    WHEN track_popularity >= 80 THEN 4  -- Mainstream
                    WHEN track_popularity >= 60 THEN 3  -- Popular
                    WHEN track_popularity >= 40 THEN 2  -- Moderate
                    WHEN track_popularity >= 20 THEN 1  -- Niche
                    ELSE 0  -- Underground
                END AS popularity_tier,
                
                -- Duration categories
                CASE
                    WHEN track_duration_ms < 120000 THEN 0  -- Very short (< 2min)
                    WHEN track_duration_ms < 180000 THEN 1  -- Short (2-3min)
                    WHEN track_duration_ms < 240000 THEN 2  -- Normal (3-4min)
                    WHEN track_duration_ms < 300000 THEN 3  -- Long (4-5min)
                    WHEN track_duration_ms < 420000 THEN 4  -- Very long (5-7min)
                    ELSE 5  -- Extended (>7min)
                END AS duration_category,
                
                -- User engagement with this track
                COUNT(*) OVER (PARTITION BY track_id) AS user_play_count,
                MAX(denver_date) OVER (PARTITION BY track_id) AS last_played_date,
                
                -- Genre rank for this track
                ROW_NUMBER() OVER (
                    PARTITION BY primary_genre 
                    ORDER BY track_popularity DESC, COUNT(*) OVER (PARTITION BY track_id) DESC
                ) AS genre_rank
                
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE denver_date >= DATEADD('days', -90, CURRENT_DATE)
        ),
        artist_features AS (
            SELECT 
                primary_artist_id,
                primary_artist_name,
                COUNT(DISTINCT primary_genre) AS artist_genre_diversity,
                AVG(track_popularity) AS artist_avg_popularity,
                COUNT(DISTINCT track_id) AS artist_track_count,
                AVG(track_duration_ms) AS artist_avg_duration,
                COUNT(*) AS artist_total_plays
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE denver_date >= DATEADD('days', -90, CURRENT_DATE)
            GROUP BY primary_artist_id, primary_artist_name
        ),
        genre_embeddings AS (
            SELECT 
                primary_genre,
                AVG(track_popularity) AS genre_avg_popularity,
                AVG(track_duration_ms) AS genre_avg_duration,
                COUNT(DISTINCT primary_artist_id) AS genre_artist_count,
                COUNT(DISTINCT track_id) AS genre_track_count,
                STDDEV(track_popularity) AS genre_popularity_variance,
                AVG(YEAR(album_release_date)) AS genre_avg_release_year
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE denver_date >= DATEADD('days', -90, CURRENT_DATE)
            GROUP BY primary_genre
        )
        SELECT 
            tf.*,
            af.artist_genre_diversity,
            af.artist_avg_popularity,
            af.artist_track_count,
            af.artist_avg_duration,
            af.artist_total_plays,
            ge.genre_avg_popularity,
            ge.genre_avg_duration,
            ge.genre_artist_count,
            ge.genre_track_count,
            ge.genre_popularity_variance,
            ge.genre_avg_release_year,
            
            -- Composite features
            tf.track_popularity - ge.genre_avg_popularity AS popularity_vs_genre_avg,
            tf.track_duration_ms - ge.genre_avg_duration AS duration_vs_genre_avg,
            af.artist_avg_popularity - ge.genre_avg_popularity AS artist_vs_genre_popularity,
            
            -- Freshness score
            DATEDIFF('days', tf.last_played_date, CURRENT_DATE) AS days_since_last_played,
            EXP(-DATEDIFF('days', tf.last_played_date, CURRENT_DATE) / 30.0) AS freshness_score
            
        FROM track_features tf
        JOIN artist_features af ON tf.primary_artist_id = af.primary_artist_id
        JOIN genre_embeddings ge ON tf.primary_genre = ge.primary_genre
        WHERE tf.genre_rank <= 50  -- Top 50 tracks per genre to keep dataset manageable
        """
        
        df = self.session.sql(query).to_pandas()
        logger.info(f"Prepared content features for {len(df)} tracks")
        
        self.track_features_df = df
        return df
    
    def train_content_model(self):
        """Train content-based similarity model using cosine similarity."""
        
        logger.info("Training content-based similarity model")
        
        if self.track_features_df is None:
            df = self.prepare_content_features()
        else:
            df = self.track_features_df
        
        if df.empty:
            raise ValueError("No content features available for training")
        
        # Select numerical features for similarity calculation
        feature_columns = [
            'track_popularity', 'popularity_tier', 'duration_category', 'release_year',
            'artist_genre_diversity', 'artist_avg_popularity', 'artist_track_count',
            'genre_avg_popularity', 'genre_avg_duration', 'genre_artist_count',
            'genre_popularity_variance', 'user_play_count', 'freshness_score',
            'popularity_vs_genre_avg', 'duration_vs_genre_avg', 'artist_vs_genre_popularity'
        ]
        
        # Handle missing values
        features_df = df[feature_columns].fillna(0)
        
        # Standardize features
        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features_df)
        
        # Calculate cosine similarity matrix
        similarity_matrix = cosine_similarity(features_scaled)
        
        logger.info(f"Computed similarity matrix: {similarity_matrix.shape}")
        
        # Store model components
        self.similarity_model = {
            'similarity_matrix': similarity_matrix,
            'track_ids': df['track_id'].tolist(),
            'track_names': df['track_name'].tolist(),
            'artist_names': df['primary_artist_name'].tolist(),
            'genres': df['primary_genre'].tolist(),
            'scaler': scaler,
            'feature_columns': feature_columns,
            'track_features_df': df[['track_id', 'track_name', 'primary_artist_name', 'primary_genre'] + feature_columns]
        }
        
        return self
    
    def get_similar_tracks(self, track_id: str, num_recommendations: int = 10) -> List[Dict]:
        """Get tracks similar to the given track."""
        
        if self.similarity_model is None:
            raise ValueError("Model not trained. Call train_content_model() first.")
        
        try:
            # Find track index
            track_idx = self.similarity_model['track_ids'].index(track_id)
        except ValueError:
            logger.warning(f"Track {track_id} not found in trained model")
            return []
        
        # Get similarity scores for this track
        similarity_scores = self.similarity_model['similarity_matrix'][track_idx]
        
        # Get top similar tracks (excluding the track itself)
        similar_indices = np.argsort(similarity_scores)[::-1][1:num_recommendations+1]
        
        recommendations = []
        for idx in similar_indices:
            recommendations.append({
                'track_id': self.similarity_model['track_ids'][idx],
                'track_name': self.similarity_model['track_names'][idx],
                'artist_name': self.similarity_model['artist_names'][idx],
                'genre': self.similarity_model['genres'][idx],
                'similarity_score': float(similarity_scores[idx]),
                'recommendation_type': 'content_based'
            })
        
        return recommendations
    
    def register_model(self, model_name: str = "spotify_content_based", version: str = "1.0"):
        """Register content-based model in Snowflake Model Registry."""
        
        if self.similarity_model is None:
            raise ValueError("Model not trained. Cannot register untrained model.")
        
        model_metadata = {
            "model_type": "content_based_filtering",
            "algorithm": "cosine_similarity",
            "features": self.similarity_model['feature_columns'],
            "similarity_computation": "cosine",
            "standardization": "standard_scaler",
            "training_data_days": 90,
            "track_count": len(self.similarity_model['track_ids']),
            "feature_count": len(self.similarity_model['feature_columns']),
            "training_timestamp": str(pd.Timestamp.now())
        }
        
        model_ref = self.model_registry.log_model(
            model=self.similarity_model,
            model_name=model_name,
            version=version,
            metadata=model_metadata,
            comment=f"Content-based filtering for Spotify track recommendations using {len(self.similarity_model['feature_columns'])} features"
        )
        
        logger.info(f"Content-based model registered: {model_ref}")
        return model_ref


class SpotifyHybridRecommender:
    """
    Hybrid recommendation system combining collaborative filtering, 
    content-based filtering, and temporal patterns.
    """
    
    def __init__(self, session: snowpark.Session):
        self.session = session
        self.model_registry = ModelRegistry(session)
        self.collaborative_model = None
        self.content_model = None
        self.temporal_model = None
        self.hybrid_weights = {
            'collaborative': 0.4,
            'content_based': 0.3,
            'temporal': 0.2,
            'discovery': 0.1
        }
        
    def train_hybrid_ensemble(self):
        """Train all components of the hybrid recommendation system."""
        
        logger.info("Training hybrid recommendation ensemble")
        
        # Train collaborative filtering model
        logger.info("Training collaborative filtering component...")
        self.collaborative_model = SpotifyCollaborativeFilter(self.session)
        self.collaborative_model.train_collaborative_model()
        
        # Train content-based model
        logger.info("Training content-based filtering component...")
        self.content_model = SpotifyContentBasedModel(self.session)
        self.content_model.train_content_model()
        
        # Train temporal patterns model
        logger.info("Training temporal patterns component...")
        self.temporal_model = self._train_temporal_patterns()
        
        logger.info("Hybrid ensemble training completed")
        return self
        
    def _train_temporal_patterns(self) -> Dict:
        """Train temporal listening pattern model."""
        
        query = """
        WITH temporal_patterns AS (
            SELECT 
                primary_genre,
                denver_hour,
                is_weekend,
                COUNT(*) AS play_count,
                AVG(track_popularity) AS avg_popularity,
                COUNT(DISTINCT primary_artist_id) AS unique_artists,
                AVG(track_duration_ms) / 1000.0 / 60.0 AS avg_duration_minutes,
                
                -- Calculate probability of this genre at this time
                COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY denver_hour, is_weekend) AS hour_genre_probability,
                
                -- Calculate genre's temporal preference
                COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY primary_genre) AS genre_time_preference
                
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE denver_date >= DATEADD('days', -90, CURRENT_DATE)
            GROUP BY primary_genre, denver_hour, is_weekend
            HAVING COUNT(*) >= 2  -- Minimum plays for pattern reliability
        ),
        genre_temporal_summary AS (
            SELECT 
                primary_genre,
                AVG(denver_hour) AS avg_listening_hour,
                STDDEV(denver_hour) AS hour_variance,
                AVG(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS weekend_preference,
                COUNT(DISTINCT denver_hour) AS hour_diversity,
                SUM(play_count) AS total_genre_plays
            FROM temporal_patterns
            GROUP BY primary_genre
        )
        SELECT 
            tp.*,
            gts.avg_listening_hour,
            gts.hour_variance,
            gts.weekend_preference,
            gts.hour_diversity,
            gts.total_genre_plays
        FROM temporal_patterns tp
        JOIN genre_temporal_summary gts ON tp.primary_genre = gts.primary_genre
        ORDER BY tp.denver_hour, tp.is_weekend, tp.play_count DESC
        """
        
        temporal_df = self.session.sql(query).to_pandas()
        
        # Create temporal lookup model
        temporal_model = {
            'patterns': {},
            'genre_summaries': {},
            'training_data': temporal_df
        }
        
        # Build hour-based lookup
        for _, row in temporal_df.iterrows():
            hour_key = (int(row['denver_hour']), bool(row['is_weekend']))
            genre = row['primary_genre']
            
            if hour_key not in temporal_model['patterns']:
                temporal_model['patterns'][hour_key] = []
            
            temporal_model['patterns'][hour_key].append({
                'genre': genre,
                'probability': float(row['hour_genre_probability']),
                'avg_popularity': float(row['avg_popularity']),
                'play_count': int(row['play_count']),
                'unique_artists': int(row['unique_artists'])
            })
        
        # Build genre temporal summaries
        for _, row in temporal_df.iterrows():
            genre = row['primary_genre']
            if genre not in temporal_model['genre_summaries']:
                temporal_model['genre_summaries'][genre] = {
                    'avg_listening_hour': float(row['avg_listening_hour']),
                    'hour_variance': float(row['hour_variance']) if pd.notna(row['hour_variance']) else 0.0,
                    'weekend_preference': float(row['weekend_preference']),
                    'hour_diversity': int(row['hour_diversity']),
                    'total_plays': int(row['total_genre_plays'])
                }
        
        logger.info(f"Trained temporal model with {len(temporal_model['patterns'])} time patterns")
        return temporal_model
    
    def predict_recommendations(
        self, 
        num_recommendations: int = 30, 
        current_hour: Optional[int] = None, 
        is_weekend: Optional[bool] = None,
        user_track_history: Optional[List[str]] = None
    ) -> List[Dict]:
        """Generate hybrid recommendations combining all models."""
        
        logger.info(f"Generating {num_recommendations} hybrid recommendations")
        
        recommendations = []
        
        # 1. Collaborative Filtering recommendations (40% weight)
        try:
            collab_recs = self.collaborative_model.get_genre_recommendations(
                int(num_recommendations * self.hybrid_weights['collaborative'])
            )
            for rec in collab_recs:
                rec['strategy'] = 'collaborative'
                rec['base_score'] = rec.get('score', 0.5) * self.hybrid_weights['collaborative']
            recommendations.extend(collab_recs)
        except Exception as e:
            logger.warning(f"Collaborative filtering failed: {e}")
        
        # 2. Content-Based recommendations (30% weight)
        if user_track_history:
            try:
                content_recs = []
                for track_id in user_track_history[-5:]:  # Use last 5 tracks
                    similar_tracks = self.content_model.get_similar_tracks(
                        track_id, num_recommendations=5
                    )
                    content_recs.extend(similar_tracks)
                
                # Remove duplicates and limit
                seen_tracks = set()
                unique_content_recs = []
                for rec in content_recs:
                    if rec['track_id'] not in seen_tracks:
                        seen_tracks.add(rec['track_id'])
                        rec['strategy'] = 'content_based'
                        rec['base_score'] = rec.get('similarity_score', 0.5) * self.hybrid_weights['content_based']
                        unique_content_recs.append(rec)
                
                recommendations.extend(unique_content_recs[:int(num_recommendations * self.hybrid_weights['content_based'])])
            except Exception as e:
                logger.warning(f"Content-based filtering failed: {e}")
        
        # 3. Temporal recommendations (20% weight)
        if current_hour is not None and is_weekend is not None:
            try:
                temporal_recs = self._get_temporal_recommendations(
                    int(num_recommendations * self.hybrid_weights['temporal']), 
                    current_hour, 
                    is_weekend
                )
                for rec in temporal_recs:
                    rec['strategy'] = 'temporal'
                    rec['base_score'] = rec.get('score', 0.5) * self.hybrid_weights['temporal']
                recommendations.extend(temporal_recs)
            except Exception as e:
                logger.warning(f"Temporal filtering failed: {e}")
        
        # 4. Discovery recommendations (10% weight)
        try:
            discovery_recs = self._get_discovery_recommendations(
                int(num_recommendations * self.hybrid_weights['discovery'])
            )
            for rec in discovery_recs:
                rec['strategy'] = 'discovery'
                rec['base_score'] = rec.get('score', 0.3) * self.hybrid_weights['discovery']
            recommendations.extend(discovery_recs)
        except Exception as e:
            logger.warning(f"Discovery recommendations failed: {e}")
        
        # Combine and rank all recommendations
        final_recommendations = self._rank_and_combine_recommendations(recommendations, num_recommendations)
        
        logger.info(f"Generated {len(final_recommendations)} final recommendations")
        return final_recommendations
    
    def _get_temporal_recommendations(self, num_recs: int, current_hour: int, is_weekend: bool) -> List[Dict]:
        """Get recommendations based on temporal patterns."""
        
        hour_key = (current_hour, is_weekend)
        
        if hour_key not in self.temporal_model['patterns']:
            # Fallback to similar hours
            similar_hours = [
                (h, is_weekend) for h in range(max(0, current_hour-2), min(24, current_hour+3))
                if (h, is_weekend) in self.temporal_model['patterns']
            ]
            if similar_hours:
                hour_key = similar_hours[0]
            else:
                return []
        
        temporal_genres = self.temporal_model['patterns'][hour_key]
        
        # Get tracks from top temporal genres
        recommendations = []
        for genre_info in temporal_genres[:num_recs]:
            
            # Query tracks from this genre
            track_query = f"""
            SELECT 
                track_id,
                track_name,
                primary_artist_name,
                primary_genre,
                track_popularity,
                album_name
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE primary_genre = '{genre_info['genre']}'
            AND denver_date >= DATEADD('days', -30, CURRENT_DATE)
            ORDER BY track_popularity DESC, RANDOM()
            LIMIT 1
            """
            
            try:
                track_df = self.session.sql(track_query).to_pandas()
                if not track_df.empty:
                    track = track_df.iloc[0]
                    recommendations.append({
                        'track_id': track['TRACK_ID'],
                        'track_name': track['TRACK_NAME'],
                        'artist_name': track['PRIMARY_ARTIST_NAME'],
                        'genre': track['PRIMARY_GENRE'],
                        'album_name': track['ALBUM_NAME'],
                        'popularity': int(track['TRACK_POPULARITY']),
                        'score': genre_info['probability'],
                        'temporal_relevance': genre_info['probability']
                    })
            except Exception as e:
                logger.warning(f"Failed to get tracks for genre {genre_info['genre']}: {e}")
                continue
        
        return recommendations[:num_recs]
    
    def _get_discovery_recommendations(self, num_recs: int) -> List[Dict]:
        """Get discovery recommendations (explore new genres/artists)."""
        
        discovery_query = f"""
        WITH user_genres AS (
            SELECT DISTINCT primary_genre
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE denver_date >= DATEADD('days', -90, CURRENT_DATE)
        ),
        discovery_tracks AS (
            SELECT 
                track_id,
                track_name,
                primary_artist_name,
                primary_genre,
                track_popularity,
                album_name,
                ROW_NUMBER() OVER (PARTITION BY primary_genre ORDER BY track_popularity DESC, RANDOM()) AS genre_rank
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE primary_genre NOT IN (SELECT primary_genre FROM user_genres)
            AND denver_date >= DATEADD('days', -60, CURRENT_DATE)
            AND track_popularity BETWEEN 30 AND 70  -- Hidden gems range
        )
        SELECT *
        FROM discovery_tracks
        WHERE genre_rank <= 2  -- Top 2 tracks per new genre
        ORDER BY RANDOM()
        LIMIT {num_recs}
        """
        
        try:
            discovery_df = self.session.sql(discovery_query).to_pandas()
            
            recommendations = []
            for _, track in discovery_df.iterrows():
                recommendations.append({
                    'track_id': track['TRACK_ID'],
                    'track_name': track['TRACK_NAME'],
                    'artist_name': track['PRIMARY_ARTIST_NAME'],
                    'genre': track['PRIMARY_GENRE'],
                    'album_name': track['ALBUM_NAME'],
                    'popularity': int(track['TRACK_POPULARITY']),
                    'score': 0.3,  # Discovery bonus
                    'discovery_reason': 'new_genre_exploration'
                })
            
            return recommendations
            
        except Exception as e:
            logger.warning(f"Discovery recommendations failed: {e}")
            return []
    
    def _rank_and_combine_recommendations(self, recommendations: List[Dict], num_final: int) -> List[Dict]:
        """Rank and combine recommendations from different strategies."""
        
        # Remove duplicates by track_id if present
        unique_recs = {}
        for rec in recommendations:
            track_id = rec.get('track_id', f"{rec.get('genre', 'unknown')}_{len(unique_recs)}")
            
            if track_id not in unique_recs:
                unique_recs[track_id] = rec
            else:
                # Combine scores for duplicate tracks
                existing = unique_recs[track_id]
                existing['base_score'] = max(existing.get('base_score', 0), rec.get('base_score', 0))
                existing['strategy'] = f"{existing.get('strategy', '')},{rec.get('strategy', '')}"
        
        # Sort by combined score
        sorted_recs = sorted(
            unique_recs.values(),
            key=lambda x: x.get('base_score', 0),
            reverse=True
        )
        
        # Add final ranking
        final_recs = []
        for i, rec in enumerate(sorted_recs[:num_final]):
            rec['final_rank'] = i + 1
            rec['final_score'] = rec.get('base_score', 0)
            final_recs.append(rec)
        
        return final_recs
    
    def register_ensemble_model(self, model_name: str = "spotify_hybrid_recommender", version: str = "1.0"):
        """Register the complete hybrid ensemble model."""
        
        ensemble_model = {
            'collaborative_model': self.collaborative_model.model if self.collaborative_model else None,
            'content_model': self.content_model.similarity_model if self.content_model else None,
            'temporal_model': self.temporal_model,
            'hybrid_weights': self.hybrid_weights,
            'model_versions': {
                'collaborative': version,
                'content_based': version,
                'temporal': version
            }
        }
        
        model_metadata = {
            "model_type": "hybrid_recommender",
            "ensemble_strategy": "weighted_combination",
            "sub_models": ["collaborative_filtering", "content_based", "temporal_patterns", "discovery"],
            "weights": self.hybrid_weights,
            "training_data_days": 180,
            "retraining_frequency": "weekly",
            "supports_real_time": True,
            "training_timestamp": str(pd.Timestamp.now())
        }
        
        model_ref = self.model_registry.log_model(
            model=ensemble_model,
            model_name=model_name,
            version=version,
            metadata=model_metadata,
            comment=f"Hybrid recommendation system combining collaborative filtering, content-based filtering, temporal patterns, and discovery"
        )
        
        logger.info(f"Hybrid ensemble model registered: {model_ref}")
        return model_ref


# Utility functions for model inference
def load_model_from_registry(session: snowpark.Session, model_name: str, version: str = "latest"):
    """Load a model from Snowflake Model Registry."""
    
    model_registry = ModelRegistry(session)
    model_ref = model_registry.get_model(model_name, version)
    return model_ref.load()


def generate_recommendations_from_registry(
    session: snowpark.Session, 
    model_name: str = "spotify_hybrid_recommender",
    num_recommendations: int = 30,
    current_hour: Optional[int] = None,
    is_weekend: Optional[bool] = None
) -> List[Dict]:
    """Generate recommendations using a registered model."""
    
    try:
        # Load model from registry
        model = load_model_from_registry(session, model_name)
        
        # Create recommender instance and load the trained model
        recommender = SpotifyHybridRecommender(session)
        recommender.collaborative_model = type('MockModel', (), {
            'model': model.get('collaborative_model'),
            'get_genre_recommendations': lambda self, n: []
        })()
        recommender.content_model = type('MockModel', (), {
            'similarity_model': model.get('content_model'),
            'get_similar_tracks': lambda self, track_id, num_recommendations: []
        })()
        recommender.temporal_model = model.get('temporal_model')
        recommender.hybrid_weights = model.get('hybrid_weights', recommender.hybrid_weights)
        
        # Generate recommendations
        return recommender.predict_recommendations(
            num_recommendations=num_recommendations,
            current_hour=current_hour,
            is_weekend=is_weekend
        )
        
    except Exception as e:
        logger.error(f"Failed to generate recommendations from registry: {e}")
        return []
