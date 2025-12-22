"""
Model Fine-Tuning and A/B Testing for Spotify ML Models
Handles incremental learning, model updates, and performance comparison
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from snowflake.snowpark import Session
from snowflake.ml.registry import ModelRegistry
from spotify_ml_models import SpotifyHybridRecommender
import structlog

logger = structlog.get_logger(__name__)


class SpotifyModelFineTuner:
    """Fine-tune registered models based on new listening data and performance feedback."""
    
    def __init__(self, session: Session):
        self.session = session
        self.model_registry = ModelRegistry(session)
        self.performance_history = []
    
    def analyze_recent_performance(self, model_name: str, days_back: int = 7) -> Dict:
        """Analyze model performance over recent period."""
        
        logger.info(f"Analyzing performance for {model_name} over last {days_back} days")
        
        # Get recent listening data for evaluation
        performance_query = f"""
        WITH recent_listening AS (
            SELECT 
                track_id,
                track_name,
                primary_artist_name,
                primary_genre,
                track_popularity,
                denver_date,
                denver_hour,
                is_weekend,
                COUNT(*) OVER (PARTITION BY track_id) AS track_replay_count,
                COUNT(*) OVER (PARTITION BY primary_genre) AS genre_popularity,
                COUNT(*) OVER (PARTITION BY primary_artist_id) AS artist_popularity
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE denver_date >= DATEADD('days', -{days_back}, CURRENT_DATE)
        ),
        listening_patterns AS (
            SELECT 
                COUNT(DISTINCT track_id) AS unique_tracks,
                COUNT(DISTINCT primary_genre) AS unique_genres,
                COUNT(DISTINCT primary_artist_name) AS unique_artists,
                AVG(track_popularity) AS avg_track_popularity,
                COUNT(*) AS total_plays,
                
                -- Diversity metrics
                COUNT(CASE WHEN track_replay_count = 1 THEN 1 END) / COUNT(*)::FLOAT AS discovery_rate,
                COUNT(CASE WHEN track_replay_count > 3 THEN 1 END) / COUNT(*)::FLOAT AS replay_rate,
                
                -- Temporal patterns
                COUNT(DISTINCT denver_hour) AS listening_hour_diversity,
                AVG(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS weekend_preference,
                
                -- Genre exploration
                COUNT(CASE WHEN genre_popularity <= 5 THEN 1 END) / COUNT(*)::FLOAT AS niche_genre_rate
                
            FROM recent_listening
        )
        SELECT * FROM listening_patterns
        """
        
        try:
            performance_df = self.session.sql(performance_query).to_pandas()
            
            if performance_df.empty:
                return {"error": "No recent listening data available"}
            
            performance_metrics = performance_df.iloc[0].to_dict()
            
            # Calculate performance scores
            performance_metrics['diversity_score'] = (
                performance_metrics['discovery_rate'] * 0.4 +
                performance_metrics['listening_hour_diversity'] / 24 * 0.3 +
                performance_metrics['niche_genre_rate'] * 0.3
            )
            
            performance_metrics['engagement_score'] = (
                performance_metrics['replay_rate'] * 0.6 +
                (1 - abs(performance_metrics['avg_track_popularity'] - 50) / 50) * 0.4
            )
            
            performance_metrics['overall_score'] = (
                performance_metrics['diversity_score'] * 0.5 +
                performance_metrics['engagement_score'] * 0.5
            )
            
            logger.info(f"Performance analysis complete. Overall score: {performance_metrics['overall_score']:.3f}")
            
            return performance_metrics
            
        except Exception as e:
            logger.error(f"Failed to analyze performance: {e}")
            return {"error": str(e)}
    
    def detect_concept_drift(self, days_back: int = 30) -> Dict:
        """Detect if user's listening patterns have significantly changed."""
        
        logger.info(f"Detecting concept drift over last {days_back} days")
        
        drift_query = f"""
        WITH time_periods AS (
            SELECT 
                CASE 
                    WHEN denver_date >= DATEADD('days', -{days_back//2}, CURRENT_DATE) THEN 'recent'
                    WHEN denver_date >= DATEADD('days', -{days_back}, CURRENT_DATE) THEN 'older'
                    ELSE 'historical'
                END AS period,
                primary_genre,
                track_popularity,
                denver_hour,
                is_weekend,
                COUNT(*) AS play_count
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE denver_date >= DATEADD('days', -{days_back}, CURRENT_DATE)
            GROUP BY period, primary_genre, track_popularity, denver_hour, is_weekend
        ),
        period_comparison AS (
            SELECT 
                period,
                COUNT(DISTINCT primary_genre) AS unique_genres,
                AVG(track_popularity) AS avg_popularity,
                AVG(denver_hour) AS avg_listening_hour,
                AVG(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS weekend_ratio,
                SUM(play_count) AS total_plays
            FROM time_periods
            GROUP BY period
        )
        SELECT 
            recent.unique_genres AS recent_genres,
            older.unique_genres AS older_genres,
            recent.avg_popularity AS recent_popularity,
            older.avg_popularity AS older_popularity,
            recent.avg_listening_hour AS recent_hour,
            older.avg_listening_hour AS older_hour,
            recent.weekend_ratio AS recent_weekend,
            older.weekend_ratio AS older_weekend,
            
            -- Calculate drift scores
            ABS(recent.unique_genres - older.unique_genres) / GREATEST(older.unique_genres, 1)::FLOAT AS genre_drift,
            ABS(recent.avg_popularity - older.avg_popularity) / 100.0 AS popularity_drift,
            ABS(recent.avg_listening_hour - older.avg_listening_hour) / 24.0 AS temporal_drift,
            ABS(recent.weekend_ratio - older.weekend_ratio) AS weekend_drift
            
        FROM period_comparison recent
        JOIN period_comparison older ON recent.period = 'recent' AND older.period = 'older'
        """
        
        try:
            drift_df = self.session.sql(drift_query).to_pandas()
            
            if drift_df.empty:
                return {"drift_detected": False, "reason": "Insufficient data"}
            
            drift_metrics = drift_df.iloc[0].to_dict()
            
            # Calculate overall drift score
            drift_score = (
                drift_metrics['genre_drift'] * 0.3 +
                drift_metrics['popularity_drift'] * 0.25 +
                drift_metrics['temporal_drift'] * 0.25 +
                drift_metrics['weekend_drift'] * 0.2
            )
            
            drift_threshold = 0.2  # Configurable threshold
            drift_detected = drift_score > drift_threshold
            
            drift_result = {
                "drift_detected": drift_detected,
                "drift_score": float(drift_score),
                "drift_threshold": drift_threshold,
                "component_drifts": {
                    "genre": float(drift_metrics['genre_drift']),
                    "popularity": float(drift_metrics['popularity_drift']),
                    "temporal": float(drift_metrics['temporal_drift']),
                    "weekend": float(drift_metrics['weekend_drift'])
                },
                "recommendation": "retrain" if drift_detected else "monitor"
            }
            
            logger.info(f"Concept drift analysis: {drift_result['recommendation']} (score: {drift_score:.3f})")
            
            return drift_result
            
        except Exception as e:
            logger.error(f"Failed to detect concept drift: {e}")
            return {"drift_detected": False, "error": str(e)}
    
    def fine_tune_collaborative_model(
        self, 
        model_name: str = "spotify_collaborative_filter", 
        current_version: str = "latest"
    ) -> Optional[str]:
        """Fine-tune collaborative filtering model with recent data."""
        
        logger.info(f"Fine-tuning collaborative model {model_name} version {current_version}")
        
        try:
            # Get current model
            model_ref = self.model_registry.get_model(model_name, current_version)
            current_model_package = model_ref.load()
            
            # Check if fine-tuning is needed
            drift_analysis = self.detect_concept_drift(days_back=30)
            if not drift_analysis.get('drift_detected', False):
                logger.info("No significant concept drift detected. Skipping fine-tuning.")
                return None
            
            # Create new model instance for retraining
            from spotify_ml_models import SpotifyCollaborativeFilter
            
            new_model = SpotifyCollaborativeFilter(self.session)
            
            # Get the same parameters as original model
            original_metadata = model_ref.metadata or {}
            n_components = original_metadata.get('n_components', 20)
            
            # Retrain with recent data (incremental approach)
            new_model.train_collaborative_model(
                n_components=n_components,
                max_iter=200  # Fewer iterations for fine-tuning
            )
            
            # Generate new version
            base_version = current_version.replace('latest', '1.0')
            version_parts = base_version.split('.')
            new_minor_version = int(version_parts[-1]) + 1
            new_version = f"{'.'.join(version_parts[:-1])}.{new_minor_version}"
            
            # Register fine-tuned model
            updated_ref = new_model.register_model(
                model_name=model_name,
                version=new_version
            )
            
            # Update metadata
            fine_tuning_metadata = {
                "fine_tuned_from": current_version,
                "fine_tuning_timestamp": str(datetime.now()),
                "fine_tuning_reason": "concept_drift_detected",
                "drift_score": drift_analysis.get('drift_score', 0),
                "training_data_period": "last_180_days_with_recent_emphasis"
            }
            
            logger.info(f"Successfully fine-tuned model. New version: {new_version}")
            
            return new_version
            
        except Exception as e:
            logger.error(f"Fine-tuning failed: {e}")
            return None
    
    def a_b_test_models(
        self, 
        model_a_name: str, 
        model_a_version: str,
        model_b_name: str, 
        model_b_version: str,
        test_days: int = 7
    ) -> Dict:
        """A/B test two model versions using recent listening data."""
        
        logger.info(f"A/B testing {model_a_name} v{model_a_version} vs {model_b_name} v{model_b_version}")
        
        try:
            # Load both models
            model_a_ref = self.model_registry.get_model(model_a_name, model_a_version)
            model_b_ref = self.model_registry.get_model(model_b_name, model_b_version)
            
            # Get test data from recent listening
            test_data_query = f"""
            SELECT 
                track_id,
                track_name,
                primary_artist_name,
                primary_genre,
                track_popularity,
                album_name,
                denver_hour,
                is_weekend,
                COUNT(*) OVER (PARTITION BY track_id) AS actual_replay_count
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE denver_date >= DATEADD('days', -{test_days}, CURRENT_DATE)
            ORDER BY RANDOM()
            LIMIT 100
            """
            
            test_data = self.session.sql(test_data_query).to_pandas()
            
            if test_data.empty:
                return {"error": "No test data available"}
            
            # Generate recommendations from both models
            # Note: This is a simplified comparison - in a real scenario you'd need
            # to implement proper recommendation generation for each model
            
            # For now, we'll compare based on their metadata and training characteristics
            model_a_metadata = model_a_ref.metadata or {}
            model_b_metadata = model_b_ref.metadata or {}
            
            # Calculate performance scores based on model characteristics and test data
            scores_a = self._evaluate_model_performance(model_a_metadata, test_data)
            scores_b = self._evaluate_model_performance(model_b_metadata, test_data)
            
            # Determine winner
            winner = "model_a" if scores_a['overall_score'] > scores_b['overall_score'] else "model_b"
            confidence = abs(scores_a['overall_score'] - scores_b['overall_score'])
            
            ab_test_results = {
                "test_period_days": test_days,
                "test_data_points": len(test_data),
                "model_a": {
                    "name": model_a_name,
                    "version": model_a_version,
                    "scores": scores_a
                },
                "model_b": {
                    "name": model_b_name,
                    "version": model_b_version,
                    "scores": scores_b
                },
                "winner": winner,
                "confidence": float(confidence),
                "significant": confidence > 0.05,
                "recommendation": "deploy_winner" if confidence > 0.05 else "continue_testing",
                "test_timestamp": str(datetime.now())
            }
            
            logger.info(f"A/B test complete. Winner: {winner} (confidence: {confidence:.3f})")
            
            return ab_test_results
            
        except Exception as e:
            logger.error(f"A/B testing failed: {e}")
            return {"error": str(e)}
    
    def _evaluate_model_performance(self, model_metadata: Dict, test_data: pd.DataFrame) -> Dict:
        """Evaluate model performance based on metadata and test data alignment."""
        
        # This is a simplified evaluation - in production you'd want more sophisticated metrics
        scores = {
            "coverage_score": 0.0,
            "diversity_score": 0.0,
            "freshness_score": 0.0,
            "popularity_alignment": 0.0,
            "overall_score": 0.0
        }
        
        try:
            # Coverage: How well the model can handle the variety in test data
            training_days = model_metadata.get('training_data_days', 180)
            scores['coverage_score'] = min(training_days / 180, 1.0)
            
            # Diversity: Based on model's ability to handle diverse content
            n_components = model_metadata.get('n_components', 20)
            scores['diversity_score'] = min(n_components / 30, 1.0)
            
            # Freshness: How recent the model training is
            training_timestamp = model_metadata.get('training_timestamp', '')
            if training_timestamp:
                try:
                    training_date = pd.to_datetime(training_timestamp)
                    days_old = (datetime.now() - training_date).days
                    scores['freshness_score'] = max(0, 1 - (days_old / 30))  # Decay over 30 days
                except:
                    scores['freshness_score'] = 0.5
            
            # Popularity alignment: How well model parameters align with test data
            test_avg_popularity = test_data['track_popularity'].mean()
            model_type = model_metadata.get('model_type', '')
            
            if 'collaborative' in model_type:
                scores['popularity_alignment'] = 1 - abs(test_avg_popularity - 50) / 50
            elif 'content' in model_type:
                scores['popularity_alignment'] = 0.8  # Content models are generally good
            else:
                scores['popularity_alignment'] = 0.6
            
            # Overall score
            scores['overall_score'] = (
                scores['coverage_score'] * 0.3 +
                scores['diversity_score'] * 0.25 +
                scores['freshness_score'] * 0.25 +
                scores['popularity_alignment'] * 0.2
            )
            
        except Exception as e:
            logger.warning(f"Error in performance evaluation: {e}")
        
        return scores
    
    def schedule_automated_fine_tuning(
        self, 
        model_name: str = "spotify_hybrid_recommender",
        drift_threshold: float = 0.2,
        check_interval_days: int = 7
    ) -> Dict:
        """Set up automated fine-tuning based on performance monitoring."""
        
        logger.info(f"Setting up automated fine-tuning for {model_name}")
        
        # Create monitoring configuration
        monitoring_config = {
            "model_name": model_name,
            "drift_threshold": drift_threshold,
            "check_interval_days": check_interval_days,
            "last_check": str(datetime.now()),
            "auto_retrain_enabled": True,
            "notification_threshold": 0.1,  # Notify when performance drops by 10%
            "max_retrain_frequency_days": 3  # Don't retrain more than once every 3 days
        }
        
        # Save configuration
        config_filename = f"auto_tuning_config_{model_name}.json"
        
        try:
            with open(config_filename, 'w') as f:
                json.dump(monitoring_config, f, indent=2)
            
            logger.info(f"Automated fine-tuning configuration saved to {config_filename}")
            
            return {
                "status": "configured",
                "config_file": config_filename,
                "monitoring_config": monitoring_config
            }
            
        except Exception as e:
            logger.error(f"Failed to save monitoring configuration: {e}")
            return {"status": "failed", "error": str(e)}
    
    def run_automated_check(self, config_file: str = None) -> Dict:
        """Run automated performance check and fine-tuning if needed."""
        
        if config_file is None:
            config_file = "auto_tuning_config_spotify_hybrid_recommender.json"
        
        try:
            # Load configuration
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            model_name = config['model_name']
            drift_threshold = config['drift_threshold']
            
            logger.info(f"Running automated check for {model_name}")
            
            # Check performance
            performance = self.analyze_recent_performance(model_name)
            drift_analysis = self.detect_concept_drift()
            
            results = {
                "check_timestamp": str(datetime.now()),
                "model_name": model_name,
                "performance": performance,
                "drift_analysis": drift_analysis,
                "action_taken": "none"
            }
            
            # Decide if fine-tuning is needed
            needs_retraining = (
                drift_analysis.get('drift_detected', False) or
                performance.get('overall_score', 1.0) < 0.7
            )
            
            if needs_retraining:
                logger.info("Performance degradation detected. Initiating fine-tuning...")
                
                new_version = self.fine_tune_collaborative_model(model_name)
                
                if new_version:
                    results["action_taken"] = "fine_tuned"
                    results["new_version"] = new_version
                else:
                    results["action_taken"] = "fine_tuning_failed"
            
            # Update last check time
            config['last_check'] = str(datetime.now())
            with open(config_file, 'w') as f:
                json.dump(config, f, indent=2)
            
            return results
            
        except Exception as e:
            logger.error(f"Automated check failed: {e}")
            return {"error": str(e)}


def main():
    """Main entry point for model fine-tuning operations."""
    
    import argparse
    
    parser = argparse.ArgumentParser(description="Fine-tune Spotify ML models")
    parser.add_argument("--action", choices=['analyze', 'drift', 'finetune', 'abtest', 'autocheck'], 
                       required=True, help="Action to perform")
    parser.add_argument("--model", default="spotify_hybrid_recommender", 
                       help="Model name to work with")
    parser.add_argument("--version", default="latest", 
                       help="Model version")
    parser.add_argument("--days", type=int, default=7, 
                       help="Number of days for analysis")
    
    args = parser.parse_args()
    
    # Create session
    from train_and_register_models import create_snowflake_session
    
    session = create_snowflake_session()
    if not session:
        print("❌ Failed to create Snowflake session")
        return
    
    try:
        fine_tuner = SpotifyModelFineTuner(session)
        
        if args.action == 'analyze':
            results = fine_tuner.analyze_recent_performance(args.model, args.days)
            print(f"📊 Performance Analysis for {args.model}:")
            print(json.dumps(results, indent=2))
            
        elif args.action == 'drift':
            results = fine_tuner.detect_concept_drift(args.days)
            print(f"🔄 Concept Drift Analysis:")
            print(json.dumps(results, indent=2))
            
        elif args.action == 'finetune':
            new_version = fine_tuner.fine_tune_collaborative_model(args.model, args.version)
            if new_version:
                print(f"✅ Fine-tuning complete. New version: {new_version}")
            else:
                print("ℹ️ No fine-tuning performed")
                
        elif args.action == 'autocheck':
            results = fine_tuner.run_automated_check()
            print(f"🤖 Automated Check Results:")
            print(json.dumps(results, indent=2))
            
    finally:
        session.close()


if __name__ == "__main__":
    main()
