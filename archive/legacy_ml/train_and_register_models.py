"""
Training and Registration Script for Spotify ML Models
Trains and registers all recommendation models in Snowflake Model Registry
"""

import os
import json
from datetime import datetime
from snowflake.snowpark import Session
from spotify_ml_models import SpotifyHybridRecommender
import structlog

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.stdlib.LoggerFactory(),
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


def create_snowflake_session():
    """Create Snowflake session using environment variables or config."""
    
    try:
        # Try to load from environment variables first
        connection_parameters = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": "spotify_analytics",
            "schema": "analytics"
        }
        
        # Check if all required parameters are present
        required_params = ["account", "user", "password", "warehouse"]
        missing_params = [param for param in required_params if not connection_parameters[param]]
        
        if missing_params:
            logger.error(f"Missing Snowflake connection parameters: {missing_params}")
            logger.info("Please set the following environment variables:")
            logger.info("- SNOWFLAKE_ACCOUNT")
            logger.info("- SNOWFLAKE_USER") 
            logger.info("- SNOWFLAKE_PASSWORD")
            logger.info("- SNOWFLAKE_WAREHOUSE")
            logger.info("- SNOWFLAKE_ROLE (optional, defaults to ACCOUNTADMIN)")
            return None
        
        session = Session.builder.configs(connection_parameters).create()
        logger.info("Successfully created Snowflake session")
        return session
        
    except Exception as e:
        logger.error(f"Failed to create Snowflake session: {e}")
        return None


def check_data_availability(session: Session) -> bool:
    """Check if sufficient data is available for training."""
    
    try:
        # Check listening history data
        listening_count_query = """
        SELECT COUNT(*) as total_tracks
        FROM spotify_analytics.medallion_arch.silver_listening_enriched
        WHERE denver_date >= DATEADD('days', -180, CURRENT_DATE)
        """
        
        result = session.sql(listening_count_query).collect()
        total_tracks = result[0]['TOTAL_TRACKS']
        
        if total_tracks < 100:
            logger.warning(f"Insufficient listening data: {total_tracks} tracks (need at least 100)")
            return False
        
        # Check genre diversity
        genre_count_query = """
        SELECT COUNT(DISTINCT primary_genre) as unique_genres
        FROM spotify_analytics.medallion_arch.silver_listening_enriched
        WHERE denver_date >= DATEADD('days', -180, CURRENT_DATE)
        """
        
        result = session.sql(genre_count_query).collect()
        unique_genres = result[0]['UNIQUE_GENRES']
        
        if unique_genres < 5:
            logger.warning(f"Insufficient genre diversity: {unique_genres} genres (need at least 5)")
            return False
        
        # Check artist diversity
        artist_count_query = """
        SELECT COUNT(DISTINCT primary_artist_id) as unique_artists
        FROM spotify_analytics.medallion_arch.silver_listening_enriched
        WHERE denver_date >= DATEADD('days', -180, CURRENT_DATE)
        """
        
        result = session.sql(artist_count_query).collect()
        unique_artists = result[0]['UNIQUE_ARTISTS']
        
        if unique_artists < 20:
            logger.warning(f"Insufficient artist diversity: {unique_artists} artists (need at least 20)")
            return False
        
        logger.info(f"Data availability check passed: {total_tracks} tracks, {unique_genres} genres, {unique_artists} artists")
        return True
        
    except Exception as e:
        logger.error(f"Failed to check data availability: {e}")
        return False


def train_and_register_recommendation_models(session: Session, force_retrain: bool = False):
    """Complete model training and registration pipeline."""
    
    logger.info("🎵 Starting Spotify ML Model Training Pipeline")
    
    try:
        # Check if models already exist
        if not force_retrain:
            try:
                existing_models_query = """
                SHOW MODELS LIKE 'spotify_%' IN DATABASE spotify_analytics
                """
                existing_models = session.sql(existing_models_query).collect()
                
                if existing_models:
                    logger.info(f"Found {len(existing_models)} existing models")
                    user_input = input("Models already exist. Retrain? (y/N): ")
                    if user_input.lower() != 'y':
                        logger.info("Skipping training. Use force_retrain=True to override.")
                        return None
            except Exception:
                # Models don't exist or we can't check - proceed with training
                pass
        
        # Check data availability
        if not check_data_availability(session):
            logger.error("Insufficient data for training. Collect more listening history first.")
            return None
        
        # Create timestamp for this training run
        training_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        version = f"v{training_timestamp}"
        
        logger.info(f"Training models with version: {version}")
        
        # Initialize hybrid recommender
        logger.info("🧠 Initializing hybrid recommendation system...")
        hybrid_model = SpotifyHybridRecommender(session)
        
        # Train the complete ensemble
        logger.info("🏋️ Training ensemble models...")
        hybrid_model.train_hybrid_ensemble()
        
        # Register individual component models
        logger.info("📝 Registering individual models in Snowflake Model Registry...")
        
        model_refs = {}
        
        # Register collaborative filtering model
        if hybrid_model.collaborative_model:
            logger.info("Registering collaborative filtering model...")
            collab_ref = hybrid_model.collaborative_model.register_model(
                model_name="spotify_collaborative_filter",
                version=version
            )
            model_refs['collaborative'] = str(collab_ref)
        
        # Register content-based model
        if hybrid_model.content_model:
            logger.info("Registering content-based filtering model...")
            content_ref = hybrid_model.content_model.register_model(
                model_name="spotify_content_based",
                version=version
            )
            model_refs['content_based'] = str(content_ref)
        
        # Register hybrid ensemble model
        logger.info("Registering hybrid ensemble model...")
        ensemble_ref = hybrid_model.register_ensemble_model(
            model_name="spotify_hybrid_recommender",
            version=version
        )
        model_refs['hybrid_ensemble'] = str(ensemble_ref)
        
        # Test the models with sample predictions
        logger.info("🧪 Testing models with sample predictions...")
        
        sample_recommendations = hybrid_model.predict_recommendations(
            num_recommendations=10,
            current_hour=datetime.now().hour,
            is_weekend=datetime.now().weekday() >= 5
        )
        
        logger.info(f"✅ Generated {len(sample_recommendations)} test recommendations")
        
        # Create training summary
        training_summary = {
            "training_timestamp": training_timestamp,
            "version": version,
            "model_references": model_refs,
            "training_status": "success",
            "sample_recommendations_count": len(sample_recommendations),
            "sample_recommendations": sample_recommendations[:3],  # Save first 3 as examples
            "data_stats": {
                "training_data_days": 180,
                "content_training_days": 90,
                "temporal_training_days": 90
            }
        }
        
        logger.info("✅ All models registered successfully!")
        logger.info(f"📊 Collaborative Model: {model_refs.get('collaborative', 'Not trained')}")
        logger.info(f"📊 Content Model: {model_refs.get('content_based', 'Not trained')}")
        logger.info(f"📊 Hybrid Ensemble: {model_refs.get('hybrid_ensemble', 'Not trained')}")
        
        return training_summary
        
    except Exception as e:
        logger.error(f"Training pipeline failed: {e}")
        training_summary = {
            "training_timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "training_status": "failed",
            "error": str(e)
        }
        return training_summary


def save_training_results(training_summary: dict, filename: str = None):
    """Save training results to a JSON file."""
    
    if filename is None:
        timestamp = training_summary.get('training_timestamp', datetime.now().strftime("%Y%m%d_%H%M%S"))
        filename = f"model_training_results_{timestamp}.json"
    
    try:
        with open(filename, 'w') as f:
            json.dump(training_summary, f, indent=2, default=str)
        
        logger.info(f"Training results saved to {filename}")
        
        # Also save the latest results
        with open('latest_model_training.json', 'w') as f:
            json.dump(training_summary, f, indent=2, default=str)
        
    except Exception as e:
        logger.error(f"Failed to save training results: {e}")


def main():
    """Main training script entry point."""
    
    print("🎵 Spotify ML Model Training & Registration")
    print("=" * 50)
    
    # Create Snowflake session
    session = create_snowflake_session()
    if session is None:
        print("❌ Failed to create Snowflake session. Check your configuration.")
        return
    
    try:
        # Run training pipeline
        training_summary = train_and_register_recommendation_models(session)
        
        if training_summary is None:
            print("❌ Training was cancelled or skipped.")
            return
        
        # Save results
        save_training_results(training_summary)
        
        # Print summary
        print("\n🎯 Training Complete!")
        print(f"Status: {training_summary['training_status']}")
        
        if training_summary['training_status'] == 'success':
            print(f"Version: {training_summary['version']}")
            print(f"Models registered: {len(training_summary.get('model_references', {}))}")
            
            if training_summary.get('sample_recommendations'):
                print("\n📋 Sample Recommendations:")
                for i, rec in enumerate(training_summary['sample_recommendations'], 1):
                    strategy = rec.get('strategy', 'unknown')
                    score = rec.get('final_score', rec.get('score', 0))
                    
                    if 'track_name' in rec:
                        print(f"  {i}. {rec['track_name']} by {rec.get('artist_name', 'Unknown')} "
                              f"({strategy}, score: {score:.3f})")
                    else:
                        print(f"  {i}. {rec.get('genre', 'Unknown genre')} "
                              f"({strategy}, score: {score:.3f})")
        else:
            print(f"Error: {training_summary.get('error', 'Unknown error')}")
        
        print(f"\n📄 Detailed results saved to: latest_model_training.json")
        
    finally:
        session.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Train and register Spotify ML models")
    parser.add_argument("--force-retrain", action="store_true", 
                       help="Force retraining even if models exist")
    parser.add_argument("--version", type=str, 
                       help="Custom version string for models")
    
    args = parser.parse_args()
    
    # Override main function if custom arguments provided
    if args.force_retrain or args.version:
        session = create_snowflake_session()
        if session:
            try:
                training_summary = train_and_register_recommendation_models(
                    session, 
                    force_retrain=args.force_retrain
                )
                
                if training_summary:
                    if args.version:
                        training_summary['custom_version'] = args.version
                    save_training_results(training_summary)
                
            finally:
                session.close()
    else:
        main()
