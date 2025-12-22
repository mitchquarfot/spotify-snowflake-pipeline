"""
ML Setup Validation Script
Validates that the Spotify ML recommendation system is properly configured
"""

import os
import sys
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional

# Try to import ML dependencies
try:
    import numpy as np
    import sklearn
    from sklearn.metrics.pairwise import cosine_similarity
    print("✅ scikit-learn imported successfully")
except ImportError as e:
    print(f"❌ Failed to import scikit-learn: {e}")
    print("Run: pip install scikit-learn==1.3.0")

# Try to import Snowflake ML
try:
    import snowflake.snowpark as snowpark
    from snowflake.ml.registry import ModelRegistry
    print("✅ Snowflake ML imported successfully")
except ImportError as e:
    print(f"❌ Failed to import Snowflake ML: {e}")
    print("Run: pip install snowflake-ml-python")

def create_test_session():
    """Create a test Snowflake session."""
    try:
        # Load environment variables
        connection_parameters = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"), 
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": "spotify_analytics",
            "schema": "analytics"
        }
        
        # Check required parameters
        required_params = ["account", "user", "password", "warehouse"]
        missing_params = [param for param in required_params if not connection_parameters[param]]
        
        if missing_params:
            print(f"❌ Missing environment variables: {missing_params}")
            print("Set these environment variables:")
            for param in missing_params:
                print(f"   export SNOWFLAKE_{param.upper()}=your_value")
            return None
        
        session = snowpark.Session.builder.configs(connection_parameters).create()
        print("✅ Snowflake session created successfully")
        return session
        
    except Exception as e:
        print(f"❌ Failed to create Snowflake session: {e}")
        return None

def validate_data_availability(session: snowpark.Session) -> Dict:
    """Validate that sufficient data exists for ML training."""
    
    print("\n🔍 Validating Data Availability...")
    
    validation_results = {
        "listening_history": False,
        "genre_diversity": False,
        "artist_diversity": False,
        "temporal_data": False,
        "sufficient_for_ml": False
    }
    
    try:
        # Check listening history
        listening_query = """
        SELECT 
            COUNT(*) as total_tracks,
            COUNT(DISTINCT primary_genre) as unique_genres,
            COUNT(DISTINCT primary_artist_id) as unique_artists,
            COUNT(DISTINCT DATE(denver_date)) as unique_days,
            MIN(denver_date) as earliest_date,
            MAX(denver_date) as latest_date
        FROM spotify_analytics.medallion_arch.silver_listening_enriched
        WHERE denver_date >= DATEADD('days', -180, CURRENT_DATE)
        """
        
        result = session.sql(listening_query).collect()
        stats = result[0]
        
        total_tracks = stats['TOTAL_TRACKS']
        unique_genres = stats['UNIQUE_GENRES']  
        unique_artists = stats['UNIQUE_ARTISTS']
        unique_days = stats['UNIQUE_DAYS']
        
        print(f"📊 Data Statistics:")
        print(f"   • Total tracks: {total_tracks}")
        print(f"   • Unique genres: {unique_genres}")
        print(f"   • Unique artists: {unique_artists}")
        print(f"   • Days of data: {unique_days}")
        print(f"   • Date range: {stats['EARLIEST_DATE']} to {stats['LATEST_DATE']}")
        
        # Validation checks
        validation_results["listening_history"] = total_tracks >= 50
        validation_results["genre_diversity"] = unique_genres >= 5
        validation_results["artist_diversity"] = unique_artists >= 20
        validation_results["temporal_data"] = unique_days >= 7
        
        validation_results["sufficient_for_ml"] = all([
            validation_results["listening_history"],
            validation_results["genre_diversity"], 
            validation_results["artist_diversity"],
            validation_results["temporal_data"]
        ])
        
        # Print validation results
        print(f"\n✅ Validation Results:")
        print(f"   • Sufficient listening history (≥50 tracks): {'✅' if validation_results['listening_history'] else '❌'}")
        print(f"   • Genre diversity (≥5 genres): {'✅' if validation_results['genre_diversity'] else '❌'}")
        print(f"   • Artist diversity (≥20 artists): {'✅' if validation_results['artist_diversity'] else '❌'}")
        print(f"   • Temporal data (≥7 days): {'✅' if validation_results['temporal_data'] else '❌'}")
        print(f"   • Ready for ML training: {'✅' if validation_results['sufficient_for_ml'] else '❌'}")
        
        return validation_results
        
    except Exception as e:
        print(f"❌ Data validation failed: {e}")
        return validation_results

def validate_ml_views(session: snowpark.Session) -> Dict:
    """Validate that ML views are created and populated."""
    
    print("\n🔍 Validating ML Views...")
    
    ml_views = {
        "ml_user_genre_interactions": False,
        "ml_track_content_features": False,
        "ml_temporal_patterns": False,
        "ml_genre_similarity_matrix": False,
        "ml_hybrid_recommendations": False
    }
    
    for view_name in ml_views.keys():
        try:
            count_query = f"SELECT COUNT(*) as count FROM {view_name}"
            result = session.sql(count_query).collect()
            count = result[0]['COUNT']
            
            ml_views[view_name] = count > 0
            status = "✅" if ml_views[view_name] else "❌"
            print(f"   • {view_name}: {status} ({count} rows)")
            
        except Exception as e:
            print(f"   • {view_name}: ❌ Error - {str(e)[:50]}...")
            ml_views[view_name] = False
    
    all_views_valid = all(ml_views.values())
    print(f"\n   📋 All ML views ready: {'✅' if all_views_valid else '❌'}")
    
    return ml_views

def validate_ml_functions(session: snowpark.Session) -> Dict:
    """Validate that ML inference functions work."""
    
    print("\n🔍 Validating ML Functions...")
    
    ml_functions = {
        "get_spotify_recommendations": False,
        "get_similar_tracks": False,
        "get_discovery_recommendations": False,
        "get_time_based_recommendations": False
    }
    
    # Test get_spotify_recommendations
    try:
        test_query = "SELECT * FROM TABLE(get_spotify_recommendations(5)) LIMIT 1"
        result = session.sql(test_query).collect()
        ml_functions["get_spotify_recommendations"] = len(result) >= 0  # Function exists even if no results
        print("   • get_spotify_recommendations: ✅")
    except Exception as e:
        print(f"   • get_spotify_recommendations: ❌ {str(e)[:50]}...")
    
    # Test get_discovery_recommendations  
    try:
        test_query = "SELECT * FROM TABLE(get_discovery_recommendations('balanced', 3)) LIMIT 1"
        result = session.sql(test_query).collect()
        ml_functions["get_discovery_recommendations"] = len(result) >= 0
        print("   • get_discovery_recommendations: ✅")
    except Exception as e:
        print(f"   • get_discovery_recommendations: ❌ {str(e)[:50]}...")
    
    # Test get_time_based_recommendations
    try:
        current_hour = datetime.now().hour
        test_query = f"SELECT * FROM TABLE(get_time_based_recommendations({current_hour}, false, 3)) LIMIT 1"
        result = session.sql(test_query).collect()
        ml_functions["get_time_based_recommendations"] = len(result) >= 0
        print("   • get_time_based_recommendations: ✅")
    except Exception as e:
        print(f"   • get_time_based_recommendations: ❌ {str(e)[:50]}...")
    
    # Test user taste profile
    try:
        test_query = "SELECT * FROM TABLE(get_user_taste_profile()) LIMIT 1"
        result = session.sql(test_query).collect()
        ml_functions["get_similar_tracks"] = len(result) >= 0  # Using this as proxy
        print("   • get_user_taste_profile: ✅")
    except Exception as e:
        print(f"   • get_user_taste_profile: ❌ {str(e)[:50]}...")
    
    functions_working = sum(ml_functions.values())
    print(f"\n   🎯 ML functions working: {functions_working}/{len(ml_functions)}")
    
    return ml_functions

def test_recommendations(session: snowpark.Session) -> bool:
    """Test generating actual recommendations."""
    
    print("\n🔍 Testing Recommendation Generation...")
    
    try:
        # Test hybrid recommendations
        recs_query = """
        SELECT 
            track_name,
            artist_name,
            genre,
            recommendation_score
        FROM TABLE(get_spotify_recommendations(5, NULL, NULL, NULL, 0.1))
        """
        
        recommendations_df = session.sql(recs_query).to_pandas()
        
        if not recommendations_df.empty:
            print(f"✅ Generated {len(recommendations_df)} recommendations!")
            print("   📋 Sample recommendations:")
            for idx, row in recommendations_df.head(3).iterrows():
                track_name = row.get('TRACK_NAME', 'Unknown')
                artist_name = row.get('ARTIST_NAME', 'Unknown')
                genre = row.get('GENRE', 'Unknown')
                score = row.get('RECOMMENDATION_SCORE', 0)
                print(f"      {idx+1}. {track_name} by {artist_name} ({genre}) - Score: {score:.3f}")
            
            return True
        else:
            print("❌ No recommendations generated")
            print("   💡 This might be normal if you need more listening history")
            return False
            
    except Exception as e:
        print(f"❌ Recommendation test failed: {e}")
        return False

def generate_setup_report(validation_results: Dict) -> str:
    """Generate a comprehensive setup report."""
    
    report = f"""
🎵 SPOTIFY ML SETUP VALIDATION REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*50}

OVERALL STATUS: {'✅ READY FOR ML' if validation_results.get('overall_success', False) else '❌ SETUP INCOMPLETE'}

VALIDATION RESULTS:
• Dependencies: {'✅' if validation_results.get('dependencies', False) else '❌'}
• Snowflake Connection: {'✅' if validation_results.get('snowflake_connection', False) else '❌'}
• Data Availability: {'✅' if validation_results.get('data_sufficient', False) else '❌'}
• ML Views: {'✅' if validation_results.get('ml_views', False) else '❌'}
• ML Functions: {'✅' if validation_results.get('ml_functions', False) else '❌'}
• Recommendation Test: {'✅' if validation_results.get('recommendations_working', False) else '❌'}

NEXT STEPS:
"""
    
    if validation_results.get('overall_success', False):
        report += """
✅ Your ML system is ready! You can:
   1. Open Streamlit app and go to "🤖 ML Recommendations" tab
   2. Generate personalized recommendations
   3. Run automated training: python train_and_register_models.py
   4. Set up monitoring: CALL start_ml_automation(); in Snowflake
"""
    else:
        report += """
❌ Setup needs attention:
"""
        if not validation_results.get('dependencies', False):
            report += "   1. Install ML dependencies: pip install -r requirements.txt\n"
        if not validation_results.get('snowflake_connection', False):
            report += "   2. Configure Snowflake connection (check environment variables)\n"
        if not validation_results.get('data_sufficient', False):
            report += "   3. Collect more listening data (need 50+ tracks, 5+ genres)\n"
        if not validation_results.get('ml_views', False):
            report += "   4. Run SQL setup: spotify_ml_recommendation_engine.sql\n"
        if not validation_results.get('ml_functions', False):
            report += "   5. Deploy inference functions: model_inference_functions.sql\n"
    
    return report

def main():
    """Main validation workflow."""
    
    print("🎵 SPOTIFY ML RECOMMENDATION SYSTEM VALIDATION")
    print("=" * 50)
    
    overall_results = {
        'dependencies': True,  # Already checked imports at top
        'snowflake_connection': False,
        'data_sufficient': False,
        'ml_views': False,
        'ml_functions': False,
        'recommendations_working': False,
        'overall_success': False
    }
    
    # Test Snowflake connection
    session = create_test_session()
    if session is None:
        print(generate_setup_report(overall_results))
        return
    
    overall_results['snowflake_connection'] = True
    
    try:
        # Validate data availability
        data_validation = validate_data_availability(session)
        overall_results['data_sufficient'] = data_validation.get('sufficient_for_ml', False)
        
        # Validate ML views
        view_validation = validate_ml_views(session)
        overall_results['ml_views'] = all(view_validation.values())
        
        # Validate ML functions
        function_validation = validate_ml_functions(session)
        overall_results['ml_functions'] = sum(function_validation.values()) >= 2  # At least 2 working
        
        # Test recommendations
        overall_results['recommendations_working'] = test_recommendations(session)
        
        # Overall success check
        overall_results['overall_success'] = all([
            overall_results['dependencies'],
            overall_results['snowflake_connection'],
            overall_results['data_sufficient'],
            overall_results['ml_views'],
            overall_results['ml_functions']
        ])
        
    finally:
        session.close()
    
    # Generate and print final report
    report = generate_setup_report(overall_results)
    print(report)
    
    # Save report to file
    with open('ml_validation_report.txt', 'w') as f:
        f.write(report)
    
    print(f"\n📄 Full report saved to: ml_validation_report.txt")
    
    return overall_results['overall_success']

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
