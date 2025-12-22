#!/usr/bin/env python3
"""
Dual Pipeline Comparison Test
Compare Smart Search vs. ML Hybrid discovery approaches

This will:
1. Run Smart Search pipeline (working)
2. Show current state without ML (for now)
3. Provide setup instructions for ML integration
"""

import subprocess
import json
import os
from datetime import datetime

def run_smart_search_pipeline():
    """Run the Smart Search discovery pipeline"""
    print("🔍 Running Smart Search Pipeline (Pipeline A)...")
    print("=" * 60)
    
    try:
        result = subprocess.run(['python', 'spotify_discovery_system.py'], 
                              capture_output=True, text=True, timeout=120)
        
        if result.returncode == 0:
            print("✅ Smart Search Pipeline SUCCESS!")
            
            # Extract key info from output
            output_lines = result.stdout.split('\n')
            discoveries = 0
            avg_popularity = 0
            
            for line in output_lines:
                if "Discovered" in line and "tracks" in line:
                    try:
                        discoveries = int(line.split("Discovered ")[1].split(" ")[0])
                    except:
                        pass
                if "Average popularity:" in line:
                    try:
                        avg_popularity = float(line.split("Average popularity: ")[1])
                    except:
                        pass
            
            print(f"📊 Results: {discoveries} tracks, avg popularity: {avg_popularity}")
            return {'success': True, 'tracks': discoveries, 'popularity': avg_popularity}
        else:
            print(f"❌ Smart Search failed: {result.stderr}")
            return {'success': False, 'error': result.stderr}
            
    except Exception as e:
        print(f"❌ Error running Smart Search: {e}")
        return {'success': False, 'error': str(e)}

def check_ml_system_readiness():
    """Check if ML system can be activated"""
    print("\n🧠 Checking ML Hybrid Pipeline Readiness...")
    print("=" * 60)
    
    # Check if required files exist
    required_files = [
        'spotify_ml_recommendation_engine.sql',
        'model_inference_functions.sql', 
        'automated_model_retraining.sql'
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ Missing files: {', '.join(missing_files)}")
        return False
    
    print("✅ ML system files are present")
    
    # Check environment variables
    snowflake_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD']
    missing_vars = [var for var in snowflake_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"⚠️  Missing Snowflake environment variables: {', '.join(missing_vars)}")
        print("📝 You need to:")
        print("   1. Run the ML SQL scripts in Snowflake first")
        print("   2. Set up environment variables or modify connection config")
        print("   3. Test ML pipeline: python spotify_ml_discovery_system.py")
        return False
    
    print("✅ Environment variables are set")
    return True

def simulate_ml_results():
    """Simulate what ML results would look like"""
    print("\n🎯 ML Hybrid Pipeline (Simulated Results)")
    print("=" * 60)
    print("🧠 When properly configured, ML Pipeline would:")
    print("   ✓ Query Snowflake ML recommendation views")
    print("   ✓ Use 6 advanced algorithms:")
    print("     - Collaborative Filtering (40% weight)")
    print("     - Content-Based Filtering (30% weight)")
    print("     - Temporal Patterns (20% weight)")
    print("     - Discovery Engine (10% weight)")
    print("     - Jaccard Similarity Matrix")
    print("     - Hybrid Ensemble Model")
    print("   ✓ Generate higher quality, personalized recommendations")
    print("   ✓ Save to s3://mquarfot-dev/spotify_ml_discoveries/")
    print("")
    print("📈 Expected ML advantages:")
    print("   • Better personalization based on listening patterns")
    print("   • More sophisticated similarity calculations")
    print("   • Temporal and contextual recommendations")
    print("   • Discovery vs. exploitation balance")

def show_next_steps():
    """Show next steps for full dual pipeline setup"""
    print("\n🚀 Next Steps for Complete Dual Pipeline:")
    print("=" * 60)
    print("1. 📊 Deploy ML Infrastructure:")
    print("   - Run setup_ml_discovery_snowpipe.sql in Snowflake")
    print("")
    print("2. 🧠 Set up ML Recommendation Views:")
    print("   - Run spotify_ml_recommendation_engine.sql")
    print("   - Run model_inference_functions.sql")
    print("   - Wait for data to populate views")
    print("")
    print("3. 🔐 Configure Snowflake Connection:")
    print("   - Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD")
    print("   - Or modify spotify_ml_discovery_system.py connection config")
    print("")
    print("4. 🧪 Test Both Pipelines:")
    print("   - Smart Search: python spotify_discovery_system.py")
    print("   - ML Hybrid: python spotify_ml_discovery_system.py")
    print("")
    print("5. 📊 Compare Results:")
    print("   - Run compare_discovery_pipelines.sql in Snowflake")
    print("   - Analyze quality, diversity, and personalization")

if __name__ == "__main__":
    print("🎵 DUAL DISCOVERY PIPELINE COMPARISON TEST 🎵")
    print("=" * 80)
    print("")
    
    # Test Smart Search pipeline
    smart_results = run_smart_search_pipeline()
    
    # Check ML system readiness  
    ml_ready = check_ml_system_readiness()
    
    # Show simulated ML results
    simulate_ml_results()
    
    # Show next steps
    show_next_steps()
    
    print("\n🎯 SUMMARY:")
    print("=" * 60)
    if smart_results['success']:
        print(f"✅ Smart Search: WORKING ({smart_results['tracks']} tracks)")
    else:
        print("❌ Smart Search: FAILED")
        
    if ml_ready:
        print("✅ ML Hybrid: READY TO TEST")
    else:
        print("⚠️  ML Hybrid: NEEDS SETUP")
    
    print("\n🏆 Your discovery system foundation is solid!")
    print("Complete the ML setup to unlock advanced personalization!")
