"""
Quick Local Configuration Test
Tests your Python environment and S3 connectivity before running full discovery pipeline
"""

import os
import sys
from datetime import datetime

def test_imports():
    """Test that all required packages are available."""
    print("🐍 Testing Python Environment...")
    
    try:
        import pandas as pd
        print("✅ pandas imported successfully")
    except ImportError as e:
        print(f"❌ pandas missing: {e}")
        return False
        
    try:
        import numpy as np
        print("✅ numpy imported successfully")
    except ImportError as e:
        print(f"❌ numpy missing: {e}")
        return False
        
    try:
        import json
        print("✅ json imported successfully")
    except ImportError as e:
        print(f"❌ json missing: {e}")
        return False
        
    try:
        from config import settings
        print("✅ config.settings imported successfully")
    except ImportError as e:
        print(f"❌ config.settings missing: {e}")
        return False
        
    try:
        from s3_client import S3Client
        print("✅ S3Client imported successfully")
    except ImportError as e:
        print(f"❌ S3Client missing: {e}")
        return False
        
    try:
        from spotify_client import SpotifyClient
        print("✅ SpotifyClient imported successfully")
    except ImportError as e:
        print(f"❌ SpotifyClient missing: {e}")
        return False
        
    return True

def test_config():
    """Test configuration settings."""
    print("\n⚙️  Testing Configuration...")
    
    try:
        from config import settings
        
        # Test Spotify config
        if hasattr(settings, 'spotify'):
            print("✅ Spotify configuration found")
            if settings.spotify.client_id and settings.spotify.client_id != 'your_spotify_client_id_here':
                print("✅ Spotify client ID configured")
            else:
                print("❌ Spotify client ID not configured")
                return False
                
            if settings.spotify.client_secret and settings.spotify.client_secret != 'your_spotify_client_secret_here':
                print("✅ Spotify client secret configured")
            else:
                print("❌ Spotify client secret not configured")  
                return False
        else:
            print("❌ Spotify configuration missing")
            return False
            
        # Test AWS config
        if hasattr(settings, 'aws'):
            print("✅ AWS configuration found")
            if settings.aws.s3_bucket_name and settings.aws.s3_bucket_name != 'your-spotify-pipeline-bucket-name':
                print(f"✅ S3 bucket configured: {settings.aws.s3_bucket_name}")
            else:
                print("❌ S3 bucket name not configured")
                return False
                
            if settings.aws.access_key_id and settings.aws.access_key_id != 'your_aws_access_key_id':
                print("✅ AWS access key ID configured")
            else:
                print("❌ AWS access key ID not configured")
                return False
                
            if settings.aws.secret_access_key and settings.aws.secret_access_key != 'your_aws_secret_access_key':
                print("✅ AWS secret access key configured")
            else:
                print("❌ AWS secret access key not configured")
                return False
        else:
            print("❌ AWS configuration missing")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

def test_s3_client():
    """Test S3 client initialization."""
    print("\n☁️  Testing S3 Client...")
    
    try:
        from s3_client import S3Client
        s3_client = S3Client()
        print(f"✅ S3 Client initialized successfully")
        print(f"✅ Bucket: {s3_client.bucket_name}")
        return True
    except Exception as e:
        print(f"❌ S3 Client initialization failed: {e}")
        print("💡 Check your AWS credentials in .env file")
        return False

def test_spotify_client():
    """Test Spotify client initialization."""
    print("\n🎵 Testing Spotify Client...")
    
    try:
        from spotify_client import SpotifyClient
        spotify_client = SpotifyClient()
        print("✅ Spotify Client initialized successfully")
        return True
    except Exception as e:
        print(f"❌ Spotify Client initialization failed: {e}")
        print("💡 Check your Spotify API credentials in .env file")
        return False

def test_file_creation():
    """Test file creation capabilities."""
    print("\n📁 Testing File Operations...")
    
    try:
        # Test JSON file creation
        test_data = {
            "test": True,
            "timestamp": datetime.now().isoformat(),
            "message": "Configuration test successful"
        }
        
        test_filename = f"test_config_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        import json
        with open(test_filename, 'w') as f:
            json.dump(test_data, f, indent=2)
            
        print(f"✅ Test file created: {test_filename}")
        
        # Clean up
        os.remove(test_filename)
        print("✅ Test file cleaned up")
        
        return True
    except Exception as e:
        print(f"❌ File operations failed: {e}")
        return False

def main():
    """Run all configuration tests."""
    print("🔧 LOCAL CONFIGURATION TEST")
    print("=" * 50)
    
    all_passed = True
    
    # Run all tests
    tests = [
        ("Python Environment", test_imports),
        ("Configuration Settings", test_config), 
        ("S3 Client", test_s3_client),
        ("Spotify Client", test_spotify_client),
        ("File Operations", test_file_creation)
    ]
    
    for test_name, test_func in tests:
        try:
            if not test_func():
                all_passed = False
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            all_passed = False
    
    print("\n" + "=" * 50)
    
    if all_passed:
        print("🎉 ALL TESTS PASSED!")
        print("""
✅ Your environment is ready for the discovery pipeline!

Next steps:
1. Run check_current_config.sql in Snowflake
2. Update setup_discovery_snowpipe.sql with your values  
3. Deploy the Snowflake infrastructure
4. Run python spotify_discovery_system.py
5. Check your new recommendations!
        """)
    else:
        print("❌ SOME TESTS FAILED!")
        print("""
🔧 Fix the issues above, then:
1. Check your .env file has all required values
2. Ensure virtual environment is activated
3. Run this test again: python test_local_config.py
        """)
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
