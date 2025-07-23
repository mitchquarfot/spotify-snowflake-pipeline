#!/usr/bin/env python3
"""Setup script for configuring the Spotify to S3 pipeline."""

import os
import sys
import getpass
from pathlib import Path


def create_env_file():
    """Create .env file with user input."""
    print("🔧 Setting up environment configuration...")
    print("\nYou'll need:")
    print("  1. Spotify App credentials (from https://developer.spotify.com/dashboard)")
    print("  2. AWS credentials with S3 access")
    print("  3. S3 bucket name (will be created if it doesn't exist)")
    print()
    
    # Spotify credentials
    print("📱 Spotify API Configuration:")
    spotify_client_id = input("  Spotify Client ID: ").strip()
    spotify_client_secret = getpass.getpass("  Spotify Client Secret: ").strip()
    
    # AWS credentials
    print("\n☁️  AWS Configuration:")
    aws_access_key = input("  AWS Access Key ID: ").strip()
    aws_secret_key = getpass.getpass("  AWS Secret Access Key: ").strip()
    aws_region = input("  AWS Region (default: us-west-2): ").strip() or "us-west-2"
    s3_bucket = input("  S3 Bucket Name: ").strip()
    
    # Pipeline configuration
    print("\n⚙️  Pipeline Configuration:")
    fetch_interval = input("  Fetch interval in minutes (default: 30): ").strip() or "30"
    batch_size = input("  Batch size (default: 50): ").strip() or "50"
    
    # Create .env file
    env_content = f"""# Spotify API Credentials
SPOTIFY_CLIENT_ID={spotify_client_id}
SPOTIFY_CLIENT_SECRET={spotify_client_secret}
SPOTIFY_REDIRECT_URI=http://localhost:8080/callback

# AWS Configuration
AWS_ACCESS_KEY_ID={aws_access_key}
AWS_SECRET_ACCESS_KEY={aws_secret_key}
AWS_REGION={aws_region}
S3_BUCKET_NAME={s3_bucket}

# Pipeline Configuration
FETCH_INTERVAL_MINUTES={fetch_interval}
BATCH_SIZE={batch_size}
MAX_RETRIES=3

# Snowflake-compatible settings
SNOWFLAKE_STAGE_PREFIX=spotify_listening_history/
DATE_PARTITION_FORMAT=%Y/%m/%d
"""
    
    with open(".env", "w") as f:
        f.write(env_content)
    
    print("\n✅ Environment configuration saved to .env")
    return True


def check_dependencies():
    """Check if required dependencies are installed."""
    print("📦 Checking dependencies...")
    
    required_packages = [
        "spotipy", "boto3", "python-dotenv", "schedule", 
        "pandas", "structlog", "tenacity", "pydantic"
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package}")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n⚠️  Missing packages: {', '.join(missing_packages)}")
        print("Run: pip install -r requirements.txt")
        return False
    
    print("\n✅ All dependencies satisfied")
    return True


def create_spotify_app_instructions():
    """Display instructions for creating a Spotify app."""
    print("""
🎵 Creating a Spotify App:

1. Go to https://developer.spotify.com/dashboard
2. Log in with your Spotify account
3. Click "Create App"
4. Fill in:
   - App name: "My Spotify Data Pipeline"
   - App description: "Personal listening history data pipeline"
   - Redirect URI: http://localhost:8080/callback
   - Which API/SDKs are you planning to use: Web API
   - Commercial/Non-Commercial: Non-Commercial
5. Save the app
6. Copy the Client ID and Client Secret

""")


def create_aws_instructions():
    """Display instructions for AWS setup."""
    print("""
☁️  AWS Setup:

1. Go to https://console.aws.amazon.com/iam/
2. Create a new user or use existing user
3. Attach policy with S3 permissions (or use AmazonS3FullAccess for simplicity)
4. Create access keys for programmatic access
5. Choose an S3 bucket name (globally unique)

Required S3 permissions:
- s3:CreateBucket
- s3:PutObject
- s3:GetObject
- s3:ListBucket

""")


def main():
    """Main setup function."""
    print("🚀 Spotify to S3 Pipeline Setup")
    print("=" * 50)
    
    # Check if .env already exists
    if os.path.exists(".env"):
        response = input("\n.env file already exists. Overwrite? (y/N): ")
        if response.lower() != 'y':
            print("Setup cancelled.")
            sys.exit(0)
    
    # Show instructions
    print("\n📋 Setup Instructions:")
    choice = input("\nDo you need setup instructions? (y/N): ")
    if choice.lower() == 'y':
        create_spotify_app_instructions()
        create_aws_instructions()
        input("Press Enter when ready to continue...")
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Create environment file
    if not create_env_file():
        sys.exit(1)
    
    print("\n🎉 Setup complete!")
    print("\nNext steps:")
    print("  1. Test the connection: python main.py test")
    print("  2. Run once: python main.py run-once")
    print("  3. Backfill data: python main.py backfill --days 7")
    print("  4. Run continuously: python main.py run-continuous")
    print("\nFor Snowflake integration, see README.md for Snowpipe setup instructions.")


if __name__ == "__main__":
    main() 