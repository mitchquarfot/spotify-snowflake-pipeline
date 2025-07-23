#!/usr/bin/env python3
"""Main entry point for Spotify to S3 data pipeline."""

import argparse
import sys
from datetime import datetime
import structlog
from dotenv import load_dotenv

from pipeline import SpotifyDataPipeline

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


def main():
    """Main entry point."""
    # Load environment variables
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description="Spotify to S3 Data Pipeline for Snowflake ingestion"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Run once command
    run_once_parser = subparsers.add_parser(
        "run-once", 
        help="Run the pipeline once and exit"
    )
    
    # Continuous run command
    continuous_parser = subparsers.add_parser(
        "run-continuous",
        help="Run the pipeline continuously on a schedule"
    )
    
    # Backfill command
    backfill_parser = subparsers.add_parser(
        "backfill",
        help="Backfill historical data"
    )
    backfill_parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to backfill (default: 7, max: 50)"
    )
    
    # Stats command
    stats_parser = subparsers.add_parser(
        "stats",
        help="Show pipeline statistics"
    )
    
    # Test command
    test_parser = subparsers.add_parser(
        "test",
        help="Test connections and configuration"
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        pipeline = SpotifyDataPipeline()
        
        if args.command == "run-once":
            logger.info("Running pipeline once")
            success = pipeline.run_once()
            if success:
                print("✅ Pipeline run completed successfully")
                sys.exit(0)
            else:
                print("❌ Pipeline run failed")
                sys.exit(1)
        
        elif args.command == "run-continuous":
            logger.info("Starting continuous pipeline")
            print("🔄 Starting continuous pipeline (Ctrl+C to stop)")
            pipeline.run_continuous()
        
        elif args.command == "backfill":
            logger.info("Starting backfill", days=args.days)
            print(f"📥 Starting backfill for {args.days} days")
            pipeline.backfill_historical_data(args.days)
            print("✅ Backfill completed")
        
        elif args.command == "stats":
            logger.info("Getting pipeline stats")
            stats = pipeline.get_pipeline_stats()
            print("\n📊 Pipeline Statistics:")
            print(f"   Last processed: {stats.get('last_updated', 'Never')}")
            print(f"   Recent files (7d): {stats.get('recent_files_count', 0)}")
            print(f"   Total size: {stats.get('total_recent_size_bytes', 0):,} bytes")
            print(f"   S3 bucket: {stats.get('bucket_name', 'N/A')}")
            print(f"   Batch size: {stats.get('config', {}).get('batch_size', 'N/A')}")
            print(f"   Fetch interval: {stats.get('config', {}).get('fetch_interval_minutes', 'N/A')} min")
        
        elif args.command == "test":
            logger.info("Testing connections")
            print("🔍 Testing connections...")
            
            # Test Spotify authentication
            print("   Testing Spotify connection...", end=" ")
            if pipeline.spotify_client.authenticate():
                print("✅")
            else:
                print("❌")
                sys.exit(1)
            
            # Test S3 connection
            print("   Testing S3 connection...", end=" ")
            if pipeline.s3_client.ensure_bucket_exists():
                print("✅")
            else:
                print("❌")
                sys.exit(1)
            
            print("✅ All tests passed!")
    
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
        print("\n👋 Pipeline stopped by user")
        sys.exit(0)
    
    except Exception as e:
        logger.error("Pipeline failed", error=str(e))
        print(f"❌ Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 