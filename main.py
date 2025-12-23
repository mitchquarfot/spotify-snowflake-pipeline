#!/usr/bin/env python3
"""Main entry point for Spotify to S3 data pipeline."""

import argparse
import sys
from datetime import datetime
import structlog
from dotenv import load_dotenv

from pipeline import SpotifyDataPipeline

# Configure structured logging
_base_processors = [
    structlog.stdlib.filter_by_level,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
]

# structlog dropped UnicodeDecoder in v24+, but older versions still expose it.
# Configure dynamically so the pipeline keeps working across versions.
_unicode_decoder = getattr(structlog.processors, "UnicodeDecoder", None)
if _unicode_decoder:
    # Older structlog exposes UnicodeDecoder as a function, newer versions as a class.
    # Detect and append the appropriate callable without breaking either version.
    if isinstance(_unicode_decoder, type):
        _base_processors.append(_unicode_decoder())
    else:
        _base_processors.append(_unicode_decoder)

_base_processors.append(structlog.processors.JSONRenderer())

structlog.configure(
    processors=_base_processors,
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
    
    # Global options
    parser.add_argument(
        "--enable-artist-genre-processing",
        action="store_true",
        help="Enable artist genre processing alongside track processing"
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
    
    # Artist genre specific commands
    artist_backfill_parser = subparsers.add_parser(
        "backfill-artists",
        help="Process artist-genre data for all unique artists from recent listening history"
    )
    artist_backfill_parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days of listening history to extract artists from (default: 30)"
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
    
    # Process specific artists command
    process_artists_parser = subparsers.add_parser(
        "process-artists",
        help="Process specific artist IDs for genre data"
    )
    process_artists_parser.add_argument(
        "artist_ids",
        help="Comma-separated list of Spotify artist IDs"
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        pipeline = SpotifyDataPipeline(enable_artist_genre_processing=args.enable_artist_genre_processing)
        
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
        
        elif args.command == "backfill-artists":
            logger.info("Starting artist backfill", days=args.days)
            print(f"📥 Starting artist backfill for {args.days} days")
            pipeline.backfill_artist_genre_data(args.days)
            print("✅ Artist backfill completed")
        
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
            
            # Display artist-genre processing stats
            artist_stats = stats.get('artist_genre_processing', {})
            if artist_stats.get('enabled'):
                print(f"\n🎭 Artist Genre Processing:")
                print(f"   Status: Enabled ✅")
                print(f"   Processed artists: {artist_stats.get('total_processed_artists', 0):,}")
                print(f"   S3 prefix: {artist_stats.get('artist_s3_prefix', 'N/A')}")
            else:
                print(f"\n🎭 Artist Genre Processing: Disabled")
        
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
            
        elif args.command == "process-artists":
            if not args.enable_artist_genre_processing:
                print("❌ Artist-genre processing must be enabled with --enable-artist-genre-processing")
                sys.exit(1)
            
            print(f"🎭 Processing specific artists: {args.artist_ids}")
            
            # Parse artist IDs
            artist_ids = [aid.strip() for aid in args.artist_ids.split(',') if aid.strip()]
            
            if not artist_ids:
                print("❌ No valid artist IDs provided")
                sys.exit(1)
            
            print(f"📝 Found {len(artist_ids)} artist IDs to process")
            
            # Process the artists
            try:
                processed_artists = pipeline.artist_genre_processor.process_artist_batch(artist_ids)
                
                if processed_artists:
                    # Upload to S3
                    s3_key = pipeline.artist_genre_processor.upload_artists_to_s3(processed_artists)
                    print(f"✅ Successfully processed {len(processed_artists)} artists")
                    print(f"📤 Uploaded to S3: {s3_key}")
                else:
                    print("⚠️ No artists were successfully processed")
                    
            except Exception as e:
                logger.error("Failed to process artists", error=str(e))
                print(f"❌ Failed to process artists: {e}")
                sys.exit(1)
    
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