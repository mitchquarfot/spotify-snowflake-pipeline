#!/usr/bin/env python3
"""
Process missing artists with comprehensive genre classification.
Uses multiple strategies including Every Noise concepts, direct mappings, and intelligent inference.
"""

import sys
from typing import List
from integrated_genre_classifier import IntegratedGenreClassifier
from spotify_client import SpotifyClient
from s3_client import S3Client
import structlog

logger = structlog.get_logger(__name__)

def process_missing_artists_from_ids(artist_ids_str: str) -> int:
    """
    Process missing artists from comma-separated ID string.
    
    Args:
        artist_ids_str: Comma-separated artist IDs
        
    Returns:
        Number of artists successfully processed
    """
    # Parse artist IDs
    artist_ids = [aid.strip() for aid in artist_ids_str.split(',') if aid.strip()]
    
    if not artist_ids:
        print("❌ No valid artist IDs provided")
        return 0
    
    print(f"🎭 Processing {len(artist_ids)} artists with comprehensive genre classification")
    
    try:
        # Initialize components
        spotify_client = SpotifyClient()
        s3_client = S3Client()
        classifier = IntegratedGenreClassifier(spotify_client, s3_client)
        
        # Process artists in batches
        batch_size = 25  # Smaller batches for better control
        total_processed = 0
        
        for i in range(0, len(artist_ids), batch_size):
            batch = artist_ids[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(artist_ids) + batch_size - 1) // batch_size
            
            print(f"\n📦 Processing batch {batch_num}/{total_batches} ({len(batch)} artists)")
            
            # Get enhanced artist data
            enhanced_artists = classifier.process_artist_batch_comprehensive(batch)
            
            if enhanced_artists:
                # Transform for Snowflake
                transformed_artists = []
                for artist in enhanced_artists:
                    transformed = spotify_client.transform_artist_data(artist, enhance_empty_genres=False)
                    transformed_artists.append(transformed)
                
                # Upload to S3
                from artist_genre_processor import ArtistGenreProcessor
                processor = ArtistGenreProcessor(spotify_client, s3_client)
                s3_key = processor.upload_artists_to_s3(transformed_artists)
                
                print(f"✅ Batch {batch_num}: Processed {len(enhanced_artists)} artists")
                print(f"📤 Uploaded to S3: {s3_key}")
                
                # Show sample results
                for artist in enhanced_artists[:3]:  # Show first 3 of each batch
                    name = artist.get('name', 'Unknown')
                    genres = artist.get('genres', [])
                    methods = artist.get('genre_inference_methods', [])
                    print(f"   • {name}: {genres} (via {methods})")
                
                total_processed += len(enhanced_artists)
            else:
                print(f"⚠️ Batch {batch_num}: No artists processed")
        
        print(f"\n🎉 Comprehensive processing complete!")
        print(f"📊 Total artists processed: {total_processed}")
        print(f"🔄 Next step: Refresh your Snowflake dynamic tables")
        
        return total_processed
        
    except Exception as e:
        logger.error("Failed comprehensive artist processing", error=str(e))
        print(f"❌ Processing failed: {e}")
        return 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python process_missing_artists_comprehensive.py <comma_separated_artist_ids>")
        print("\nExample:")
        print("python process_missing_artists_comprehensive.py '4Z8W4fKeB5YxbusRsdQVPb,0TnOYISbd1XYRBk9myaseg'")
        sys.exit(1)
    
    artist_ids_str = sys.argv[1]
    processed_count = process_missing_artists_from_ids(artist_ids_str)
    
    if processed_count > 0:
        print(f"\n✅ Successfully processed {processed_count} artists!")
        print("🔄 Run your Snowflake refresh script to see the updated data.")
    else:
        print("\n❌ No artists were processed successfully.")
        sys.exit(1)
