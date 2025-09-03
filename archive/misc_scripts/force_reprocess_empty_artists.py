#!/usr/bin/env python3
"""
Force reprocess artists that currently have empty genres in Snowflake.
This ensures they get enhanced genre data uploaded to S3.
"""

import sys
from typing import List
from integrated_genre_classifier import IntegratedGenreClassifier
from spotify_client import SpotifyClient
from s3_client import S3Client
from artist_genre_processor import ArtistGenreProcessor
import structlog

logger = structlog.get_logger(__name__)

def force_reprocess_artists(artist_ids: List[str]) -> int:
    """
    Force reprocess specific artists, bypassing the "already processed" state.
    
    Args:
        artist_ids: List of artist IDs to reprocess
        
    Returns:
        Number of artists successfully processed
    """
    if not artist_ids:
        print("❌ No artist IDs provided")
        return 0
    
    print(f"🔄 Force reprocessing {len(artist_ids)} artists with enhanced genres")
    
    try:
        # Initialize components
        spotify_client = SpotifyClient()
        s3_client = S3Client()
        classifier = IntegratedGenreClassifier(spotify_client, s3_client)
        processor = ArtistGenreProcessor(spotify_client, s3_client)
        
        # Remove these artists from the "processed" state so they get reprocessed
        for artist_id in artist_ids:
            if artist_id in processor.processed_artists:
                processor.processed_artists.remove(artist_id)
                print(f"🗑️ Removed {artist_id} from processed state")
        
        # Save the updated state
        processor._save_processed_artists()
        
        # Process in batches
        batch_size = 25
        total_processed = 0
        
        for i in range(0, len(artist_ids), batch_size):
            batch = artist_ids[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(artist_ids) + batch_size - 1) // batch_size
            
            print(f"\n📦 Processing batch {batch_num}/{total_batches} ({len(batch)} artists)")
            
            # Get enhanced artist data using comprehensive classification
            enhanced_artists = classifier.process_artist_batch_comprehensive(batch)
            
            if enhanced_artists:
                # Transform for Snowflake (disable additional enhancement since we already did it)
                transformed_artists = []
                for artist in enhanced_artists:
                    transformed = spotify_client.transform_artist_data(artist, enhance_empty_genres=False)
                    transformed_artists.append(transformed)
                
                # Upload to S3
                s3_key = processor.upload_artists_to_s3(transformed_artists)
                
                print(f"✅ Batch {batch_num}: Processed {len(enhanced_artists)} artists")
                print(f"📤 Uploaded to S3: {s3_key}")
                
                # Show sample results
                for artist in enhanced_artists[:3]:
                    name = artist.get('name', 'Unknown')
                    genres = artist.get('genres', [])
                    methods = artist.get('genre_inference_methods', [])
                    print(f"   • {name}: {genres} (via {methods})")
                
                total_processed += len(enhanced_artists)
            else:
                print(f"⚠️ Batch {batch_num}: No artists processed")
        
        print(f"\n🎉 Force reprocessing complete!")
        print(f"📊 Total artists reprocessed: {total_processed}")
        
        return total_processed
        
    except Exception as e:
        logger.error("Failed force reprocessing", error=str(e))
        print(f"❌ Force reprocessing failed: {e}")
        return 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python force_reprocess_empty_artists.py <comma_separated_artist_ids>")
        print("\nExample:")
        print("python force_reprocess_empty_artists.py '4Z8W4fKeB5YxbusRsdQVPb,0TnOYISbd1XYRBk9myaseg'")
        sys.exit(1)
    
    artist_ids_str = sys.argv[1]
    artist_ids = [aid.strip() for aid in artist_ids_str.split(',') if aid.strip()]
    
    processed_count = force_reprocess_artists(artist_ids)
    
    if processed_count > 0:
        print(f"\n✅ Successfully reprocessed {processed_count} artists!")
        print("🔄 Now run the clean_and_repopulate_artists.sql script in Snowflake.")
    else:
        print("\n❌ No artists were reprocessed successfully.")
        sys.exit(1)
