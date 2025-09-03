#!/usr/bin/env python3
"""
Script to populate missing artists in the spotify_artist_genres table.
This script extracts missing artist IDs from Snowflake and processes them through the pipeline.
"""

import json
import sys
from typing import List, Set
import structlog
from spotify_client import SpotifyClient
from s3_client import S3Client
from artist_genre_processor import ArtistGenreProcessor

logger = structlog.get_logger(__name__)

def get_missing_artist_ids_from_query_result(query_result_file: str) -> List[str]:
    """
    Extract artist IDs from a JSON file containing query results.
    
    Args:
        query_result_file: Path to JSON file with missing artists query results
        
    Returns:
        List of missing artist IDs
    """
    try:
        with open(query_result_file, 'r') as f:
            results = json.load(f)
        
        artist_ids = []
        for row in results:
            artist_id = row.get('PRIMARY_ARTIST_ID')
            if artist_id:
                artist_ids.append(artist_id)
        
        logger.info("Extracted missing artist IDs", count=len(artist_ids))
        return artist_ids
        
    except Exception as e:
        logger.error("Failed to extract artist IDs from query result", error=str(e))
        return []

def populate_missing_artists_from_list(artist_ids: List[str]) -> int:
    """
    Process a list of artist IDs to populate their genre data.
    
    Args:
        artist_ids: List of Spotify artist IDs to process
        
    Returns:
        Number of artists successfully processed
    """
    if not artist_ids:
        logger.warning("No artist IDs provided")
        return 0
    
    try:
        # Initialize clients
        spotify_client = SpotifyClient()
        s3_client = S3Client()
        processor = ArtistGenreProcessor(spotify_client, s3_client)
        
        # Process artists in batches
        batch_size = 50  # Spotify API limit
        total_processed = 0
        
        for i in range(0, len(artist_ids), batch_size):
            batch = artist_ids[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}", 
                       batch_size=len(batch), 
                       total_batches=(len(artist_ids) + batch_size - 1) // batch_size)
            
            try:
                # Process the batch
                processed_artists = processor.process_artist_batch(batch)
                
                if processed_artists:
                    # Upload to S3
                    s3_key = processor.upload_artists_to_s3(processed_artists)
                    logger.info("Uploaded artist batch to S3", 
                               s3_key=s3_key, 
                               artist_count=len(processed_artists))
                    total_processed += len(processed_artists)
                
            except Exception as e:
                logger.error(f"Failed to process batch {i//batch_size + 1}", error=str(e))
                continue
        
        logger.info("Missing artist population completed", 
                   total_requested=len(artist_ids),
                   total_processed=total_processed)
        
        return total_processed
        
    except Exception as e:
        logger.error("Failed to populate missing artists", error=str(e))
        return 0

def populate_missing_artists_from_ids(artist_ids_str: str) -> int:
    """
    Process artist IDs from a comma-separated string.
    
    Args:
        artist_ids_str: Comma-separated string of artist IDs
        
    Returns:
        Number of artists successfully processed
    """
    artist_ids = [aid.strip() for aid in artist_ids_str.split(',') if aid.strip()]
    return populate_missing_artists_from_list(artist_ids)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage:")
        print("  python populate_missing_artists.py <comma_separated_artist_ids>")
        print("  python populate_missing_artists.py query_results.json")
        print("")
        print("Examples:")
        print("  python populate_missing_artists.py '4Z8W4fKeB5YxbusRsdQVPb,0TnOYISbd1XYRBk9myaseg'")
        print("  python populate_missing_artists.py missing_artists.json")
        sys.exit(1)
    
    input_arg = sys.argv[1]
    
    # Check if it's a file or comma-separated IDs
    if input_arg.endswith('.json'):
        # File input
        artist_ids = get_missing_artist_ids_from_query_result(input_arg)
        processed_count = populate_missing_artists_from_list(artist_ids)
    else:
        # Comma-separated IDs
        processed_count = populate_missing_artists_from_ids(input_arg)
    
    print(f"\n✅ Successfully processed {processed_count} missing artists!")
    print("🔄 Run your Snowflake refresh script to see the updated data.")
