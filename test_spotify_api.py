#!/usr/bin/env python3
"""Simple test script to debug Spotify API issues"""

import os
from spotify_client import SpotifyClient
import structlog

logger = structlog.get_logger(__name__)

def test_spotify_api():
    """Test basic Spotify API functionality"""
    print("🔧 Testing Spotify API...")
    
    try:
        # Initialize client
        client = SpotifyClient()
        print("✅ Spotify client initialized")
        
        # Test 1: Basic search (should work)
        print("\n🔍 Test 1: Artist Search")
        try:
            results = client.search("Ed Sheeran", "artist", limit=1)
            if results and 'artists' in results and results['artists']['items']:
                artist = results['artists']['items'][0]
                print(f"✅ Found artist: {artist['name']} (ID: {artist['id']})")
                artist_id = artist['id']
            else:
                print("❌ No artist found")
                return
        except Exception as e:
            print(f"❌ Search failed: {e}")
            return
            
        # Test 2: Simple recommendations (no complex parameters)
        print("\n🎵 Test 2: Simple Recommendations")
        try:
            simple_recs = client.sp.recommendations(
                seed_artists=[artist_id],
                limit=5
            )
            print(f"✅ Got {len(simple_recs['tracks'])} recommendations")
            for track in simple_recs['tracks'][:2]:
                print(f"   - {track['name']} by {track['artists'][0]['name']}")
        except Exception as e:
            print(f"❌ Simple recommendations failed: {e}")
            
        # Test 3: Genre-based recommendations
        print("\n🎸 Test 3: Genre Recommendations")
        try:
            genre_recs = client.sp.recommendations(
                seed_genres=['rock'],
                limit=3
            )
            print(f"✅ Got {len(genre_recs['tracks'])} genre recommendations")
            for track in genre_recs['tracks']:
                print(f"   - {track['name']} by {track['artists'][0]['name']}")
        except Exception as e:
            print(f"❌ Genre recommendations failed: {e}")
            
        # Test 4: Available genres
        print("\n📋 Test 4: Available Genres")
        try:
            available_genres = client.sp.recommendation_genre_seeds()
            print(f"✅ Found {len(available_genres['genres'])} available genres")
            print(f"   First 10: {available_genres['genres'][:10]}")
        except Exception as e:
            print(f"❌ Genre seeds failed: {e}")
            
    except Exception as e:
        print(f"❌ Failed to initialize Spotify client: {e}")

if __name__ == "__main__":
    test_spotify_api()
