#!/usr/bin/env python3
"""
Enhanced Genre Processor with fallback strategies for artists with empty genres.
Uses multiple data sources and inference techniques to assign genres.
"""

import json
import re
from typing import List, Dict, Optional, Set
import structlog
from spotify_client import SpotifyClient

logger = structlog.get_logger(__name__)

class EnhancedGenreProcessor:
    """Enhanced processor that uses multiple strategies to determine artist genres."""
    
    def __init__(self, spotify_client: SpotifyClient):
        self.spotify_client = spotify_client
        
        # Common genre mappings based on artist name patterns, popularity, and followers
        self.genre_inference_rules = {
            # Electronic/EDM patterns
            'electronic': ['dj ', 'dj_', 'electronic', 'edm', 'house', 'techno', 'trance'],
            'hip hop': ['lil ', 'young ', 'big ', 'rapper', 'mc ', 'hip hop', 'rap'],
            'rock': ['band', 'rock', 'metal', 'punk'],
            'pop': ['pop', 'mainstream'],
            'indie': ['indie', 'alternative'],
            'country': ['country', 'nashville'],
            'jazz': ['jazz', 'blues'],
            'classical': ['orchestra', 'symphony', 'classical'],
            'latin': ['latin', 'spanish', 'reggaeton'],
            'r&b': ['r&b', 'soul', 'rnb']
        }
        
        # Popularity-based genre inference
        self.popularity_genre_mapping = {
            (80, 100): 'mainstream pop',  # Very high popularity
            (60, 79): 'pop',              # High popularity  
            (40, 59): 'alternative',      # Medium popularity
            (20, 39): 'indie',            # Lower popularity
            (0, 19): 'underground'        # Very low popularity
        }
    
    def infer_genre_from_name(self, artist_name: str) -> Optional[str]:
        """
        Infer genre from artist name patterns.
        
        Args:
            artist_name: The artist's name
            
        Returns:
            Inferred genre or None
        """
        if not artist_name:
            return None
            
        name_lower = artist_name.lower()
        
        # Check for common patterns
        for genre, patterns in self.genre_inference_rules.items():
            for pattern in patterns:
                if pattern in name_lower:
                    logger.info("Inferred genre from name", 
                               artist=artist_name, 
                               pattern=pattern, 
                               genre=genre)
                    return genre
        
        return None
    
    def infer_genre_from_popularity(self, popularity: int, followers: int) -> Optional[str]:
        """
        Infer genre based on popularity and follower count.
        
        Args:
            popularity: Spotify popularity score (0-100)
            followers: Number of followers
            
        Returns:
            Inferred genre or None
        """
        if popularity is None:
            return None
            
        # Find appropriate popularity range
        for (min_pop, max_pop), genre in self.popularity_genre_mapping.items():
            if min_pop <= popularity <= max_pop:
                # Adjust based on follower count
                if followers and followers > 1000000:  # 1M+ followers
                    if genre == 'alternative':
                        return 'pop'
                    elif genre == 'indie':
                        return 'alternative'
                
                logger.info("Inferred genre from popularity", 
                           popularity=popularity, 
                           followers=followers, 
                           genre=genre)
                return genre
        
        return None
    
    def get_related_artists_genres(self, artist_id: str) -> List[str]:
        """
        Get genres from related artists (requires additional API call).
        
        Args:
            artist_id: Spotify artist ID
            
        Returns:
            List of genres from related artists
        """
        try:
            # This would require implementing get_related_artists in SpotifyClient
            # For now, return empty list
            logger.info("Related artists genre lookup not implemented yet", artist_id=artist_id)
            return []
        except Exception as e:
            logger.error("Failed to get related artists", artist_id=artist_id, error=str(e))
            return []
    
    def enhance_artist_with_inferred_genres(self, artist_data: Dict) -> Dict:
        """
        Enhance artist data with inferred genres when original genres are empty.
        
        Args:
            artist_data: Original artist data from Spotify API
            
        Returns:
            Enhanced artist data with inferred genres
        """
        original_genres = artist_data.get("genres", [])
        
        # If we already have genres, return as-is
        if original_genres:
            return artist_data
        
        # Try to infer genres using various strategies
        inferred_genres = []
        inference_methods = []
        
        # Strategy 1: Name-based inference
        name_genre = self.infer_genre_from_name(artist_data.get("name", ""))
        if name_genre:
            inferred_genres.append(name_genre)
            inference_methods.append("name_pattern")
        
        # Strategy 2: Popularity-based inference
        popularity = artist_data.get("popularity")
        followers = artist_data.get("followers", {}).get("total")
        popularity_genre = self.infer_genre_from_popularity(popularity, followers)
        if popularity_genre and popularity_genre not in inferred_genres:
            inferred_genres.append(popularity_genre)
            inference_methods.append("popularity_analysis")
        
        # Strategy 3: Related artists (future enhancement)
        # related_genres = self.get_related_artists_genres(artist_data.get("id"))
        # inferred_genres.extend([g for g in related_genres if g not in inferred_genres])
        
        # If we inferred any genres, update the artist data
        if inferred_genres:
            enhanced_data = artist_data.copy()
            enhanced_data["genres"] = inferred_genres
            enhanced_data["genre_inference_methods"] = inference_methods
            enhanced_data["original_genres_empty"] = True
            
            logger.info("Enhanced artist with inferred genres",
                       artist_name=artist_data.get("name"),
                       artist_id=artist_data.get("id"),
                       inferred_genres=inferred_genres,
                       methods=inference_methods)
            
            return enhanced_data
        
        # If no genres could be inferred, add a generic classification
        enhanced_data = artist_data.copy()
        enhanced_data["genres"] = ["unclassified"]
        enhanced_data["genre_inference_methods"] = ["fallback"]
        enhanced_data["original_genres_empty"] = True
        
        logger.warning("Could not infer genres for artist",
                      artist_name=artist_data.get("name"),
                      artist_id=artist_data.get("id"),
                      popularity=popularity,
                      followers=followers)
        
        return enhanced_data

def enhance_artist_batch_with_genres(raw_artists: List[Dict], spotify_client: SpotifyClient) -> List[Dict]:
    """
    Enhance a batch of artists with inferred genres where needed.
    
    Args:
        raw_artists: List of raw artist data from Spotify API
        spotify_client: Spotify client instance
        
    Returns:
        List of enhanced artist data
    """
    processor = EnhancedGenreProcessor(spotify_client)
    enhanced_artists = []
    
    empty_genre_count = 0
    enhanced_count = 0
    
    for artist in raw_artists:
        if not artist:
            continue
            
        original_genres = artist.get("genres", [])
        
        if not original_genres:  # Empty genres array
            empty_genre_count += 1
            enhanced_artist = processor.enhance_artist_with_inferred_genres(artist)
            
            # Check if we successfully added genres
            if enhanced_artist.get("genres") and enhanced_artist["genres"] != ["unclassified"]:
                enhanced_count += 1
                
            enhanced_artists.append(enhanced_artist)
        else:
            enhanced_artists.append(artist)
    
    logger.info("Enhanced artist batch",
               total_artists=len(raw_artists),
               empty_genre_artists=empty_genre_count,
               successfully_enhanced=enhanced_count)
    
    return enhanced_artists

if __name__ == "__main__":
    # Example usage
    spotify_client = SpotifyClient()
    
    # Example artist with empty genres (like your Knox example)
    example_artist = {
        "id": "61S5H9Lxn1PDUvu1TV0kCX",
        "name": "Knox",
        "popularity": 60,
        "followers": {"total": 155617},
        "genres": []
    }
    
    processor = EnhancedGenreProcessor(spotify_client)
    enhanced = processor.enhance_artist_with_inferred_genres(example_artist)
    
    print("Original:", example_artist.get("genres"))
    print("Enhanced:", enhanced.get("genres"))
    print("Methods:", enhanced.get("genre_inference_methods"))
