#!/usr/bin/env python3
"""
Every Noise at Once Genre Classifier
Uses everynoise.com data to classify artists with missing genres.
"""

import requests
import json
import time
import re
from typing import Dict, List, Optional, Set
from urllib.parse import quote
import structlog
from bs4 import BeautifulSoup
from spotify_client import SpotifyClient
from s3_client import S3Client

logger = structlog.get_logger(__name__)

class EveryNoiseGenreClassifier:
    """Classifier that uses Every Noise at Once data for genre assignment."""
    
    def __init__(self):
        self.base_url = "https://everynoise.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        # Cache for genre mappings
        self.artist_genre_cache = {}
        self.genre_artist_cache = {}
        
        logger.info("Every Noise Genre Classifier initialized")
    
    def search_artist_on_everynoise(self, artist_name: str) -> Optional[str]:
        """
        Search for an artist on Every Noise at Once.
        
        Args:
            artist_name: Name of the artist to search for
            
        Returns:
            Genre if found, None otherwise
        """
        if not artist_name:
            return None
            
        # Check cache first
        if artist_name.lower() in self.artist_genre_cache:
            return self.artist_genre_cache[artist_name.lower()]
        
        try:
            # Clean artist name for search
            clean_name = re.sub(r'[^\w\s-]', '', artist_name).strip()
            
            # Try the search endpoint
            search_url = f"{self.base_url}/search.cgi"
            params = {'q': clean_name}
            
            response = self.session.get(search_url, params=params, timeout=10)
            response.raise_for_status()
            
            # Parse the response
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for artist links and associated genres
            genre = self._extract_genre_from_search_results(soup, artist_name)
            
            # Cache the result
            self.artist_genre_cache[artist_name.lower()] = genre
            
            if genre:
                logger.info("Found genre on Every Noise", artist=artist_name, genre=genre)
            else:
                logger.debug("No genre found on Every Noise", artist=artist_name)
            
            return genre
            
        except Exception as e:
            logger.error("Failed to search Every Noise", artist=artist_name, error=str(e))
            return None
    
    def _extract_genre_from_search_results(self, soup: BeautifulSoup, artist_name: str) -> Optional[str]:
        """Extract genre from Every Noise search results."""
        try:
            # Look for artist entries in the search results
            # Every Noise typically shows results as links with genre context
            
            # Method 1: Look for direct artist matches
            links = soup.find_all('a', href=True)
            for link in links:
                link_text = link.get_text().strip().lower()
                if artist_name.lower() in link_text:
                    # Try to extract genre from the link or surrounding context
                    href = link.get('href', '')
                    if 'genre=' in href:
                        genre_match = re.search(r'genre=([^&]+)', href)
                        if genre_match:
                            genre = genre_match.group(1).replace('%20', ' ')
                            return self._clean_genre_name(genre)
            
            # Method 2: Look for genre context in the page
            text_content = soup.get_text()
            genre_patterns = [
                r'(\w+(?:\s+\w+)*)\s*:\s*.*' + re.escape(artist_name.lower()),
                r'genre:\s*(\w+(?:\s+\w+)*)',
                r'style:\s*(\w+(?:\s+\w+)*)'
            ]
            
            for pattern in genre_patterns:
                match = re.search(pattern, text_content.lower())
                if match:
                    potential_genre = match.group(1).strip()
                    if self._is_valid_genre(potential_genre):
                        return self._clean_genre_name(potential_genre)
            
            return None
            
        except Exception as e:
            logger.error("Failed to extract genre from search results", error=str(e))
            return None
    
    def _clean_genre_name(self, genre: str) -> str:
        """Clean and normalize genre names."""
        if not genre:
            return None
            
        # Remove URL encoding
        genre = genre.replace('%20', ' ').replace('+', ' ')
        
        # Clean up common patterns
        genre = re.sub(r'[^\w\s-]', '', genre)
        genre = ' '.join(genre.split())  # Normalize whitespace
        
        # Map common Every Noise genres to standard genres
        genre_mappings = {
            'indie rock': 'indie',
            'alternative rock': 'alternative',
            'hip hop': 'hip-hop',
            'electronic dance': 'electronic',
            'pop rock': 'pop',
            'folk rock': 'folk',
            'country rock': 'country',
            'jazz fusion': 'jazz',
            'classical crossover': 'classical',
            'latin pop': 'latin',
            'rhythm and blues': 'r-b'
        }
        
        genre_lower = genre.lower()
        return genre_mappings.get(genre_lower, genre.lower())
    
    def _is_valid_genre(self, genre: str) -> bool:
        """Check if a string looks like a valid genre."""
        if not genre or len(genre) < 2 or len(genre) > 50:
            return False
            
        # Should contain mostly letters and spaces
        if not re.match(r'^[a-zA-Z\s-]+$', genre):
            return False
            
        # Shouldn't be common non-genre words
        invalid_words = {'the', 'and', 'or', 'of', 'in', 'on', 'at', 'to', 'for', 'with', 'by'}
        if genre.lower().strip() in invalid_words:
            return False
            
        return True
    
    def get_genre_for_artist_batch(self, artist_names: List[str]) -> Dict[str, Optional[str]]:
        """
        Get genres for a batch of artists from Every Noise.
        
        Args:
            artist_names: List of artist names
            
        Returns:
            Dictionary mapping artist names to genres
        """
        results = {}
        
        for i, artist_name in enumerate(artist_names):
            if not artist_name:
                continue
                
            genre = self.search_artist_on_everynoise(artist_name)
            results[artist_name] = genre
            
            # Rate limiting - be respectful to Every Noise
            if i < len(artist_names) - 1:
                time.sleep(1.0)  # 1 second between requests
        
        logger.info("Completed Every Noise batch lookup", 
                   total_artists=len(artist_names),
                   found_genres=sum(1 for g in results.values() if g))
        
        return results
    
    def enhance_artists_with_everynoise_genres(self, artists_data: List[Dict]) -> List[Dict]:
        """
        Enhance artist data with Every Noise genres.
        
        Args:
            artists_data: List of artist data dictionaries
            
        Returns:
            Enhanced artist data with Every Noise genres where found
        """
        enhanced_artists = []
        artist_names = [artist.get('name', '') for artist in artists_data if artist.get('name')]
        
        # Get genres from Every Noise
        everynoise_genres = self.get_genre_for_artist_batch(artist_names)
        
        for artist in artists_data:
            if not artist:
                continue
                
            artist_name = artist.get('name', '')
            original_genres = artist.get('genres', [])
            
            # If artist already has genres, keep them
            if original_genres:
                enhanced_artists.append(artist)
                continue
            
            # Try to get genre from Every Noise
            everynoise_genre = everynoise_genres.get(artist_name)
            
            if everynoise_genre:
                enhanced_artist = artist.copy()
                enhanced_artist['genres'] = [everynoise_genre]
                enhanced_artist['genre_inference_methods'] = ['everynoise_lookup']
                enhanced_artist['original_genres_empty'] = True
                
                logger.info("Enhanced artist with Every Noise genre",
                           artist_name=artist_name,
                           genre=everynoise_genre)
                
                enhanced_artists.append(enhanced_artist)
            else:
                # No genre found, keep original
                enhanced_artists.append(artist)
        
        return enhanced_artists

def test_everynoise_classifier():
    """Test the Every Noise classifier with sample artists."""
    classifier = EveryNoiseGenreClassifier()
    
    # Test with some known artists
    test_artists = [
        "Knox",
        "Taylor Swift", 
        "Radiohead",
        "Daft Punk",
        "Miles Davis"
    ]
    
    print("Testing Every Noise Genre Classifier:")
    print("=" * 50)
    
    for artist in test_artists:
        genre = classifier.search_artist_on_everynoise(artist)
        print(f"{artist}: {genre or 'Not found'}")
        time.sleep(1)  # Rate limiting

if __name__ == "__main__":
    test_everynoise_classifier()
