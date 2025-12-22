#!/usr/bin/env python3
"""
Every Noise Dataset Genre Classifier
Uses the Every Noise dataset and site structure for genre classification.
"""

import requests
import json
import time
import re
import csv
from typing import Dict, List, Optional, Set
from urllib.parse import quote, unquote
import structlog
from bs4 import BeautifulSoup
from difflib import SequenceMatcher

logger = structlog.get_logger(__name__)

class EveryNoiseDatasetClassifier:
    """Classifier using Every Noise dataset and site structure."""
    
    def __init__(self):
        self.base_url = "https://everynoise.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        # Cache for mappings
        self.artist_genre_cache = {}
        self.genre_data = {}
        
        logger.info("Every Noise Dataset Classifier initialized")
    
    def download_everynoise_dataset(self) -> bool:
        """
        Download the Every Noise dataset from GitHub.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Try to get the dataset from EveryNoise-Watch repository
            dataset_url = "https://raw.githubusercontent.com/AyrtonB/EveryNoise-Watch/main/data/everynoise_genres.csv"
            
            response = self.session.get(dataset_url, timeout=30)
            response.raise_for_status()
            
            # Save the dataset locally
            with open('everynoise_genres.csv', 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            logger.info("Downloaded Every Noise dataset successfully")
            return True
            
        except Exception as e:
            logger.error("Failed to download Every Noise dataset", error=str(e))
            return False
    
    def load_everynoise_dataset(self) -> bool:
        """
        Load the Every Noise dataset from local file.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with open('everynoise_genres.csv', 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    genre = row.get('genre', '').strip()
                    if genre:
                        self.genre_data[genre] = row
            
            logger.info("Loaded Every Noise dataset", genres_count=len(self.genre_data))
            return True
            
        except FileNotFoundError:
            logger.warning("Every Noise dataset file not found, attempting download")
            if self.download_everynoise_dataset():
                return self.load_everynoise_dataset()
            return False
        except Exception as e:
            logger.error("Failed to load Every Noise dataset", error=str(e))
            return False
    
    def search_artist_by_genre_exploration(self, artist_name: str) -> Optional[str]:
        """
        Search for artist by exploring genre pages on Every Noise.
        
        Args:
            artist_name: Name of the artist to search for
            
        Returns:
            Genre if found, None otherwise
        """
        if not artist_name:
            return None
            
        # Check cache first
        cache_key = artist_name.lower().strip()
        if cache_key in self.artist_genre_cache:
            return self.artist_genre_cache[cache_key]
        
        try:
            # Try the main site with artist search
            search_url = f"{self.base_url}/engenremap.html"
            
            response = self.session.get(search_url, timeout=15)
            response.raise_for_status()
            
            # Look for artist in the main genre map
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all genre links
            genre_links = soup.find_all('a', href=True)
            
            # Try a few promising genres based on common patterns
            candidate_genres = self._get_candidate_genres_for_artist(artist_name)
            
            for genre in candidate_genres:
                found_genre = self._check_artist_in_genre(artist_name, genre)
                if found_genre:
                    self.artist_genre_cache[cache_key] = found_genre
                    return found_genre
                    
                # Rate limiting
                time.sleep(0.5)
            
            # If not found in candidate genres, return None
            self.artist_genre_cache[cache_key] = None
            return None
            
        except Exception as e:
            logger.error("Failed to search artist on Every Noise", artist=artist_name, error=str(e))
            return None
    
    def _get_candidate_genres_for_artist(self, artist_name: str) -> List[str]:
        """Get candidate genres to check for an artist based on name patterns."""
        candidates = []
        name_lower = artist_name.lower()
        
        # Genre inference based on artist name patterns
        if any(pattern in name_lower for pattern in ['dj ', 'dj_', 'electronic']):
            candidates.extend(['electronic', 'house', 'techno', 'edm', 'dance'])
        elif any(pattern in name_lower for pattern in ['lil ', 'young ', 'big ', 'mc ']):
            candidates.extend(['hip hop', 'rap', 'trap', 'hip-hop'])
        elif 'band' in name_lower or 'rock' in name_lower:
            candidates.extend(['rock', 'indie rock', 'alternative rock', 'pop rock'])
        elif 'pop' in name_lower:
            candidates.extend(['pop', 'indie pop', 'electropop'])
        else:
            # Default candidates for unknown artists
            candidates.extend(['pop', 'indie', 'alternative', 'rock', 'electronic', 'hip hop'])
        
        return candidates[:5]  # Limit to top 5 candidates
    
    def _check_artist_in_genre(self, artist_name: str, genre: str) -> Optional[str]:
        """
        Check if an artist appears in a specific genre page.
        
        Args:
            artist_name: Name of the artist
            genre: Genre to check
            
        Returns:
            Genre if artist found, None otherwise
        """
        try:
            # Format genre for URL
            genre_url_safe = quote(genre.replace(' ', ''))
            genre_page_url = f"{self.base_url}/engenremap-{genre_url_safe}.html"
            
            response = self.session.get(genre_page_url, timeout=10)
            
            # If page doesn't exist, try alternative format
            if response.status_code == 404:
                genre_url_safe = quote(genre.replace(' ', '_'))
                genre_page_url = f"{self.base_url}/engenremap-{genre_url_safe}.html"
                response = self.session.get(genre_page_url, timeout=10)
            
            if response.status_code != 200:
                return None
            
            # Parse the genre page
            soup = BeautifulSoup(response.text, 'html.parser')
            page_text = soup.get_text().lower()
            
            # Look for artist name in the page
            artist_lower = artist_name.lower()
            
            # Direct match
            if artist_lower in page_text:
                logger.info("Found artist in genre page", artist=artist_name, genre=genre)
                return genre
            
            # Fuzzy matching for similar names
            words = page_text.split()
            for word in words:
                if len(word) > 3 and SequenceMatcher(None, artist_lower, word).ratio() > 0.8:
                    logger.info("Found similar artist in genre page", 
                               artist=artist_name, similar_word=word, genre=genre)
                    return genre
            
            return None
            
        except Exception as e:
            logger.debug("Failed to check artist in genre", artist=artist_name, genre=genre, error=str(e))
            return None
    
    def classify_artists_with_everynoise(self, artists_data: List[Dict]) -> List[Dict]:
        """
        Classify artists using Every Noise data.
        
        Args:
            artists_data: List of artist data dictionaries
            
        Returns:
            Enhanced artist data with Every Noise genres
        """
        enhanced_artists = []
        processed_count = 0
        found_count = 0
        
        for artist in artists_data:
            if not artist:
                continue
                
            artist_name = artist.get('name', '')
            original_genres = artist.get('genres', [])
            
            # If artist already has genres, keep them
            if original_genres:
                enhanced_artists.append(artist)
                continue
            
            # Try to find genre using Every Noise
            everynoise_genre = self.search_artist_by_genre_exploration(artist_name)
            
            if everynoise_genre:
                enhanced_artist = artist.copy()
                enhanced_artist['genres'] = [everynoise_genre]
                enhanced_artist['genre_inference_methods'] = ['everynoise_exploration']
                enhanced_artist['original_genres_empty'] = True
                
                logger.info("Enhanced artist with Every Noise genre",
                           artist_name=artist_name,
                           genre=everynoise_genre)
                
                enhanced_artists.append(enhanced_artist)
                found_count += 1
            else:
                # No genre found, keep original
                enhanced_artists.append(artist)
            
            processed_count += 1
            
            # Rate limiting - be respectful
            if processed_count % 5 == 0:
                time.sleep(2.0)
        
        logger.info("Completed Every Noise classification",
                   total_processed=processed_count,
                   genres_found=found_count,
                   success_rate=f"{(found_count/processed_count)*100:.1f}%" if processed_count > 0 else "0%")
        
        return enhanced_artists

def test_everynoise_dataset_classifier():
    """Test the Every Noise dataset classifier."""
    classifier = EveryNoiseDatasetClassifier()
    
    # Test with sample artists
    test_artists = [
        {"name": "Knox", "genres": []},
        {"name": "Daft Punk", "genres": []},
        {"name": "Taylor Swift", "genres": []},
        {"name": "DJ Shadow", "genres": []},
        {"name": "Lil Wayne", "genres": []}
    ]
    
    print("Testing Every Noise Dataset Classifier:")
    print("=" * 50)
    
    enhanced = classifier.classify_artists_with_everynoise(test_artists)
    
    for original, enhanced_artist in zip(test_artists, enhanced):
        original_genres = original.get('genres', [])
        new_genres = enhanced_artist.get('genres', [])
        methods = enhanced_artist.get('genre_inference_methods', [])
        
        print(f"{original['name']}:")
        print(f"  Original: {original_genres}")
        print(f"  Enhanced: {new_genres}")
        print(f"  Method: {methods}")
        print()

if __name__ == "__main__":
    test_everynoise_dataset_classifier()
