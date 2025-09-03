#!/usr/bin/env python3
"""
Integrated Genre Classifier
Combines multiple strategies: Spotify API, popularity analysis, name patterns, and Every Noise data.
"""

import json
import time
from typing import Dict, List, Optional
import structlog
from spotify_client import SpotifyClient
from s3_client import S3Client

logger = structlog.get_logger(__name__)

class IntegratedGenreClassifier:
    """Multi-strategy genre classifier for artists with missing genres."""
    
    def __init__(self, spotify_client: SpotifyClient, s3_client: S3Client):
        self.spotify_client = spotify_client
        self.s3_client = s3_client
        
        # Genre mapping from various sources
        self.comprehensive_genre_mapping = {
            # Electronic/EDM artists
            'daft punk': 'electronic',
            'deadmau5': 'electronic', 
            'skrillex': 'electronic',
            'calvin harris': 'electronic',
            'david guetta': 'electronic',
            'tiësto': 'electronic',
            'armin van buuren': 'electronic',
            'diplo': 'electronic',
            'flume': 'electronic',
            'odesza': 'electronic',
            
            # Hip-hop/Rap artists
            'lil wayne': 'hip hop',
            'drake': 'hip hop',
            'kendrick lamar': 'hip hop',
            'j. cole': 'hip hop',
            'travis scott': 'hip hop',
            'future': 'hip hop',
            'lil baby': 'hip hop',
            'dababy': 'hip hop',
            'migos': 'hip hop',
            'cardi b': 'hip hop',
            
            # Pop artists
            'taylor swift': 'pop',
            'ariana grande': 'pop',
            'billie eilish': 'pop',
            'dua lipa': 'pop',
            'the weeknd': 'pop',
            'ed sheeran': 'pop',
            'justin bieber': 'pop',
            'selena gomez': 'pop',
            'katy perry': 'pop',
            'lady gaga': 'pop',
            
            # Rock/Alternative
            'radiohead': 'alternative',
            'foo fighters': 'rock',
            'red hot chili peppers': 'rock',
            'arctic monkeys': 'indie',
            'the strokes': 'indie',
            'coldplay': 'alternative',
            'imagine dragons': 'pop rock',
            'twenty one pilots': 'alternative',
            'fall out boy': 'pop punk',
            'panic! at the disco': 'pop punk',
            
            # R&B/Soul
            'beyoncé': 'r&b',
            'rihanna': 'r&b',
            'the weeknd': 'r&b',
            'frank ocean': 'r&b',
            'sza': 'r&b',
            'h.e.r.': 'r&b',
            'john legend': 'r&b',
            'alicia keys': 'r&b',
            
            # Country
            'taylor swift': 'country',  # Early career
            'carrie underwood': 'country',
            'keith urban': 'country',
            'blake shelton': 'country',
            'luke bryan': 'country',
            'florida georgia line': 'country',
            
            # Jazz/Blues
            'miles davis': 'jazz',
            'john coltrane': 'jazz',
            'ella fitzgerald': 'jazz',
            'louis armstrong': 'jazz',
            'bb king': 'blues',
            'muddy waters': 'blues',
            
            # Latin
            'bad bunny': 'latin',
            'j balvin': 'latin',
            'ozuna': 'latin',
            'daddy yankee': 'latin',
            'maluma': 'latin',
            'shakira': 'latin',
            
            # Indie/Alternative specific
            'tame impala': 'indie',
            'vampire weekend': 'indie',
            'the national': 'indie',
            'bon iver': 'indie folk',
            'fleet foxes': 'indie folk',
            'arcade fire': 'indie',
        }
        
        logger.info("Integrated Genre Classifier initialized")
    
    def classify_artist_comprehensive(self, artist_data: Dict) -> Dict:
        """
        Comprehensive artist classification using multiple strategies.
        
        Args:
            artist_data: Artist data from Spotify API
            
        Returns:
            Enhanced artist data with genre classification
        """
        artist_name = artist_data.get('name', '').strip()
        original_genres = artist_data.get('genres', [])
        
        # If already has genres, return as-is
        if original_genres:
            return artist_data
        
        # Strategy 1: Direct mapping lookup
        direct_genre = self._get_direct_mapping_genre(artist_name)
        if direct_genre:
            return self._create_enhanced_artist(artist_data, [direct_genre], ['direct_mapping'])
        
        # Strategy 2: Enhanced name-based inference
        name_genre = self._enhanced_name_inference(artist_name)
        if name_genre:
            return self._create_enhanced_artist(artist_data, [name_genre], ['enhanced_name_pattern'])
        
        # Strategy 3: Popularity and follower analysis
        popularity_genre = self._popularity_analysis(artist_data)
        if popularity_genre:
            return self._create_enhanced_artist(artist_data, [popularity_genre], ['popularity_analysis'])
        
        # Strategy 4: Contextual inference (if we have additional context)
        contextual_genre = self._contextual_inference(artist_data)
        if contextual_genre:
            return self._create_enhanced_artist(artist_data, [contextual_genre], ['contextual_inference'])
        
        # Fallback: Assign based on popularity tier
        fallback_genre = self._fallback_classification(artist_data)
        return self._create_enhanced_artist(artist_data, [fallback_genre], ['fallback_classification'])
    
    def _get_direct_mapping_genre(self, artist_name: str) -> Optional[str]:
        """Get genre from direct artist mapping."""
        if not artist_name:
            return None
            
        name_key = artist_name.lower().strip()
        return self.comprehensive_genre_mapping.get(name_key)
    
    def _enhanced_name_inference(self, artist_name: str) -> Optional[str]:
        """Enhanced name-based genre inference."""
        if not artist_name:
            return None
            
        name_lower = artist_name.lower()
        
        # Enhanced patterns with more specificity
        patterns = {
            'electronic': [
                'dj ', 'dj_', 'dj-', 'electronic', 'edm', 'house', 'techno', 'trance',
                'dubstep', 'bass', 'beats', 'remix', 'mix', 'synth', 'digital'
            ],
            'hip hop': [
                'lil ', 'young ', 'big ', 'mc ', 'rapper', 'rap', 'hip hop', 'hiphop',
                'trap', 'drill', 'gang', 'mob', 'crew', 'squad'
            ],
            'rock': [
                'band', 'rock', 'metal', 'punk', 'hardcore', 'grunge', 'alternative rock'
            ],
            'pop': [
                'pop', 'mainstream', 'boy band', 'girl group'
            ],
            'indie': [
                'indie', 'alternative', 'underground', 'experimental'
            ],
            'country': [
                'country', 'nashville', 'bluegrass', 'folk country'
            ],
            'jazz': [
                'jazz', 'blues', 'swing', 'bebop', 'fusion'
            ],
            'classical': [
                'orchestra', 'symphony', 'classical', 'philharmonic', 'quartet', 'ensemble'
            ],
            'latin': [
                'latin', 'spanish', 'reggaeton', 'salsa', 'bachata', 'merengue'
            ],
            'r&b': [
                'r&b', 'soul', 'rnb', 'rhythm', 'blues', 'motown'
            ]
        }
        
        # Check patterns with scoring
        genre_scores = {}
        for genre, genre_patterns in patterns.items():
            score = 0
            for pattern in genre_patterns:
                if pattern in name_lower:
                    score += len(pattern)  # Longer matches get higher scores
            if score > 0:
                genre_scores[genre] = score
        
        # Return highest scoring genre
        if genre_scores:
            best_genre = max(genre_scores, key=genre_scores.get)
            logger.info("Enhanced name inference", artist=artist_name, genre=best_genre, score=genre_scores[best_genre])
            return best_genre
        
        return None
    
    def _popularity_analysis(self, artist_data: Dict) -> Optional[str]:
        """Enhanced popularity-based analysis."""
        popularity = artist_data.get('popularity')
        followers = artist_data.get('followers', {}).get('total', 0)
        
        if popularity is None:
            return None
        
        # More nuanced popularity-based classification
        if popularity >= 85:
            return 'mainstream pop'
        elif popularity >= 70:
            if followers and followers > 5000000:  # 5M+ followers
                return 'pop'
            else:
                return 'alternative'
        elif popularity >= 50:
            if followers and followers > 1000000:  # 1M+ followers
                return 'alternative'
            else:
                return 'indie'
        elif popularity >= 30:
            return 'indie'
        else:
            return 'underground'
    
    def _contextual_inference(self, artist_data: Dict) -> Optional[str]:
        """Contextual inference based on multiple factors."""
        popularity = artist_data.get('popularity', 0)
        followers = artist_data.get('followers', {}).get('total', 0)
        name = artist_data.get('name', '').lower()
        
        # Combine multiple signals
        if followers > 10000000 and popularity > 80:  # Mega-popular artists
            return 'pop'
        elif followers > 1000000 and popularity > 60:  # Popular artists
            if any(word in name for word in ['the ', 'band', 'group']):
                return 'rock'
            else:
                return 'pop'
        elif followers < 100000 and popularity < 40:  # Niche artists
            return 'indie'
        
        return None
    
    def _fallback_classification(self, artist_data: Dict) -> str:
        """Fallback classification when all else fails."""
        popularity = artist_data.get('popularity', 0)
        
        # Simple fallback based on popularity
        if popularity >= 60:
            return 'pop'
        elif popularity >= 40:
            return 'alternative'
        elif popularity >= 20:
            return 'indie'
        else:
            return 'unclassified'
    
    def _create_enhanced_artist(self, artist_data: Dict, genres: List[str], methods: List[str]) -> Dict:
        """Create enhanced artist data with inferred genres."""
        enhanced = artist_data.copy()
        enhanced['genres'] = genres
        enhanced['genre_inference_methods'] = methods
        enhanced['original_genres_empty'] = True
        
        return enhanced
    
    def process_artist_batch_comprehensive(self, artist_ids: List[str]) -> List[Dict]:
        """
        Process a batch of artists with comprehensive genre classification.
        
        Args:
            artist_ids: List of Spotify artist IDs
            
        Returns:
            List of enhanced artist data
        """
        if not artist_ids:
            return []
        
        try:
            # Get raw artist data from Spotify
            raw_artists = self.spotify_client.get_multiple_artists(artist_ids)
            
            # Enhance with comprehensive classification
            enhanced_artists = []
            for artist in raw_artists:
                if artist:
                    enhanced = self.classify_artist_comprehensive(artist)
                    enhanced_artists.append(enhanced)
            
            logger.info("Completed comprehensive artist classification",
                       requested=len(artist_ids),
                       processed=len(enhanced_artists))
            
            return enhanced_artists
            
        except Exception as e:
            logger.error("Failed comprehensive artist processing", error=str(e))
            return []

def test_integrated_classifier():
    """Test the integrated classifier."""
    from spotify_client import SpotifyClient
    from s3_client import S3Client
    
    spotify_client = SpotifyClient()
    s3_client = S3Client()
    classifier = IntegratedGenreClassifier(spotify_client, s3_client)
    
    # Test with sample artist data (simulated empty genres)
    test_artists = [
        {"name": "Knox", "genres": [], "popularity": 60, "followers": {"total": 155617}},
        {"name": "Daft Punk", "genres": [], "popularity": 85, "followers": {"total": 5000000}},
        {"name": "Lil Wayne", "genres": [], "popularity": 75, "followers": {"total": 8000000}},
        {"name": "Unknown Artist", "genres": [], "popularity": 25, "followers": {"total": 50000}},
    ]
    
    print("Testing Integrated Genre Classifier:")
    print("=" * 50)
    
    for artist in test_artists:
        enhanced = classifier.classify_artist_comprehensive(artist)
        
        print(f"{artist['name']}:")
        print(f"  Popularity: {artist['popularity']}")
        print(f"  Followers: {artist['followers']['total']:,}")
        print(f"  Assigned Genre: {enhanced.get('genres', [])}")
        print(f"  Method: {enhanced.get('genre_inference_methods', [])}")
        print()

if __name__ == "__main__":
    test_integrated_classifier()
