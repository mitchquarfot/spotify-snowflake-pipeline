#!/usr/bin/env python3
"""
SPOTIFY ML DISCOVERY SYSTEM
Advanced ML-powered music discovery using Snowflake recommendation engine

This pipeline:
1. Queries ML recommendation views for personalized candidates
2. Searches Spotify for those ML-recommended tracks  
3. Saves to dedicated S3 path: spotify_ml_discoveries/
4. Enables A/B testing vs. smart search approach
"""

import json
import logging
import os
from datetime import datetime
from typing import List, Dict, Optional
import snowflake.connector
from config import settings
from spotify_client import SpotifyClient
from s3_client import S3Client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpotifyMLDiscoveryEngine:
    """ML-powered discovery engine using Snowflake recommendations"""
    
    def __init__(self):
        self.settings = settings
        self.spotify = SpotifyClient()
        self.s3_client = S3Client()
        
        # Snowflake connection for ML queries
        self.snowflake_config = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'), 
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'SPOTIFY_ANALYTICS'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'ANALYTICS'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        }
        
        logger.info("ML Discovery Engine initialized")
    
    def get_ml_recommendations(self, limit: int = 100) -> List[Dict]:
        """Query Snowflake ML recommendation engine for candidates"""
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            cursor = conn.cursor()
            
            # Progressive ML Query - Try materialized sources first (much better performance)
            ml_queries = [
                # Try dynamic table first (best performance, auto-refresh)
                """
                SELECT 
                    track_name,
                    primary_artist_name as artist_name,
                    primary_genre as genre,
                    album_name,
                    track_popularity,
                    final_recommendation_score as recommendation_score,
                    recommendation_strategies,
                    playlist_position,
                    ml_confidence,
                    recommendation_reason
                FROM ml_hybrid_recommendations_dt
                WHERE final_recommendation_score > 0.3
                ORDER BY final_recommendation_score DESC
                LIMIT %s
                """,
                # Fallback to regular table (good performance, manual refresh)
                """
                SELECT 
                    track_name,
                    primary_artist_name as artist_name,
                    primary_genre as genre,
                    album_name,
                    track_popularity,
                    final_recommendation_score as recommendation_score,
                    recommendation_strategies,
                    playlist_position,
                    ml_confidence,
                    recommendation_reason
                FROM ml_hybrid_recommendations_tbl
                WHERE final_recommendation_score > 0.3
                ORDER BY final_recommendation_score DESC
                LIMIT %s
                """,
                # Fallback to view (if materialization fails)
                """
                SELECT 
                    track_name,
                    primary_artist_name as artist_name,
                    primary_genre as genre,
                    album_name,
                    track_popularity,
                    final_recommendation_score as recommendation_score,
                    recommendation_strategies,
                    playlist_position,
                    ml_confidence,
                    recommendation_reason
                FROM ml_hybrid_recommendations_working
                WHERE final_recommendation_score > 0.3
                ORDER BY final_recommendation_score DESC
                LIMIT %s
                """,
                # Final fallback to collaborative only
                """
                SELECT 
                    track_name,
                    primary_artist_name as artist_name,
                    primary_genre as genre,
                    album_name,
                    track_popularity,
                    recommendation_score as final_recommendation_score,
                    recommendation_strategy as recommendation_strategies,
                    genre_track_rank as playlist_position,
                    'Single Algorithm' as ml_confidence,
                    'Users with similar taste love this track' as recommendation_reason
                FROM ml_collaborative_recommendations
                WHERE recommendation_score > 0.3
                ORDER BY recommendation_score DESC
                LIMIT %s
                """
            ]
            
            # Try ML queries in order until one works
            results = []
            for i, query in enumerate(ml_queries):
                try:
                    logger.info(f"Attempting ML query approach {i+1}...")
                    cursor.execute(query, (limit,))
                    results = cursor.fetchall()
                    if results:
                        logger.info(f"✅ ML query approach {i+1} successful - found {len(results)} candidates")
                        break
                except Exception as query_error:
                    logger.warning(f"ML query approach {i+1} failed: {query_error}")
                    continue
            
            if not results:
                logger.error("All ML query approaches failed")
                return []
            
            ml_candidates = []
            for row in results:
                ml_candidates.append({
                    'track_name': row[0],
                    'artist_name': row[1], 
                    'genre': row[2],
                    'album_name': row[3],
                    'popularity': row[4],
                    'ml_score': row[5],
                    'ml_strategies': row[6],
                    'playlist_position': row[7],
                    'ml_confidence': row[8] if len(row) > 8 else 'Medium',
                    'recommendation_reason': row[9] if len(row) > 9 else 'ML-powered recommendation'
                })
            
            logger.info(f"🧠 ML Engine found {len(ml_candidates)} recommendation candidates")
            return ml_candidates
            
        except Exception as e:
            logger.error(f"Failed to query ML recommendations: {e}")
            return []
        finally:
            if 'conn' in locals():
                conn.close()
    
    def get_discovery_recommendations(self, limit: int = 50) -> List[Dict]:
        """Get discovery-focused ML recommendations for completely new music"""
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            cursor = conn.cursor()
            
            # Query discovery-specific recommendations
            discovery_query = """
            SELECT 
                track_name,
                primary_artist_name,
                primary_genre,
                album_name,
                track_popularity,
                recommendation_score,
                genre_novelty_score,
                artist_novelty_score
            FROM ml_discovery_recommendations
            WHERE recommendation_score > 0.6
            ORDER BY recommendation_score DESC
            LIMIT %s
            """
            
            cursor.execute(discovery_query, (limit * 2))  # Get more candidates
            results = cursor.fetchall()
            
            discovery_candidates = []
            for row in results:
                discovery_candidates.append({
                    'track_name': row[0],
                    'artist_name': row[1],
                    'genre': row[2], 
                    'album_name': row[3],
                    'popularity': row[4],
                    'ml_score': row[5],
                    'genre_novelty': row[6],
                    'artist_novelty': row[7]
                })
            
            logger.info(f"🔍 Discovery Engine found {len(discovery_candidates)} novelty candidates")
            return discovery_candidates
            
        except Exception as e:
            logger.error(f"Failed to query discovery recommendations: {e}")
            return []
        finally:
            if 'conn' in locals():
                conn.close()
    
    def search_spotify_for_ml_candidates(self, ml_candidates: List[Dict], limit: int = 50) -> List[Dict]:
        """Search Spotify for ML-recommended tracks"""
        discovered_tracks = []
        
        for candidate in ml_candidates[:limit * 2]:  # Get more candidates to ensure we reach limit
            try:
                # Create search query from ML recommendation
                artist_name = candidate['artist_name']
                track_name = candidate['track_name']
                
                # Search for exact track first
                exact_query = f'track:"{track_name}" artist:"{artist_name}"'
                exact_results = self.spotify.search(exact_query, 'track', limit=5)
                
                tracks_found = exact_results.get('tracks', {}).get('items', [])
                
                # If no exact match, search by artist + genre
                if not tracks_found:
                    genre_query = f'artist:"{artist_name}" genre:{candidate["genre"].replace(" ", "-")}'
                    genre_results = self.spotify.search(genre_query, 'track', limit=3)
                    tracks_found = genre_results.get('tracks', {}).get('items', [])
                
                # If still no results, try broader search
                if not tracks_found:
                    broad_query = f'artist:"{artist_name}"'
                    broad_results = self.spotify.search(broad_query, 'track', limit=2)
                    tracks_found = broad_results.get('tracks', {}).get('items', [])
                
                # Process found tracks
                for track in tracks_found:
                    discovered_tracks.append({
                        'track_id': track['id'],
                        'track_name': track['name'],
                        'primary_artist_name': track['artists'][0]['name'],
                        'primary_artist_id': track['artists'][0]['id'],
                        'album_name': track['album']['name'],
                        'album_release_date': track['album']['release_date'],
                        'track_popularity': track['popularity'],
                        'track_duration_ms': track['duration_ms'],
                        'preview_url': track['preview_url'],
                        'discovery_strategy': 'ml_hybrid_advanced',
                        'ml_recommendation_score': candidate['ml_score'],
                        'ml_strategies_used': candidate['ml_strategies'],
                        'ml_confidence_level': candidate.get('ml_confidence', 'Medium'),
                        'ml_recommendation_reason': candidate.get('recommendation_reason', 'Advanced ML recommendation'),
                        'seed_track': f"{candidate['track_name']} - {candidate['artist_name']}",
                        'discovered_at': datetime.now()
                    })
                
                if len(discovered_tracks) >= limit:
                    break
                    
            except Exception as e:
                logger.error(f"Failed to search for ML candidate {candidate['track_name']}: {e}")
                continue
        
        logger.info(f"🎵 Found {len(discovered_tracks)} tracks via ML recommendations")
        return discovered_tracks[:limit]
    
    def search_spotify_for_discovery_candidates(self, candidates: List[Dict], limit: int) -> List[Dict]:
        """Search Spotify for discovery-focused candidates"""
        discovered_tracks = []
        
        for candidate in candidates[:limit * 2]:  # Get more candidates
            try:
                # Search by genre for novelty discovery
                genre_name = candidate['genre'].replace(' ', '-').lower()
                search_query = f'genre:{genre_name} year:2020-2025'  # Recent music
                
                search_results = self.spotify.search(search_query, 'track', limit=3)
                tracks = search_results.get('tracks', {}).get('items', [])
                
                for track in tracks:
                    discovered_tracks.append({
                        'track_id': track['id'],
                        'track_name': track['name'],
                        'primary_artist_name': track['artists'][0]['name'],
                        'primary_artist_id': track['artists'][0]['id'],
                        'album_name': track['album']['name'],
                        'album_release_date': track['album']['release_date'],
                        'track_popularity': track['popularity'],
                        'track_duration_ms': track['duration_ms'],
                        'preview_url': track['preview_url'],
                        'discovery_strategy': 'ml_discovery_exploration',
                        'ml_recommendation_score': candidate['ml_score'],
                        'genre_novelty_score': candidate['genre_novelty'],
                        'artist_novelty_score': candidate['artist_novelty'],
                        'seed_genre': candidate['genre'],
                        'discovered_at': datetime.now()
                    })
                
                if len(discovered_tracks) >= limit:
                    break
                    
            except Exception as e:
                logger.error(f"Failed to search for discovery candidate in {candidate['genre']}: {e}")
                continue
        
        return discovered_tracks[:limit]
    
    def save_ml_discoveries_to_s3(self, discoveries: List[Dict]) -> None:
        """Save ML discoveries to dedicated S3 path"""
        if not discoveries:
            logger.warning("No ML discoveries to save")
            return
            
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Prepare data for S3 (convert datetime objects to strings)
        processed_discoveries = []
        for discovery in discoveries:
            processed_discovery = discovery.copy()
            processed_discovery['discovered_at'] = discovery['discovered_at'].isoformat()
            processed_discoveries.append(processed_discovery)
        
        try:
            # Create JSON Lines content
            jsonl_content = '\n'.join(json.dumps(discovery) for discovery in processed_discoveries)
            
            # Upload to dedicated ML discoveries path
            ml_s3_key = f"spotify_ml_discoveries/{timestamp}/ml_discovered_tracks_{timestamp}.json"
            
            # Direct S3 upload to ML discovery path
            self.s3_client.s3_client.put_object(
                Bucket=self.s3_client.bucket_name,
                Key=ml_s3_key,
                Body=jsonl_content.encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.info(f"✅ Saved {len(discoveries)} ML discoveries to S3: s3://{self.s3_client.bucket_name}/{ml_s3_key}")
            
            # Also save locally for reference
            local_file = f"ml_discovered_tracks_{timestamp}.json"
            with open(local_file, 'w') as f:
                f.write(jsonl_content)
            logger.info(f"✅ Saved ML copy locally: {local_file}")
            
        except Exception as e:
            logger.error(f"Failed to save ML discoveries to S3: {e}")
            # Save locally as backup
            backup_file = f"backup_ml_discoveries_{timestamp}.json"
            with open(backup_file, 'w') as f:
                jsonl_content = '\n'.join(json.dumps(discovery) for discovery in processed_discoveries)
                f.write(jsonl_content)
            logger.info(f"💾 Saved ML backup locally: {backup_file}")
            raise
    
    def run_ml_discovery_pipeline(self, strategy: str = 'hybrid', limit: int = 50) -> Dict:
        """
        Main ML discovery pipeline
        
        Args:
            strategy: 'hybrid' (all ML strategies) or 'discovery' (exploration focus)
            limit: Number of tracks to discover
        """
        logger.info("🧠 Starting ML-powered discovery pipeline...")
        
        try:
            if strategy == 'hybrid':
                logger.info("Using hybrid ML recommendation strategy")
                ml_candidates = self.get_ml_recommendations(limit * 3)  # Get more candidates
                discoveries = self.search_spotify_for_ml_candidates(ml_candidates, limit)
            elif strategy == 'discovery':
                logger.info("Using discovery exploration strategy")  
                discovery_candidates = self.get_discovery_recommendations(limit * 2)
                discoveries = self.search_spotify_for_discovery_candidates(discovery_candidates, limit)
            else:
                raise ValueError(f"Unknown strategy: {strategy}")
            
            if discoveries:
                logger.info(f"Saving {len(discoveries)} ML discoveries to S3...")
                self.save_ml_discoveries_to_s3(discoveries)
                
                # Calculate stats
                avg_ml_score = sum(d.get('ml_recommendation_score', 0) for d in discoveries) / len(discoveries)
                strategies_used = set()
                for d in discoveries:
                    if 'ml_strategies_used' in d and d['ml_strategies_used']:
                        strategies_used.update(str(d['ml_strategies_used']).split(','))
                
                result = {
                    'total_discoveries': len(discoveries),
                    'ml_strategy': strategy,
                    'avg_ml_recommendation_score': avg_ml_score,
                    'ml_strategies_used': list(strategies_used) if strategies_used else ['ml_hybrid'],
                    'discovery_timestamp': datetime.now()
                }
                
                logger.info(f"🧠 ML Discovery pipeline complete: {result}")
                return result
            else:
                logger.warning("No ML discoveries found")
                return {'total_discoveries': 0, 'ml_strategy': strategy}
                
        except Exception as e:
            logger.error(f"ML Discovery pipeline failed: {e}")
            raise

if __name__ == "__main__":
    # Initialize ML discovery engine
    ml_engine = SpotifyMLDiscoveryEngine()
    
    # Run hybrid ML pipeline
    print("\n🧠 RUNNING ML-POWERED DISCOVERY...")
    hybrid_result = ml_engine.run_ml_discovery_pipeline(strategy='hybrid', limit=30)
    
    print(f"""
    🎵 ML DISCOVERY COMPLETE 🎵
    
    📊 Discovered {hybrid_result['total_discoveries']} tracks using ML recommendations
    🧠 ML Strategy: {hybrid_result.get('ml_strategy', 'unknown')}
    📈 Average ML Score: {hybrid_result.get('avg_ml_recommendation_score', 0):.3f}
    🎯 ML Strategies Used: {', '.join(hybrid_result.get('ml_strategies_used', []))}
    ☁️  Saved to S3 - Ready for Snowpipe ingestion!
    
    Compare with smart search pipeline to see which works better!
    """)
