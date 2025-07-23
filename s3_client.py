"""S3 client for uploading Spotify listening data."""

import json
import gzip
from datetime import datetime
from typing import List, Dict, Optional
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from config import settings

logger = structlog.get_logger(__name__)


class S3Client:
    """Client for uploading data to S3 in Snowflake-compatible format."""
    
    def __init__(self):
        """Initialize S3 client."""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=settings.aws.access_key_id,
                aws_secret_access_key=settings.aws.secret_access_key,
                region_name=settings.aws.region
            )
            self.bucket_name = settings.aws.s3_bucket_name
            logger.info("S3 client initialized", bucket=self.bucket_name)
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
        except Exception as e:
            logger.error("Failed to initialize S3 client", error=str(e))
            raise
    
    def ensure_bucket_exists(self) -> bool:
        """Ensure the S3 bucket exists."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info("S3 bucket exists", bucket=self.bucket_name)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.info("Creating S3 bucket", bucket=self.bucket_name)
                try:
                    if settings.aws.region == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': settings.aws.region}
                        )
                    logger.info("S3 bucket created successfully", bucket=self.bucket_name)
                    return True
                except ClientError as create_error:
                    logger.error("Failed to create S3 bucket", error=str(create_error))
                    return False
            else:
                logger.error("Error checking S3 bucket", error=str(e))
                return False
    
    def generate_s3_key(self, timestamp: datetime, file_suffix: str = "") -> str:
        """
        Generate S3 key with date partitioning for Snowflake.
        
        Args:
            timestamp: Timestamp for partitioning
            file_suffix: Additional suffix for the filename
            
        Returns:
            S3 key path
        """
        date_partition = timestamp.strftime(settings.pipeline.date_partition_format)
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
        
        filename = f"spotify_tracks_{timestamp_str}"
        if file_suffix:
            filename += f"_{file_suffix}"
        filename += ".json.gz"
        
        s3_key = f"{settings.pipeline.snowflake_stage_prefix}{date_partition}/{filename}"
        return s3_key
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def upload_tracks_batch(
        self, 
        tracks: List[Dict], 
        timestamp: Optional[datetime] = None
    ) -> str:
        """
        Upload a batch of tracks to S3 as compressed JSONL.
        
        Args:
            tracks: List of transformed track dictionaries
            timestamp: Timestamp for partitioning (defaults to now)
            
        Returns:
            S3 key of uploaded file
        """
        if not tracks:
            logger.warning("No tracks to upload")
            return ""
        
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        s3_key = self.generate_s3_key(timestamp, f"batch_{len(tracks)}")
        
        try:
            # Convert tracks to JSONL format (one JSON object per line)
            jsonl_data = "\n".join(json.dumps(track, ensure_ascii=False) for track in tracks)
            
            # Compress the data
            compressed_data = gzip.compress(jsonl_data.encode('utf-8'))
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=compressed_data,
                ContentType='application/gzip',
                ContentEncoding='gzip',
                Metadata={
                    'source': 'spotify-api',
                    'track_count': str(len(tracks)),
                    'format': 'jsonl',
                    'ingestion_timestamp': timestamp.isoformat()
                }
            )
            
            logger.info(
                "Successfully uploaded tracks batch",
                s3_key=s3_key,
                track_count=len(tracks),
                compressed_size=len(compressed_data)
            )
            
            return s3_key
            
        except Exception as e:
            logger.error(
                "Failed to upload tracks batch",
                s3_key=s3_key,
                track_count=len(tracks),
                error=str(e)
            )
            raise
    
    def upload_single_track(self, track: Dict, timestamp: Optional[datetime] = None) -> str:
        """
        Upload a single track to S3.
        
        Args:
            track: Transformed track dictionary
            timestamp: Timestamp for partitioning (defaults to now)
            
        Returns:
            S3 key of uploaded file
        """
        return self.upload_tracks_batch([track], timestamp)
    
    def list_recent_files(self, days: int = 7) -> List[Dict]:
        """
        List recent files in the bucket for monitoring.
        
        Args:
            days: Number of days to look back
            
        Returns:
            List of file information dictionaries
        """
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=settings.pipeline.snowflake_stage_prefix
            )
            
            files = []
            cutoff_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            cutoff_date = cutoff_date.timestamp() - (days * 24 * 3600)
            
            for page in page_iterator:
                for obj in page.get('Contents', []):
                    if obj['LastModified'].timestamp() >= cutoff_date:
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat(),
                            'etag': obj['ETag'].strip('"')
                        })
            
            logger.info("Listed recent files", count=len(files), days=days)
            return files
            
        except Exception as e:
            logger.error("Failed to list recent files", error=str(e))
            return []
    
    def get_file_metadata(self, s3_key: str) -> Optional[Dict]:
        """Get metadata for a specific file."""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return {
                'key': s3_key,
                'size': response['ContentLength'],
                'last_modified': response['LastModified'].isoformat(),
                'metadata': response.get('Metadata', {}),
                'content_type': response.get('ContentType'),
                'content_encoding': response.get('ContentEncoding')
            }
        except Exception as e:
            logger.error("Failed to get file metadata", s3_key=s3_key, error=str(e))
            return None 