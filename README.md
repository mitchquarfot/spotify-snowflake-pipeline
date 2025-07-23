# Spotify to Snowflake Data Pipeline

A robust, production-ready pipeline that extracts your Spotify listening history via the Web API, stores it in Amazon S3 with Snowflake-compatible partitioning, and enables real-time data ingestion into Snowflake using Snowpipe.

## 🎯 Features

- **Spotify Integration**: OAuth-based authentication with retry logic and rate limiting
- **S3 Storage**: Automatically partitioned, compressed JSONL files optimized for Snowflake
- **Incremental Processing**: State management to avoid duplicate data processing  
- **Scheduling**: Configurable intervals for continuous data collection
- **Error Handling**: Comprehensive retry logic with exponential backoff
- **Monitoring**: Structured logging and pipeline statistics
- **Snowflake Ready**: Data format and partitioning designed for Snowpipe ingestion

## 📊 Data Schema

The pipeline transforms Spotify's raw API data into a flattened, analytics-friendly schema:

```json
{
  "played_at": "2024-01-15T14:30:00Z",
  "played_at_timestamp": 1705329000,
  "played_at_date": "2024-01-15",
  "played_at_hour": 14,
  "track_id": "4iV5W9uYEdYUVa79Axb7Rh",
  "track_name": "Never Gonna Give You Up",
  "track_duration_ms": 213573,
  "primary_artist_name": "Rick Astley",
  "album_name": "Whenever You Need Somebody",
  "context_type": "playlist",
  "ingested_at": "2024-01-15T14:31:00Z"
}
```

## 🚀 Quick Start

### 1. Prerequisites

- Python 3.8+
- Spotify account and Developer App
- AWS account with S3 access
- (Optional) Snowflake account for final data warehouse

### 2. Setup

```bash
# Clone and navigate to the project
git clone <your-repo>
cd spotify-snowflake-pipeline

# Install dependencies
pip install -r requirements.txt

# Run the interactive setup
python setup.py
```

The setup script will guide you through:
- Creating a Spotify Developer App
- Configuring AWS credentials
- Setting pipeline parameters

### 3. Test Your Configuration

```bash
# Test all connections
python main.py test

# Run the pipeline once
python main.py run-once

# Check statistics
python main.py stats
```

### 4. Start Continuous Collection

```bash
# Run continuously (Ctrl+C to stop)
python main.py run-continuous

# Or backfill historical data first
python main.py backfill --days 30
```

## 📁 S3 Data Structure

Files are organized for optimal Snowflake ingestion:

```
s3://your-bucket/spotify_listening_history/
├── 2024/01/15/spotify_tracks_20240115_143000_batch_50.json.gz
├── 2024/01/15/spotify_tracks_20240115_150000_batch_42.json.gz
└── 2024/01/16/spotify_tracks_20240116_090000_batch_35.json.gz
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file or set these environment variables:

```bash
# Spotify API
SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret
SPOTIFY_REDIRECT_URI=http://localhost:8080/callback

# AWS
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-west-2
S3_BUCKET_NAME=your-spotify-bucket

# Pipeline
FETCH_INTERVAL_MINUTES=30
BATCH_SIZE=50
SNOWFLAKE_STAGE_PREFIX=spotify_listening_history/
```

### Command Line Options

```bash
# Run once and exit
python main.py run-once

# Run continuously on schedule  
python main.py run-continuous

# Backfill historical data
python main.py backfill --days 7

# Show pipeline statistics
python main.py stats

# Test connections
python main.py test
```

## ❄️ Snowflake Integration

### 1. Create Table

```sql
CREATE TABLE spotify_listening_history (
    played_at TIMESTAMP_NTZ,
    played_at_timestamp NUMBER,
    played_at_date DATE,
    played_at_hour NUMBER,
    track_id STRING,
    track_name STRING,
    track_duration_ms NUMBER,
    track_popularity NUMBER,
    track_explicit BOOLEAN,
    primary_artist_id STRING,
    primary_artist_name STRING,
    album_id STRING,
    album_name STRING,
    album_type STRING,
    album_release_date STRING,
    context_type STRING,
    ingested_at TIMESTAMP_NTZ,
    data_source STRING DEFAULT 'spotify_recently_played_api'
);
```

### 2. Create Stage

```sql
CREATE STAGE spotify_stage
URL = 's3://your-spotify-bucket/spotify_listening_history/'
CREDENTIALS = (
    AWS_KEY_ID = 'your_access_key'
    AWS_SECRET_KEY = 'your_secret_key'
)
FILE_FORMAT = (
    TYPE = JSON
    COMPRESSION = GZIP
    STRIP_OUTER_ARRAY = FALSE
);
```

### 3. Create Snowpipe

```sql
CREATE PIPE spotify_pipe 
AUTO_INGEST = TRUE
AS
COPY INTO spotify_listening_history
FROM @spotify_stage
FILE_FORMAT = (TYPE = JSON COMPRESSION = GZIP)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

### 4. Configure S3 Event Notifications

Set up S3 bucket notifications to trigger Snowpipe on new file uploads:

1. Go to your S3 bucket → Properties → Event notifications
2. Create notification for `s3:ObjectCreated:*` events
3. Set destination to your Snowpipe SQS queue
4. Filter by prefix: `spotify_listening_history/`

## 🔧 Advanced Configuration

### Custom Data Transformations

Modify `spotify_client.py`'s `transform_track_data()` method to add custom fields:

```python
def transform_track_data(self, track_item: Dict) -> Dict:
    # ... existing transformation ...
    
    # Add custom fields
    transformed_data.update({
        "listening_session_id": self.generate_session_id(),
        "user_timezone": self.get_user_timezone(),
        "custom_tags": self.extract_custom_tags(track)
    })
    
    return transformed_data
```

### Production Deployment

For production environments:

1. **Use AWS IAM roles** instead of access keys
2. **Deploy on EC2/ECS** with appropriate IAM permissions
3. **Set up CloudWatch monitoring** for logs and metrics
4. **Configure S3 lifecycle policies** for cost optimization
5. **Use AWS Secrets Manager** for credential management

### Error Monitoring

The pipeline includes structured logging. Integrate with your monitoring stack:

```python
import structlog

logger = structlog.get_logger(__name__)
# Logs are JSON formatted for easy parsing
```

## 📊 Monitoring & Analytics

### Key Metrics to Monitor

- **Processing Rate**: Tracks processed per hour
- **API Rate Limits**: Spotify API usage
- **S3 Upload Success Rate**: File upload reliability  
- **Data Freshness**: Time lag between listening and ingestion
- **Error Rates**: Failed requests and retries

### Sample Queries

```sql
-- Daily listening summary
SELECT 
    played_at_date,
    COUNT(*) AS tracks_played,
    COUNT(DISTINCT track_id) AS unique_tracks,
    COUNT(DISTINCT primary_artist_name) AS unique_artists
FROM spotify_listening_history 
WHERE played_at_date >= CURRENT_DATE - 30
GROUP BY played_at_date
ORDER BY played_at_date;

-- Top artists this month
SELECT 
    primary_artist_name,
    COUNT(*) AS play_count,
    SUM(track_duration_ms) / 1000 / 60 AS total_minutes
FROM spotify_listening_history 
WHERE played_at_date >= DATE_TRUNC('MONTH', CURRENT_DATE)
GROUP BY primary_artist_name
ORDER BY play_count DESC
LIMIT 10;
```

## 🚨 Troubleshooting

### Common Issues

**Authentication Errors**
- Verify Spotify app redirect URI matches exactly
- Check that all required scopes are enabled
- Clear `.spotify_cache` and re-authenticate

**S3 Upload Failures**  
- Verify AWS credentials and bucket permissions
- Check bucket exists and region matches
- Ensure S3 bucket policy allows PutObject

**Missing Data**
- Spotify API only provides ~50 days of history
- Check pipeline state file for last processed timestamp
- Use backfill command to catch missed data

**Rate Limiting**
- Pipeline includes built-in rate limiting (60 req/min)
- Increase `FETCH_INTERVAL_MINUTES` if hitting limits
- Monitor Spotify API quota in developer dashboard

### Debug Mode

Enable verbose logging by setting log level:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## 📄 License

MIT License - see LICENSE file for details.

## 🙋‍♀️ Support

- 📚 Check the troubleshooting section above
- 🐛 Open an issue for bugs or feature requests  
- 💬 Start a discussion for usage questions

---

**Happy analyzing your music! 🎵📊** 