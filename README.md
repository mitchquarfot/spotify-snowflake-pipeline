# 🎵 Spotify Analytics Pipeline

A comprehensive, production-ready pipeline that transforms your Spotify listening history into rich, queryable analytics with AI-enhanced genre classification, medallion data architecture, and natural language querying capabilities.

## ✨ Features

### 🎯 Core Pipeline
- **🔐 Spotify Integration**: OAuth-based authentication with retry logic and rate limiting
- **☁️ S3 Storage**: Automatically partitioned, compressed JSONL files optimized for Snowflake
- **🔄 Incremental Processing**: State management to avoid duplicate data processing  
- **⏰ Scheduling**: Configurable intervals for continuous data collection
- **🛡️ Error Handling**: Comprehensive retry logic with exponential backoff
- **📊 Monitoring**: Structured logging and pipeline statistics

### 🎨 Genre Intelligence
- **🧠 AI-Enhanced Classification**: Automatically fills missing genre data using name patterns and popularity analysis
- **📈 Complete Coverage**: Ensures every artist has genre classification
- **🔍 Source Tracking**: Distinguishes between Spotify-provided and AI-inferred genres
- **🎭 Multi-Genre Support**: Handles artists with multiple genre classifications

### 🏔️ Mountain Time Analytics
- **🌄 Denver Timezone**: All temporal analysis in Mountain Time (Denver, CO)
- **🕐 Time-of-Day Insights**: Morning, afternoon, evening, and night listening patterns
- **📅 Weekend Analysis**: Distinguish weekday vs weekend listening behavior
- **📆 Seasonal Trends**: Monthly and quarterly listening pattern analysis

### 🥇 Medallion Architecture
- **🥉 Bronze Layer**: Raw data with minimal processing and deduplication
- **🥈 Silver Layer**: Enriched, business-ready data with genre enhancement
- **🥇 Gold Layer**: Analytics-ready aggregations for daily, genre, and monthly insights
- **⚡ Dynamic Tables**: Auto-refreshing materialized views for real-time analytics

### 🤖 Natural Language Querying
- **🗣️ Conversational Analytics**: Ask questions about your data in plain English
- **🧠 LLM Integration**: Powered by Snowflake Cortex Analyst
- **📊 Semantic Model**: Rich metadata for accurate query interpretation
- **💬 Example Queries**: Pre-verified questions for immediate insights

### 🤖 AI-Powered Music Recommendations
- **🧠 Multiple ML Algorithms**: Collaborative filtering, content-based, temporal patterns, discovery, and hybrid ensemble
- **🎯 Personalized Discovery**: Find new artists and genres tailored to your taste
- **⏰ Context-Aware**: Time-of-day and mood-based recommendations
- **📊 Real-Time Generation**: On-demand playlist creation in Streamlit
- **🔍 Advanced Discovery**: Hidden gems, genre exploration, and artist discovery
- **🏗️ Snowflake Model Registry**: Production-grade ML model management and versioning
- **🔄 Automated Retraining**: Self-monitoring system with performance tracking

### 🔄 Automation & CI/CD
- **🚀 GitHub Actions**: Automated daily data collection
- **📈 Pipeline Monitoring**: Track collection statistics and data quality
- **🔧 Health Checks**: Automated testing and validation
- **📧 Notifications**: Pipeline status and error reporting
- **🤖 ML Automation**: Automated model monitoring, retraining, and deployment
- **🛡️ Keep-Alive Script**: [`scripts/trigger_actions_keepalive.sh`](scripts/trigger_actions_keepalive.sh) + [`docs/github_actions_keepalive.md`](docs/github_actions_keepalive.md) show how to trigger a `workflow_dispatch` run via cron (e.g., first Monday of the month at noon MT) so GitHub never disables the scheduled workflow due to inactivity.

## 📊 Data Schema & Analytics

### Raw Data Transformation
```json
{
  "unique_play": "20240115_143000_4iV5W9uYEdYUVa79Axb7Rh",
  "played_at": "2024-01-15T14:30:00Z",
  "denver_timestamp": "2024-01-15T07:30:00",
  "track_id": "4iV5W9uYEdYUVa79Axb7Rh",
  "track_name": "Never Gonna Give You Up",
  "primary_artist_name": "Rick Astley",
  "primary_genre": "dance pop",
  "artist_popularity": 73,
  "mainstream_score": 78.5,
  "time_of_day_category": "morning",
  "is_weekend": false
}
```

### Analytics Tables Available
- **📅 Daily Summaries**: `gold_daily_listening_summary`
- **🎵 Genre Analysis**: `gold_genre_analysis_complete` 
- **📈 Monthly Trends**: `gold_monthly_insights_complete`
- **👨‍🎤 Artist Insights**: `silver_artist_summary`
- **🎧 Enhanced Listening**: `silver_listening_enriched`

## 🚀 Quick Start

### Repository Layout

- `main.py`, `pipeline.py`, `spotify_client.py`, `s3_client.py` – core pipeline code
- `ml_pipeline/` – optional Snowflake ML utilities kept for advanced workflows
- `docs/` – operational guides, including the new [`docs/ARCHIVE.md`](docs/ARCHIVE.md) for retrieving legacy assets
- `scripts/` – operational tooling (e.g. keep-alive cron helper)

### Option 1: Automated Setup (Recommended)
```bash
# Clone the repository
git clone https://github.com/your-username/spotify-snowflake-pipeline.git
cd spotify-snowflake-pipeline

# Run the automated setup script
chmod +x quick_start.sh
./quick_start.sh

# The script will:
# ✅ Check prerequisites
# ✅ Create virtual environment  
# ✅ Install dependencies
# ✅ Configure credentials
# ✅ Test setup
# ✅ Run first collection
```

### Option 2: Manual Setup
```bash
# 1. Clone and setup environment
git clone https://github.com/your-username/spotify-snowflake-pipeline.git
cd spotify-snowflake-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure credentials
python3 setup.py

# 3. Test and run
python3 main.py test
python3 main.py run-once --enable-artist-genre-processing
```

## 🔧 Configuration

### Prerequisites
- **Python 3.8+**
- **Spotify Developer App**: [Create here](https://developer.spotify.com/dashboard)
- **AWS Account**: S3 access required
- **Snowflake Account**: Optional but recommended for full analytics

### Environment Variables
Create a `.env` file with:

```bash
# Spotify API Credentials
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
SPOTIFY_REDIRECT_URI=http://localhost:8080/callback

# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=us-west-2
S3_BUCKET_NAME=your-spotify-analytics-bucket

# Pipeline Configuration
FETCH_INTERVAL_MINUTES=30
BATCH_SIZE=50
SNOWFLAKE_STAGE_PREFIX=spotify_listening_history/
```

## 💻 Usage

### Basic Commands
```bash
# One-time collection with genre processing
python main.py run-once --enable-artist-genre-processing

# Continuous collection (every 30 minutes)
python main.py run-continuous --enable-artist-genre-processing

# Backfill historical data (last 7 days)
python main.py backfill --days 7 --enable-artist-genre-processing

# View pipeline statistics
python main.py stats --enable-artist-genre-processing

# Process specific artists for genre enhancement
python main.py process-artists --artists "4iHNK0tOyZPYnBU7nGAgpQ,1Xyo4u8uXC1ZmMpatF05PJ"

# Test all connections
python main.py test
```

### Advanced Features
```bash
# Backfill artist genre data for existing tracks
python main.py backfill-artists --days 30

# Run without genre processing (faster)
python main.py run-once

# Custom batch sizes for large backlogs
python main.py backfill --days 30 --batch-size 100
```

## ❄️ Snowflake Setup

### 1. Database & Schema Setup
```sql
-- Run the complete setup script
-- File: snowflake_setup.sql

CREATE DATABASE spotify_analytics;
CREATE ROLE spotify_analyst_role;
CREATE SCHEMA raw_data;
CREATE SCHEMA medallion_arch;

-- Tables, Snowpipe, and external stages are configured automatically
```

### 2. Medallion Architecture Deployment
```sql
-- Deploy the Bronze → Silver → Gold transformation layers
-- File: medallion_architecture_views.sql

-- Bronze: Raw data with deduplication
-- Silver: Enriched data with genre and temporal analysis  
-- Gold: Analytics-ready daily, genre, and monthly insights
```

### 3. Enhanced Analytics
```sql
-- Deploy corrected analytics views
-- File: fix_medallion_simple_approach.sql

-- Fixes "MIN() alphabetical issues" with actual top artists/genres/tracks
-- Provides gold_*_complete views with accurate rankings
```

### 4. AI-Powered ML Recommendations 🤖
```sql
-- Deploy the complete ML recommendation system
-- File: spotify_ml_recommendation_engine.sql
-- File: automated_model_retraining.sql
-- File: model_inference_functions.sql

-- Quick deployment:
-- 1. Execute all SQL files in Snowflake
-- 2. Run: CALL initialize_ml_automation();
-- 3. Update Streamlit app with new ML tab

-- Creates:
-- ✅ 5 ML algorithms (collaborative, content-based, temporal, discovery, hybrid)
-- ✅ Real-time recommendation functions
-- ✅ Automated model monitoring and retraining
-- ✅ Snowflake Model Registry integration
```

### 5. Natural Language Querying
```sql
-- Upload semantic model for LLM integration
CREATE STAGE semantic_models;
PUT file://spotify_semantic_model.yml @semantic_models;

-- Ask questions in plain English!
SELECT SNOWFLAKE.CORTEX.ANALYST(
    @semantic_models/spotify_semantic_model.yml,
    'What is my most played genre this month?'
);
```

## 📈 Example Analytics Queries

### Daily Insights
```sql
-- Your recent listening summary
SELECT 
    denver_date,
    total_plays,
    unique_artists,
    unique_genres,
    mainstream_score,
    genre_diversity_score,
    average_listening_hour
FROM gold_daily_listening_summary
ORDER BY denver_date DESC
LIMIT 7;
```

### Genre Analysis
```sql
-- Top genres with actual top artists (not alphabetical!)
SELECT 
    primary_genre,
    total_plays,
    top_artist,
    top_artist_plays,
    percentage_of_total_listening,
    average_artist_popularity
FROM gold_genre_analysis_complete
ORDER BY total_plays DESC
LIMIT 10;
```

### AI-Powered Recommendations 🤖
```sql
-- Get personalized recommendations (Hybrid AI)
SELECT * FROM TABLE(get_spotify_recommendations(30));

-- Discover new genres and artists
SELECT * FROM TABLE(get_discovery_recommendations('balanced', 20, 70));

-- Time-based recommendations
SELECT * FROM TABLE(get_time_based_recommendations(14, false, 15));

-- Find similar tracks to one you love
SELECT * FROM TABLE(get_similar_tracks('4iV5W9uYEdYUVa79Axb7Rh', 10));

-- Get your music taste profile
SELECT * FROM TABLE(get_user_taste_profile());
```

### Monthly Trends
```sql
-- Monthly listening evolution with growth metrics
SELECT 
    year,
    month_name,
    total_plays,
    top_artist,
    top_genre,
    plays_growth_rate,
    mainstream_tendency
FROM gold_monthly_insights_complete
ORDER BY year DESC, month DESC
LIMIT 12;
```

### Natural Language Examples
```sql
-- Ask questions conversationally
SELECT SNOWFLAKE.CORTEX.ANALYST(
    @semantic_models/spotify_semantic_model.yml,
    'How many songs did I listen to yesterday?'
);

SELECT SNOWFLAKE.CORTEX.ANALYST(
    @semantic_models/spotify_semantic_model.yml,
    'Who is my top country artist and how many times have I played them?'
);

SELECT SNOWFLAKE.CORTEX.ANALYST(
    @semantic_models/spotify_semantic_model.yml,
    'Show me my listening trends over the past 3 months'
);
```

## 🔄 GitHub Actions Automation

### Setup Automated Collection
1. **Fork this repository** on GitHub
2. **Add secrets** to your repo settings:
   ```
   SPOTIFY_CLIENT_ID
   SPOTIFY_CLIENT_SECRET
   AWS_ACCESS_KEY_ID
   AWS_SECRET_ACCESS_KEY
   S3_BUCKET_NAME
   ```
3. **Enable GitHub Actions** - your data will be collected automatically every day at 9 AM!

### Workflow Features
- ✅ **Daily Collection**: Automated data gathering
- ✅ **Genre Processing**: Enhanced artist genre data
- ✅ **Error Handling**: Notifications on failures
- ✅ **Statistics Reporting**: Daily collection summaries

## 🛠️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Spotify API   │───▶│  Python Pipeline │───▶│   Amazon S3     │
│   (OAuth 2.0)   │    │   (Enhanced)     │    │  (Partitioned)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │ Genre AI Engine │    │   Snowflake     │
                       │  (Enhancement)  │    │  (Data Warehouse)│
                       └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
                              ┌─────────────────────────────────────┐
                              │        Medallion Architecture       │
                              │  Bronze ───▶ Silver ───▶ Gold      │
                              │   (Raw)     (Enhanced)  (Analytics) │
                              └─────────────────────────────────────┘
                                                        │
                                                        ▼
                              ┌─────────────────────────────────────┐
                              │       Natural Language Queries      │
                              │    "What's my most played genre?"   │
                              │         (Cortex Analyst)            │
                              └─────────────────────────────────────┘
```

## 🧠 Genre Enhancement Engine

### How It Works
1. **Spotify Data**: Extracts genre arrays from artist profiles
2. **Gap Detection**: Identifies artists with empty genre arrays (~30% of artists)
3. **AI Enhancement**: Uses multiple inference strategies:
   - **Name Pattern Analysis**: "DJ" → "electronic", "& The" → "band"
   - **Popularity Mapping**: High popularity + followers → "mainstream pop"
   - **Fallback Classification**: "unclassified" for unclear cases
4. **Source Tracking**: Maintains data lineage (Spotify vs AI-enhanced)

### Enhancement Statistics
- **Coverage**: 100% genre classification (vs ~70% from Spotify alone)
- **Accuracy**: Pattern-based inference with conservative fallbacks
- **Transparency**: Clear distinction between original and enhanced data

## 📊 Monitoring & Observability

### Built-in Monitoring
```bash
# Check pipeline health
python main.py stats --enable-artist-genre-processing

# Output includes:
# • Total tracks collected
# • Artists processed
# • Genre enhancement statistics
# • S3 upload metrics
# • Error rates and retry statistics
```

### Snowflake Monitoring
```sql
-- Pipeline health dashboard
SELECT 
    'Listening Events' as metric,
    COUNT(*) as value,
    MAX(ingested_at) as last_updated
FROM raw_data.spotify_listening_history

UNION ALL

SELECT 
    'Artist Genres',
    COUNT(*),
    MAX(ingested_at)
FROM raw_data.spotify_artist_genres

UNION ALL

SELECT 
    'Daily Summaries',
    COUNT(*),
    MAX(last_updated)
FROM gold_daily_listening_summary;
```

## 🔧 Troubleshooting

### Common Issues

**Authentication Errors**
```bash
# Clear cached tokens and re-authenticate
rm .spotify_cache
python main.py run-once
```

**Missing Genre Data**
```bash
# Force reprocess specific artists
python main.py process-artists --artists "artist_id_here"

# Bulk reprocess artists with empty genres
python main.py backfill-artists --days 30
```

**Snowflake Dynamic Table Issues**
```sql
-- Manual refresh if tables seem stale
ALTER DYNAMIC TABLE gold_daily_listening_summary REFRESH;
ALTER DYNAMIC TABLE silver_listening_enriched REFRESH;
```

**S3 Permission Issues**
```bash
# Verify AWS credentials and bucket access
aws s3 ls s3://your-bucket-name
python main.py test
```

### Debug Mode
```bash
# Enable verbose logging
export LOG_LEVEL=DEBUG
python main.py run-once --enable-artist-genre-processing
```

## 🤝 Contributing

We welcome contributions! Here's how:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Development Setup
```bash
# Create an isolated environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies and tooling
pip install -r requirements.txt
pip install pytest black flake8

# Run tests
pytest

# Format code
black .
flake8 .
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Spotify Web API** for providing comprehensive listening data
- **Snowflake** for powerful data warehousing and analytics capabilities
- **OpenAI/Anthropic** for inspiring the natural language query features
- **The Python Community** for excellent libraries that make this possible

## 📚 Additional Resources

- **📖 Complete Setup Guide**: [QUICK_START.md](QUICK_START.md)
- **🗃️ Legacy Materials**: [docs/ARCHIVE.md](docs/ARCHIVE.md)
- **🐛 Issue Tracker**: [GitHub Issues](https://github.com/your-username/spotify-snowflake-pipeline/issues)
- **💬 Discussions**: [GitHub Discussions](https://github.com/your-username/spotify-snowflake-pipeline/discussions)
- **📊 Spotify API Docs**: [Developer Documentation](https://developer.spotify.com/documentation/web-api/)
- **❄️ Snowflake Docs**: [Data Cloud Documentation](https://docs.snowflake.com/)

---

**Transform your Spotify listening into actionable insights! 🎵📊**

*Your personal music analytics journey starts here.*