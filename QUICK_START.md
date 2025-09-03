# 🎵 Spotify Analytics Pipeline - Quick Start Guide

A comprehensive personal Spotify analytics pipeline with genre enrichment, medallion data architecture, and natural language querying capabilities.

## ✨ What You'll Get

- **📊 Complete Listening Analytics**: Daily, monthly, and genre-based insights
- **🎨 Genre Intelligence**: Auto-enhanced genre data for artists with missing information
- **🏔️ Mountain Time Analysis**: All data converted to Denver, CO timezone
- **🥇 Medallion Architecture**: Bronze → Silver → Gold data transformation layers
- **🤖 Natural Language Queries**: Ask questions about your data in plain English
- **⚡ Real-time Updates**: Automatic data refresh with Snowflake Dynamic Tables
- **📈 GitHub Actions**: Automated daily data collection

## 🚀 Quick Setup (5 minutes)

### 1. Prerequisites

```bash
# Required
- Python 3.8+
- Spotify account
- AWS account (S3 access)
- Snowflake account

# Optional but recommended
- GitHub account (for automated scheduling)
```

### 2. Clone & Setup

```bash
# Clone the repository
git clone https://github.com/your-username/spotify-snowflake-pipeline.git
cd spotify-snowflake-pipeline

# Run the automated setup
chmod +x quick_start.sh
./quick_start.sh
```

The setup script will:
- ✅ Check Python version
- ✅ Create virtual environment
- ✅ Install dependencies
- ✅ Configure credentials interactively
- ✅ Test your setup

### 3. First Run

```bash
# Activate virtual environment (if not already active)
source venv/bin/activate

# Test the connection
python main.py test

# Collect your first batch of data
python main.py run-once --enable-artist-genre-processing

# Check what you collected
python main.py stats --enable-artist-genre-processing
```

## 🏗️ Complete Setup Guide

### Step 1: Spotify Developer App

1. Go to [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
2. Create a new app:
   - **App Name**: "Personal Analytics Pipeline" 
   - **Redirect URI**: `http://localhost:8080/callback`
3. Note your **Client ID** and **Client Secret**

### Step 2: AWS S3 Setup

```bash
# Create S3 bucket (replace with your bucket name)
aws s3 mb s3://your-spotify-data-bucket

# Verify access
aws s3 ls s3://your-spotify-data-bucket
```

### Step 3: Snowflake Setup

```sql
-- Run in Snowflake to create the complete data warehouse
-- File: snowflake_setup.sql (included)

-- 1. Create database and roles
CREATE DATABASE spotify_analytics;
CREATE ROLE spotify_analyst_role;

-- 2. Create schemas
CREATE SCHEMA raw_data;
CREATE SCHEMA medallion_arch;

-- 3. Set up tables, stages, and Snowpipe
-- (Full script provided in snowflake_setup.sql)
```

### Step 4: Advanced Features Setup

#### A. Medallion Architecture (Bronze/Silver/Gold)

```sql
-- Deploy the medallion architecture
-- File: medallion_architecture_views.sql

-- Bronze: Raw data with minimal processing  
-- Silver: Enriched, business-ready data
-- Gold: Analytics-ready aggregations
```

#### B. Natural Language Querying

```sql
-- Upload semantic model for LLM integration
CREATE STAGE semantic_models;
PUT file://spotify_semantic_model.yml @semantic_models;

-- Test natural language queries
SELECT SNOWFLAKE.CORTEX.ANALYST(
    @semantic_models/spotify_semantic_model.yml,
    'What is my most played genre?'
);
```

#### C. GitHub Actions (Automated Daily Collection)

1. Fork this repository
2. Add secrets to your GitHub repo:
   ```
   SPOTIFY_CLIENT_ID
   SPOTIFY_CLIENT_SECRET  
   AWS_ACCESS_KEY_ID
   AWS_SECRET_ACCESS_KEY
   S3_BUCKET_NAME
   ```
3. Enable GitHub Actions - data will collect automatically daily at 9 AM!

## 📊 Usage Examples

### Basic Data Collection

```bash
# One-time collection with genre processing
python main.py run-once --enable-artist-genre-processing

# Backfill historical data (last 7 days)
python main.py backfill --days 7 --enable-artist-genre-processing

# Continuous collection (runs every 30 minutes)
python main.py run-continuous --enable-artist-genre-processing

# Process specific artists for genre enhancement
python main.py process-artists --artists "4iHNK0tOyZPYnBU7nGAgpQ,1Xyo4u8uXC1ZmMpatF05PJ"
```

### Analytics Queries

Once your data is in Snowflake, explore with these example queries:

```sql
-- Your daily listening summary
SELECT 
    denver_date,
    total_plays,
    unique_artists,
    mainstream_score,
    genre_diversity_score
FROM gold_daily_listening_summary
ORDER BY denver_date DESC
LIMIT 7;

-- Top genres with actual top artists (not alphabetical!)
SELECT 
    primary_genre,
    total_plays,
    top_artist,
    top_artist_plays,
    percentage_of_total_listening
FROM gold_genre_analysis_complete
ORDER BY total_plays DESC;

-- Monthly listening trends
SELECT 
    year,
    month_name,
    total_plays,
    top_artist,
    top_genre,
    plays_growth_rate
FROM gold_monthly_insights_complete
ORDER BY year DESC, month DESC;
```

### Natural Language Queries

```sql
-- Ask questions in plain English!
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

## 🎯 Key Features Explained

### 🎨 Genre Enhancement
- **Problem**: ~30% of artists have empty genre arrays from Spotify
- **Solution**: AI-powered genre inference using artist name patterns and popularity analysis
- **Result**: Complete genre coverage for all your artists

### 🏔️ Mountain Time Analysis  
- **Why**: All analysis in your local timezone (Denver, CO)
- **What**: Automatic UTC → Mountain Time conversion
- **Benefits**: Accurate morning/afternoon/evening listening patterns

### 🥇 Medallion Architecture
- **Bronze**: Raw data with deduplication
- **Silver**: Enriched data with genre information and temporal analysis
- **Gold**: Analytics-ready daily, genre, and monthly insights

### ⚡ Dynamic Tables
- **What**: Automatically refreshing materialized views
- **Why**: Always up-to-date analytics without manual refresh
- **How**: Incremental updates based on source data changes

## 🛠️ Troubleshooting

### Common Issues

**Issue**: `spotify.exceptions.SpotifyException: Invalid access token`
```bash
# Solution: Re-authenticate
rm .spotify_cache
python main.py run-once
```

**Issue**: Genre data missing for artists
```bash
# Solution: Force reprocessing with enhancement
python main.py process-artists --artists "artist_id_here"
```

**Issue**: Snowflake Dynamic Tables not refreshing
```sql
-- Solution: Manual refresh
ALTER DYNAMIC TABLE gold_daily_listening_summary REFRESH;
```

### Monitoring Your Pipeline

```bash
# Check pipeline health
python main.py stats --enable-artist-genre-processing

# Validate data in Snowflake
SELECT 
    table_name,
    row_count,
    last_updated
FROM (
    SELECT 'Raw Listening' as table_name, COUNT(*) as row_count, MAX(ingested_at) as last_updated FROM raw_data.spotify_listening_history
    UNION ALL
    SELECT 'Artist Genres', COUNT(*), MAX(ingested_at) FROM raw_data.spotify_artist_genres
    UNION ALL  
    SELECT 'Daily Summary', COUNT(*), MAX(last_updated) FROM gold_daily_listening_summary
);
```

## 📈 What's Next?

### Immediate Next Steps:
1. ✅ Run your first data collection
2. ✅ Set up Snowflake tables
3. ✅ Deploy medallion architecture  
4. ✅ Test natural language queries
5. ✅ Enable GitHub Actions for automation

### Advanced Exploration:
- **Custom Analytics**: Build your own queries and dashboards
- **BI Integration**: Connect Tableau, Looker, or Power BI
- **ML Models**: Predict your music taste or discover new genres
- **API Extensions**: Add more Spotify endpoints (playlists, saved tracks)

## 🤝 Community & Support

- **Issues**: [GitHub Issues](https://github.com/your-username/spotify-snowflake-pipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-username/spotify-snowflake-pipeline/discussions)
- **Contributions**: PRs welcome! See CONTRIBUTING.md

## 📄 License

MIT License - feel free to fork, modify, and share!

---

**Happy listening and analyzing! 🎵📊**

*Your personal Spotify data has never been more insightful.*
