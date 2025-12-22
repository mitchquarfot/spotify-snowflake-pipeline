# 🤖 Spotify ML Recommendation System Setup Guide

Complete setup guide for the AI-powered music recommendation system using Snowflake Model Registry and machine learning.

## 📋 Prerequisites

### Required
- ✅ Existing Spotify data pipeline (main pipeline working)
- ✅ Snowflake account with ML features enabled
- ✅ At least 50+ tracks in your listening history
- ✅ Python 3.8+ with virtual environment

### Recommended
- 🎯 100+ tracks for better recommendations
- 🎯 Multiple genres in listening history
- 🎯 At least 30 days of listening data

## 🚀 Quick Setup (5 Steps)

### Step 1: Install ML Dependencies

```bash
# Activate your virtual environment
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install ML requirements
pip install scikit-learn numpy pandas snowflake-ml-python
```

### Step 2: Set Up Snowflake ML Infrastructure

```sql
-- Run in Snowflake (as ACCOUNTADMIN if needed)
-- File: spotify_ml_recommendation_engine.sql

USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- Execute the entire file to create:
-- • ML training data views
-- • Similarity matrices  
-- • Recommendation generation views
-- • Hybrid recommendation engine
```

### Step 3: Set Up Automated ML System

```sql
-- Run in Snowflake
-- File: automated_model_retraining.sql

-- Initialize the automation system
CALL initialize_ml_automation();

-- Start automated tasks
CALL start_ml_automation();
```

### Step 4: Train Initial Models

```bash
# Set Snowflake environment variables
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_WAREHOUSE=your_warehouse

# Train and register models
python train_and_register_models.py
```

### Step 5: Deploy Inference Functions

```sql
-- Run in Snowflake
-- File: model_inference_functions.sql

-- Creates real-time recommendation functions
-- Available functions:
-- • get_spotify_recommendations()
-- • get_similar_tracks()
-- • get_discovery_recommendations()
-- • get_time_based_recommendations()
```

## 🎯 Detailed Setup Instructions

### A. Snowflake ML Setup

#### 1. Create ML Training Views

The ML system uses several materialized views for training data:

```sql
-- Core training data views
CREATE OR REPLACE VIEW ml_user_genre_interactions AS...
CREATE OR REPLACE VIEW ml_track_content_features AS...
CREATE OR REPLACE VIEW ml_temporal_patterns AS...
CREATE OR REPLACE VIEW ml_genre_similarity_matrix AS...
```

#### 2. Model Training Infrastructure

```sql
-- Training and deployment tracking
CREATE TABLE ml_training_log (...);
CREATE TABLE ml_deployment_log (...);
CREATE TABLE ml_alerts_log (...);
```

#### 3. Automated Monitoring

```sql
-- Daily performance monitoring
CREATE TASK spotify_model_monitoring
    WAREHOUSE = 'ML_WAREHOUSE'
    SCHEDULE = 'CRON 0 8 * * *'
AS CALL monitor_model_performance(...);

-- Weekly automated retraining  
CREATE TASK spotify_model_retraining
    WAREHOUSE = 'ML_WAREHOUSE'
    SCHEDULE = 'CRON 0 6 * * 1'
AS CALL retrain_recommendation_models(...);
```

### B. Python ML Model Training

#### 1. Environment Setup

```bash
# Install Python dependencies
pip install scikit-learn==1.3.0
pip install numpy>=1.24.0
pip install pandas>=2.0.0
pip install snowflake-ml-python>=1.0.0
```

#### 2. Configure Snowflake Connection

Create `.env` file or set environment variables:

```bash
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
```

#### 3. Train Models

```bash
# Train all models and register in Snowflake
python train_and_register_models.py

# Force retrain if models exist
python train_and_register_models.py --force-retrain

# Train with custom version
python train_and_register_models.py --version "v2.0"
```

#### 4. Monitor and Fine-Tune

```bash
# Analyze model performance
python model_fine_tuning.py --action analyze --days 7

# Check for concept drift
python model_fine_tuning.py --action drift --days 14

# Manual fine-tuning
python model_fine_tuning.py --action finetune --model spotify_hybrid_recommender

# Run automated check
python model_fine_tuning.py --action autocheck
```

### C. Streamlit App Integration

The ML recommendations are integrated into tab 6 of the Streamlit app:

#### Features:
- 🎯 **Multiple AI Strategies**: Collaborative, content-based, temporal, discovery
- ⚡ **Real-Time Recommendations**: Context-aware suggestions
- 📊 **Analytics Dashboard**: Recommendation performance metrics
- 🔗 **Export Functions**: Download playlists, share recommendations
- 💡 **Quick Actions**: One-click trending, temporal, and discovery recommendations

#### Usage:
1. Navigate to "🤖 ML Recommendations" tab
2. Choose recommendation strategy and parameters
3. Click "🎵 Generate Recommendations"
4. Explore results in playlist, analytics, and export views

## 📊 Available ML Algorithms

### 1. 🧠 Collaborative Filtering
- **Algorithm**: Non-negative Matrix Factorization (NMF)
- **Use Case**: "People with similar taste also like..."
- **Data**: Genre co-occurrence patterns, user preferences
- **Output**: Genre recommendations with similarity scores

### 2. 📈 Content-Based Filtering  
- **Algorithm**: Cosine similarity on feature vectors
- **Use Case**: "Similar to tracks you already love"
- **Data**: Track features (popularity, duration, genre, era)
- **Output**: Track recommendations with similarity scores

### 3. ⏰ Temporal Pattern Recognition
- **Algorithm**: Time-series pattern matching
- **Use Case**: "Perfect for this time of day"
- **Data**: Hour-based listening patterns, weekday/weekend preferences
- **Output**: Context-aware recommendations

### 4. 🔍 Discovery Engine
- **Algorithm**: Novelty scoring with popularity balancing
- **Use Case**: "Discover hidden gems and new genres"
- **Data**: User listening history vs. unexplored content
- **Output**: Discovery recommendations with novelty scores

### 5. 🎯 Hybrid Ensemble
- **Algorithm**: Weighted combination of all approaches
- **Weights**: Collaborative (40%), Content-Based (30%), Temporal (20%), Discovery (10%)
- **Use Case**: Best overall recommendations
- **Output**: Unified recommendations with multi-strategy support

## 🔧 Configuration and Tuning

### Model Parameters

```python
# Collaborative Filtering
nmf_components = 20      # Latent factors
max_iterations = 500     # Training iterations
regularization = 0.1     # L1/L2 regularization

# Content-Based
similarity_threshold = 0.4   # Minimum similarity
feature_weights = {          # Feature importance
    'genre': 0.3,
    'popularity': 0.25,
    'duration': 0.2,
    'era': 0.15,
    'artist': 0.1
}

# Hybrid Ensemble
strategy_weights = {
    'collaborative': 0.4,
    'content_based': 0.3,
    'temporal': 0.2,
    'discovery': 0.1
}
```

### Performance Thresholds

```python
performance_monitoring = {
    'diversity_threshold': 0.7,     # Recommendation diversity
    'engagement_threshold': 0.7,    # User engagement score
    'drift_threshold': 0.2,         # Concept drift detection
    'retraining_frequency': 7       # Days between checks
}
```

## 🧪 Testing and Validation

### 1. Validate Data Setup

```sql
-- Check training data availability
SELECT COUNT(*) FROM ml_user_genre_interactions;
SELECT COUNT(*) FROM ml_track_content_features;
SELECT COUNT(*) FROM ml_temporal_patterns;

-- Verify recommendation views
SELECT COUNT(*) FROM ml_hybrid_recommendations;
```

### 2. Test Recommendation Functions

```sql
-- Test hybrid recommendations
SELECT * FROM TABLE(get_spotify_recommendations(10));

-- Test similarity function
SELECT * FROM TABLE(get_similar_tracks('your_track_id', 5));

-- Test discovery
SELECT * FROM TABLE(get_discovery_recommendations('balanced', 10));

-- Test temporal recommendations
SELECT * FROM TABLE(get_time_based_recommendations(14, false, 10));
```

### 3. Monitor Performance

```sql
-- Check model performance
CALL monitor_model_performance('spotify_hybrid_recommender');

-- View training history
SELECT * FROM ml_training_history ORDER BY training_timestamp DESC;

-- Check recommendation analytics
SELECT * FROM ml_recommendation_analytics;
```

## 🚨 Troubleshooting

### Common Issues

#### 1. **No Recommendations Generated**
```sql
-- Check data availability
SELECT COUNT(*) as tracks, 
       COUNT(DISTINCT primary_genre) as genres,
       COUNT(DISTINCT primary_artist_id) as artists
FROM spotify_analytics.medallion_arch.silver_listening_enriched
WHERE denver_date >= DATEADD('days', -90, CURRENT_DATE);
```
**Solution**: Need at least 50 tracks, 5 genres, 20 artists

#### 2. **Python Training Fails**
```bash
# Check Snowpark ML installation
python -c "import snowflake.ml; print('Snowpark ML OK')"

# Verify Snowflake connection
python -c "from train_and_register_models import create_snowflake_session; print('Connection OK' if create_snowflake_session() else 'Connection Failed')"
```

#### 3. **Low Recommendation Quality**
```sql
-- Analyze recommendation diversity
SELECT * FROM ml_recommendation_analytics;

-- Check for concept drift
CALL monitor_model_performance('spotify_hybrid_recommender', 0.7, 0.2);
```
**Solution**: Retrain models or adjust parameters

#### 4. **Streamlit Errors**
- Ensure all SQL views are created
- Check Snowflake session permissions
- Verify function names match exactly

### Performance Optimization

#### 1. **Improve Training Speed**
```sql
-- Use smaller training windows for development
-- Increase batch sizes for production
-- Consider data sampling for very large datasets
```

#### 2. **Optimize Recommendations**
```sql
-- Add indexes on frequently queried columns
CREATE INDEX idx_track_content_genre ON ml_track_content_features(primary_genre);
CREATE INDEX idx_user_interactions_genre ON ml_user_genre_interactions(primary_genre);
```

#### 3. **Scale for Multiple Users**
```sql
-- Extend user_id concept in training views
-- Implement user-specific model versioning
-- Add user preference caching
```

## 📈 Production Deployment

### 1. **Automated Monitoring**
```sql
-- Start all automation tasks
CALL start_ml_automation();

-- Monitor task status
SHOW TASKS LIKE 'spotify_%';
```

### 2. **Model Versioning**
```sql
-- Deploy new model version
CALL deploy_model_version('spotify_hybrid_recommender', 'v2.1', 'production');

-- Rollback if needed
CALL rollback_model_version('spotify_hybrid_recommender', 'production');
```

### 3. **Performance Tracking**
```sql
-- Set up alerts for performance degradation
-- Monitor recommendation quality metrics
-- Track user engagement with recommendations
```

## 🎵 Usage Examples

### Generate Recommendations

```python
# In Streamlit app or Python script
recommendations = session.sql("""
    SELECT * FROM TABLE(get_spotify_recommendations(30, 14, false, NULL, 0.3))
""").to_pandas()
```

### Analyze Your Taste Profile

```sql
-- Get your music taste summary
SELECT * FROM TABLE(get_user_taste_profile());

-- Find your genre similarities  
SELECT * FROM ml_genre_similarity_matrix 
WHERE from_genre IN (
    SELECT primary_genre FROM ml_user_genre_interactions 
    ORDER BY weighted_preference DESC LIMIT 3
);
```

### Export Recommendations

```python
# Create Spotify playlist format
playlist_urls = recommendations['SPOTIFY_URL'].tolist()
playlist_text = "\n".join([f"{i+1}. {url}" for i, url in enumerate(playlist_urls)])
```

## 🔄 Maintenance Schedule

### Daily
- ✅ Automated view refresh (2:30 AM)
- ✅ Performance monitoring (8:00 AM)

### Weekly  
- ✅ Automated model retraining (Monday 6:00 AM)
- 📊 Review recommendation analytics
- 🔍 Check for concept drift

### Monthly
- 🧪 A/B test new model versions
- 📈 Analyze user engagement trends
- ⚙️ Optimize model parameters
- 🚀 Deploy improvements to production

---

## 🎯 Success Metrics

Your ML recommendation system is working well when you see:

- 🎵 **High Diversity**: 8+ genres in 30 recommendations
- 📊 **Balanced Popularity**: Mix of mainstream (40%), rising (40%), hidden gems (20%)
- ⚡ **Fast Generation**: Recommendations in <5 seconds
- 🎯 **High Relevance**: >0.6 average recommendation score
- 🔄 **Regular Updates**: Models retrain weekly automatically

**🎉 Enjoy your personalized AI-powered music discovery! 🎉**
