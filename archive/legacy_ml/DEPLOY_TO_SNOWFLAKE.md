# 🚀 Deploy ML Recommendations to Snowflake Native Streamlit

**Quick deployment guide for adding ML recommendations to your existing Snowflake Native Streamlit app.**

## ✅ Pre-Flight Checklist

- [ ] Existing Spotify pipeline working in Snowflake
- [ ] Snowflake Native Streamlit app already deployed
- [ ] At least 50+ tracks in `silver_listening_enriched` table (medallion_arch schema)
- [ ] ACCOUNTADMIN or sufficient privileges for creating functions/procedures

## 🛠️ Deployment Steps

### Step 1: Deploy SQL Infrastructure (10 minutes)

Execute these files **in order** in Snowflake SQL worksheets:

#### A. Core ML Engine
```sql
-- File: spotify_ml_recommendation_engine.sql
-- Copy entire contents → Paste in Snowflake → Execute
-- Creates: ML views, similarity matrices, recommendation generation
```

#### B. Automated ML System  
```sql
-- File: automated_model_retraining.sql
-- Copy entire contents → Paste in Snowflake → Execute  
-- Creates: Monitoring, retraining procedures, scheduled tasks
```

#### C. Real-Time Functions
```sql
-- File: model_inference_functions.sql
-- Copy entire contents → Paste in Snowflake → Execute
-- Creates: get_spotify_recommendations(), get_discovery_recommendations(), etc.
```

#### D. Initialize System
```sql
-- Initialize the ML automation system
CALL initialize_ml_automation();

-- Expected result: "✅ ML automation system initialized..."
```

### Step 2: Test SQL Functions (2 minutes)

```sql
-- Quick test of core functions
SELECT 'Testing basic recommendations...' as status;
SELECT COUNT(*) as rec_count FROM TABLE(get_spotify_recommendations(5));

SELECT 'Testing discovery...' as status;
SELECT COUNT(*) as discovery_count FROM TABLE(get_discovery_recommendations('balanced', 3, 70));

-- If both return counts > 0, you're ready for Streamlit!
```

### Step 3: Update Streamlit App (3 minutes)

Your `spotify_analytics_streamlit_app.py` is already updated with:
- ✅ New "🤖 ML Recommendations" tab
- ✅ Snowflake Native session handling
- ✅ Full ML recommendation UI

**Deploy Options:**

#### Option A: Edit Existing App in Snowsight
1. Go to Snowsight → Projects → Streamlit → [Your App]
2. Click "Edit"
3. Replace content with updated `spotify_analytics_streamlit_app.py`
4. Click "Run"

#### Option B: Re-Upload App
1. Download `spotify_analytics_streamlit_app.py` to your computer
2. Snowsight → Projects → Streamlit → Create Streamlit App
3. Upload the file
4. Set Database: `SPOTIFY_ANALYTICS`, Schema: `ANALYTICS`
5. Deploy

### Step 4: Test Your ML System (5 minutes)

1. **Open your Streamlit app**
2. **Look for new "🤖 ML Recommendations" tab**
3. **Click "🎵 Generate Recommendations"**
4. **Try Quick Actions:**
   - 🔥 Trending for Me
   - 🎯 Perfect for Now
   - 🔍 Discover Hidden Gems

### Step 5: Optional - Start Automation (1 minute)

```sql
-- Start automated monitoring and retraining (optional)
CALL start_ml_automation();

-- This enables:
-- • Daily performance monitoring (8 AM)
-- • Weekly model retraining (Monday 6 AM)  
-- • Daily view refresh (2:30 AM)
```

## 🧪 Validation Commands

### Check ML Views Created:
```sql
SELECT 'ml_user_genre_interactions' as view_name, COUNT(*) as rows FROM ml_user_genre_interactions
UNION ALL
SELECT 'ml_track_content_features', COUNT(*) FROM ml_track_content_features
UNION ALL  
SELECT 'ml_temporal_patterns', COUNT(*) FROM ml_temporal_patterns
UNION ALL
SELECT 'ml_hybrid_recommendations', COUNT(*) FROM ml_hybrid_recommendations;

-- All should have rows > 0
```

### Check Functions Created:
```sql
SHOW FUNCTIONS LIKE 'get_spotify%';
-- Should show 4+ functions
```

### Test Recommendations:
```sql
-- Test each recommendation type
SELECT 'Hybrid' as type, COUNT(*) as count FROM TABLE(get_spotify_recommendations(10))
UNION ALL
SELECT 'Discovery', COUNT(*) FROM TABLE(get_discovery_recommendations('balanced', 10, 70))
UNION ALL
SELECT 'Temporal', COUNT(*) FROM TABLE(get_time_based_recommendations(14, false, 10));
```

## 🚨 Troubleshooting

### "Function not found" errors
```sql
-- Check you're in the right database/schema
USE DATABASE SPOTIFY_ANALYTICS;
USE SCHEMA ANALYTICS;

-- Re-run model_inference_functions.sql if needed
```

### "No recommendations generated"
```sql
-- Check data availability
SELECT 
    COUNT(*) as total_tracks,
    COUNT(DISTINCT primary_genre) as genres,
    COUNT(DISTINCT primary_artist_id) as artists
FROM spotify_analytics.medallion_arch.silver_listening_enriched
WHERE denver_date >= DATEADD('days', -90, CURRENT_DATE);

-- Need: 50+ tracks, 5+ genres, 20+ artists
```

### Streamlit "session not defined" 
- Make sure line 11-14 in the Streamlit app has:
```python
from snowflake.snowpark.context import get_active_session
session = get_active_session()
```

### ML tab not appearing
- Check line 774 has all 6 tabs:
```python
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([...])
```

## 📊 Success Indicators

✅ **ML views populated with data**  
✅ **SQL functions return recommendations**  
✅ **New Streamlit tab appears and works**  
✅ **Different strategies produce different results**  
✅ **Recommendations include new artists/genres**  
✅ **Export functionality works**

## ⏱️ Total Time: ~20 minutes

- SQL deployment: 15 minutes
- Streamlit update: 3 minutes  
- Testing: 2 minutes

## 🎯 What You Get

After deployment, you'll have:

- **🧠 5 AI Algorithms**: Collaborative, content-based, temporal, discovery, hybrid
- **⚡ Real-Time Recommendations**: Generated on-demand in Streamlit
- **📊 Analytics Dashboard**: Track recommendation quality and diversity  
- **🔗 Export Functions**: Download playlists, share recommendations
- **🤖 Automated System**: Self-monitoring and retraining capabilities
- **🎵 Personalized Discovery**: Find new artists and genres tailored to your taste

Your personal AI-powered Discover Weekly alternative is ready! 🎵✨
