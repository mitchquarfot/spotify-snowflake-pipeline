# 🔧 Complete S3 Discovery Pipeline Setup Guide

## 🎯 **Goal**
Set up a complete discovery pipeline that:
- ✅ **Rediscovery System**: Working (792 candidates, 50 recommendations)
- 🚀 **New Discovery System**: Fetch completely new tracks from Spotify → S3 → Snowflake

---

## 📋 **Prerequisites Checklist**

**✅ What You Already Have:**
- [x] Working Spotify API credentials
- [x] Existing S3 bucket with listening history pipeline
- [x] Snowflake database with medallion architecture  
- [x] S3Client class and AWS credentials configured
- [x] Rediscovery system deployed and working

**📝 What We Need to Identify:**
- [ ] Your actual S3 bucket name
- [ ] Your actual AWS credentials (same as existing setup)
- [ ] Your Snowflake warehouse name
- [ ] Your existing S3 event notification setup (if any)

---

## 🛠️ **PHASE 1: Configuration Discovery**

### Step 1.1: Find Your S3 Bucket Name

**Run this to find your bucket:**
```bash
# Check your .env file for the bucket name
grep "S3_BUCKET_NAME" .env
```

**Or check your existing Snowflake setup:**
1. Go to your Snowflake worksheet
2. Run: `SHOW STAGES;`  
3. Look for `SPOTIFY_S3_STAGE` 
4. Note the S3 URL (e.g., `s3://your-actual-bucket-name/`)

### Step 1.2: Find Your Warehouse Name

**In Snowflake, run:**
```sql
SHOW WAREHOUSES;
-- Look for the warehouse you use for your existing pipeline
-- Probably something like COMPUTE_WH, ANALYTICS_WH, etc.
```

### Step 1.3: Get Your AWS Credentials

**These should be the same as your existing setup.**
**Check your .env file:**
```bash
grep -E "AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY" .env
```

---

## 🚀 **PHASE 2: Deploy S3 Infrastructure**

### Step 2.1: Update Configuration Files

**Edit `setup_discovery_snowpipe.sql`** and replace:
- `YOUR_BUCKET_NAME` → Your actual S3 bucket name
- `YOUR_ACCESS_KEY` → Your actual AWS access key ID  
- `YOUR_SECRET_KEY` → Your actual AWS secret access key
- `YOUR_WAREHOUSE_NAME` → Your actual Snowflake warehouse name

### Step 2.2: Deploy Snowflake Infrastructure

**In Snowflake, run:**
```sql
-- Run the entire setup_discovery_snowpipe.sql script
-- This creates:
-- ✅ Tables: raw_spotify_discoveries, ml_spotify_discoveries  
-- ✅ Stage: discovery_s3_stage (pointing to s3://bucket/spotify_discoveries/)
-- ✅ Pipe: discovery_snowpipe (auto-ingestion)
-- ✅ Task: process_new_discoveries (data processing)
```

### Step 2.3: Configure S3 Event Notifications

**Get the SQS Queue ARN:**
```sql
-- In Snowflake, run this to get the SQS queue:
SELECT SYSTEM$PIPE_STATUS('discovery_snowpipe');
```

**In AWS S3 Console:**
1. Go to your S3 bucket → Properties → Event Notifications
2. Create new event notification:
   - **Name**: `spotify-discoveries-notification`
   - **Prefix**: `spotify_discoveries/`
   - **Event types**: `s3:ObjectCreated:*`
   - **Destination**: SQS Queue
   - **SQS queue ARN**: (from the query above)

---

## 🧪 **PHASE 3: Test Infrastructure**

### Step 3.1: Test Snowflake Infrastructure

**Run in Snowflake:**
```sql
-- Run: test_s3_discovery_pipeline.sql
-- Should show:
-- ✅ Tables created
-- ✅ Stage created  
-- ✅ Pipe created
-- ✅ Task running
```

### Step 3.2: Test S3 Connectivity

**Run locally:**
```bash
source venv/bin/activate
python -c "
from s3_client import S3Client
s3 = S3Client()
print('S3 Client initialized successfully!')
print(f'Bucket: {s3.bucket_name}')
"
```

---

## 🎵 **PHASE 4: Generate User Profile**

### Step 4.1: Analyze Your Listening Patterns

**In Snowflake, run:**
```sql
-- Run: generate_user_profile.sql
-- This analyzes your listening history and shows:
-- 📊 Top genres for discovery seeds
-- 🎤 Top artists for discovery seeds  
-- 📈 Your listening preferences (popularity, duration, eras)
```

### Step 4.2: Create Profile File (Optional)

**Copy the JSON output from Step 4.1 to:**
```bash
# Create user_music_profile.json with the JSON output
# This gives more personalized recommendations
# (Script will use defaults if file doesn't exist)
```

---

## 🚀 **PHASE 5: Run Discovery Pipeline**

### Step 5.1: Execute Discovery Script

**Run the Python discovery:**
```bash
source venv/bin/activate
python spotify_discovery_system.py
```

**Expected Output:**
```
🎵 SPOTIFY DISCOVERY COMPLETE 🎵

📊 Discovered 50 new tracks
🎭 Based on your top genres: rock, pop, indie
🎤 Based on your top artists: [Your top artists]
📈 Average popularity: 65.2
☁️  Saved to S3 - Snowpipe will auto-ingest!
```

### Step 5.2: Verify Data Ingestion

**In Snowflake, check:**
```sql
-- Check raw ingestion
SELECT COUNT(*) FROM raw_spotify_discoveries;

-- Check processed discoveries  
SELECT COUNT(*) FROM ml_spotify_discoveries;

-- Sample discovered tracks
SELECT track_name, primary_artist_name, discovery_strategy 
FROM ml_spotify_discoveries 
ORDER BY discovered_at DESC 
LIMIT 10;
```

---

## 🎯 **PHASE 6: Deploy Recommendation Views**

### Step 6.1: Create Discovery Recommendation Views

**In Snowflake, run:**
```sql
-- Run: discovery_recommendation_views.sql
-- Creates views:
-- ✅ ml_smart_discovery_recommendations
-- ✅ ml_discovery_analytics  
-- ✅ ml_top_discovery_recommendations
```

### Step 6.2: Get Your New Music!

**Query your recommendations:**
```sql
-- Get top 20 new track recommendations
SELECT * FROM ml_top_discovery_recommendations LIMIT 20;

-- See analytics dashboard
SELECT * FROM ml_discovery_analytics;

-- Combined recommendations (rediscovery + new discovery)
SELECT 
    track_name,
    primary_artist_name,
    CASE 
        WHEN track_id IN (SELECT track_id FROM ml_spotify_discoveries) 
        THEN '🆕 New Discovery' 
        ELSE '🔄 Rediscovery' 
    END as type,
    'Listen: https://open.spotify.com/track/' || track_id as spotify_link
FROM ml_rediscovery_collaborative
ORDER BY rediscovery_score DESC
LIMIT 10

UNION ALL

SELECT 
    track_name,
    primary_artist_name, 
    '🆕 New Discovery' as type,
    spotify_link
FROM ml_top_discovery_recommendations
LIMIT 10;
```

---

## 🎉 **PHASE 7: Automation & Maintenance**

### Step 7.1: Schedule Regular Discovery

**Add to cron or task scheduler:**
```bash
# Run weekly discovery (Sundays at 10 AM)
0 10 * * 0 cd /path/to/project && source venv/bin/activate && python spotify_discovery_system.py
```

### Step 7.2: Monitor Pipeline Health

**Create monitoring dashboard queries:**
```sql
-- Discovery pipeline health check
SELECT 
    'Recent Discoveries' as metric,
    COUNT(*) as value,
    'Last 7 days' as period
FROM ml_spotify_discoveries 
WHERE created_at >= DATEADD('days', -7, CURRENT_TIMESTAMP())

UNION ALL

SELECT 
    'Active Recommendations' as metric,
    COUNT(*) as value,
    'Current' as period  
FROM ml_smart_discovery_recommendations

UNION ALL

SELECT 
    'Pipeline Status' as metric,
    CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END as value,
    'Operational' as period
FROM information_schema.pipes 
WHERE pipe_name = 'DISCOVERY_SNOWPIPE' 
AND pipe_state = 'RUNNING';
```

---

## 📊 **Expected Final Results**

**✅ What You'll Have:**
- **Rediscovery System**: 792 candidates → 50 recommendations from your library
- **New Discovery System**: 50+ completely new tracks from Spotify  
- **Automated Pipeline**: Weekly discovery of new music
- **Analytics Dashboard**: Track discovery performance
- **Combined Recommendations**: Best of both systems

**🎵 Total Music Discovery Power:**
- **100+ recommendations** updated regularly
- **Personalized** based on your actual listening patterns
- **Diverse** mix of familiar and completely new music
- **Automated** pipeline requiring minimal maintenance

---

## 🆘 **Troubleshooting**

### Common Issues:

**1. S3 Permission Errors:**
- Verify AWS credentials in .env match existing setup
- Check S3 bucket policy allows your AWS user

**2. Snowpipe Not Ingesting:**
- Verify S3 event notifications configured correctly
- Check pipe status: `SELECT SYSTEM$PIPE_STATUS('discovery_snowpipe');`
- Manual refresh: `ALTER PIPE discovery_snowpipe REFRESH;`

**3. No Recommendations Generated:**
- Check if discoveries exist: `SELECT COUNT(*) FROM ml_spotify_discoveries;`
- Check user profile: Run `generate_user_profile.sql`
- Verify Spotify API credentials still valid

**4. Python Script Errors:**
- Check virtual environment activated
- Verify .env file has all required values
- Test S3 connectivity separately

---

## 🎯 **Next Steps After Setup**

1. **🎵 Use Your Recommendations**: Query the recommendation views daily
2. **📊 Monitor Performance**: Check analytics weekly
3. **🔄 Provide Feedback**: Update user_feedback column for tracks you like/dislike  
4. **📈 Scale Up**: Increase discovery limits, add more strategies
5. **🎨 Integrate**: Add recommendations to your Streamlit app

**You now have a complete, production-ready music discovery system!** 🚀
