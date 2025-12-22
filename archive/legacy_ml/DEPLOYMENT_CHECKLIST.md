# ✅ ML System Deployment Checklist

**Follow this checklist to deploy ML recommendations to your existing Snowflake Native Streamlit app.**

## 📋 Pre-Deployment Checklist

- [ ] **Existing Spotify pipeline is working**
  - [ ] Data flowing into `silver_listening_enriched` table in `medallion_arch` schema
  - [ ] At least 50+ tracks collected
  - [ ] Multiple genres and artists in data

- [ ] **Snowflake permissions**
  - [ ] ACCOUNTADMIN role OR sufficient privileges to create functions/procedures
  - [ ] Access to `spotify_analytics` database
  - [ ] Can create functions, procedures, tasks, tables

- [ ] **Streamlit app deployed**
  - [ ] Current Streamlit app working in Snowflake
  - [ ] Know how to edit/update the app

## 🚀 Deployment Steps (20 minutes total)

### Step 1: Deploy SQL Infrastructure (15 minutes)

#### 1.1 Core ML Engine (5 minutes)
- [ ] Open `spotify_ml_recommendation_engine.sql`
- [ ] Copy entire contents
- [ ] Paste into Snowflake SQL worksheet
- [ ] Execute (should take 2-3 minutes)
- [ ] Verify: No errors, sees "Views created successfully" messages

#### 1.2 Automated ML System (3 minutes)  
- [ ] Open `automated_model_retraining.sql`
- [ ] Copy entire contents
- [ ] Paste into Snowflake SQL worksheet
- [ ] Execute
- [ ] Verify: No errors, procedures created

#### 1.3 Real-Time Functions (3 minutes)
- [ ] Open `model_inference_functions.sql`
- [ ] Copy entire contents
- [ ] Paste into Snowflake SQL worksheet
- [ ] Execute
- [ ] Verify: No errors, functions created

#### 1.4 Initialize System (1 minute)
- [ ] Run: `CALL initialize_ml_automation();`
- [ ] Verify: Returns "✅ ML automation system initialized..."

#### 1.5 Quick Validation (3 minutes)
- [ ] Run: `deploy_ml_system.sql` (optional validation script)
- [ ] Verify: All checks pass, sample recommendations generated

### Step 2: Update Streamlit App (3 minutes)

Choose **one** option:

#### Option A: Edit Existing App in Snowsight
- [ ] Go to Snowsight → Projects → Streamlit → [Your App Name]
- [ ] Click "Edit"
- [ ] Replace app content with updated `spotify_analytics_streamlit_app.py`
- [ ] Click "Run"
- [ ] Verify: App loads without errors

#### Option B: Upload New App
- [ ] Download `spotify_analytics_streamlit_app.py` to your computer
- [ ] Snowsight → Projects → Streamlit → Create Streamlit App
- [ ] Upload the file
- [ ] Set Database: `spotify_analytics`, Schema: `analytics`
- [ ] Deploy
- [ ] Verify: App loads without errors

### Step 3: Test ML System (2 minutes)

#### 3.1 Test SQL Functions
- [ ] Run: `SELECT COUNT(*) FROM TABLE(get_spotify_recommendations(5));`
- [ ] Verify: Returns count > 0
- [ ] Run: `SELECT COUNT(*) FROM TABLE(get_discovery_recommendations('balanced', 3, 70));`
- [ ] Verify: Returns count > 0

#### 3.2 Test Streamlit App
- [ ] Open your Streamlit app
- [ ] Look for new "🤖 ML Recommendations" tab
- [ ] Click "🎵 Generate Recommendations"
- [ ] Verify: Recommendations appear with track cards
- [ ] Test Quick Actions:
  - [ ] 🔥 Trending for Me
  - [ ] 🎯 Perfect for Now
  - [ ] 🔍 Discover Hidden Gems

## ✅ Success Verification

Your ML system is working correctly when you see:

### SQL Level:
- [ ] `SELECT COUNT(*) FROM ml_user_genre_interactions;` returns > 0
- [ ] `SELECT COUNT(*) FROM ml_hybrid_recommendations;` returns > 0
- [ ] `SHOW FUNCTIONS LIKE 'get_spotify%';` shows 4+ functions
- [ ] Test recommendations return actual tracks

### Streamlit Level:
- [ ] New "🤖 ML Recommendations" tab appears
- [ ] "Generate Recommendations" button works
- [ ] Returns personalized track cards with Spotify links
- [ ] Different strategies produce different results
- [ ] Quick action buttons work
- [ ] Export functionality works

### Quality Checks:
- [ ] Recommendations include artists you've never heard
- [ ] Mix of familiar and new genres
- [ ] Reasonable popularity distribution (not all mainstream/underground)
- [ ] Temporal recommendations change based on time of day

## 🚨 Troubleshooting

### "Function not found" errors
- [ ] Check you're in right database: `USE DATABASE spotify_analytics; USE SCHEMA analytics;`
- [ ] Re-run `model_inference_functions.sql`

### "No recommendations generated"
- [ ] Check data: `SELECT COUNT(*), COUNT(DISTINCT primary_genre) FROM spotify_analytics.medallion_arch.silver_listening_enriched;`
- [ ] Need: 50+ tracks, 5+ genres
- [ ] Collect more listening history if needed

### Streamlit "session not defined"
- [ ] Check line 11-14 has: `from snowflake.snowpark.context import get_active_session; session = get_active_session()`

### ML tab not appearing
- [ ] Check line 774: `tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([...])`
- [ ] Ensure all 6 tabs are listed

## 🎯 Post-Deployment (Optional)

### Start Automation (Optional)
- [ ] Run: `CALL start_ml_automation();`
- [ ] Enables automated monitoring and retraining

### Local ML Training (Optional)
- [ ] Install: `pip install scikit-learn numpy snowflake-ml-python`
- [ ] Set environment variables for Snowflake connection
- [ ] Run: `python train_and_register_models.py`
- [ ] Enhances recommendations with Snowflake Model Registry

### Performance Monitoring (Optional)
- [ ] Run: `CALL monitor_model_performance('spotify_hybrid_recommender');`
- [ ] Check: `SELECT * FROM ml_recommendation_analytics;`

## 🎉 Congratulations!

You now have a **production-grade AI music recommendation system** with:

✅ **5 ML Algorithms** working together  
✅ **Real-time recommendations** in Streamlit  
✅ **Automated monitoring** and retraining  
✅ **Personal Discover Weekly** alternative  
✅ **Advanced music discovery** capabilities  

**Your AI-powered music discovery journey begins now!** 🎵✨

---

**Need help?** Check these files:
- `DEPLOY_TO_SNOWFLAKE.md` - Detailed deployment guide
- `ML_SETUP_GUIDE.md` - Comprehensive ML system documentation
- `validate_ml_setup.py` - Automated validation script
