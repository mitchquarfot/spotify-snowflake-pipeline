# 🎵 Spotify Analytics Streamlit App Deployment Guide

Deploy your interactive Spotify analytics dashboard directly in Snowflake!

## 🚀 Quick Deployment Steps

### 1. **Upload the App via Snowsight** (Recommended)

**Option A: Via Snowsight UI**
1. Open [Snowsight](https://app.snowflake.com) 
2. Navigate to **Streamlit** in the left sidebar
3. Click **"+ Streamlit App"**
4. Configure:
   - **Database**: `spotify_analytics`
   - **Schema**: `streamlit_apps` (will be created)
   - **App Name**: `spotify_analytics_dashboard`
   - **Warehouse**: `SPOTIFY_WH`
5. Copy/paste the entire contents of `spotify_analytics_streamlit_app.py`
6. Click **"Create App"**
7. 🎉 Your dashboard is live!

**Option B: Via SQL + File Upload**
1. Run `deploy_streamlit_app.sql` in Snowflake
2. Upload `spotify_analytics_streamlit_app.py` via the Snowsight UI

### 2. **Access Your Dashboard**

After deployment:
- Go to **Snowsight > Streamlit**
- Click on **"spotify_analytics_dashboard"**
- Start exploring your music data! 🎶

## 📊 Dashboard Features

### **5 Interactive Tabs:**

#### 📈 **Trends Tab**
- Daily listening activity line chart
- Weekly pattern analysis  
- Monthly listening trends
- Genre diversity tracking over time

#### 🎨 **Genres Tab**
- Top genres pie chart
- Genre play counts bar chart
- Detailed genre statistics table
- Percentage of total listening time

#### 👨‍🎤 **Artists Tab**
- Most played artists ranking
- Artist discovery scatter plot (listening time vs track diversity)
- Artist details with popularity metrics
- Weekend listening percentage per artist

#### ⏰ **Time Patterns Tab**
- Hourly listening activity
- Time of day distribution (Morning/Afternoon/Evening/Night)
- Weekend vs weekday comparison
- Listening source analysis (playlist/album/artist)

#### 🔍 **Detailed View Tab**
- Track-level data exploration
- Advanced filtering and sorting
- Exportable data tables
- Summary statistics

### **🎛️ Interactive Filters (Sidebar):**
- **📅 Date Range**: Filter any time period
- **🎨 Genre**: Focus on specific music genres  
- **⏰ Time of Day**: Morning, afternoon, evening, or night listening
- **📅 Weekend Filter**: Weekends only, weekdays only, or all days

### **📊 Key Metrics Dashboard:**
- 🎵 Total Plays
- 🎤 Unique Tracks  
- 👨‍🎤 Unique Artists
- ⏱️ Hours Listened
- 📊 Average Daily Plays

## 🏗️ Architecture Integration

The app leverages your **existing medallion architecture**:

```sql
-- Data Sources Used:
gold_daily_listening_summary     -- Daily aggregations
gold_genre_analysis              -- Genre insights  
gold_monthly_insights            -- Monthly trends
silver_artist_summary            -- Artist details
silver_listening_enriched        -- Detailed track data
```

**No additional data processing needed!** The app queries your existing Gold and Silver layer tables.

## 🔧 Troubleshooting

### **Issue**: App won't load
**Solution**: Verify permissions:
```sql
-- Grant necessary permissions
GRANT USAGE ON SCHEMA spotify_analytics.medallion_arch TO ROLE SPOTIFY_ANALYST_ROLE;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA spotify_analytics.medallion_arch TO ROLE SPOTIFY_ANALYST_ROLE;
```

### **Issue**: No data showing
**Solution**: Check if your medallion architecture tables have data:
```sql
-- Verify data exists
SELECT COUNT(*) FROM spotify_analytics.medallion_arch.gold_daily_listening_summary;
SELECT COUNT(*) FROM spotify_analytics.medallion_arch.silver_listening_enriched;
```

### **Issue**: Performance issues
**Solution**: The app uses `@st.cache_data` for performance. If needed:
1. Clear cache via Streamlit app settings
2. Increase warehouse size temporarily
3. Limit date ranges for large datasets

## 📱 Usage Tips

### **Best Practices:**
- **Start with recent data**: Use last 30-90 days for faster loading
- **Use genre filters**: Focus analysis on specific music styles
- **Compare time periods**: Use weekend vs weekday filters for insights
- **Export data**: Use the detailed view tab for data extraction

### **Cool Discoveries You Can Make:**
- 🕐 **Peak listening hours**: When do you listen most?
- 🎵 **Genre evolution**: How have your tastes changed over time?
- 📅 **Weekend patterns**: Do you listen differently on weekends?
- 🎤 **Artist loyalty**: Which artists do you return to most?
- 🔍 **Discovery rate**: How many new artists do you find monthly?

## 🎯 Next Steps

### **Customize Your Dashboard:**
1. **Add new visualizations**: Modify the Python code to add charts
2. **Create new metrics**: Add calculated fields for deeper insights  
3. **Integrate more data**: Connect additional Spotify API endpoints
4. **Share insights**: Export charts or create scheduled reports

### **Advanced Features to Add:**
- 📊 **Recommendation engine**: Suggest new music based on patterns
- 🎵 **Mood analysis**: Correlate listening with time/weather data
- 📈 **Prediction models**: Forecast your music taste evolution
- 🔗 **Social features**: Compare with friends' listening patterns

## 🎉 Enjoy Your Analytics!

Your personal Spotify data has never been more insightful. Explore patterns, discover trends, and understand your music journey like never before!

**Happy analyzing! 🎵📊**

---

*Built with ❤️ using Snowflake Native Streamlit and your personal listening data*
