-- Deploy Spotify Analytics Streamlit App in Snowflake
-- Run this script in Snowflake to create and deploy your analytics dashboard

-- Context setup
USE ROLE SPOTIFY_ANALYST_ROLE;
USE WAREHOUSE SPOTIFY_WH;
USE DATABASE spotify_analytics;

-- =============================================================================
-- STEP 1: CREATE STREAMLIT SCHEMA (IF NOT EXISTS)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS streamlit_apps;
USE SCHEMA streamlit_apps;

-- =============================================================================
-- STEP 2: CREATE THE STREAMLIT APP
-- =============================================================================

-- Create the Streamlit app
-- Note: You'll need to copy/paste the Python code from spotify_analytics_streamlit_app.py
-- into the FROM clause below, or upload it via Snowsight UI

CREATE OR REPLACE STREAMLIT spotify_analytics_dashboard
ROOT_LOCATION = '@spotify_analytics.streamlit_apps.streamlit_stage'
MAIN_FILE = 'spotify_analytics_streamlit_app.py'
QUERY_WAREHOUSE = SPOTIFY_WH
COMMENT = 'Interactive Spotify Analytics Dashboard';

-- =============================================================================
-- STEP 3: GRANT NECESSARY PERMISSIONS
-- =============================================================================

-- Grant usage on the app
GRANT USAGE ON STREAMLIT spotify_analytics_dashboard TO ROLE SPOTIFY_ANALYST_ROLE;

-- Grant access to data schemas
GRANT USAGE ON SCHEMA spotify_analytics.medallion_arch TO ROLE SPOTIFY_ANALYST_ROLE;
GRANT USAGE ON SCHEMA spotify_analytics.raw_data TO ROLE SPOTIFY_ANALYST_ROLE;

-- Grant select permissions on all tables/views in medallion architecture
GRANT SELECT ON ALL TABLES IN SCHEMA spotify_analytics.medallion_arch TO ROLE SPOTIFY_ANALYST_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA spotify_analytics.medallion_arch TO ROLE SPOTIFY_ANALYST_ROLE;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA spotify_analytics.medallion_arch TO ROLE SPOTIFY_ANALYST_ROLE;

-- Grant select on raw data (if needed)
GRANT SELECT ON ALL TABLES IN SCHEMA spotify_analytics.raw_data TO ROLE SPOTIFY_ANALYST_ROLE;

-- =============================================================================
-- STEP 4: SHOW APP DETAILS
-- =============================================================================

-- Show the created app
SHOW STREAMLIT LIKE 'spotify_analytics_dashboard';

-- Get the app URL
SELECT 
    'App successfully created!' as status,
    'Navigate to Snowsight > Streamlit to access your dashboard' as next_step,
    'Or use SHOW STREAMLIT to get the direct URL' as alternative;

-- =============================================================================
-- DEPLOYMENT NOTES
-- =============================================================================

/*
DEPLOYMENT STEPS:

1. Run this SQL script to create the Streamlit app structure

2. Upload the Python file via Snowsight:
   - Go to Snowsight > Streamlit > Create Streamlit App
   - Select your database: spotify_analytics
   - Select schema: streamlit_apps  
   - App name: spotify_analytics_dashboard
   - Copy/paste the contents of spotify_analytics_streamlit_app.py
   - Click "Create App"

3. Access your dashboard:
   - Go to Snowsight > Streamlit
   - Click on "spotify_analytics_dashboard"
   - Enjoy your interactive analytics!

FEATURES INCLUDED:
- 📈 Daily/weekly/monthly listening trends
- 🎨 Genre analysis with pie charts and bar graphs
- 👨‍🎤 Artist discovery and popularity analysis
- ⏰ Time pattern analysis (hourly, time of day, weekends)
- 🔍 Detailed track-level filtering and exploration
- 📊 Interactive filters for date ranges, genres, time periods
- 📱 Responsive design with multiple tabs

DATA SOURCES:
- gold_daily_listening_summary
- gold_genre_analysis  
- gold_monthly_insights
- silver_artist_summary
- silver_listening_enriched

The app automatically uses your existing medallion architecture!
*/
