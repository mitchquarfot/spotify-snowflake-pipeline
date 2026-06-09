-- =============================================================================
-- Deploy Spotify Analytics Streamlit App (SiS - Streamlit-in-Snowflake)
--
-- Prerequisites:
--   - SPOTIFY_ANALYTICS database exists
--   - SPOTIFY_WH warehouse exists
--   - User has ACCOUNTADMIN or CREATE STREAMLIT privilege
--
-- After running this SQL, upload files to the stage:
--   PUT file://app/spotify_analytics_streamlit_app.py @SPOTIFY_ANALYTICS.STREAMLIT_APPS.STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--   PUT file://app/spotify_semantic_model.yml        @SPOTIFY_ANALYTICS.STREAMLIT_APPS.STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE SPOTIFY_WH;
USE DATABASE SPOTIFY_ANALYTICS;

-- =============================================================================
-- STEP 1: Create schema and internal stage for the Streamlit app files
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS SPOTIFY_ANALYTICS.STREAMLIT_APPS;
USE SCHEMA STREAMLIT_APPS;

CREATE STAGE IF NOT EXISTS SPOTIFY_ANALYTICS.STREAMLIT_APPS.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Spotify Analytics SiS app files';

-- =============================================================================
-- STEP 2: Upload files (run these from SnowSQL or Snowsight worksheet)
-- =============================================================================

-- PUT file://app/spotify_analytics_streamlit_app.py
--     @SPOTIFY_ANALYTICS.STREAMLIT_APPS.STREAMLIT_STAGE
--     AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
--
-- PUT file://app/spotify_semantic_model.yml
--     @SPOTIFY_ANALYTICS.STREAMLIT_APPS.STREAMLIT_STAGE
--     AUTO_COMPRESS = FALSE OVERWRITE = TRUE;

-- =============================================================================
-- STEP 3: Create or replace the Streamlit application object
-- =============================================================================

CREATE OR REPLACE STREAMLIT SPOTIFY_ANALYTICS.STREAMLIT_APPS.SPOTIFY_DASHBOARD
    ROOT_LOCATION  = '@SPOTIFY_ANALYTICS.STREAMLIT_APPS.STREAMLIT_STAGE'
    MAIN_FILE      = 'spotify_analytics_streamlit_app.py'
    QUERY_WAREHOUSE = SPOTIFY_WH
    COMMENT        = 'Interactive Spotify Analytics Dashboard with Cortex AI Discovery and NL Q&A';

-- =============================================================================
-- STEP 4: Grant access
-- =============================================================================

-- Grant to a dedicated analyst role (create if needed)
CREATE ROLE IF NOT EXISTS SPOTIFY_ANALYST_ROLE;

GRANT USAGE ON STREAMLIT SPOTIFY_ANALYTICS.STREAMLIT_APPS.SPOTIFY_DASHBOARD
    TO ROLE SPOTIFY_ANALYST_ROLE;

-- Data access grants
GRANT USAGE ON DATABASE SPOTIFY_ANALYTICS TO ROLE SPOTIFY_ANALYST_ROLE;
GRANT USAGE ON SCHEMA SPOTIFY_ANALYTICS.MEDALLION_ARCH TO ROLE SPOTIFY_ANALYST_ROLE;
GRANT USAGE ON SCHEMA SPOTIFY_ANALYTICS.RAW_DATA TO ROLE SPOTIFY_ANALYST_ROLE;
GRANT USAGE ON SCHEMA SPOTIFY_ANALYTICS.ANALYTICS TO ROLE SPOTIFY_ANALYST_ROLE;
GRANT USAGE ON SCHEMA SPOTIFY_ANALYTICS.STREAMLIT_APPS TO ROLE SPOTIFY_ANALYST_ROLE;

GRANT SELECT ON ALL TABLES IN SCHEMA SPOTIFY_ANALYTICS.MEDALLION_ARCH TO ROLE SPOTIFY_ANALYST_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA SPOTIFY_ANALYTICS.MEDALLION_ARCH TO ROLE SPOTIFY_ANALYST_ROLE;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA SPOTIFY_ANALYTICS.MEDALLION_ARCH TO ROLE SPOTIFY_ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA SPOTIFY_ANALYTICS.RAW_DATA TO ROLE SPOTIFY_ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA SPOTIFY_ANALYTICS.ANALYTICS TO ROLE SPOTIFY_ANALYST_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA SPOTIFY_ANALYTICS.ANALYTICS TO ROLE SPOTIFY_ANALYST_ROLE;

GRANT USAGE ON WAREHOUSE SPOTIFY_WH TO ROLE SPOTIFY_ANALYST_ROLE;

-- Grant the analyst role to the current user
GRANT ROLE SPOTIFY_ANALYST_ROLE TO USER MQUARFOT;

-- =============================================================================
-- STEP 5: Verify deployment
-- =============================================================================

SHOW STREAMLITS IN SCHEMA SPOTIFY_ANALYTICS.STREAMLIT_APPS;

SELECT
    'Deployment complete' AS status,
    'Navigate to Snowsight > Streamlit to access your dashboard' AS next_step;
