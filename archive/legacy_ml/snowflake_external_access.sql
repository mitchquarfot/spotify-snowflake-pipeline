-- Snowflake External Access Integration for Spotify API
-- This allows direct API calls to Spotify from Snowflake sessions
-- Prerequisites: ACCOUNTADMIN role and Snowflake Enterprise Edition or higher

-- Switch to ACCOUNTADMIN role (required for creating integrations)
USE ROLE ACCOUNTADMIN;

-- 1. Create Network Rule for Spotify API endpoints
CREATE OR REPLACE NETWORK RULE spotify_api_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'api.spotify.com:443',
    'accounts.spotify.com:443'
  )
  COMMENT = 'Network rule allowing access to Spotify API endpoints';

-- 2. Create Secret for Spotify Client Credentials
-- Replace YOUR_CLIENT_ID and YOUR_CLIENT_SECRET with actual values
CREATE OR REPLACE SECRET spotify_client_credentials
  TYPE = GENERIC_STRING
  SECRET_STRING = '{
    "client_id": "YOUR_CLIENT_ID",
    "client_secret": "YOUR_CLIENT_SECRET"
  }'
  COMMENT = 'Spotify API client credentials for OAuth authentication';

-- 3. Create External Access Integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION spotify_api_integration
  ALLOWED_NETWORK_RULES = (spotify_api_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (spotify_client_credentials)
  ENABLED = TRUE
  COMMENT = 'Integration for accessing Spotify Web API from Snowflake';

-- 4. Grant usage on the integration to your database role
-- Replace 'your_database_role' with your actual role name
-- GRANT USAGE ON INTEGRATION spotify_api_integration TO ROLE your_database_role;

-- 5. Switch to your working database and schema
USE DATABASE spotify_analytics;
USE SCHEMA raw_data;

-- 6. Create a UDF to get Spotify access token
CREATE OR REPLACE FUNCTION get_spotify_access_token()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'get_access_token'
EXTERNAL_ACCESS_INTEGRATIONS = (spotify_api_integration)
SECRETS = ('cred' = spotify_client_credentials)
PACKAGES = ('requests')
AS
$$
import _snowflake
import requests
import json

def get_access_token():
    """Get OAuth access token using client credentials flow."""
    try:
        # Get credentials from secret
        cred_json = _snowflake.get_generic_secret_string('cred')
        credentials = json.loads(cred_json)
        
        # Spotify token endpoint
        token_url = "https://accounts.spotify.com/api/token"
        
        # Request access token using client credentials flow
        auth_response = requests.post(token_url, {
            'grant_type': 'client_credentials',
            'client_id': credentials['client_id'],
            'client_secret': credentials['client_secret'],
        })
        
        if auth_response.status_code == 200:
            token_data = auth_response.json()
            return token_data['access_token']
        else:
            return f"Error: {auth_response.status_code} - {auth_response.text}"
            
    except Exception as e:
        return f"Error getting access token: {str(e)}"
$$;

-- 7. Create UDF to fetch artist details by ID
CREATE OR REPLACE FUNCTION get_artist_details(artist_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'get_artist_info'
EXTERNAL_ACCESS_INTEGRATIONS = (spotify_api_integration)
SECRETS = ('cred' = spotify_client_credentials)
PACKAGES = ('requests')
AS
$$
import _snowflake
import requests
import json

def get_artist_info(artist_id):
    """Get detailed artist information from Spotify API."""
    try:
        # Get credentials from secret
        cred_json = _snowflake.get_generic_secret_string('cred')
        credentials = json.loads(cred_json)
        
        # Get access token
        token_url = "https://accounts.spotify.com/api/token"
        auth_response = requests.post(token_url, {
            'grant_type': 'client_credentials',
            'client_id': credentials['client_id'],
            'client_secret': credentials['client_secret'],
        })
        
        if auth_response.status_code != 200:
            return {"error": f"Auth failed: {auth_response.status_code}"}
        
        access_token = auth_response.json()['access_token']
        
        # Get artist details
        artist_url = f"https://api.spotify.com/v1/artists/{artist_id}"
        headers = {'Authorization': f'Bearer {access_token}'}
        
        artist_response = requests.get(artist_url, headers=headers)
        
        if artist_response.status_code == 200:
            return artist_response.json()
        else:
            return {"error": f"Artist API failed: {artist_response.status_code}"}
            
    except Exception as e:
        return {"error": f"Exception: {str(e)}"}
$$;

-- 8. Create UDF to fetch multiple artists in batch
CREATE OR REPLACE FUNCTION get_multiple_artists(artist_ids ARRAY)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'get_artists_batch'
EXTERNAL_ACCESS_INTEGRATIONS = (spotify_api_integration)
SECRETS = ('cred' = spotify_client_credentials)
PACKAGES = ('requests')
AS
$$
import _snowflake
import requests
import json

def get_artists_batch(artist_ids):
    """Get details for multiple artists (up to 50)."""
    try:
        # Get credentials from secret
        cred_json = _snowflake.get_generic_secret_string('cred')
        credentials = json.loads(cred_json)
        
        # Get access token
        token_url = "https://accounts.spotify.com/api/token"
        auth_response = requests.post(token_url, {
            'grant_type': 'client_credentials',
            'client_id': credentials['client_id'],
            'client_secret': credentials['client_secret'],
        })
        
        if auth_response.status_code != 200:
            return {"error": f"Auth failed: {auth_response.status_code}"}
        
        access_token = auth_response.json()['access_token']
        
        # Convert array to comma-separated string (max 50 IDs)
        ids_string = ','.join(artist_ids[:50])
        
        # Get multiple artists
        artists_url = f"https://api.spotify.com/v1/artists?ids={ids_string}"
        headers = {'Authorization': f'Bearer {access_token}'}
        
        artists_response = requests.get(artists_url, headers=headers)
        
        if artists_response.status_code == 200:
            return artists_response.json()
        else:
            return {"error": f"Artists API failed: {artists_response.status_code}"}
            
    except Exception as e:
        return {"error": f"Exception: {str(e)}"}
$$;

-- 9. Create stored procedure to refresh artist genre data
CREATE OR REPLACE PROCEDURE refresh_artist_genres(artist_limit INTEGER DEFAULT 100)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    artist_count INTEGER;
    result_msg STRING;
BEGIN
    -- Get artists that need genre refresh (older than 30 days or missing genres)
    CREATE OR REPLACE TEMPORARY TABLE artists_to_refresh AS
    SELECT artist_id, artist_name
    FROM spotify_artist_genres 
    WHERE (ingested_at < DATEADD('day', -30, CURRENT_TIMESTAMP()) 
           OR genre_count = 0 
           OR primary_genre IS NULL)
    AND artist_id IS NOT NULL
    LIMIT :artist_limit;
    
    SELECT COUNT(*) INTO :artist_count FROM artists_to_refresh;
    
    IF (artist_count > 0) THEN
        -- Update artist data with fresh API calls
        UPDATE spotify_artist_genres 
        SET 
            genres = get_artist_details(artist_id):genres,
            genres_list = get_artist_details(artist_id):genres,
            primary_genre = get_artist_details(artist_id):genres[0]::STRING,
            genre_count = ARRAY_SIZE(get_artist_details(artist_id):genres),
            popularity = get_artist_details(artist_id):popularity,
            followers_total = get_artist_details(artist_id):followers.total,
            ingested_at = CURRENT_TIMESTAMP(),
            data_source = 'spotify_api_snowflake_refresh'
        WHERE artist_id IN (SELECT artist_id FROM artists_to_refresh);
        
        result_msg := 'Refreshed ' || artist_count || ' artists';
    ELSE
        result_msg := 'No artists need refreshing';
    END IF;
    
    RETURN result_msg;
END;
$$;

-- 10. Create view for real-time artist lookups
CREATE OR REPLACE VIEW artist_realtime_lookup AS
SELECT 
    h.primary_artist_id,
    h.primary_artist_name,
    -- Real-time API call for fresh data
    get_artist_details(h.primary_artist_id) as live_artist_data,
    get_artist_details(h.primary_artist_id):genres as live_genres,
    get_artist_details(h.primary_artist_id):popularity as live_popularity,
    COUNT(*) as play_count
FROM spotify_listening_history h
WHERE h.primary_artist_id IS NOT NULL
GROUP BY h.primary_artist_id, h.primary_artist_name
ORDER BY play_count DESC;

-- 11. Create task to auto-refresh stale artist data (optional)
CREATE OR REPLACE TASK refresh_stale_artists_task
WAREHOUSE = 'COMPUTE_WH'  -- Replace with your warehouse name
SCHEDULE = 'USING CRON 0 2 * * 0 UTC'  -- Weekly on Sunday at 2 AM UTC
AS
CALL refresh_artist_genres(50);  -- Refresh up to 50 artists per week

-- Uncomment to enable the task:
-- ALTER TASK refresh_stale_artists_task RESUME;

-- 12. Sample usage queries

-- Test access token generation
-- SELECT get_spotify_access_token();

-- Get details for a specific artist
-- SELECT get_artist_details('4Z8W4fKeB5YxbusRsdQVPb');  -- Queen

-- Get multiple artists at once
-- SELECT get_multiple_artists(['4Z8W4fKeB5YxbusRsdQVPb', '1dfeR4HaWDbWqFHLkxsg1d']);

-- Refresh outdated artist data
-- CALL refresh_artist_genres(10);

-- Get real-time data for your top artists (careful - makes API calls!)
-- SELECT * FROM artist_realtime_lookup LIMIT 5;

-- Find artists missing genre data and refresh them
-- SELECT artist_id, artist_name, get_artist_details(artist_id):genres as fresh_genres
-- FROM spotify_artist_genres 
-- WHERE genre_count = 0 
-- LIMIT 5;

-- 13. Grant permissions (adjust roles as needed)
-- GRANT USAGE ON FUNCTION get_spotify_access_token() TO ROLE your_role;
-- GRANT USAGE ON FUNCTION get_artist_details(STRING) TO ROLE your_role;
-- GRANT USAGE ON FUNCTION get_multiple_artists(ARRAY) TO ROLE your_role;
-- GRANT USAGE ON PROCEDURE refresh_artist_genres(INTEGER) TO ROLE your_role;

-- Setup complete!
-- Next steps:
-- 1. Replace YOUR_CLIENT_ID and YOUR_CLIENT_SECRET with actual values
-- 2. Update role names in GRANT statements
-- 3. Test the functions with sample queries
-- 4. Consider enabling the auto-refresh task
