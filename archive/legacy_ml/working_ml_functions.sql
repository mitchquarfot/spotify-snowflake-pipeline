-- WORKING ML FUNCTIONS - SIMPLIFIED SNOWFLAKE SYNTAX
USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- =====================================================================
-- APPROACH 1: USE VIEWS INSTEAD OF TABLE FUNCTIONS
-- =====================================================================

-- Simple recommendation view (works around UDF table function issues)
CREATE OR REPLACE VIEW get_top_recommendations AS
SELECT 
    TRACK_ID as track_id,
    TRACK_NAME as track_name,
    PRIMARY_ARTIST_NAME as artist_name,
    PRIMARY_GENRE as genre,
    ALBUM_NAME as album_name,
    TRACK_POPULARITY as track_popularity,
    final_recommendation_score as recommendation_score,
    playlist_position
FROM ml_hybrid_recommendations_simple
WHERE final_recommendation_score >= 0.3
ORDER BY final_recommendation_score DESC
LIMIT 30;

-- =====================================================================
-- APPROACH 2: SCALAR FUNCTIONS (THESE USUALLY WORK)
-- =====================================================================

-- Get recommendation count
CREATE OR REPLACE FUNCTION get_recommendation_count()
RETURNS INTEGER
LANGUAGE SQL
AS
$$
    SELECT COUNT(*) FROM ml_hybrid_recommendations_simple
$$;

-- Get max recommendation score
CREATE OR REPLACE FUNCTION get_max_recommendation_score()
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    SELECT MAX(final_recommendation_score) FROM ml_hybrid_recommendations_simple
$$;

-- =====================================================================
-- APPROACH 3: STORED PROCEDURES (ALTERNATIVE TO TABLE FUNCTIONS)
-- =====================================================================

CREATE OR REPLACE PROCEDURE get_recommendations_proc(
    num_recs INTEGER DEFAULT 30,
    min_score FLOAT DEFAULT 0.3
)
RETURNS TABLE(
    track_id STRING,
    track_name STRING,
    artist_name STRING,
    recommendation_score FLOAT
)
LANGUAGE SQL
AS
$$
DECLARE
    result_cursor CURSOR FOR 
        SELECT 
            TRACK_ID::STRING as track_id,
            TRACK_NAME::STRING as track_name,
            PRIMARY_ARTIST_NAME::STRING as artist_name,
            final_recommendation_score::FLOAT as recommendation_score
        FROM ml_hybrid_recommendations_simple
        WHERE final_recommendation_score >= min_score
        ORDER BY final_recommendation_score DESC
        LIMIT num_recs;
BEGIN
    OPEN result_cursor;
    RETURN TABLE(result_cursor);
END;
$$;

-- =====================================================================
-- APPROACH 4: SIMPLER TABLE FUNCTION SYNTAX
-- =====================================================================

-- If table functions work at all, try this simpler syntax
CREATE OR REPLACE FUNCTION simple_recommendations()
RETURNS TABLE(track_name STRING, score FLOAT)
LANGUAGE SQL
AS
$$
SELECT 
    TRACK_NAME::STRING,
    final_recommendation_score::FLOAT
FROM ml_hybrid_recommendations_simple
LIMIT 10
$$;

-- =====================================================================
-- TESTING QUERIES
-- =====================================================================

-- Test the view approach
SELECT * FROM get_top_recommendations LIMIT 5;

-- Test scalar functions
SELECT 
    get_recommendation_count() as total_recs,
    get_max_recommendation_score() as max_score;

-- Test stored procedure (call syntax)
CALL get_recommendations_proc(10, 0.4);

-- Test simple table function (if it works)
SELECT * FROM TABLE(simple_recommendations());

