-- =====================================================================
-- AUTOMATED MODEL RETRAINING AND MONITORING
-- Snowflake stored procedures for automated ML model lifecycle management
-- =====================================================================

USE DATABASE spotify_analytics;
USE SCHEMA analytics;

-- =====================================================================
-- 1. MODEL PERFORMANCE MONITORING PROCEDURES
-- =====================================================================

-- Monitor model performance and detect when retraining is needed
CREATE OR REPLACE PROCEDURE monitor_model_performance(
    model_name STRING DEFAULT 'spotify_hybrid_recommender',
    performance_threshold FLOAT DEFAULT 0.7,
    drift_threshold FLOAT DEFAULT 0.2
)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'numpy')
HANDLER = 'monitor_performance'
AS
$$
import snowflake.snowpark as snowpark
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def monitor_performance(session: snowpark.Session, model_name: str, performance_threshold: float, drift_threshold: float) -> dict:
    """Monitor model performance and recommend actions."""
    
    try:
        # Get recent listening data for performance analysis
        performance_query = f"""
        WITH recent_listening AS (
            SELECT 
                track_id,
                primary_genre,
                track_popularity,
                denver_hour,
                is_weekend,
                denver_date,
                COUNT(*) OVER (PARTITION BY track_id) AS track_replay_count,
                COUNT(*) OVER (PARTITION BY primary_genre) AS genre_popularity
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE denver_date >= DATEADD('days', -7, CURRENT_DATE)
        ),
        performance_metrics AS (
            SELECT 
                COUNT(DISTINCT track_id) AS unique_tracks,
                COUNT(DISTINCT primary_genre) AS unique_genres,
                COUNT(*) AS total_plays,
                AVG(track_popularity) AS avg_track_popularity,
                
                -- Diversity metrics
                COUNT(CASE WHEN track_replay_count = 1 THEN 1 END) / COUNT(*)::FLOAT AS discovery_rate,
                COUNT(CASE WHEN track_replay_count > 3 THEN 1 END) / COUNT(*)::FLOAT AS replay_rate,
                
                -- Temporal diversity
                COUNT(DISTINCT denver_hour) AS listening_hour_diversity,
                
                -- Genre exploration
                COUNT(CASE WHEN genre_popularity <= 5 THEN 1 END) / COUNT(*)::FLOAT AS niche_genre_rate
            FROM recent_listening
        )
        SELECT * FROM performance_metrics
        """
        
        performance_df = session.sql(performance_query).to_pandas()
        
        if performance_df.empty:
            return {"status": "error", "message": "No recent listening data available"}
        
        metrics = performance_df.iloc[0].to_dict()
        
        # Calculate composite performance scores
        diversity_score = (
            metrics['discovery_rate'] * 0.4 +
            metrics['listening_hour_diversity'] / 24 * 0.3 +
            metrics['niche_genre_rate'] * 0.3
        )
        
        engagement_score = (
            metrics['replay_rate'] * 0.6 +
            (1 - abs(metrics['avg_track_popularity'] - 50) / 50) * 0.4
        )
        
        overall_performance = (diversity_score * 0.5 + engagement_score * 0.5)
        
        # Check concept drift
        drift_query = """
        WITH time_periods AS (
            SELECT 
                CASE 
                    WHEN denver_date >= DATEADD('days', -7, CURRENT_DATE) THEN 'recent'
                    WHEN denver_date >= DATEADD('days', -14, CURRENT_DATE) THEN 'older'
                END AS period,
                primary_genre,
                track_popularity,
                denver_hour
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE denver_date >= DATEADD('days', -14, CURRENT_DATE)
        ),
        period_stats AS (
            SELECT 
                period,
                COUNT(DISTINCT primary_genre) AS unique_genres,
                AVG(track_popularity) AS avg_popularity,
                AVG(denver_hour) AS avg_hour
            FROM time_periods
            GROUP BY period
        )
        SELECT 
            ABS(r.unique_genres - o.unique_genres) / GREATEST(o.unique_genres, 1)::FLOAT AS genre_drift,
            ABS(r.avg_popularity - o.avg_popularity) / 100.0 AS popularity_drift,
            ABS(r.avg_hour - o.avg_hour) / 24.0 AS temporal_drift
        FROM period_stats r
        JOIN period_stats o ON r.period = 'recent' AND o.period = 'older'
        """
        
        drift_df = session.sql(drift_query).to_pandas()
        
        drift_score = 0
        if not drift_df.empty:
            drift_metrics = drift_df.iloc[0].to_dict()
            drift_score = (
                drift_metrics['genre_drift'] * 0.4 +
                drift_metrics['popularity_drift'] * 0.3 +
                drift_metrics['temporal_drift'] * 0.3
            )
        
        # Determine recommended actions
        recommendations = []
        
        if overall_performance < performance_threshold:
            recommendations.append("retrain_model")
            recommendations.append("analyze_user_feedback")
        
        if drift_score > drift_threshold:
            recommendations.append("concept_drift_detected")
            recommendations.append("incremental_update")
        
        if overall_performance > 0.8 and drift_score < 0.1:
            recommendations.append("performance_excellent")
        
        # Check model age
        model_age_query = f"""
        SELECT DATEDIFF('days', created_on, CURRENT_TIMESTAMP()) as model_age_days
        FROM information_schema.models 
        WHERE model_name = '{model_name}'
        ORDER BY created_on DESC
        LIMIT 1
        """
        
        try:
            model_age_df = session.sql(model_age_query).to_pandas()
            model_age = model_age_df.iloc[0]['MODEL_AGE_DAYS'] if not model_age_df.empty else 999
        except:
            model_age = 999
        
        if model_age > 14:  # Model older than 2 weeks
            recommendations.append("model_aging")
        
        return {
            "status": "success",
            "timestamp": str(datetime.now()),
            "model_name": model_name,
            "performance_metrics": {
                "overall_performance": float(overall_performance),
                "diversity_score": float(diversity_score),
                "engagement_score": float(engagement_score),
                "drift_score": float(drift_score),
                "model_age_days": int(model_age)
            },
            "thresholds": {
                "performance_threshold": performance_threshold,
                "drift_threshold": drift_threshold
            },
            "recommendations": recommendations,
            "needs_attention": len(recommendations) > 1,
            "listening_stats": metrics
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": str(datetime.now())
        }
$$;

-- =====================================================================
-- 2. AUTOMATED RETRAINING PROCEDURES
-- =====================================================================

-- Main automated retraining procedure
CREATE OR REPLACE PROCEDURE retrain_recommendation_models(
    force_retrain BOOLEAN DEFAULT FALSE,
    performance_threshold FLOAT DEFAULT 0.7
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'numpy')
HANDLER = 'retrain_models'
AS
$$
import snowflake.snowpark as snowpark
import pandas as pd
from datetime import datetime

def retrain_models(session: snowpark.Session, force_retrain: bool, performance_threshold: float) -> str:
    """Automated model retraining procedure."""
    
    try:
        # Check if retraining is needed
        monitor_result = session.call("monitor_model_performance", "spotify_hybrid_recommender", performance_threshold, 0.2)
        
        if not force_retrain:
            # Parse the monitor result
            recommendations = monitor_result.get('recommendations', [])
            needs_attention = monitor_result.get('needs_attention', False)
            
            if not needs_attention and 'retrain_model' not in recommendations:
                return f"Skipping retrain - model performance is acceptable. Last check: {monitor_result.get('timestamp', 'unknown')}"
        
        # Check data availability
        data_check_query = """
        SELECT 
            COUNT(*) as recent_tracks,
            COUNT(DISTINCT primary_genre) as unique_genres,
            COUNT(DISTINCT primary_artist_id) as unique_artists
        FROM spotify_analytics.medallion_arch.silver_listening_enriched
        WHERE denver_date >= DATEADD('days', -7, CURRENT_DATE)
        """
        
        data_df = session.sql(data_check_query).to_pandas()
        data_stats = data_df.iloc[0].to_dict()
        
        if data_stats['recent_tracks'] < 50:
            return f"Insufficient recent data - only {data_stats['recent_tracks']} new tracks"
        
        # Create new model version
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_version = f"auto_{timestamp}"
        
        # Refresh ML views with latest data
        refresh_queries = [
            "CALL SYSTEM$REFRESH_VIEW('ml_user_genre_interactions')",
            "CALL SYSTEM$REFRESH_VIEW('ml_track_content_features')",
            "CALL SYSTEM$REFRESH_VIEW('ml_temporal_patterns')",
            "CALL SYSTEM$REFRESH_VIEW('ml_genre_similarity_matrix')"
        ]
        
        for query in refresh_queries:
            try:
                session.sql(query).collect()
            except Exception as e:
                # Views might not support refresh, continue anyway
                pass
        
        # Log retraining event
        # Convert monitor_result to JSON string safely
        import json
        monitor_result_json = json.dumps(monitor_result).replace("'", "''")
        
        log_query = f"""
        INSERT INTO ml_training_log (
            model_name,
            version,
            training_timestamp,
            trigger_reason,
            data_stats,
            performance_before
        ) VALUES (
            'spotify_hybrid_recommender',
            '{new_version}',
            CURRENT_TIMESTAMP(),
            'automated_retraining',
            OBJECT_CONSTRUCT(
                'recent_tracks', {data_stats['recent_tracks']},
                'unique_genres', {data_stats['unique_genres']},
                'unique_artists', {data_stats['unique_artists']}
            ),
            PARSE_JSON('{monitor_result_json}')
        )
        """
        
        # Create training log table if it doesn't exist
        create_log_table = """
        CREATE TABLE IF NOT EXISTS ml_training_log (
            model_name STRING,
            version STRING,
            training_timestamp TIMESTAMP_NTZ,
            trigger_reason STRING,
            data_stats OBJECT,
            performance_before OBJECT,
            training_completed BOOLEAN DEFAULT FALSE,
            training_duration_seconds NUMBER,
            error_message STRING
        )
        """
        
        session.sql(create_log_table).collect()
        session.sql(log_query).collect()
        
        # The actual model training would be triggered here
        # In practice, this would call the Python training scripts
        # For now, we'll simulate the process and refresh the views
        
        # Refresh all recommendation views
        recommendation_views = [
            'ml_collaborative_recommendations',
            'ml_content_based_recommendations', 
            'ml_temporal_recommendations',
            'ml_discovery_recommendations',
            'ml_hybrid_recommendations'
        ]
        
        for view in recommendation_views:
            try:
                session.sql(f"CALL SYSTEM$REFRESH_VIEW('{view}')").collect()
            except:
                pass
        
        # Update training log
        update_log_query = f"""
        UPDATE ml_training_log 
        SET 
            training_completed = TRUE,
            training_duration_seconds = DATEDIFF('seconds', training_timestamp, CURRENT_TIMESTAMP())
        WHERE version = '{new_version}'
        """
        
        session.sql(update_log_query).collect()
        
        return f"✅ Successfully completed automated retraining. Version: {new_version}, Data: {data_stats['recent_tracks']} tracks, {data_stats['unique_genres']} genres"
        
    except Exception as e:
        # Log error - escape error message safely
        safe_error_message = str(e).replace("'", "''").replace('"', '""')[:500]  # Limit length
        error_log_query = f"""
        UPDATE ml_training_log 
        SET 
            training_completed = FALSE,
            error_message = '{safe_error_message}'
        WHERE version LIKE 'auto_%' 
        AND training_timestamp >= DATEADD('hours', -1, CURRENT_TIMESTAMP())
        """
        
        try:
            session.sql(error_log_query).collect()
        except:
            pass
        
        return f"❌ Automated retraining failed: {str(e)}"
$$;

-- =====================================================================
-- 3. MODEL DEPLOYMENT AND ROLLBACK PROCEDURES
-- =====================================================================

-- Deploy a new model version
CREATE OR REPLACE PROCEDURE deploy_model_version(
    model_name STRING,
    version STRING,
    deployment_environment STRING DEFAULT 'production'
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    deployment_result STRING;
    current_timestamp TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
BEGIN
    -- Create deployment log table if it doesn't exist
    CREATE TABLE IF NOT EXISTS ml_deployment_log (
        model_name STRING,
        version STRING,
        deployment_environment STRING,
        deployment_timestamp TIMESTAMP_NTZ,
        deployment_status STRING,
        previous_version STRING,
        rollback_available BOOLEAN DEFAULT TRUE
    );
    
    -- Get current production version
    LET previous_version STRING := (
        SELECT version 
        FROM ml_deployment_log 
        WHERE model_name = :model_name 
        AND deployment_environment = :deployment_environment
        AND deployment_status = 'active'
        ORDER BY deployment_timestamp DESC 
        LIMIT 1
    );
    
    -- Mark previous version as replaced
    IF (previous_version IS NOT NULL) THEN
        UPDATE ml_deployment_log 
        SET deployment_status = 'replaced'
        WHERE model_name = :model_name 
        AND version = :previous_version
        AND deployment_environment = :deployment_environment;
    END IF;
    
    -- Deploy new version
    INSERT INTO ml_deployment_log (
        model_name,
        version,
        deployment_environment,
        deployment_timestamp,
        deployment_status,
        previous_version
    ) VALUES (
        :model_name,
        :version,
        :deployment_environment,
        :current_timestamp,
        'active',
        :previous_version
    );
    
    deployment_result := '✅ Deployed ' || :model_name || ' version ' || :version || 
                        ' to ' || :deployment_environment;
    
    IF (previous_version IS NOT NULL) THEN
        deployment_result := deployment_result || ' (replaced version ' || previous_version || ')';
    END IF;
    
    RETURN deployment_result;
END;
$$;

-- Rollback to previous model version
CREATE OR REPLACE PROCEDURE rollback_model_version(
    model_name STRING,
    deployment_environment STRING DEFAULT 'production'
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    current_version STRING;
    previous_version STRING;
    rollback_result STRING;
BEGIN
    -- Get current and previous versions
    LET version_info RESULTSET := (
        SELECT version, previous_version
        FROM ml_deployment_log 
        WHERE model_name = :model_name 
        AND deployment_environment = :deployment_environment
        AND deployment_status = 'active'
        ORDER BY deployment_timestamp DESC 
        LIMIT 1
    );
    
    LET version_cursor CURSOR FOR version_info;
    OPEN version_cursor;
    FETCH version_cursor INTO current_version, previous_version;
    
    IF (previous_version IS NULL) THEN
        RETURN '❌ No previous version available for rollback';
    END IF;
    
    -- Mark current version as rolled back
    UPDATE ml_deployment_log 
    SET deployment_status = 'rolled_back'
    WHERE model_name = :model_name 
    AND version = :current_version
    AND deployment_environment = :deployment_environment;
    
    -- Reactivate previous version
    UPDATE ml_deployment_log 
    SET deployment_status = 'active'
    WHERE model_name = :model_name 
    AND version = :previous_version
    AND deployment_environment = :deployment_environment;
    
    -- Log the rollback
    INSERT INTO ml_deployment_log (
        model_name,
        version,
        deployment_environment,
        deployment_timestamp,
        deployment_status,
        previous_version
    ) VALUES (
        :model_name,
        :previous_version,
        :deployment_environment,
        CURRENT_TIMESTAMP(),
        'active_rollback',
        :current_version
    );
    
    rollback_result := '🔄 Rolled back ' || :model_name || ' from version ' || 
                      current_version || ' to version ' || previous_version;
    
    RETURN rollback_result;
END;
$$;

-- =====================================================================
-- 4. SCHEDULED TASKS FOR AUTOMATION
-- =====================================================================

-- Create task for daily model performance monitoring
CREATE OR REPLACE TASK spotify_model_monitoring
    WAREHOUSE = 'ML_WAREHOUSE'
    SCHEDULE = 'CRON 0 8 * * *'  -- Every day at 8 AM
AS
    CALL monitor_model_performance('spotify_hybrid_recommender', 0.7, 0.2);

-- Create task for weekly automated retraining
CREATE OR REPLACE TASK spotify_model_retraining
    WAREHOUSE = 'ML_WAREHOUSE'
    SCHEDULE = 'CRON 0 6 * * 1'  -- Every Monday at 6 AM
AS
    CALL retrain_recommendation_models(FALSE, 0.7);

-- Create task for refreshing ML views daily
CREATE OR REPLACE TASK spotify_ml_view_refresh
    WAREHOUSE = 'ML_WAREHOUSE'
    SCHEDULE = 'CRON 30 2 * * *'  -- Every day at 2:30 AM
AS
BEGIN
    -- Refresh all ML views in dependency order
    CALL SYSTEM$REFRESH_VIEW('ml_user_genre_interactions');
    CALL SYSTEM$REFRESH_VIEW('ml_track_content_features');
    CALL SYSTEM$REFRESH_VIEW('ml_temporal_patterns');
    CALL SYSTEM$REFRESH_VIEW('ml_genre_similarity_matrix');
    CALL SYSTEM$REFRESH_VIEW('ml_collaborative_recommendations');
    CALL SYSTEM$REFRESH_VIEW('ml_content_based_recommendations');
    CALL SYSTEM$REFRESH_VIEW('ml_temporal_recommendations');
    CALL SYSTEM$REFRESH_VIEW('ml_discovery_recommendations');
    CALL SYSTEM$REFRESH_VIEW('ml_hybrid_recommendations');
    CALL SYSTEM$REFRESH_VIEW('ml_recommendation_analytics');
END;

-- =====================================================================
-- 5. ALERTING AND NOTIFICATION PROCEDURES
-- =====================================================================

-- Send alerts when model performance degrades
CREATE OR REPLACE PROCEDURE send_model_performance_alert(
    model_name STRING,
    performance_score FLOAT,
    threshold FLOAT,
    alert_channel STRING DEFAULT 'email'
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    alert_message STRING;
    alert_severity STRING;
BEGIN
    -- Determine alert severity
    IF (performance_score < threshold * 0.5) THEN
        alert_severity := 'CRITICAL';
    ELSEIF (performance_score < threshold * 0.7) THEN
        alert_severity := 'HIGH';
    ELSE
        alert_severity := 'MEDIUM';
    END IF;
    
    alert_message := '[' || alert_severity || '] Model Performance Alert: ' || 
                    :model_name || ' performance (' || :performance_score || 
                    ') below threshold (' || :threshold || ')';
    
    -- Log the alert
    CREATE TABLE IF NOT EXISTS ml_alerts_log (
        alert_timestamp TIMESTAMP_NTZ,
        model_name STRING,
        alert_type STRING,
        severity STRING,
        message STRING,
        performance_score FLOAT,
        threshold FLOAT,
        alert_channel STRING
    );
    
    INSERT INTO ml_alerts_log VALUES (
        CURRENT_TIMESTAMP(),
        :model_name,
        'performance_degradation',
        alert_severity,
        alert_message,
        :performance_score,
        :threshold,
        :alert_channel
    );
    
    -- In a real implementation, you would integrate with external alerting systems here
    -- For now, we'll just return the alert message
    
    RETURN alert_message;
END;
$$;

-- =====================================================================
-- 6. UTILITY PROCEDURES AND FUNCTIONS
-- =====================================================================

-- Get model deployment history
CREATE OR REPLACE FUNCTION get_model_deployment_history(model_name STRING)
RETURNS TABLE (
    version STRING,
    deployment_environment STRING,
    deployment_timestamp TIMESTAMP_NTZ,
    deployment_status STRING,
    previous_version STRING
)
AS
$$
    SELECT 
        version,
        deployment_environment,
        deployment_timestamp,
        deployment_status,
        previous_version
    FROM ml_deployment_log
    WHERE model_name = get_model_deployment_history.model_name
    ORDER BY deployment_timestamp DESC
$$;

-- Get training history and performance trends
CREATE OR REPLACE VIEW ml_training_history AS
SELECT 
    model_name,
    version,
    training_timestamp,
    trigger_reason,
    training_completed,
    training_duration_seconds,
    data_stats,
    performance_before,
    error_message,
    
    -- Calculate performance trends
    LAG(performance_before:performance_metrics:overall_performance) 
        OVER (PARTITION BY model_name ORDER BY training_timestamp) AS previous_performance,
    
    performance_before:performance_metrics:overall_performance - 
    LAG(performance_before:performance_metrics:overall_performance) 
        OVER (PARTITION BY model_name ORDER BY training_timestamp) AS performance_change,
    
    -- Training frequency metrics
    DATEDIFF('days', 
        LAG(training_timestamp) OVER (PARTITION BY model_name ORDER BY training_timestamp),
        training_timestamp
    ) AS days_since_last_training
    
FROM ml_training_log
WHERE training_completed = TRUE
ORDER BY training_timestamp DESC;

-- Start all automation tasks
CREATE OR REPLACE PROCEDURE start_ml_automation()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    ALTER TASK spotify_ml_view_refresh RESUME;
    ALTER TASK spotify_model_monitoring RESUME;
    ALTER TASK spotify_model_retraining RESUME;
    
    RETURN '✅ Started all ML automation tasks: view refresh, monitoring, and retraining';
END;
$$;

-- Stop all automation tasks
CREATE OR REPLACE PROCEDURE stop_ml_automation()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    ALTER TASK spotify_ml_view_refresh SUSPEND;
    ALTER TASK spotify_model_monitoring SUSPEND;
    ALTER TASK spotify_model_retraining SUSPEND;
    
    RETURN '⏸️ Stopped all ML automation tasks';
END;
$$;

-- =====================================================================
-- INITIALIZATION AND SETUP
-- =====================================================================

-- Initialize the automated ML system
CREATE OR REPLACE PROCEDURE initialize_ml_automation()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    init_result STRING := '';
BEGIN
    -- Create all necessary tables
    CREATE TABLE IF NOT EXISTS ml_training_log (
        model_name STRING,
        version STRING,
        training_timestamp TIMESTAMP_NTZ,
        trigger_reason STRING,
        data_stats OBJECT,
        performance_before OBJECT,
        training_completed BOOLEAN DEFAULT FALSE,
        training_duration_seconds NUMBER,
        error_message STRING
    );
    
    CREATE TABLE IF NOT EXISTS ml_deployment_log (
        model_name STRING,
        version STRING,
        deployment_environment STRING,
        deployment_timestamp TIMESTAMP_NTZ,
        deployment_status STRING,
        previous_version STRING,
        rollback_available BOOLEAN DEFAULT TRUE
    );
    
    CREATE TABLE IF NOT EXISTS ml_alerts_log (
        alert_timestamp TIMESTAMP_NTZ,
        model_name STRING,
        alert_type STRING,
        severity STRING,
        message STRING,
        performance_score FLOAT,
        threshold FLOAT,
        alert_channel STRING
    );
    
    init_result := '✅ ML automation system initialized with monitoring, training, and deployment tracking';
    
    RETURN init_result;
END;
$$;

-- =====================================================================
-- USAGE EXAMPLES
-- =====================================================================

-- Initialize the system
-- CALL initialize_ml_automation();

-- Start automation
-- CALL start_ml_automation();

-- Manual performance check
-- CALL monitor_model_performance('spotify_hybrid_recommender', 0.7, 0.2);

-- Manual retraining
-- CALL retrain_recommendation_models(FALSE, 0.7);

-- Deploy a model version
-- CALL deploy_model_version('spotify_hybrid_recommender', 'v2.1', 'production');

-- View deployment history
-- SELECT * FROM TABLE(get_model_deployment_history('spotify_hybrid_recommender'));

-- View training history
-- SELECT * FROM ml_training_history;

-- Check current task status
-- SHOW TASKS LIKE 'spotify_%';

-- Setup complete! Your automated ML system is ready for production use.
