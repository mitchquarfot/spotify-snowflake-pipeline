-- Dead-letter queue for records that fail transformation or validation during pipeline runs.
-- Enables post-mortem debugging without dropping entire batches.
CREATE TABLE IF NOT EXISTS SPOTIFY_ANALYTICS.RAW_DATA.PIPELINE_ERRORS (
    error_id        VARCHAR(36)     NOT NULL DEFAULT UUID_STRING(),
    run_id          VARCHAR(36)     NOT NULL,
    error_timestamp TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    error_type      VARCHAR(128)    NOT NULL,
    error_message   VARCHAR(4096),
    record_payload  VARIANT,
    CONSTRAINT pk_pipeline_errors PRIMARY KEY (error_id)
);
