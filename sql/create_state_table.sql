-- Pipeline state table: replaces file-based pipeline_state.json with a durable Snowflake store.
-- Each row holds a named state key (e.g. 'last_processed_timestamp') with an arbitrary VARIANT payload.
CREATE TABLE IF NOT EXISTS SPOTIFY_ANALYTICS.RAW_DATA.PIPELINE_STATE (
    state_key   VARCHAR(256)    NOT NULL,
    state_value VARIANT         NOT NULL,
    updated_at  TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_pipeline_state PRIMARY KEY (state_key)
);
