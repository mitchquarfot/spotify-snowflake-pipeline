# Lightweight ML Pipeline

This directory contains a minimal, deterministic pipeline that reads your
listening history from Snowflake and uses it to generate seed genres/artists
for Spotify API recommendations.  It avoids the earlier dynamic-table
complexity and can be iterated on locally.

## Setup

1. Ensure the Bronze dynamic table `raw_data.spotify_mt_listening_deduped`
   exists and is populated (as already created in your Snowflake account).
2. Run the feature view script:

   ```sql
   USE DATABASE spotify_analytics;
   USE SCHEMA analytics;
   -- Execute file: ml_pipeline/create_ml_feature_views.sql
   ```

3. Install Python dependencies:

   ```bash
   pip install -r requirements_ml.txt
   ```

## Run the Recommender

```bash
python ml_pipeline/spotify_ml_pipeline.py
```

The script prints a list of candidate tracks sourced from both your top
genres and top artists.  It automatically filters out anything already in
your listening history.

## Next Steps

- Tune the genre/artist thresholds or add additional heuristics in
  `SpotifyRecommender.recommend`.
- Persist the candidate list to S3 or Snowflake if you want to track history.
- Enhance `RecommendationCandidate` with additional metadata (preview URL,
  album info, etc.) as required by downstream consumers.

