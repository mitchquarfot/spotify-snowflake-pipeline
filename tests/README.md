# Running the test suite

All tests run **offline** — no live Spotify, Last.fm, MusicBrainz, or Snowflake credentials are needed.

```bash
# Activate the project venv
source .venv/bin/activate

# Run the full suite
python -m pytest tests/ -q

# Run a single file
python -m pytest tests/test_spotify_client.py -v
```

Dependencies: `pytest`, `responses` (both listed in `requirements.txt`).
