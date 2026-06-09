"""Shared pytest fixtures for the Spotify pipeline test suite."""

import json
import pytest


@pytest.fixture
def sample_track_item():
    """A realistic Spotify recently-played track item."""
    return {
        "track": {
            "id": "4iV5W9uYEdYUVa79Axb7Rh",
            "name": "Wall Of Glass",
            "duration_ms": 287000,
            "explicit": False,
            "preview_url": "https://p.scdn.co/mp3-preview/abc",
            "external_urls": {"spotify": "https://open.spotify.com/track/4iV5W9uYEdYUVa79Axb7Rh"},
            "external_ids": {"isrc": "GBARL1700417"},
            "uri": "spotify:track:4iV5W9uYEdYUVa79Axb7Rh",
            "artists": [
                {
                    "id": "2DaxqgrOhkeH0fpeiQq2f4",
                    "name": "Liam Gallagher",
                    "uri": "spotify:artist:2DaxqgrOhkeH0fpeiQq2f4",
                    "external_urls": {"spotify": "https://open.spotify.com/artist/2DaxqgrOhkeH0fpeiQq2f4"},
                }
            ],
            "album": {
                "id": "5YMw7GNbhp5DbA90NVzpnH",
                "name": "As You Were",
                "album_type": "album",
                "release_date": "2017-10-06",
                "total_tracks": 12,
                "images": [{"url": "https://i.scdn.co/image/ab67", "height": 640, "width": 640}],
            },
        },
        "played_at": "2026-06-08T14:30:00Z",
        "context": {
            "type": "album",
            "uri": "spotify:album:5YMw7GNbhp5DbA90NVzpnH",
            "external_urls": {"spotify": "https://open.spotify.com/album/5YMw7GNbhp5DbA90NVzpnH"},
        },
    }


@pytest.fixture
def sample_track_item_no_external_ids():
    """Track item where external_ids is None (removed field scenario)."""
    return {
        "track": {
            "id": "6rqhFgbbKwnb9MLmUQDhG6",
            "name": "Some Track",
            "duration_ms": 200000,
            "explicit": True,
            "preview_url": None,
            "external_urls": {"spotify": "https://open.spotify.com/track/6rqhFgbbKwnb9MLmUQDhG6"},
            "external_ids": None,
            "uri": "spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
            "artists": [
                {
                    "id": "3TVXtAsR1Inumwj472S9r4",
                    "name": "Drake",
                    "uri": "spotify:artist:3TVXtAsR1Inumwj472S9r4",
                    "external_urls": {"spotify": "https://open.spotify.com/artist/3TVXtAsR1Inumwj472S9r4"},
                }
            ],
            "album": {
                "id": "7lLaAXb9luOJBNfjTMj4A3",
                "name": "Another Album",
                "album_type": "album",
                "release_date": "2025-01-01",
                "total_tracks": 14,
                "images": [],
            },
        },
        "played_at": "2026-06-08T10:00:00Z",
        "context": None,
    }


@pytest.fixture
def sample_artist_payload():
    """A realistic Spotify artist object (post-Feb-2026: no popularity/followers)."""
    return {
        "id": "2DaxqgrOhkeH0fpeiQq2f4",
        "name": "Liam Gallagher",
        "uri": "spotify:artist:2DaxqgrOhkeH0fpeiQq2f4",
        "genres": ["britpop", "rock"],
        "external_urls": {"spotify": "https://open.spotify.com/artist/2DaxqgrOhkeH0fpeiQq2f4"},
        "images": [{"url": "https://i.scdn.co/image/ab67", "height": 640, "width": 640}],
    }


@pytest.fixture
def lastfm_artist_info_response():
    """Fixture: Last.fm artist.getinfo JSON response body."""
    return {
        "artist": {
            "name": "Liam Gallagher",
            "mbid": "fd857293-5ab8-40de-b29e-55a69d4e4d0f",
            "url": "https://www.last.fm/music/Liam+Gallagher",
            "stats": {"listeners": "1350000", "playcount": "28000000"},
            "tags": {
                "tag": [
                    {"name": "britpop", "count": "100"},
                    {"name": "rock", "count": "90"},
                    {"name": "indie", "count": "70"},
                ]
            },
            "bio": {"summary": "Liam Gallagher is a British singer..."},
        }
    }


@pytest.fixture
def lastfm_similar_artists_response():
    """Fixture: Last.fm artist.getsimilar JSON response body."""
    return {
        "similarartists": {
            "artist": [
                {"name": "Noel Gallagher", "mbid": "some-mbid-1", "match": "0.95"},
                {"name": "Oasis", "mbid": "some-mbid-2", "match": "0.88"},
            ]
        }
    }


@pytest.fixture
def musicbrainz_search_response():
    """Fixture: musicbrainzngs search_artists result."""
    return {
        "artist-list": [
            {
                "id": "fd857293-5ab8-40de-b29e-55a69d4e4d0f",
                "name": "Liam Gallagher",
                "sort-name": "Gallagher, Liam",
                "type": "Person",
                "country": "GB",
                "area": {"name": "Manchester"},
                "life-span": {"begin": "1972-09-21", "end": None},
                "disambiguation": "Oasis singer",
                "tag-list": [
                    {"name": "britpop", "count": "5"},
                    {"name": "rock", "count": "3"},
                ],
            }
        ]
    }


@pytest.fixture
def musicbrainz_isrc_response():
    """Fixture: musicbrainzngs get_recordings_by_isrc result."""
    return {
        "isrc": {
            "recording-list": [
                {
                    "id": "rec-123",
                    "title": "Wall Of Glass",
                    "artist-credit": [
                        {
                            "artist": {
                                "id": "fd857293-5ab8-40de-b29e-55a69d4e4d0f",
                                "name": "Liam Gallagher",
                            }
                        }
                    ],
                }
            ]
        }
    }


@pytest.fixture
def musicbrainz_artist_by_id_response():
    """Fixture: musicbrainzngs get_artist_by_id result (with tags)."""
    return {
        "artist": {
            "id": "fd857293-5ab8-40de-b29e-55a69d4e4d0f",
            "name": "Liam Gallagher",
            "sort-name": "Gallagher, Liam",
            "type": "Person",
            "country": "GB",
            "area": {"name": "Manchester"},
            "life-span": {"begin": "1972-09-21", "end": None},
            "disambiguation": "Oasis singer",
            "tag-list": [
                {"name": "britpop", "count": "5"},
                {"name": "rock", "count": "3"},
            ],
        }
    }
