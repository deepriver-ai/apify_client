from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

# --- Mock MongoDB before any src imports ---
# sources_management.py loads data from MongoDB at module level.
# We intercept this by pre-populating sys.modules with a mock mongoconnection.
_mock_mongoconn = MagicMock()
_mock_mongoconn.admin_app.CrawlersAll.find.return_value = []
sys.modules.setdefault("src.helpers.mongoconnection", MagicMock(mongoconn=_mock_mongoconn))

# Also mock dotenv load to avoid issues
_mock_dotenv = MagicMock()
sys.modules.setdefault("dotenv", _mock_dotenv)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")


def _load_cache(filename: str) -> List[Dict[str, Any]]:
    with open(os.path.join(CACHE_DIR, filename), encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def sample_google_news_results() -> List[Dict[str, Any]]:
    return _load_cache("google_news_sample.json")


@pytest.fixture
def sample_instagram_results() -> List[Dict[str, Any]]:
    return _load_cache("instagram_sample.json")


@pytest.fixture
def sample_instagram_comments() -> List[Dict[str, Any]]:
    return _load_cache("instagram_comments_sample.json")


@pytest.fixture
def mock_apify_run_actor(request):
    """Patch ApifyActor.run_actor to return the provided data."""
    data = getattr(request, "param", [])
    with patch("src.actors.actor.ApifyActor.run_actor", return_value=data) as m:
        yield m


@pytest.fixture
def mock_fetch_html():
    with patch("src.models.news_parser.load_url.fetch_html", return_value=None) as m:
        yield m


@pytest.fixture(autouse=True)
def mock_geocode():
    """Mock geocode to avoid HTTP calls in tests."""
    with patch("src.models.post.geocode", return_value={"1": [], "2": []}) as m:
        yield m


@pytest.fixture
def sample_facebook_posts_results() -> List[Dict[str, Any]]:
    return _load_cache("facebook_posts_sample.json")


@pytest.fixture
def sample_facebook_page_profiles() -> List[Dict[str, Any]]:
    return _load_cache("facebook_page_profile_sample.json")


@pytest.fixture
def sample_facebook_comments() -> List[Dict[str, Any]]:
    return _load_cache("facebook_comments_sample.json")


@pytest.fixture
def mock_sources_manager():
    mgr = MagicMock()
    mgr.get_domain.return_value = "example.com"
    mgr.get_location.return_value = {
        "author_location_text": "Mexico",
        "author_location_id": "_484",
        "location_author_formatted_name": "Mexico",
        "location_author_geoid": "_484",
        "location_author_coords": {"lat": 19.43, "lon": -99.13},
        "location_author_precision_level": 1,
        "location_author_level_1": "Mexico",
        "location_author_level_1_id": "_484",
        "location_author_level_2": None,
        "location_author_level_2_id": None,
        "location_author_level_3": None,
        "location_author_level_3_id": None,
    }
    mgr.check_source.return_value = True
    mgr.save.return_value = None
    return mgr


