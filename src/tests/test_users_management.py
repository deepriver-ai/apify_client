"""Tests for UsersManagement."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timedelta

import pytest

from src.models.users_management import UsersManagement


@pytest.fixture
def users_mgr(tmp_path):
    """UsersManagement backed by a temp file."""
    cache_file = str(tmp_path / "users.json")
    return UsersManagement(cache_path=cache_file)


class TestBasicOperations:

    def test_empty_initially(self, users_mgr):
        assert users_mgr.get_user("https://instagram.com/test/") is None
        assert not users_mgr.is_known("https://instagram.com/test/")

    def test_save_and_get_stats(self, users_mgr):
        url = "https://instagram.com/user1/"
        users_mgr.save_stats(url, {
            "website_visits": 1000,
            "author": "User One",
            "author_full_name": "User One Full",
            "author_profile_bio": "Bio text here",
        })
        user = users_mgr.get_user(url)
        assert user["website_visits"] == 1000
        assert user["author"] == "User One"
        assert user["author_full_name"] == "User One Full"
        assert user["author_profile_bio"] == "Bio text here"
        assert user["date_stats_updated"] is not None

    def test_get_stats_returns_bio_and_full_name(self, users_mgr):
        url = "https://instagram.com/user1/"
        users_mgr.save_stats(url, {
            "website_visits": 500,
            "author": "User",
            "author_full_name": "Full Name",
            "author_profile_bio": "A bio",
        })
        stats = users_mgr.get_stats(url)
        assert stats["author_full_name"] == "Full Name"
        assert stats["author_profile_bio"] == "A bio"

    def test_save_and_get_location(self, users_mgr):
        url = "https://instagram.com/user1/"
        users_mgr.save_location(url, {
            "location_author_geoid": "_484",
            "location_author_formatted_name": "Mexico",
        })
        loc = users_mgr.get_location(url)
        assert loc is not None
        assert loc["location_author_geoid"] == "_484"

    def test_get_location_none_when_no_geoid(self, users_mgr):
        url = "https://instagram.com/user1/"
        users_mgr.save_stats(url, {"website_visits": 100})
        assert users_mgr.get_location(url) is None

    def test_has_location(self, users_mgr):
        url = "https://instagram.com/user1/"
        assert not users_mgr.has_location(url)
        users_mgr.save_location(url, {"location_author_geoid": "_484"})
        assert users_mgr.has_location(url)

    def test_get_stats_none_when_no_visits(self, users_mgr):
        url = "https://instagram.com/user1/"
        users_mgr.save_location(url, {"location_author_geoid": "_484"})
        assert users_mgr.get_stats(url) is None


class TestNeedsStatsUpdate:

    def test_unknown_user_needs_update(self, users_mgr):
        assert users_mgr.needs_stats_update("https://instagram.com/unknown/")

    def test_fresh_stats_no_update(self, users_mgr):
        url = "https://instagram.com/user1/"
        users_mgr.save_stats(url, {"website_visits": 500})
        assert not users_mgr.needs_stats_update(url, max_age_days=90)

    def test_stale_stats_need_update(self, users_mgr):
        url = "https://instagram.com/user1/"
        users_mgr.save_stats(url, {"website_visits": 500})
        # Manually backdate (profile URLs are normalized: trailing slash stripped)
        users_mgr._users[url.rstrip("/")]["date_stats_updated"] = (
            datetime.now() - timedelta(days=100)
        ).isoformat()
        assert users_mgr.needs_stats_update(url, max_age_days=90)

    def test_custom_max_age(self, users_mgr):
        url = "https://instagram.com/user1/"
        users_mgr.save_stats(url, {"website_visits": 500})
        users_mgr._users[url.rstrip("/")]["date_stats_updated"] = (
            datetime.now() - timedelta(days=10)
        ).isoformat()
        assert not users_mgr.needs_stats_update(url, max_age_days=30)
        assert users_mgr.needs_stats_update(url, max_age_days=5)


class TestPersistence:

    def test_save_and_reload(self, tmp_path):
        cache_file = str(tmp_path / "users.json")
        mgr1 = UsersManagement(cache_path=cache_file)
        mgr1.save_stats("https://instagram.com/u/", {"website_visits": 42})

        # Reload from same file
        mgr2 = UsersManagement(cache_path=cache_file)
        user = mgr2.get_user("https://instagram.com/u/")
        assert user is not None
        assert user["website_visits"] == 42


class TestTrailingSlashNormalization:

    def test_lookup_slash_tolerant(self, users_mgr):
        users_mgr.save_stats("https://instagram.com/u", {"website_visits": 10})
        assert users_mgr.is_known("https://instagram.com/u")
        assert users_mgr.is_known("https://instagram.com/u/")
        assert users_mgr.get_stats("https://instagram.com/u/")["website_visits"] == 10

    def test_write_slash_tolerant(self, users_mgr):
        users_mgr.save_stats("https://instagram.com/u/", {"website_visits": 10})
        users_mgr.save_stats("https://instagram.com/u", {"website_visits": 20})
        assert users_mgr.get_stats("https://instagram.com/u")["website_visits"] == 20

    def test_legacy_keys_normalized_on_load(self, tmp_path):
        cache_file = str(tmp_path / "users.json")
        with open(cache_file, "w") as f:
            json.dump({
                "https://instagram.com/legacy/": {
                    "profile_url": "https://instagram.com/legacy/",
                    "website_visits": 5,
                }
            }, f)
        mgr = UsersManagement(cache_path=cache_file)
        assert mgr.is_known("https://instagram.com/legacy")
        assert mgr.is_known("https://instagram.com/legacy/")
        assert mgr.get_user("https://instagram.com/legacy")["profile_url"] == "https://instagram.com/legacy"
