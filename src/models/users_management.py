"""User profile data management for social media authors.

Caches user profile stats (followers, location) to avoid redundant scraping.
Similar pattern to SourcesManagement but backed by a JSON file (no MongoDB yet).

TODO: Add MongoDB collection for persistent user data storage.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

USERS_CACHE_PATH = os.path.join("cache", "users.json")

# Location fields stored per user
LOCATION_FIELDS = [
    "author_location_text",
    "author_location_id",
    "location_author_formatted_name",
    "location_author_geoid",
    "location_author_coords",
    "location_author_precision_level",
    "location_author_level_1",
    "location_author_level_1_id",
    "location_author_level_2",
    "location_author_level_2_id",
    "location_author_level_3",
    "location_author_level_3_id",
]


class UsersManagement:
    """Cache and manage social media user profile data.

    Keyed by ``profile_url`` (e.g. ``https://www.instagram.com/username/``).
    Stores follower counts, author name, geocoded location, and a timestamp
    for when stats were last updated.
    """

    def __init__(self, cache_path: str = USERS_CACHE_PATH):
        self.cache_path = cache_path
        self._users: Dict[str, Dict[str, Any]] = self._load()

    def _load(self) -> Dict[str, Dict[str, Any]]:
        """Load user cache from disk."""
        if os.path.exists(self.cache_path):
            try:
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (OSError, json.JSONDecodeError):
                logger.warning("Could not load users cache from %s, starting fresh", self.cache_path)
        return {}

    def save(self) -> None:
        """Persist user cache to disk."""
        os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
        try:
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(self._users, f, ensure_ascii=False, indent=2)
        except OSError:
            logger.warning("Could not save users cache to %s", self.cache_path)

    def get_user(self, profile_url: str) -> Optional[Dict[str, Any]]:
        """Return cached user data or None."""
        return self._users.get(profile_url)

    def is_known(self, profile_url: str) -> bool:
        """Return True if we have any data for this user."""
        return profile_url in self._users

    def has_location(self, profile_url: str) -> bool:
        """Return True if we have geocoded location data for this user."""
        user = self._users.get(profile_url)
        if not user:
            return False
        return user.get("location_author_geoid") is not None

    def needs_stats_update(self, profile_url: str, max_age_days: int = 90) -> bool:
        """Return True if user stats are missing or older than max_age_days."""
        user = self._users.get(profile_url)
        if not user:
            return True
        date_str = user.get("date_stats_updated")
        if not date_str:
            return True
        try:
            last_updated = datetime.fromisoformat(date_str)
            return datetime.now() - last_updated > timedelta(days=max_age_days)
        except (ValueError, TypeError):
            return True

    def save_stats(self, profile_url: str, stats: Dict[str, Any]) -> None:
        """Update user with profile stats and set date_stats_updated to now.

        Args:
            profile_url: User identifier (profile URL).
            stats: Dict with keys like ``website_visits``, ``author``, etc.
        """
        if profile_url not in self._users:
            self._users[profile_url] = {"profile_url": profile_url}
        self._users[profile_url].update(stats)
        self._users[profile_url]["date_stats_updated"] = datetime.now().isoformat()
        self.save()

    def save_location(self, profile_url: str, location: Dict[str, Any]) -> None:
        """Update user with geocoded location fields.

        Args:
            profile_url: User identifier (profile URL).
            location: Dict with location_author_* fields.
        """
        if profile_url not in self._users:
            self._users[profile_url] = {"profile_url": profile_url}
        for field in LOCATION_FIELDS:
            if field in location and location[field] is not None:
                self._users[profile_url][field] = location[field]
        self.save()

    def get_location(self, profile_url: str) -> Optional[Dict[str, Any]]:
        """Return cached location fields for a user, or None if not available."""
        user = self._users.get(profile_url)
        if not user or not user.get("location_author_geoid"):
            return None
        return {field: user.get(field) for field in LOCATION_FIELDS}

    def get_stats(self, profile_url: str) -> Optional[Dict[str, Any]]:
        """Return cached stats (website_visits, author) for a user, or None."""
        user = self._users.get(profile_url)
        if not user or user.get("website_visits") is None:
            return None
        return {
            "website_visits": user.get("website_visits"),
            "author": user.get("author"),
            "author_full_name": user.get("author_full_name"),
            "author_profile_bio": user.get("author_profile_bio"),
            "author_location_text": user.get("author_location_text"),
            "date_stats_updated": user.get("date_stats_updated"),
        }
