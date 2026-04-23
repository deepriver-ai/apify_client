from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from src.helpers.geocode import geocode
from src.models.document import Document
from src.models.users_management import UsersManagement

if TYPE_CHECKING:
    from src.models.news import News

logger = logging.getLogger(__name__)

# Shared UsersManagement instance for all posts
_users_manager = UsersManagement()

# Social platform domains to skip when looking for attached URLs
_SOCIAL_DOMAINS = {
    "instagram.com", "facebook.com", "twitter.com", "x.com",
    "tiktok.com", "youtube.com", "threads.net", "linkedin.com",
}

_URL_PATTERN = re.compile(r"https?://\S+")


def _extract_first_external_url(text: str, own_url: str | None = None) -> str | None:
    """Extract the first non-social, non-self URL from text."""
    from src.helpers.str_fn import get_domain

    if not text:
        return None
    urls = _URL_PATTERN.findall(text)
    for url in urls:
        url = url.rstrip(".,;:!?)\"'")
        domain = get_domain(url)
        if not domain:
            continue
        if any(social in domain for social in _SOCIAL_DOMAINS):
            continue
        if own_url:
            own_domain = get_domain(own_url)
            if own_domain and own_domain == domain:
                continue
        return url
    return None


class Post(Document):
    """Base class for social media posts. Platform-specific subclasses handle data mapping.

    All posts share a single ``UsersManagement`` instance (module-level) that
    caches user profile data (location, follower stats) across the pipeline.

    Attributes:
        _raw: The raw API response dict, stored for use by enrichment pipeline stages.
        attached_news: News object created when fetching an attached URL, if any.
        users_manager: Class-level shared UsersManagement instance.
    """

    users_manager: UsersManagement = _users_manager
    _raw: Dict[str, Any]
    attached_news: Optional[News]

    def __init__(self, data: Dict[str, Any] | None = None, raw: Dict[str, Any] | None = None):
        super().__init__(data)
        self._raw = raw or {}
        self.attached_news = None

    def fetch_attached_url(self, url: str | None = None) -> None:
        """Fetch and parse an attached URL and attach the resulting News to this post.

        When ``url`` is provided, it is used directly. Otherwise the URL is
        resolved from the raw API response's ``link`` field or by scanning the
        post body for the first external (non-social, non-self) URL.

        Creates a News object from the URL, appends its article text to the
        post body tagged as attached_url_text, and stores the News object in
        ``self.attached_news`` for later access (e.g. publishing to RabbitMQ).

        Location fields from the article's source (via SourcesManagement) are
        copied to the post when not already set.
        """
        from src.models.news import News

        body = self.data.get("body") or ""

        if not url:
            # Prefer explicit link from raw API response (e.g. Facebook 'link' field) TODO: move to FacebookPagePosts
            url = self._raw.get("link")
        if not url:
            own_url = self.data.get("url")
            url = _extract_first_external_url(body, own_url)
        if not url:
            return

        logger.info("Fetching attached URL: %s", url)
        news = News.from_url(url)
        if not news:
            logger.warning("Could not fetch/parse attached URL: %s", url)
            return

        self.attached_news = news

        article_body = news.data.get("body")
        if article_body:
            self.data["body"] = body + "\n\n attached_url_text: " + article_body

        # Copy location fields from the News object (populated by News.from_url via SourcesManagement)
        for key, value in news.data.items():
            if key.startswith("location_author") or key in ("author_location_text", "author_location_id"):
                if value is not None and self.data.get(key) is None:
                    self.data[key] = value

    def enrich_location(self, **kwargs) -> None:
        """Geocode body text to populate location_ids and author location fields.

        Checks UsersManagement for cached user location first (by profile_url).
        If found, applies it and skips geocoding. Otherwise geocodes the body
        text and saves the result to UsersManagement for future reuse.
        """
        profile_url = self.data.get("profile_url")

        # Try cached user location first
        if profile_url:
            cached_location = self.users_manager.get_location(profile_url)
            if cached_location:
                for key, value in cached_location.items():
                    if value is not None and self.data.get(key) is None:
                        self.data[key] = value
                return

        # Geocode body text
        author_location_text = self.data.get("author_location_text")
        text = self.data.get("body")

        location = geocode(text, context=author_location_text)

        if "error" in location:
            logger.warning("Skipping location enrichment for post %s: %s", self.data.get("url"), location["error"])
            return

        all_locations = []
        for k in location.keys():
            all_locations.extend(location[k])

        self.data["location_ids"] = [t["geoid"] for t in all_locations]

        if "2" in location and location["2"]:
            loc = location["2"][0]
            location_data = {}
            for key in loc.keys():
                target = f"location_author_{key}"
                if self.data.get(target) is None:
                    self.data[target] = loc[key]
                location_data[target] = loc[key]

            # Save geocoded location to UsersManagement for future reuse
            if profile_url and location_data:
                self.users_manager.save_location(profile_url, location_data)

    def needs_user_author_update(self, max_age_days: int = 90) -> bool:
        """Return True if this post's author needs a profile stats update."""
        profile_url = self.data.get("profile_url")
        if not profile_url:
            return False
        return self.users_manager.needs_stats_update(profile_url, max_age_days)

    def apply_cached_user_author(self) -> None:
        """Apply cached user author stats from UsersManagement to this post's data."""
        profile_url = self.data.get("profile_url")
        if not profile_url:
            return
        stats = self.users_manager.get_stats(profile_url)
        if not stats:
            return
        if self.data.get("website_visits") is None:
            self.data["website_visits"] = stats.get("website_visits")
        if self.data.get("author_full_name") is None:
            self.data["author_full_name"] = stats.get("author_full_name")
        if self.data.get("author_profile_bio") is None:
            self.data["author_profile_bio"] = stats.get("author_profile_bio")
        if self.data.get("author_location_text") is None:
            self.data["author_location_text"] = stats.get("author_location_text")

    def save_user_author_stats(self, stats: Dict[str, Any]) -> None:
        """Save user author stats to UsersManagement and apply to this post."""
        profile_url = self.data.get("profile_url")
        if not profile_url:
            return
        self.users_manager.save_stats(profile_url, stats)
        if self.data.get("website_visits") is None:
            self.data["website_visits"] = stats.get("website_visits")
        if self.data.get("author_full_name") is None:
            self.data["author_full_name"] = stats.get("author_full_name")
        if self.data.get("author_profile_bio") is None:
            self.data["author_profile_bio"] = stats.get("author_profile_bio")
        if self.data.get("author_location_text") is None:
            self.data["author_location_text"] = stats.get("author_location_text")

    def to_final_schema(self) -> Dict[str, Any]:
        """Fill post-specific fallback fields, then delegate to Document."""

        if not self.data.get("body"):
            self.data["body"] = f"{self.data.get('author')} - {self.data.get('url')}"

        self.data["fb_likes"] = self.data.get("likes")

        if not self.data.get("title"):
            body = self.data.get("body") or ""
            if body:
                self.data["title"] = body.replace("\n", " ").replace("attached_url_text: ", "")[:80].strip()
            else:
                author = self.data.get("author") or "Unknown"
                url = self.data.get("url") or ""
                self.data["title"] = f"{author} - {url}" if url else author
        try:
            return super().to_final_schema()
        except Exception as e:
            logger.error("Error normalizing post %s: %s", self.data.get("url"), e)
            return None
