"""Facebook page profile scraper for user/author enrichment.

Wraps ``apify/facebook-pages-scraper`` to fetch page-level metadata
(followers, location, bio, categories, contact info, etc.).

This is a utility class — not a search-task actor. It is used by
Facebook actor classes (e.g. ``FacebookPagePostsActor``) in their
``_enrich_user_author`` stage to enrich posts with page metadata.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from apify_client import ApifyClient

logger = logging.getLogger(__name__)


class FacebookProfileActor:
    """Scrape Facebook page profiles via apify/facebook-pages-scraper.

    Not a search-task actor — used internally by Facebook actors for
    author enrichment. Accepts an existing ``ApifyClient`` so the
    calling actor can share its client/token.
    """

    actor_id = "apify/facebook-pages-scraper"

    def __init__(self, client: ApifyClient):
        self.client = client

    def scrape_pages(self, page_urls: List[str]) -> List[Dict[str, Any]]:
        """Call the Facebook pages scraper and return raw results.

        Args:
            page_urls: List of Facebook page URLs
                (e.g. ``["https://www.facebook.com/SomePage/"]``).

        Returns:
            List of raw profile dicts from the Apify actor.
        """
        run_input = {"startUrls": [{"url": url} for url in page_urls]}
        run = self.client.actor(self.actor_id).call(run_input=run_input)
        results = []
        for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
            results.append(item)
        return results

    @staticmethod
    def map_profile(raw: Dict[str, Any]) -> Dict[str, Any]:
        """Map raw Apify page profile output to a stats dict.

        The returned dict is compatible with ``Post.save_user_author_stats()``
        and ``UsersManagement.save_stats()``.

        Mapping:
            - ``followers`` (preferred) or ``likes`` → ``website_visits``
            - ``title`` (or ``personalProfile.name``) → ``author_full_name``
            - ``info`` (list) → ``author_profile_bio`` (joined with newline)
            - ``address`` → ``current_city``/``CURRENT_CITY`` → ``hometown``/``HOMETOWN`` → ``author_location_text``
              (with "Lives in "/"From " prefixes stripped)

        Additional fields (categories, rating, email, phone, website) are
        stored at the top level for persistence in UsersManagement, even
        though they are not mapped to the intermediate document schema.
        The full raw response is kept under ``_raw_profile``.
        """
        info = raw.get("info")
        bio = "\n".join(info) if isinstance(info, list) and info else None

        personal = raw.get("personalProfile") or {}
        full_name = raw.get("title") or personal.get("name")

        followers = raw.get("followers")
        if followers is None:
            followers = raw.get("likes")

        location_text = (
            raw.get("address")
            or _strip_location_prefix(raw.get("current_city") or raw.get("CURRENT_CITY"))
            or _strip_location_prefix(raw.get("hometown") or raw.get("HOMETOWN"))
        )

        return {
            # Fields mapped to intermediate schema
            "website_visits": followers,
            "author_full_name": full_name,
            "author_profile_bio": bio,
            "author_location_text": location_text,
            # Extra fields stored for future use
            "categories": raw.get("categories"),
            "rating": raw.get("rating"),
            "email": raw.get("email"),
            "phone": raw.get("phone"),
            "website": raw.get("website"),
            # Full raw response
            "_raw_profile": raw,
        }


def _strip_location_prefix(value: str | None) -> str | None:
    """Strip Facebook's 'Lives in '/'From ' wrappers around a city name."""
    if not value:
        return value
    text = value.strip()
    for prefix in ("Lives in ", "From "):
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return text


def index_profiles_by_page_name(raw_profiles: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Build a lookup ``{page_name → profile}`` keyed by every name representation
    the actor returns (``pageName`` plus names extracted from ``facebookUrl`` and
    ``pageUrl``), so callers can resolve a profile by whichever key matches the
    post's ``profile_url``.
    """
    from src.models.facebook_post import _extract_facebook_page_name

    index: Dict[str, Dict[str, Any]] = {}
    for profile in raw_profiles:
        keys: List[str] = []
        page_name = profile.get("pageName")
        if page_name:
            keys.append(page_name)
        for url_field in ("facebookUrl", "pageUrl"):
            extracted = _extract_facebook_page_name(profile.get(url_field, ""))
            if extracted:
                keys.append(extracted)
        for k in keys:
            index.setdefault(k, profile)
    return index
