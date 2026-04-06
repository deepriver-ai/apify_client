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
            - ``likes`` → ``website_visits`` (page likes used as follower proxy)
            - ``title`` → ``author_full_name``
            - ``info`` (list) → ``author_profile_bio`` (joined with newline)
            - ``address`` → ``author_location_text``

        Additional fields (categories, rating, email, phone, website) are
        stored at the top level for persistence in UsersManagement, even
        though they are not mapped to the intermediate document schema.
        The full raw response is kept under ``_raw_profile``.
        """
        info = raw.get("info")
        bio = "\n".join(info) if isinstance(info, list) and info else None

        return {
            # Fields mapped to intermediate schema
            "website_visits": raw.get("likes"),
            "author_full_name": raw.get("title"),
            "author_profile_bio": bio,
            "author_location_text": raw.get("address"),
            # Extra fields stored for future use
            "categories": raw.get("categories"),
            "rating": raw.get("rating"),
            "email": raw.get("email"),
            "phone": raw.get("phone"),
            "website": raw.get("website"),
            # Full raw response
            "_raw_profile": raw,
        }
