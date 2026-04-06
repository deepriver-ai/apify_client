"""Facebook comment scraper for comment enrichment.

Wraps ``apify/facebook-comments-scraper`` to fetch comments and replies
from Facebook posts.

This is a utility class — not a search-task actor. It is used by
Facebook actor classes (e.g. ``FacebookPagePostsActor``) in their
``_enrich_comments`` stage to scrape comments for each post.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List

from apify_client import ApifyClient

logger = logging.getLogger(__name__)


class FacebookCommentsActor:
    """Scrape Facebook post comments via apify/facebook-comments-scraper.

    Not a search-task actor — used internally by Facebook actors for
    comment enrichment. Accepts an existing ``ApifyClient`` so the
    calling actor can share its client/token.
    """

    actor_id = "apify/facebook-comments-scraper"

    def __init__(self, client: ApifyClient):
        self.client = client

    def scrape_comments(
        self,
        post_urls: List[str],
        max_comments: int = 15,
    ) -> List[Dict[str, Any]]:
        """Call the Facebook comments scraper and return raw results.

        Args:
            post_urls: List of Facebook post URLs.
            max_comments: Maximum number of comments per post.

        Returns:
            List of raw comment dicts from the Apify actor.
        """
        run_input: Dict[str, Any] = {
            "startUrls": [{"url": url} for url in post_urls],
            "resultsLimit": max_comments,
        }
        run = self.client.actor(self.actor_id).call(run_input=run_input)
        results: List[Dict[str, Any]] = []
        for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
            results.append(item)
        return results

    @staticmethod
    def map_comment(raw: Dict[str, Any]) -> Dict[str, Any]:
        """Map a raw Apify comment to the common comments schema.

        The returned dict matches the comment schema used across all
        social platforms (``comment_text``, ``comment_author``,
        ``comment_timestamp``, ``comment_likes``).
        """
        return {
            "comment_text": raw.get("text"),
            "comment_author": raw.get("profileName"),
            "comment_timestamp": raw.get("date"),
            "comment_likes": raw.get("likesCount"),
        }

    @staticmethod
    def group_by_post_url(
        raw_comments: List[Dict[str, Any]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Group mapped comments by their source post URL.

        Args:
            raw_comments: Raw comment dicts from the Apify actor.

        Returns:
            Dict mapping post URL → list of mapped comment dicts.
        """
        comments_by_url: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for c in raw_comments:
            post_url = c.get("postUrl") or c.get("facebookUrl")
            if post_url and "error" not in c:
                comments_by_url[post_url].append(
                    FacebookCommentsActor.map_comment(c)
                )
        return comments_by_url
