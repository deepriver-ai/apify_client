from __future__ import annotations

import logging
from typing import Any, Dict, List

from src.actors.instagram.profile_posts import InstagramProfilePostsActor
from src.models.instagram_post import InstagramPost

# Instagram Posts/Reels Scraper (no cookies) by queenlike_xystos
# https://console.apify.com/actors/queenlike_xystos~instagram-posts-reels-scraper---no-cookies/
#
# Differs from the default InstagramProfilePostsActor in two main ways:
#   1. Search params are usernames (not profile URLs).
#   2. The actor accepts a single username per run, so search() loops over inputs.
#   3. The response surfaces a real `reshare_count` (mapped to `shares`), which
#      the default Instagram actor does not return.

logger = logging.getLogger(__name__)


class InstagramProfileQueenlikeActor(InstagramProfilePostsActor):

    actor_id = "queenlike_xystos/instagram-posts-reels-scraper---no-cookies"

    def search(self, search_params: List[str], **kwargs) -> List[InstagramPost]:
        """Scrape posts/reels for a list of Instagram usernames.

        Args:
            search_params: List of Instagram usernames (e.g. ``"nina.deloto"``).
                A leading ``@`` and any wrapping ``https://www.instagram.com/...``
                URL form are tolerated and stripped down to the bare username.
        """
        self.search_params_keywords = []  # usernames, not keywords
        results_limit = kwargs.get("results_limit") or kwargs.get("max_results", 12)
        scrape_type = kwargs.get("scrape_type", "posts")
        output_mode = kwargs.get("output_mode", "clean")

        seen: set[str] = set()
        all_posts: List[InstagramPost] = []
        for raw_param in search_params:
            username = _normalize_username(raw_param)
            if not username:
                continue

            run_input: Dict[str, Any] = {
                "username": username,
                "max_items": results_limit,
                "scrape_type": scrape_type,
                "output_mode": output_mode,
            }
            raw_results = self.run_actor(run_input)
            for item in raw_results:
                url = item.get("url")
                if url and url in seen:
                    continue
                if url:
                    seen.add(url)
                all_posts.append(InstagramPost.from_instagram_queenlike(item))

        logger.info(
            "Scraped %d posts from %d usernames via %s",
            len(all_posts), len(search_params), self.actor_id,
        )
        return self.process_documents(all_posts, **kwargs)


def _normalize_username(raw: str) -> str:
    """Strip `@`, surrounding whitespace, and any URL form down to the username."""
    if not raw:
        return ""
    raw = raw.strip()
    if raw.startswith("@"):
        raw = raw[1:]
    if raw.startswith("http://") or raw.startswith("https://"):
        # extract path segment after instagram.com/
        path = raw.split("instagram.com/", 1)[-1]
        raw = path.split("/", 1)[0]
    return raw.strip("/")
