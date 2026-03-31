from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List

from src.actors.actor import ApifyActor
from src.models.post import Post

# Instagram Hashtag Scraper
# https://console.apify.com/actors/reGe1ST3OBgYZSsZJ/

logger = logging.getLogger(__name__)


class InstagramHashtagActor(ApifyActor):

    actor_id = "reGe1ST3OBgYZSsZJ"
    comments_actor_id = "apify/instagram-comment-scraper"

    def search(self, search_params: List[str], **kwargs) -> List[Post]:
        results_type = kwargs.get("results_type", "posts")
        results_limit = kwargs.get("results_limit") or kwargs.get("max_results", 10)
        keyword_search = kwargs.get("keyword_search", False)

        run_input = {
            "hashtags": search_params,
            "resultsType": results_type,
            "resultsLimit": results_limit,
            "keywordSearch": keyword_search,
        }

        raw_results = self.run_actor(run_input)
        posts = [Post.from_instagram(item) for item in raw_results]
        return self.process_documents(posts, **kwargs)

    def _enrich_comments(self, documents: List, **kwargs) -> List:
        """Scrape comments for each post via apify/instagram-comment-scraper."""
        get_comments = kwargs.get("get_comments", False)
        if not get_comments:
            return documents

        max_comments = kwargs.get("max_comments", 15)
        post_urls = [doc.data.get("url") for doc in documents if doc.data.get("url")]
        if not post_urls:
            return documents

        logger.info("Scraping comments for %d posts (max %d per post)", len(post_urls), max_comments)

        run_input: Dict[str, Any] = {
            "directUrls": post_urls,
            "resultsLimit": max_comments,
        }

        run = self.client.actor(self.comments_actor_id).call(run_input=run_input)
        raw_comments: List[Dict[str, Any]] = []
        for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
            raw_comments.append(item)

        # Group comments by post URL
        comments_by_url: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for c in raw_comments:
            post_url = c.get("postUrl") or c.get("inputUrl")
            if post_url and not 'error' in c:
                comments_by_url[post_url].append({
                    "comment_text": c.get("text"),
                    "comment_author": c.get("ownerUsername"),
                    "comment_timestamp": c.get("timestamp"),
                    "comment_likes": c.get("likesCount"),
                })

        for doc in documents:
            url = doc.data.get("url")
            if url:
                doc.data["comments"] = comments_by_url[url]

        logger.info("Enriched posts with %d total comments", len(raw_comments))
        return documents
