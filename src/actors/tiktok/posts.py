from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from src.actors.actor import ApifyActor
from src.models.tiktok_post import TikTokPost, _map_tiktok_comments

# TikTok Scraper
# https://console.apify.com/actors/GdWCkxBtKWOsKjdch/

logger = logging.getLogger(__name__)

COUNTRY_ID_TO_PROXY_COUNTRY_CODE = {
    "_484": "MX",
}

APIFY_INPUT_KEYS = {
    "excludePinnedPosts",
    "hashtags",
    "postURLs",
    "resultsPerPage",
    "scrapeRelatedVideos",
    "searchQueries",
    "searchSection",
    "shouldDownloadAvatars",
    "shouldDownloadCovers",
    "shouldDownloadMusicCovers",
    "shouldDownloadSlideshowImages",
    "shouldDownloadSubtitles",
    "shouldDownloadVideos",
}


class TikTokPostsActor(ApifyActor):
    """Download TikTok posts from URLs, hashtags, or text searches."""

    actor_id = "GdWCkxBtKWOsKjdch"
    comments_actor_id = "clockworks/tiktok-comments-scraper"

    def search(self, search_params: List[str], **kwargs) -> List[TikTokPost]:
        self.search_params_keywords = search_params
        links, hashtags, queries = _split_search_params(search_params)

        raw_results: List[Dict[str, Any]] = []
        if links:
            raw_results.extend(self.run_actor(self._build_run_input(links=links, **kwargs)))
        if hashtags or queries or not links:
            raw_results.extend(self.run_actor(self._build_run_input(hashtags=hashtags, queries=queries, **kwargs)))

        posts = [TikTokPost.from_tiktok(item) for item in raw_results]
        return self.process_documents(posts, **kwargs)

    def _build_run_input(
        self,
        links: List[str] | None = None,
        hashtags: List[str] | None = None,
        queries: List[str] | None = None,
        **kwargs,
    ) -> Dict[str, Any]:
        results_limit = kwargs.get("results_limit") or kwargs.get("max_results", 30)

        run_input: Dict[str, Any] = {
            "excludePinnedPosts": False,
            "resultsPerPage": results_limit,
            "scrapeRelatedVideos": False,
            "searchSection": "/video",
            "shouldDownloadAvatars": False,
            "shouldDownloadCovers": False,
            "shouldDownloadMusicCovers": False,
            "shouldDownloadSlideshowImages": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadVideos": False,
        }

        country_id = kwargs.get("country_id")
        if country_id:
            proxy_country_code = COUNTRY_ID_TO_PROXY_COUNTRY_CODE.get(country_id)
            if not proxy_country_code:
                raise ValueError(f"Unknown TikTok proxy country mapping for country_id={country_id!r}")
            run_input["proxyCountryCode"] = proxy_country_code

        if links:
            run_input["postURLs"] = links
        else:
            run_input["hashtags"] = hashtags or []
            run_input["searchQueries"] = queries or []

        run_input.update({key: kwargs[key] for key in APIFY_INPUT_KEYS if key in kwargs})

        overrides = kwargs.get("apify_input") or {}
        if isinstance(overrides, dict):
            run_input.update({key: value for key, value in overrides.items() if key != "proxyCountryCode"})

        return run_input

    def _enrich_comments(self, documents: List[TikTokPost], **kwargs) -> List[TikTokPost]:
        """Scrape comments for filtered posts via clockworks/tiktok-comments-scraper."""
        if not kwargs.get("get_comments", False) or not documents:
            return documents

        max_comments = kwargs.get("max_comments", 15)
        post_urls = [doc.data.get("url") for doc in documents if doc.data.get("url")]
        if not post_urls:
            return documents

        logger.info("Scraping TikTok comments for %d posts (max %d per post)", len(post_urls), max_comments)

        run_input: Dict[str, Any] = {
            "commentsPerPost": max_comments,
            "excludePinnedPosts": False,
            "maxRepliesPerComment": 0,
            "postURLs": post_urls,
            "resultsPerPage": max_comments,
        }

        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        try:
            run = self.client.actor(self.comments_actor_id).call(run_input=run_input)
            raw_comments = list(self.client.dataset(run["defaultDatasetId"]).iterate_items())
        except Exception as exc:
            logger.warning("Could not fetch TikTok comments: %s", exc)
            return documents

        for comment in raw_comments:
            if not isinstance(comment, dict):
                continue
            video_url = comment.get("videoWebUrl")
            if video_url:
                grouped[video_url].append(comment)

        for doc in documents:
            url = doc.data.get("url")
            doc.data["comments"] = _map_tiktok_comments(grouped.get(url, [])) if url else []

        logger.info("Enriched TikTok posts with %d total comments", len(raw_comments))
        return documents


def _split_search_params(search_params: List[str]) -> Tuple[List[str], List[str], List[str]]:
    links: List[str] = []
    hashtags: List[str] = []
    queries: List[str] = []

    for param in search_params:
        value = param.strip()
        if not value:
            continue
        if value.startswith(("http://", "https://")):
            links.append(value)
        elif value.startswith("#"):
            tag = value.lstrip("#").strip()
            if tag:
                hashtags.append(tag)
        else:
            queries.append(value)

    return links, hashtags, queries
