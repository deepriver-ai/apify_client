from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

from src.actors.actor import ApifyActor
from src.models.tiktok_post import TikTokPost

# TikTok Scraper
# https://console.apify.com/actors/GdWCkxBtKWOsKjdch/

logger = logging.getLogger(__name__)

COUNTRY_ID_TO_PROXY_COUNTRY_CODE = {
    "_484": "MX",
}

APIFY_INPUT_KEYS = {
    "commentsPerPost",
    "excludePinnedPosts",
    "hashtags",
    "maxRepliesPerComment",
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

    def search(self, search_params: List[str], **kwargs) -> List[TikTokPost]:
        self.search_params_keywords = search_params
        links, hashtags, queries = _split_search_params(search_params)

        raw_results: List[Dict[str, Any]] = []
        if links:
            raw_results.extend(self.run_actor(self._build_run_input(links=links, **kwargs)))
        if hashtags or queries or not links:
            raw_results.extend(self.run_actor(self._build_run_input(hashtags=hashtags, queries=queries, **kwargs)))

        if kwargs.get("get_comments"):
            self._attach_comments_from_datasets(raw_results)

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
        get_comments = kwargs.get("get_comments", False)
        max_comments = kwargs.get("max_comments", 15)

        run_input: Dict[str, Any] = {
            "commentsPerPost": max_comments if get_comments else 0,
            "excludePinnedPosts": False,
            "maxRepliesPerComment": 0,
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

    def _attach_comments_from_datasets(self, items: List[Dict[str, Any]]) -> None:
        """Fetch Apify comment datasets and attach comments to matching raw posts."""
        comments_by_dataset: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

        for item in items:
            dataset_url = item.get("commentsDatasetUrl")
            if not dataset_url:
                continue

            dataset_id = _extract_dataset_id(dataset_url)
            if not dataset_id:
                logger.warning("Could not parse TikTok comments dataset id from %s", dataset_url)
                continue

            if dataset_id not in comments_by_dataset:
                comments_by_dataset[dataset_id] = self._fetch_comments_by_video_url(dataset_id)

            post_url = _post_url(item)
            if not post_url:
                continue
            item["comments"] = comments_by_dataset[dataset_id].get(post_url, [])

    def _fetch_comments_by_video_url(self, dataset_id: str) -> Dict[str, List[Dict[str, Any]]]:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        try:
            result = self.client.dataset(dataset_id).list_items()
            comments = getattr(result, "items", result)
        except Exception as exc:
            logger.warning("Could not fetch TikTok comments dataset %s: %s", dataset_id, exc)
            return grouped

        if not isinstance(comments, list):
            return grouped

        for comment in comments:
            if not isinstance(comment, dict):
                continue
            video_url = comment.get("videoWebUrl")
            if video_url:
                grouped[video_url].append(comment)

        return grouped


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


def _extract_dataset_id(dataset_url: str) -> str | None:
    path_parts = urlparse(dataset_url).path.strip("/").split("/")
    try:
        return path_parts[path_parts.index("datasets") + 1]
    except (ValueError, IndexError):
        return None


def _post_url(item: Dict[str, Any]) -> str | None:
    return item.get("webVideoUrl") or item.get("url") or item.get("videoUrl")
