from __future__ import annotations

import logging
from typing import Any, Dict, List

from src.actors.actor import ApifyActor
from src.models.linkedin_post import LinkedInPost

# LinkedIn Post Search Scraper (No Cookies)
# https://apify.com/harvestapi/linkedin-post-search

logger = logging.getLogger(__name__)

_PERIOD_TO_POSTED_LIMIT = {"d": "24h", "w": "week", "m": "month"}


class LinkedInKeywordSearchActor(ApifyActor):
    """Keyword/hashtag-based LinkedIn post search.

    Each post already carries author name, headline (bio) and location, so we
    apply cached stats and only call the profile-enrichment flow when
    ``enrich_followers`` is set.
    """

    actor_id = "harvestapi/linkedin-post-search"

    def process_documents(self, documents: List, **kwargs) -> List:
        """Location before user author — geocoding uses body + author.location
        which are already present; extra profile scraping is optional."""
        override = kwargs.get("override_filters", False)
        task_id = kwargs.get("task_id", "")
        all_docs = documents

        if not override and task_id:
            before = len(documents)
            documents = [
                doc for doc in documents
                if self._filter_cache.get(self._filter_cache_key(doc, task_id)) is not False
            ]
            if len(documents) < before:
                logger.info("Filter cache: %d → %d documents (skipped previously filtered for task %s)", before, len(documents), task_id)

        documents = self._filter_keywords(documents, **kwargs)
        documents = self._filter_date(documents, **kwargs)
        documents = self._enrich_content(documents, **kwargs)
        documents = self._filter_language(documents, **kwargs)
        documents = self._enrich_location(documents, **kwargs)
        documents = self._filter_location(documents, **kwargs)
        documents = self._filter_llm(documents, **kwargs)
        documents = self._enrich_user_author(documents, **kwargs)
        documents = self._filter_llm(documents, snippet_max_len=2500, **kwargs)
        documents = self._enrich_comments(documents, **kwargs)

        if task_id:
            survived = {id(doc) for doc in documents}
            for doc in all_docs:
                key = self._filter_cache_key(doc, task_id)
                self._filter_cache[key] = id(doc) in survived
            self._save_filter_cache()

        return documents

    def search(self, search_params: List[str], **kwargs) -> List[LinkedInPost]:
        self.search_params_keywords = search_params
        results_limit = kwargs.get("results_limit") or kwargs.get("max_results", 30)
        period = kwargs.get("period")

        run_input: Dict[str, Any] = {
            "searchQueries": search_params,
            "maxPosts": results_limit,
            "sortBy": "date",
            "profileScraperMode": "short",
        }
        posted_limit = _PERIOD_TO_POSTED_LIMIT.get(period) if period else None
        if posted_limit:
            run_input["postedLimit"] = posted_limit

        logger.info("LinkedIn keyword search: queries=%s max=%d", search_params, results_limit)
        raw_results = self.run_actor(run_input)

        posts = [LinkedInPost.from_linkedin(item) for item in raw_results]
        return self.process_documents(posts, **kwargs)

    def _enrich_content(self, documents: List, **kwargs) -> List:
        if kwargs.get("fetch_attached_url"):
            for doc in documents:
                doc.fetch_attached_url()
        return documents

    def _enrich_user_author(self, documents: List, **kwargs) -> List:
        """Apply cached stats; bulk-scrape only profiles missing follower data
        and only when ``enrich_followers`` is set."""
        max_age_days = kwargs.get("stats_max_age_days", 90)

        to_scrape: List[str] = []
        for doc in documents:
            doc.apply_cached_user_author()
            if doc.needs_user_author_update(max_age_days):
                url = doc.data.get("profile_url")
                if url:
                    to_scrape.append(url)

        if not to_scrape or not kwargs.get("enrich_followers"):
            if not kwargs.get("enrich_followers"):
                logger.info("enrich_followers not set, applied cached stats only for %d posts", len(documents))
            else:
                logger.info("All %d profiles have fresh stats, skipping scraper", len(documents))
            return documents

        unique_urls = list(set(to_scrape))
        logger.info("LinkedIn: enriching %d profiles", len(unique_urls))

        run_input: Dict[str, Any] = {
            "searchQueries": [""],
            "authorUrls": unique_urls,
            "maxPosts": 1,
            "profileScraperMode": "main",
        }
        run = self.client.actor(self.actor_id).call(run_input=run_input)
        profile_data_by_url: Dict[str, Dict[str, Any]] = {}
        for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
            author = item.get("author") or {}
            url = author.get("linkedinUrl") or author.get("url")
            if url:
                profile_data_by_url[url] = author

        for doc in documents:
            url = doc.data.get("profile_url")
            if not url:
                continue
            author = profile_data_by_url.get(url)
            if not author:
                continue
            doc.save_user_author_stats({
                "website_visits": author.get("followersCount") or author.get("connectionsCount"),
                "author": author.get("name"),
                "author_full_name": author.get("name"),
                "author_profile_bio": author.get("headline") or author.get("about"),
                "author_location_text": author.get("location") or author.get("locationName"),
            })

        logger.info("LinkedIn: enriched %d profiles with follower stats", len(profile_data_by_url))
        return documents

    def _enrich_comments(self, documents: List, **kwargs) -> List:
        if not kwargs.get("get_comments"):
            return documents

        max_comments = kwargs.get("max_comments", 15)
        post_urls = [
            doc.data.get("url") for doc in documents
            if doc.data.get("url") and (doc.data.get("n_comments") or 0) > 0
        ]
        if not post_urls:
            return documents

        logger.info("Scraping LinkedIn comments for %d posts (max %d per post)", len(post_urls), max_comments)

        run_input: Dict[str, Any] = {
            "searchQueries": [""],
            "authorUrls": post_urls,
            "maxPosts": 1,
            "scrapeComments": True,
            "maxComments": max_comments,
            "postNestedComments": True,
        }
        run = self.client.actor(self.actor_id).call(run_input=run_input)
        raw_items = list(self.client.dataset(run["defaultDatasetId"]).iterate_items())

        comments_by_url: Dict[str, List[Dict[str, Any]]] = {}
        for item in raw_items:
            post_url = item.get("url") or item.get("postUrl")
            if not post_url:
                continue
            mapped: List[Dict[str, Any]] = []
            for c in item.get("comments", []) or []:
                mapped.append({
                    "comment_text": c.get("text") or c.get("content"),
                    "comment_author": (c.get("author") or {}).get("name"),
                    "comment_timestamp": c.get("postedAt") or c.get("createdAt"),
                    "comment_likes": c.get("likesCount") or c.get("numLikes"),
                })
            comments_by_url[post_url] = mapped

        total = 0
        for doc in documents:
            url = doc.data.get("url")
            if url and url in comments_by_url:
                doc.data["comments"] = comments_by_url[url]
                total += len(comments_by_url[url])

        logger.info("Enriched LinkedIn posts with %d total comments", total)
        return documents
