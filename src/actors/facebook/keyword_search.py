from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from src.actors.actor import PERIOD_DAYS, ApifyActor
from src.actors.facebook.comments import FacebookCommentsActor
from src.actors.facebook.profiles import FacebookProfileActor, index_profiles_by_page_name
from src.models.facebook_post import FacebookPost, _extract_facebook_page_name

# Facebook Search Posts
# https://apify.com/scrapeforge/facebook-search-posts

logger = logging.getLogger(__name__)


class FacebookKeywordSearchActor(ApifyActor):
    """Keyword/hashtag-based Facebook post search.

    The upstream actor accepts a single ``query`` per run, so this actor loops
    over each entry in ``search_params`` and concatenates the raw results.
    Results are mapped to ``FacebookPost`` via ``FacebookPost.from_facebook_search``.
    """

    actor_id = "scrapeforge/facebook-search-posts"

    def search(self, search_params: List[str], **kwargs) -> List[FacebookPost]:
        self.search_params_keywords = search_params
        results_limit = kwargs.get("results_limit") or kwargs.get("max_results", 20)
        recent_first = kwargs.get("recent_posts", True)
        location_uid = kwargs.get("location_uid")

        start_date = None
        min_date = kwargs.get("min_date")
        period = kwargs.get("period")
        if min_date and isinstance(min_date, datetime):
            start_date = min_date.strftime("%Y-%m-%d")
        elif period:
            days = PERIOD_DAYS.get(period)
            if days:
                start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        raw_all: List[Dict[str, Any]] = []
        for query in search_params:
            run_input: Dict[str, Any] = {
                "query": query,
                "search_type": "posts",
                "max_results": results_limit,
                "recent_posts": recent_first,
            }
            if start_date:
                run_input["start_date"] = start_date
            if location_uid:
                run_input["location_uid"] = location_uid
            logger.info("Facebook keyword search: query=%r max=%d", query, results_limit)
            raw_all.extend(self.run_actor(run_input))

        seen: Dict[str, Dict[str, Any]] = {}
        for item in raw_all:
            key = item.get("post_url") or item.get("url") or id(item)
            if key not in seen:
                seen[key] = item
        raw_results = list(seen.values())
        logger.info("Facebook keyword search: %d raw results (deduped from %d)", len(raw_results), len(raw_all))

        posts = [FacebookPost.from_facebook_search(item) for item in raw_results]
        return self.process_documents(posts, **kwargs)

    def process_documents(self, documents: List, **kwargs) -> List:
        """Override pipeline order: run ``_filter_llm`` right after
        ``_filter_language`` so LLM-rejected posts skip the expensive
        author/location enrichment and comment scraping stages.
        """
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
        documents = self._filter_llm(documents, **kwargs)
        documents = self._filter_llm(documents, snippet_max_len=2500, **kwargs)
        documents = self._enrich_user_author(documents, **kwargs)
        documents = self._enrich_location(documents, **kwargs)
        documents = self._filter_location(documents, **kwargs)
        documents = self._enrich_comments(documents, **kwargs)

        if task_id:
            survived = {id(doc) for doc in documents}
            for doc in all_docs:
                key = self._filter_cache_key(doc, task_id)
                self._filter_cache[key] = id(doc) in survived
            self._save_filter_cache()

        return documents

    def _enrich_content(self, documents: List, **kwargs) -> List:
        if kwargs.get("fetch_attached_url"):
            for doc in documents:
                external_url = doc._raw.get("external_url")
                doc.fetch_attached_url(url=external_url)
        return documents

    def _enrich_user_author(self, documents: List, **kwargs) -> List:
        """Apply cached stats; when ``enrich_followers`` is set, bulk-scrape
        stale page profiles via ``FacebookProfileActor`` and persist results to
        ``UsersManagement``. Mirrors ``FacebookPagePostsActor._enrich_user_author``.

        ``enrich_author_after_likes`` (int, optional): only scrape profiles for
        posts whose ``likes`` exceed this threshold. Cached stats still apply
        to all posts regardless of likes.
        """
        max_age_days = kwargs.get("stats_max_age_days", 90)
        likes_threshold = kwargs.get("enrich_author_after_likes")

        profiles_to_scrape: Dict[str, str] = {}  # profile_url → page_name
        for doc in documents:
            doc.apply_cached_user_author()
            if not doc.needs_user_author_update(max_age_days):
                continue
            if likes_threshold is not None and (doc.data.get("likes") or 0) <= likes_threshold:
                continue
            profile_url = doc.data.get("profile_url", "")
            page_name = _extract_facebook_page_name(profile_url)
            if page_name:
                profiles_to_scrape[profile_url] = page_name

        if not profiles_to_scrape or not kwargs.get("enrich_followers"):
            if not kwargs.get("enrich_followers"):
                logger.info("enrich_followers not set, applied cached stats only for %d posts", len(documents))
            else:
                logger.info("All %d profiles have fresh stats, skipping scraper", len(documents))
            return documents

        page_urls = list(set(profiles_to_scrape.keys()))
        logger.info("Scraping profiles for %d Facebook pages", len(page_urls))

        profile_actor = FacebookProfileActor(self.client)
        raw_profiles = profile_actor.scrape_pages(page_urls)

        profiles_by_page = index_profiles_by_page_name(raw_profiles)

        for doc in documents:
            profile_url = doc.data.get("profile_url", "")
            page_name = profiles_to_scrape.get(profile_url)
            if not page_name:
                continue
            profile_data = profiles_by_page.get(page_name)
            if not profile_data:
                continue
            mapped = FacebookProfileActor.map_profile(profile_data)
            doc.save_user_author_stats(mapped)

        logger.info("Enriched %d profiles with page stats", len(raw_profiles))
        return documents

    def _enrich_comments(self, documents: List, **kwargs) -> List:
        """Scrape comments for posts that have any.

        ``get_comments_after_likes`` (int, optional): only scrape comments for
        posts whose ``likes`` exceed this threshold.
        """
        if not kwargs.get("get_comments"):
            return documents

        max_comments = kwargs.get("max_comments", 15)
        likes_threshold = kwargs.get("get_comments_after_likes")
        post_urls: List[str] = []
        for doc in documents:
            url = doc.data.get("url")
            if not url or (doc.data.get("n_comments") or 0) <= 0:
                continue
            if likes_threshold is not None and (doc.data.get("likes") or 0) <= likes_threshold:
                continue
            post_urls.append(url)
        if not post_urls:
            return documents

        logger.info("Scraping comments for %d Facebook posts (max %d per post)", len(post_urls), max_comments)

        comments_actor = FacebookCommentsActor(self.client)
        raw_comments = comments_actor.scrape_comments(post_urls, max_comments=max_comments)
        comments_by_url = FacebookCommentsActor.group_by_post_url(raw_comments)

        for doc in documents:
            url = doc.data.get("url")
            if url:
                doc.data["comments"] = comments_by_url.get(url, [])

        logger.info("Enriched Facebook posts with %d total comments", len(raw_comments))
        return documents
