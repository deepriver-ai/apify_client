from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from src.actors.actor import PERIOD_DAYS, ApifyActor
from src.actors.facebook.posts import append_image_text, enrich_page_authors, enrich_post_comments
from src.models.facebook_post import FacebookPost

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
        self._reset_filtered_documents()

        documents = self._filter_cached_documents(documents, task_id, override)

        documents = self._filter_keywords(documents, **kwargs)
        documents = self._filter_date(documents, **kwargs)
        documents = self._filter_existing_in_elasticsearch(documents, **kwargs)
        documents = self._enrich_content(documents, **kwargs)
        documents = self._filter_language(documents, **kwargs)
        documents = self._filter_llm(documents, **kwargs)
        documents = self._filter_llm(documents, snippet_max_len=2500, **kwargs)
        documents = self._enrich_user_author(documents, **kwargs)
        documents = self._enrich_location(documents, **kwargs)
        documents = self._filter_location(documents, **kwargs)
        documents = self._enrich_comments(documents, **kwargs)

        self._remember_filter_decisions(all_docs, documents, task_id)

        return documents

    def _enrich_content(self, documents: List, **kwargs) -> List:
        if kwargs.get("fetch_attached_url"):
            for doc in documents:
                external_url = doc._raw.get("external_url")
                doc.fetch_attached_url(url=external_url)
        # Free (reads media[].ocrText when the search actor returns it); no-op otherwise.
        if kwargs.get("add_text_from_images", True):
            append_image_text(documents)
        return documents

    def _filter_llm(self, documents: List, **kwargs) -> List:
        filtered = super()._filter_llm(documents, **kwargs)
        if not kwargs.get("llm_filter_condition") or not documents:
            return filtered

        kept_ids = {id(doc) for doc in filtered}
        removed = [doc for doc in documents if id(doc) not in kept_ids]
        if not removed:
            return filtered

        snippet_max_len = kwargs.get("snippet_max_len", 250)
        for doc in removed:
            logger.info(
                "Facebook keyword LLM filtered url=%s snippet_max_len=%s",
                doc.data.get("url") or "<missing>",
                snippet_max_len,
            )
        return filtered

    def _enrich_user_author(self, documents: List, **kwargs) -> List:
        """Apply cached stats; when ``enrich_followers`` is set, bulk-scrape stale page
        profiles via ``FacebookProfileActor`` (persisted to ``UsersManagement``) — the
        page actor's stage (``enrich_page_authors``), incl. ``enrich_author_after_likes``:
        only posts whose ``likes`` exceed it are scraped; cached stats apply to all.
        """
        return enrich_page_authors(self, documents, **kwargs)

    def _enrich_comments(self, documents: List, **kwargs) -> List:
        """Scrape comments for posts that have any (``n_comments > 0``) — the page
        actor's stage (``enrich_post_comments``), incl. ``get_comments_after_likes``:
        only posts whose ``likes`` exceed it are scraped.
        """
        return enrich_post_comments(self, documents, only_with_comments=True, **kwargs)
