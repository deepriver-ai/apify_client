from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

from src.actors.actor import ApifyActor
from src.helpers.language import normalize_language
from src.models.twitter_post import TwitterPost

# X (Twitter) Advanced Search Scraper
# https://apify.com/api-ninja/x-twitter-advanced-search

logger = logging.getLogger(__name__)

_PERIOD_TO_WITHIN = {"d": "1d", "w": "7d", "m": "30d"}


class TwitterKeywordSearchActor(ApifyActor):
    """Keyword/phrase/hashtag-based X (Twitter) search.

    Each tweet already carries author name, bio, location and follower count
    on the ``user`` object, so we skip ``_enrich_user_author`` entirely and
    also do not need a dedicated comments actor — X replies are themselves
    tweets.
    """

    actor_id = "api-ninja/x-twitter-advanced-search"

    def process_documents(self, documents: List, **kwargs) -> List:
        """Drop ``_enrich_user_author`` (author fields arrive with each tweet)."""
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
        documents = self._filter_llm(documents, snippet_max_len=2500, **kwargs)
        documents = self._enrich_comments(documents, **kwargs)

        if task_id:
            survived = {id(doc) for doc in documents}
            for doc in all_docs:
                key = self._filter_cache_key(doc, task_id)
                self._filter_cache[key] = id(doc) in survived
            self._save_filter_cache()

        return documents

    def search(self, search_params: List[str], **kwargs) -> List[TwitterPost]:
        self.search_params_keywords = search_params
        results_limit = kwargs.get("results_limit") or kwargs.get("max_results", 30)
        language = kwargs.get("language")

        keywords: List[str] = []
        phrases: List[str] = []
        hashtags: List[str] = []
        for term in search_params:
            t = term.strip().strip('"').strip("'")
            if not t:
                continue
            if t.startswith("#"):
                hashtags.append(t.lstrip("#"))
            elif " " in t:
                phrases.append(t)
            else:
                keywords.append(t)

        run_input: Dict[str, Any] = {
            "search_type": "Latest",
            "numberOfTweets": max(20, results_limit),
        }
        if keywords:
            run_input["contentKeywords"] = keywords
        if phrases:
            run_input["contentExactPhrases"] = phrases
        if hashtags:
            run_input["contentHashtags"] = hashtags

        lang_iso = normalize_language(language) if language else None
        if lang_iso:
            run_input["contentLanguage"] = lang_iso

        min_date = kwargs.get("min_date")
        period = kwargs.get("period")
        if min_date and isinstance(min_date, datetime):
            run_input["timeSince"] = min_date.strftime("%Y-%m-%d")
        elif period and period in _PERIOD_TO_WITHIN:
            run_input["timeWithinTime"] = _PERIOD_TO_WITHIN[period]

        if not any(k in run_input for k in ("contentKeywords", "contentExactPhrases", "contentHashtags")):
            run_input["query"] = " OR ".join(search_params) if search_params else ""

        logger.info("X keyword search: keywords=%s phrases=%s hashtags=%s", keywords, phrases, hashtags)
        raw_results = self.run_actor(run_input)

        posts = [TwitterPost.from_twitter(item) for item in raw_results]
        return self.process_documents(posts, **kwargs)

    def _enrich_content(self, documents: List, **kwargs) -> List:
        if kwargs.get("fetch_attached_url"):
            for doc in documents:
                doc.fetch_attached_url()
        return documents
