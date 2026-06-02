from __future__ import annotations

import logging
from typing import Any, Dict, List

from src.actors.actor import ApifyActor
from src.models.news import News

# Google News Scraper
# https://apify.com/gNuQaPoeEXpEyrp6d/google-news-scraper (actor ID: 3Z6SK7F2WoPU3t2sg)

logger = logging.getLogger(__name__)

GOOGLE_NEWS_TOPICS = {
    "WORLD",
    "NATION",
    "BUSINESS",
    "TECHNOLOGY",
    "ENTERTAINMENT",
    "SPORTS",
    "SCIENCE",
    "HEALTH",
}


class GoogleNewsActor(ApifyActor):

    actor_id = "3Z6SK7F2WoPU3t2sg"

    def search(self, search_params: List[str], **kwargs) -> List[News]:
        self.search_params = search_params
        max_articles = kwargs.get("max_articles") or kwargs.get("max_results", 30)
        timeframe = kwargs.get("timeframe", "1d")
        region_language = kwargs.get("region_language", "MX:es-419")
        decode_urls = kwargs.get("decode_urls", True)
        extract_descriptions = kwargs.get("extract_descriptions", True)
        extract_images = kwargs.get("extract_images", False)
        topics = self._normalize_topics(kwargs.get("topics"))

        run_input: Dict[str, Any] = {
            "keywords": search_params,
            "topics": topics,
            "topicUrls": [],
            "maxArticles": max_articles,
            "timeframe": timeframe,
            "region_language": region_language,
            "decodeUrls": decode_urls,
            "extractDescriptions": extract_descriptions,
            "extractImages": extract_images,
            "proxyConfiguration": {"useApifyProxy": True},
        }

        raw_results = self.run_actor(run_input)
        articles = [News.from_google_news(item) for item in raw_results]

        return self.process_documents(articles, **kwargs)

    @staticmethod
    def _normalize_topics(raw_topics: Any) -> List[str]:
        """Normalize Google News topic filters from actor_params."""
        if raw_topics is None or raw_topics == "":
            return []

        if isinstance(raw_topics, str):
            topics = [raw_topics]
        elif isinstance(raw_topics, list):
            topics = raw_topics
        else:
            raise ValueError("GoogleNewsActor topics must be a string or list of strings")

        normalized = [str(topic).strip().upper() for topic in topics if str(topic).strip()]
        invalid = [topic for topic in normalized if topic not in GOOGLE_NEWS_TOPICS]
        if invalid:
            allowed = ", ".join(sorted(GOOGLE_NEWS_TOPICS))
            raise ValueError(f"Invalid Google News topic(s): {', '.join(invalid)}. Allowed topics: {allowed}")
        return normalized

    def _enrich_content(self, documents: List, **kwargs) -> List:
        """Fetch and parse each news article (HTTP + content extraction)."""
        enrich = kwargs.get("enrich", True)
        if not enrich:
            return documents
        for doc in documents:
            doc.fetch_and_parse()
        return documents

    def _enrich_comments(self, documents: List, **kwargs) -> List:
        """Comments are not supported for Google News."""
        if kwargs.get("get_comments"):
            logger.info("get_comments is not supported for Google News scraper, ignoring")
        return documents

    def _enrich_location(self, documents: List, **kwargs) -> List:
        """Set location from SourcesManagement domain lookup and track unknown sources."""
        for doc in documents:
            doc.enrich_location()
        News.sources_manager.save()
        return documents
