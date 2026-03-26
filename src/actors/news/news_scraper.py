from __future__ import annotations

import logging
from typing import Any, Dict, List

from src.actors.actor import ApifyActor
from src.models.news import News
from src.models.sources_management import SourcesManagement

# Google News Scraper
# https://apify.com/gNuQaPoeEXpEyrp6d/google-news-scraper (actor ID: 3Z6SK7F2WoPU3t2sg)

logger = logging.getLogger(__name__)


class GoogleNewsActor(ApifyActor):

    actor_id = "3Z6SK7F2WoPU3t2sg"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sources_manager = SourcesManagement()

    def search(self, keywords: List[str], **kwargs) -> List[News]:
        max_articles = kwargs.get("max_articles") or kwargs.get("max_results", 30)
        timeframe = kwargs.get("timeframe", "1d")
        region_language = kwargs.get("region_language", "MX:es-419")
        decode_urls = kwargs.get("decode_urls", True)
        extract_descriptions = kwargs.get("extract_descriptions", True)
        extract_images = kwargs.get("extract_images", False)

        run_input: Dict[str, Any] = {
            "keywords": keywords,
            "topics": [],
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

    def _enrich_content(self, documents: List, **kwargs) -> List:
        """Fetch and parse each news article (HTTP + content extraction)."""
        enrich = kwargs.get("enrich", True)
        if not enrich:
            return documents
        for doc in documents:
            doc.fetch_and_parse()
        return documents

    def _enrich_location(self, documents: List, **kwargs) -> List:
        """Set location from SourcesManagement domain lookup and track unknown sources."""
        for doc in documents:
            doc.enrich_location(self.sources_manager)
        self.sources_manager.save()
        return documents
