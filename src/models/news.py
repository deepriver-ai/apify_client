from __future__ import annotations

import logging
from typing import Any, Dict

from src.models.document import Document
from src.models.news_parser.load_url import fetch_html
from src.models.news_parser.parser import extract_article
from src.models.sources_management import SourcesManagement
from src.schema import normalize_record

logger = logging.getLogger(__name__)


class News(Document):
    """A news article document with fetching, parsing, and schema normalization."""

    sources_manager = SourcesManagement()

    @classmethod
    def from_url(cls, url: str) -> News | None:
        """Create a News object from a URL, fetch and parse it.

        Skips blacklisted domains. Tracks unknown sources and copies source
        location fields into the News data when available.

        Returns the News object if successful, None if fetch/parse fails or blacklisted.
        """
        if cls.sources_manager.is_blacklisted(url):
            logger.info("Skipping blacklisted URL: %s", url)
            return None

        news = cls()
        news.data["url"] = url
        news.data["type"] = "news"
        if not news.fetch_and_parse():
            return None

        # Track source and copy location fields from SourcesManagement
        final_url = news.data.get("url") or url
        domain = cls.sources_manager.get_domain(final_url)
        cls.sources_manager.check_source(final_url, news.data.get("source"))
        if domain:
            location = cls.sources_manager.get_location(domain)
            for key, value in location.items():
                if value is not None and news.data.get(key) is None:
                    news.data[key] = value

        return news

    @classmethod
    def from_google_news(cls, item: Dict[str, Any]) -> News:
        """Create a News object from a raw Google News Apify result.

        Pure data mapping — no enrichment or source lookups.
        """
        url = item.get("link") or item.get("url")
        data = cls._empty_data()
        data.update({
            "url": url,
            "title": item.get("title"),
            "body": item.get("description"),
            "media_urls": [item["image"]] if item.get("image") else [],
            "timestamp": item.get("publishedAt") or item.get("timestamp"),
            "source": item.get("source"),
            "type": "news",
        })
        return cls(data=data)

    def fetch_and_parse(self) -> bool:
        """Fetch HTML and parse the article, updating self.data in place.

        Returns True if enrichment succeeded, False otherwise.
        """
        url = self.data.get("url")
        if not url:
            return False

        try:
            html, final_url = fetch_html(url)
            if final_url and final_url != url:
                logger.info("URL redirected: %s -> %s", url, final_url)
                self.data["url"] = final_url
            if html:
                parse_url = final_url or url
                parsed = extract_article(html, parse_url)
                if parsed:
                    self.data["title"] = parsed.get("title") or self.data["title"]
                    self.data["body"] = parsed.get("body") or self.data["body"]
                    self.data["author"] = parsed.get("author") or self.data["author"]
                    self.data["media_urls"] = parsed.get("media_urls") or self.data["media_urls"]
                else:
                    logger.warning("Could not parse article for %s", parse_url)
            else:
                logger.warning("Could not fetch HTML for %s", url)
        except Exception as ex:
            logger.error("Enrichment failed for %s: %s", url, ex)
            return False

        return True

    def enrich_location(self, **kwargs) -> None:
        """Set full author location fields from SourcesManagement domain lookup.

        Also tracks unknown sources for later review.
        """
        url = self.data.get("url")
        if not url:
            return
        domain = self.sources_manager.get_domain(url)
        location = self.sources_manager.get_location(domain)
        for key, value in location.items():
            self.data[key] = value
        self.sources_manager.check_source(url, self.data.get("source"))

    def to_final_schema(self) -> Dict[str, Any]:
        """Normalize to the MessageWrapper schema."""
        return normalize_record(self.data, "MessageWrapper")
