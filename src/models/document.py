from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from dateutil import parser as dateutil_parser
from langdetect import LangDetectException, detect

from src.helpers.language import normalize_language


class Document:
    """Base class for media documents (news articles, social media posts, etc.)."""

    def __init__(self, data: Dict[str, Any] | None = None):
        self.data: Dict[str, Any] = data if data is not None else self._empty_data()

    @staticmethod
    def _empty_data() -> Dict[str, Any]:
        """Return an empty intermediate schema dictionary with all fields set to None/[]."""
        return {
            "timestamp": None,
            "source": None,
            "title": None,
            "body": None,
            "url": None,
            "media_urls": [],
            "type": None,
            "author": None,
            "article_value": None,
            "website_visits": None,
            "likes": None,
            "shares": None,
            "views": None,
            "n_comments": None,
            "profile_url": None,
            "post_type": None,
            "location_text": None,
            "location_id": None,
            "language": None,
        }

    def detect_language(self) -> str | None:
        """Detect language from body/title text, cache in self.data['language'].

        If language is already set (e.g. by the actor from metadata), returns
        the cached value without re-detecting. Returns ISO 639-1 code or None.
        """
        cached = self.data.get("language")
        if cached:
            return cached
        text = self.data.get("body") or self.data.get("title")
        if not text:
            return None
        try:
            lang = detect(text)
            self.data["language"] = lang
            return lang
        except LangDetectException:
            return None

    def matches_language(self, language: str) -> bool:
        """Return True if document's language matches the given code.

        Normalizes both sides to ISO 639-1 before comparing.
        Documents where detection fails are kept.
        """
        if not language:
            return True
        target = normalize_language(language)
        if not target:
            return True
        detected = normalize_language(self.detect_language())
        if detected is None:
            return True  # keep documents where detection fails
        return detected == target

    def matches_min_date(self, min_date: datetime) -> bool:
        """Return True if the document's timestamp is >= min_date.

        Documents with no timestamp are kept. Timezone info is stripped before
        comparison so naive and aware datetimes can be compared safely.
        """
        raw = self.data.get("timestamp")
        if not raw:
            return True
        try:
            if isinstance(raw, datetime):
                ts = raw
            else:
                ts = dateutil_parser.parse(str(raw))
            # Normalize both sides to naive for comparison
            ts_naive = ts.replace(tzinfo=None)
            min_naive = min_date.replace(tzinfo=None)
            return ts_naive >= min_naive
        except (ValueError, OverflowError):
            return True  # keep documents with unparseable timestamps

    def matches_location(self, country_id: str) -> bool:
        """Return True if the document's location_id matches the given country_id prefix.

        Uses geoid prefix matching: location_id ``_48416053`` matches
        country_id ``_484`` (Mexico). Documents with no location_id or
        non-geoid location_ids (e.g. Instagram numeric IDs) are kept.
        """
        if not country_id:
            return True
        loc_id = self.data.get("location_id")
        if not loc_id:
            return True
        loc_str = str(loc_id)
        # Only filter on geoid-style IDs (start with "_")
        if not loc_str.startswith("_"):
            return True
        return loc_str.startswith(country_id)

    def to_final_schema(self) -> Dict[str, Any]:
        """Normalize data to the final schema. Subclasses must implement."""
        raise NotImplementedError
