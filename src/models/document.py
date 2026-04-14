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
            "author_full_name": None,
            "author_profile_bio": None,
            "author_location_text": None,
            "author_location_id": None,
            "location_author_formatted_name": None,
            "location_author_geoid": None,
            "location_author_coords": None,
            "location_author_precision_level": None,
            "location_author_level_1": None,
            "location_author_level_1_id": None,
            "location_author_level_2": None,
            "location_author_level_2_id": None,
            "location_author_level_3": None,
            "location_author_level_3_id": None,
            "location_ids": [],
            "language": None,
            "comments": [],
            "video_filename": None,
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
        """Return True if any location geoid matches the given country_id prefix.

        Checks ``location_ids`` first (populated by geocoding). If non-empty,
        keeps the document when ANY geoid-style ID starts with ``country_id``.
        If ``location_ids`` is empty, falls back to ``author_location_id``
        prefix match (set by SourcesManagement for news articles).

        Documents with no location data at all are kept.
        """
        if not country_id:
            return True

        # Primary: check location_ids (from geocoding)
        location_ids = self.data.get("location_ids", [])
        if location_ids:
            geoid_style = [str(g) for g in location_ids if str(g).startswith("_")]
            if not geoid_style:
                return True  # no geoid-style IDs → keep
            return any(g.startswith(country_id) for g in geoid_style)

        # Fallback: check author_location_id (from SourcesManagement)
        loc_id = self.data.get("author_location_id")
        if not loc_id:
            return True
        loc_str = str(loc_id)
        if not loc_str.startswith("_"):
            return True
        return loc_str.startswith(country_id)

    def to_final_schema(self) -> Dict[str, Any]:
        """Normalize data to the final schema. Subclasses must implement."""
        raise NotImplementedError

    def enrich_location(self, **kwargs) -> None:
        """Enrich document with location data. Subclasses must implement."""
        raise NotImplementedError