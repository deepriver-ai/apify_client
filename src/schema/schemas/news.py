from __future__ import annotations

from typing import Any, Dict, Optional
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from tldextract import TLDExtract
from src.schema.types import EnumStr, Url, UrlList
from src.helpers.str_fn import _is_valid_url

news_types = ["news", "X", "Facebook", "impreso", "Instagram", "Radio", "TV"]


def date_now(*args, **kwargs) -> datetime:
    return datetime.now(tz=ZoneInfo("America/Mexico_City"))


def default_timestamp_added(obj: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> str:
    """Default timestamp_added to current execution time"""
    return datetime.now(tz=ZoneInfo("America/Mexico_City")).isoformat()



def default_source_extra_found_source(obj: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> bool:
    """Default __FOUND_SOURCE__ flag"""
    return obj.get("__FOUND_SOURCE__") is not None


def require_url(obj: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> bool:
    if obj['message'].get("type", "").lower() != "impreso":
        return _is_valid_url(obj['message'].get("url"))
    return True


# preprocess testigos: media_urls
# Schema for News document, as expected by news ingest GP3
NEWS_SCHEMA: Dict[str, Dict[str, Any]] = {
    # Message structure fields
    "author": {"type": list},
    "body": {"type": str, "required": True},
    "body_links": {"type": str},
    "footnote": {"type": str},
    "highlights": {"type": str},
    "lead": {"type": str},
    "location": {"type": str},
    "media_pictures": {"type": str},
    "column_name": {"type": str},
    "column_author": {"type": str},
    "media_videos": {"type": str},
    "fb_likes": {"type": int},
    "source": {
        "type": str,
        "required": True  # Required if source not found
    },
    "source_category": {"type": str},
    "source_comments": {"type": str},
    "source_related": {"type": str},
    "source_social_networks_shares": {"type": str},
    "source_tags": {"type": str},
    "timestamp": {"type": datetime, "required": True},
    "timestamp_added": {"type": datetime, "default": date_now},
    "title": {"type": str, "required": True},
    "type": {"type": EnumStr, "default": "news", "enum": news_types},
    "url": {
        "type": Url, 
        "required": require_url  # Only required if type is not "impreso"
    },
    "media_urls": {"type": UrlList},
    "comments": {"type": "List[Comment]"},
    "location_ids": {"type": list},

    # Nested objects
    "source_extra": {"type": "SourceExtra"},
    "supplier": {"type": "Supplier"}
}

# Schema for a single Comment (nested list item on News)
COMMENT_SCHEMA: Dict[str, Dict[str, Any]] = {
    "comment_text":      {"type": str},
    "comment_author":    {"type": str},
    "comment_timestamp": {"type": datetime},
    "comment_likes":     {"type": int},
}

# Schema for SourceExtra (nested object)
SOURCE_EXTRA_SCHEMA: Dict[str, Dict[str, Any]] = {
    "stats": {"type": "SourceExtraStats"},
    "__FOUND_SOURCE__": {"type": bool, "default": default_source_extra_found_source}
}

# Schema for SourceExtraStats (nested in SourceExtra)
SOURCE_EXTRA_STATS_SCHEMA: Dict[str, Dict[str, Any]] = {
    "article_value": {"type": int},
    "website_visits": {"type": int},
    "likes": {"type": int},
    "shares": {"type": int},
    "views": {"type": int},
    "n_comments": {"type": int},
    "profile_url": {"type": str},
    "post_type": {"type": str},
    "location_author_formatted_name":   {"type": str},
    "location_author_geoid":            {"type": str},
    "location_author_coords":           {"type": "LocationCoords"},
    "location_author_precision_level":  {"type": int},
    "location_author_level_1":          {"type": str},
    "location_author_level_1_id":       {"type": str},
    "location_author_level_2":          {"type": str},
    "location_author_level_2_id":       {"type": str},
    "location_author_level_3":          {"type": str},
    "location_author_level_3_id":       {"type": str},
    "author_full_name":                 {"type": str},
    "author_profile_bio":               {"type": str},
    "video_filename":                   {"type": str}
}

# Schema for LocationCoords (nested object)
LOCATION_COORDS_SCHEMA: Dict[str, Dict[str, Any]] = {
    "lat": {"type": float},
    "lon": {"type": float}
}

# Schema for Supplier (nested object)
SUPPLIER_SCHEMA: Dict[str, Dict[str, Any]] = {
    "name": {"type": str},
    "creador": {"type": int},
    "opinion": {"type": str}
}

# Schema for the final message wrapper
MESSAGE_WRAPPER_SCHEMA: Dict[str, Dict[str, Any]] = {
    "type": {"type": str, "default": "news"},
    "message": {"type": "News"}
}

__all__ = [
    "NEWS_SCHEMA",
    "COMMENT_SCHEMA",
    "SOURCE_EXTRA_SCHEMA",
    "SOURCE_EXTRA_STATS_SCHEMA",
    "SUPPLIER_SCHEMA",
    "MESSAGE_WRAPPER_SCHEMA",
    "default_timestamp_added",
    "default_source_extra_found_source",
]
