from __future__ import annotations

from typing import Any, Dict, Optional
import re
from datetime import datetime
from src.schema.types import UrlList
from src.helpers.str_fn import _is_null


def default_sitio_from_domain(domain: Optional[str]) -> Optional[str]:
    if not domain or not isinstance(domain, str):
        return None
    return re.sub(r"\..*(\..*)?", "", domain.strip())

def default_tier(website_visits: Optional[int]) -> Optional[str]:
    if _is_null(website_visits):
        return None
    if website_visits < 15000:
        return 3
    elif website_visits < 30000:
        return 2
    else:
        return 1

def default_valuacion(website_visits: Optional[int]) -> Optional[str]:
    if _is_null(website_visits):
        return None
    if website_visits < 1000:
        return 5000
    elif website_visits < 10000:
        return 10000
    elif website_visits < 20000:
        return 15000
    elif website_visits < 50000:
        return 20000
    elif website_visits < 100000:
        return 25000
    elif website_visits < 250000:
        return 30000
    elif website_visits < 1000000:
        return 40000
    else:
        return 50000

def date_now(*args, **kwargs) -> datetime:
    return datetime.now()


# Schema for Source object (top-level) with field specs
SOURCE_SCHEMA: Dict[str, Dict[str, Any]] = {
    "minutes_to_sleep":     {"type": int,      "default": 90},
    "domain":               {"type": str,      "required": True},
    "crawler_type":         {"type": str,      "default": "not_local"},
    "sitio":                {"type": str,      "default": lambda obj, ctx: default_sitio_from_domain(obj.get("domain"))},
    "depth":                {"type": int,      "default": 2},
    "urls":                 {"type": UrlList,  "required": True},
    "fecha_maxima":         {"type": int,      "default": 14},
    "type":                 {"type": str,      "default": "site"},
    "crawl_strategy":       {"type": str,      "default": "classic"},
    "date_added":           {"type": datetime, "default": date_now},
    "stats":                {"type": "SourceStats"}
}

# Schema for SourceStats (nested stats object) with field specs
SOURCE_STATS_SCHEMA: Dict[str, Dict[str, Any]] = {
    "stats_date_updated":               {"type": datetime, "default": date_now},
    "source":                           {"type": str},
    "reuters_trust_pct":                {"type": float},
    "tier":                             {"type": int, "default": lambda obj, ctx: default_tier(obj.get("stats", {}).get("website_visits"))},
    "extracted_news":                   {"type": int},
    "article_value":                    {"type": int, "default": lambda obj, ctx: default_valuacion(obj.get("stats", {}).get("website_visits"))},
    "website_visits":                   {"type": int},
    "reach_offline":                    {"type": int},
    "reach_facebook":                   {"type": int},
    "reach_instagram":                  {"type": int},
    "reach_x":                          {"type": int},
    "reach_tiktok":                     {"type": int},
    "reach_online":                     {"type": int},
    "reach_youtube":                    {"type": int},
    "fraccion":                         {"type": str},
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
    "location_author_level_4":          {"type": str},
    "location_author_level_4_id":       {"type": str},
    "location_author_level_5":          {"type": str},
    "location_author_level_5_id":       {"type": str},
    "location_author_level_6":          {"type": str},
    "location_author_level_6_id":       {"type": str},
    "location_author_level_7":          {"type": str},
    "location_author_level_7_id":       {"type": str},
}

LOCATION_COORDS_SCHEMA: Dict[str, Dict[str, Any]] = {
    "lat": {"type": float},
    "lon": {"type": float}
}

__all__ = ["SOURCE_SCHEMA", "SOURCE_STATS_SCHEMA", "LOCATION_COORDS_SCHEMA", "default_sitio_from_domain"]
