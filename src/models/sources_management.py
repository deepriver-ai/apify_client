from __future__ import annotations

import copy
import json
import logging
import os
from typing import Any, Dict, List, Set

from src.helpers.mongoconnection import mongoconn
from src.helpers.str_fn import get_domain

logger = logging.getLogger(__name__)

DEFAULT_CACHE_PATH = "cache/unknown_sources.json"


def _load_sources() -> List[Dict[str, Any]]:
    """Fetch all sources from MongoDB CrawlersAll collection."""
    cursor = mongoconn.admin_app.CrawlersAll.find()
    return [copy.deepcopy(site) for site in cursor]


def _build_known_sources(sources: List[Dict[str, Any]]) -> Set[str]:
    """Build a set of known source domains."""
    return {s["domain"] for s in sources}


def _build_domain_country_id(sources: List[Dict[str, Any]]) -> Dict[str, str]:
    """Build a mapping of domain -> country_id (first 4 chars of location_author_geoid)."""
    return {
        s["domain"]: str(s.get("stats", {}).get("location_author_geoid"))[:4]
        for s in sources
        if s.get("stats", {}).get("location_author_geoid") is not None
    }


def _build_domain_location(sources: List[Dict[str, Any]]) -> Dict[str, Dict[str, str | None]]:
    """Build a mapping of domain -> {location_text, location_id} from source stats."""
    result = {}
    for s in sources:
        stats = s.get("stats", {})
        geoid = stats.get("location_author_geoid")
        if geoid is not None:
            result[s["domain"]] = {
                "location_text": stats.get("location_author_formatted_name"),
                "location_id": geoid,
            }
    return result


# Module-level data loaded once from Mongo
_sources = _load_sources()
_known_sources = _build_known_sources(_sources)
_domain_country_id = _build_domain_country_id(_sources)
_domain_location = _build_domain_location(_sources)


class SourcesManagement:
    """Single interface for all source-related operations.

    Reads source data from MongoDB, provides domain/country lookups,
    tracks unknown sources, and persists them to a cache file.
    """

    def __init__(self, cache_path: str = DEFAULT_CACHE_PATH):
        self.cache_path = cache_path
        self._unknown: List[Dict[str, str]] = []

    # --- Source lookups ---

    def get_domain(self, url: str) -> str:
        """Extract the domain from a URL."""
        return get_domain(url)

    def is_known(self, domain: str) -> bool:
        """Return True if the domain is in the known sources set."""
        return domain in _known_sources

    def get_country_id(self, domain: str) -> str | None:
        """Return the country_id for a domain, or None if not mapped."""
        return _domain_country_id.get(domain)

    def get_location(self, domain: str) -> Dict[str, str | None]:
        """Return {location_text, location_id} for a domain, or both None if not mapped."""
        return _domain_location.get(domain, {"location_text": None, "location_id": None})

    # --- Unknown source tracking ---

    def check_source(self, url: str, source_name: str | None = None) -> bool:
        """Check if a URL's domain is a known source.

        Returns True if known, False if unknown (and records it for later saving).
        """
        domain = get_domain(url)
        if domain in _known_sources:
            return True

        self._unknown.append({
            "name": source_name,
            "domain": domain,
        })
        return False

    def save(self) -> None:
        """Load existing unknown sources from cache, prune known ones, merge new entries, and write back."""
        existing: List[Dict[str, str]] = []
        if os.path.exists(self.cache_path):
            try:
                with open(self.cache_path, "r") as f:
                    existing = json.load(f)
            except (json.JSONDecodeError, OSError):
                existing = []

        seen_domains: set = set()
        merged: List[Dict[str, str]] = []

        for entry in existing + self._unknown:
            domain = entry.get("domain")
            if domain and domain not in seen_domains and domain not in _known_sources:
                seen_domains.add(domain)
                merged.append(entry)

        os.makedirs(os.path.dirname(self.cache_path) or ".", exist_ok=True)
        with open(self.cache_path, "w") as f:
            json.dump(merged, f, indent=2, ensure_ascii=False)

    # --- Filtering ---

    def filter_by_country(self, items: List[Dict[str, Any]], country_id: str = None) -> List[Dict[str, Any]]:
        """Filter items by country_id using the domain_country_id lookup.

        Keeps items whose source domain is unknown, has no country, or matches country_id.
        Discards items whose source domain is mapped to a different country.
        """
        filtered = []
        for item in items:
            url = item.get("link") or item.get("url")
            if not url:
                filtered.append(item)
                continue

            domain = self.get_domain(url)

            if not self.is_known(domain):
                filtered.append(item)
            elif self.get_country_id(domain) is None:
                filtered.append(item)
            elif country_id is None or str(self.get_country_id(domain)) == country_id:
                filtered.append(item)
            else:
                logger.info("Filtered out %s (domain: %s, country: %s)", url, domain, self.get_country_id(domain))

        return filtered


# Source sample schema (from MongoDB CrawlersAll collection):
#
# {'_id': ObjectId('6995408253f4236008c59468'),
#   'minutes_to_sleep': 90,
#   'domain': 'nanchemichoacan.com.mx',
#   'crawler_type': 'not_local',
#   'sitio': 'Nanche Michoacan',
#   'depth': 2,
#   'urls': ['https://nanchemichoacan.com.mx/'],
#   'fecha_maxima': 14,
#   'type': 'site',
#   'crawl_strategy': 'classic',
#   'date_added': datetime.datetime(2026, 2, 17, 22, 30, 6, 70000),
#   'stats': {'stats_date_updated': datetime.datetime(2026, 2, 17, 23, 12, 10, 365000),
#    'source': None,
#    'reuters_trust_pct': None,
#    'tier': 3,
#    'extracted_news': None,
#    'article_value': 15000,
#    'website_visits': 13799,
#    'reach_offline': None,
#    'reach_facebook': None,
#    'reach_instagram': None,
#    'reach_x': None,
#    'reach_tiktok': None,
#    'reach_online': None,
#    'reach_youtube': None,
#    'fraccion': None,
#    'location_author_formatted_name': 'Morelia, Michoacan de Ocampo, Mexico',
#    'location_author_geoid': '_48416053',
#    'location_author_coords': {'lat': 19.68114, 'lon': -101.22292},
#    'location_author_precision_level': 3,
#    'location_author_level_1': 'Mexico',
#    'location_author_level_1_id': '_484',
#    'location_author_level_2': 'Michoacan de Ocampo',
#    'location_author_level_2_id': '_48416',
#    'location_author_level_3': 'Morelia',
#    'location_author_level_3_id': '_48416053',
#    'location_author_level_4': None,
#    'location_author_level_4_id': None,
#    'location_author_level_5': None,
#    'location_author_level_5_id': None,
#    'location_author_level_6': None,
#    'location_author_level_6_id': None,
#    'location_author_level_7': None,
#    'location_author_level_7_id': None}
# }
