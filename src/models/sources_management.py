from __future__ import annotations

import copy
import json
import logging
import os
from typing import Any, Dict, List, Set

from src.helpers.mongoconnection import mongoconn
from src.helpers.str_fn import domainsplitter, get_domain

logger = logging.getLogger(__name__)

DEFAULT_CACHE_PATH = "cache/unknown_sources.json"

_BLACKLISTED_DOMAINS = {
    "youtube.com", "youtu.be",
    "x.com", "twitter.com",
    "facebook.com", "fb.com",
    "instagram.com",
    "linkedin.com",
    "tiktok.com",
    "threads.net",
    "reddit.com",
    "pinterest.com",
    "whatsapp.com", "wa.me",
    "t.me",
    "goo.gl",  # Google URL shortener (covers maps.app.goo.gl, etc.) — never news
}

# Domains that are only blacklisted under specific path prefixes (e.g.
# google.com hosts a lot of things, but /maps URLs are never news articles).
_BLACKLISTED_PATH_PREFIXES: Dict[str, tuple] = {
    "google.com": ("/maps",),
}


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


def _build_domain_source_name(sources: List[Dict[str, Any]]) -> Dict[str, str]:
    """Build a mapping of domain -> source name (``sitio`` in Mongo)."""
    return {s["domain"]: s["sitio"] for s in sources if s.get("sitio")}


def _build_domain_location(sources: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Build a mapping of domain -> full author location dict from source stats."""
    result = {}
    for s in sources:
        stats = s.get("stats", {})
        geoid = stats.get("location_author_geoid")
        if geoid is not None:
            loc: Dict[str, Any] = {
                "author_location_text": stats.get("location_author_formatted_name"),
                "author_location_id": geoid,
                "location_author_formatted_name": stats.get("location_author_formatted_name"),
                "location_author_geoid": geoid,
                "location_author_coords": stats.get("location_author_coords"),
                "location_author_precision_level": stats.get("location_author_precision_level"),
            }
            for i in range(1, 4):
                loc[f"location_author_level_{i}"] = stats.get(f"location_author_level_{i}")
                loc[f"location_author_level_{i}_id"] = stats.get(f"location_author_level_{i}_id")
            result[s["domain"]] = loc
    return result


# Module-level data loaded once from Mongo
_sources = _load_sources()
_known_sources = _build_known_sources(_sources)
_domain_country_id = _build_domain_country_id(_sources)
_domain_source_name = _build_domain_source_name(_sources)
_domain_location = _build_domain_location(_sources)


class SourcesManagement:
    """Single interface for all source-related operations.

    Reads source data from MongoDB, provides domain/country lookups,
    tracks unknown sources, and persists them to a cache file.
    """

    def __init__(self, cache_path: str = DEFAULT_CACHE_PATH):
        self.cache_path = cache_path
        self._unknown: List[Dict[str, str]] = []

    # --- Blacklist ---

    def is_blacklisted(self, url: str) -> bool:
        """Return True if the URL is a known non-news platform.

        Matches by domain (full or subdomain of an entry in
        ``_BLACKLISTED_DOMAINS``), or by domain+path prefix for entries in
        ``_BLACKLISTED_PATH_PREFIXES`` (e.g. ``google.com/maps``).
        """
        domain = get_domain(url)
        if not domain:
            return False
        if any(domain == bl or domain.endswith("." + bl) for bl in _BLACKLISTED_DOMAINS):
            return True
        from urllib.parse import urlparse
        path = urlparse(url).path or ""
        for bl_domain, prefixes in _BLACKLISTED_PATH_PREFIXES.items():
            if domain == bl_domain or domain.endswith("." + bl_domain):
                if any(path.startswith(p) for p in prefixes):
                    return True
        return False

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

    def get_source_name(self, domain: str) -> str | None:
        """Return the source name for a domain.

        Looks up the known ``sitio`` from Mongo; falls back to the domain
        portion without its public-suffix TLD (``elfinanciero.com.mx`` →
        ``elfinanciero``). Returns ``None`` for a falsy domain.
        """
        if not domain:
            return None
        name = _domain_source_name.get(domain)
        if name:
            return name
        extracted = domainsplitter(domain)
        return extracted.domain or domain

    def get_location(self, domain: str) -> Dict[str, Any]:
        """Return full author location dict for a domain, or all-None if not mapped."""
        default: Dict[str, Any] = {
            "author_location_text": None,
            "author_location_id": None,
            "location_author_formatted_name": None,
            "location_author_geoid": None,
            "location_author_coords": None,
            "location_author_precision_level": None,
        }
        for i in range(1, 4):
            default[f"location_author_level_{i}"] = None
            default[f"location_author_level_{i}_id"] = None
        return _domain_location.get(domain, default)

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
