#!/usr/bin/env python3
"""Fetch page bios / followers for post-author accounts and cache them (WS-4 v2 input).

The classifier's evidence for a page is its ``author_profile_bio``; ES only carries it
for posts ingested with ``enrich_followers`` on, so most pages arrive with no bio
(Morelia, 2026-09-14: 66 of 87 publishing pages). This script scrapes the missing
profiles through ``apify/facebook-pages-scraper`` (``FacebookProfileActor``) and stores
the result in the users cache (``UsersManagement``, ``cache/users.json``) — the same
store the ingestion actors use. ``classify_users`` reads that cache at harvest time,
so the next run sees the bios. Nothing is written to Elasticsearch.

    python -m src.scripts.enrich_page_bios --report cache/runs/classify_users_X.json
    python -m src.scripts.enrich_page_bios --urls https://www.facebook.com/somepage ...
    [--refresh]  re-scrape even when the cache already has a bio
    [--dry-run]  list what would be scraped, no Apify call

Env: ``APIFI_API_TOKEN`` (repo ``.env``). Cost: pages scraper, ≈ $0.01 per profile.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Dict, List

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def canonical_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return u
    if not u.startswith("http"):
        u = "https://" + u
    if "://facebook.com" in u:
        u = u.replace("://facebook.com", "://www.facebook.com")
    return u.rstrip("/") + "/"


def urls_from_report(path: str) -> List[str]:
    rep = json.load(open(path, encoding="utf-8"))
    return sorted({a["profile_url"] for a in rep
                   if a.get("tier") == "profile" and a.get("n_posts") and a.get("profile_url")})


def main(argv=None) -> Dict[str, int]:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Scrape and cache page bios for WS-4.")
    ap.add_argument("--report", help="classify_users JSON report: take its post-author profile urls")
    ap.add_argument("--urls", nargs="*", default=None, help="explicit profile/page urls")
    ap.add_argument("--refresh", action="store_true", help="re-scrape cached profiles too")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    load_dotenv()

    from src.models.users_management import UsersManagement
    from src.actors.facebook.profiles import FacebookProfileActor

    urls = list(args.urls or [])
    if args.report:
        urls += urls_from_report(args.report)
    urls = sorted({canonical_url(u) for u in urls if u})
    if not urls:
        raise SystemExit("give --report and/or --urls")

    um = UsersManagement()
    todo = []
    for u in urls:
        st = um.get_stats(u)
        if st and st.get("author_profile_bio") and not args.refresh:
            continue
        todo.append(u)
    print(f"profiles {len(urls)} · cached with bio {len(urls) - len(todo)} · to scrape {len(todo)}")
    if args.dry_run or not todo:
        for u in todo:
            print("  ", u)
        return {"profiles": len(urls), "scraped": 0}

    from apify_client import ApifyClient
    from src.actors.actor import _get_apify_token
    actor = FacebookProfileActor(ApifyClient(_get_apify_token()))
    raw = actor.scrape_pages(todo)
    print(f"actor returned {len(raw)} items")

    # Index results by every url form the actor echoes back.
    by_url: Dict[str, dict] = {}
    for item in raw:
        for f in ("facebookUrl", "pageUrl", "url", "inputUrl"):
            v = item.get(f)
            if v:
                by_url[canonical_url(v).lower()] = item
        for f in ("pageName", "pageId", "facebookId"):
            v = item.get(f)
            if v:
                by_url[canonical_url(f"facebook.com/{v}").lower()] = item

    saved = with_bio = 0
    for u in todo:
        item = by_url.get(u.lower())
        if item is None:
            # numeric ids come back as /profile.php?id= or /people/<name>/<id>/ — match by id
            ident = u.rstrip("/").rsplit("/", 1)[-1]
            item = next((it for it in raw if ident and ident in json.dumps(
                {k: it.get(k) for k in ("facebookUrl", "pageUrl", "url", "pageId", "facebookId", "inputUrl")})), None)
        if item is None:
            logger.warning("no profile returned for %s", u)
            continue
        mapped = FacebookProfileActor.map_profile(item)
        mapped["author_full_name"] = mapped.get("author_full_name")
        if mapped.get("website_visits") is None:
            mapped["website_visits"] = 0          # get_stats() treats None as "no stats"
        um.save_stats(u, mapped)
        saved += 1
        if mapped.get("author_profile_bio"):
            with_bio += 1
    print(f"saved {saved} profiles to the users cache · {with_bio} with a bio · "
          f"{len(todo) - saved} not returned")
    return {"profiles": len(urls), "scraped": saved, "with_bio": with_bio}


if __name__ == "__main__":
    main()
