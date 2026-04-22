from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib.parse import urlparse

from src.models.post import Post


class FacebookPost(Post):
    """A Facebook post."""

    @classmethod
    def from_facebook(cls, item: Dict[str, Any]) -> FacebookPost:
        """Create a FacebookPost from a raw Facebook Posts Scraper result."""
        url = item.get("url", "")

        # Prefer explicit user/page fields from the API; fall back to the page slug in the URL.
        page_name = (
            item.get('user', {}).get('name')
            or item.get("pageName")
            or _extract_facebook_page_name(url)
        )
        profile_url = item.get('facebookUrl') or (f"https://www.facebook.com/{page_name}/" if page_name else None)

        media = item.get("media", [])
        post_type = _infer_facebook_post_type(media, url)
        media_urls = _collect_facebook_media_urls(media)

        # Extract timestamp (unix epoch) if available
        timestamp = None
        raw_ts = item.get("timestamp")
        if raw_ts is not None:
            try:
                timestamp = datetime.fromtimestamp(int(raw_ts), tz=timezone.utc).isoformat()
            except (ValueError, TypeError, OSError):
                pass

        body = item.get("text") or ""

        data = cls._empty_data()
        data.update({
            "timestamp": timestamp,
            "source": page_name or "Facebook", #"Facebook",
            "body": body,
            "title": (item.get("text") or "")[:80] or None,
            "url": url,
            "media_urls": media_urls,
            "type": "news",  # Has to be news as -for now- it is saved as a news object in Elasticsearch
            "author": page_name,
            "likes": item.get("likes"),
            "shares": item.get("shares"),
            "n_comments": item.get("comments"),
            "profile_url": profile_url,
            "post_type": post_type,
        })
        return cls(data=data, raw=item)

    @classmethod
    def from_facebook_search(cls, item: Dict[str, Any]) -> FacebookPost:
        """Create a FacebookPost from a raw scrapeforge/facebook-search-posts result."""
        author = item.get("author") or {}
        author_name = (
            author.get("name")
            or author.get("full_name")
            or item.get("page_name")
        )
        profile_url = (
            author.get("profile_url")
            or author.get("url")
            or (f"https://www.facebook.com/{author_name}/" if author_name else None)
        )

        url = (
            item.get("post_url")
            or item.get("url")
            or item.get("permalink")
        )

        body = item.get("text") or item.get("content") or ""

        timestamp = item.get("timestamp") or item.get("created_time") or item.get("posted_at")
        if isinstance(timestamp, (int, float)):
            try:
                timestamp = datetime.fromtimestamp(int(timestamp), tz=timezone.utc).isoformat()
            except (ValueError, OSError):
                timestamp = None

        post_type = _infer_facebook_search_post_type(item)
        media_urls = _collect_facebook_search_media_urls(item)

        reactions = item.get("reactions") or {}
        likes = (
            item.get("likes_count")
            or item.get("reactions_count")
            or reactions.get("like")
            or reactions.get("total")
        )

        data = cls._empty_data()
        data.update({
            "timestamp": timestamp,
            "source": author_name or "Facebook",
            "body": body,
            "title": (body[:80] or None) if body else None,
            "url": url,
            "media_urls": media_urls,
            "type": "news",
            "author": author_name,
            "author_full_name": author_name,
            "likes": likes,
            "shares": item.get("shares_count") or item.get("shares"),
            "n_comments": item.get("comments_count") or item.get("comments"),
            "profile_url": profile_url,
            "post_type": post_type,
        })
        return cls(data=data, raw=item)


def _infer_facebook_search_post_type(item: Dict[str, Any]) -> str:
    if item.get("video_url") or item.get("videos"):
        return "Video"
    if item.get("images") or item.get("image_url"):
        return "Photo"
    return "Status"


def _collect_facebook_search_media_urls(item: Dict[str, Any]) -> List[str]:
    urls: List[str] = []
    for key in ("image_url", "video_url", "thumbnail"):
        u = item.get(key)
        if u:
            urls.append(u)
    for img in item.get("images", []) or []:
        if isinstance(img, str):
            urls.append(img)
        elif isinstance(img, dict):
            u = img.get("url") or img.get("src")
            if u:
                urls.append(u)
    for vid in item.get("videos", []) or []:
        if isinstance(vid, str):
            urls.append(vid)
        elif isinstance(vid, dict):
            u = vid.get("url") or vid.get("hd") or vid.get("sd")
            if u:
                urls.append(u)
    return urls


def _extract_facebook_page_name(url: str) -> str | None:
    """Extract the page name from a Facebook post URL."""
    if not url:
        return None
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    if parts:
        # Skip known non-page segments
        if parts[0] in ("reel", "watch", "photo", "story"):
            return None
        return parts[0]
    return None


def _infer_facebook_post_type(media: List[Dict[str, Any]], url: str) -> str:
    """Infer Facebook post type from media content and URL."""
    if "/reel/" in url:
        return "Reel"
    if not media:
        return "Status"
    type_names = {m.get("__typename") for m in media}
    if "Video" in type_names:
        return "Video"
    if "Photo" in type_names:
        return "Photo"
    return "Status"


def _collect_facebook_media_urls(media: List[Dict[str, Any]]) -> List[str]:
    """Collect media URLs from Facebook post media objects."""
    urls = []
    for m in media:
        typename = m.get("__typename")
        if typename == "Photo":
            photo_image = m.get("photo_image", {})
            if photo_image.get("uri"):
                urls.append(photo_image["uri"])
            elif m.get("thumbnail"):
                urls.append(m["thumbnail"])
        elif typename == "Video":
            vd = m.get("videoDeliveryLegacyFields", {})
            if vd.get("browser_native_hd_url"):
                urls.append(vd["browser_native_hd_url"])
            elif vd.get("browser_native_sd_url"):
                urls.append(vd["browser_native_sd_url"])
            elif m.get("thumbnail"):
                urls.append(m["thumbnail"])
    return urls
