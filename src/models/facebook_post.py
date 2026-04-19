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

        # Extract page name from URL (e.g., "QroMunicipio" from facebook.com/QroMunicipio/...)
        #page_name = _extract_facebook_page_name(url)
        page_name = item.get('user', {}).get('name') or item.get("pageName")
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
