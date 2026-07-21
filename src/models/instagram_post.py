from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from src.models.post import Post


class InstagramPost(Post):
    """An Instagram post."""

    @classmethod
    def from_instagram(cls, item: Dict[str, Any]) -> InstagramPost:
        """Create an InstagramPost from a raw Instagram Apify result."""
        username = item.get("ownerUsername", "")
        profile_url = f"https://www.instagram.com/{username}/" if username else None

        data = cls._empty_data()
        data.update({
            "timestamp": item.get("timestamp"),
            "source": item.get("ownerFullName") or username or "Instagram",
            "body": item.get("caption"),
            "title": item.get("caption", item.get("ownerFullName", ""))[:80],
            "url": item.get("url"),
            "media_urls": _collect_media_urls(item),
            "type": "instagram",
            "author": item.get("ownerFullName") or username,
            "likes": item.get("likesCount"),
            "shares": item.get("reshareCount"),
            "views": item.get("videoPlayCount") or item.get("igPlayCount"),
            "n_comments": item.get("commentsCount"),
            "profile_url": profile_url,
            "post_type": item.get("type"),
            "author_location_text": item.get("locationName"),
            "author_location_id": item.get("locationId"),  # TODO: map to geoid format
        })
        return cls(data=data, raw=item)

    @classmethod
    def from_instagram_queenlike(cls, item: Dict[str, Any]) -> InstagramPost:
        """Create an InstagramPost from a queenlike_xystos/instagram-posts-reels-scraper result."""
        author = item.get("author") or {}
        username = author.get("username", "")
        profile_url = f"https://www.instagram.com/{username}/" if username else None

        timestamp = item.get("taken_at")
        if isinstance(timestamp, (int, float)):
            try:
                timestamp = datetime.fromtimestamp(int(timestamp), tz=timezone.utc).isoformat()
            except (ValueError, OSError):
                timestamp = None

        location = item.get("location") if isinstance(item.get("location"), dict) else None
        author_location_text = None
        author_location_id = None
        if location:
            author_location_text = location.get("name") or location.get("short_name")
            loc_pk = location.get("pk") or location.get("id")
            if loc_pk is not None:
                author_location_id = str(loc_pk)

        caption = item.get("caption") or ""
        full_name = author.get("full_name")

        data = cls._empty_data()
        data.update({
            "timestamp": timestamp,
            "source": full_name or username or "Instagram",
            "body": caption,
            "title": (caption or full_name or username or "")[:80] or None,
            "url": item.get("url"),
            "media_urls": _collect_queenlike_media_urls(item),
            "type": "instagram",
            "author": full_name or username,
            "author_full_name": full_name,
            "website_visits": author.get("follower_count"),
            "likes": item.get("like_count"),
            "shares": item.get("reshare_count"),
            "views": item.get("play_count"),
            "n_comments": item.get("comment_count"),
            "profile_url": profile_url,
            "post_type": _infer_queenlike_post_type(item),
            "author_location_text": author_location_text,
            "author_location_id": author_location_id,
        })
        return cls(data=data, raw=item)


def _collect_media_urls(item: Dict[str, Any]) -> List[str]:
    """Recursively collect media URLs from an Instagram post item."""
    urls = []
    if item.get("displayUrl"):
        urls.append(item["displayUrl"])
    if item.get("videoUrl"):
        urls.append(item["videoUrl"])
    for img in item.get("images", []):
        if isinstance(img, str):
            urls.append(img)
        elif isinstance(img, dict) and img.get("url"):
            urls.append(img["url"])
    for child in item.get("childPosts", []):
        urls.extend(_collect_media_urls(child))
    return urls


def _infer_queenlike_post_type(item: Dict[str, Any]) -> str:
    """Infer Instagram post type from queenlike scraper output."""
    product_type = (item.get("product_type") or "").lower()
    if product_type in ("clips", "reel", "reels"):
        return "Reel"
    if product_type == "story":
        return "Story"
    if item.get("carousel_media") or item.get("children"):
        return "Sidecar"
    if item.get("video_url") or item.get("video_versions"):
        return "Video"
    return "Image"


def _collect_queenlike_media_urls(item: Dict[str, Any]) -> List[str]:
    """Collect media URLs from a queenlike scraper Instagram item."""
    urls: List[str] = []
    for key in ("video_url", "thumbnail_url", "image_url", "display_url"):
        u = item.get(key)
        if isinstance(u, str) and u:
            urls.append(u)
    for child in item.get("carousel_media", []) or item.get("children", []) or []:
        if isinstance(child, dict):
            urls.extend(_collect_queenlike_media_urls(child))
        elif isinstance(child, str):
            urls.append(child)
    return urls
