from __future__ import annotations

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
            "source": "Instagram",
            "body": item.get("caption"),
            "title": item.get("caption", item.get("ownerFullName"))[:80],
            "url": item.get("url"),
            "media_urls": _collect_media_urls(item),
            "type": "Instagram",
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
