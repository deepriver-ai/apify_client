from __future__ import annotations

from typing import Any, Dict, List

from src.models.document import Document
from src.schema import normalize_record


class Post(Document):
    """A social media post (Instagram, Facebook, X, etc.)."""

    @classmethod
    def from_instagram(cls, item: Dict[str, Any]) -> Post:
        """Create a Post from a raw Instagram Apify result."""
        username = item.get("ownerUsername", "")
        profile_url = f"https://www.instagram.com/{username}/" if username else None

        data = cls._empty_data()
        data.update({
            "timestamp": item.get("timestamp"),
            "source": "Instagram",
            "body": item.get("caption"),
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
            "location_text": item.get("locationName"),
            "location_id": item.get("locationId"),
        })
        return cls(data=data)

    def to_final_schema(self) -> Dict[str, Any]:
        """Normalize to the MessageWrapper schema."""
        return normalize_record(self.data, "MessageWrapper")


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
