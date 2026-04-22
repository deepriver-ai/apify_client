from __future__ import annotations

from typing import Any, Dict, List

from src.models.post import Post


class LinkedInPost(Post):
    """A LinkedIn post."""

    @classmethod
    def from_linkedin(cls, item: Dict[str, Any]) -> LinkedInPost:
        """Create a LinkedInPost from a raw harvestapi/linkedin-post-search result."""
        author = item.get("author") or {}
        author_name = (
            author.get("name")
            or author.get("fullName")
            or author.get("firstName", "") + " " + author.get("lastName", "")
        ).strip() or None

        profile_url = (
            author.get("linkedinUrl")
            or author.get("url")
            or author.get("publicIdentifierUrl")
        )

        body = item.get("content") or item.get("text") or item.get("postText") or ""

        reactions = item.get("reactions") or {}
        likes = (
            item.get("likesCount")
            or item.get("numLikes")
            or reactions.get("numLikes")
            or reactions.get("total")
        )

        post_type = _infer_linkedin_post_type(item)
        bio = author.get("headline") or author.get("about") or author.get("summary")

        data = cls._empty_data()
        data.update({
            "timestamp": item.get("postedAt") or item.get("postedAtTimestamp") or item.get("createdAt"),
            "source": author_name or "LinkedIn",
            "body": body,
            "title": (body[:80] or None) if body else None,
            "url": item.get("url") or item.get("postUrl") or item.get("linkedinUrl"),
            "media_urls": _collect_linkedin_media_urls(item),
            "type": "news",
            "author": author_name,
            "author_full_name": author_name,
            "author_profile_bio": bio,
            "author_location_text": author.get("location") or author.get("locationName"),
            "likes": likes,
            "shares": item.get("repostsCount") or item.get("numShares"),
            "n_comments": item.get("commentsCount") or item.get("numComments"),
            "views": item.get("viewsCount") or item.get("numViews"),
            "website_visits": author.get("followersCount") or author.get("connectionsCount"),
            "profile_url": profile_url,
            "post_type": post_type,
        })
        return cls(data=data, raw=item)


def _infer_linkedin_post_type(item: Dict[str, Any]) -> str:
    media = item.get("media") or []
    types = {
        (m.get("type") or m.get("mediaType") or "").lower()
        for m in media if isinstance(m, dict)
    }
    if "video" in types:
        return "Video"
    if "image" in types or "photo" in types:
        return "Image"
    if item.get("documentUrl") or "document" in types:
        return "Document"
    if item.get("articleUrl") or "article" in types:
        return "Article"
    return "Text"


def _collect_linkedin_media_urls(item: Dict[str, Any]) -> List[str]:
    urls: List[str] = []
    for m in item.get("media", []) or []:
        if isinstance(m, dict):
            u = m.get("url") or m.get("mediaUrl") or m.get("imageUrl") or m.get("videoUrl")
            if u:
                urls.append(u)
    for key in ("imageUrl", "videoUrl", "documentUrl", "articleImageUrl"):
        u = item.get(key)
        if u and u not in urls:
            urls.append(u)
    return urls
