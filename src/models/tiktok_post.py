from __future__ import annotations

from typing import Any, Dict, List

from src.helpers.str_fn import _is_valid_url
from src.models.post import Post


class TikTokPost(Post):
    """A TikTok post."""

    @classmethod
    def from_tiktok(cls, item: Dict[str, Any]) -> TikTokPost:
        """Create a TikTokPost from a raw TikTok scraper result."""
        author = item.get("authorMeta") or item.get("author") or {}
        if not isinstance(author, dict):
            author = {}
        username = (
            author.get("name")
            or author.get("nickName")
            or item.get("authorName")
            or item.get("username")
            or ""
        )
        author_full_name = author.get("nickName") or author.get("nickname") or item.get("authorNickname")
        profile_url = (
            author.get("profileUrl")
            or author.get("url")
            or (f"https://www.tiktok.com/@{username}" if username else None)
        )

        body = (
            item.get("text")
            or item.get("desc")
            or item.get("description")
            or item.get("caption")
            or ""
        )

        data = cls._empty_data()
        data.update({
            "timestamp": item.get("createTimeISO") or item.get("createTime") or item.get("timestamp"),
            "source": "TikTok",
            "body": body,
            "title": (body[:80] or author_full_name or username or None) if body else (author_full_name or username or None),
            "url": item.get("webVideoUrl") or item.get("url") or item.get("videoUrl"),
            "media_urls": _collect_tiktok_media_urls(item),
            "type": "tiktok",
            "author": author_full_name or username or None,
            "author_full_name": author_full_name,
            "author_profile_bio": author.get("signature") or author.get("bio"),
            "likes": item.get("diggCount") or item.get("likes") or item.get("likeCount"),
            "shares": item.get("shareCount") or item.get("shares"),
            "views": item.get("playCount") or item.get("views") or item.get("viewCount"),
            "n_comments": item.get("commentCount") or item.get("commentsCount"),
            "website_visits": author.get("fans") or author.get("followerCount") or author.get("followers"),
            "profile_url": profile_url,
            "post_type": "Video",
            "language": item.get("textLanguage") or item.get("language"),
            "comments": _map_tiktok_comments(item.get("comments")),
        })
        return cls(data=data, raw=item)


def _collect_tiktok_media_urls(item: Dict[str, Any]) -> List[str]:
    urls: List[str] = []

    for key in (
        "videoUrl",
        "videoDownloadUrl",
        "downloadUrl",
        "coverUrl",
        "dynamicCover",
        "originCover",
        "musicCoverUrl",
    ):
        _append_url(urls, item.get(key))

    video_meta = item.get("videoMeta")
    if isinstance(video_meta, dict):
        for key in ("downloadAddr", "playAddr", "coverUrl", "dynamicCover", "originCover"):
            _append_url(urls, video_meta.get(key))

    for image in item.get("imagePost", {}).get("images", []) if isinstance(item.get("imagePost"), dict) else []:
        if isinstance(image, dict):
            _append_url(urls, image.get("imageURL") or image.get("url"))
        else:
            _append_url(urls, image)

    return urls


def _append_url(urls: List[str], value: Any) -> None:
    if isinstance(value, list):
        for item in value:
            _append_url(urls, item)
        return
    if not isinstance(value, str):
        return
    url = value.strip()
    if url and _is_valid_url(url) and url not in urls:
        urls.append(url)


def _map_tiktok_comments(comments: Any) -> List[Dict[str, Any]]:
    if not isinstance(comments, list):
        return []

    mapped: List[Dict[str, Any]] = []
    for comment in comments:
        if not isinstance(comment, dict):
            continue
        author = comment.get("user") or comment.get("author") or {}
        if not isinstance(author, dict):
            author = {}
        mapped.append({
            "comment_text": comment.get("text") or comment.get("commentText"),
            "comment_author": (
                author.get("nickname")
                or author.get("uniqueId")
                or author.get("name")
                or comment.get("author")
            ),
            "comment_timestamp": comment.get("createTimeISO") or comment.get("createTime") or comment.get("timestamp"),
            "comment_likes": comment.get("diggCount") or comment.get("likes") or comment.get("likeCount"),
        })
    return mapped
