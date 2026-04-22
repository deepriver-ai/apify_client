from __future__ import annotations

from typing import Any, Dict, List

from src.models.post import Post


class TwitterPost(Post):
    """An X (Twitter) post."""

    @classmethod
    def from_twitter(cls, item: Dict[str, Any]) -> TwitterPost:
        """Create a TwitterPost from a raw api-ninja/x-twitter-advanced-search tweet."""
        user = item.get("author") or item.get("user") or {}
        screen_name = (
            user.get("userName")
            or user.get("screen_name")
            or user.get("username")
            or ""
        )
        profile_url = (
            user.get("url")
            or (f"https://x.com/{screen_name}" if screen_name else None)
        )

        tweet_id = item.get("id") or item.get("id_str") or item.get("tweet_id")
        url = item.get("url") or item.get("twitterUrl") or (
            f"https://x.com/{screen_name}/status/{tweet_id}"
            if screen_name and tweet_id else None
        )

        body = (
            item.get("text")
            or item.get("full_text")
            or item.get("fullText")
            or ""
        )

        post_type = _infer_tweet_type(item)
        author_name = user.get("name") or screen_name or None

        data = cls._empty_data()
        data.update({
            "timestamp": item.get("createdAt") or item.get("created_at"),
            "source": screen_name or "X",
            "body": body,
            "title": (body[:80] or None) if body else None,
            "url": url,
            "media_urls": _collect_tweet_media_urls(item),
            "type": "x",
            "author": author_name,
            "author_full_name": user.get("name"),
            "author_profile_bio": user.get("description"),
            "author_location_text": user.get("location"),
            "likes": item.get("likeCount") or item.get("favorite_count") or item.get("likes"),
            "shares": item.get("retweetCount") or item.get("retweet_count") or item.get("retweets"),
            "views": item.get("viewCount") or item.get("view_count") or item.get("views"),
            "n_comments": item.get("replyCount") or item.get("reply_count") or item.get("replies"),
            "website_visits": user.get("followersCount") or user.get("followers_count"),
            "profile_url": profile_url,
            "post_type": post_type,
            "language": item.get("lang"),
        })
        return cls(data=data, raw=item)


def _infer_tweet_type(item: Dict[str, Any]) -> str:
    if item.get("isReply") or item.get("in_reply_to_status_id"):
        return "Reply"
    if item.get("isQuote") or item.get("quoted_status_id"):
        return "Quote"
    if item.get("isRetweet") or item.get("retweeted_status"):
        return "Retweet"
    return "Tweet"


def _collect_tweet_media_urls(item: Dict[str, Any]) -> List[str]:
    urls: List[str] = []
    media = item.get("media") or item.get("extendedEntities", {}).get("media") or []
    for m in media:
        if isinstance(m, dict):
            u = m.get("media_url_https") or m.get("mediaUrl") or m.get("url")
            if u:
                urls.append(u)
        elif isinstance(m, str):
            urls.append(m)
    entities = item.get("entities") or {}
    for m in entities.get("media", []) or []:
        u = m.get("media_url_https") or m.get("url")
        if u and u not in urls:
            urls.append(u)
    return urls
