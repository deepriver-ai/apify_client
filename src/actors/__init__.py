from __future__ import annotations

from src.actors.actor import ApifyActor
from src.actors.facebook.keyword_search import FacebookKeywordSearchActor
from src.actors.facebook.post_comments import FacebookPostCommentsActor
from src.actors.facebook.posts import FacebookPagePostsActor
from src.actors.instagram.hashtags import InstagramHashtagActor
from src.actors.instagram.profile_posts import InstagramProfilePostsActor
from src.actors.instagram.profile_queenlike import InstagramProfileQueenlikeActor
from src.actors.linkedin.keyword_search import LinkedInKeywordSearchActor
from src.actors.news.news_scraper import GoogleNewsActor
from src.actors.tiktok.posts import TikTokPostsActor
from src.actors.twitter.keyword_search import TwitterKeywordSearchActor

ACTOR_REGISTRY = {
    "google_news": GoogleNewsActor,
    "instagram_hashtags": InstagramHashtagActor,
    "instagram_profile_posts": InstagramProfilePostsActor,
    "instagram_profile_queenlike": InstagramProfileQueenlikeActor,
    "facebook_page_posts": FacebookPagePostsActor,
    "facebook_post_comments": FacebookPostCommentsActor,
    "facebook_keyword_search": FacebookKeywordSearchActor,
    "twitter_keyword_search": TwitterKeywordSearchActor,
    "linkedin_keyword_search": LinkedInKeywordSearchActor,
    "tiktok_posts": TikTokPostsActor,
}


def get_actor(actor_class: str, client=None) -> ApifyActor:
    """Instantiate an actor by its registry key.

    ``client`` (optional) is the Apify client the actor and its utility actors use;
    the crawl-run queue worker passes a recording client so every call is metered.
    """
    cls = ACTOR_REGISTRY.get(actor_class)
    if cls is None:
        raise ValueError(
            f"Unknown actor_class '{actor_class}'. "
            f"Available: {', '.join(ACTOR_REGISTRY)}"
        )
    return cls(client=client) if client is not None else cls()
