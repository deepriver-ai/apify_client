from __future__ import annotations

from src.actors.instagram.hashtags import InstagramHashtagActor
from src.actors.news.news_scraper import GoogleNewsActor

ACTOR_REGISTRY = {
    "google_news": GoogleNewsActor,
    "instagram_hashtags": InstagramHashtagActor,
}


def get_actor(actor_class: str) -> GoogleNewsActor | InstagramHashtagActor:
    """Instantiate an actor by its registry key."""
    cls = ACTOR_REGISTRY.get(actor_class)
    if cls is None:
        raise ValueError(
            f"Unknown actor_class '{actor_class}'. "
            f"Available: {', '.join(ACTOR_REGISTRY)}"
        )
    return cls()
