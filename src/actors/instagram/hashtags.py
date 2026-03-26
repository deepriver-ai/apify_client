from __future__ import annotations

from typing import List

from src.actors.actor import ApifyActor
from src.models.post import Post

# Instagram Hashtag Scraper
# https://console.apify.com/actors/reGe1ST3OBgYZSsZJ/


class InstagramHashtagActor(ApifyActor):

    actor_id = "reGe1ST3OBgYZSsZJ"

    def search(self, keywords: List[str], **kwargs) -> List[Post]:
        results_type = kwargs.get("results_type", "posts")
        results_limit = kwargs.get("results_limit") or kwargs.get("max_results", 10)
        keyword_search = kwargs.get("keyword_search", False)

        run_input = {
            "hashtags": keywords,
            "resultsType": results_type,
            "resultsLimit": results_limit,
            "keywordSearch": keyword_search,
        }

        raw_results = self.run_actor(run_input)
        posts = [Post.from_instagram(item) for item in raw_results]
        return self.process_documents(posts, **kwargs)
