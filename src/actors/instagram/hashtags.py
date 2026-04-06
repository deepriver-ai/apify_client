from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List

from src.actors.actor import ApifyActor
from src.models.instagram_post import InstagramPost
from src.models.sources_management import SourcesManagement

# Instagram Hashtag Scraper
# https://console.apify.com/actors/reGe1ST3OBgYZSsZJ/

logger = logging.getLogger(__name__)


class InstagramHashtagActor(ApifyActor):

    actor_id = "reGe1ST3OBgYZSsZJ"
    comments_actor_id = "apify/instagram-comment-scraper"
    profile_actor_id = "apify/instagram-profile-scraper"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sources_manager = SourcesManagement()

    def search(self, search_params: List[str], **kwargs) -> List[InstagramPost]:
        self.search_params_keywords = search_params
        results_type = kwargs.get("results_type", "posts")
        results_limit = kwargs.get("results_limit") or kwargs.get("max_results", 10)
        keyword_search = kwargs.get("keyword_search", False)

        run_input = {
            "hashtags": search_params,
            "resultsType": results_type,
            "resultsLimit": results_limit,
            "keywordSearch": keyword_search,
        }

        raw_results = self.run_actor(run_input)
        # import pickle
        # with open('/Users/oscarcuellar/Downloads/raw_ig_results_valvoline_test.pkl', 'rb') as f:
        #     raw_results = pickle.load(f)

        posts = [InstagramPost.from_instagram(item) for item in raw_results]
        return self.process_documents(posts, **kwargs)

    def _enrich_content(self, documents: List, **kwargs) -> List:
        """Enrich post content: fetch attached URLs, download media, extract text.

        Platform-specific enrichment stubs will call Instagram-specific actors
        once implemented.
        """
        if kwargs.get("fetch_attached_url"):
            for doc in documents:
                doc.fetch_attached_url(self.sources_manager)
        if kwargs.get("download_images"):
            self._download_images(documents, **kwargs)
        if kwargs.get("download_video"):
            self._download_video(documents, **kwargs)
        if kwargs.get("add_text_from_images"):
            self._add_text_from_images(documents, **kwargs)
        if kwargs.get("add_subtitles"):
            self._add_subtitles(documents, **kwargs)
        if kwargs.get("add_ai_transcription"):
            self._add_ai_transcription(documents, **kwargs)
        return documents

    def _download_images(self, documents: List, **kwargs) -> List:
        raise NotImplementedError(
            "download_images is not yet implemented for Instagram. "
            "Will download post images to cache/ directory."
        )

    def _download_video(self, documents: List, **kwargs) -> List:
        raise NotImplementedError(
            "download_video is not yet implemented for Instagram. "
            "Will download post videos to cache/ directory."
        )

    def _add_text_from_images(self, documents: List, **kwargs) -> List:
        raise NotImplementedError(
            "add_text_from_images is not yet implemented for Instagram. "
            "Will extract OCR text from images (Image, Sidecar types) "
            "and append as image_text to post body."
        )

    def _add_subtitles(self, documents: List, **kwargs) -> List:
        raise NotImplementedError(
            "add_subtitles is not yet implemented for Instagram. "
            "Will extract video subtitles (Video, Story, Reel types) "
            "and append as transcript to post body."
        )

    def _add_ai_transcription(self, documents: List, **kwargs) -> List:
        raise NotImplementedError(
            "add_ai_transcription is not yet implemented for Instagram. "
            "Will use AI to transcribe video audio (Video, Story, Reel types) "
            "and append as transcript to post body."
        )

    def _enrich_user_author(self, documents: List, **kwargs) -> List:
        """Enrich posts with author profile data via apify/instagram-profile-scraper.

        Each Post knows whether it needs a stats update (via Post.needs_user_author_update).
        This method only handles the bulk Apify call and hands results back to
        each Post via Post.save_user_author_stats(). All caching logic lives in Post
        and UsersManagement.
        """
        if not kwargs.get("enrich_followers"):
            return documents

        max_age_days = kwargs.get("stats_max_age_days", 90)

        # Apply cached stats first, collect profiles that need scraping
        profiles_to_scrape: Dict[str, str] = {}  # profile_url → username
        for doc in documents:
            doc.apply_cached_user_author()
            if doc.needs_user_author_update(max_age_days):
                profile_url = doc.data.get("profile_url", "")
                username = profile_url.rstrip("/").rsplit("/", 1)[-1]
                if username:
                    profiles_to_scrape[profile_url] = username

        if not profiles_to_scrape:
            logger.info("All %d profiles have fresh stats, skipping scraper", len(documents))
            return documents

        # Bulk scrape profiles
        usernames = list(set(profiles_to_scrape.values()))
        logger.info("Scraping profiles for %d users", len(usernames))

        run_input: Dict[str, Any] = {"usernames": usernames}
        run = self.client.actor(self.profile_actor_id).call(run_input=run_input)
        raw_profiles: List[Dict[str, Any]] = []
        for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
            raw_profiles.append(item)

        # Index by username
        profiles_by_username: Dict[str, Dict[str, Any]] = {}
        for profile in raw_profiles:
            uname = profile.get("username")
            if uname:
                profiles_by_username[uname] = profile

        # Hand results back to posts
        for doc in documents:
            profile_url = doc.data.get("profile_url", "")
            username = profiles_to_scrape.get(profile_url)
            if not username:
                continue
            profile_data = profiles_by_username.get(username)
            if not profile_data:
                continue
            doc.save_user_author_stats({
                "website_visits": profile_data.get("followersCount"),
                "author": profile_data.get("fullName") or username,
                "author_full_name": profile_data.get("fullName"),
                "author_profile_bio": profile_data.get("biography"),
                "profile_url": profile_url,
            })

        logger.info("Enriched %d profiles with follower stats", len(raw_profiles))
        return documents

    def _enrich_comments(self, documents: List, **kwargs) -> List:
        """Scrape comments for each post via apify/instagram-comment-scraper."""
        get_comments = kwargs.get("get_comments", False)
        if not get_comments:
            return documents

        max_comments = kwargs.get("max_comments", 15)
        post_urls = [doc.data.get("url") for doc in documents if doc.data.get("url")]
        if not post_urls:
            return documents

        logger.info("Scraping comments for %d posts (max %d per post)", len(post_urls), max_comments)

        run_input: Dict[str, Any] = {
            "directUrls": post_urls,
            "resultsLimit": max_comments,
        }

        run = self.client.actor(self.comments_actor_id).call(run_input=run_input)
        raw_comments: List[Dict[str, Any]] = []
        for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
            raw_comments.append(item)

        # Group comments by post URL
        comments_by_url: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for c in raw_comments:
            post_url = c.get("postUrl") or c.get("inputUrl")
            if post_url and not 'error' in c:
                comments_by_url[post_url].append({
                    "comment_text": c.get("text"),
                    "comment_author": c.get("ownerUsername"),
                    "comment_timestamp": c.get("timestamp"),
                    "comment_likes": c.get("likesCount"),
                })

        for doc in documents:
            url = doc.data.get("url")
            if url:
                doc.data["comments"] = comments_by_url[url]

        logger.info("Enriched posts with %d total comments", len(raw_comments))
        return documents
