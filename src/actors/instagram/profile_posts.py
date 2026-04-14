from __future__ import annotations

import glob
import hashlib
import logging
import os
from collections import defaultdict
from typing import Any, Dict, List

import requests

from src.actors.actor import ApifyActor
from src.models.instagram_post import InstagramPost

# Instagram Posts Scraper (profile-based)
# https://console.apify.com/actors/shu8hvrXbJbY3Eb9W/

logger = logging.getLogger(__name__)

DEFAULT_VIDEO_DIR = os.path.join("cache", "media", "instagram")

# Post types that contain video content
_VIDEO_POST_TYPES = {"Video", "Reel"}


def _url_hash(url: str) -> str:
    """Generate a short deterministic hash from a URL for use as a filename."""
    return hashlib.md5(url.encode()).hexdigest()[:12]


def _video_exists(target_dir: str, url_hash: str) -> str | None:
    """Return the local path if a video with *url_hash* already exists, else None."""
    matches = glob.glob(os.path.join(target_dir, f"{url_hash}.*"))
    return matches[0] if matches else None


def _download_video_file(
    download_url: str,
    target_dir: str,
    url_hash: str,
    ext: str = "mp4",
) -> str | None:
    """Download a video file and return its local path, or None on failure."""
    dest = os.path.join(target_dir, f"{url_hash}.{ext}")
    try:
        resp = requests.get(download_url, stream=True, timeout=120)
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
        return dest
    except Exception as exc:
        logger.error("Failed to download video %s: %s", download_url, exc)
        return None


class InstagramProfilePostsActor(ApifyActor):

    actor_id = "shu8hvrXbJbY3Eb9W"
    video_downloader_actor_id = "igview-owner/instagram-video-downloader"
    comments_actor_id = "apify/instagram-comment-scraper"
    profile_actor_id = "apify/instagram-profile-scraper"

    def process_documents(self, documents: List, **kwargs) -> List:
        """Override pipeline order: enrich location before user author.

        Same pipeline as InstagramHashtagActor — location enrichment runs
        before user author enrichment because geocoding uses body text which
        doesn't depend on profile data.
        """
        override = kwargs.get("override_filters", False)
        task_id = kwargs.get("task_id", "")
        all_docs = documents

        if not override and task_id:
            before = len(documents)
            documents = [
                doc for doc in documents
                if self._filter_cache.get(self._filter_cache_key(doc, task_id)) is not False
            ]
            if len(documents) < before:
                logger.info("Filter cache: %d -> %d documents (skipped previously filtered for task %s)", before, len(documents), task_id)

        documents = self._filter_keywords(documents, **kwargs)
        documents = self._filter_date(documents, **kwargs)
        documents = self._enrich_content(documents, **kwargs)
        documents = self._filter_language(documents, **kwargs)
        documents = self._enrich_location(documents, **kwargs)
        documents = self._filter_location(documents, **kwargs)
        documents = self._filter_llm(documents, **kwargs)
        documents = self._enrich_user_author(documents, **kwargs)
        documents = self._filter_llm(documents, snippet_max_len=2500, **kwargs)
        documents = self._enrich_comments(documents, **kwargs)

        if task_id:
            survived = {id(doc) for doc in documents}
            for doc in all_docs:
                key = self._filter_cache_key(doc, task_id)
                self._filter_cache[key] = id(doc) in survived
            self._save_filter_cache()

        return documents

    # ------------------------------------------------------------------
    # search
    # ------------------------------------------------------------------

    def search(self, search_params: List[str], **kwargs) -> List[InstagramPost]:
        """Scrape posts from Instagram profile URLs.

        Args:
            search_params: List of Instagram profile URLs.
        """
        self.search_params_keywords = []  # profile URLs, not keywords
        results_type = kwargs.get("results_type", "posts")
        results_limit = kwargs.get("results_limit") or kwargs.get("max_results", 10)

        run_input: Dict[str, Any] = {
            "directUrls": search_params,
            "resultsType": results_type,
            "resultsLimit": results_limit,
        }

        raw_results = self.run_actor(run_input)
        posts = [InstagramPost.from_instagram(item) for item in raw_results]
        return self.process_documents(posts, **kwargs)

    # ------------------------------------------------------------------
    # content enrichment
    # ------------------------------------------------------------------

    def _enrich_content(self, documents: List, **kwargs) -> List:
        """Enrich post content: fetch attached URLs, download media, extract text."""
        if kwargs.get("fetch_attached_url"):
            for doc in documents:
                doc.fetch_attached_url()
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
            "download_images is not yet implemented for Instagram profile posts. "
            "Will download post images to cache/ directory."
        )

    def _download_video(self, documents: List, **kwargs) -> List:
        """Download videos for video/reel posts via igview-owner/instagram-video-downloader.

        Sets ``doc.data["video_filename"]`` to the local path for each video post.
        Videos are cached by URL hash so repeated runs skip already-downloaded files.
        """
        video_docs: List[InstagramPost] = [
            doc for doc in documents
            if doc.data.get("post_type") in _VIDEO_POST_TYPES
            or doc._raw.get("videoUrl")
        ]
        if not video_docs:
            logger.info("No video posts to download")
            return documents

        target_dir = kwargs.get("video_dir", DEFAULT_VIDEO_DIR)
        os.makedirs(target_dir, exist_ok=True)

        video_urls = [doc.data["url"] for doc in video_docs if doc.data.get("url")]
        if not video_urls:
            return documents

        logger.info("Downloading videos for %d posts to %s", len(video_urls), target_dir)

        # Bulk call the video downloader actor
        run_input: Dict[str, Any] = {"instagram_urls": video_urls}
        run = self.client.actor(self.video_downloader_actor_id).call(run_input=run_input)

        raw_results: List[Dict[str, Any]] = []
        for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
            raw_results.append(item)

        # Download each video and index by source URL
        downloaded: Dict[str, str | None] = {}
        for item in raw_results:
            source_url = item.get("source_url") or item.get("input_url")
            if not source_url:
                continue
            h = _url_hash(source_url)
            download_url = item.get("download_url")
            ext = item.get("file_extension") or "mp4"

            local_path = _video_exists(target_dir, h)
            if not local_path and download_url:
                local_path = _download_video_file(download_url, target_dir, h, ext)

            downloaded[source_url.rstrip("/")] = local_path

        # Map back to documents
        for doc in video_docs:
            url = (doc.data.get("url") or "").rstrip("/")
            if url in downloaded:
                doc.data["video_filename"] = downloaded[url]

        logger.info("Downloaded %d / %d videos", sum(1 for v in downloaded.values() if v), len(video_urls))
        return documents

    def _add_text_from_images(self, documents: List, **kwargs) -> List:
        raise NotImplementedError(
            "add_text_from_images is not yet implemented for Instagram profile posts. "
            "Will extract OCR text from images and append to post body."
        )

    def _add_subtitles(self, documents: List, **kwargs) -> List:
        raise NotImplementedError(
            "add_subtitles is not yet implemented for Instagram profile posts. "
            "Will extract video subtitles and append to post body."
        )

    def _add_ai_transcription(self, documents: List, **kwargs) -> List:
        raise NotImplementedError(
            "add_ai_transcription is not yet implemented for Instagram profile posts. "
            "Will use AI to transcribe video audio and append to post body."
        )

    # ------------------------------------------------------------------
    # user author enrichment
    # ------------------------------------------------------------------

    def _enrich_user_author(self, documents: List, **kwargs) -> List:
        """Enrich posts with author profile data via apify/instagram-profile-scraper."""
        max_age_days = kwargs.get("stats_max_age_days", 90)

        profiles_to_scrape: Dict[str, str] = {}  # profile_url -> username
        for doc in documents:
            doc.apply_cached_user_author()
            if doc.needs_user_author_update(max_age_days):
                profile_url = doc.data.get("profile_url", "")
                username = profile_url.rstrip("/").rsplit("/", 1)[-1]
                if username:
                    profiles_to_scrape[profile_url] = username

        if not profiles_to_scrape or not kwargs.get("enrich_followers"):
            if not kwargs.get("enrich_followers"):
                logger.info("enrich_followers not set, applied cached stats only for %d posts", len(documents))
            else:
                logger.info("All %d profiles have fresh stats, skipping scraper", len(documents))
            return documents

        usernames = list(set(profiles_to_scrape.values()))
        logger.info("Scraping profiles for %d users", len(usernames))

        run_input: Dict[str, Any] = {"usernames": usernames}
        run = self.client.actor(self.profile_actor_id).call(run_input=run_input)
        raw_profiles: List[Dict[str, Any]] = []
        for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
            raw_profiles.append(item)

        profiles_by_username: Dict[str, Dict[str, Any]] = {}
        for profile in raw_profiles:
            uname = profile.get("username")
            if uname:
                profiles_by_username[uname] = profile

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

    # ------------------------------------------------------------------
    # comment enrichment
    # ------------------------------------------------------------------

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

        comments_by_url: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for c in raw_comments:
            post_url = c.get("postUrl") or c.get("inputUrl")
            if post_url and 'error' not in c:
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
