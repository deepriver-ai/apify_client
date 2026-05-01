from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from src.actors.actor import ApifyActor, PERIOD_DAYS
from src.actors.facebook.comments import FacebookCommentsActor
from src.actors.facebook.profiles import FacebookProfileActor, index_profiles_by_page_name
from src.models.facebook_post import FacebookPost, _extract_facebook_page_name
from src.models.post import _extract_first_external_url

# Facebook Posts Scraper
# https://apify.com/apify/facebook-posts-scraper

logger = logging.getLogger(__name__)


class FacebookPagePostsActor(ApifyActor):
    '''
    Works for pages urls
    '''
    actor_id = "apify/facebook-posts-scraper"

    def search(self, search_params: List[str], **kwargs) -> List[FacebookPost]:
        '''
        scraping is url-based, skip the search_params_keywords
        '''
        results_limit = kwargs.get("results_limit") or kwargs.get("max_results", 20)

        start_urls = [{"url": url.strip()} for url in search_params]

        # Build date filter from min_date or period
        newer_than = None
        min_date = kwargs.get("min_date")
        period = kwargs.get("period")
        if min_date and isinstance(min_date, datetime):
            newer_than = min_date.strftime("%Y-%m-%d")
        elif period:
            days = PERIOD_DAYS.get(period)
            if days:
                newer_than = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        run_input: Dict[str, Any] = {
            "startUrls": start_urls,
            "resultsLimit": results_limit,
            "captionText": True,
        }
        if newer_than:
            run_input["onlyPostsNewerThan"] = newer_than

        raw_results = self.run_actor(run_input)
        import pickle
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        task_id = kwargs.get("task_id")
        with open(f'/Users/oscarcuellar/Downloads/raw_fbpage_results_{task_id}_{ts}.pkl', 'wb') as f:
            pickle.dump(raw_results, f)

        
        #with open('/Users/oscarcuellar/Downloads/raw_fbpage_results_5_20260406_230451.pkl', 'rb') as f:
        #    raw_results = pickle.load(f)

        posts = [FacebookPost.from_facebook(item) for item in raw_results]

        return self.process_documents(posts, **kwargs)

    def process_documents(self, documents: List, **kwargs) -> List:
        """Run the base pipeline, then fall back to the first comment for an
        attached URL when the post body did not yield one.

        Facebook pages frequently drop the article link into the first comment
        rather than the post body, so after comments have been scraped we
        retry the attach step for any post still missing ``attached_news``.
        """
        documents = super().process_documents(documents, **kwargs)

        if not kwargs.get("fetch_attached_url"):
            return documents

        for doc in documents:
            if doc.attached_news is not None:
                continue
            comments = doc.data.get("comments") or []
            if not comments:
                continue
            first_comment_text = comments[0].get("comment_text") or ""
            if not first_comment_text:
                continue
            url = _extract_first_external_url(first_comment_text, doc.data.get("url"))
            if not url:
                continue
            logger.info("Fetching attached URL from first comment: %s", url)
            doc.fetch_attached_url(url=url)

        return documents

    def _enrich_content(self, documents: List, **kwargs) -> List:
        """Enrich post content: fetch attached URLs, download media, extract text.

        Platform-specific enrichment stubs will call Facebook-specific actors
        once implemented.
        """
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
        """Download images from Facebook posts. Not yet implemented."""
        raise NotImplementedError(
            "download_images is not yet implemented for Facebook. "
            "Will download post images to cache/ directory."
        )

    def _download_video(self, documents: List, **kwargs) -> List:
        """Download videos from Facebook posts. Not yet implemented."""
        raise NotImplementedError(
            "download_video is not yet implemented for Facebook. "
            "Will download post videos to cache/ directory."
        )

    def _add_text_from_images(self, documents: List, **kwargs) -> List:
        """Append OCR text from Facebook media ocrText fields to post body.

        The Facebook scraper already returns OCR text in media[].ocrText.
        Appends each non-trivial OCR result as ``\\n\\n image_text_N: <text>``.
        Skips generic descriptions like "May be an image of".
        """
        for doc in documents:
            raw_media = doc._raw.get("media", [])
            if not raw_media:
                continue
            body = doc.data.get("body") or ""
            ocr_index = 0
            for m in raw_media:
                ocr_text = m.get("ocrText")
                if ocr_text and not ocr_text.startswith("May be an image of"):
                    ocr_index += 1
                    body += f"\n\n image_text_{ocr_index}: {ocr_text}"
            if ocr_index > 0:
                doc.data["body"] = body
                logger.info("Added %d OCR text(s) to post %s", ocr_index, doc.data.get("url", ""))
        return documents

    def _add_subtitles(self, documents: List, **kwargs) -> List:
        """Extract subtitles from Facebook videos. Not yet implemented.

        Applicable post types: Video, Reel.
        Will call a Facebook-specific subtitle extraction actor.
        """
        raise NotImplementedError(
            "add_subtitles is not yet implemented for Facebook. "
            "Will extract video subtitles (Video, Reel types) "
            "and append as transcript to post body."
        )

    def _add_ai_transcription(self, documents: List, **kwargs) -> List:
        """AI-transcribe Facebook videos. Not yet implemented.

        Applicable post types: Video, Reel.
        Will call a Facebook-specific AI transcription actor.
        """
        raise NotImplementedError(
            "add_ai_transcription is not yet implemented for Facebook. "
            "Will use AI to transcribe video audio (Video, Reel types) "
            "and append as transcript to post body."
        )

    def _enrich_user_author(self, documents: List, **kwargs) -> List:
        """Enrich posts with page profile data via apify/facebook-pages-scraper.

        Follows the same pattern as Instagram's _enrich_user_author:
        apply cached stats first, collect stale profiles, bulk scrape,
        map results back to posts.
        """
        max_age_days = kwargs.get("stats_max_age_days", 90)

        # Always apply cached stats, even if enrich_followers is not set
        profiles_to_scrape: Dict[str, str] = {}  # profile_url → page_name
        for doc in documents:
            doc.apply_cached_user_author()
            if doc.needs_user_author_update(max_age_days):
                profile_url = doc.data.get("profile_url", "")
                page_name = _extract_facebook_page_name(profile_url)
                if page_name:
                    profiles_to_scrape[profile_url] = page_name

        if not profiles_to_scrape or not kwargs.get("enrich_followers"):
            if not kwargs.get("enrich_followers"):
                logger.info("enrich_followers not set, applied cached stats only for %d posts", len(documents))
            else:
                logger.info("All %d profiles have fresh stats, skipping scraper", len(documents))
            return documents

        # Deduplicate page URLs and bulk scrape
        page_urls = list(set(profiles_to_scrape.keys()))
        logger.info("Scraping profiles for %d Facebook pages", len(page_urls))

        profile_actor = FacebookProfileActor(self.client)
        raw_profiles = profile_actor.scrape_pages(page_urls)

        # Index results by every known page-name representation so the
        # lookup succeeds regardless of which key on the profile matches
        # the post's profile_url.
        profiles_by_page = index_profiles_by_page_name(raw_profiles)

        # Map results back to posts
        for doc in documents:
            profile_url = doc.data.get("profile_url", "")
            page_name = profiles_to_scrape.get(profile_url)
            if not page_name:
                continue
            profile_data = profiles_by_page.get(page_name)
            if not profile_data:
                continue
            mapped = FacebookProfileActor.map_profile(profile_data)
            doc.save_user_author_stats(mapped)

        logger.info("Enriched %d profiles with page stats", len(raw_profiles))
        return documents

    def _enrich_comments(self, documents: List, **kwargs) -> List:
        """Scrape comments for each post via apify/facebook-comments-scraper."""
        if not kwargs.get("get_comments"):
            return documents

        max_comments = kwargs.get("max_comments", 15)
        post_urls = [doc.data.get("url") for doc in documents if doc.data.get("url")]
        if not post_urls:
            return documents

        logger.info("Scraping comments for %d Facebook posts (max %d per post)", len(post_urls), max_comments)

        comments_actor = FacebookCommentsActor(self.client)
        raw_comments = comments_actor.scrape_comments(post_urls, max_comments=max_comments)
        comments_by_url = FacebookCommentsActor.group_by_post_url(raw_comments)

        for doc in documents:
            url = doc.data.get("url")
            if url:
                doc.data["comments"] = comments_by_url.get(url, [])

        logger.info("Enriched Facebook posts with %d total comments", len(raw_comments))
        return documents
