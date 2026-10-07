from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from src.actors.actor import ApifyActor, PERIOD_DAYS
from src.actors.facebook.comments import FacebookCommentsActor
from src.actors.facebook.profiles import FacebookProfileActor, index_profiles_by_page_name
from src.models.facebook_post import FacebookPost, _extract_facebook_page_name
from src.models.post import _extract_first_external_url

# Facebook Posts Scraper
# https://apify.com/apify/facebook-posts-scraper

logger = logging.getLogger(__name__)

# Facebook's own alt-text carries the embedded image text as
# "May be a graphic/illustration/image of … text that says '<TEXT>'". Only the
# quoted <TEXT> is content; the wrapper (and wrappers without a quote) is not.
_OCR_QUOTED_TEXT_RE = re.compile(
    r"text that says\s*['\"\u2018\u2019\u201c\u201d]?\s*(?P<text>.+?)\s*['\"\u2018\u2019\u201c\u201d]?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_OCR_WRAPPER_PREFIXES = ("may be a ", "may be an ")
_OCR_JUNK = ("no photo description available",)
_OCR_INVISIBLE = ("\u200e", "\u200f", "\u200b")


def extract_ocr_text(ocr_text: Optional[str]) -> Optional[str]:
    """Return the usable text inside a Facebook ``ocrText`` value, or ``None``.

    - ``"May be an image of map and text that says 'HORARIOS…'"`` → ``"HORARIOS…"``
    - ``"May be an image of dog and text"`` (wrapper, no quoted text) → ``None``
    - ``"No photo description available."`` → ``None``
    - anything else (a plain OCR string) is returned stripped.
    """
    if not ocr_text:
        return None
    text = ocr_text
    for ch in _OCR_INVISIBLE:
        text = text.replace(ch, "")
    text = text.strip()
    lowered = text.lower()
    if not text or lowered.startswith(_OCR_JUNK):
        return None
    match = _OCR_QUOTED_TEXT_RE.search(text)
    if match:
        text = match.group("text").strip().strip("'\"\u2018\u2019\u201c\u201d").strip()
        return text or None
    if lowered.startswith(_OCR_WRAPPER_PREFIXES):
        return None
    return text


def append_image_text(documents: List) -> int:
    """Append each post's usable ``media[].ocrText`` to its body as ``image_text_N``.

    Free: the Facebook scrapers already return ``ocrText`` with every media item;
    this only reads the raw response. Returns the number of posts changed.
    """
    changed = 0
    for doc in documents:
        raw_media = (getattr(doc, "_raw", None) or {}).get("media") or []
        if not raw_media:
            continue
        body = doc.data.get("body") or ""
        ocr_index = 0
        for m in raw_media:
            ocr_text = extract_ocr_text((m or {}).get("ocrText"))
            if ocr_text:
                ocr_index += 1
                body += f"\n\n image_text_{ocr_index}: {ocr_text}"
        if ocr_index > 0:
            doc.data["body"] = body
            changed += 1
            logger.info("Added %d image text(s) to post %s", ocr_index, doc.data.get("url", ""))
    return changed


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
        if kwargs.get("add_text_from_images", True):
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
        """Append the text embedded in post images to the body (on by default).

        The Facebook scraper already returns Facebook's alt-text in
        ``media[].ocrText``; ``extract_ocr_text`` keeps only the quoted
        ``text that says '…'`` part and drops wrappers/junk. Appends each usable
        result as ``\\n\\n image_text_N: <text>``. Pass
        ``add_text_from_images=False`` (``actor_params``) to disable.
        """
        append_image_text(documents)
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

        Follows the same pattern as Instagram's _enrich_user_author: apply cached
        stats first, collect stale profiles, bulk scrape, map results back.
        ``enrich_author_after_likes`` gates the scrape (see ``enrich_page_authors``).
        """
        return enrich_page_authors(self, documents, **kwargs)

    def _enrich_comments(self, documents: List, **kwargs) -> List:
        """Scrape comments via apify/facebook-comments-scraper for every post with a URL,
        or — with ``get_comments_after_likes`` — only for posts whose likes exceed it."""
        return enrich_post_comments(self, documents, **kwargs)


# --- Facebook enrichment stages shared by the page and keyword actors -------------


def enrich_page_authors(actor: ApifyActor, documents: List, **kwargs) -> List:
    """``_enrich_user_author`` for Facebook posts (page actor and keyword actor).

    Always applies cached stats. When ``enrich_followers`` is set, bulk-scrapes the
    stale page profiles via ``FacebookProfileActor`` and saves them to
    ``UsersManagement``. ``enrich_author_after_likes`` (int, optional): only posts whose
    ``likes`` exceed it are scraped (``ApifyActor._gate_by_likes``); cached stats still
    apply to all posts. ``stats_max_age_days`` (default 90).
    """
    max_age_days = kwargs.get("stats_max_age_days", 90)

    stale = []
    for doc in documents:
        doc.apply_cached_user_author()  # always, even if enrich_followers is not set
        if doc.needs_user_author_update(max_age_days):
            stale.append(doc)

    if not kwargs.get("enrich_followers"):
        logger.info("enrich_followers not set, applied cached stats only for %d posts", len(documents))
        return documents

    profiles_to_scrape: Dict[str, str] = {}  # profile_url → page_name
    for doc in actor._gate_by_likes(stale, kwargs.get("enrich_author_after_likes"), "profiles"):
        profile_url = doc.data.get("profile_url", "")
        page_name = _extract_facebook_page_name(profile_url)
        if page_name:
            profiles_to_scrape[profile_url] = page_name

    if not profiles_to_scrape:
        logger.info("No stale qualifying profiles among %d posts, skipping scraper", len(documents))
        return documents

    # Deduplicate page URLs and bulk scrape
    page_urls = list(set(profiles_to_scrape.keys()))
    logger.info("Scraping profiles for %d Facebook pages", len(page_urls))

    profile_actor = FacebookProfileActor(actor.client)
    raw_profiles = profile_actor.scrape_pages(page_urls)

    # Index results by every known page-name representation so the lookup succeeds
    # regardless of which key on the profile matches the post's profile_url.
    profiles_by_page = index_profiles_by_page_name(raw_profiles)

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


def enrich_post_comments(actor: ApifyActor, documents: List, only_with_comments: bool = False,
                         **kwargs) -> List:
    """``_enrich_comments`` for Facebook posts (page actor and keyword actor).

    When ``get_comments`` is set, one ``FacebookCommentsActor`` call scrapes up to
    ``max_comments`` (default 15) per qualifying post: posts with a URL (and, with
    ``only_with_comments``, a positive ``n_comments``) whose ``likes`` exceed
    ``get_comments_after_likes`` when given (``ApifyActor._gate_by_likes``). No
    qualifying post → no call. Every post with a URL gets ``comments`` set when a call
    was made (``[]`` for the ones not scraped).
    """
    if not kwargs.get("get_comments"):
        return documents

    max_comments = kwargs.get("max_comments", 15)
    candidates = [doc for doc in documents if doc.data.get("url")
                  and (not only_with_comments or (doc.data.get("n_comments") or 0) > 0)]
    qualifying = actor._gate_by_likes(candidates, kwargs.get("get_comments_after_likes"), "comments")
    post_urls = [doc.data["url"] for doc in qualifying]
    if not post_urls:
        return documents

    logger.info("Scraping comments for %d Facebook posts (max %d per post)", len(post_urls), max_comments)

    comments_actor = FacebookCommentsActor(actor.client)
    raw_comments = comments_actor.scrape_comments(post_urls, max_comments=max_comments)
    comments_by_url = FacebookCommentsActor.group_by_post_url(raw_comments)

    for doc in documents:
        url = doc.data.get("url")
        if url:
            doc.data["comments"] = comments_by_url.get(url, [])

    logger.info("Enriched Facebook posts with %d total comments", len(raw_comments))
    return documents
