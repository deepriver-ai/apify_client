"""Comments-on-demand actor for individual Facebook post URLs.

``FacebookPostCommentsActor`` is a task-addressable, targeted comment fetch:
given a list of individual Facebook post/video/photo/reel URLs, it scrapes up
to ``max_comments`` comments per URL (one bulk call to
``apify/facebook-comments-scraper`` via :class:`FacebookCommentsActor`) and
builds one minimal-but-schema-valid :class:`FacebookPost` per input URL whose
``url`` equals the input URL exactly.

When published to RabbitMQ, gp3's ``update_social_news_engagement`` recognizes
the URL as an existing ``_id`` in the ES ``news`` index and MERGES the scraped
comments into the existing document (deduped) rather than creating a new one.

Unlike the crawl actors, this actor does NOT run the full
``process_documents`` pipeline: there is no keyword/date/LLM filtering, no
attached-URL fetch, no geocoding, no author-profile scrape, and — critically —
no ``_filter_existing_in_elasticsearch`` stage (which would drop every URL,
since the whole point is to update documents that already exist). ``search()``
returns the built documents directly.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from src.actors.actor import ApifyActor
from src.actors.facebook.comments import FacebookCommentsActor
from src.models.facebook_post import FacebookPost, _extract_facebook_page_name

logger = logging.getLogger(__name__)


def _normalize_post_url(url: str) -> str:
    """Normalize a Facebook URL for matching scraper output back to input.

    Lower-cases the host, drops the scheme, a leading ``www.``, any query
    string / fragment, and a trailing slash. Two URLs that differ only in
    those respects normalize to the same key.
    """
    if not url:
        return ""
    parsed = urlparse(url.strip())
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = parsed.path.rstrip("/")
    return f"{host}{path}"


class FacebookPostCommentsActor(ApifyActor):
    """Fetch N comments for specific Facebook post URLs and publish updates.

    ``search_params`` are individual Facebook post/video/photo/reel URLs.
    Recognized kwargs:
        max_comments (int, default 15): comments to scrape per post.
    """

    actor_id = "apify/facebook-comments-scraper"

    def search(self, search_params: List[str], **kwargs) -> List[FacebookPost]:
        """Scrape comments for each URL and build one FacebookPost per URL.

        Deliberately bypasses ``process_documents`` (no filters/enrichment) so
        that documents for URLs already in Elasticsearch are NOT dropped — this
        operation exists to update those very documents in place.
        """
        self.search_params = search_params
        max_comments = kwargs.get("max_comments", 15)

        post_urls = [u.strip() for u in (search_params or []) if u and u.strip()]
        if not post_urls:
            logger.warning("FacebookPostCommentsActor: no post URLs provided")
            return []

        logger.info(
            "Fetching comments for %d Facebook post URL(s) (max %d per post)",
            len(post_urls),
            max_comments,
        )

        comments_actor = FacebookCommentsActor(self.client)
        raw_comments = comments_actor.scrape_comments(post_urls, max_comments=max_comments)
        comments_by_url = FacebookCommentsActor.group_by_post_url(raw_comments)

        # Index grouped comments by normalized URL so the scraper's echoed
        # postUrl (which may differ from the input in www/scheme/query/slash)
        # still matches the input URL it belongs to.
        normalized_index: Dict[str, List[Dict[str, Any]]] = {}
        for scraped_url, mapped in comments_by_url.items():
            normalized_index.setdefault(_normalize_post_url(scraped_url), []).extend(mapped)

        single_url = len(post_urls) == 1
        # Total mapped comments (used as the fallback for the single-URL case,
        # where every scraped comment necessarily belongs to that one post even
        # if the scraper reported a canonicalized postUrl).
        all_mapped: List[Dict[str, Any]] = [c for cs in comments_by_url.values() for c in cs]

        docs: List[FacebookPost] = []
        for url in post_urls:
            comments = comments_by_url.get(url)
            if not comments:
                comments = normalized_index.get(_normalize_post_url(url), [])
            if not comments and single_url:
                comments = all_mapped
            docs.append(self._build_post(url, comments))

        total = sum(len(d.data.get("comments") or []) for d in docs)
        logger.info(
            "Built %d comment-update document(s) with %d total comments",
            len(docs),
            total,
        )
        return docs

    def _build_post(self, url: str, comments: List[Dict[str, Any]]) -> FacebookPost:
        """Build a minimal, schema-valid FacebookPost carrying only comments.

        ``url`` is set to the input URL verbatim so it matches the existing ES
        ``_id`` and gp3 merges the comments in place. The page slug from the URL
        supplies ``source``/``author`` when derivable (honest best-effort — the
        comments scraper carries no reliable post-level author name).

        Post engagement metrics (likes/shares) AND ``n_comments`` are
        intentionally left unset. The comments scraper carries no post-level
        likes/shares, and ``n_comments`` here would only be the *sampled* count
        (capped at ``max_comments``), which is usually smaller than the post's
        true comment total — gp3's engagement update overwrites ``n_comments``
        with whatever the doc carries, so sending the sample count would clobber
        a truer existing value downward. Leaving all three unset makes gp3 keep
        the existing stats (verified live: likes/shares/n_comments preserved
        while comments still merge in). The ``comments`` list is what this
        operation actually contributes.
        """
        page_name = _extract_facebook_page_name(url)

        data = FacebookPost._empty_data()
        data.update(
            {
                # fetch time — only needed to satisfy the required-timestamp
                # schema constraint; gp3's engagement update does not overwrite
                # the existing document's timestamp.
                "timestamp": datetime.now(tz=ZoneInfo("America/Mexico_City")).isoformat(),
                "source": page_name or "Facebook",
                "url": url,
                "type": "facebook",
                "author": page_name,
                "profile_url": (
                    f"https://www.facebook.com/{page_name}/" if page_name else None
                ),
                "comments": comments,
            }
        )
        return FacebookPost(data=data, raw={})
