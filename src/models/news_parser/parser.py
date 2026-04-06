from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

MIN_BODY_LENGTH = 200


def extract_article(html: str, url: str) -> Optional[Dict[str, Any]]:
    """
    Extract article content from HTML using a three-tier strategy:
    1. NewsPlease
    2. newspaper (fills gaps or standalone fallback)
    3. LLM fallback
    """
    result = _try_newsplease(html, url)

    if result is not None:
        _fill_from_newspaper(result, html, url)
    else:
        result = _try_newspaper(html, url)

    if result and _has_meaningful_content(result):
        return result

    # Both parsers failed or produced insufficient content — try LLM
    logger.warning("Both parsers failed or produced insufficient content — trying LLM")
    llm_result = _parse_with_llm(html, url)
    if llm_result and _has_meaningful_content(llm_result):
        return llm_result

    return result


def _try_newsplease(html: str, url: str) -> Optional[Dict[str, Any]]:
    """Parse article with NewsPlease."""
    try:
        from newsplease import NewsPlease

        parsed = NewsPlease.from_html(html, url=url).get_dict()
        return {
            "title": parsed.get("title") or "",
            "body": parsed.get("maintext") or "",
            "author": _first_author(parsed.get("authors")),
            "media_urls": [parsed["image_url"]] if parsed.get("image_url") else [],
        }
    except Exception as ex:
        logger.warning("NewsPlease failed for %s: %s", url, ex)
        return None


def _try_newspaper(html: str, url: str) -> Optional[Dict[str, Any]]:
    """Parse article with newspaper as standalone fallback."""
    try:
        import newspaper

        article = newspaper.Article(url=url)
        article.html = html
        article.parse()

        return {
            "title": article.title or "",
            "body": article.text or "",
            "author": article.authors[0] if article.authors else None,
            "media_urls": [article.top_image] if article.top_image else [],
        }
    except Exception as ex:
        logger.warning("newspaper failed for %s: %s", url, ex)
        return None


def _fill_from_newspaper(result: Dict[str, Any], html: str, url: str) -> None:
    """Fill missing fields in a NewsPlease result using newspaper."""
    fields_to_check = ["title", "body", "author"]
    if all(_field_ok(result, f) for f in fields_to_check):
        return

    try:
        import newspaper

        article = newspaper.Article(url=url)
        #article.set_html(html)  # Older newspaper version?
        article.html = html
        article.parse()

        if not _field_ok(result, "title") and article.title:
            result["title"] = article.title
        if not _field_ok(result, "body") and article.text:
            result["body"] = article.text
        if not _field_ok(result, "author") and article.authors:
            result["author"] = article.authors[0]
        if not result.get("media_urls") and article.top_image:
            result["media_urls"] = [article.top_image]
    except Exception as ex:
        logger.warning("newspaper fill failed for %s: %s", url, ex)


def _parse_with_llm(html: str, url: str, override: bool = False) -> Optional[Dict[str, Any]]:
    """Last-resort: ask an LLM to extract article content from truncated HTML."""
    try:
        from src.oai.llm_core import llm_cached_call, parse_json_response, get_text_content
        from src.helpers.html_cleaner import clean_html

        cleaned_html = clean_html(html)[:20000]

        def build_messages():
            return [
                {
                    "role": "system",
                    "content": (
                        "Extract the news article content from the following pre cleaned HTML (stripped of many tags, attributes and trash).\n"
                        "Return a JSON object matching this schema:\n\n"
                        "{\n"
                        '  "title": (string) The article headline. The main title of the news piece, '
                        "not the site name, section header, or navigation text.\n"
                        '  "body": (string) The full article text. Concatenate all content paragraphs in order. '
                        "Exclude navigation, ads, sidebars, related-article links, comments, and footers. "
                        "Do NOT repeat the title in the body.\n"
                        '  "author": (string|null) The journalist or writer who wrote the article. '
                        "This is a person's name, NOT the news organization or source name "
                        '(e.g. "Juan Pérez", not "Reuters"). null if not found.\n'
                        '  "media_urls": (list of strings) Absolute URLs of images directly illustrating '
                        "the article (hero image, inline editorial photos). "
                        "Exclude site logos, icons, ad banners, author avatars, and UI elements. "
                        "Empty list [] if none found.\n"
                        "}\n\n"
                        "If a field cannot be determined, use null for strings and [] for media_urls. "
                        "Return ONLY the JSON object, no extra text."
                    ),
                },
                {
                    "role": "user",
                    "content": cleaned_html,
                },
            ]

        def parse_fn(response):
            return parse_json_response(get_text_content(response))

        parsed = llm_cached_call(
            cache_tag="article_parse",
            request_id=url,
            cache_field="article",
            messages_builder=build_messages,
            parse_fn=parse_fn,
            override=override,
        )

        if isinstance(parsed, dict):
            return {
                "title": parsed.get("title") or "",
                "body": parsed.get("body") or "",
                "author": parsed.get("author"),
                "media_urls": parsed.get("media_urls") or [],
            }
    except Exception as ex:
        logger.warning("LLM parse failed for %s: %s", url, ex)

    return None


def _has_meaningful_content(result: Dict[str, Any]) -> bool:
    """Check if the parsed result has a title and enough body text."""
    if not result:
        return False
    has_title = bool(result.get("title", "").strip())
    has_body = len(result.get("body", "")) >= MIN_BODY_LENGTH
    return has_title and has_body


def _field_ok(result: Dict[str, Any], field: str) -> bool:
    """Check if a field has a non-empty value."""
    val = result.get(field)
    if val is None:
        return False
    if isinstance(val, str):
        return len(val) > 0
    if isinstance(val, list):
        return len(val) > 0
    return True


def _first_author(authors: Optional[List[str]]) -> Optional[str]:
    """Extract the first author from a list, or None."""
    if authors and len(authors) > 0:
        return authors[0]
    return None
