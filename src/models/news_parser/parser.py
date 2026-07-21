from __future__ import annotations

import json
import logging
import re
from collections import Counter
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

MIN_BODY_LENGTH = 200

# A JSON-LD ``articleBody`` (or an extractor-produced body) shorter than this is
# treated as too thin to trust, so the cascade continues to the next tier.
MIN_JSONLD_BODY_LENGTH = 200

# "See more" link texts that repeat when a parser mistakes a related-posts
# carousel for the article body. Three or more hits = list, not article.
_RELATED_LINK_MARKERS = (
    "ver más",
    "leer más",
    "read more",
    "see more",
    "continue reading",
    "saber más",
    "más información",
)
_RELATED_LINK_THRESHOLD = 3

# Section-label lines that repeat when a parser mistakes a related-content
# block for the article body. On OEM (Organización Editorial Mexicana) pages the
# extracted "body" is a stack of "SECTION\nHeadline\nDek" triples, e.g.
# "LOCAL\nMejoraron movilidad...\nLOCAL\nResguardan a lomitos...". These labels
# never appear as standalone lines inside a real article's prose, so 3+ of them
# (counted as full lines) means we grabbed a navigation/related list, not the story.
_SECTION_LABELS = frozenset(
    {
        "local",
        "policiaca",
        "policíaca",
        "cultura",
        "deportes",
        "finanzas",
        "nacional",
        "internacional",
        "mundo",
        "republica",
        "república",
        "gossip",
        "elecciones",
        "espectaculos",
        "espectáculos",
        "tecnologia",
        "tecnología",
        "sociedad",
        "economia",
        "economía",
        "estados",
        "municipios",
        "opinion",
        "opinión",
        "seguridad",
        "salud",
        "negocios",
        "virales",
    }
)
_SECTION_LABEL_THRESHOLD = 3
# A short standalone line that repeats verbatim is a label/nav artifact, not prose.
_REPEATED_LINE_MAX_LEN = 40
_REPEATED_LINE_THRESHOLD = 3


def extract_article(html: str, url: str) -> Optional[Dict[str, Any]]:
    """
    Extract article content from HTML using a multi-tier strategy:

    1. **JSON-LD ``articleBody``** — deterministic, generic across any site that
       ships a ``NewsArticle``/``Article``/``ReportageNewsArticle`` node with a
       non-trivial ``articleBody``. Instant, no LLM.
    2. **Domain-keyed extractor registry** — site-specific deterministic
       extractors (e.g. OEM's Next.js RSC flight-stream parser) keyed by
       registrable domain. Instant, no LLM.
    3. **NewsPlease** (+ ``newspaper4k`` gap-fill), guarded by
       ``_has_meaningful_content`` (rejects related-content lists).
    4. **LLM fallback** (slow, last resort) when everything above fails.

    Tiers 1 and 2 return immediately and log at INFO which tier produced the
    body; on any failure they return ``None`` so the cascade continues
    unchanged.
    """
    jsonld_result = _try_jsonld(html, url)
    if jsonld_result is not None:
        return jsonld_result

    domain_result = _try_domain_extractor(html, url)
    if domain_result is not None:
        return domain_result

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


# --------------------------------------------------------------------------- #
# Tier 1 — generic JSON-LD articleBody
# --------------------------------------------------------------------------- #
# schema.org article types whose ``articleBody`` we trust as the full story.
_ARTICLE_LD_TYPES = frozenset(
    {
        "newsarticle",
        "article",
        "reportagenewsarticle",
        "blogposting",
        "backgroundnewsarticle",
        "opinionnewsarticle",
        "analysisnewsarticle",
    }
)

_LD_JSON_RE = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)


def _iter_jsonld_nodes(html: str):
    """Yield every JSON object found in the page's ld+json scripts.

    Handles bare objects, top-level arrays, and ``@graph`` arrays by flattening
    them into a stream of candidate nodes.
    """
    for match in _LD_JSON_RE.finditer(html):
        raw = match.group(1).strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, list):
                stack.extend(node)
            elif isinstance(node, dict):
                yield node
                graph = node.get("@graph")
                if isinstance(graph, list):
                    stack.extend(graph)


def _ld_type_matches(node: Dict[str, Any]) -> bool:
    node_type = node.get("@type")
    if isinstance(node_type, list):
        types = node_type
    elif node_type is not None:
        types = [node_type]
    else:
        return False
    return any(isinstance(t, str) and t.lower() in _ARTICLE_LD_TYPES for t in types)


def _find_jsonld_article(html: str) -> Optional[Dict[str, Any]]:
    """Return the first JSON-LD node whose ``@type`` is an article type."""
    for node in _iter_jsonld_nodes(html):
        if _ld_type_matches(node):
            return node
    return None


def _jsonld_author(node: Dict[str, Any]) -> Optional[str]:
    author = node.get("author")
    if isinstance(author, list):
        author = author[0] if author else None
    if isinstance(author, dict):
        name = author.get("name")
        return name if isinstance(name, str) and name.strip() else None
    if isinstance(author, str) and author.strip():
        return author
    return None


def _jsonld_image_urls(node: Dict[str, Any]) -> List[str]:
    image = node.get("image")
    urls: List[str] = []

    def _collect(img: Any) -> None:
        if isinstance(img, str) and img.strip():
            urls.append(img)
        elif isinstance(img, dict):
            url = img.get("url")
            if isinstance(url, str) and url.strip():
                urls.append(url)
        elif isinstance(img, list):
            for it in img:
                _collect(it)

    _collect(image)
    return urls


def _jsonld_word_count(node: Dict[str, Any]) -> Optional[int]:
    wc = node.get("wordCount")
    if isinstance(wc, bool):
        return None
    if isinstance(wc, int):
        return wc
    if isinstance(wc, str):
        try:
            return int(wc.strip())
        except ValueError:
            return None
    return None


def _try_jsonld(html: str, url: str) -> Optional[Dict[str, Any]]:
    """Tier 1: return an article built from a JSON-LD ``articleBody``.

    Generic across any site that publishes a full ``articleBody`` in an
    ``application/ld+json`` article node (including inside a ``@graph``). Returns
    ``None`` (cascade continues) when no such node exists or its body is too
    thin. OEM pages have no ``articleBody`` and fall through to Tier 2.
    """
    try:
        node = _find_jsonld_article(html)
        if node is None:
            return None
        body = node.get("articleBody")
        if not isinstance(body, str):
            return None
        body = body.strip()
        if len(body) < MIN_JSONLD_BODY_LENGTH:
            return None
        title = node.get("headline") or node.get("name") or ""
        logger.info("extract_article: JSON-LD articleBody tier produced body for %s", url)
        return {
            "title": title if isinstance(title, str) else "",
            "body": body,
            "author": _jsonld_author(node),
            "media_urls": _jsonld_image_urls(node),
            "timestamp": node.get("datePublished"),
        }
    except Exception as ex:
        logger.warning("JSON-LD parse failed for %s: %s", url, ex)
        return None


# --------------------------------------------------------------------------- #
# Tier 2 — domain-keyed deterministic extractors
# --------------------------------------------------------------------------- #
def _registrable_domain(url: str) -> str:
    """Return the registrable domain (e.g. ``oem.com.mx``) for a URL."""
    try:
        import tldextract

        ext = tldextract.extract(url)
        if ext.domain and ext.suffix:
            return f"{ext.domain}.{ext.suffix}".lower()
        return (ext.domain or "").lower()
    except Exception:
        return ""


def _try_domain_extractor(html: str, url: str) -> Optional[Dict[str, Any]]:
    """Tier 2: dispatch to a site-specific extractor by registrable domain."""
    extractor = _DOMAIN_EXTRACTORS.get(_registrable_domain(url))
    if extractor is None:
        return None
    try:
        result = extractor(html, url)
    except Exception as ex:
        logger.warning("Domain extractor failed for %s: %s", url, ex)
        return None
    if result is None:
        return None
    logger.info(
        "extract_article: domain extractor (%s) produced body for %s",
        _registrable_domain(url),
        url,
    )
    return result


# --- OEM (Organización Editorial Mexicana) Next.js RSC flight-stream ---------- #
# OEM outlets (El Sol de San Juan del Río, Diario de Querétaro, Esto, ...) serve
# Next.js pages whose real article paragraphs live ONLY inside the React Server
# Component flight stream: <script>self.__next_f.push([1,"..."])</script> chunks.
# The visible DOM that NewsPlease/newspaper4k see is only related-article
# sidebars, and JSON-LD carries metadata but no articleBody.
_FLIGHT_CHUNK_RE = re.compile(
    r'self\.__next_f\.push\(\[\s*\d+\s*,\s*"((?:[^"\\]|\\.)*)"\s*\]\)'
)
# A storyline paragraph object carries its own ``publishedAt`` (article date for
# body paragraphs; a different date for the trailing author-bio card).
_STORYLINE_PARA_PUB_RE = re.compile(
    r'"type":"storyline_paragraph","publishedAt":"([^"]+)"'
)
# The paragraph text node itself: ...,"fields":{"paragraph":{"value":"<text>"}}.
_STORYLINE_PARA_VAL_RE = re.compile(
    r'"type":"storyline_paragraph","fields":\{"paragraph":\{"value":"'
    r'((?:[^"\\]|\\.)*)"'
)


def _json_unescape(fragment: str) -> str:
    """Decode a JSON string-body fragment (``\\"`` ``\\n`` ``\\uXXXX`` ...).

    The flight-stream chunks are escaped JSON string fragments of UTF-8 text;
    decoding them as a JSON string yields correct accents (``año``, not the
    ``Ã±`` mojibake produced by a naive ``unicode_escape``).
    """
    return json.loads('"' + fragment + '"')


def _extract_oem(html: str, url: str) -> Optional[Dict[str, Any]]:
    """Extract an OEM article body from the Next.js RSC flight stream.

    Collects every ``storyline_paragraph`` value node across all flight chunks
    (in document order), pairs each with the ``publishedAt`` of its enclosing
    node, keeps only the paragraphs whose date matches the article's (the mode,
    or the JSON-LD ``datePublished`` when present — this drops the trailing
    author-bio card), drops the newsletter CTA and verbatim repeats, then
    reassembles the body. Metadata (title/author/date/image/wordCount) comes
    from JSON-LD. Returns ``None`` on any failure so the cascade continues.
    """
    node = _find_jsonld_article(html)
    article_date = node.get("datePublished") if isinstance(node, dict) else None

    paragraphs: List[tuple] = []  # (publishedAt | None, text)
    for chunk_match in _FLIGHT_CHUNK_RE.finditer(html):
        try:
            decoded = _json_unescape(chunk_match.group(1))
        except Exception:
            continue
        # Walk paragraph markers in positional order; pair each value node with
        # the most recent preceding publishedAt (robust to count mismatches).
        events = []
        for pm in _STORYLINE_PARA_PUB_RE.finditer(decoded):
            events.append((pm.start(), "pub", pm.group(1)))
        for vm in _STORYLINE_PARA_VAL_RE.finditer(decoded):
            events.append((vm.start(), "val", vm.group(1)))
        events.sort(key=lambda e: e[0])
        current_pub: Optional[str] = None
        for _pos, kind, payload in events:
            if kind == "pub":
                current_pub = payload
            else:
                try:
                    text = _json_unescape(payload)
                except Exception:
                    text = payload
                paragraphs.append((current_pub, text))

    if not paragraphs:
        return None

    if not article_date:
        dated = [p for p, _ in paragraphs if p]
        if dated:
            article_date = Counter(dated).most_common(1)[0][0]

    kept: List[str] = []
    seen: set = set()
    for pub, text in paragraphs:
        if article_date and pub and pub != article_date:
            continue  # off-article node (e.g. author-bio card)
        cleaned = text.replace("\xa0", " ").strip()
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if cleaned.startswith("📩") or ("suscríbete" in lowered and "newsletter" in lowered):
            continue  # injected newsletter CTA
        if cleaned in seen:
            continue  # chunks can repeat verbatim
        seen.add(cleaned)
        kept.append(cleaned)

    if not kept:
        return None
    body = "\n\n".join(kept)
    if len(body) < MIN_JSONLD_BODY_LENGTH:
        return None

    # Sanity-check against JSON-LD wordCount when available: reject a fragment
    # that captured only a fraction of the article (heuristic drift).
    word_count = _jsonld_word_count(node) if isinstance(node, dict) else None
    if word_count and word_count > 0:
        extracted_words = len(body.split())
        if extracted_words < 0.3 * word_count:
            logger.warning(
                "OEM extractor got %d words vs JSON-LD wordCount %d for %s — "
                "too thin, falling through",
                extracted_words,
                word_count,
                url,
            )
            return None

    title = ""
    author = None
    media_urls: List[str] = []
    timestamp = article_date
    if isinstance(node, dict):
        headline = node.get("headline") or node.get("name")
        if isinstance(headline, str):
            title = headline
        author = _jsonld_author(node)
        media_urls = _jsonld_image_urls(node)
        timestamp = node.get("datePublished") or article_date

    return {
        "title": title,
        "body": body,
        "author": author,
        "media_urls": media_urls,
        "timestamp": timestamp,
    }


# registrable domain -> deterministic extractor(html, url) -> result | None
_DOMAIN_EXTRACTORS: Dict[str, Callable[[str, str], Optional[Dict[str, Any]]]] = {
    "oem.com.mx": _extract_oem,
}


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
            "timestamp": parsed.get("date_publish"),
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
            "timestamp": article.publish_date,
        }
    except Exception as ex:
        logger.warning("newspaper failed for %s: %s", url, ex)
        return None


def _fill_from_newspaper(result: Dict[str, Any], html: str, url: str) -> None:
    """Fill missing fields in a NewsPlease result using newspaper."""
    fields_to_check = ["title", "body", "author", "timestamp"]
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
        if not _field_ok(result, "timestamp") and article.publish_date:
            result["timestamp"] = article.publish_date
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
                        '  "published_at": (string|null) The article publication date in ISO 8601 format '
                        '(e.g. "2024-03-15T10:30:00" or "2024-03-15"). Look for <meta> tags, JSON-LD, '
                        "or <time> elements. null if not found.\n"
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
                "timestamp": parsed.get("published_at"),
            }
    except Exception as ex:
        logger.warning("LLM parse failed for %s: %s", url, ex)

    return None


def _has_meaningful_content(result: Dict[str, Any]) -> bool:
    """Check if the parsed result has a title and enough body text."""
    if not result:
        return False
    title = result.get("title", "").strip()
    body = result.get("body", "") or ""
    if not title or len(body) < MIN_BODY_LENGTH:
        return False
    if _looks_like_related_posts_list(body):
        return False
    if _looks_like_section_label_list(body):
        return False
    return True


def _looks_like_related_posts_list(body: str) -> bool:
    # When NewsPlease/newspaper4k latch onto a related-posts carousel (e.g.
    # Breakdance/Elementor `bde-loop-item` blocks on expresoqueretaro.com)
    # the extracted text is a list of headlines each followed by "Ver más".
    lowered = body.lower()
    hits = sum(lowered.count(marker) for marker in _RELATED_LINK_MARKERS)
    return hits >= _RELATED_LINK_THRESHOLD


def _looks_like_section_label_list(body: str) -> bool:
    """Detect a related-content list built from repeated section headers.

    Catches the OEM variant that evades the "Ver más" heuristic: the body is a
    stack of "SECTION\\nHeadline\\nDek" triples. Fires when either
    (a) known section-label lines (LOCAL, POLICIACA, CULTURA, ...) appear as
    standalone lines 3+ times, or (b) any short line repeats verbatim 3+ times
    (a nav/label artifact). Both are structural signals — a real article merely
    *mentioning* these words inline (e.g. "la policía local") is not affected,
    because the words are not alone on their own line.
    """
    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
    if not lines:
        return False

    section_line_hits = sum(1 for ln in lines if ln.lower() in _SECTION_LABELS)
    if section_line_hits >= _SECTION_LABEL_THRESHOLD:
        return True

    counts: Dict[str, int] = {}
    for ln in lines:
        if len(ln) <= _REPEATED_LINE_MAX_LEN:
            counts[ln] = counts.get(ln, 0) + 1
    if counts and max(counts.values()) >= _REPEATED_LINE_THRESHOLD:
        return True

    return False


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
