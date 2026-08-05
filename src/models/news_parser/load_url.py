from __future__ import annotations

import logging
import random
import re
import time
from typing import Optional
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

_HEADER_OPTIONS = [
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/17.4 Safari/605.1.15"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-419,es;q=0.9,en-US;q=0.8,en;q=0.7",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) "
            "Gecko/20100101 Firefox/128.0"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "es-MX,es;q=0.8,en-US;q=0.5,en;q=0.3",
    },
]


def _random_headers() -> dict:
    return random.choice(_HEADER_OPTIONS)

# Connecting to a live host is fast; a dead/unroutable host should fail quickly
# rather than tie up an attempt. So we keep the *connect* timeout short and make
# the *read* timeout generous — the read timeout is only an upper bound on how
# long we wait between bytes, it does NOT slow down fast servers (they still
# return in their natural time). Some legitimate origins behind slow CDNs
# (e.g. oem.com.mx) have a time-to-first-byte of ~80s: a browser gets the page,
# but the previous flat 15s cap killed attempts 1-2 and only a lucky retry
# succeeded, costing ~2 min for a page that returns valid HTML in one attempt.
#
# The read timeout escalates per attempt so a transient stall gets a longer
# grace period on retry, while the first attempt is already generous enough to
# cover known slow-but-succeeding origins in a single request.
CONNECT_TIMEOUT = 10
READ_TIMEOUTS = (90, 120, 150)
# Kept for backwards compatibility / external references.
REQUESTS_TIMEOUT = READ_TIMEOUTS[0]
MAX_RESP_SIZE = 10 * 1024 * 1024  # 10 MB

# --- Client-side (JS / meta-refresh) redirect interstitials ------------------ #
# Some shorteners answer HTTP 200 with a tiny HTML stub that redirects from the
# browser instead of sending a 3xx, so ``requests`` has nothing to follow.
# X's t.co is the case that matters here — a tweet's article link arrives as
# https://t.co/XXXX and returns ~400 bytes of:
#     <head><noscript><META http-equiv="refresh" content="0;URL=https://real/..."></noscript>
#     <script>window.opener = null; location.replace("https:\/\/real\/...")</script></head>
# Without this, the attached-article fetch keeps the t.co URL as the document's
# URL (i.e. its ES _id) and hands a contentless stub down the parse cascade.
#
# Only *small* responses are inspected: a real article page that happens to
# contain a meta-refresh (cookie walls, "this page has moved" banners appended
# to full content) must not be redirected away from its own content.
MAX_INTERSTITIAL_SIZE = 4096
MAX_CLIENT_REDIRECTS = 3

_META_REFRESH_RE = re.compile(
    r"""<meta[^>]+http-equiv\s*=\s*['"]?refresh['"]?[^>]*"""
    r"""content\s*=\s*['"]\s*\d+\s*;\s*url\s*=\s*([^'"]+)['"]""",
    re.IGNORECASE,
)
_JS_REDIRECT_RE = re.compile(
    r"""(?:location\.replace|location\.assign)\s*\(\s*['"]([^'"]+)['"]"""
    r"""|(?:window\.)?location(?:\.href)?\s*=\s*['"]([^'"]+)['"]""",
    re.IGNORECASE,
)


def _extract_client_redirect(html: str, base_url: str) -> Optional[str]:
    """Return the target of a JS/meta-refresh interstitial, or None.

    Only fires for responses small enough to be an interstitial rather than a
    real page. JS string escapes (``https:\\/\\/host``) are unescaped and
    relative targets are resolved against ``base_url``.
    """
    if not html or len(html) > MAX_INTERSTITIAL_SIZE:
        return None

    match = _META_REFRESH_RE.search(html)
    target = match.group(1) if match else None
    if target is None:
        match = _JS_REDIRECT_RE.search(html)
        if match:
            target = match.group(1) or match.group(2)
    if not target:
        return None

    target = target.strip().replace("\\/", "/").replace("&amp;", "&")
    resolved = urljoin(base_url, target)
    if not resolved.lower().startswith(("http://", "https://")):
        return None
    return resolved


def fetch_html(url: str, max_retries: int = 3) -> tuple[Optional[str], Optional[str]]:
    """Download a URL and return (html, final_url), or (None, None) on failure.

    ``final_url`` is the URL after following any redirects — both HTTP 3xx
    (handled by ``requests``) and client-side JS/meta-refresh interstitials
    (handled here, see ``_extract_client_redirect``) — so it may differ from the
    input when the original URL is shortened or redirected.
    """
    seen = {url}
    html, final_url = _fetch_html_once(url, max_retries)
    for _ in range(MAX_CLIENT_REDIRECTS):
        if html is None or final_url is None:
            break
        target = _extract_client_redirect(html, final_url)
        if target is None or target in seen:
            break
        logger.info("Client-side redirect: %s -> %s", final_url, target)
        seen.add(target)
        html, final_url = _fetch_html_once(target, max_retries)
    return html, final_url


def _fetch_html_once(url: str, max_retries: int = 3) -> tuple[Optional[str], Optional[str]]:
    """Download a URL following HTTP 3xx only. See ``fetch_html``."""
    for attempt in range(max_retries):
        read_timeout = READ_TIMEOUTS[min(attempt, len(READ_TIMEOUTS) - 1)]
        try:
            r = requests.get(
                url,
                headers=_random_headers(),
                timeout=(CONNECT_TIMEOUT, read_timeout),
                stream=True,
            )
            r.raise_for_status()

            if int(r.headers.get("Content-Length", 0)) > MAX_RESP_SIZE:
                logger.warning("Response too large for %s", url)
                return None, None

            body = b""
            size = 0
            start = time.time()
            for chunk in r.iter_content(1024):
                # Cap total streaming time by the same per-attempt read budget so
                # a server that trickles bytes forever can't hang the fetch.
                if time.time() - start > read_timeout:
                    raise ValueError("Streaming timeout reached")
                size += len(chunk)
                if size > MAX_RESP_SIZE:
                    logger.warning("Response exceeded size limit for %s", url)
                    return None, None
                body += chunk

            encoding = r.encoding if r.encoding else "utf-8"
            return body.decode(encoding, errors="replace"), r.url

        except Exception as ex:
            logger.warning("Attempt %d failed for %s: %s", attempt + 1, url, ex)
            if attempt >= max_retries - 1:
                logger.error("All retries exhausted for %s", url)
                return None, None
        time.sleep(random.randint(1, 10))
    return None, None
