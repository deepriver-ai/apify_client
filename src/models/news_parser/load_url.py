from __future__ import annotations

import logging
import random
import time
from typing import Optional

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

REQUESTS_TIMEOUT = 15
MAX_RESP_SIZE = 10 * 1024 * 1024  # 10 MB


def fetch_html(url: str, max_retries: int = 3) -> tuple[Optional[str], Optional[str]]:
    """Download a URL and return (html, final_url), or (None, None) on failure.

    ``final_url`` is the URL after following any redirects, which may differ
    from the input when the original URL is shortened or redirected.
    """
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=_random_headers(), timeout=REQUESTS_TIMEOUT, stream=True)
            r.raise_for_status()

            if int(r.headers.get("Content-Length", 0)) > MAX_RESP_SIZE:
                logger.warning("Response too large for %s", url)
                return None, None

            body = b""
            size = 0
            start = time.time()
            for chunk in r.iter_content(1024):
                if time.time() - start > REQUESTS_TIMEOUT:
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
