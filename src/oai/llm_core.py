"""
LLM Core - Caching and OpenRouter client infrastructure.

This module handles:
- OpenRouter client configuration (via src.oai.openrouter)
- Caching logic (in-memory and file-based)
- Retry logic for API calls
- Generic cached LLM call wrapper

The public API (``llm_cached_call``, ``parse_json_response``,
``get_text_content``) is unchanged from the previous OpenAI-backed
implementation, so existing call-sites do not need to be updated.
"""

import json
import os
import time
from typing import Any, Callable, Dict, Iterable, Optional

import requests

from src.oai.openrouter import call_openrouter


# In-memory cache
tagged: Dict[Any, Dict[str, Any]] = dict()

# File-based cache path
cache_path = os.getenv('LLM_CACHE_PATH', 'cache/llm_core')


def _cache_file(cache_id: Any) -> str:
    """Generate the file path for a cache entry."""
    return os.path.join(cache_path, str(hash(cache_id)))


def _load_cached(cache_id: Any, cache_field: str) -> Optional[Any]:
    """Load a cached value from memory or disk."""
    entry = tagged.get(cache_id)
    if entry and cache_field in entry:
        return entry[cache_field]

    cache_file = _cache_file(cache_id)
    if not os.path.exists(cache_file):
        return None

    try:
        with open(cache_file, 'r') as f:
            cached = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None

    tagged[cache_id] = cached
    return cached.get(cache_field)


def _persist_cache(cache_id: Any, payload: Dict[str, Any]) -> None:
    """Persist a cache entry to memory and disk."""
    tagged[cache_id] = payload
    os.makedirs(cache_path, exist_ok=True)
    cache_file = _cache_file(cache_id)
    with open(cache_file, 'w') as f:
        json.dump(payload, f)


def _call_llm_with_retry(messages: Iterable[Dict[str, Any]], max_retries: int = 3) -> str:
    """Call OpenRouter with automatic retry on transient errors.

    Returns the response content string. Raises on persistent failure.
    """
    retries = 0
    while True:
        try:
            return call_openrouter(list(messages))
        except (requests.HTTPError, requests.RequestException, ValueError) as ex:
            print('sleeping', ex)
            retries += 1
            if retries > max_retries - 1:
                raise
            time.sleep(15)


def llm_cached_call(
    cache_tag: str,
    request_id: Any,
    *,
    cache_field: str,
    messages_builder: Callable[[], Iterable[Dict[str, Any]]],
    parse_fn: Callable[[Any], Any],
    override: bool = False,
    fallback_fn: Optional[Callable[[Any], Any]] = None,
    on_parse_error: Optional[Callable[[Any, Exception], None]] = None,
) -> Any:
    """Execute an LLM call with caching support.

    Args:
        cache_tag: Category/type tag for the cache (e.g., 'sentiment', 'summarize')
        request_id: Unique identifier for this specific request
        cache_field: Field name to store/retrieve the result in cache
        messages_builder: Function that returns the messages to send to the LLM
        parse_fn: Function to parse the LLM response into the desired format
        override: If True, bypass cache and force a new API call
        fallback_fn: Optional function to generate fallback value on parse error
        on_parse_error: Optional callback for parse errors

    Returns:
        Parsed response from the LLM (or cached value). The raw LLM response
        is a plain content string (OpenRouter), so ``parse_fn`` / ``fallback_fn``
        receive that string directly.
    """
    cache_id = (cache_tag, request_id)

    if not override:
        cached_value = _load_cached(cache_id, cache_field)
        if cached_value is not None:
            return cached_value

    if not override and cache_id in tagged and cache_field in tagged[cache_id]:
        return tagged[cache_id][cache_field]

    messages = messages_builder()
    response = _call_llm_with_retry(messages)

    try:
        value = parse_fn(response)
    except Exception as ex:
        if on_parse_error:
            on_parse_error(cache_id, ex)
        else:
            print('exception parsing', cache_id, ex)
        value = fallback_fn(response) if fallback_fn else response

    payload = {
        cache_field: value,
        'id': cache_id,
    }
    _persist_cache(cache_id, payload)

    return value


def parse_json_response(content: str) -> Any:
    """Parse a JSON response from the LLM, handling common formatting issues."""
    return json.loads(content.replace('`', '').replace('json', ''))


def get_text_content(response) -> str:
    """Extract text content from an LLM response.

    With the OpenRouter client, ``_call_llm_with_retry`` already returns a
    plain content string, so this is effectively a pass-through. The
    OpenAI-style fallback is kept for backwards compatibility with any
    cached or in-flight call-sites still passing a response object.
    """
    if isinstance(response, str):
        return response
    return response.choices[0].message.content
