"""
LLM Core - Caching and OpenAI client infrastructure.

This module handles:
- OpenAI client configuration
- Caching logic (in-memory and file-based)
- Retry logic for API calls
- Generic cached LLM call wrapper
"""

import json
import os
import time
from typing import Any, Callable, Dict, Iterable, Optional

from openai import OpenAI


# OpenAI Configuration
OPENAI_PROJECT_ID = os.getenv('OPENAI_PROJECT_ID', 'proj_hG3dijzH50mvRZDFs6aASmqS')
OPENAI_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_ORGANIZATION = os.getenv('OPENAI_ORGANIZATION', 'org-OSGYrp5SnEAis7CDgoxEmNiu')
OPENAI_MODEL = 'gpt-4o'
OPENAI_TEMPERATURE = float(os.getenv('OPENAI_TEMPERATURE', 0.3))

# Initialize OpenAI client
client = OpenAI(
    api_key=OPENAI_KEY,
    organization=OPENAI_ORGANIZATION,
)

# In-memory cache
tagged: Dict[Any, Dict[str, Any]] = dict()

# File-based cache path
cache_path = '/Users/oscarcuellar/ocn/media/reports/event_report/cache'


def _cache_file(cache_id: Any) -> str:
    """Generate the file path for a cache entry."""
    return os.path.join(cache_path, str(hash(cache_id)))


def _load_cached(cache_id: Any, cache_field: str) -> Optional[Any]:
    """
    Load a cached value from memory or disk.
    
    Args:
        cache_id: Unique identifier for the cache entry
        cache_field: Field name within the cache entry
    
    Returns:
        Cached value if found, None otherwise
    """
    # Check in-memory cache first
    entry = tagged.get(cache_id)
    if entry and cache_field in entry:
        return entry[cache_field]

    # Try loading from disk
    cache_file = _cache_file(cache_id)
    if not os.path.exists(cache_file):
        return None

    try:
        with open(cache_file, 'r') as f:
            cached = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None

    # Store in memory for faster subsequent access
    tagged[cache_id] = cached
    return cached.get(cache_field)


def _persist_cache(cache_id: Any, payload: Dict[str, Any]) -> None:
    """
    Persist a cache entry to memory and disk.
    
    Args:
        cache_id: Unique identifier for the cache entry
        payload: Data to cache
    """
    tagged[cache_id] = payload
    cache_file = _cache_file(cache_id)
    with open(cache_file, 'w') as f:
        json.dump(payload, f)


def _call_llm_with_retry(messages: Iterable[Dict[str, Any]], max_retries: int = 3):
    """
    Call the OpenAI API with automatic retry on failure.
    
    Args:
        messages: List of message dicts for the chat completion
        max_retries: Maximum number of retry attempts
    
    Returns:
        OpenAI chat completion response
    
    Raises:
        Exception: If all retries fail
    """
    retries = 0
    while True:
        try:
            return client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=list(messages),
                temperature=OPENAI_TEMPERATURE,
            )
        except Exception as ex:
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
    """
    Execute an LLM call with caching support.
    
    This is the main wrapper for all LLM calls. It handles:
    - Checking cache before making API calls
    - Building messages using the provided builder function
    - Parsing responses using the provided parser
    - Falling back to default parsing on errors
    - Persisting results to cache
    
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
        Parsed response from the LLM (or cached value)
    
    Example:
        def build_messages():
            return [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Hello!"}
            ]
        
        result = llm_cached_call(
            cache_tag='greeting',
            request_id='hello_001',
            cache_field='response',
            messages_builder=build_messages,
            parse_fn=lambda r: r.choices[0].message.content,
        )
    """
    cache_id = (cache_tag, request_id)

    # Check cache first (unless override is True)
    if not override:
        cached_value = _load_cached(cache_id, cache_field)
        if cached_value is not None:
            return cached_value

    # Double-check in-memory cache
    if not override and cache_id in tagged and cache_field in tagged[cache_id]:
        return tagged[cache_id][cache_field]

    # Build messages and call LLM
    messages = messages_builder()
    response = _call_llm_with_retry(messages)

    # Parse the response
    try:
        value = parse_fn(response)
    except Exception as ex:
        if on_parse_error:
            on_parse_error(cache_id, ex)
        else:
            print('exception parsing', cache_id, ex)
        # Use fallback if provided, otherwise return raw content
        fallback = fallback_fn(response) if fallback_fn else response.choices[0].message.content
        value = fallback

    # Cache the result
    payload = {
        cache_field: value,
        'id': cache_id
    }
    _persist_cache(cache_id, payload)
    
    return value


def parse_json_response(content: str) -> Any:
    """
    Parse a JSON response from the LLM, handling common formatting issues.
    
    Args:
        content: Raw response content from the LLM
    
    Returns:
        Parsed JSON object
    """
    return json.loads(content.replace('`', '').replace('json', ''))


def get_text_content(response) -> str:
    """
    Extract text content from an OpenAI response.
    
    Args:
        response: OpenAI chat completion response
    
    Returns:
        Text content from the first choice
    """
    return response.choices[0].message.content

