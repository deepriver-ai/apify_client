from tldextract import TLDExtract
from typing import Any
# Try to import numpy for NaN detection
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False


domainsplitter = TLDExtract(suffix_list_urls=())  # Initialize without call to update cache. See https://pypi.org/project/tldextract/


def _is_null(value: Any) -> bool:
    """Check if value is None or np.nan"""
    if value is None:
        return True
    if not NUMPY_AVAILABLE:
        return str(value) == "nan"
    try:
        return bool(np.isnan(value))
    except Exception:
        return False


def get_domain(url, full=True):
    if _is_null(url):
        return None

    extracted = domainsplitter(url)
    domain = extracted.top_domain_under_public_suffix
    
    if full:
        return domain
    else:
        # Return only the subdomain (e.g., "reforma" from "reforma.com")
        if domain and '.' in domain:
            return '.'.join(domain.split('.')[:-1])
        return domain


def _is_valid_url(url: str) -> bool:
    import re
    
    if not url or not isinstance(url, str):
        return False
    
    # Strip whitespace
    url = url.strip()
    
    # Regex pattern for validating URLs
    # Accepts:
    # - Optional protocol (http:// or https://)
    # - Optional www.
    # - Domain with at least one dot (e.g., domain.com)
    # - Optional port
    # - Optional path, query params, and fragments
    pattern = r'^(?:(?:https?://)?(?:www\.)?)?([a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}(?::\d{1,5})?(?:/[^\s]*)?$'
    
    if not re.match(pattern, url):
        return False
    
    # Additional check: ensure there's a proper domain (at least one dot)
    # Extract the domain part (before any path)
    domain_part = url.split('://')[1] if '://' in url else url
    domain_part = domain_part.split('/')[0]  # Get part before path
    domain_part = domain_part.split(':')[0]  # Remove port if present
    
    # Must have at least one dot for a valid domain
    if '.' not in domain_part:
        return False
    
    # Check that the domain doesn't start or end with dots/hyphens
    domain_parts = domain_part.split('.')
    for part in domain_parts:
        if not part or part.startswith('-') or part.endswith('-'):
            return False
    
    return True


