from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import List, Tuple


# Tags whose entire content (including children) is stripped
_STRIP_TAGS = frozenset({
    "script", "style", "noscript", "svg", "math",
    "iframe", "object", "embed", "applet",
    "head", "header", "footer", "aside", "nav",
})

# Tags to remove (the tag itself) but keep text children
_UNWRAP_TAGS = frozenset({
    "button", "input", "select", "textarea", "form",
    "menu", "menuitem",
})

# Tags that add no value and are removed along with their markup (but children pass through)
_DROP_TAG_ONLY = frozenset({
    "span", "div", "section", "article", "figure",
    "figcaption", "ul", "ol", "li", "dl", "dt", "dd",
    "table", "thead", "tbody", "tfoot", "tr", "td", "th",
    "main", "details", "summary",
})

# Attributes worth preserving (everything else is dropped)
_KEEP_ATTRS = frozenset({
    "href", "src", "alt", "datetime", "content", "name", "property",
})

# Regex for base64 data URIs
_BASE64_RE = re.compile(r'data:[^;]+;base64,[A-Za-z0-9+/=]+', re.IGNORECASE)

# Social share / non-content link patterns
_SOCIAL_SHARE_RE = re.compile(
    r'(api\.whatsapp\.com|facebook\.com/sharer|twitter\.com/share'
    r'|linkedin\.com/shareArticle|^mailto:)',
    re.IGNORECASE,
)

# Placeholder / lazy-load image patterns
_PLACEHOLDER_RE = re.compile(r'placeholder/svg|data:image/svg', re.IGNORECASE)

# Whitespace normalization
_MULTISPACE_RE = re.compile(r'[ \t]+')
_MULTILINE_RE = re.compile(r'\n{3,}')

# Post-processing: remove empty tags like <p></p>, <h2> </h2>, etc.
_EMPTY_TAG_RE = re.compile(r'<(\w+)[^>]*>\s*</\1>')


class _HTMLCleaner(HTMLParser):
    """Single-pass HTML parser that strips non-content elements and attributes."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.pieces: List[str] = []
        self._skip_depth: int = 0
        self._skip_tag_stack: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str | None]]) -> None:
        tag = tag.lower()

        if tag in _STRIP_TAGS:
            self._skip_depth += 1
            self._skip_tag_stack.append(tag)
            return

        if self._skip_depth > 0:
            return

        if tag in _UNWRAP_TAGS:
            return

        # Drop wrapper tags but let children through
        if tag in _DROP_TAG_ONLY:
            return

        attrs_dict = {k.lower(): v for k, v in attrs}

        # Skip social share links
        if tag == "a":
            href = attrs_dict.get("href", "") or ""
            if _SOCIAL_SHARE_RE.search(href):
                return
            if href.startswith("javascript:") or href == "#":
                return

        # Skip placeholder/lazy images and images without src
        if tag == "img":
            src = attrs_dict.get("src", "") or ""
            if not src or _PLACEHOLDER_RE.search(src) or _BASE64_RE.search(src):
                return

        # Build cleaned tag with minimal attributes
        clean_attrs = []
        for name, value in attrs:
            name_l = name.lower()
            if name_l not in _KEEP_ATTRS:
                continue
            if value and _BASE64_RE.search(value):
                continue
            if name_l == "href" and value:
                if _SOCIAL_SHARE_RE.search(value) or value.startswith("javascript:") or value == "#":
                    continue
            if value is not None:
                clean_attrs.append(f'{name}="{value}"')
            else:
                clean_attrs.append(name)

        if clean_attrs:
            self.pieces.append(f'<{tag} {" ".join(clean_attrs)}>')
        else:
            self.pieces.append(f'<{tag}>')

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()

        if self._skip_depth > 0:
            if self._skip_tag_stack and self._skip_tag_stack[-1] == tag:
                self._skip_tag_stack.pop()
                self._skip_depth -= 1
            return

        if tag in _UNWRAP_TAGS or tag in _DROP_TAG_ONLY:
            return

        self.pieces.append(f'</{tag}>')

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        self.pieces.append(data)

    def handle_comment(self, data: str) -> None:
        pass

    def handle_decl(self, decl: str) -> None:
        pass

    def handle_pi(self, data: str) -> None:
        pass

    def get_clean_html(self) -> str:
        result = "".join(self.pieces)
        result = _MULTISPACE_RE.sub(" ", result)
        result = _MULTILINE_RE.sub("\n\n", result)
        # Remove empty tags iteratively (nested empty tags)
        prev = None
        while prev != result:
            prev = result
            result = _EMPTY_TAG_RE.sub("", result)
        result = _MULTILINE_RE.sub("\n\n", result)
        return result.strip()


def clean_html(html: str) -> str:
    """Remove non-content elements from HTML to reduce token count for LLM parsing.

    Strips: script, style, noscript, SVG, iframes, head, header, footer, aside,
    nav, HTML comments, base64 data URIs, social share links, placeholder images,
    empty tags, and non-essential attributes. Unwraps wrapper tags (div, span,
    section, article, li, table cells, etc.) keeping only their text content.

    Preserves: article text, content links (href), real images (src/alt),
    meta tags, and semantic heading structure (h1-h6, p, a, img, meta).
    """
    cleaner = _HTMLCleaner()
    cleaner.feed(html)
    return cleaner.get_clean_html()
