"""Language code normalization.

Maps various language representations to ISO 639-1 two-letter codes.
"""

from __future__ import annotations

# Common language names → ISO 639-1
_NAME_TO_CODE: dict[str, str] = {
    "spanish": "es",
    "español": "es",
    "english": "en",
    "french": "fr",
    "français": "fr",
    "portuguese": "pt",
    "português": "pt",
    "german": "de",
    "deutsch": "de",
    "italian": "it",
    "italiano": "it",
    "japanese": "ja",
    "chinese": "zh",
    "korean": "ko",
    "arabic": "ar",
    "russian": "ru",
    "dutch": "nl",
    "swedish": "sv",
    "norwegian": "no",
    "danish": "da",
    "finnish": "fi",
    "polish": "pl",
    "turkish": "tr",
    "hebrew": "he",
    "hindi": "hi",
    "thai": "th",
    "vietnamese": "vi",
    "indonesian": "id",
    "malay": "ms",
    "catalan": "ca",
    "czech": "cs",
    "greek": "el",
    "hungarian": "hu",
    "romanian": "ro",
    "ukrainian": "uk",
}


def normalize_language(raw: str | None) -> str | None:
    """Normalize a language identifier to ISO 639-1 (two-letter code).

    Handles:
        - Already ISO 639-1: ``"es"`` → ``"es"``
        - Full names: ``"spanish"`` → ``"es"``
        - BCP 47: ``"es-MX"``, ``"es-419"`` → ``"es"``
        - Google News region format: ``"MX:es-419"`` → ``"es"``

    Returns None if input is empty or unrecognizable.
    """
    if not raw:
        return None
    raw = raw.strip().lower()
    if not raw:
        return None

    # Already ISO 639-1
    if len(raw) == 2 and raw.isalpha():
        return raw

    # Full name lookup
    if raw in _NAME_TO_CODE:
        return _NAME_TO_CODE[raw]

    # Google News region format: "MX:es-419" → take lang part after ":"
    if ":" in raw:
        lang_part = raw.split(":", 1)[1]
        return normalize_language(lang_part)

    # BCP 47: "es-MX", "es-419" → take prefix before "-"
    if "-" in raw:
        prefix = raw.split("-", 1)[0]
        if len(prefix) == 2 and prefix.isalpha():
            return prefix

    # Underscore variant: "es_MX" → take prefix before "_"
    if "_" in raw:
        prefix = raw.split("_", 1)[0]
        if len(prefix) == 2 and prefix.isalpha():
            return prefix

    return None
