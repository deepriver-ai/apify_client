from .schemas.source import (
    SOURCE_SCHEMA,
    SOURCE_STATS_SCHEMA,
    LOCATION_COORDS_SCHEMA
)
from .schemas.news import (
    NEWS_SCHEMA,
    COMMENT_SCHEMA,
    SOURCE_EXTRA_SCHEMA,
    SOURCE_EXTRA_STATS_SCHEMA,
    SUPPLIER_SCHEMA,
    MESSAGE_WRAPPER_SCHEMA
)
from .parse_object import Parser
from .types import (
    IntParser,
    FloatParser,
    StrParser,
    BoolParser,
    DateTimeParser,
    UrlParser,
    UrlListParser,
    EnumStrParser,
    TYPE_PARSER_MAP,
    resolve_parser_from_spec
)

# Unified schema exports
SCHEMA = {
    "Source": SOURCE_SCHEMA,
    "SourceStats": SOURCE_STATS_SCHEMA,
    "LocationCoords": LOCATION_COORDS_SCHEMA,
    "News": NEWS_SCHEMA,
    "Comment": COMMENT_SCHEMA,
    "SourceExtra": SOURCE_EXTRA_SCHEMA,
    "SourceExtraStats": SOURCE_EXTRA_STATS_SCHEMA,
    "Supplier": SUPPLIER_SCHEMA,
    "MessageWrapper": MESSAGE_WRAPPER_SCHEMA
}

# Convenience function for normalizing records
def normalize_record(record: dict, type_name: str = "Source", context: dict = None) -> dict:
    """
    Normalize a record using the schema system.
    
    Args:
        record: Raw data dictionary to normalize
        type_name: Type name to use for normalization (default: "Source")
        context: Optional context for default functions
        
    Returns:
        Normalized record dictionary
    """
    parser = Parser(SCHEMA)
    return parser.normalize_record(record, type_name, context)


__all__ = [
    "SCHEMA",
    "SOURCE_SCHEMA",
    "SOURCE_STATS_SCHEMA",
    "NEWS_SCHEMA",
    "COMMENT_SCHEMA",
    "SOURCE_EXTRA_SCHEMA",
    "SOURCE_EXTRA_STATS_SCHEMA",
    "SUPPLIER_SCHEMA",
    "MESSAGE_WRAPPER_SCHEMA",
    "Parser",
    "normalize_record"
]
