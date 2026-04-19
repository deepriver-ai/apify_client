from __future__ import annotations

import copy
import functools

from typing import Any, Callable, Dict, List, Optional, Type
from ast import literal_eval
from datetime import datetime

from dateutil.parser import parse as parse_datetime_str
from src.helpers.str_fn import _is_valid_url, _is_null

local_tz = datetime.now().astimezone().tzinfo

# Try to import pandas for Timestamp support
try:
    import pandas as pn
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# ------------------------------
# Primitive parsers
# ------------------------------

def parse_int(value: Any) -> Optional[int]:
    if _is_null(value) or value == "":
        return None
    try:
        if isinstance(value, bool):
            return int(value)
        return int(float(str(value).strip()))
    except Exception:
        return None


def parse_float(value: Any) -> Optional[float]:
    if _is_null(value) or value == "":
        return None
    try:
        if isinstance(value, bool):
            return float(int(value))
        return float(str(value).strip())
    except Exception:
        return None


def parse_str(value: Any) -> Optional[str]:
    if _is_null(value):
        return None
    s = str(value).strip()
    return s if s != "" else None


def parse_bool(value: Any) -> Optional[bool]:
    if _is_null(value):
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    return None


def parse_datetime(value: Any) -> Optional[datetime]:
    if _is_null(value) or value == "":
        ret = None

    if isinstance(value, datetime):
        ret = value

    # Handle pandas Timestamp
    if PANDAS_AVAILABLE and isinstance(value, pn.Timestamp):
        ret = value.to_pydatetime()

    if isinstance(value, str):
        s = value.strip()
        if not s:
            ret = None
        try:
            # TODO: if format like "2026-04-14T12:00:00.000Z" parses wrong day-month when dayfirst=True, 
            # if format is "14/04/2026" parses wrong month-day when dayfirst=False
            ret = parse_datetime_str(s, dayfirst=False)
        except Exception:
            try:
                ret = parse_datetime_str(s)
            except Exception:
                ret = None

    if isinstance(ret, datetime):
        if ret.tzinfo is None:
            ret = ret.replace(tzinfo=local_tz)

    return ret


def parse_url_list(value: Any) -> List[str]:
    urls: List[str]
    if _is_null(value):
        urls = []
    elif isinstance(value, list):
        urls = [str(v) for v in value]
    elif isinstance(value, str):
        v = value.strip()
        try:
            parsed = literal_eval(v)
            if isinstance(parsed, list):
                urls = [str(u) for u in parsed]
            else:
                urls = [v]
        except Exception:
            urls = [v]
    else:
        urls = [str(value)]

    cleaned: List[str] = []
    seen: set[str] = set()
    for u in urls:
        u = u.strip()
        if not u:
            continue
        if u not in seen:
            seen.add(u)
            cleaned.append(u)
    return cleaned


class TypeParser:
    def parse(self, value: Any, spec: Optional[Dict[str, Any]] = None) -> Any:
        return value

    def validate(
        self, 
        value: Any, 
        spec: Optional[Dict[str, Any]] = None, 
        field_name: Optional[str] = None,
        full_object: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        if spec:
            required = spec.get("required")

            is_required = bool(required)

            # Support callable required (like default values)
            if callable(required):

                if not required(full_object or {}, context or {}):
                    raise ValueError(f"Missing required field: {field_name or 'field'} by function {spec.get('required')}")
            
            if is_required and _is_null(value):
                raise ValueError(f"Missing required field: {field_name or 'field'}")


class IntParser(TypeParser):
    def parse(self, value: Any, spec: Optional[Dict[str, Any]] = None) -> Optional[int]:
        return parse_int(value)

    def validate(
        self, 
        value: Any, 
        spec: Optional[Dict[str, Any]] = None, 
        field_name: Optional[str] = None,
        full_object: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        super().validate(value, spec, field_name, full_object, context)
        if value is not None and not isinstance(value, int):
            raise ValueError(f"{field_name or 'field'} must be int")


class FloatParser(TypeParser):
    def parse(self, value: Any, spec: Optional[Dict[str, Any]] = None) -> Optional[float]:
        return parse_float(value)

    def validate(
        self, 
        value: Any, 
        spec: Optional[Dict[str, Any]] = None, 
        field_name: Optional[str] = None,
        full_object: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        super().validate(value, spec, field_name, full_object, context)
        if value is not None and not isinstance(value, (int, float)):
            raise ValueError(f"{field_name or 'field'} must be float")


class StrParser(TypeParser):
    def parse(self, value: Any, spec: Optional[Dict[str, Any]] = None) -> Optional[str]:
        return parse_str(value)

    def validate(
        self, 
        value: Any, 
        spec: Optional[Dict[str, Any]] = None, 
        field_name: Optional[str] = None,
        full_object: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        super().validate(value, spec, field_name, full_object, context)
        if value is not None and not isinstance(value, str):
            raise ValueError(f"{field_name or 'field'} must be str")


class BoolParser(TypeParser):
    def parse(self, value: Any, spec: Optional[Dict[str, Any]] = None) -> Optional[bool]:
        return parse_bool(value)

    def validate(
        self, 
        value: Any, 
        spec: Optional[Dict[str, Any]] = None, 
        field_name: Optional[str] = None,
        full_object: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        super().validate(value, spec, field_name, full_object, context)
        if value is not None and not isinstance(value, bool):
            raise ValueError(f"{field_name or 'field'} must be bool")


class DateTimeParser(TypeParser):
    def parse(self, value: Any, spec: Optional[Dict[str, Any]] = None) -> Optional[datetime]:
        return parse_datetime(value)

    def validate(
        self, 
        value: Any, 
        spec: Optional[Dict[str, Any]] = None, 
        field_name: Optional[str] = None,
        full_object: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        super().validate(value, spec, field_name, full_object, context)
        if value is not None and not isinstance(value, datetime):
            raise ValueError(f"{field_name or 'field'} must be datetime")


class ListParser(TypeParser):
    def parse(self, value: Any, spec: Optional[Dict[str, Any]] = None) -> List[Any]:
        if _is_null(value):
            return []
        elif isinstance(value, list):
            return value
        elif isinstance(value, str):
            v = value.strip()
            try:
                parsed = literal_eval(v)
                if isinstance(parsed, list):
                    return parsed
                else:
                    return [v]
            except Exception:
                return [v]
        else:
            return [value]
    
    def validate(
        self, 
        value: Any, 
        spec: Optional[Dict[str, Any]] = None, 
        field_name: Optional[str] = None,
        full_object: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        super().validate(value, spec, field_name, full_object, context)
        
        # List must be a non-empty list if required
        if spec and spec.get("required") and (_is_null(value) or (isinstance(value, list) and len(value) == 0)):
            raise ValueError(f"Missing required field: {field_name or 'field'} (empty list)")
        
        if value is not None:
            if not isinstance(value, list):
                raise ValueError(f"{field_name or 'field'} must be a list")


class Url:  # marker type
    pass


class UrlList:  # marker type
    pass


class EnumStr:  # marker type
    pass


class UrlParser(TypeParser):

    def parse(self, value: Any, spec: Optional[Dict[str, Any]] = None) -> Optional[str]:
        s = parse_str(value)
        if _is_null(s):
            return None
        return s

    def validate(
        self, 
        value: Any, 
        spec: Optional[Dict[str, Any]] = None, 
        field_name: Optional[str] = None,
        full_object: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        super().validate(value, spec, field_name, full_object, context)
        if value is not None:
            if not isinstance(value, str) or not _is_valid_url(value):
                raise ValueError(f"{field_name or 'field'} must be a valid URL")


class UrlListParser(TypeParser):
    def parse(self, value: Any, spec: Optional[Dict[str, Any]] = None) -> List[str]:
        return parse_url_list(value)

    def validate(
        self, 
        value: Any, 
        spec: Optional[Dict[str, Any]] = None, 
        field_name: Optional[str] = None,
        full_object: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        super().validate(value, spec, field_name, full_object, context)
        
        # Url list must be a non-empty list of valid urls if required
        if spec and spec.get("required") and (_is_null(value) or (isinstance(value, list) and len(value) == 0)):
            raise ValueError(f"Missing required field: {field_name or 'field'} (empty list)")

        if value is not None:

            if not isinstance(value, list):
                raise ValueError(f"{field_name or 'field'} must be a list of URLs")

            for u in value:
                if not isinstance(u, str) or not _is_valid_url(u):
                    raise ValueError(f"{field_name or 'field'} contains invalid URL: {u}")


class EnumStrParser(TypeParser):
    def parse(self, value: Any, spec: Optional[Dict[str, Any]] = None) -> Optional[str]:
        return parse_str(value)

    def validate(
        self, 
        value: Any, 
        spec: Optional[Dict[str, Any]] = None, 
        field_name: Optional[str] = None,
        full_object: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        super().validate(value, spec, field_name, full_object, context)
        
        if _is_null(value):
            return
        
        if not isinstance(value, str):
            raise ValueError(f"{field_name or 'field'} must be str")
        
        allowed = (spec or {}).get("enum")
        if allowed is not None and value not in allowed:
            raise ValueError(f"{field_name or 'field'} must be one of {allowed}")


# Registry mapping python types/markers to parser instances
TYPE_PARSER_MAP: Dict[Type[Any], TypeParser] = {
    int: IntParser(),
    float: FloatParser(),
    str: StrParser(),
    bool: BoolParser(),
    datetime: DateTimeParser(),
    list: ListParser(),
    Url: UrlParser(),
    UrlList: UrlListParser(),
    EnumStr: EnumStrParser(),
}


def resolve_parser_from_spec(spec: Dict[str, Any]) -> Optional[TypeParser]:
    
    field_type = spec.get("type")
    
    if isinstance(field_type, type) and field_type in TYPE_PARSER_MAP:
        return TYPE_PARSER_MAP[field_type]
    
    return None
    