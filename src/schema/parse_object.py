from __future__ import annotations

import copy

from typing import Any, Callable, Dict, Optional
from .types import resolve_parser_from_spec, extract_list_object_type

# ------------------------------
# Structure mapping and normalization
# ------------------------------


class Parser:
    def __init__(
        self,
        object_schemas: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> None:
        self.object_schemas = object_schemas

    def _get_field_spec(self, type_name: str, field_name: str) -> Dict[str, Any]:
        schema = self.object_schemas[type_name]
        return schema.get(field_name, {})

    def parse_object_structure(self, obj: Dict[str, Any], type_name: str) -> Dict[str, Any]:
        """
        Parse the object structure with nested objects, add all fields in the schema to each one.
        """
        if not isinstance(obj, dict):
            return {}
        
        schema = self.object_schemas[type_name]
        structured: Dict[str, Any] = {}
        
        for field_name, spec in schema.items():
            
            field_type = spec.get("type")
            raw_value = obj.get(field_name)
            
            if field_type in self.object_schemas:  # nested object type

                nested_from_flat = self.parse_object_structure(obj, field_type)

                if nested_from_flat:
                    structured[field_name] = nested_from_flat

                if isinstance(raw_value, dict):
                    structured[field_name].update(raw_value)

                continue

            element_type = extract_list_object_type(field_type)
            if element_type and element_type in self.object_schemas:
                structured[field_name] = raw_value if isinstance(raw_value, list) else []
                continue

            structured[field_name] = raw_value

        return structured

    def parse_object_types(self, obj: Any, type_name: str, **kwargs) -> Dict[str, Any]:

        if not isinstance(obj, dict):
            return {}
        
        schema = self.object_schemas[type_name]

        parsed: Dict[str, Any] = {}

        for field_name, spec in schema.items():
            #assert field_name != 'minutes_to_sleep'
            field_type = spec.get("type")

            raw_value = obj.get(field_name)

            # Top-level only: do not recurse into nested objects here
            if field_type in self.object_schemas:
                parsed[field_name] = raw_value if isinstance(raw_value, dict) else {}
                continue

            element_type = extract_list_object_type(field_type)
            if element_type and element_type in self.object_schemas:
                parsed[field_name] = raw_value if isinstance(raw_value, list) else []
                continue

            parser = resolve_parser_from_spec(spec)
            
            parsed[field_name] = parser.parse(raw_value, spec) if parser else raw_value

        return parsed

    def _apply_defaults(
        self,
        record: Dict[str, Any],
        type_name: str,
        full_object: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:

        context = context or {}
        if not isinstance(record, dict):
            return {}

        out = copy.deepcopy(record)

        schema = self.object_schemas[type_name]

        # Top-level only: apply defaults for fields on this object, no nested traversal
        for field_name, spec in schema.items():
            
            if out.get(field_name) is None and "default" in spec:
                default_value = spec["default"]
                try:
                    out[field_name] = default_value(full_object, context) if callable(default_value) else default_value
                except Exception:
                    raise

        return out

    def traverse_nested(
        self,
        processor: Callable[[Dict[str, Any], str, Optional[Dict[str, Any]]], Dict[str, Any]],
        obj: Dict[str, Any],
        type_name: str,
        full_object: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Apply a top-level processor at each nested object level recursively and merge results."""
        
        if not isinstance(obj, dict):
            return {}
        
        context = context or {}
        result = processor(obj, type_name, full_object=full_object, context=context)  # type: ignore[arg-type]
        
        if not isinstance(result, dict):
            result = {}
        
        schema = self.object_schemas[type_name]
        for field_name, spec in schema.items():
            field_type = spec.get("type")
            
            if field_type in self.object_schemas:
                child_in = obj.get(field_name) if isinstance(obj.get(field_name), dict) else {}
                child_out = self.traverse_nested(processor, child_in, field_type, full_object, context)

                result[field_name] = child_out
                continue

            element_type = extract_list_object_type(field_type)
            if element_type and element_type in self.object_schemas:
                items = obj.get(field_name) if isinstance(obj.get(field_name), list) else []
                result[field_name] = [
                    self.traverse_nested(
                        processor,
                        item if isinstance(item, dict) else {},
                        element_type, full_object, context,
                    )
                    for item in items
                ]
                continue

        return result

    def _validate(
            self,
            record: Dict[str, Any],
            type_name: str,
            full_object: Optional[Dict[str, Any]] = None,
            context: Optional[Dict[str, Any]] = None) -> None:
        schema = self.object_schemas[type_name]
        context = context or {}
        for field_name, spec in schema.items():
            value = record[field_name]
            parser = resolve_parser_from_spec(spec)
            if parser:
                parser.validate(value, spec, field_name, full_object=full_object, context=context)

    def normalize_record(
        self,
        record: Dict[str, Any],
        type_name: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:

        structured = self.parse_object_structure(record, type_name)
        typed = self.traverse_nested(self.parse_object_types, structured, type_name, structured, context)
        with_defaults = self.traverse_nested(self._apply_defaults, typed, type_name, typed, context)

        self.traverse_nested(self._validate, with_defaults, type_name, with_defaults, context)

        return with_defaults



__all__ = [
    "Parser",
]
