"""Canonical wire serialization for queue messages.

ES rejects space-separated datetimes ("2026-07-20 17:50:17+00:00"); the wire
format for every published document must be ISO-8601 with the "T" separator.
2026-07-21 incident: an ad-hoc publisher used json.dumps(default=str), whose
str(datetime) emits the space form — gp3's engagement merge then failed with
ES 400 and the message redelivered forever, jamming the prod queue. Wire
serialization therefore lives HERE, once; call sites must never hand-roll
json.dumps for published documents.

Kept separate from helpers.rabbitmq because that module opens a connection at
import time; this one is import-safe for tests and offline tools.
"""
from __future__ import annotations

import json
from datetime import date, datetime


def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


def serialize_message(payload) -> str:
    """Canonical JSON serialization for queue messages (ISO-T datetimes)."""
    return json.dumps(payload, ensure_ascii=False, default=_json_default)


def publish_document(doc) -> None:
    """Serialize a Document's final schema canonically and publish it."""
    from src.helpers.rabbitmq import publish

    publish(serialize_message(doc.to_final_schema()))
