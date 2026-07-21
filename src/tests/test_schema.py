from __future__ import annotations

from datetime import datetime

from src.schema import normalize_record


def _base_record(**overrides):
    """Minimal valid record for News normalization."""
    record = {
        "body": "Test body content that is long enough",
        "title": "Test title",
        "timestamp": "2026-01-01T00:00:00",
        "source": "TestSource",
        "type": "news",
        "url": "https://example.com/article",
        "comments": [],
    }
    record.update(overrides)
    return record


class TestNewsSchemaComments:
    def test_accepts_comments_list(self):
        comments = [
            {
                "comment_text": "Great article!",
                "comment_author": "user1",
                "comment_timestamp": "2026-01-01T01:00:00",
                "comment_likes": 5,
            }
        ]
        result = normalize_record(_base_record(comments=comments), "News")
        parsed = result["comments"]
        assert len(parsed) == 1
        assert parsed[0]["comment_text"] == "Great article!"
        assert parsed[0]["comment_author"] == "user1"
        assert isinstance(parsed[0]["comment_timestamp"], datetime)
        assert parsed[0]["comment_likes"] == 5

    def test_coerces_facebook_shape(self):
        # Real shape from scrapeforge/facebook-search-posts: likes as a string.
        comments = [
            {
                "comment_text": "Por cuál de las dos puertas será el acceso?",
                "comment_author": "Mia G Aguillon",
                "comment_timestamp": "2026-04-08T01:13:11.000Z",
                "comment_likes": "1",
            }
        ]
        result = normalize_record(_base_record(comments=comments), "News")
        item = result["comments"][0]
        assert isinstance(item["comment_timestamp"], datetime)
        assert item["comment_likes"] == 1

    def test_comments_missing_fields_filled(self):
        result = normalize_record(
            _base_record(comments=[{"comment_text": "x"}]),
            "News",
        )
        item = result["comments"][0]
        assert item["comment_text"] == "x"
        assert item["comment_author"] is None
        assert item["comment_timestamp"] is None
        assert item["comment_likes"] is None

    def test_accepts_empty_list(self):
        result = normalize_record(_base_record(comments=[]), "News")
        assert result["comments"] == []

    def test_none_normalized(self):
        result = normalize_record(_base_record(comments=None), "News")
        assert result["comments"] == []

    def test_all_expected_fields_in_message(self):
        result = normalize_record(_base_record(), "News")
        assert "body" in result
        assert "title" in result
        assert "timestamp" in result
        assert "source" in result
        assert "comments" in result
        assert "url" in result
        assert "media_urls" in result


def test_serialize_message_emits_iso_t_datetimes():
    """Wire contract: datetimes serialize ISO-8601 with the T separator —
    the space form (str(datetime)) breaks ES date parsing downstream and
    poison-loops gp3's engagement merge (2026-07-21 incident)."""
    import json
    from datetime import datetime, timezone
    from src.helpers.serialization import serialize_message

    payload = {"message": {
        "timestamp": datetime(2026, 7, 20, 17, 50, 17, tzinfo=timezone.utc),
        "comments": [{"comment_timestamp": datetime(2026, 7, 21, 4, 22, 6, tzinfo=timezone.utc)}],
    }}
    wire = serialize_message(payload)
    data = json.loads(wire)
    assert data["message"]["timestamp"] == "2026-07-20T17:50:17+00:00"
    assert data["message"]["comments"][0]["comment_timestamp"] == "2026-07-21T04:22:06+00:00"
    assert " 17:50:17" not in wire and " 04:22:06" not in wire


def test_publish_document_uses_canonical_serializer():
    from unittest.mock import MagicMock, patch
    from datetime import datetime, timezone
    import sys
    from src.helpers import serialization as sz

    doc = MagicMock()
    doc.to_final_schema.return_value = {"type": "news", "message": {
        "timestamp": datetime(2026, 7, 20, 23, 17, 44, tzinfo=timezone.utc)}}
    # helpers.rabbitmq connects at import — stub the module (import-safety is
    # the reason publish_document lives in serialization.py)
    fake_rb = MagicMock()
    with patch.dict(sys.modules, {"src.helpers.rabbitmq": fake_rb}):
        sz.publish_document(doc)
    body = fake_rb.publish.call_args[0][0]
    assert "2026-07-20T23:17:44+00:00" in body
