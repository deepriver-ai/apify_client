from __future__ import annotations

from datetime import datetime

from src.schema import normalize_record


def _base_record(**overrides):
    """Minimal valid record for MessageWrapper normalization."""
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
        result = normalize_record(_base_record(comments=comments), "MessageWrapper")
        parsed = result["message"]["comments"]
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
        result = normalize_record(_base_record(comments=comments), "MessageWrapper")
        item = result["message"]["comments"][0]
        assert isinstance(item["comment_timestamp"], datetime)
        assert item["comment_likes"] == 1

    def test_comments_missing_fields_filled(self):
        result = normalize_record(
            _base_record(comments=[{"comment_text": "x"}]),
            "MessageWrapper",
        )
        item = result["message"]["comments"][0]
        assert item["comment_text"] == "x"
        assert item["comment_author"] is None
        assert item["comment_timestamp"] is None
        assert item["comment_likes"] is None

    def test_accepts_empty_list(self):
        result = normalize_record(_base_record(comments=[]), "MessageWrapper")
        assert result["message"]["comments"] == []

    def test_none_normalized(self):
        result = normalize_record(_base_record(comments=None), "MessageWrapper")
        assert result["message"]["comments"] == []

    def test_all_expected_fields_in_message(self):
        result = normalize_record(_base_record(), "MessageWrapper")
        msg = result["message"]
        assert "body" in msg
        assert "title" in msg
        assert "timestamp" in msg
        assert "source" in msg
        assert "comments" in msg
        assert "url" in msg
        assert "media_urls" in msg
