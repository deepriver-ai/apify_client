from __future__ import annotations

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
        assert result["message"]["comments"] == comments

    def test_accepts_empty_list(self):
        result = normalize_record(_base_record(comments=[]), "MessageWrapper")
        assert result["message"]["comments"] == []

    def test_none_normalized(self):
        result = normalize_record(_base_record(comments=None), "MessageWrapper")
        # ListParser should handle None gracefully
        assert result["message"]["comments"] is None or result["message"]["comments"] == []

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
