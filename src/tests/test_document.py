from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from src.models.document import Document


class TestEmptyData:
    def test_has_comments_field(self):
        data = Document._empty_data()
        assert "comments" in data
        assert data["comments"] == []

    def test_all_expected_fields_present(self):
        data = Document._empty_data()
        expected = {
            "timestamp", "source", "title", "body", "url", "media_urls",
            "type", "author", "article_value", "website_visits", "likes",
            "shares", "views", "n_comments", "profile_url", "post_type",
            "author_location_text", "author_location_id",
            "location_author_formatted_name", "location_author_geoid",
            "location_author_coords", "location_author_precision_level",
            "location_author_level_1", "location_author_level_1_id",
            "location_author_level_2", "location_author_level_2_id",
            "location_author_level_3", "location_author_level_3_id",
            "location_ids",
            "language", "comments",
            "author_full_name", "author_profile_bio",
            "video_filename",
        }
        assert set(data.keys()) == expected


class TestMatchesMinDate:
    def test_no_timestamp_returns_true(self):
        doc = Document({"timestamp": None})
        assert doc.matches_min_date(datetime.now()) is True

    def test_recent_timestamp_passes(self):
        doc = Document({"timestamp": datetime.now().isoformat()})
        assert doc.matches_min_date(datetime.now() - timedelta(days=1)) is True

    def test_old_timestamp_fails(self):
        doc = Document({"timestamp": "2020-01-01T00:00:00"})
        assert doc.matches_min_date(datetime(2025, 1, 1)) is False

    def test_unparseable_timestamp_returns_true(self):
        doc = Document({"timestamp": "not-a-date"})
        assert doc.matches_min_date(datetime.now()) is True


class TestMatchesLocation:
    def test_no_country_id_returns_true(self):
        doc = Document()
        assert doc.matches_location("") is True

    def test_empty_location_ids_no_author_id_returns_true(self):
        doc = Document({"location_ids": [], "author_location_id": None})
        assert doc.matches_location("_484") is True

    def test_location_ids_matching_geoid(self):
        doc = Document({"location_ids": ["_48416053", "_840"]})
        assert doc.matches_location("_484") is True

    def test_location_ids_no_match(self):
        doc = Document({"location_ids": ["_840", "_152"]})
        assert doc.matches_location("_484") is False

    def test_location_ids_non_geoid_only(self):
        doc = Document({"location_ids": ["12345", "67890"]})
        assert doc.matches_location("_484") is True

    def test_location_ids_mix_geoid_and_non_geoid(self):
        doc = Document({"location_ids": ["12345", "_48416053"]})
        assert doc.matches_location("_484") is True

    def test_fallback_to_author_location_id(self):
        doc = Document({"location_ids": [], "author_location_id": "_48416053"})
        assert doc.matches_location("_484") is True

    def test_fallback_author_location_id_no_match(self):
        doc = Document({"location_ids": [], "author_location_id": "_840"})
        assert doc.matches_location("_484") is False

    def test_fallback_non_geoid_author_location_id(self):
        doc = Document({"location_ids": [], "author_location_id": "12345"})
        assert doc.matches_location("_484") is True


def _valid_data(**overrides):
    """Intermediate-schema data that passes NEWS_SCHEMA validation."""
    data = Document._empty_data()
    data.update({
        "body": "Body content long enough for parsing",
        "title": "Title",
        "timestamp": "2026-01-01T00:00:00",
        "source": "TestSource",
        "type": "news",
        "url": "https://example.com/article",
    })
    data.update(overrides)
    return data


class TestToFinalSchema:
    def test_envelope_is_news_and_inner_type_preserved(self):
        doc = Document(_valid_data(type="instagram"))
        result = doc.to_final_schema()
        assert result["type"] == "news"
        assert result["message"]["type"] == "instagram"

    def test_news_type_passes_through(self):
        doc = Document(_valid_data(type="news"))
        result = doc.to_final_schema()
        assert result["message"]["type"] == "news"

    def test_all_platform_types_accepted(self):
        for platform in ("news", "x", "facebook", "instagram", "linkedin", "radio", "tv"):
            doc = Document(_valid_data(type=platform))
            result = doc.to_final_schema()
            assert result["message"]["type"] == platform

    def test_impreso_allows_missing_url(self):
        data = _valid_data(type="impreso")
        data["url"] = None
        doc = Document(data)
        result = doc.to_final_schema()
        assert result["message"]["type"] == "impreso"

    def test_non_impreso_requires_url(self):
        data = _valid_data(type="news")
        data["url"] = None
        doc = Document(data)
        with pytest.raises(ValueError, match="url"):
            doc.to_final_schema()
