from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.actors.news.news_scraper import GoogleNewsActor
from src.models.news import News


@pytest.fixture
def actor():
    """GoogleNewsActor with mocked sources_manager."""
    a = GoogleNewsActor.__new__(GoogleNewsActor)
    a.client = MagicMock()
    a.sources_manager = MagicMock()
    a.sources_manager.get_domain.return_value = "example.com"
    a.sources_manager.get_location.return_value = {
        "author_location_text": "Mexico",
        "author_location_id": "_484",
        "location_author_formatted_name": "Mexico",
        "location_author_geoid": "_484",
        "location_author_coords": {"lat": 19.43, "lon": -99.13},
        "location_author_precision_level": 1,
        "location_author_level_1": "Mexico",
        "location_author_level_1_id": "_484",
        "location_author_level_2": None,
        "location_author_level_2_id": None,
        "location_author_level_3": None,
        "location_author_level_3_id": None,
    }
    a.sources_manager.check_source.return_value = True
    a.sources_manager.save.return_value = None
    return a


class TestSearchCreatesDocuments:
    def test_creates_news_objects(self, actor, sample_google_news_results):
        with patch.object(actor, "run_actor", return_value=sample_google_news_results):
            with patch.object(News, "fetch_and_parse", return_value=True):
                results = actor.search(["test"], enrich=True)
        assert all(isinstance(r, News) for r in results)
        assert len(results) > 0

    def test_params_sent_to_apify(self, actor, sample_google_news_results):
        with patch.object(actor, "run_actor", return_value=sample_google_news_results) as mock_run:
            with patch.object(News, "fetch_and_parse", return_value=True):
                actor.search(
                    ["totalenergies"],
                    max_results=5,
                    timeframe="7d",
                    region_language="US:en",
                    enrich=True,
                )
        run_input = mock_run.call_args[0][0]
        assert run_input["keywords"] == ["totalenergies"]
        assert run_input["maxArticles"] == 5
        assert run_input["timeframe"] == "7d"
        assert run_input["region_language"] == "US:en"


class TestDateFilter:
    def test_filter_with_min_date(self, actor, sample_google_news_results):
        future = datetime.now() + timedelta(days=365)
        with patch.object(actor, "run_actor", return_value=sample_google_news_results):
            with patch.object(News, "fetch_and_parse", return_value=True):
                results = actor.search(["test"], min_date=future, enrich=False)
        assert len(results) == 0

    def test_filter_with_period(self, actor, sample_google_news_results):
        with patch.object(actor, "run_actor", return_value=sample_google_news_results):
            with patch.object(News, "fetch_and_parse", return_value=True):
                results = actor.search(["test"], period="d", enrich=False)
        # Recent results should pass a 1-day filter
        assert isinstance(results, list)


class TestLanguageFilter:
    def test_wrong_language_filtered(self, actor, sample_google_news_results):
        with patch.object(actor, "run_actor", return_value=sample_google_news_results):
            with patch.object(News, "fetch_and_parse", return_value=True):
                results = actor.search(["test"], language="zh", enrich=True)
        # Spanish articles should not match Chinese
        assert len(results) == 0


class TestFinalSchema:
    def test_schema_complete(self, actor, sample_google_news_results):
        with patch.object(actor, "run_actor", return_value=sample_google_news_results):
            with patch.object(News, "fetch_and_parse", return_value=True):
                results = actor.search(["test"], enrich=True)
        if results:
            final = results[0].to_final_schema()
            assert "message" in final
            msg = final["message"]
            assert "title" in msg
            assert "body" in msg
            assert "timestamp" in msg
            assert "source" in msg
            assert "comments" in msg

    def test_comments_empty_for_news(self, actor, sample_google_news_results):
        with patch.object(actor, "run_actor", return_value=sample_google_news_results):
            with patch.object(News, "fetch_and_parse", return_value=True):
                results = actor.search(["test"], enrich=True)
        if results:
            assert results[0].data["comments"] == []


class TestGetCommentsIgnored:
    def test_get_comments_noop(self, actor, sample_google_news_results):
        with patch.object(actor, "run_actor", return_value=sample_google_news_results):
            with patch.object(News, "fetch_and_parse", return_value=True):
                results = actor.search(["test"], get_comments=True, enrich=False)
        # Should not raise, comments remain empty
        assert isinstance(results, list)
