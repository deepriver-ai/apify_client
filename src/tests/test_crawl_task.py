from __future__ import annotations

import pytest

from src.models.crawl_task import CrawlTask, load_tasks


def _make_row(**overrides) -> dict:
    """Build a minimal CSV row dict with sensible defaults."""
    row = {
        "actor_class": "google_news",
        "search_params": "test",
        "country_id": "",
        "language": "",
        "min_date": "",
        "period": "",
        "max_results": "10",
        "enabled": "true",
        "publish": "true",
        "get_comments": "",
        "max_comments": "",
        "actor_params": "",
    }
    row.update(overrides)
    return row


class TestPeriodParsing:
    def test_period_parsed_correctly(self):
        for p in ("d", "w", "m"):
            task = CrawlTask.from_csv_row(_make_row(period=p))
            assert task.period == p

    def test_period_uppercase_normalised(self):
        task = CrawlTask.from_csv_row(_make_row(period="W"))
        assert task.period == "w"

    def test_invalid_period_ignored(self):
        task = CrawlTask.from_csv_row(_make_row(period="x"))
        assert task.period is None

    def test_empty_period_is_none(self):
        task = CrawlTask.from_csv_row(_make_row(period=""))
        assert task.period is None

    def test_period_and_min_date_mutually_exclusive(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            CrawlTask.from_csv_row(_make_row(min_date="2026-01-01", period="d"))


class TestGetCommentsParsing:
    def test_get_comments_true(self):
        task = CrawlTask.from_csv_row(_make_row(get_comments="true"))
        assert task.get_comments is True

    def test_get_comments_defaults_false(self):
        task = CrawlTask.from_csv_row(_make_row())
        assert task.get_comments is False

    def test_max_comments_parsed(self):
        task = CrawlTask.from_csv_row(_make_row(max_comments="25"))
        assert task.max_comments == 25

    def test_max_comments_defaults_15(self):
        task = CrawlTask.from_csv_row(_make_row())
        assert task.max_comments == 15


class TestToActorKwargs:
    def test_includes_period(self):
        task = CrawlTask.from_csv_row(_make_row(period="w"))
        kwargs = task.to_actor_kwargs()
        assert kwargs["period"] == "w"

    def test_includes_get_comments(self):
        task = CrawlTask.from_csv_row(_make_row(get_comments="true"))
        kwargs = task.to_actor_kwargs()
        assert kwargs["get_comments"] is True

    def test_includes_max_comments(self):
        task = CrawlTask.from_csv_row(_make_row(max_comments="20"))
        kwargs = task.to_actor_kwargs()
        assert kwargs["max_comments"] == 20
