from __future__ import annotations

import pytest

from src.models.crawl_task import CrawlTask, load_tasks


def _make_row(**overrides) -> dict:
    """Build a minimal CSV row dict with sensible defaults."""
    row = {
        "task_id": "test_task",
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
        "not_keywords": "",
        "llm_filter_condition": "",
        "override_filters": "",
        "enrich_followers": "",
        "fetch_attached_url": "",
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

    def test_includes_not_keywords(self):
        task = CrawlTask.from_csv_row(_make_row(not_keywords="spam|ads"))
        kwargs = task.to_actor_kwargs()
        assert kwargs["not_keywords"] == ["spam", "ads"]


class TestNotKeywordsParsing:
    def test_pipe_separated(self):
        task = CrawlTask.from_csv_row(_make_row(not_keywords="spam|ads|promo"))
        assert task.not_keywords == ["spam", "ads", "promo"]

    def test_single_keyword(self):
        task = CrawlTask.from_csv_row(_make_row(not_keywords="spam"))
        assert task.not_keywords == ["spam"]

    def test_empty_string(self):
        task = CrawlTask.from_csv_row(_make_row(not_keywords=""))
        assert task.not_keywords == []

    def test_missing_column(self):
        row = _make_row()
        del row["not_keywords"]
        task = CrawlTask.from_csv_row(row)
        assert task.not_keywords == []

    def test_whitespace_trimmed(self):
        task = CrawlTask.from_csv_row(_make_row(not_keywords=" spam | ads "))
        assert task.not_keywords == ["spam", "ads"]


class TestLlmFilterConditionParsing:
    def test_condition_parsed(self):
        task = CrawlTask.from_csv_row(_make_row(llm_filter_condition="elimina spam"))
        assert task.llm_filter_condition == "elimina spam"

    def test_empty_is_none(self):
        task = CrawlTask.from_csv_row(_make_row(llm_filter_condition=""))
        assert task.llm_filter_condition is None

    def test_missing_column_is_none(self):
        row = _make_row()
        del row["llm_filter_condition"]
        task = CrawlTask.from_csv_row(row)
        assert task.llm_filter_condition is None

    def test_included_in_kwargs(self):
        task = CrawlTask.from_csv_row(_make_row(llm_filter_condition="keep only relevant"))
        kwargs = task.to_actor_kwargs()
        assert kwargs["llm_filter_condition"] == "keep only relevant"


class TestOverrideFiltersParsing:
    def test_true(self):
        task = CrawlTask.from_csv_row(_make_row(override_filters="true"))
        assert task.override_filters is True

    def test_defaults_false(self):
        task = CrawlTask.from_csv_row(_make_row())
        assert task.override_filters is False

    def test_included_in_kwargs(self):
        task = CrawlTask.from_csv_row(_make_row(override_filters="true"))
        kwargs = task.to_actor_kwargs()
        assert kwargs["override_filters"] is True


class TestFetchAttachedUrlParsing:
    def test_true(self):
        task = CrawlTask.from_csv_row(_make_row(fetch_attached_url="true"))
        assert task.fetch_attached_url is True

    def test_defaults_false(self):
        task = CrawlTask.from_csv_row(_make_row())
        assert task.fetch_attached_url is False

    def test_included_in_kwargs(self):
        task = CrawlTask.from_csv_row(_make_row(fetch_attached_url="true"))
        kwargs = task.to_actor_kwargs()
        assert kwargs["fetch_attached_url"] is True


class TestTaskIdParsing:
    def test_explicit_id(self):
        task = CrawlTask.from_csv_row(_make_row(task_id="my_task"))
        assert task.task_id == "my_task"

    def test_auto_generated(self):
        task = CrawlTask.from_csv_row(_make_row(task_id=""))
        assert task.task_id == "google_news:test"

    def test_included_in_kwargs(self):
        task = CrawlTask.from_csv_row(_make_row(task_id="t1"))
        kwargs = task.to_actor_kwargs()
        assert kwargs["task_id"] == "t1"
