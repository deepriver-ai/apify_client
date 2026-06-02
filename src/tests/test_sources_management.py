from __future__ import annotations

from unittest.mock import MagicMock

from src.models.news import News
from src.models.sources_management import SourcesManagement
import src.models.sources_management as sources_management


def test_check_source_does_not_track_unknowns_when_catalog_unavailable(monkeypatch, tmp_path):
    monkeypatch.setattr(sources_management, "_SOURCE_CATALOG_AVAILABLE", False)
    manager = SourcesManagement(cache_path=str(tmp_path / "unknown_sources.json"))

    assert manager.check_source("https://example.com/article", "Example") is False
    assert manager._unknown == []

    manager.save()
    assert not (tmp_path / "unknown_sources.json").exists()


def test_news_enrich_location_skips_mongo_fields_when_catalog_unavailable(monkeypatch):
    manager = MagicMock()
    manager.source_catalog_available.return_value = False
    manager.get_domain.return_value = "example.com"
    manager.get_location.return_value = {"author_location_text": "Mongo Location"}
    monkeypatch.setattr(News, "sources_manager", manager)

    news = News()
    news.data["url"] = "https://example.com/article"
    news.data["author_location_text"] = "Existing Location"

    news.enrich_location()

    assert news.data["author_location_text"] == "Existing Location"
    manager.get_domain.assert_not_called()
    manager.get_location.assert_not_called()
    manager.check_source.assert_not_called()
