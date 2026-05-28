from __future__ import annotations

from unittest.mock import MagicMock

from src.actors.actor import ApifyActor
from src.models.document import Document


class RecordingActor(ApifyActor):
    def __init__(self):
        self.client = MagicMock()
        self.search_params_keywords = []
        self._filter_cache = {}
        self.enriched_urls = []
        self.existing_lookup_count = 0

    def _save_filter_cache(self) -> None:
        pass

    def _existing_news_ids(self, urls):
        self.existing_lookup_count += 1
        return {"https://example.com/existing"}

    def _enrich_content(self, documents, **kwargs):
        self.enriched_urls = [doc.data.get("url") for doc in documents]
        return documents

    def _enrich_location(self, documents, **kwargs):
        return documents


def _doc(url: str) -> Document:
    return Document({"url": url, "body": "body", "title": "title"})


def test_existing_elasticsearch_filter_runs_before_content_enrichment():
    actor = RecordingActor()
    documents = [
        _doc("https://example.com/existing"),
        _doc("https://example.com/new"),
    ]

    result = actor.process_documents(documents)

    assert [doc.data["url"] for doc in result] == ["https://example.com/new"]
    assert actor.enriched_urls == ["https://example.com/new"]
    assert [(entry["reason"], entry["url"]) for entry in actor.filtered_documents] == [
        ("existing_elasticsearch", "https://example.com/existing")
    ]


def test_existing_elasticsearch_filter_can_be_disabled():
    actor = RecordingActor()
    documents = [
        _doc("https://example.com/existing"),
        _doc("https://example.com/new"),
    ]

    result = actor.process_documents(documents, check_existing_elasticsearch=False)

    assert [doc.data["url"] for doc in result] == [
        "https://example.com/existing",
        "https://example.com/new",
    ]
    assert actor.enriched_urls == [
        "https://example.com/existing",
        "https://example.com/new",
    ]


def test_update_existing_keeps_existing_docs_and_skips_lookup():
    actor = RecordingActor()
    documents = [
        _doc("https://example.com/existing"),
        _doc("https://example.com/new"),
    ]

    result = actor.process_documents(documents, update_existing=True)

    assert [doc.data["url"] for doc in result] == [
        "https://example.com/existing",
        "https://example.com/new",
    ]
    assert actor.enriched_urls == [
        "https://example.com/existing",
        "https://example.com/new",
    ]
    assert actor.existing_lookup_count == 0
