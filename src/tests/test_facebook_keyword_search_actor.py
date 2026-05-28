from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from src.actors.facebook.keyword_search import FacebookKeywordSearchActor
from src.models.post import Post


def _actor() -> FacebookKeywordSearchActor:
    actor = FacebookKeywordSearchActor.__new__(FacebookKeywordSearchActor)
    actor.client = MagicMock()
    actor.search_params_keywords = ["query"]
    actor._filter_cache = {}
    actor._save_filter_cache = MagicMock()
    return actor


def _post(url: str, body: str) -> Post:
    post = Post()
    post.data["url"] = url
    post.data["body"] = body
    return post


def test_logs_urls_filtered_by_llm(caplog):
    actor = _actor()
    docs = [
        _post("https://facebook.com/posts/1", "query keep"),
        _post("https://facebook.com/posts/2", "query drop"),
    ]

    with patch("src.oai.llm_core.llm_cached_call", return_value=[1]):
        with caplog.at_level(logging.INFO, logger="src.actors.facebook.keyword_search"):
            result = actor._filter_llm(docs, llm_filter_condition="keep only first")

    assert result == [docs[0]]
    assert "Facebook keyword LLM filtered url=https://facebook.com/posts/2" in caplog.text
    assert "https://facebook.com/posts/1" not in caplog.text
    assert actor.filtered_documents[0]["reason"] == "llm_filter"
    assert actor.filtered_documents[0]["url"] == "https://facebook.com/posts/2"
