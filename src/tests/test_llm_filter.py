"""Tests for _filter_llm and _build_snippet in ApifyActor."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.actors.actor import ApifyActor
from src.models.post import Post


@pytest.fixture
def actor():
    with patch("src.actors.actor.ApifyActor._load_filter_cache", return_value={}):
        a = ApifyActor.__new__(ApifyActor)
    a.client = MagicMock()
    a.search_params = ["totalenergies"]
    a._filter_cache = {}
    a._save_filter_cache = MagicMock()  # avoid writing to disk in tests
    return a


class TestBuildSnippet:
    """Test _build_snippet keyword context extraction."""

    def test_keyword_context(self, actor):
        doc = Post()
        doc.data["body"] = "x" * 200 + "totalenergies es una empresa" + "y" * 200
        snippet = actor._build_snippet(doc)
        assert "totalenergies" in snippet
        assert len(snippet) <= 300  # ~250 + keyword length

    def test_keyword_at_start(self, actor):
        doc = Post()
        doc.data["body"] = "totalenergies is great " + "x" * 300
        snippet = actor._build_snippet(doc)
        assert snippet.startswith("totalenergies")

    def test_no_keyword_match_uses_first_chars(self, actor):
        doc = Post()
        doc.data["body"] = "This post has nothing related " + "x" * 300
        snippet = actor._build_snippet(doc)
        assert snippet == doc.data["body"][:250]

    def test_no_search_params_uses_first_chars(self, actor):
        actor.search_params = []
        doc = Post()
        doc.data["body"] = "Some post text " + "x" * 300
        snippet = actor._build_snippet(doc)
        assert snippet == doc.data["body"][:250]

    def test_empty_body(self, actor):
        doc = Post()
        doc.data["body"] = ""
        snippet = actor._build_snippet(doc)
        assert snippet == ""


class TestFilterLlm:
    """Test _filter_llm pipeline step."""

    def test_skips_when_no_condition(self, actor):
        docs = [Post(), Post()]
        result = actor._filter_llm(docs)
        assert len(result) == 2

    def test_skips_when_condition_is_none(self, actor):
        docs = [Post(), Post()]
        result = actor._filter_llm(docs, llm_filter_condition=None)
        assert len(result) == 2

    def test_skips_when_empty_docs(self, actor):
        result = actor._filter_llm([], llm_filter_condition="some condition")
        assert result == []

    def test_filters_based_on_llm_response(self, actor):
        docs = []
        for i in range(3):
            d = Post()
            d.data["body"] = f"Post number {i} about totalenergies"
            d.data["url"] = f"https://example.com/post/{i}"
            docs.append(d)

        # LLM returns [1, 3] meaning keep doc 0 and doc 2 (1-indexed)
        with patch("src.oai.llm_core.llm_cached_call", return_value=[1, 3]):
            result = actor._filter_llm(docs, llm_filter_condition="keep relevant")

        assert len(result) == 2
        assert result[0] is docs[0]
        assert result[1] is docs[2]

    def test_keeps_all_on_non_list_response(self, actor):
        docs = [Post(), Post()]
        docs[0].data["body"] = "text"
        docs[0].data["url"] = "https://example.com/1"
        docs[1].data["body"] = "text"
        docs[1].data["url"] = "https://example.com/2"

        with patch("src.oai.llm_core.llm_cached_call", return_value="error"):
            result = actor._filter_llm(docs, llm_filter_condition="keep all")

        assert len(result) == 2

    def test_batching(self, actor):
        """Documents should be processed in batches of LLM_FILTER_BATCH_SIZE."""
        docs = []
        for i in range(25):
            d = Post()
            d.data["body"] = f"Post {i}"
            d.data["url"] = f"https://example.com/{i}"
            docs.append(d)

        call_count = 0

        def mock_llm_call(**kwargs):
            nonlocal call_count
            call_count += 1
            # Keep all in each batch
            return list(range(1, 21)) if call_count == 1 else list(range(1, 6))

        with patch("src.oai.llm_core.llm_cached_call", side_effect=mock_llm_call):
            result = actor._filter_llm(docs, llm_filter_condition="keep all")

        assert call_count == 2  # 20 + 5
        assert len(result) == 25

    def test_llm_snippet_includes_author_bio(self, actor):
        """author_profile_bio should appear in LLM snippet metadata with newlines replaced."""
        docs = [Post()]
        docs[0].data["body"] = "Post about totalenergies"
        docs[0].data["url"] = "https://example.com/0"
        docs[0].data["author"] = "testuser"
        docs[0].data["author_profile_bio"] = "Line one\nLine two\nLine three"
        docs[0].data["author_location_text"] = "Mexico City"

        captured_kwargs = {}

        def mock_llm_call(**kwargs):
            captured_kwargs.update(kwargs)
            return [1]

        with patch("src.oai.llm_core.llm_cached_call", side_effect=mock_llm_call):
            actor._filter_llm(docs, llm_filter_condition="test")

        messages = captured_kwargs["messages_builder"]()
        user_msg = messages[1]["content"]
        assert "user_name: testuser" in user_msg
        assert "user_location: Mexico City" in user_msg
        assert "user_bio: Line one Line two Line three" in user_msg
        # No newlines in the bio part
        assert "Line one\n" not in user_msg

    def test_llm_call_receives_correct_snippets(self, actor):
        docs = []
        for i in range(2):
            d = Post()
            d.data["body"] = f"Post about totalenergies number {i}"
            d.data["url"] = f"https://example.com/{i}"
            docs.append(d)

        captured_kwargs = {}

        def mock_llm_call(**kwargs):
            captured_kwargs.update(kwargs)
            return [1, 2]

        with patch("src.oai.llm_core.llm_cached_call", side_effect=mock_llm_call):
            actor._filter_llm(docs, llm_filter_condition="test condition")

        # Verify the messages_builder produces correct format
        messages = captured_kwargs["messages_builder"]()
        system_msg = messages[0]["content"]
        user_msg = messages[1]["content"]

        assert "test condition" in system_msg
        assert "[1]" in user_msg
        assert "[2]" in user_msg
        assert "totalenergies" in user_msg


class TestProcessDocumentsFilterCache:
    """Test general filter cache and override_filters in process_documents."""

    TASK_ID = "test_task"

    def test_filtered_docs_cached_as_false(self, actor):
        """Documents that don't survive filtering should be cached as False."""
        docs = []
        for i in range(3):
            d = Post()
            d.data["body"] = f"Post {i} about something"
            d.data["url"] = f"https://example.com/{i}"
            d.data["timestamp"] = "2026-03-28T12:00:00"
            docs.append(d)

        docs[1].data["body"] = "This post has spam in it"
        result = actor.process_documents(docs, task_id=self.TASK_ID, not_keywords=["spam"])

        assert len(result) == 2
        key1 = actor._filter_cache_key(docs[1], self.TASK_ID)
        assert actor._filter_cache.get(key1) is False

    def test_surviving_docs_cached_as_true(self, actor):
        docs = [Post()]
        docs[0].data["body"] = "Good post"
        docs[0].data["url"] = "https://example.com/good"

        result = actor.process_documents(docs, task_id=self.TASK_ID)
        key = actor._filter_cache_key(docs[0], self.TASK_ID)
        assert actor._filter_cache.get(key) is True

    def test_cached_false_docs_skipped(self, actor):
        """Documents previously cached as filtered-out should be skipped."""
        docs = []
        for i in range(3):
            d = Post()
            d.data["body"] = f"Post {i}"
            d.data["url"] = f"https://example.com/{i}"
            docs.append(d)

        # Pre-cache doc 1 as filtered out for this task
        actor._filter_cache[actor._filter_cache_key(docs[1], self.TASK_ID)] = False

        result = actor.process_documents(docs, task_id=self.TASK_ID)
        assert len(result) == 2
        assert docs[1] not in result

    def test_different_tasks_independent_cache(self, actor):
        """Same URL can have different filter results for different tasks."""
        doc = Post()
        doc.data["body"] = "Some post"
        doc.data["url"] = "https://example.com/0"

        # Cached as filtered-out for task_a, not cached for task_b
        actor._filter_cache[actor._filter_cache_key(doc, "task_a")] = False

        result_a = actor.process_documents([doc], task_id="task_a")
        assert len(result_a) == 0

        result_b = actor.process_documents([doc], task_id="task_b")
        assert len(result_b) == 1

    def test_override_filters_ignores_cache(self, actor):
        """override_filters should ignore cached results and re-filter."""
        docs = [Post()]
        docs[0].data["body"] = "Some post"
        docs[0].data["url"] = "https://example.com/0"

        # Pre-cache as filtered out
        actor._filter_cache[actor._filter_cache_key(docs[0], self.TASK_ID)] = False

        result = actor.process_documents(docs, task_id=self.TASK_ID, override_filters=True)
        assert len(result) == 1

    def test_override_filters_still_runs_filters(self, actor):
        """override_filters should still run all filter steps, not skip them."""
        docs = []
        for i in range(3):
            d = Post()
            d.data["body"] = f"Post {i} with spam"
            d.data["url"] = f"https://example.com/{i}"
            docs.append(d)

        result = actor.process_documents(
            docs,
            task_id=self.TASK_ID,
            not_keywords=["spam"],
            override_filters=True,
        )

        # Keyword filter should still apply even with override
        assert len(result) == 0
