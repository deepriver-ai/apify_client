"""Tests for social_enrichment utilities and actor-specific stubs."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.models.post import Post, _extract_first_external_url
from src.actors.instagram.hashtags import InstagramHashtagActor
from src.actors.facebook.posts import FacebookPagePostsActor


class TestExtractFirstExternalUrl:
    """Test URL extraction from text."""

    def test_extracts_url(self):
        text = "Check out https://example.com/article for details"
        assert _extract_first_external_url(text) == "https://example.com/article"

    def test_skips_social_domains(self):
        text = "Follow us https://instagram.com/page and visit https://example.com/news"
        assert _extract_first_external_url(text) == "https://example.com/news"

    def test_skips_own_url(self):
        text = "Visit https://example.com/page and https://other.com/article"
        result = _extract_first_external_url(text, own_url="https://example.com/post")
        assert result == "https://other.com/article"

    def test_returns_none_for_no_urls(self):
        assert _extract_first_external_url("no urls here") is None

    def test_returns_none_for_only_social_urls(self):
        text = "https://facebook.com/page https://twitter.com/user"
        assert _extract_first_external_url(text) is None

    def test_strips_trailing_punctuation(self):
        text = "Visit https://example.com/article."
        assert _extract_first_external_url(text) == "https://example.com/article"


class TestFetchAttachedUrl:
    """Test fetch_attached_url enrichment."""

    def test_appends_article_text_to_body(self):
        post = Post()
        post.data["body"] = "Check this out: https://example.com/article"
        post.data["url"] = "https://instagram.com/p/123"

        mock_news = MagicMock()
        mock_news.data = {"body": "Article content here"}

        with patch("src.models.news.News.from_url", return_value=mock_news):
            post.fetch_attached_url()

        assert "attached_url_text: Article content here" in post.data["body"]
        assert post.attached_news is mock_news

    def test_no_url_no_change(self):
        post = Post()
        post.data["body"] = "Just a caption with no links"
        original_body = post.data["body"]

        post.fetch_attached_url()
        assert post.data["body"] == original_body
        assert post.attached_news is None

    def test_sets_location_from_sources_manager(self):
        post = Post()
        post.data["body"] = "Read more: https://example.com/article"
        post.data["url"] = "https://instagram.com/p/123"

        mock_news = MagicMock()
        mock_news.data = {"body": "Article text"}

        sources = MagicMock()
        sources.is_known.return_value = True
        sources.get_location.return_value = {
            "author_location_text": "Mexico City",
            "author_location_id": "_48416053",
        }

        with patch("src.models.news.News.from_url", return_value=mock_news):
            post.fetch_attached_url(sources_manager=sources)

        assert post.data["author_location_text"] == "Mexico City"

    def test_does_not_overwrite_existing_location(self):
        post = Post()
        post.data["body"] = "Read more: https://example.com/article"
        post.data["url"] = "https://instagram.com/p/123"
        post.data["author_location_text"] = "Existing Location"

        mock_news = MagicMock()
        mock_news.data = {"body": "Article text"}

        sources = MagicMock()
        sources.is_known.return_value = True
        sources.get_location.return_value = {
            "author_location_text": "New Location",
        }

        with patch("src.models.news.News.from_url", return_value=mock_news):
            post.fetch_attached_url(sources_manager=sources)

        assert post.data["author_location_text"] == "Existing Location"


class TestInstagramStubsRaiseNotImplemented:
    """Verify Instagram actor stubs raise NotImplementedError."""

    @pytest.fixture
    def actor(self):
        a = InstagramHashtagActor.__new__(InstagramHashtagActor)
        a.client = MagicMock()
        a.sources_manager = MagicMock()
        a.search_params = []
        a._filter_cache = {}
        a._save_filter_cache = MagicMock()
        return a

    def test_download_images(self, actor):
        with pytest.raises(NotImplementedError, match="Instagram"):
            actor._download_images([])

    def test_download_video(self, actor):
        with pytest.raises(NotImplementedError, match="Instagram"):
            actor._download_video([])

    def test_add_text_from_images(self, actor):
        with pytest.raises(NotImplementedError, match="Instagram"):
            actor._add_text_from_images([])

    def test_add_subtitles(self, actor):
        with pytest.raises(NotImplementedError, match="Instagram"):
            actor._add_subtitles([])

    def test_add_ai_transcription(self, actor):
        with pytest.raises(NotImplementedError, match="Instagram"):
            actor._add_ai_transcription([])


class TestFacebookStubsRaiseNotImplemented:
    """Verify Facebook actor stubs raise NotImplementedError."""

    @pytest.fixture
    def actor(self):
        a = FacebookPagePostsActor.__new__(FacebookPagePostsActor)
        a.client = MagicMock()
        a.sources_manager = MagicMock()
        a.search_params = []
        a._filter_cache = {}
        a._save_filter_cache = MagicMock()
        return a

    def test_download_images(self, actor):
        with pytest.raises(NotImplementedError, match="Facebook"):
            actor._download_images([])

    def test_download_video(self, actor):
        with pytest.raises(NotImplementedError, match="Facebook"):
            actor._download_video([])

    def test_add_subtitles(self, actor):
        with pytest.raises(NotImplementedError, match="Facebook"):
            actor._add_subtitles([])

    def test_add_ai_transcription(self, actor):
        with pytest.raises(NotImplementedError, match="Facebook"):
            actor._add_ai_transcription([])
