from __future__ import annotations

import os
import tempfile
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from src.actors.instagram.profile_posts import (
    InstagramProfilePostsActor,
    _download_video_file,
    _url_hash,
    _video_exists,
)
from src.models.instagram_post import InstagramPost


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def actor(mock_sources_manager):
    a = InstagramProfilePostsActor.__new__(InstagramProfilePostsActor)
    a.client = MagicMock()
    a.sources_manager = mock_sources_manager
    a.search_params_keywords = []
    a._filter_cache = {}
    a._save_filter_cache = MagicMock()
    return a


# ---------------------------------------------------------------------------
# TestSearchCreatesDocuments
# ---------------------------------------------------------------------------

class TestSearchCreatesDocuments:
    def test_creates_post_objects(self, actor, sample_instagram_profile_posts_results):
        with patch.object(actor, "run_actor", return_value=sample_instagram_profile_posts_results):
            posts = actor.search(
                ["https://www.instagram.com/totalenergies_mx/"],
                max_results=10,
            )
        assert all(isinstance(p, InstagramPost) for p in posts)
        assert len(posts) == 3

    def test_params_sent_to_apify(self, actor, sample_instagram_profile_posts_results):
        with patch.object(actor, "run_actor", return_value=sample_instagram_profile_posts_results) as mock_run:
            actor.search(
                ["https://www.instagram.com/totalenergies_mx/"],
                max_results=5,
                results_type="posts",
            )
        call_args = mock_run.call_args[0][0]
        assert call_args["directUrls"] == ["https://www.instagram.com/totalenergies_mx/"]
        assert call_args["resultsType"] == "posts"
        assert call_args["resultsLimit"] == 5

    def test_video_post_fields_mapped(self, actor, sample_instagram_profile_posts_results):
        with patch.object(actor, "run_actor", return_value=sample_instagram_profile_posts_results):
            posts = actor.search(
                ["https://www.instagram.com/totalenergies_mx/"],
                max_results=10,
            )
        video_post = [p for p in posts if p.data["post_type"] == "Video"][0]
        assert video_post.data["source"] == "Instagram"
        assert video_post.data["views"] == 5000
        assert video_post.data["profile_url"] == "https://www.instagram.com/totalenergies_mx/"


# ---------------------------------------------------------------------------
# TestDateFilter
# ---------------------------------------------------------------------------

class TestDateFilter:
    def test_period_d_filters_old_posts(self, actor, sample_instagram_profile_posts_results):
        """Period 'd' should keep only posts from the last day."""
        with patch.object(actor, "run_actor", return_value=sample_instagram_profile_posts_results):
            posts = actor.search(
                ["https://www.instagram.com/totalenergies_mx/"],
                period="d",
            )
        # All sample posts are from March 2026 — with period="d" relative to now (April 2026),
        # all should be filtered out.
        assert len(posts) == 0


# ---------------------------------------------------------------------------
# TestVideoDownload
# ---------------------------------------------------------------------------

class TestVideoDownload:
    def test_download_video_sets_video_filename(
        self, actor, sample_instagram_profile_posts_results, sample_instagram_video_downloads,
    ):
        """_download_video should set video_filename on video/reel posts."""
        posts = [InstagramPost.from_instagram(item) for item in sample_instagram_profile_posts_results]

        # Mock the video downloader actor call
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter(sample_instagram_video_downloads)
        actor.client.dataset.return_value = mock_dataset
        actor.client.actor.return_value.call.return_value = {"defaultDatasetId": "ds123"}

        with patch(
            "src.actors.instagram.profile_posts._download_video_file",
            side_effect=lambda url, d, h, ext="mp4": os.path.join(d, f"{h}.{ext}"),
        ):
            result = actor._download_video(posts, download_video=True)

        # Video post should have video_filename set
        video_post = [p for p in result if p.data["post_type"] == "Video"][0]
        assert video_post.data["video_filename"] is not None
        assert video_post.data["video_filename"].endswith(".mp4")

        # Reel post should also have video_filename set
        reel_post = [p for p in result if p.data["post_type"] == "Reel"][0]
        assert reel_post.data["video_filename"] is not None

        # Image post should NOT have video_filename set
        image_post = [p for p in result if p.data["post_type"] == "Image"][0]
        assert image_post.data["video_filename"] is None

    def test_download_video_not_called_without_flag(
        self, actor, sample_instagram_profile_posts_results,
    ):
        """_enrich_content should NOT call _download_video when flag is not set."""
        posts = [InstagramPost.from_instagram(item) for item in sample_instagram_profile_posts_results]
        with patch.object(actor, "_download_video") as mock_dl:
            actor._enrich_content(posts)
        mock_dl.assert_not_called()

    def test_download_video_called_with_flag(
        self, actor, sample_instagram_profile_posts_results,
    ):
        """_enrich_content should call _download_video when download_video=True."""
        posts = [InstagramPost.from_instagram(item) for item in sample_instagram_profile_posts_results]
        with patch.object(actor, "_download_video") as mock_dl:
            actor._enrich_content(posts, download_video=True)
        mock_dl.assert_called_once()

    def test_download_video_skips_when_no_video_posts(self, actor):
        """_download_video returns early when there are no video posts."""
        image_item = {
            "type": "Image",
            "shortCode": "IMG123",
            "caption": "An image",
            "url": "https://www.instagram.com/p/IMG123/",
            "ownerUsername": "user",
            "ownerFullName": "User",
            "likesCount": 1,
            "commentsCount": 0,
            "timestamp": "2026-03-20T10:00:00.000Z",
        }
        posts = [InstagramPost.from_instagram(image_item)]
        # Should not call the actor at all
        result = actor._download_video(posts)
        actor.client.actor.assert_not_called()
        assert result == posts

    def test_download_video_uses_cache(
        self, actor, sample_instagram_profile_posts_results, sample_instagram_video_downloads,
    ):
        """Already downloaded videos should not be re-downloaded."""
        posts = [InstagramPost.from_instagram(item) for item in sample_instagram_profile_posts_results]

        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter(sample_instagram_video_downloads)
        actor.client.dataset.return_value = mock_dataset
        actor.client.actor.return_value.call.return_value = {"defaultDatasetId": "ds123"}

        # Pretend all videos already exist locally
        with patch(
            "src.actors.instagram.profile_posts._video_exists",
            return_value="/cached/video.mp4",
        ), patch(
            "src.actors.instagram.profile_posts._download_video_file",
        ) as mock_dl_file:
            actor._download_video(posts, download_video=True)

        # _download_video_file should never be called since all are cached
        mock_dl_file.assert_not_called()


# ---------------------------------------------------------------------------
# TestVideoHelpers
# ---------------------------------------------------------------------------

class TestVideoHelpers:
    def test_url_hash_deterministic(self):
        url = "https://www.instagram.com/p/ABC123/"
        assert _url_hash(url) == _url_hash(url)

    def test_url_hash_different_urls(self):
        assert _url_hash("https://a.com/1") != _url_hash("https://a.com/2")

    def test_url_hash_length(self):
        assert len(_url_hash("https://example.com")) == 12

    def test_video_exists_returns_none_when_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            assert _video_exists(tmpdir, "nonexistent") is None

    def test_video_exists_returns_path_when_found(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "abc123456789.mp4")
            open(path, "w").close()
            result = _video_exists(tmpdir, "abc123456789")
            assert result == path


# ---------------------------------------------------------------------------
# TestFinalSchema
# ---------------------------------------------------------------------------

class TestFinalSchema:
    def test_schema_includes_video_filename(self, sample_instagram_profile_posts_results):
        """video_filename should flow through to source_extra.stats in the final schema."""
        post = InstagramPost.from_instagram(sample_instagram_profile_posts_results[0])
        post.data["video_filename"] = "/path/to/video.mp4"
        schema = post.to_final_schema()
        stats = schema["message"]["source_extra"]["stats"]
        assert stats["video_filename"] == "/path/to/video.mp4"

    def test_schema_video_filename_none_when_not_set(self, sample_instagram_profile_posts_results):
        """video_filename should be None in final schema when no video was downloaded."""
        post = InstagramPost.from_instagram(sample_instagram_profile_posts_results[1])  # Image post
        schema = post.to_final_schema()
        stats = schema["message"]["source_extra"]["stats"]
        assert stats.get("video_filename") is None


# ---------------------------------------------------------------------------
# TestStubs
# ---------------------------------------------------------------------------

class TestStubs:
    def test_download_images_not_implemented(self, actor):
        with pytest.raises(NotImplementedError):
            actor._download_images([])

    def test_add_text_from_images_not_implemented(self, actor):
        with pytest.raises(NotImplementedError):
            actor._add_text_from_images([])

    def test_add_subtitles_not_implemented(self, actor):
        with pytest.raises(NotImplementedError):
            actor._add_subtitles([])

    def test_add_ai_transcription_not_implemented(self, actor):
        with pytest.raises(NotImplementedError):
            actor._add_ai_transcription([])
