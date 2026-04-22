"""Tests for FacebookPagePostsActor."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.actors.facebook.comments import FacebookCommentsActor
from src.actors.facebook.posts import FacebookPagePostsActor
from src.actors.facebook.profiles import FacebookProfileActor
from src.models.facebook_post import FacebookPost


class TestFromFacebook:
    """Test FacebookPost.from_facebook() mapping."""

    def test_creates_post_from_photo(self, sample_facebook_posts_results):
        item = sample_facebook_posts_results[0]  # Photo post
        post = FacebookPost.from_facebook(item)

        assert post.data["type"] == "facebook"
        assert post.data["source"] == "QroMunicipio"
        assert post.data["author"] == "QroMunicipio"
        assert post.data["post_type"] == "Photo"
        assert post.data["url"] is not None
        assert post.data["body"] is not None
        assert post.data["likes"] is not None
        assert post.data["shares"] is not None
        assert post.data["profile_url"] == "https://www.facebook.com/QroMunicipio/"

    def test_creates_post_from_video(self, sample_facebook_posts_results):
        item = sample_facebook_posts_results[1]  # Video/Reel post
        post = FacebookPost.from_facebook(item)

        assert post.data["type"] == "facebook"
        assert post.data["post_type"] in ("Video", "Reel")
        # Video posts have publish_time as timestamp
        assert post.data["timestamp"] is not None
        # Video posts should have video URLs in media_urls
        assert len(post.data["media_urls"]) > 0

    def test_media_urls_collected(self, sample_facebook_posts_results):
        # Photo post should have photo_image URI
        item = sample_facebook_posts_results[0]
        post = FacebookPost.from_facebook(item)
        assert len(post.data["media_urls"]) > 0

    def test_ocr_not_in_body_by_default(self, sample_facebook_posts_results):
        """OCR text should NOT be in body from from_facebook — requires _add_text_from_images."""
        item = sample_facebook_posts_results[0]
        post = FacebookPost.from_facebook(item)
        assert "image_text_" not in post.data["body"]

    def test_raw_response_stored(self, sample_facebook_posts_results):
        """from_facebook should store the full raw API response."""
        item = sample_facebook_posts_results[0]
        post = FacebookPost.from_facebook(item)
        assert post._raw is item
        assert "media" in post._raw

    def test_comments_count_mapped(self, sample_facebook_posts_results):
        item = sample_facebook_posts_results[3]  # Has comments field
        post = FacebookPost.from_facebook(item)
        assert post.data["n_comments"] is not None


class TestSearchCreatesDocuments:
    """Test that search() creates Post objects."""

    @pytest.fixture(autouse=True)
    def _setup(self, sample_facebook_posts_results):
        self.sample_data = sample_facebook_posts_results

    @pytest.mark.parametrize("mock_apify_run_actor", [None], indirect=True)
    def test_creates_post_objects(self, mock_apify_run_actor):
        mock_apify_run_actor.return_value = self.sample_data
        actor = FacebookPagePostsActor()
        posts = actor.search(["https://www.facebook.com/QroMunicipio/"])
        assert all(isinstance(p, FacebookPost) for p in posts)
        assert len(posts) == len(self.sample_data)

    @pytest.mark.parametrize("mock_apify_run_actor", [None], indirect=True)
    def test_params_sent_to_apify(self, mock_apify_run_actor):
        mock_apify_run_actor.return_value = []
        actor = FacebookPagePostsActor()
        actor.search(
            ["https://www.facebook.com/QroMunicipio/"],
            max_results=10,
        )
        call_args = mock_apify_run_actor.call_args[0][0]
        assert call_args["startUrls"] == [{"url": "https://www.facebook.com/QroMunicipio/"}]
        assert call_args["resultsLimit"] == 10
        assert call_args["captionText"] is True


class TestKeywordFilter:
    """Test not_keywords filtering."""

    @pytest.mark.parametrize("mock_apify_run_actor", [None], indirect=True)
    def test_keyword_filter_removes_matching(self, mock_apify_run_actor, sample_facebook_posts_results):
        mock_apify_run_actor.return_value = sample_facebook_posts_results
        actor = FacebookPagePostsActor()
        # "tacos" appears in one post
        posts = actor.search(
            ["https://www.facebook.com/QroMunicipio/"],
            not_keywords=["tacos"],
        )
        bodies = [p.data.get("body", "") for p in posts]
        assert not any("tacos" in (b or "").lower() for b in bodies)

    @pytest.mark.parametrize("mock_apify_run_actor", [None], indirect=True)
    def test_keyword_filter_empty_keeps_all(self, mock_apify_run_actor, sample_facebook_posts_results):
        mock_apify_run_actor.return_value = sample_facebook_posts_results
        actor = FacebookPagePostsActor()
        posts = actor.search(
            ["https://www.facebook.com/QroMunicipio/"],
            not_keywords=[],
        )
        assert len(posts) == len(sample_facebook_posts_results)


class TestAddTextFromImages:
    """Test _add_text_from_images enrichment."""

    @pytest.fixture
    def actor(self):
        a = FacebookPagePostsActor.__new__(FacebookPagePostsActor)
        a.client = MagicMock()
        a.sources_manager = MagicMock()
        a.search_params = []
        a._filter_cache = {}
        a._save_filter_cache = MagicMock()
        return a

    def test_appends_ocr_text(self, actor, sample_facebook_posts_results):
        """Non-generic OCR text should be appended to body."""
        post = FacebookPost.from_facebook(sample_facebook_posts_results[3])
        post._raw["media"] = [{"ocrText": "Important text from image"}]
        actor._add_text_from_images([post])
        assert "\n\n image_text_1: Important text from image" in post.data["body"]

    def test_skips_generic_ocr(self, actor, sample_facebook_posts_results):
        """OCR starting with 'May be an image of' should be skipped."""
        post = FacebookPost.from_facebook(sample_facebook_posts_results[2])
        original_body = post.data["body"]
        actor._add_text_from_images([post])
        assert post.data["body"] == original_body

    def test_multiple_ocr_indexed(self, actor):
        """Multiple OCR texts should be indexed as image_text_1, image_text_2, etc."""
        post = FacebookPost(data=FacebookPost._empty_data(), raw={
            "media": [
                {"ocrText": "First OCR"},
                {"ocrText": "Second OCR"},
            ]
        })
        post.data["body"] = "caption"
        actor._add_text_from_images([post])
        assert "image_text_1: First OCR" in post.data["body"]
        assert "image_text_2: Second OCR" in post.data["body"]

    def test_no_raw_media_no_change(self, actor):
        """Posts without media in _raw should be unchanged."""
        post = FacebookPost(data=FacebookPost._empty_data())
        post.data["body"] = "caption"
        actor._add_text_from_images([post])
        assert post.data["body"] == "caption"


class TestFinalSchema:
    """Test to_final_schema() on Facebook posts."""

    def test_schema_complete(self, sample_facebook_posts_results):
        # Use a post with timestamp — set one manually on a photo post for reliable testing
        post = FacebookPost.from_facebook(sample_facebook_posts_results[0])
        post.data["timestamp"] = "2026-03-28T12:00:00"
        final = post.to_final_schema()
        assert final["type"] == "news"  # envelope is always "news"
        msg = final["message"]
        assert msg["source"] == "QroMunicipio"
        assert msg["type"] == "facebook"
        assert "body" in msg
        assert "title" in msg
        assert "comments" in msg


class TestFacebookProfileActorMapProfile:
    """Test FacebookProfileActor.map_profile() field mapping."""

    def test_maps_standard_fields(self, sample_facebook_page_profiles):
        raw = sample_facebook_page_profiles[0]
        mapped = FacebookProfileActor.map_profile(raw)

        assert mapped["website_visits"] == 120967
        assert mapped["author_full_name"] == "Desde El Marqués | Querétaro"
        assert "Noticias de El Marqués" in mapped["author_profile_bio"]
        assert "Querétaro" in mapped["author_location_text"]

    def test_bio_joins_info_list(self, sample_facebook_page_profiles):
        raw = sample_facebook_page_profiles[0]
        mapped = FacebookProfileActor.map_profile(raw)
        # info is a list, should be joined with newline
        assert "\n" in mapped["author_profile_bio"]

    def test_stores_extra_fields(self, sample_facebook_page_profiles):
        raw = sample_facebook_page_profiles[0]
        mapped = FacebookProfileActor.map_profile(raw)

        assert mapped["categories"] == ["Page", "Interest"]
        assert mapped["rating"] is not None
        assert mapped["email"] == "circulonoticiasqueretaro@gmail.com"
        assert mapped["phone"] == "+52 442 104 2095"
        assert mapped["website"] == "http://www.circulonoticias.com/"

    def test_stores_raw_profile(self, sample_facebook_page_profiles):
        raw = sample_facebook_page_profiles[0]
        mapped = FacebookProfileActor.map_profile(raw)
        assert mapped["_raw_profile"] is raw

    def test_handles_missing_fields(self):
        mapped = FacebookProfileActor.map_profile({})
        assert mapped["website_visits"] is None
        assert mapped["author_full_name"] is None
        assert mapped["author_profile_bio"] is None
        assert mapped["author_location_text"] is None


class TestEnrichUserAuthor:
    """Test FacebookPagePostsActor._enrich_user_author()."""

    @pytest.fixture(autouse=True)
    def _reset_users_cache(self):
        """Reset shared UsersManagement cache between tests."""
        from src.models.post import Post
        Post.users_manager._users = {}

    @pytest.fixture
    def actor(self):
        a = FacebookPagePostsActor.__new__(FacebookPagePostsActor)
        a.client = MagicMock()
        a.sources_manager = MagicMock()
        a.search_params = []
        a._filter_cache = {}
        a._save_filter_cache = MagicMock()
        return a

    def test_skips_when_enrich_followers_false(self, actor, sample_facebook_posts_results):
        posts = [FacebookPost.from_facebook(item) for item in sample_facebook_posts_results]
        result = actor._enrich_user_author(posts, enrich_followers=False)
        assert len(result) == len(posts)
        # No Apify call should have been made
        actor.client.actor.assert_not_called()

    def test_scrapes_and_maps_profiles(self, actor, sample_facebook_posts_results, sample_facebook_page_profiles):
        # Use only posts with a profile_url (skip reels whose URL doesn't identify a page)
        posts = [
            FacebookPost.from_facebook(item)
            for item in sample_facebook_posts_results
            if FacebookPost.from_facebook(item).data.get("profile_url")
        ][:2]
        assert len(posts) > 0

        # Return profile data with pageUrl matching the posts' page
        profile_data = sample_facebook_page_profiles[0].copy()
        profile_data["pageUrl"] = "https://www.facebook.com/QroMunicipio"

        with patch.object(FacebookProfileActor, "scrape_pages", return_value=[profile_data]):
            result = actor._enrich_user_author(posts, enrich_followers=True)

        assert len(result) == len(posts)
        for post in result:
            assert post.data["website_visits"] == 120967
            assert post.data["author_full_name"] == "Desde El Marqués | Querétaro"
            assert post.data["author_location_text"] is not None

    def test_applies_cached_stats(self, actor, sample_facebook_posts_results):
        posts = [FacebookPost.from_facebook(item) for item in sample_facebook_posts_results[:1]]

        # Pre-cache stats in UsersManagement
        profile_url = posts[0].data["profile_url"]
        posts[0].users_manager.save_stats(profile_url, {
            "website_visits": 50000,
            "author_full_name": "Cached Name",
            "author_profile_bio": "Cached bio",
            "author_location_text": "Cached Location",
        })

        result = actor._enrich_user_author(posts, enrich_followers=True)

        # Should use cached data, no Apify call needed
        assert result[0].data["website_visits"] == 50000
        assert result[0].data["author_full_name"] == "Cached Name"
        assert result[0].data["author_location_text"] == "Cached Location"

    def test_deduplicates_page_urls(self, actor, sample_facebook_posts_results):
        # All sample posts are from QroMunicipio — should only scrape once
        posts = [FacebookPost.from_facebook(item) for item in sample_facebook_posts_results]

        with patch.object(FacebookProfileActor, "scrape_pages", return_value=[]) as mock_scrape:
            actor._enrich_user_author(posts, enrich_followers=True)

            # scrape_pages should have been called once with a single deduplicated URL
            mock_scrape.assert_called_once()
            page_urls = mock_scrape.call_args[0][0]
            assert len(page_urls) == 1
            assert "QroMunicipio" in page_urls[0]


class TestFacebookCommentsActorMapComment:
    """Test FacebookCommentsActor.map_comment() field mapping."""

    def test_maps_standard_fields(self, sample_facebook_comments):
        raw = sample_facebook_comments[0]
        mapped = FacebookCommentsActor.map_comment(raw)

        assert mapped["comment_text"] == "Excelente trabajo del municipio!"
        assert mapped["comment_author"] == "Maria Garcia"
        assert mapped["comment_timestamp"] == "2026-03-28T14:30:00.000Z"
        assert mapped["comment_likes"] == 5

    def test_handles_missing_fields(self):
        mapped = FacebookCommentsActor.map_comment({})
        assert mapped["comment_text"] is None
        assert mapped["comment_author"] is None
        assert mapped["comment_timestamp"] is None
        assert mapped["comment_likes"] is None


class TestFacebookCommentsActorGroupByPostUrl:
    """Test FacebookCommentsActor.group_by_post_url() grouping."""

    def test_groups_by_post_url(self, sample_facebook_comments):
        grouped = FacebookCommentsActor.group_by_post_url(sample_facebook_comments)

        url1 = "https://www.facebook.com/QroMunicipio/posts/pfbid02ABC123"
        url2 = "https://www.facebook.com/QroMunicipio/posts/pfbid02DEF456"
        assert len(grouped[url1]) == 2
        assert len(grouped[url2]) == 2

    def test_skips_error_entries(self):
        raw = [
            {"postUrl": "https://facebook.com/post/1", "text": "ok", "error": "failed"},
            {"postUrl": "https://facebook.com/post/1", "text": "good", "profileName": "User"},
        ]
        grouped = FacebookCommentsActor.group_by_post_url(raw)
        assert len(grouped["https://facebook.com/post/1"]) == 1

    def test_empty_input(self):
        grouped = FacebookCommentsActor.group_by_post_url([])
        assert len(grouped) == 0

    def test_falls_back_to_facebookUrl(self):
        raw = [{"facebookUrl": "https://facebook.com/post/1", "text": "hi", "profileName": "A"}]
        grouped = FacebookCommentsActor.group_by_post_url(raw)
        assert len(grouped["https://facebook.com/post/1"]) == 1


class TestEnrichComments:
    """Test FacebookPagePostsActor._enrich_comments()."""

    @pytest.fixture
    def actor(self):
        a = FacebookPagePostsActor.__new__(FacebookPagePostsActor)
        a.client = MagicMock()
        a.sources_manager = MagicMock()
        a.search_params = []
        a._filter_cache = {}
        a._save_filter_cache = MagicMock()
        return a

    def test_skips_when_get_comments_false(self, actor, sample_facebook_posts_results):
        posts = [FacebookPost.from_facebook(item) for item in sample_facebook_posts_results]
        result = actor._enrich_comments(posts, get_comments=False)
        assert len(result) == len(posts)
        actor.client.actor.assert_not_called()

    def test_scrapes_and_maps_comments(self, actor, sample_facebook_posts_results, sample_facebook_comments):
        posts = [FacebookPost.from_facebook(item) for item in sample_facebook_posts_results[:2]]
        # Set URLs matching the sample comments
        posts[0].data["url"] = "https://www.facebook.com/QroMunicipio/posts/pfbid02ABC123"
        posts[1].data["url"] = "https://www.facebook.com/QroMunicipio/posts/pfbid02DEF456"

        with patch.object(FacebookCommentsActor, "scrape_comments", return_value=sample_facebook_comments):
            result = actor._enrich_comments(posts, get_comments=True, max_comments=15)

        assert len(result[0].data["comments"]) == 2
        assert len(result[1].data["comments"]) == 2
        assert result[0].data["comments"][0]["comment_author"] == "Maria Garcia"
        assert result[1].data["comments"][0]["comment_author"] == "Ana Martinez"

    def test_empty_comments_for_unmatched_urls(self, actor, sample_facebook_posts_results, sample_facebook_comments):
        posts = [FacebookPost.from_facebook(item) for item in sample_facebook_posts_results[:1]]
        posts[0].data["url"] = "https://www.facebook.com/QroMunicipio/posts/pfbid02NOMATCH"

        with patch.object(FacebookCommentsActor, "scrape_comments", return_value=sample_facebook_comments):
            result = actor._enrich_comments(posts, get_comments=True)

        assert result[0].data["comments"] == []

    def test_passes_max_comments(self, actor, sample_facebook_posts_results):
        posts = [FacebookPost.from_facebook(item) for item in sample_facebook_posts_results[:1]]
        posts[0].data["url"] = "https://www.facebook.com/QroMunicipio/posts/pfbid02ABC123"

        with patch.object(FacebookCommentsActor, "scrape_comments", return_value=[]) as mock_scrape:
            actor._enrich_comments(posts, get_comments=True, max_comments=25)
            mock_scrape.assert_called_once_with(
                ["https://www.facebook.com/QroMunicipio/posts/pfbid02ABC123"],
                max_comments=25,
            )
