from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.actors.tiktok.posts import TikTokPostsActor
from src.models.tiktok_post import TikTokPost


@pytest.fixture
def actor():
    a = TikTokPostsActor.__new__(TikTokPostsActor)
    a.client = MagicMock()
    a.search_params_keywords = []
    a._filter_cache = {}
    a._save_filter_cache = MagicMock()
    return a


@pytest.fixture
def sample_tiktok_item():
    return {
        "id": "7423000000000000000",
        "text": "Valvoline en Queretaro con promocion especial",
        "createTimeISO": "2026-05-01T12:00:00.000Z",
        "webVideoUrl": "https://www.tiktok.com/@valvoline_mx/video/7423000000000000000",
        "videoUrl": "https://example.com/video.mp4",
        "coverUrl": "https://example.com/cover.jpg",
        "diggCount": 11,
        "shareCount": 2,
        "playCount": 300,
        "commentCount": 4,
        "textLanguage": "es",
        "authorMeta": {
            "name": "valvoline_mx",
            "nickName": "Valvoline Mexico",
            "signature": "Lubricantes y servicio",
            "fans": 12345,
        },
        "comments": [
            {
                "text": "Donde?",
                "createTimeISO": "2026-05-01T13:00:00.000Z",
                "diggCount": 1,
                "user": {"nickname": "Cliente"},
            }
        ],
    }


class TestTikTokSearchInput:
    def test_links_produce_post_urls(self, actor, sample_tiktok_item):
        with patch.object(actor, "run_actor", return_value=[sample_tiktok_item]) as mock_run:
            with patch.object(actor, "process_documents", side_effect=lambda docs, **kwargs: docs):
                actor.search(["https://www.tiktok.com/@u/video/1"], max_results=5)

        run_input = mock_run.call_args[0][0]
        assert run_input["postURLs"] == ["https://www.tiktok.com/@u/video/1"]
        assert "searchQueries" not in run_input
        assert "hashtags" not in run_input
        assert run_input["resultsPerPage"] == 5
        assert "commentsPerPost" not in run_input
        assert "maxRepliesPerComment" not in run_input
        assert "proxyCountryCode" not in run_input

    def test_hashtags_and_queries_share_search_run(self, actor, sample_tiktok_item):
        with patch.object(actor, "run_actor", return_value=[sample_tiktok_item]) as mock_run:
            with patch.object(actor, "process_documents", side_effect=lambda docs, **kwargs: docs):
                actor.search(["#lubricantes", "valvoline mexico"], max_results=7)

        run_input = mock_run.call_args[0][0]
        assert run_input["hashtags"] == ["lubricantes"]
        assert run_input["searchQueries"] == ["valvoline mexico"]
        assert run_input["resultsPerPage"] == 7

    def test_mixed_links_and_search_terms_run_separately(self, actor, sample_tiktok_item):
        with patch.object(actor, "run_actor", return_value=[sample_tiktok_item]) as mock_run:
            with patch.object(actor, "process_documents", side_effect=lambda docs, **kwargs: docs):
                results = actor.search([
                    "https://www.tiktok.com/@u/video/1",
                    "#lubricantes",
                    "valvoline",
                ])

        assert mock_run.call_count == 2
        assert mock_run.call_args_list[0][0][0]["postURLs"] == ["https://www.tiktok.com/@u/video/1"]
        assert mock_run.call_args_list[1][0][0]["hashtags"] == ["lubricantes"]
        assert mock_run.call_args_list[1][0][0]["searchQueries"] == ["valvoline"]
        assert len(results) == 2

    def test_country_id_maps_to_proxy_country_code(self, actor, sample_tiktok_item):
        with patch.object(actor, "run_actor", return_value=[sample_tiktok_item]) as mock_run:
            with patch.object(actor, "process_documents", side_effect=lambda docs, **kwargs: docs):
                actor.search(["valvoline"], country_id="_484")

        run_input = mock_run.call_args[0][0]
        assert run_input["proxyCountryCode"] == "MX"

    def test_unknown_country_id_raises(self, actor, sample_tiktok_item):
        with patch.object(actor, "run_actor", return_value=[sample_tiktok_item]):
            with pytest.raises(ValueError, match="country_id='_999'"):
                actor.search(["valvoline"], country_id="_999")

    def test_proxy_country_code_override_is_ignored(self, actor, sample_tiktok_item):
        with patch.object(actor, "run_actor", return_value=[sample_tiktok_item]) as mock_run:
            with patch.object(actor, "process_documents", side_effect=lambda docs, **kwargs: docs):
                actor.search(["valvoline"], proxyCountryCode="US", apify_input={"proxyCountryCode": "US"})

        run_input = mock_run.call_args[0][0]
        assert "proxyCountryCode" not in run_input

    def test_comments_and_actor_params_override_defaults(self, actor, sample_tiktok_item):
        with patch.object(actor, "run_actor", return_value=[sample_tiktok_item]) as mock_run:
            with patch.object(actor, "process_documents", side_effect=lambda docs, **kwargs: docs):
                actor.search(
                    ["valvoline"],
                    get_comments=True,
                    max_comments=9,
                    shouldDownloadVideos=True,
                )

        run_input = mock_run.call_args[0][0]
        assert "commentsPerPost" not in run_input
        assert "maxRepliesPerComment" not in run_input
        assert "proxyCountryCode" not in run_input
        assert run_input["shouldDownloadVideos"] is True

    def test_comment_actor_joins_by_video_url_after_filtering(self, actor, sample_tiktok_item):
        post_url = sample_tiktok_item["webVideoUrl"]
        other_url = "https://www.tiktok.com/@other/video/123"
        posts = [
            TikTokPost.from_tiktok({**sample_tiktok_item, "comments": []}),
            TikTokPost.from_tiktok({**sample_tiktok_item, "webVideoUrl": other_url, "comments": []}),
        ]
        comments = [
            {
                "videoWebUrl": post_url,
                "text": "y esa quien es?",
                "createTimeISO": "2026-05-24T05:01:26.000Z",
                "diggCount": 3,
                "uniqueId": "itz.sonrisa.bonit",
            },
            {
                "videoWebUrl": other_url,
                "text": "otro comentario",
                "createTimeISO": "2026-05-24T05:02:26.000Z",
                "diggCount": 1,
                "uniqueId": "otro.user",
            },
        ]
        actor.client.actor.return_value.call.return_value = {"defaultDatasetId": "comments-dataset"}
        actor.client.dataset.return_value.iterate_items.return_value = iter(comments)

        results = actor._enrich_comments(posts, get_comments=True, max_comments=5)

        actor.client.actor.assert_called_once_with("clockworks/tiktok-comments-scraper")
        run_input = actor.client.actor.return_value.call.call_args.kwargs["run_input"]
        assert run_input == {
            "commentsPerPost": 5,
            "excludePinnedPosts": False,
            "maxRepliesPerComment": 0,
            "postURLs": [post_url, other_url],
            "resultsPerPage": 5,
        }
        actor.client.dataset.assert_called_once_with("comments-dataset")
        assert results[0].data["comments"][0]["comment_text"] == "y esa quien es?"
        assert results[0].data["comments"][0]["comment_author"] == "itz.sonrisa.bonit"
        assert results[1].data["comments"][0]["comment_text"] == "otro comentario"

    def test_comment_actor_not_called_when_get_comments_false(self, actor, sample_tiktok_item):
        posts = [TikTokPost.from_tiktok({**sample_tiktok_item, "comments": []})]
        results = actor._enrich_comments(posts, get_comments=False)

        actor.client.actor.assert_not_called()
        actor.client.dataset.assert_not_called()
        assert results[0].data["comments"] == []

    def test_comment_actor_receives_only_documents_after_processing(self, actor, sample_tiktok_item):
        with patch.object(actor, "run_actor", return_value=[sample_tiktok_item]):
            with patch.object(actor, "process_documents", return_value=[]):
                actor.search(["valvoline"], get_comments=True, max_comments=5)

        actor.client.actor.assert_not_called()


class TestTikTokMapping:
    def test_raw_item_maps_to_post(self, sample_tiktok_item):
        post = TikTokPost.from_tiktok(sample_tiktok_item)

        assert post.data["type"] == "tiktok"
        assert post.data["post_type"] == "Video"
        assert post.data["body"] == "Valvoline en Queretaro con promocion especial"
        assert post.data["url"] == "https://www.tiktok.com/@valvoline_mx/video/7423000000000000000"
        assert post.data["profile_url"] == "https://www.tiktok.com/@valvoline_mx"
        assert post.data["author"] == "Valvoline Mexico"
        assert post.data["likes"] == 11
        assert post.data["shares"] == 2
        assert post.data["views"] == 300
        assert post.data["n_comments"] == 4
        assert post.data["website_visits"] == 12345
        assert post.data["media_urls"] == ["https://example.com/video.mp4", "https://example.com/cover.jpg"]
        assert post.data["comments"][0]["comment_text"] == "Donde?"

    def test_final_schema_accepts_tiktok_type(self, sample_tiktok_item):
        post = TikTokPost.from_tiktok(sample_tiktok_item)
        final = post.to_final_schema()

        assert final["type"] == "news"
        assert final["message"]["type"] == "tiktok"
        assert final["message"]["comments"][0]["comment_author"] == "Cliente"
