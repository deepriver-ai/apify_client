"""Tests for FacebookPostCommentsActor (comments-on-demand)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.actors import ACTOR_REGISTRY, get_actor
from src.actors.facebook.comments import FacebookCommentsActor
from src.actors.facebook.post_comments import (
    FacebookPostCommentsActor,
    _normalize_post_url,
)

VIDEO_URL = "https://www.facebook.com/robertocabrerav/videos/841969948996525"
POST_A = "https://www.facebook.com/QroMunicipio/posts/pfbid02ABC123"
POST_B = "https://www.facebook.com/QroMunicipio/posts/pfbid02DEF456"


@pytest.fixture
def actor() -> FacebookPostCommentsActor:
    return FacebookPostCommentsActor(client=MagicMock())


class TestRegistry:
    def test_registered(self):
        assert "facebook_post_comments" in ACTOR_REGISTRY
        assert ACTOR_REGISTRY["facebook_post_comments"] is FacebookPostCommentsActor

    def test_get_actor_instantiates(self):
        assert isinstance(get_actor("facebook_post_comments"), FacebookPostCommentsActor)


class TestNormalizeUrl:
    def test_strips_scheme_www_query_slash(self):
        assert _normalize_post_url(
            "https://www.facebook.com/QroMunicipio/posts/pfbid02ABC123/?foo=1"
        ) == "facebook.com/QroMunicipio/posts/pfbid02ABC123"

    def test_matches_variants(self):
        a = _normalize_post_url("http://facebook.com/QroMunicipio/posts/pfbid02ABC123")
        b = _normalize_post_url("https://www.facebook.com/QroMunicipio/posts/pfbid02ABC123/")
        assert a == b

    def test_empty(self):
        assert _normalize_post_url("") == ""


class TestSearchBuildsDocs:
    """search() builds one FacebookPost per input URL with grouped comments."""

    def test_grouping_and_url_is_doc_id(self, actor, sample_facebook_comments):
        with patch.object(
            FacebookCommentsActor, "scrape_comments", return_value=sample_facebook_comments
        ):
            docs = actor.search([POST_A, POST_B], max_comments=15)

        assert len(docs) == 2
        # url must equal the input URL exactly (it is the ES _id / gp3 merge key)
        assert docs[0].data["url"] == POST_A
        assert docs[1].data["url"] == POST_B
        # comments grouped to the right post
        assert len(docs[0].data["comments"]) == 2
        assert len(docs[1].data["comments"]) == 2
        assert docs[0].data["comments"][0]["comment_author"] == "Maria Garcia"
        assert docs[1].data["comments"][0]["comment_author"] == "Ana Martinez"

    def test_comment_normalization(self, actor, sample_facebook_comments):
        with patch.object(
            FacebookCommentsActor, "scrape_comments", return_value=sample_facebook_comments
        ):
            docs = actor.search([POST_A], max_comments=15)

        c = docs[0].data["comments"][0]
        # mapped to the common comment schema keys
        assert set(c.keys()) == {
            "comment_text",
            "comment_author",
            "comment_timestamp",
            "comment_likes",
        }
        assert c["comment_text"] == "Excelente trabajo del municipio!"
        assert c["comment_likes"] == 5

    def test_n_comments_left_unset(self, actor, sample_facebook_comments):
        """n_comments is intentionally unset so gp3 keeps the post's truer
        existing total (the scraped count is only a sample capped at
        max_comments and would clobber it downward)."""
        with patch.object(
            FacebookCommentsActor, "scrape_comments", return_value=sample_facebook_comments
        ):
            docs = actor.search([POST_A, POST_B])
        assert docs[0].data["n_comments"] is None
        assert docs[1].data["n_comments"] is None

    def test_source_author_from_url_slug(self, actor):
        with patch.object(FacebookCommentsActor, "scrape_comments", return_value=[]):
            docs = actor.search([VIDEO_URL])
        assert docs[0].data["source"] == "robertocabrerav"
        assert docs[0].data["author"] == "robertocabrerav"
        assert docs[0].data["type"] == "facebook"
        # no post engagement fabricated — left unset so gp3 preserves existing
        assert docs[0].data["n_comments"] is None
        assert docs[0].data["likes"] is None
        assert docs[0].data["shares"] is None

    def test_max_comments_forwarded_as_results_limit(self, actor):
        """max_comments must reach the scraper's resultsLimit."""
        actor.client.actor.return_value.call.return_value = {"defaultDatasetId": "ds"}
        actor.client.dataset.return_value.iterate_items.return_value = []
        actor.search([VIDEO_URL], max_comments=30)
        run_input = actor.client.actor.return_value.call.call_args.kwargs["run_input"]
        assert run_input["resultsLimit"] == 30
        assert run_input["startUrls"] == [{"url": VIDEO_URL}]

    def test_single_url_fallback_when_scraper_canonicalizes(self, actor):
        """Single URL: comments echoed under a differing postUrl still attach."""
        raw = [
            {
                "text": "great",
                "profileName": "X",
                "date": "2026-03-28T14:30:00.000Z",
                "likesCount": 1,
                # scraper echoes a canonical URL differing from the input
                "postUrl": "https://m.facebook.com/robertocabrerav/videos/841969948996525",
            }
        ]
        with patch.object(FacebookCommentsActor, "scrape_comments", return_value=raw):
            docs = actor.search([VIDEO_URL], max_comments=30)
        assert docs[0].data["url"] == VIDEO_URL
        assert len(docs[0].data["comments"]) == 1

    def test_normalized_match_multi_url(self, actor):
        """Multi-URL: trailing-slash/query differences still match by normalization."""
        raw = [
            {
                "text": "a",
                "profileName": "A",
                "date": "2026-03-28T14:30:00.000Z",
                "likesCount": 0,
                "postUrl": POST_A + "/?comment_id=1",
            },
            {
                "text": "b",
                "profileName": "B",
                "date": "2026-03-28T14:30:00.000Z",
                "likesCount": 0,
                "postUrl": POST_B + "/",
            },
        ]
        with patch.object(FacebookCommentsActor, "scrape_comments", return_value=raw):
            docs = actor.search([POST_A, POST_B])
        assert len(docs[0].data["comments"]) == 1
        assert len(docs[1].data["comments"]) == 1

    def test_empty_search_params(self, actor):
        with patch.object(FacebookCommentsActor, "scrape_comments") as m:
            docs = actor.search([])
        assert docs == []
        m.assert_not_called()


class TestFinalSchema:
    def test_to_final_schema_validates(self, actor, sample_facebook_comments):
        with patch.object(
            FacebookCommentsActor, "scrape_comments", return_value=sample_facebook_comments
        ):
            docs = actor.search([POST_A])
        final = docs[0].to_final_schema()
        assert final is not None
        assert final["type"] == "news"  # envelope
        msg = final["message"]
        assert msg["type"] == "facebook"  # platform
        assert msg["url"] == POST_A
        assert msg["source"] == "QroMunicipio"
        assert "comments" in msg
        assert len(msg["comments"]) == 2
        # comment_id default is generated per comment
        assert all(c.get("comment_id") for c in msg["comments"])


class TestBypassesExistingElasticsearchFilter:
    """The whole point is to update docs that already exist in ES, so the
    existing-doc filter must NOT run (search() never calls process_documents)."""

    def test_process_documents_not_called(self, actor, sample_facebook_comments):
        with patch.object(
            FacebookCommentsActor, "scrape_comments", return_value=sample_facebook_comments
        ), patch.object(
            FacebookPostCommentsActor, "process_documents"
        ) as mock_pipeline:
            docs = actor.search([POST_A, POST_B])
        mock_pipeline.assert_not_called()
        assert len(docs) == 2

    def test_existing_es_filter_not_invoked(self, actor, sample_facebook_comments):
        with patch.object(
            FacebookCommentsActor, "scrape_comments", return_value=sample_facebook_comments
        ), patch.object(
            FacebookPostCommentsActor, "_filter_existing_in_elasticsearch"
        ) as mock_filter:
            actor.search([POST_A])
        mock_filter.assert_not_called()
