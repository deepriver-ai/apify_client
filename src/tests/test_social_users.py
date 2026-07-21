"""Tests for the SocialUsers store and two-tier identity normalization."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.models.social_users import (
    CLASSIFIER_CLASSES,
    IDENTITY_CONFIDENCE,
    TIER_NAME,
    TIER_PROFILE,
    SocialUsers,
    account_id,
    build_record,
    normalize_name,
    normalize_profile_url,
    strip_accents,
)


class TestNameNormalization:
    def test_casefold_and_whitespace(self):
        assert normalize_name("  Carlos   MARTINEZ ") == "carlos martinez"

    def test_strip_accents(self):
        assert normalize_name("Ma Elena Vázquez") == "ma elena vazquez"
        assert strip_accents("ñoño áéíóú") == "nono aeiou"

    def test_empty(self):
        assert normalize_name("") == ""
        assert normalize_name(None) == ""


class TestProfileUrlNormalization:
    def test_scheme_www_slash_stripped(self):
        a = normalize_profile_url("https://www.facebook.com/presidenciaSJR/")
        b = normalize_profile_url("http://facebook.com/presidenciaSJR")
        assert a == b == "facebook.com/presidenciasjr"

    def test_query_and_fragment_dropped(self):
        assert normalize_profile_url(
            "https://facebook.com/page?ref=bookmarks#top"
        ) == "facebook.com/page"

    def test_empty(self):
        assert normalize_profile_url(None) == ""


class TestAccountId:
    def test_profile_tier_id_is_url_keyed(self):
        _id = account_id(TIER_PROFILE, profile_url="https://www.facebook.com/X/")
        assert _id == "profile:facebook.com/x"

    def test_name_tier_id_is_network_name_keyed(self):
        _id = account_id(TIER_NAME, network="Facebook", name="Carlos Martínez")
        assert _id == "name:facebook:carlos martinez"

    def test_name_tier_collapses_accent_and_case(self):
        assert account_id(TIER_NAME, network="facebook", name="CARLOS  martinez") == \
               account_id(TIER_NAME, network="facebook", name="Carlos Martínez")


class TestBuildRecord:
    def test_profile_record_identity(self):
        rec = build_record(
            tier=TIER_PROFILE, network="facebook",
            display_name="Presidencia", profile_url="https://facebook.com/presidenciaSJR",
        )
        assert rec["_id"] == "profile:facebook.com/presidenciasjr"
        assert rec["profile_url"] == "facebook.com/presidenciasjr"
        assert rec["normalized_name"] is None
        assert rec["identity_confidence"] == IDENTITY_CONFIDENCE[TIER_PROFILE]

    def test_name_record_identity_lower_ceiling(self):
        rec = build_record(tier=TIER_NAME, network="facebook", display_name="Carlos Martínez")
        assert rec["_id"] == "name:facebook:carlos martinez"
        assert rec["normalized_name"] == "carlos martinez"
        assert rec["identity_confidence"] == IDENTITY_CONFIDENCE[TIER_NAME]
        assert rec["identity_confidence"] < IDENTITY_CONFIDENCE[TIER_PROFILE]

    def test_unknown_class_coerced(self):
        rec = build_record(tier=TIER_NAME, network="facebook",
                           display_name="x", classification="bot")
        assert rec["classification"] == "organico"  # bot is not a stored class

    def test_all_stored_classes_valid(self):
        assert "bot" not in CLASSIFIER_CLASSES
        assert "influencer" not in CLASSIFIER_CLASSES
        assert "organico" in CLASSIFIER_CLASSES

    def test_evidence_sample_capped(self):
        rec = build_record(tier=TIER_NAME, network="facebook", display_name="x",
                           evidence_sample=[str(i) for i in range(10)])
        assert len(rec["evidence_sample"]) == 5


class TestSocialUsersStore:
    def test_get_by_profile_url(self):
        coll = MagicMock()
        coll.find_one.return_value = {"_id": "profile:facebook.com/x", "classification": "organico"}
        store = SocialUsers(collection=coll)
        rec = store.get_by_profile_url("https://www.facebook.com/x/")
        coll.find_one.assert_called_once_with({"_id": "profile:facebook.com/x"})
        assert rec["classification"] == "organico"

    def test_get_degrades_on_error(self):
        coll = MagicMock()
        coll.find_one.side_effect = RuntimeError("mongo down")
        store = SocialUsers(collection=coll)
        assert store.get("profile:x") is None

    def test_upsert_keyed_on_id(self):
        coll = MagicMock()
        store = SocialUsers(collection=coll)
        store.upsert({"_id": "name:facebook:x", "classification": "politico"})
        args, kwargs = coll.update_one.call_args
        assert args[0] == {"_id": "name:facebook:x"}
        assert kwargs.get("upsert") is True

    def test_needs_reclassification_new_account(self):
        coll = MagicMock()
        coll.find_one.return_value = None
        store = SocialUsers(collection=coll)
        assert store.needs_reclassification({"_id": "x", "evidence_as_of": "2026-07-10"})

    def test_needs_reclassification_newer_evidence(self):
        coll = MagicMock()
        coll.find_one.return_value = {
            "_id": "x", "classification": "organico", "evidence_as_of": "2026-07-01T00:00:00",
        }
        store = SocialUsers(collection=coll)
        assert store.needs_reclassification({"_id": "x", "evidence_as_of": "2026-07-15T00:00:00"})

    def test_needs_reclassification_no_new_evidence(self):
        coll = MagicMock()
        coll.find_one.return_value = {
            "_id": "x", "classification": "organico", "evidence_as_of": "2026-07-15T00:00:00",
        }
        store = SocialUsers(collection=coll)
        assert not store.needs_reclassification({"_id": "x", "evidence_as_of": "2026-07-15T00:00:00"})
