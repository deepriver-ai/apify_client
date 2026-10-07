"""Unit tests for the tasks.xlsx → crawl_tasks importer (mapping + validation).

No database: ``plan_rows`` is pure; the DB-backed --apply test lives in
``test_import_tasks_xlsx_db.py`` and only runs against a throwaway userdb.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.scripts import import_tasks_xlsx as imp

ACTORS = {a: {} for a in imp.ACTOR_PARAM_KEYS}
ORGS = {62, 93, 101, 102, 105, 106}


def _row(**kw) -> dict:
    row = {
        "task_id": "1", "actor_class": "facebook_keyword_search", "theme": "zitacuaro",
        "name": "n", "search_params": "zitácuaro", "followers": "", "country_id": "",
        "language": "", "min_date": "", "period": "w", "max_results": "50", "enabled": "1",
        "publish": "1", "get_comments": "", "max_comments": "", "not_keywords": "",
        "llm_filter_condition": "", "override_filters": "", "enrich_followers": "",
        "fetch_attached_url": "", "actor_params": "", "update_existing": "",
        "classify_users": "", "official_page": "",
    }
    row.update({k: str(v) for k, v in kw.items()})
    return row


def _plan(*rows, **kw):
    numbered = [(i + 2, r) for i, r in enumerate(rows)]
    kw.setdefault("known_orgs", ORGS)
    return imp.plan_rows(numbered, ACTORS, **kw)


# --- ids / status -----------------------------------------------------------


def test_duplicate_legacy_ids_split_first_keeps_key():
    rows = [(2, {"task_id": "35"}), (3, {"task_id": "36"}), (4, {"task_id": "35"}),
            (5, {"task_id": "35"}), (6, {"task_id": ""})]
    assert imp.split_duplicate_ids(rows) == {2: "35", 3: "36", 4: "35~2", 5: "35~3", 6: "row6"}


@pytest.mark.parametrize("theme,enabled,lid,expected", [
    ("totalenergies", True, "1", (62, "active")),
    ("totalenergies_guadalupe", True, "gd1", (62, "active")),
    ("totalenergies", False, "65", (62, "paused")),
    ("zitacuaro", True, "146", (102, "active")),
    ("pinal_amoles", True, "159", (106, "active")),
    ("pinal_amoles", False, "145", (106, "retired")),  # past informe override
    ("sjdr", True, "121", (101, "retired")),           # retired theme wins over enabled
    ("zona_fest", False, "87", (101, "retired")),
    ("morelia_descifra", False, "125", (105, "retired")),
    ("queretaro", False, "4", (93, "retired")),
    ("orizaba", False, "9", (None, None)),
    ("mc", False, "100", (None, None)),
    ("unknown_theme", True, "x", (None, None)),
    (None, True, "x", (None, None)),
])
def test_decide_org_status(theme, enabled, lid, expected):
    org, status, reason = imp.decide_org_status(theme, enabled, lid)
    assert (org, status) == expected
    if org is None:
        assert reason
    if status in ("paused", "retired"):
        assert reason


# --- row mapping --------------------------------------------------------------


def test_values_mapping_and_sorting():
    [p] = _plan(_row(search_params="b, a ,b", not_keywords="x|y", language="Spanish",
                     followers="12k", actor_params='{"recent_posts": false}'))
    assert p.action == "insert"
    v = p.values
    assert v["search_params"] == ["a", "b"]  # sorted + de-duplicated
    assert v["not_keywords"] == ["x", "y"]
    assert v["language"] == "es"
    assert v["org_id"] == 102 and v["status"] == "active" and v["status_reason"] is None
    assert v["legacy_theme"] == "zitacuaro" and v["legacy_task_id"] == "1"
    assert v["metadata"]["followers"] == "12k"
    assert v["metadata"]["import"]["excel_row"] == 2
    assert v["actor_params"] == {"recent_posts": False}
    assert set(imp.INSERT_COLS) <= set(v)


def test_max_results_clamped_to_ceiling():
    [p] = _plan(_row(max_results="1500"))
    assert p.action == "insert"
    assert p.values["max_results"] == 1000
    assert p.values["metadata"]["import"]["max_results_xlsx"] == 1500
    assert any("clamped" in w for w in p.warnings)


def test_max_results_blank_defaults_and_zero_invalid():
    [ok] = _plan(_row(max_results=""))
    assert ok.values["max_results"] == 30
    [bad] = _plan(_row(max_results="0"))
    assert bad.action == "invalid"


def test_no_window_allowed():
    [p] = _plan(_row(period=""))
    assert p.action == "insert" and p.values["period"] is None and p.values["min_date"] is None


def test_min_date_and_period_both_set_is_invalid():
    [p] = _plan(_row(min_date="2026-09-01", period="w"))
    assert p.action == "invalid"


def test_min_date_naive_becomes_utc():
    [p] = _plan(_row(min_date="2026-09-01", period=""))
    assert p.values["min_date"].tzinfo is not None


def test_invalid_period_warns_and_stores_null():
    [p] = _plan(_row(period="x"))
    assert p.action == "insert" and p.values["period"] is None
    assert any("period" in w for w in p.warnings)


def test_empty_search_params_invalid():
    [p] = _plan(_row(search_params=" , "))
    assert p.action == "invalid" and "search_params empty" in p.reason


def test_unknown_actor_class_invalid():
    [p] = _plan(_row(actor_class="GROUPS"))
    assert p.action == "invalid" and "crawl_actor_classes" in p.reason


def test_bad_json_actor_params_invalid_for_kept_theme_skip_for_dropped():
    [p] = _plan(_row(actor_params="{not json"))
    assert p.action == "invalid"
    [q] = _plan(_row(actor_params="{not json", theme="mc"))
    assert q.action == "skip"


def test_actor_params_must_be_object():
    errors, _ = imp.validate_actor_params("google_news", ["x"])
    assert errors


def test_google_news_timeframe_and_tiktok_cap():
    [p] = _plan(_row(actor_class="google_news", actor_params='{"timeframe": "2d"}'))
    assert p.action == "invalid" and "timeframe" in p.reason
    [q] = _plan(_row(actor_class="tiktok_posts", actor_params='{"apify_input": {"maxItems": 5000}}'))
    assert q.action == "invalid" and "cap" in q.reason


def test_unknown_actor_param_key_is_warning_only():
    [p] = _plan(_row(actor_class="facebook_page_posts", search_params="https://fb.com/x",
                     actor_params='{"keyword_search": true}'))
    assert p.action == "insert"
    assert any("not read by facebook_page_posts" in w for w in p.warnings)


def test_page_posts_like_thresholds_are_known_keys():
    """facebook_page_posts reads get_comments_after_likes / enrich_author_after_likes
    (crawl-worker branch, ApifyActor._gate_by_likes) — no 'not read by' warning."""
    [p] = _plan(_row(actor_class="facebook_page_posts", search_params="https://fb.com/x",
                     actor_params='{"get_comments_after_likes": 10, "enrich_author_after_likes": 30}'))
    assert p.action == "insert"
    assert not any("not read by" in w for w in p.warnings)


def test_params_schema_from_db_takes_precedence():
    _, w = imp.validate_actor_params("google_news", {"foo": 1}, {"properties": {"foo": {}}})
    assert not w


def test_missing_org_invalid():
    [p] = _plan(_row(), known_orgs={62})
    assert p.action == "invalid" and "org 102" in p.reason


# --- dedupe / idempotency ------------------------------------------------------


def test_live_key_conflict_within_file_is_order_insensitive():
    a, b = _plan(_row(task_id="1", search_params="a,b"), _row(task_id="2", search_params="b,a"))
    assert a.action == "insert"
    assert b.action == "conflict" and "'1'" in b.reason


def test_retired_rows_do_not_hold_the_live_key():
    a, b = _plan(_row(task_id="1", theme="sjdr"), _row(task_id="2", theme="zona_fest"))
    assert a.action == b.action == "insert"  # both org 101, same key, both retired


def test_paused_rows_hold_the_live_key():
    a, b = _plan(_row(task_id="1", enabled="0"), _row(task_id="2"))
    assert a.values["status"] == "paused" and b.action == "conflict"


def test_conflict_with_existing_db_row():
    live = {(102, "facebook_keyword_search", ("zitácuaro",)): "db:7"}
    [p] = _plan(_row(), existing_live_keys=live)
    assert p.action == "conflict" and "db:7" in p.reason


def test_existing_legacy_id_is_skipped():
    a, b = _plan(_row(task_id="35"), _row(task_id="35", search_params="other"),
                 existing_legacy={"35"})
    assert a.action == "exists"
    assert b.action == "insert" and b.legacy_task_id == "35~2"


# --- ids.json -------------------------------------------------------------------


def _customer(tmp: Path, slug: str, theme: str, org: int, crawl_ids: dict | None = None) -> Path:
    d = tmp / slug
    d.mkdir()
    (d / "customer.yaml").write_text(f"org:\n  id: {org}\ntheme: {theme}\n", encoding="utf-8")
    if crawl_ids is not None:
        (d / "ids.json").write_text(json.dumps({"crawl_task_ids": crawl_ids}), encoding="utf-8")
    return d


def test_check_ids_json_flags_stale_ids(tmp_path):
    _customer(tmp_path, "zitacuaro", "zitacuaro", 102, {"124": "Zitácuaro keyword", "146": "Zitácuaro keyword"})
    _customer(tmp_path, "pinal_amoles", "pinal_amoles", 999, {})
    rows = [(2, _row(task_id="124", theme="orizaba", name="El Chirimoyo")),
            (3, _row(task_id="146", theme="zitacuaro", name="Zitácuaro keyword"))]
    warns = imp.check_ids_json(tmp_path, rows)
    assert any("zitacuaro" in w and "124 (xlsx theme 'orizaba')" in w and "146" not in w for w in warns)
    assert any("pinal_amoles: customer.yaml org 999" in w for w in warns)


def test_write_ids_records_section_and_keeps_existing(tmp_path):
    d = _customer(tmp_path, "zitacuaro", "zitacuaro", 102, {"146": "Zitácuaro keyword"})
    _customer(tmp_path, "orizaba", "orizaba", 47)
    imp.write_ids(tmp_path, {"zitacuaro": {"146": 11}, "sjdr": {"106": 3}}, dry_run=True)
    assert "crawl_task_db_ids" not in json.loads((d / "ids.json").read_text())
    written = imp.write_ids(tmp_path, {"zitacuaro": {"146": 11}, "sjdr": {"106": 3}})
    assert written == [d / "ids.json"]
    ids = json.loads((d / "ids.json").read_text())
    assert ids["crawl_task_db_ids"] == {"146": 11}
    assert ids["crawl_task_ids"] == {"146": "Zitácuaro keyword"}
    assert "crawl_task_db_ids" in ids["_updated"]
    assert not (tmp_path / "orizaba" / "ids.json").exists()


# --- the real workbook (read-only) ---------------------------------------------------


@pytest.mark.skipif(not imp.DEFAULT_XLSX.exists(), reason="tasks.xlsx not present")
def test_real_workbook_plan_has_no_failures():
    rows = imp.read_xlsx(imp.DEFAULT_XLSX)
    planned = imp.plan_rows(rows, ACTORS, known_orgs=ORGS)
    by = {p.legacy_task_id: p for p in planned}
    assert not [p for p in planned if p.action in ("invalid", "conflict")]
    assert all(p.action == "skip" for p in planned if p.theme in imp.SKIP_THEMES)
    assert by["145"].status == "retired"
    legacy = [p.legacy_task_id for p in planned]
    assert len(legacy) == len(set(legacy))  # duplicate ids split
    assert all(p.values["max_results"] <= 1000 for p in planned if p.action == "insert")
