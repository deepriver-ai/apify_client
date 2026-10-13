"""Crawl worker against a real throwaway userdb, connected as ``crawler_worker``.

Proves the SQL in src/worker/db.py and the usage_client ledger traffic (admission reads,
``usage_events`` / ``usage_blocks`` inserts) run within the role's grants (design Q5,
rate_limiting.md §2.2). Skipped unless both DSNs are set — never point them at stg/prod.
The usage tables come from the pending migrations until they are folded into schema.sql:

    M=media-backend-paid/docs/db/migrations
    dev/testdb.sh proto06_test_crawl_usage media-backend-paid/db/user_db \\
        $M/2026-10-06_usage_ledger_userdb.sql $M/2026-10-06_usage_admin_rbac_userdb.sql \\
        $M/2026-10-06_usage_org_view_rbac_userdb.sql $M/2026-10-07_crawler_usage_grants_userdb.sql
    psql ... -d proto06_test_crawl_usage -v crawler_password=localcrawler \\
        -f $M/2026-10-06_crawl_worker_role_userdb.sql
    TEST_CRAWL_USERDB_ADMIN_URI=postgresql://postgres:postgres@localhost:5334/proto06_test_crawl_usage \\
    TEST_CRAWL_USERDB_URI=postgresql://crawler_worker:localcrawler@localhost:5334/proto06_test_crawl_usage \\
        python -m pytest src/tests/test_crawl_worker_db.py -v
"""
from __future__ import annotations

import json
import os
import uuid
from typing import Any, Dict, List

import pytest

ADMIN_URI = os.getenv("TEST_CRAWL_USERDB_ADMIN_URI")
WORKER_URI = os.getenv("TEST_CRAWL_USERDB_URI")

pytestmark = pytest.mark.skipif(not (ADMIN_URI and WORKER_URI), reason="TEST_CRAWL_USERDB_* not set")

for _uri in (ADMIN_URI, WORKER_URI):
    if _uri and "_test" not in _uri.rsplit("/", 1)[-1]:
        raise RuntimeError(f"refusing: test DSN must name a *_test* database, got {_uri!r}")

psycopg = pytest.importorskip("psycopg")


def _admin(sql: str, params=()) -> List[Dict[str, Any]]:
    from psycopg.rows import dict_row

    with psycopg.connect(ADMIN_URI, row_factory=dict_row, autocommit=True) as conn:
        cur = conn.execute(sql, params)
        return list(cur.fetchall()) if cur.description else []


@pytest.fixture
def seeded(tmp_path):
    tag = uuid.uuid4().hex[:8]
    org = _admin("INSERT INTO orgs (public_id, name, contact_email) VALUES (%s, %s, %s) RETURNING id",
                 (f"org-{tag}", f"crawl worker test {tag}", f"{tag}@example.test"))[0]["id"]
    user = _admin("INSERT INTO users (public_id, email, password) VALUES (%s, %s, 'x') RETURNING id",
                  (f"user-{tag}", f"{tag}@example.test"))[0]["id"]
    task = _admin(
        """INSERT INTO crawl_tasks (org_id, actor_class, search_params, max_results, status, actor_params, legacy_task_id)
           VALUES (%s, 'facebook_page_posts', %s, 30, 'active', '{"add_text_from_images": false}', NULL)
           RETURNING task_id""",
        (org, json.dumps([f"https://www.facebook.com/QroMunicipio/?t={tag}"])))[0]["task_id"]
    run = _admin(
        """INSERT INTO crawl_runs (kind, task_id, org_id, trigger, requested_by, status, params, max_items, trace_id, queued_at)
           VALUES ('crawl', %s, %s, 'schedule', %s, 'queued', '{"override_filters": false}', 50, %s, now())
           RETURNING run_id""", (task, org, user, f"crawl-test-{tag}"))[0]["run_id"]
    _admin("INSERT INTO crawl_schedules (task_id, every, next_run_at) VALUES (%s, '1 day', now())", (task,))
    yield {"org": org, "user": user, "task": task, "run": run, "tag": tag}
    _admin("DELETE FROM usage_events WHERE org_id = %s", (org,))
    _admin("DELETE FROM usage_blocks WHERE org_id = %s", (org,))
    _admin("DELETE FROM usage_limits WHERE org_id = %s", (org,))
    _admin("DELETE FROM crawl_runs WHERE org_id = %s", (org,))
    _admin("DELETE FROM crawl_comment_requests WHERE org_id = %s", (org,))
    _admin("DELETE FROM crawl_tasks WHERE org_id = %s", (org,))
    _admin("DELETE FROM users WHERE id = %s", (user,))
    _admin("DELETE FROM orgs WHERE id = %s", (org,))


def _db(tmp_path):
    from src.worker.db import CrawlDB

    return CrawlDB(WORKER_URI, spool_path=str(tmp_path / "spool.jsonl"))


def _ledger(tmp_path):
    """The usage_client ledger as the worker builds it: same role (crawler_worker)."""
    from usage_client.ledger import Ledger

    return Ledger(WORKER_URI, spool_dir=tmp_path / "usage_spool")


def _deps(tmp_path, apify, published=None):
    from src.actors import get_actor
    from src.worker.runner import WorkerDeps

    return WorkerDeps(db=_db(tmp_path), apify_client_factory=lambda: apify,
                      actor_factory=lambda ac, client: get_actor(ac, client=client),
                      publish=(published if published is not None else []).append, worker_name="pytest",
                      output_dir=str(tmp_path / "runs"), usage_ledger=_ledger(tmp_path))


def _msg(seeded, **over):
    msg = {"type": "crawl_run", "run_id": seeded["run"], "kind": "crawl", "task_id": seeded["task"],
           "org_id": seeded["org"], "user_id": seeded["user"], "trace_id": f"crawl-test-{seeded['tag']}",
           "max_items": 50, "max_cost_usd": 0.2, "dry_run": True, "params": {}}
    msg.update(over)
    return msg


def test_connected_as_crawler_worker():
    from psycopg.rows import dict_row

    with psycopg.connect(WORKER_URI, row_factory=dict_row) as conn:
        assert conn.execute("SELECT current_user AS u").fetchone()["u"] == "crawler_worker"


def test_lifecycle_sql_within_grants(seeded, tmp_path):
    db = _db(tmp_path)
    run_id = seeded["run"]
    claimed = db.claim_run(run_id, "pytest")
    assert claimed["status"] == "running" and claimed["worker"] == "pytest"
    assert db.claim_run(run_id, "pytest") is None  # redelivery: not claimable
    assert db.heartbeat(run_id) == 1
    assert db.get_task(seeded["task"])["org_id"] == seeded["org"]
    assert db.get_actor_class("facebook_page_posts")["cap_scope"] == "per_param"

    row = {"run_id": run_id, "purpose": "search", "apify_actor_id": "apify/facebook-posts-scraper",
           "apify_run_id": f"apifyrun-{seeded['tag']}", "apify_status": "SUCCEEDED", "items": 4,
           "cost_usd": 0.0123, "estimated": False, "usage": {"COMPUTE_UNITS": 0.1},
           "usage_event_id": None, "started_at": None, "finished_at": None}
    db.insert_call(row)
    db.insert_call(row)  # idempotent on apify_run_id
    assert not os.path.exists(db.spool_path) or open(db.spool_path).read() == ""

    counters = {"items_paid": 4, "items_already_seen": 1, "items_new": 3, "cost_usd": 0.0123,
                "items_dropped_llm": 0}
    assert db.finish_run(run_id, "succeeded", None, None, counters, {"funnel": {"docs_in": 4}}) is True
    assert db.finish_run(run_id, "failed", "late", None, counters, {}) is False   # no longer running
    assert db.update_schedule_after_run(seeded["task"], run_id, True) == 1

    r = _admin("SELECT * FROM crawl_runs WHERE run_id = %s", (run_id,))[0]
    assert r["status"] == "succeeded" and r["finished_at"] is not None
    assert float(r["overlap_rate"]) == pytest.approx(0.25) and float(r["yield_rate"]) == pytest.approx(0.75)
    assert r["stats"]["funnel"]["docs_in"] == 4
    calls = _admin("SELECT * FROM crawl_run_calls WHERE run_id = %s", (run_id,))
    assert len(calls) == 1 and float(calls[0]["cost_usd"]) == pytest.approx(0.0123)
    s = _admin("SELECT * FROM crawl_schedules WHERE task_id = %s", (seeded["task"],))[0]
    assert s["last_run_id"] == run_id and s["last_success_at"] is not None and s["consecutive_failures"] == 0


def test_worker_role_cannot_touch_task_definitions(seeded, tmp_path):
    db = _db(tmp_path)
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        db._execute("UPDATE public.crawl_tasks SET status = 'paused' WHERE task_id = %s", (seeded["task"],))


def test_spool_replays_into_real_db(seeded, tmp_path):
    from src.worker.db import CrawlDB

    run_id = seeded["run"]
    _db(tmp_path).claim_run(run_id, "pytest")
    creds, host = WORKER_URI.split("@", 1)
    down = CrawlDB(creds.rsplit(":", 1)[0] + ":wrong@" + host,  # same DB, wrong password
                   spool_path=str(tmp_path / "spool.jsonl"), connect_timeout=3)
    down.insert_call({"run_id": run_id, "purpose": "search", "apify_actor_id": "a",
                      "apify_run_id": f"spooled-{seeded['tag']}", "cost_usd": 0.5})
    down.finish_run(run_id, "succeeded", None, None, {"cost_usd": 0.5, "items_paid": 1}, {})
    assert len(open(tmp_path / "spool.jsonl").read().splitlines()) == 2
    assert _db(tmp_path).replay_spool() == 2
    r = _admin("SELECT status, cost_usd FROM crawl_runs WHERE run_id = %s", (run_id,))[0]
    assert r["status"] == "succeeded" and float(r["cost_usd"]) == 0.5
    assert _admin("SELECT count(*) AS n FROM crawl_run_calls WHERE apify_run_id = %s",
                  (f"spooled-{seeded['tag']}",))[0]["n"] == 1


def test_handle_message_end_to_end_against_db(seeded, tmp_path, sample_facebook_posts_results, monkeypatch):
    """Queue message → claim → actor (fake Apify, metered through usage_client as
    crawler_worker) → crawl_run_calls + usage_events + funnel → succeeded, no publish."""
    from unittest.mock import patch

    from src.tests.test_crawl_worker import FakeApify
    from src.worker.runner import ACK, handle_message

    monkeypatch.setattr("src.actors.actor.FILTER_CACHE_PATH", str(tmp_path / "fc.json"))
    _admin("UPDATE crawl_runs SET dry_run = true, query_id = NULL WHERE run_id = %s", (seeded["run"],))
    apify = FakeApify({"apify/facebook-posts-scraper": sample_facebook_posts_results}, cost_per_call=0.0104)
    published: list = []
    deps = _deps(tmp_path, apify, published)
    with patch("src.actors.actor.ApifyActor._existing_news_ids",
               return_value={sample_facebook_posts_results[0]["url"]}):
        assert handle_message(json.dumps(_msg(seeded)), deps) == ACK
    r = _admin("SELECT * FROM crawl_runs WHERE run_id = %s", (seeded["run"],))[0]
    assert r["status"] == "succeeded", r["error"]
    assert (r["items_paid"], r["items_already_seen"], r["items_new"]) == (4, 1, 3)
    assert float(r["cost_usd"]) == pytest.approx(0.0104) and r["output_path"]
    assert r["stats"]["effective_params"]["task_id"] == f"db:{seeded['task']}"
    assert published == []
    (call,) = _admin("SELECT * FROM crawl_run_calls WHERE run_id = %s", (seeded["run"],))
    # one usage_events row per Apify run, linked from crawl_run_calls.usage_event_id (F1)
    (ev,) = _admin("SELECT * FROM usage_events WHERE org_id = %s", (seeded["org"],))
    assert call["usage_event_id"] == ev["event_id"] == \
        f"apify-client:crawl-run:{seeded['run']}:{seeded['org']}:crawl:{call['apify_run_id']}"
    assert (ev["tool"], ev["kind"], ev["unit"], ev["service"], ev["scope"]) == \
        ("apify", "crawl", "items", "apify-client", "org")
    assert ev["user_id"] == seeded["user"] and float(ev["quantity"]) == 4
    assert float(ev["cost_usd"]) == pytest.approx(0.0104) and ev["estimated"] is False
    assert ev["raw"]["run_id"] == call["apify_run_id"]
    assert _admin("SELECT count(*) AS n FROM usage_blocks WHERE org_id = %s", (seeded["org"],))[0]["n"] == 0


def test_over_limit_blocks_the_run_with_a_usage_block(seeded, tmp_path, sample_facebook_posts_results):
    """An org with no Apify budget left: nothing bought, run 'blocked', usage_blocks row
    written by crawler_worker."""
    from src.tests.test_crawl_worker import FakeApify
    from src.worker.runner import ACK, handle_message

    _admin("""INSERT INTO usage_limits (org_id, scope, tool, kind, window_kind, limit_usd, mode, note)
              VALUES (%s, 'org', 'apify', NULL, 'day', 0, 'hard', 'pytest: no apify budget')""", (seeded["org"],))
    apify = FakeApify({"apify/facebook-posts-scraper": sample_facebook_posts_results})
    assert handle_message(json.dumps(_msg(seeded)), _deps(tmp_path, apify)) == ACK
    assert apify.calls == []
    r = _admin("SELECT * FROM crawl_runs WHERE run_id = %s", (seeded["run"],))[0]
    assert r["status"] == "blocked" and "over_limit" in r["error"]
    assert r["stats"]["usage_refusal"]["reason"] == "over_limit"
    (b,) = _admin("SELECT * FROM usage_blocks WHERE org_id = %s", (seeded["org"],))
    assert (b["path"], b["tool"], b["kind"], b["reason"]) == ("worker:apify-client", "apify", "crawl", "over_limit")
    s = _admin("SELECT * FROM crawl_schedules WHERE task_id = %s", (seeded["task"],))[0]
    assert s["last_run_id"] is None and s["consecutive_failures"] == 0


def test_missing_org_block_row_as_crawler_worker(seeded, tmp_path):
    from src.tests.test_crawl_worker import FakeApify
    from src.worker.runner import ACK, handle_message

    marker = 900_000_000 + seeded["run"]  # usage_blocks.search_id has no FK: a findable marker
    apify = FakeApify({})
    try:
        assert handle_message(json.dumps(_msg(seeded, org_id=None, query_id=marker)), _deps(tmp_path, apify)) == ACK
        (b,) = _admin("SELECT * FROM usage_blocks WHERE search_id = %s", (marker,))
        assert (b["org_id"], b["path"], b["tool"], b["kind"], b["reason"]) == \
            (None, "worker:apify-client", "apify", "crawl", "missing_org")
        assert _admin("SELECT status FROM crawl_runs WHERE run_id = %s", (seeded["run"],))[0]["status"] == "failed"
        assert apify.calls == []
    finally:
        _admin("DELETE FROM usage_blocks WHERE search_id = %s", (marker,))


def test_release_run_within_grants(seeded, tmp_path):
    db = _db(tmp_path)
    db.claim_run(seeded["run"], "pytest")
    assert db.release_run(seeded["run"], "AdmissionRefused(ledger_unreachable)") == 1
    r = _admin("SELECT status, worker, started_at, error FROM crawl_runs WHERE run_id = %s", (seeded["run"],))[0]
    assert r["status"] == "queued" and r["worker"] is None and r["started_at"] is None
    assert db.claim_run(seeded["run"], "pytest")["status"] == "running"  # claimable again


def test_comment_request_progress(seeded, tmp_path):
    org, user = seeded["org"], seeded["user"]
    req = _admin(
        """INSERT INTO crawl_comment_requests (org_id, selector, status, estimate_usd, approved_by, approved_at)
           VALUES (%s, 'hand', 'approved', 0.02, %s, now()) RETURNING request_id""", (org, user))[0]["request_id"]
    run = _admin(
        """INSERT INTO crawl_runs (kind, comment_request_id, org_id, trigger, status, params, max_items, trace_id)
           VALUES ('comments', %s, %s, 'api', 'queued', '{}', 15, 't') RETURNING run_id""", (req, org))[0]["run_id"]
    post = _admin(
        """INSERT INTO crawl_comment_request_posts (request_id, post_url, tier, depth, run_id)
           VALUES (%s, 'https://www.facebook.com/x/posts/1/', 'deep', 50, %s) RETURNING row_id""",
        (req, run))[0]["row_id"]
    db = _db(tmp_path)
    assert db.claim_run(run, "pytest")["kind"] == "comments"
    assert [p["row_id"] for p in db.get_comment_posts(run)] == [post]
    assert db.mark_comment_request_running(req) == 1
    assert db.set_comment_post_fetched(post, 12) == 1
    assert db.close_comment_request_if_done(req) == 0  # its run is still running
    db.finish_run(run, "succeeded", None, None, {"items_paid": 12, "cost_usd": 0.01}, {})
    assert db.close_comment_request_if_done(req) == 1
    r = _admin("SELECT status, finished_at FROM crawl_comment_requests WHERE request_id = %s", (req,))[0]
    assert r["status"] == "done" and r["finished_at"] is not None


def test_adhoc_run_end_to_end_against_db(seeded, tmp_path, sample_facebook_posts_results, monkeypatch):
    """Ad-hoc run (design §6.4; needs 2026-10-07_crawl_runs_adhoc_userdb.sql): kind 'adhoc',
    no task, spec in params.spec → claimed, built with CrawlTask.from_spec, metered and
    finished as crawler_worker; no schedule bookkeeping."""
    from unittest.mock import patch

    from src.models.crawl_task import adhoc_cache_key
    from src.tests.test_crawl_worker import FakeApify
    from src.worker.runner import ACK, handle_message

    monkeypatch.setattr("src.actors.actor.FILTER_CACHE_PATH", str(tmp_path / "fc.json"))
    spec = {"name": "", "actor_class": "facebook_page_posts",
            "search_params": [f"https://www.facebook.com/QroMunicipio/?adhoc={seeded['tag']}"],
            "country_id": None, "language": None, "max_results": 30, "min_date": None, "period": "w",
            "publish": True, "get_comments": False, "max_comments": 15, "not_keywords": [],
            "llm_filter_condition": None, "update_existing": False, "enrich_followers": False,
            "fetch_attached_url": False, "classify_users": False, "official_page": False,
            "actor_params": {"add_text_from_images": False}, "max_cost_usd_per_run": 0.5, "metadata": {}}
    params = {"override_filters": False, "task_id": adhoc_cache_key(spec), "spec": spec}
    run = _admin(
        """INSERT INTO crawl_runs (kind, task_id, org_id, trigger, requested_by, status, params, max_items,
                                   trace_id, queued_at, window_since, dry_run)
           VALUES ('adhoc', NULL, %s, 'api', %s, 'queued', %s, 20, %s, now(), now() - interval '7 days', true)
           RETURNING run_id""",
        (seeded["org"], seeded["user"], json.dumps(params), f"adhoc-test-{seeded['tag']}"))[0]["run_id"]
    apify = FakeApify({"apify/facebook-posts-scraper": sample_facebook_posts_results}, cost_per_call=0.0104)
    deps = _deps(tmp_path, apify)
    msg = _msg(seeded, run_id=run, kind="adhoc", task_id=None, params=params)
    with patch("src.actors.actor.ApifyActor._existing_news_ids", return_value=set()):
        assert handle_message(json.dumps(msg), deps) == ACK
    r = _admin("SELECT * FROM crawl_runs WHERE run_id = %s", (run,))[0]
    assert r["status"] == "succeeded", r["error"]
    assert r["kind"] == "adhoc" and r["task_id"] is None
    assert r["items_paid"] == 4 and float(r["cost_usd"]) == pytest.approx(0.0104) and r["output_path"]
    assert r["stats"]["effective_params"]["task_id"] == adhoc_cache_key(spec)
    assert r["stats"]["effective_params"]["max_results"] == 20
    (call,) = _admin("SELECT * FROM crawl_run_calls WHERE run_id = %s", (run,))
    (ev,) = _admin("SELECT * FROM usage_events WHERE org_id = %s", (seeded["org"],))
    assert call["usage_event_id"] == ev["event_id"] == \
        f"apify-client:crawl-run:{run}:{seeded['org']}:crawl:{call['apify_run_id']}"
    s = _admin("SELECT * FROM crawl_schedules WHERE task_id = %s", (seeded["task"],))[0]
    assert s["last_run_id"] is None                         # the seeded task's schedule is untouched


def test_batch_run_end_to_end_against_db(seeded, tmp_path, sample_facebook_posts_results, monkeypatch):
    """Batch run (design §6.5; needs 2026-10-13_crawl_batch_runs_userdb.sql): kind 'batch',
    merged spec in params.spec, members in crawl_run_tasks → claimed (members turn live),
    one Apify call over both pages, attribution written to crawl_run_tasks and every
    member's schedule updated, all as crawler_worker; members not live once finished."""
    from unittest.mock import patch

    from src.tests.test_crawl_worker import FakeApify
    from src.worker.runner import ACK, handle_message

    if not _admin("SELECT to_regclass('public.crawl_run_tasks') IS NOT NULL AS ok")[0]["ok"]:
        pytest.skip("crawl_run_tasks missing: apply 2026-10-13_crawl_batch_runs_userdb.sql to the test DB")
    monkeypatch.setattr("src.actors.actor.FILTER_CACHE_PATH", str(tmp_path / "fc.json"))
    org, user, tag = seeded["org"], seeded["user"], seeded["tag"]
    page_a = _admin("SELECT search_params FROM crawl_tasks WHERE task_id = %s", (seeded["task"],))[0]["search_params"][0]
    page_b = f"https://www.facebook.com/OtraPagina/?t={tag}"
    task_b = _admin(
        """INSERT INTO crawl_tasks (org_id, actor_class, search_params, max_results, status, actor_params)
           VALUES (%s, 'facebook_page_posts', %s, 30, 'active', '{"add_text_from_images": false}')
           RETURNING task_id""", (org, json.dumps([page_b])))[0]["task_id"]
    _admin("INSERT INTO crawl_schedules (task_id, every, next_run_at) VALUES (%s, '1 day', now())", (task_b,))
    spec = {"name": "batch", "actor_class": "facebook_page_posts", "search_params": sorted([page_a, page_b]),
            "country_id": None, "language": None, "max_results": 30, "min_date": None, "period": "w",
            "publish": True, "get_comments": False, "max_comments": 15, "not_keywords": [],
            "llm_filter_condition": None, "update_existing": False, "enrich_followers": False,
            "fetch_attached_url": False, "classify_users": False, "official_page": False,
            "actor_params": {"add_text_from_images": False}, "max_cost_usd_per_run": None,
            "metadata": {"batch_task_ids": [seeded["task"], task_b]}}
    key = "batch:" + uuid.uuid4().hex + "abcdefgh"
    params = {"override_filters": False, "task_id": key, "spec": spec}
    run = _admin(
        """INSERT INTO crawl_runs (kind, task_id, org_id, trigger, requested_by, status, params, max_items,
                                   trace_id, window_since, dry_run)
           VALUES ('batch', NULL, %s, 'schedule', %s, 'requested', %s, 30, %s, now() - interval '7 days', true)
           RETURNING run_id""", (org, user, json.dumps(params), f"batch-test-{tag}"))[0]["run_id"]
    for t, p in ((seeded["task"], page_a), (task_b, page_b)):
        _admin("""INSERT INTO crawl_run_tasks (run_id, task_id, org_id, search_params, max_items, filter_cache_key)
                  VALUES (%s, %s, %s, %s, 30, %s)""", (run, t, org, json.dumps([p]), f"db:{t}"))
    # the seeded single run of task A is queued: the batch can't go live with it (one live run per task)
    with pytest.raises(psycopg.errors.UniqueViolation):
        _admin("UPDATE crawl_runs SET status = 'queued', queued_at = now() WHERE run_id = %s", (run,))
    _admin("UPDATE crawl_runs SET status = 'cancelled' WHERE run_id = %s", (seeded["run"],))
    _admin("UPDATE crawl_runs SET status = 'queued', queued_at = now() WHERE run_id = %s", (run,))
    assert [r["live"] for r in _admin("SELECT live FROM crawl_run_tasks WHERE run_id = %s", (run,))] == [True, True]

    apify = FakeApify({"apify/facebook-posts-scraper": sample_facebook_posts_results}, cost_per_call=0.0104)
    msg = _msg(seeded, run_id=run, kind="batch", task_id=None, max_items=30, params=params)
    with patch("src.actors.actor.ApifyActor._existing_news_ids", return_value=set()):
        assert handle_message(json.dumps(msg), _deps(tmp_path, apify)) == ACK
    r = _admin("SELECT * FROM crawl_runs WHERE run_id = %s", (run,))[0]
    assert r["status"] == "succeeded", r["error"]
    assert r["items_paid"] == 4 and r["stats"]["effective_params"]["task_id"] == key
    (call,) = apify.calls
    assert sorted(u["url"] for u in call["run_input"]["startUrls"]) == sorted([page_a, page_b])
    rows = {m["task_id"]: m for m in _admin("SELECT * FROM crawl_run_tasks WHERE run_id = %s", (run,))}
    a, b = rows[seeded["task"]], rows[task_b]
    assert (a["items_matched"], b["items_matched"]) == (3, 0)        # 3 QroMunicipio posts, 1 reel URL
    assert a["live"] is False and b["live"] is False and a["newest_item_at"] is not None
    assert float(a["cost_usd"]) + float(b["cost_usd"]) == pytest.approx(0.0104, abs=1e-5)
    assert a["stats"]["attribution"] == "keyword_match"
    for t in (seeded["task"], task_b):
        s = _admin("SELECT last_run_id, last_success_at FROM crawl_schedules WHERE task_id = %s", (t,))[0]
        assert s["last_run_id"] == run and s["last_success_at"] is not None
