"""Crawl worker against a real throwaway userdb, connected as ``crawler_worker``.

Proves the SQL in src/worker/db.py runs within the role's grants (design Q5). Skipped
unless both DSNs are set — never point them at stg/prod:

    dev/testdb.sh proto06_test_crawl_worker
    psql ... -d proto06_test_crawl_worker -v crawler_password=localcrawler \\
        -f media-backend-paid/docs/db/migrations/2026-10-06_crawl_worker_role_userdb.sql
    TEST_CRAWL_USERDB_ADMIN_URI=postgresql://postgres:postgres@localhost:5334/proto06_test_crawl_worker \\
    TEST_CRAWL_USERDB_URI=postgresql://crawler_worker:localcrawler@localhost:5334/proto06_test_crawl_worker \\
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
    _admin("DELETE FROM crawl_runs WHERE org_id = %s", (org,))
    _admin("DELETE FROM crawl_comment_requests WHERE org_id = %s", (org,))
    _admin("DELETE FROM crawl_tasks WHERE org_id = %s", (org,))
    _admin("DELETE FROM users WHERE id = %s", (user,))
    _admin("DELETE FROM orgs WHERE id = %s", (org,))


def _db(tmp_path):
    from src.worker.db import CrawlDB

    return CrawlDB(WORKER_URI, spool_path=str(tmp_path / "spool.jsonl"))


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
    db.finish_run(run_id, "succeeded", None, None, counters, {"funnel": {"docs_in": 4}})
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
    down = CrawlDB("postgresql://crawler_worker:wrong@127.0.0.1:5334/proto06_test_crawl_worker",
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
    """Queue message → claim → actor (fake Apify) → crawl_run_calls + funnel → succeeded, no publish."""
    from unittest.mock import patch

    from src.actors import get_actor
    from src.tests.test_crawl_worker import FakeApify
    from src.worker.runner import ACK, WorkerDeps, handle_message

    monkeypatch.setattr("src.actors.actor.FILTER_CACHE_PATH", str(tmp_path / "fc.json"))
    _admin("UPDATE crawl_runs SET dry_run = true WHERE run_id = %s", (seeded["run"],))
    apify = FakeApify({"apify/facebook-posts-scraper": sample_facebook_posts_results}, cost_per_call=0.0104)
    published: list = []
    deps = WorkerDeps(db=_db(tmp_path), apify_client_factory=lambda: apify,
                      actor_factory=lambda ac, client: get_actor(ac, client=client),
                      publish=published.append, worker_name="pytest", output_dir=str(tmp_path / "runs"))
    msg = {"type": "crawl_run", "run_id": seeded["run"], "kind": "crawl", "task_id": seeded["task"],
           "org_id": seeded["org"], "user_id": seeded["user"], "trace_id": f"crawl-test-{seeded['tag']}",
           "max_items": 50, "max_cost_usd": 0.2, "dry_run": True, "params": {}}
    with patch("src.actors.actor.ApifyActor._existing_news_ids",
               return_value={sample_facebook_posts_results[0]["url"]}):
        assert handle_message(json.dumps(msg), deps) == ACK
    r = _admin("SELECT * FROM crawl_runs WHERE run_id = %s", (seeded["run"],))[0]
    assert r["status"] == "succeeded", r["error"]
    assert (r["items_paid"], r["items_already_seen"], r["items_new"]) == (4, 1, 3)
    assert float(r["cost_usd"]) == pytest.approx(0.0104) and r["output_path"]
    assert r["stats"]["effective_params"]["task_id"] == f"db:{seeded['task']}"
    assert published == []
    assert _admin("SELECT count(*) AS n FROM crawl_run_calls WHERE run_id = %s", (seeded["run"],))[0]["n"] == 1


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
