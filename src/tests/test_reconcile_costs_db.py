"""src/scripts/reconcile_costs.py against a throwaway userdb (admin DSN: the script UPDATEs
crawl_run_calls, which crawler has no grant for). Same DB recipe and env as
test_crawl_worker_db.py (``TEST_CRAWL_USERDB_ADMIN_URI``); skipped without it. Fake Apify
client on the recorded run payload — no network."""
from __future__ import annotations

import json
import os
import uuid

import pytest

from src.tests.test_cost_settle import SEARCH_RUN, FakeRunsClient, recorded

ADMIN_URI = os.getenv("TEST_CRAWL_USERDB_ADMIN_URI")
pytestmark = pytest.mark.skipif(not ADMIN_URI, reason="TEST_CRAWL_USERDB_ADMIN_URI not set")
if ADMIN_URI and "_test" not in ADMIN_URI.rsplit("/", 1)[-1]:
    raise RuntimeError(f"refusing: test DSN must name a *_test* database, got {ADMIN_URI!r}")
psycopg = pytest.importorskip("psycopg")


def _sql(sql, params=()):
    from psycopg.rows import dict_row

    with psycopg.connect(ADMIN_URI, row_factory=dict_row, autocommit=True) as conn:
        cur = conn.execute(sql, params)
        return list(cur.fetchall()) if cur.description else []


@pytest.fixture
def seeded():
    tag = uuid.uuid4().hex[:8]
    org = _sql("INSERT INTO orgs (public_id, name, contact_email) VALUES (%s, %s, %s) RETURNING id",
               (f"org-{tag}", f"reconcile test {tag}", f"{tag}@example.test"))[0]["id"]
    task = _sql("INSERT INTO crawl_tasks (org_id, actor_class, search_params, max_results, status) "
                "VALUES (%s, 'facebook_keyword_search', %s, 30, 'active') RETURNING task_id",
                (org, json.dumps([f"q {tag}"])))[0]["task_id"]
    runs = {"succeeded": _sql(
        "INSERT INTO crawl_runs (kind, task_id, org_id, trigger, status, params, max_items, trace_id, cost_usd) "
        "VALUES ('crawl', %s, %s, 'cli', 'succeeded', '{}', 30, %s, 0.00005) RETURNING run_id",
        (task, org, f"t-{tag}-done"))[0]["run_id"]}
    # still running (an ad-hoc row: no task, so the one-live-run-per-task index doesn't apply)
    runs["running"] = _sql(
        "INSERT INTO crawl_runs (kind, org_id, trigger, status, params, max_items, trace_id) "
        "VALUES ('adhoc', %s, 'cli', 'running', '{\"spec\": {}}', 30, %s) RETURNING run_id",
        (org, f"t-{tag}-running"))[0]["run_id"]
    calls = {}
    for key, run_id, apify_id in (("done", runs["succeeded"], SEARCH_RUN), ("live", runs["running"], f"{SEARCH_RUN}x")):
        eid = f"apify-client:crawl-run:{run_id}:{org}:crawl:{apify_id}"
        calls[key] = _sql(
            "INSERT INTO crawl_run_calls (run_id, purpose, apify_actor_id, apify_run_id, apify_status, items, cost_usd, "
            "estimated, usage_event_id) VALUES (%s, 'search', 'scrapeforge/facebook-search-posts', %s, 'SUCCEEDED', "
            "29, 0.00005, false, %s) RETURNING call_id", (run_id, apify_id, eid))[0]["call_id"]
        _sql("INSERT INTO usage_events (event_id, org_id, service, tool, kind, quantity, unit, cost_usd, model, raw) "
             "VALUES (%s, %s, 'apify-client', 'apify', 'crawl', 29, 'items', 0.00005, "
             "'scrapeforge/facebook-search-posts', %s)", (eid, org, json.dumps({"run_id": apify_id})))
    yield {"org": org, "runs": runs, "calls": calls}
    _sql("DELETE FROM usage_events WHERE org_id = %s", (org,))
    _sql("DELETE FROM crawl_runs WHERE org_id = %s", (org,))
    _sql("DELETE FROM crawl_tasks WHERE org_id = %s", (org,))
    _sql("DELETE FROM orgs WHERE id = %s", (org,))


def _client():
    run = recorded(SEARCH_RUN)
    return FakeRunsClient({SEARCH_RUN: run, f"{SEARCH_RUN}x": dict(run, id=f"{SEARCH_RUN}x")},
                          {run["defaultDatasetId"]: 29})


def test_dry_run_writes_nothing_then_apply_corrects_calls_runs_and_ledger(seeded, capsys):
    from src.scripts import reconcile_costs as rc

    run_ids = [str(r) for r in seeded["runs"].values()]
    args = ["--db-uri", ADMIN_URI, "--all"] + sum((["--run-id", r] for r in run_ids), [])
    report = rc.main(args, apify_client=_client())
    assert report["mode"] == "dry-run" and {c["status"] for c in report["calls"]} == {"corrected"}
    assert "DRY RUN" in capsys.readouterr().out
    assert _sql("SELECT sum(cost_usd)::float AS s FROM crawl_run_calls WHERE call_id = ANY(%s)",
                (list(seeded["calls"].values()),))[0]["s"] == pytest.approx(0.0001)
    assert _sql("SELECT count(*) AS n FROM usage_events WHERE org_id = %s", (seeded["org"],))[0]["n"] == 2

    report = rc.main(args + ["--apply"], apify_client=_client())
    calls = _sql("SELECT call_id, cost_usd::float AS c, estimated, usage FROM crawl_run_calls WHERE call_id = ANY(%s) "
                 "ORDER BY call_id", (list(seeded["calls"].values()),))
    assert [c["c"] for c in calls] == [pytest.approx(0.07516)] * 2 and not any(c["estimated"] for c in calls)
    assert calls[0]["usage"]["chargedEventCounts"]["apify-default-dataset-item"] == 29
    done = _sql("SELECT cost_usd::float AS c, stats FROM crawl_runs WHERE run_id = %s", (seeded["runs"]["succeeded"],))[0]
    assert done["c"] == pytest.approx(0.07516) and "cost_reconciled_at" in done["stats"]
    live = _sql("SELECT cost_usd FROM crawl_runs WHERE run_id = %s", (seeded["runs"]["running"],))[0]
    assert live["cost_usd"] is None                       # left to its worker
    assert report["runs_retotalled"] == [seeded["runs"]["succeeded"]]
    assert report["runs_not_terminal"] == [seeded["runs"]["running"]]
    ledger = _sql("SELECT event_id, cost_usd::float AS c FROM usage_events WHERE org_id = %s ORDER BY event_id",
                  (seeded["org"],))
    assert len(ledger) == 4 and sum(r["c"] for r in ledger) == pytest.approx(2 * 0.07516)
    assert sum(r["event_id"].endswith(":correction") for r in ledger) == 2

    report = rc.main(args + ["--apply"], apify_client=_client())    # idempotent
    assert {c["status"] for c in report["calls"]} == {"ok"}
    assert {c["status"] for c in report["ledger"]} == {"ok"}
    assert _sql("SELECT count(*) AS n FROM usage_events WHERE org_id = %s", (seeded["org"],))[0]["n"] == 4
