"""Crawl-run queue worker (src/worker/*, docs/db/crawl_tasks.md §6.1).

No network: Apify is a fake client (no paid calls), userdb and the usage_client ledger
are in-memory fakes, the document publisher is a list. Every Apify call goes through the
real ``usage_client.apify.run`` (admission + recording against the fake ledger). The
DB-backed counterpart (real SQL as crawler_worker) is test_crawl_worker_db.py.
"""
from __future__ import annotations

from usage_client.apify import APIFY_MIN_CHARGE_CAP_USD  # Apify refuses a lower charge cap
import copy
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

from src.actors import get_actor
from src.models.crawl_task import CrawlTask, adhoc_cache_key
from src.worker import runner as runner_mod
from src.worker.db import CrawlDB
from usage_client import AdmissionRefused
from usage_client.context import LedgerUnavailable
from usage_client.ledger import ANY_TOOL, WINDOWS

from src.worker.recording import (
    CallRecorder,
    CostCapExceeded,
    ItemCapExceeded,
    RecordingApifyClient,
    UsageMeter,
    instrument_actor,
)
from src.worker.runner import ACK, REQUEUE, WorkerDeps, build_crawl_kwargs, diff_params, handle_message

FB_PAGE = "https://www.facebook.com/QroMunicipio/"


# --------------------------------------------------------------------------- fakes


class FakeApify:
    """ApifyClient stand-in: ``actor(id).call`` returns a run object, ``dataset`` serves items."""

    def __init__(self, items_by_actor: Dict[str, List[dict]], cost_per_call: float = 0.01,
                 fail_on: Optional[str] = None):
        self.items_by_actor = items_by_actor
        self.cost_per_call = cost_per_call
        self.fail_on = fail_on
        self.calls: List[dict] = []
        self.datasets: Dict[str, List[dict]] = {}
        self.runs: Dict[str, dict] = {}

    def run(self, run_id):
        """GET /v2/actor-runs/{id}: the finished run, charges settled."""
        fake = self

        class _R:
            def get(self):
                return copy.deepcopy(fake.runs.get(run_id))

        return _R()

    def actor(self, actor_id):
        fake = self

        class _A:
            def call(self, run_input=None, **kwargs):
                if fake.fail_on == actor_id:
                    raise RuntimeError(f"apify {actor_id} exploded")
                n = len(fake.calls) + 1
                items = copy.deepcopy(fake.items_by_actor.get(actor_id, []))
                if kwargs.get("max_items") is not None:
                    items = items[: kwargs["max_items"]]
                fake.calls.append({"actor_id": actor_id, "run_input": run_input, "kwargs": kwargs})
                fake.datasets[f"ds-{n}"] = items
                run = {
                    "id": f"apifyrun-{actor_id.replace('/', '_')}-{n}-{uuid.uuid4().hex[:8]}",
                    "status": "SUCCEEDED",
                    "defaultDatasetId": f"ds-{n}",
                    "usageTotalUsd": fake.cost_per_call,
                    "usage": {"COMPUTE_UNITS": 0.001},
                    "startedAt": "2026-10-06T12:00:00.000Z",
                    "finishedAt": "2026-10-06T12:00:05.000Z",
                }
                fake.runs[run["id"]] = run
                return dict(run)

        return _A()

    def dataset(self, dataset_id):
        items = self.datasets.get(dataset_id, [])

        class _D:
            def get(self):
                return {"itemCount": len(items)}

            def iterate_items(self):
                return iter(items)

        return _D()


class FakeLedger:
    """usage_client.ledger.Ledger stand-in (the subset admission/apify.run/block use)."""

    def __init__(self, apify_day_usd: Optional[float] = 15.0):
        self.dsn = "fake://ledger"
        self.limits: List[dict] = []
        self.events: Dict[str, Any] = {}
        self.blocks: List[dict] = []
        self.down = False
        if apify_day_usd is not None:  # the seeded default: apify $15/day per org (hard)
            self.add_limit(limit_usd=apify_day_usd)

    def add_limit(self, *, limit_usd, tool="apify", kind=None, org_id=None, scope="org",
                  window_kind="day", mode="hard", limit_units=None):
        self.limits.append(dict(limit_id=len(self.limits) + 1, org_id=org_id, scope=scope, tool=tool, kind=kind,
                                window_kind=window_kind, limit_usd=float(limit_usd), limit_units=limit_units,
                                mode=mode, expires_at=None, note=None,
                                updated_at=datetime(2026, 10, 1, tzinfo=timezone.utc)))

    def spent(self, org_id: int, usd: float, kind: str = "crawl") -> None:
        from usage_client.ledger import UsageEvent

        eid = f"seed:{len(self.events)}"
        self.events[eid] = UsageEvent(event_id=eid, org_id=org_id, service="test", tool="apify", kind=kind,
                                      quantity=1, unit="items", cost_usd=usd)

    def live_limits(self, *, now=None):
        if self.down:
            raise LedgerUnavailable("fake ledger down")
        return [dict(r) for r in self.limits]

    def spend(self, org_id, window, tool=None, kind=None, scope=None, *, now=None):
        if self.down:
            raise LedgerUnavailable("fake ledger down")
        since = (now or datetime.now(timezone.utc)) - WINDOWS[window]
        return sum(e.cost_usd for e in self.events.values()
                   if e.timestamp > since and (org_id is None or e.org_id == org_id)
                   and (tool in (None, ANY_TOOL) or e.tool == tool)
                   and (kind is None or e.kind == kind) and (scope is None or e.scope == scope))

    def block(self, **kw):
        self.blocks.append(kw)
        return True

    def record(self, event, conn=None):
        self.events.setdefault(event.event_id, event)
        return True

    def recorded(self) -> List[Any]:
        return [e for k, e in self.events.items() if not k.startswith("seed:")]


def _meter(ledger=None, org_id=106, run_id=1234) -> UsageMeter:
    return UsageMeter(org_id=org_id, event_key=f"crawl-run:{run_id}", ledger=ledger or FakeLedger())


class FakeDB:
    """In-memory userdb with the CrawlDB interface."""

    def __init__(self):
        self.runs: Dict[int, dict] = {}
        self.tasks: Dict[int, dict] = {}
        self.actor_classes: Dict[str, dict] = {}
        self.calls: Dict[str, dict] = {}
        self.finished: List[dict] = []
        self.schedule_updates: List[tuple] = []
        self.comment_posts: List[dict] = []
        self.fetched: Dict[int, int] = {}
        self.unreachable = False
        self.heartbeats = 0

    def replay_spool(self):
        return 0

    def claim_run(self, run_id, worker):
        if self.unreachable:
            raise ConnectionError("userdb down")
        run = self.runs.get(run_id)
        if not run or run["status"] != "queued":
            return None
        run.update(status="running", worker=worker)
        return dict(run)

    def get_run(self, run_id):
        return self.runs.get(run_id)

    def release_run(self, run_id, error):
        run = self.runs.get(run_id)
        if run and run["status"] == "running":
            run.update(status="queued", error=error, worker=None)
            return 1
        return 0

    def fail_queued_run(self, run_id, error):
        run = self.runs.get(run_id)
        if run and run["status"] == "queued":
            run.update(status="failed", error=error)
            return 1
        return 0

    def heartbeat(self, run_id):
        self.heartbeats += 1
        return 1

    def get_task(self, task_id):
        return self.tasks.get(task_id)

    def get_actor_class(self, actor_class):
        return self.actor_classes.get(actor_class)

    def insert_call(self, row):
        self.calls.setdefault(row["apify_run_id"], row)  # ON CONFLICT DO NOTHING

    def finish_run(self, run_id, status, error, output_path, counters, stats):
        run = self.runs[run_id]
        if run["status"] != "running":
            return False
        run.update(status=status, error=error, output_path=output_path, **counters)
        run["stats"] = {**run.get("stats", {}), **json.loads(json.dumps(stats, default=str))}
        self.finished.append(run_id)
        return True

    def update_schedule_after_run(self, task_id, run_id, success):
        self.schedule_updates.append((task_id, run_id, success))
        return 1

    def get_comment_posts(self, run_id):
        return [p for p in self.comment_posts if p["run_id"] == run_id]

    def set_comment_post_fetched(self, row_id, n):
        self.fetched[row_id] = n

    def mark_comment_request_running(self, request_id):
        return 1

    def close_comment_request_if_done(self, request_id):
        return 1


def _task_row(**over) -> dict:
    row = {
        "task_id": 159, "org_id": 106, "name": "", "actor_class": "facebook_page_posts",
        "search_params": [FB_PAGE], "country_id": None, "language": None, "max_results": 30,
        "min_date": None, "period": None, "publish": True, "get_comments": False, "max_comments": 15,
        "not_keywords": [], "llm_filter_condition": None, "update_existing": False,
        "enrich_followers": False, "fetch_attached_url": False, "classify_users": False,
        "official_page": False, "actor_params": {"add_text_from_images": False},
        "max_cost_usd_per_run": None, "status": "active", "legacy_theme": None, "legacy_task_id": None,
    }
    row.update(over)
    return row


def _run_row(**over) -> dict:
    row = {
        "run_id": 1234, "kind": "crawl", "task_id": 159, "comment_request_id": None, "org_id": 106,
        "query_id": None, "trigger": "api", "requested_by": 23, "status": "queued",
        "params": {"override_filters": False}, "window_since": None, "window_until": None,
        "max_items": 50, "admitted_usd": None, "dry_run": False, "trace_id": "crawl-1234-t", "stats": {},
    }
    row.update(over)
    return row


def _message(**over) -> dict:
    msg = {
        "type": "crawl_run", "run_id": 1234, "kind": "crawl", "task_id": 159, "comment_request_id": None,
        "org_id": 106, "user_id": 23, "query_id": None, "trace_id": "crawl-1234-t",
        "max_items": 50, "max_cost_usd": 0.20, "dry_run": False,
        "params": {"actor_class": "facebook_page_posts", "search_params": [FB_PAGE],
                   "min_date": None, "period": None, "override_filters": False},
    }
    msg.update(over)
    return msg


ACTOR_CLASSES = {
    "facebook_page_posts": {"actor_class": "facebook_page_posts", "run_kind": "crawl", "enabled": True,
                            "cap_scope": "per_param", "params_schema": {}},
    "facebook_keyword_search": {"actor_class": "facebook_keyword_search", "run_kind": "crawl", "enabled": True,
                                "cap_scope": "per_param", "params_schema": {}},
    "google_news": {"actor_class": "google_news", "run_kind": "crawl", "enabled": True,
                    "cap_scope": "per_call", "params_schema": {}},
    "facebook_post_comments": {"actor_class": "facebook_post_comments", "run_kind": "comments",
                               "enabled": True, "cap_scope": "per_call", "params_schema": {}},
}


@pytest.fixture(autouse=True)
def _isolated_caches(tmp_path, monkeypatch):
    monkeypatch.setattr("src.actors.actor.FILTER_CACHE_PATH", str(tmp_path / "filter_cache.json"))


@pytest.fixture
def world(tmp_path, sample_facebook_posts_results, sample_facebook_comments):
    db = FakeDB()
    db.tasks[159] = _task_row()
    db.runs[1234] = _run_row()
    db.actor_classes = copy.deepcopy(ACTOR_CLASSES)
    apify = FakeApify({
        "apify/facebook-posts-scraper": sample_facebook_posts_results,
        "apify/facebook-comments-scraper": sample_facebook_comments,
        "scrapeforge/facebook-search-posts": [],
    })
    published: List[Any] = []
    deps = WorkerDeps(
        db=db,
        apify_client_factory=lambda: apify,
        actor_factory=lambda actor_class, client: get_actor(actor_class, client=client),
        publish=published.append,
        usage_ledger=FakeLedger(),
        worker_name="test-worker",
        output_dir=str(tmp_path / "runs"),
        heartbeat_secs=0.01,
    )
    return db, apify, deps, published


def _no_es(urls=()):
    return patch("src.actors.actor.ApifyActor._existing_news_ids", return_value=set(urls))


# --------------------------------------------------------------------------- message rules


class TestMessageRules:
    def test_malformed_message_is_acked_without_claim(self, world):
        db, apify, deps, _ = world
        assert handle_message(b"not json", deps) == ACK
        assert handle_message(json.dumps({"type": "other", "run_id": 1}), deps) == ACK
        assert handle_message(json.dumps({"type": "crawl_run", "run_id": "x"}), deps) == ACK
        assert db.runs[1234]["status"] == "queued"
        assert apify.calls == []

    def test_missing_org_acks_and_never_spends(self, world):
        db, apify, deps, _ = world
        assert handle_message(json.dumps(_message(org_id=None)), deps) == ACK
        assert apify.calls == []
        assert db.runs[1234]["status"] == "failed"
        assert "missing org_id" in db.runs[1234]["error"]

    def test_redelivery_of_running_or_terminal_run_is_dropped(self, world):
        db, apify, deps, _ = world
        for status in ("running", "succeeded", "cancelled"):
            db.runs[1234]["status"] = status
            assert handle_message(json.dumps(_message()), deps) == ACK
        assert apify.calls == []

    def test_db_unreachable_at_claim_requeues(self, world):
        db, apify, deps, _ = world
        db.unreachable = True
        assert handle_message(json.dumps(_message()), deps) == REQUEUE
        assert apify.calls == []

    def test_second_delivery_after_success_does_not_rebuy(self, world):
        db, apify, deps, _ = world
        with _no_es():
            assert handle_message(json.dumps(_message()), deps) == ACK
            n_calls = len(apify.calls)
            assert handle_message(json.dumps(_message()), deps) == ACK
        assert len(apify.calls) == n_calls


# --------------------------------------------------------------------------- happy paths


class TestRunExecution:
    def test_publish_run_records_calls_funnel_and_cost(self, world, sample_facebook_posts_results):
        db, apify, deps, published = world
        seen_url = sample_facebook_posts_results[0]["url"]
        with _no_es([seen_url]):
            assert handle_message(json.dumps(_message()), deps) == ACK
        run = db.runs[1234]
        assert run["status"] == "succeeded", run.get("error")
        # one search call, recorded with Apify's usageTotalUsd
        assert len(db.calls) == 1
        call = next(iter(db.calls.values()))
        assert call["purpose"] == "search" and call["cost_usd"] == 0.01 and call["items"] == 4
        assert call["apify_actor_id"] == "apify/facebook-posts-scraper"
        # funnel
        assert run["items_paid"] == 4
        assert run["items_already_seen"] == 1
        assert run["items_new"] == 3 == len(published)
        assert run["cost_usd"] == pytest.approx(0.01)
        assert run["newest_item_at"] is not None and run["oldest_item_at"] is not None
        assert run["stats"]["funnel"]["docs_in"] == 4
        assert run["stats"]["effective_params"]["task_id"] == "db:159"

    def test_dry_run_saves_output_and_does_not_publish(self, world):
        db, apify, deps, published = world
        db.runs[1234]["dry_run"] = True
        with _no_es():
            handle_message(json.dumps(_message(dry_run=True)), deps)
        run = db.runs[1234]
        assert run["status"] == "succeeded"
        assert published == []
        assert run["output_path"] and json.load(open(run["output_path"]))
        assert run["stats"]["dry_run"] is True

    def test_task_publish_false_is_a_dry_run(self, world):
        db, apify, deps, published = world
        db.tasks[159]["publish"] = False
        with _no_es():
            handle_message(json.dumps(_message()), deps)
        assert published == [] and db.runs[1234]["output_path"]

    def test_enrichment_calls_are_tagged_by_purpose(self, world):
        db, apify, deps, _ = world
        db.tasks[159]["get_comments"] = True
        with _no_es():
            handle_message(json.dumps(_message()), deps)
        purposes = sorted(c["purpose"] for c in db.calls.values())
        assert purposes == ["comments", "search"]
        run = db.runs[1234]
        assert run["items_paid"] == 4          # search items only
        assert run["cost_usd"] == pytest.approx(0.02)
        assert isinstance(run["comments_fetched"], int)

    def test_page_actor_like_thresholds_gate_comment_and_profile_calls(self, world, sample_facebook_posts_results):
        """facebook_page_posts honours get_comments_after_likes / enrich_author_after_likes:
        the comments call carries only the posts over the threshold, and no profiles call is
        made when no post clears it (sample likes: 26, 10, 26, 203)."""
        db, apify, deps, _ = world
        db.tasks[159].update(get_comments=True, max_comments=30, enrich_followers=True,
                             actor_params={"add_text_from_images": False, "get_comments_after_likes": 30,
                                           "enrich_author_after_likes": 1000})
        with _no_es():
            handle_message(json.dumps(_message()), deps)
        run = db.runs[1234]
        assert run["status"] == "succeeded", run.get("error")
        assert sorted(c["purpose"] for c in db.calls.values()) == ["comments", "search"]   # no profiles call
        (comments_call,) = [c for c in apify.calls if c["actor_id"] == "apify/facebook-comments-scraper"]
        over = [i["url"] for i in sample_facebook_posts_results if (i.get("likes") or 0) > 30]
        assert [u["url"] for u in comments_call["run_input"]["startUrls"]] == over and len(over) == 1
        funnel = run["stats"]["funnel"]
        assert funnel["comments_skipped_likes"] == 3 and funnel["profiles_skipped_likes"] >= 1
        assert run["profiles_scraped"] == 0

    def test_publish_failure_is_partial_with_cost_kept(self, world):
        db, apify, deps, _ = world

        def boom(doc):
            raise ValueError("schema says no")

        deps.publish = boom
        with _no_es():
            handle_message(json.dumps(_message()), deps)
        run = db.runs[1234]
        assert run["status"] == "partial"
        assert "failed to publish" in run["error"]
        assert run["cost_usd"] == pytest.approx(0.01) and len(db.calls) == 1

    def test_amqp_error_stops_publishing_partial(self, world):
        from pika.exceptions import AMQPConnectionError

        db, apify, deps, _ = world
        sent = []

        def flaky(doc):
            if sent:
                raise AMQPConnectionError("broker gone")
            sent.append(doc)

        deps.publish = flaky
        with _no_es():
            handle_message(json.dumps(_message()), deps)
        run = db.runs[1234]
        assert run["status"] == "partial" and "RabbitMQ" in run["error"]
        assert run["items_new"] == 1

    def test_failure_after_paid_call_is_partial(self, world):
        db, apify, deps, _ = world
        db.tasks[159]["get_comments"] = True
        apify.fail_on = "apify/facebook-comments-scraper"
        with _no_es():
            handle_message(json.dumps(_message()), deps)
        run = db.runs[1234]
        assert run["status"] == "partial"
        assert "exploded" in run["error"]
        assert run["cost_usd"] == pytest.approx(0.01) and run["items_paid"] == 4

    def test_failure_before_any_call_is_failed(self, world):
        db, apify, deps, _ = world
        apify.fail_on = "apify/facebook-posts-scraper"
        with _no_es():
            handle_message(json.dumps(_message()), deps)
        run = db.runs[1234]
        assert run["status"] == "failed" and run["cost_usd"] == 0
        assert db.calls == {}

    def test_missing_task_fails_without_spend(self, world):
        db, apify, deps, _ = world
        del db.tasks[159]
        handle_message(json.dumps(_message()), deps)
        assert db.runs[1234]["status"] == "failed" and "not found" in db.runs[1234]["error"]
        assert apify.calls == []

    def test_params_schema_rejects_unknown_key(self, world):
        db, apify, deps, _ = world
        db.actor_classes["facebook_page_posts"]["params_schema"] = {"properties": {"add_text_from_images": {}}}
        db.tasks[159]["actor_params"] = {"add_text_from_images": False, "surprise": 1}
        handle_message(json.dumps(_message()), deps)
        assert db.runs[1234]["status"] == "failed" and "surprise" in db.runs[1234]["error"]
        assert apify.calls == []

    def test_cap_lifting_apify_input_is_rejected(self, world):
        db, apify, deps, _ = world
        db.tasks[159]["actor_params"] = {"apify_input": {"resultsPerPage": 5000}}
        handle_message(json.dumps(_message()), deps)
        assert db.runs[1234]["status"] == "failed" and "lift" in db.runs[1234]["error"]

    def test_message_params_diff_runs_db_version(self, world):
        db, apify, deps, _ = world
        msg = _message()
        msg["params"]["max_comments"] = 99  # stale copy
        with _no_es():
            handle_message(json.dumps(msg), deps)
        run = db.runs[1234]
        assert run["stats"]["message_params_diff"] == ["max_comments"]
        assert run["stats"]["effective_params"]["max_comments"] == 15

    def test_backend_window_message_shows_no_diff(self, world):
        """The backend sends the run's window as min_date = window_since in UTC with period
        null (design §6.2); the worker reads window_since in the DB session's zone. Same
        instant → no diff; a different instant still shows."""
        db, apify, deps, _ = world
        db.tasks[159]["period"] = "w"
        db.runs[1234]["window_since"] = datetime(2026, 9, 29, 6, 4, 11, 532871,
                                                 tzinfo=timezone(timedelta(hours=-6)))
        msg = _message()
        msg["params"].update(min_date="2026-09-29T12:04:11.532871+00:00", period=None)
        with _no_es():
            handle_message(json.dumps(msg), deps)
        assert "message_params_diff" not in db.runs[1234]["stats"]

        assert diff_params({"min_date": "2026-09-29T12:04:11+00:00"},
                           {"min_date": datetime(2026, 9, 29, 12, 5, tzinfo=timezone.utc)}) == ["min_date"]
        assert diff_params({"min_date": "2026-09-29T12:04:11"},
                           {"min_date": datetime(2026, 9, 29, 12, 4, 11, tzinfo=timezone.utc)}) == []

    def test_schedule_bookkeeping(self, world):
        db, apify, deps, _ = world
        db.runs[1234]["trigger"] = "schedule"
        with _no_es():
            handle_message(json.dumps(_message()), deps)
        assert db.schedule_updates == [(159, 1234, True)]

    def test_reaped_run_skips_schedule_bookkeeping(self, world):
        """The scheduler's reaper failed the run mid-flight (stale heartbeat): the late
        finish is not written and the schedule is left to the scheduler."""
        db, apify, deps, _ = world
        db.runs[1234]["trigger"] = "schedule"
        real_actor = apify.actor

        def reaping_actor(actor_id):
            db.runs[1234]["status"] = "failed"   # reaper: running -> failed
            return real_actor(actor_id)

        apify.actor = reaping_actor
        with _no_es():
            handle_message(json.dumps(_message()), deps)
        assert db.runs[1234]["status"] == "failed" and 1234 not in db.finished
        assert db.schedule_updates == []

    def test_heartbeat_runs_during_execution(self, world):
        db, apify, deps, _ = world
        real_call = apify.actor

        def slow_actor(actor_id):
            import time

            a = real_call(actor_id)
            orig = a.call

            def call(**kw):
                time.sleep(0.05)
                return orig(**kw)

            a.call = call
            return a

        apify.actor = slow_actor
        with _no_es():
            handle_message(json.dumps(_message()), deps)
        assert db.heartbeats >= 1

    def test_comments_run(self, world, sample_facebook_comments):
        db, apify, deps, published = world
        url = sample_facebook_comments[0].get("postUrl") or sample_facebook_comments[0].get("facebookUrl")
        db.runs[77] = _run_row(run_id=77, kind="comments", task_id=None, comment_request_id=5, max_items=15)
        db.comment_posts = [{"row_id": 1, "request_id": 5, "post_url": url, "tier": "deep", "depth": 50,
                             "stored_comments": 0, "run_id": 77}]
        handle_message(json.dumps(_message(run_id=77, kind="comments", task_id=None, comment_request_id=5)), deps)
        run = db.runs[77]
        assert run["status"] == "succeeded", run.get("error")
        assert apify.calls[0]["run_input"]["resultsLimit"] == 50
        assert [c["purpose"] for c in db.calls.values()] == ["comments"]
        assert run["items_paid"] == len(apify.datasets["ds-1"])
        assert db.fetched[1] > 0 and len(published) == 1


# --------------------------------------------------------------------------- usage metering


class TestUsageMetering:
    """Every Apify call goes through usage_client.apify.run (rate_limiting.md §2.7)."""

    def test_missing_org_writes_usage_block(self, world):
        db, apify, deps, _ = world
        assert handle_message(json.dumps(_message(org_id=0, query_id=228, kind="crawl")), deps) == ACK
        assert apify.calls == []
        assert deps.usage_ledger.blocks == [{"path": "worker:apify-client", "tool": "apify", "kind": "crawl",
                                             "reason": "missing_org", "search_id": 228}]
        assert db.runs[1234]["status"] == "failed"

    def test_missing_org_block_is_best_effort(self, world):
        db, apify, deps, _ = world

        def broken(**kw):
            raise RuntimeError("ledger exploded")

        deps.usage_ledger.block = broken
        assert handle_message(json.dumps(_message(org_id=None, kind="comments")), deps) == ACK
        assert db.runs[1234]["status"] == "failed"

    def test_one_usage_event_per_apify_run_linked_from_crawl_run_calls(self, world):
        db, apify, deps, _ = world
        db.tasks[159]["get_comments"] = True
        db.runs[1234].update(query_id=228, requested_by=23)
        with _no_es():
            handle_message(json.dumps(_message(query_id=228)), deps)
        assert db.runs[1234]["status"] == "succeeded", db.runs[1234].get("error")
        events = deps.usage_ledger.recorded()
        assert len(events) == len(apify.calls) == len(db.calls) == 2
        by_id = {e.event_id: e for e in events}
        for call in db.calls.values():
            ev = by_id[call["usage_event_id"]]  # crawl_run_calls.usage_event_id -> usage_events
            kind = {"search": "crawl", "comments": "comments"}[call["purpose"]]
            assert ev.event_id == f"apify-client:crawl-run:1234:106:{kind}:{call['apify_run_id']}"
            assert (ev.org_id, ev.user_id, ev.search_id, ev.scope) == (106, 23, 228, "org")
            assert (ev.service, ev.tool, ev.kind, ev.unit) == ("apify-client", "apify", kind, "items")
            assert ev.cost_usd == call["cost_usd"] == 0.01 and ev.estimated is False
            assert ev.quantity == call["items"]
            assert ev.raw["run_id"] == call["apify_run_id"]
        # crawl_runs cost stays the sum of the calls, as before
        assert db.runs[1234]["cost_usd"] == pytest.approx(0.02)

    def test_redelivered_record_is_one_row(self, world):
        """A replay of the same Apify run maps to the same event id (no double count)."""
        db, apify, deps, _ = world
        with _no_es():
            handle_message(json.dumps(_message()), deps)
        (ev,) = deps.usage_ledger.recorded()
        deps.usage_ledger.record(ev)
        assert len(deps.usage_ledger.recorded()) == 1

    def test_run_caps_reach_apify(self, world):
        """The run's own caps (max_items, admitted_usd / task max_cost_usd_per_run / message
        max_cost_usd) are passed through usage_client to Apify."""
        db, apify, deps, _ = world
        db.runs[1234].update(max_items=20, admitted_usd=0.06)
        db.tasks[159]["max_cost_usd_per_run"] = 0.5
        with _no_es():
            handle_message(json.dumps(_message(max_items=20, max_cost_usd=0.20)), deps)
        assert db.runs[1234]["status"] == "succeeded", db.runs[1234].get("error")
        kw = apify.calls[0]["kwargs"]
        # apify/facebook-posts-scraper's real price: $0.001 / start + $0.005 / post (incl. the
        # date-filter add-on) → min(search budget 20, floor((0.06 - 0.001) / 0.005) = 11) items,
        # charge capped to the admitted 11 x 0.005 + the start charge
        assert kw["max_items"] == 11
        assert float(kw["max_total_charge_usd"]) == pytest.approx(max(0.056, APIFY_MIN_CHARGE_CAP_USD))
        (ev,) = deps.usage_ledger.recorded()
        assert ev.raw["max_items"] == 11 and ev.raw["admitted_charge_usd"] == pytest.approx(0.056)

    def test_org_budget_caps_the_call(self, world):
        """Partial admission: the org has $0.02 left today → 4 posts at $0.005 (+ the $0.001 start)."""
        db, apify, deps, _ = world
        deps.usage_ledger.spent(106, 14.98)
        with _no_es():
            handle_message(json.dumps(_message()), deps)
        assert db.runs[1234]["status"] == "succeeded", db.runs[1234].get("error")
        assert apify.calls[0]["kwargs"]["max_items"] == 4
        assert float(apify.calls[0]["kwargs"]["max_total_charge_usd"]) == pytest.approx(max(0.021, APIFY_MIN_CHARGE_CAP_USD))

    def test_over_limit_before_any_call_blocks_the_run(self, world):
        db, apify, deps, _ = world
        db.runs[1234]["trigger"] = "schedule"
        deps.usage_ledger.spent(106, 15.0)
        with _no_es():
            assert handle_message(json.dumps(_message()), deps) == ACK
        run = db.runs[1234]
        assert apify.calls == [] and db.calls == {}
        assert run["status"] == "blocked"
        assert "AdmissionRefused(over_limit)" in run["error"]
        assert run["stats"]["usage_refusal"]["reason"] == "over_limit"
        assert run["stats"]["usage_refusal"]["kind"] == "crawl"
        assert run["cost_usd"] == 0
        (block,) = deps.usage_ledger.blocks
        assert block["path"] == "worker:apify-client" and block["reason"] == "over_limit"
        assert block["org_id"] == 106 and block["kind"] == "crawl"
        assert db.schedule_updates == []   # not a task failure: the next tick tries again

    def test_over_limit_after_a_paid_call_is_partial(self, world):
        """Search admitted, the comments call refused (a $0 comments row): money already
        spent → partial, cost kept, one block row."""
        db, apify, deps, _ = world
        db.tasks[159]["get_comments"] = True
        deps.usage_ledger.add_limit(limit_usd=0.0, kind="comments")
        with _no_es():
            assert handle_message(json.dumps(_message()), deps) == ACK
        run = db.runs[1234]
        assert run["status"] == "partial"
        assert "AdmissionRefused(over_limit) on a comments call" in run["error"]
        assert [c["purpose"] for c in db.calls.values()] == ["search"]
        assert run["cost_usd"] == pytest.approx(0.01)
        assert len(deps.usage_ledger.recorded()) == 1
        assert [b["reason"] for b in deps.usage_ledger.blocks] == ["over_limit"]

    def test_ledger_unreachable_before_any_call_releases_and_requeues(self, world):
        db, apify, deps, _ = world
        deps.usage_ledger.down = True
        with _no_es():
            assert handle_message(json.dumps(_message()), deps) == REQUEUE
        assert apify.calls == []
        assert db.runs[1234]["status"] == "queued"
        assert "ledger_unreachable" in db.runs[1234]["error"]
        assert deps.usage_ledger.blocks[0]["reason"] == "ledger_unreachable"
        # the ledger is back: the redelivered message runs normally
        deps.usage_ledger.down = False
        with _no_es():
            assert handle_message(json.dumps(_message()), deps) == ACK
        assert db.runs[1234]["status"] == "succeeded"
        assert len(deps.usage_ledger.recorded()) == 1

    def test_ledger_unreachable_and_release_failing_fails_the_run(self, world):
        db, apify, deps, _ = world
        deps.usage_ledger.down = True

        def release_down(run_id, error):
            raise ConnectionError("userdb down")

        db.release_run = release_down
        with _no_es():
            assert handle_message(json.dumps(_message()), deps) == ACK
        assert db.runs[1234]["status"] == "failed" and "ledger_unreachable" in db.runs[1234]["error"]
        assert apify.calls == []

    def test_ledger_unreachable_after_a_paid_call_is_partial(self, world):
        db, apify, deps, _ = world
        db.tasks[159]["get_comments"] = True
        real_call = apify.actor

        def actor_then_down(actor_id):
            a = real_call(actor_id)
            orig = a.call

            def call(**kw):
                out = orig(**kw)
                deps.usage_ledger.down = True  # the ledger drops after the search call
                return out

            a.call = call
            return a

        apify.actor = actor_then_down
        with _no_es():
            assert handle_message(json.dumps(_message()), deps) == ACK
        assert db.runs[1234]["status"] == "partial"
        assert "ledger_unreachable" in db.runs[1234]["error"]
        assert len(apify.calls) == 1

    def test_comments_run_meters_as_comments(self, world, sample_facebook_comments):
        db, apify, deps, _ = world
        url = sample_facebook_comments[0].get("postUrl") or sample_facebook_comments[0].get("facebookUrl")
        db.runs[77] = _run_row(run_id=77, kind="comments", task_id=None, comment_request_id=5, max_items=15)
        db.comment_posts = [{"row_id": 1, "request_id": 5, "post_url": url, "tier": "deep", "depth": 50,
                             "stored_comments": 0, "run_id": 77}]
        handle_message(json.dumps(_message(run_id=77, kind="comments", task_id=None, comment_request_id=5)), deps)
        assert db.runs[77]["status"] == "succeeded", db.runs[77].get("error")
        (ev,) = deps.usage_ledger.recorded()
        assert ev.kind == "comments" and ev.event_id.startswith("apify-client:crawl-run:77:106:comments:")
        # message max_cost_usd 0.20 → floor((0.20 - $0.001 start) / $0.002) = 99 comment items
        assert apify.calls[0]["kwargs"]["max_items"] == 99

    def test_refused_run_never_asks_again(self):
        """After a refusal the client refuses locally: one block row, no Apify call."""
        ledger = FakeLedger()
        ledger.spent(106, 15.0)
        rec = CallRecorder()
        fake = FakeApify({"a/b": [{}]})
        client = RecordingApifyClient(fake, rec, _meter(ledger))
        for _ in range(3):
            with pytest.raises(AdmissionRefused):
                client.actor("a/b").call(run_input={})
        assert fake.calls == [] and len(ledger.blocks) == 1
        assert rec.refusal.reason == "over_limit" and rec.refusal_purpose == "search"

    def test_actor_kwargs_and_caps_are_forwarded(self):
        rec = CallRecorder(search_item_budget=7)
        fake = FakeApify({"a/b": [{}]})
        RecordingApifyClient(fake, rec, _meter()).actor("a/b").call(
            run_input={"q": 1, "maxItems": 50}, memory_mbytes=256, timeout_secs=60, max_items=9)
        call = fake.calls[0]
        assert call["kwargs"]["memory_mbytes"] == 256 and call["kwargs"]["timeout_secs"] == 60
        assert call["kwargs"]["max_items"] == 7          # min(caller 9, run budget 7)
        assert call["run_input"] == {"q": 1, "maxItems": 7}

    def test_profiles_calls_meter_as_profiles(self):
        ledger = FakeLedger()
        rec = CallRecorder()
        fake = FakeApify({"a/b": [{}]})
        rec.push("profiles")
        RecordingApifyClient(fake, rec, _meter(ledger)).actor("a/b").call(run_input={})
        (ev,) = ledger.recorded()
        assert ev.kind == "profiles" and rec.calls[0].usage_event_id == ev.event_id

    def test_budget_below_one_item_stops_without_asking(self):
        ledger = FakeLedger()
        rec = CallRecorder(cost_budget_usd=0.003)
        fake = FakeApify({"a/b": [{}]})
        with pytest.raises(CostCapExceeded):
            RecordingApifyClient(fake, rec, _meter(ledger)).actor("a/b").call(run_input={})
        assert fake.calls == [] and ledger.blocks == []


# --------------------------------------------------------------------------- params & caps


class TestKwargs:
    def _task(self, **over):
        return CrawlTask.from_db_row(_task_row(**over))

    def test_kwargs_come_from_to_actor_kwargs(self):
        task = self._task(not_keywords=["x"], llm_filter_condition="c")
        kwargs, max_items = build_crawl_kwargs(task, _run_row(max_items=20))
        base = task.to_actor_kwargs()
        assert set(base) <= set(kwargs)
        assert kwargs["max_results"] == 20 == max_items
        assert kwargs["not_keywords"] == ["x"] and kwargs["task_id"] == "db:159"

    def test_max_items_never_exceeds_task_ceiling(self):
        kwargs, max_items = build_crawl_kwargs(self._task(max_results=10), _run_row(max_items=500))
        assert max_items == 10 == kwargs["max_results"]

    def test_result_count_actor_params_are_clamped(self):
        task = self._task(actor_params={"results_limit": 900})
        kwargs, _ = build_crawl_kwargs(task, _run_row(max_items=25))
        assert kwargs["results_limit"] == 25

    def test_window_since_replaces_task_window(self):
        since = datetime(2026, 10, 1, tzinfo=timezone.utc)
        kwargs, _ = build_crawl_kwargs(self._task(period="w"), _run_row(window_since=since))
        assert kwargs["min_date"] == since and kwargs["period"] is None

    def test_task_window_kept_without_run_window(self):
        kwargs, _ = build_crawl_kwargs(self._task(period="d"), _run_row())
        assert kwargs["period"] == "d" and kwargs["min_date"] is None

    def test_no_window_sends_no_date_filter(self):
        """§2.8 R2: no min_date/period → no date filter, also not google_news' 1d default."""
        task = self._task(actor_class="google_news", search_params=["pinal"], actor_params={})
        kwargs, _ = build_crawl_kwargs(task, _run_row())
        assert kwargs["min_date"] is None and kwargs["period"] is None
        assert kwargs["timeframe"] == "all"

    def test_explicit_timeframe_is_kept(self):
        task = self._task(actor_class="google_news", search_params=["pinal"], actor_params={"timeframe": "7d"})
        kwargs, _ = build_crawl_kwargs(task, _run_row())
        assert kwargs["timeframe"] == "7d"

    def test_no_window_reaches_actor_input(self):
        """The Apify input carries no date key when the task has no window."""
        rec = CallRecorder()
        fake = FakeApify({"scrapeforge/facebook-search-posts": []})
        actor = get_actor("facebook_keyword_search", client=RecordingApifyClient(fake, rec, _meter()))
        task = self._task(actor_class="facebook_keyword_search", search_params=["pinal de amoles"], actor_params={})
        kwargs, _ = build_crawl_kwargs(task, _run_row())
        actor.search(task.search_params, **kwargs)
        assert "start_date" not in fake.calls[0]["run_input"]

    def test_override_filters_from_run_params(self):
        kwargs, _ = build_crawl_kwargs(self._task(), _run_row(params={"override_filters": True}))
        assert kwargs["override_filters"] is True

    def test_override_filters_default_false(self):
        kwargs, _ = build_crawl_kwargs(self._task(), _run_row(params={}))
        assert kwargs["override_filters"] is False


class TestFromDbRow:
    def test_legacy_task_id_is_the_filter_cache_key(self):
        assert CrawlTask.from_db_row(_task_row(legacy_task_id="34.a")).task_id == "34.a"
        assert CrawlTask.from_db_row(_task_row()).task_id == "db:159"

    def test_min_date_and_period_exclusive(self):
        with pytest.raises(ValueError):
            CrawlTask.from_db_row(_task_row(min_date=datetime(2026, 1, 1), period="d"))

    def test_fields(self):
        task = CrawlTask.from_db_row(_task_row(not_keywords=["a", "b"], status="paused", legacy_theme="sjdr"))
        assert task.not_keywords == ["a", "b"] and task.enabled is False and task.theme == "sjdr"


# --------------------------------------------------------------------------- ad-hoc runs (§6.4)

# Cross-repo test vector: media-backend-paid tests/unit/test_crawl.py pins the same key for
# the same spec (backend canonical form: every writable column, min_date as UTC ISO).
ADHOC_VECTOR_SPEC = {
    "name": "", "country_id": None, "language": None, "max_results": 30, "min_date": "2026-10-01T06:00:00+00:00",
    "period": None, "publish": True, "get_comments": False, "max_comments": 15, "not_keywords": ["fútbol"],
    "llm_filter_condition": "Solo Querétaro", "update_existing": False, "enrich_followers": False,
    "fetch_attached_url": False, "classify_users": False, "official_page": False,
    "actor_params": {"get_comments_after_likes": 100}, "max_cost_usd_per_run": None, "metadata": {},
    "actor_class": "facebook_keyword_search", "search_params": ["alfa", "zeta"],
}
ADHOC_VECTOR_KEY = "adhoc:dea0c96527bc7d8bd769211e88f1bd157ce2b839"


def _spec(**over) -> dict:
    spec = copy.deepcopy(ADHOC_VECTOR_SPEC)
    spec.update(actor_class="facebook_page_posts", search_params=[FB_PAGE], min_date=None, not_keywords=[],
                llm_filter_condition=None, actor_params={"add_text_from_images": False})
    spec.update(over)
    return spec


def _adhoc_world(world, spec=None, **run_over):
    db, apify, deps, published = world
    db.tasks.clear()                                     # an ad-hoc run reads no task row
    spec = spec or _spec()
    db.runs[1234] = _run_row(**{"kind": "adhoc", "task_id": None,
                                "params": {"override_filters": False, "task_id": adhoc_cache_key(spec), "spec": spec},
                                **run_over})
    msg = _message(kind="adhoc", task_id=None)
    msg["params"] = dict(db.runs[1234]["params"])
    return db, apify, deps, published, msg


class TestFromSpec:
    def test_cache_key_vector_matches_the_backend(self):
        assert adhoc_cache_key(ADHOC_VECTOR_SPEC) == ADHOC_VECTOR_KEY
        assert CrawlTask.from_spec(ADHOC_VECTOR_SPEC).task_id == ADHOC_VECTOR_KEY
        # same instant in another offset / params unsorted → same key; volume fields ignored
        same = {**ADHOC_VECTOR_SPEC, "min_date": "2026-10-01T00:00:00-06:00", "search_params": ["zeta", "alfa"],
                "max_results": 999, "publish": False, "name": "x", "metadata": {"a": 1}, "max_cost_usd_per_run": 1}
        assert adhoc_cache_key(same) == ADHOC_VECTOR_KEY
        assert adhoc_cache_key({**ADHOC_VECTOR_SPEC, "not_keywords": []}) != ADHOC_VECTOR_KEY

    def test_shares_the_db_row_param_path(self):
        spec = {**ADHOC_VECTOR_SPEC, "language": "es", "max_results": 40}
        task = CrawlTask.from_spec(spec)
        row = {**spec, "task_id": 1, "legacy_task_id": adhoc_cache_key(spec),
               "min_date": datetime(2026, 10, 1, 6, tzinfo=timezone.utc), "status": "active"}
        assert task.to_actor_kwargs() == CrawlTask.from_db_row(row).to_actor_kwargs()
        kwargs = task.to_actor_kwargs()
        assert kwargs["min_date"] == datetime(2026, 10, 1, 6, tzinfo=timezone.utc)
        assert kwargs["task_id"] == adhoc_cache_key(spec) != ADHOC_VECTOR_KEY and kwargs["get_comments_after_likes"] == 100
        assert task.enabled is True and task.override_filters is False

    @pytest.mark.parametrize("bad", [
        None, "x", {"search_params": ["a"]}, {"actor_class": "google_news", "search_params": []},
        {"actor_class": "google_news", "search_params": "a,b"},
        {"actor_class": "google_news", "search_params": ["a"], "org_id": 1},
        {"actor_class": "google_news", "search_params": ["a"], "period": "w", "min_date": "2026-10-01T00:00:00+00:00"},
        {"actor_class": "google_news", "search_params": ["a"], "min_date": "yesterday"},
        {"actor_class": "google_news", "search_params": ["a"], "period": "y"},
    ])
    def test_refuses_bad_specs(self, bad):
        with pytest.raises(ValueError):
            CrawlTask.from_spec(bad)


class TestAdhocRuns:
    def test_adhoc_run_is_built_from_the_run_row_spec(self, world, sample_facebook_posts_results):
        db, apify, deps, published, msg = _adhoc_world(world)
        with _no_es():
            assert handle_message(json.dumps(msg), deps) == ACK
        run = db.runs[1234]
        assert run["status"] == "succeeded", run.get("error")
        assert run["items_paid"] == 4 and run["items_new"] == len(published) == 4
        assert run["cost_usd"] == pytest.approx(0.01) and len(db.calls) == 1
        assert run["stats"]["effective_params"]["task_id"] == adhoc_cache_key(_spec())
        assert "message_params_diff" not in run["stats"]
        assert db.schedule_updates == []                               # no schedule bookkeeping
        # metered like any run: one usage event under the run's org, kind crawl
        (event,) = deps.usage_ledger.recorded()
        assert event.org_id == 106 and event.kind == "crawl" and event.event_id.startswith("apify-client:crawl-run:1234:")

    def test_db_spec_wins_over_the_message(self, world):
        db, apify, deps, _, msg = _adhoc_world(world)
        msg["params"]["spec"] = _spec(max_comments=99)                  # stale / forged copy
        msg["params"]["max_comments"] = 99
        with _no_es():
            handle_message(json.dumps(msg), deps)
        run = db.runs[1234]
        assert run["stats"]["effective_params"]["max_comments"] == 15
        assert run["stats"]["message_params_diff"] == ["max_comments"]

    def test_budgets_come_from_the_spec_and_the_run(self, world):
        db, apify, deps, _, msg = _adhoc_world(world, spec=_spec(max_cost_usd_per_run=0.05, max_results=40,
                                                                  search_params=[FB_PAGE, FB_PAGE + "x"]),
                                               max_items=25, admitted_usd=0.08)
        plan = runner_mod.plan_run(db.runs[1234], _message(max_cost_usd=0.5), deps)
        assert plan.cost_budget_usd == pytest.approx(0.05)
        assert plan.search_item_budget == 25 * 2 and plan.kwargs["max_results"] == 25 and plan.task_id is None

    def test_same_spec_reuses_cached_filter_decisions(self, world, sample_facebook_posts_results):
        db, apify, deps, _, msg = _adhoc_world(world)
        seen = sample_facebook_posts_results[0]["url"]
        with _no_es([seen]):
            handle_message(json.dumps(msg), deps)
        assert db.runs[1234]["items_already_seen"] == 1
        # the same ad-hoc crawl again (another run): the ES drop is now a cached drop
        db.runs[1235] = {**copy.deepcopy(db.runs[1234]), "run_id": 1235, "status": "queued", "stats": {}}
        with _no_es():
            handle_message(json.dumps({**msg, "run_id": 1235}), deps)
        assert db.runs[1235]["items_cached_filtered"] == 1 and db.runs[1235]["items_already_seen"] == 0
        # a different spec is a different cache scope
        other = _spec(not_keywords=["zzz-nunca"])
        db.runs[1236] = {**copy.deepcopy(db.runs[1234]), "run_id": 1236, "status": "queued", "stats": {},
                         "params": {"override_filters": False, "spec": other}}
        with _no_es():
            handle_message(json.dumps({**msg, "run_id": 1236}), deps)
        assert db.runs[1236]["items_cached_filtered"] == 0

    @pytest.mark.parametrize("params,why", [
        ({"override_filters": False}, "bad params.spec"),
        ({"spec": {"actor_class": "facebook_page_posts"}}, "bad params.spec"),
        ({"spec": {**ADHOC_VECTOR_SPEC, "org_id": 7}}, "unknown field"),
        ({"spec": {**ADHOC_VECTOR_SPEC, "actor_class": "facebook_post_comments"}}, "not crawl"),
        ({"spec": {**ADHOC_VECTOR_SPEC, "actor_class": "nope"}}, "not enabled"),
        ({"spec": {**ADHOC_VECTOR_SPEC, "actor_params": {"apify_input": {"maxItems": 5000}}}}, "lift"),
    ])
    def test_bad_adhoc_runs_fail_without_spend(self, world, params, why):
        db, apify, deps, _, msg = _adhoc_world(world)
        db.runs[1234]["params"] = params
        handle_message(json.dumps(msg), deps)
        run = db.runs[1234]
        assert run["status"] == "failed" and why in run["error"], run["error"]
        assert apify.calls == [] and db.calls == {}

    def test_adhoc_run_with_a_task_id_is_rejected(self, world):
        db, apify, deps, _, msg = _adhoc_world(world, task_id=159)
        handle_message(json.dumps(msg), deps)
        assert db.runs[1234]["status"] == "failed" and apify.calls == []

    def test_adhoc_over_limit_blocks(self, world):
        db, apify, deps, _, msg = _adhoc_world(world)
        deps.usage_ledger.spent(106, 15.0)
        handle_message(json.dumps(msg), deps)
        assert db.runs[1234]["status"] == "blocked" and apify.calls == []

    def test_missing_org_block_uses_the_crawl_usage_kind(self, world):
        db, apify, deps, _, msg = _adhoc_world(world)
        handle_message(json.dumps({**msg, "org_id": None}), deps)
        (block,) = deps.usage_ledger.blocks
        assert block["kind"] == "crawl" and block["reason"] == "missing_org"
        assert db.runs[1234]["status"] == "failed"


class _Model:
    """apify_client 3.x shape: a pydantic model (snake_case attributes, no ``.get``)."""

    def __init__(self, **by_alias):
        self._data = by_alias

    def model_dump(self, *, by_alias=False, mode="python", exclude_none=False):
        assert by_alias
        return dict(self._data)


class FakeApifyModels(FakeApify):
    """FakeApify returning models, as apify_client 3.0.2 (requirements.txt) does."""

    def actor(self, actor_id):
        inner = super().actor(actor_id)

        class _A:
            def call(self, run_input=None, **kwargs):
                return _Model(**inner.call(run_input=run_input, **kwargs))

        return _A()

    def dataset(self, dataset_id):
        inner = super().dataset(dataset_id)

        class _D:
            def get(self):
                return _Model(**inner.get())

            def iterate_items(self):
                return inner.iterate_items()

        return _D()


class TestRecordingClient:
    def test_apify_client_3_models_record_items_and_cost(self, sample_facebook_posts_results):
        """apify_client 3.x returns models: the call row still gets its items, cost and
        usage_event_id (the Wave B integration run recorded items=NULL before the fix)."""
        rec = CallRecorder(search_item_budget=3)
        fake = FakeApifyModels({"a/b": sample_facebook_posts_results})
        run = RecordingApifyClient(fake, rec, _meter()).actor("a/b").call(run_input={})
        (call,) = rec.calls
        assert call.items == 3 and call.cost_usd == 0.01
        assert call.usage_event_id and call.usage_event_id.endswith(call.apify_run_id)
        assert run["defaultDatasetId"]          # the actors read the run as a dict

    def test_search_calls_get_apify_max_items_and_budget(self, sample_facebook_posts_results):
        rec = CallRecorder(search_item_budget=3)
        fake = FakeApify({"a/b": sample_facebook_posts_results})
        client = RecordingApifyClient(fake, rec, _meter())
        client.actor("a/b").call(run_input={})
        assert fake.calls[0]["kwargs"]["max_items"] == 3
        assert rec.calls[0].items == 3
        with pytest.raises(ItemCapExceeded):
            client.actor("a/b").call(run_input={})
        assert len(fake.calls) == 1

    def test_enrichment_calls_not_capped_by_the_search_budget(self):
        """The run's search-item budget doesn't apply to enrichment calls; usage_client
        still caps them (DEFAULT_MAX_ITEMS when nothing tighter is known)."""
        rec = CallRecorder(search_item_budget=0)
        fake = FakeApify({"a/b": [{}]})
        rec.push("comments")
        RecordingApifyClient(fake, rec, _meter()).actor("a/b").call(run_input={})
        assert fake.calls[0]["kwargs"]["max_items"] == 1000

    def test_cost_budget(self):
        rec = CallRecorder(cost_budget_usd=0.015)
        fake = FakeApify({"a/b": [{}]}, cost_per_call=0.01)
        client = RecordingApifyClient(fake, rec, _meter())
        client.actor("a/b").call(run_input={})
        # usage_client asks for floor(0.015 / 0.004) = 3 crawl items and caps the charge to
        # what admission granted for them (3 x 0.004).
        assert fake.calls[0]["kwargs"]["max_items"] == 3
        assert float(fake.calls[0]["kwargs"]["max_total_charge_usd"]) == pytest.approx(max(0.012, APIFY_MIN_CHARGE_CAP_USD))
        client.actor("a/b").call(run_input={})
        assert fake.calls[1]["kwargs"]["max_items"] == 1  # 0.005 left
        with pytest.raises(CostCapExceeded):
            client.actor("a/b").call(run_input={})

    def test_missing_usage_is_reread_once(self):
        rec = CallRecorder()

        class F(FakeApify):
            def run(self, run_id):
                class _R:
                    def get(self_inner):
                        return {"usageTotalUsd": 0.042}

                return _R()

        fake = F({"a/b": []}, cost_per_call=None)
        RecordingApifyClient(fake, rec, _meter()).actor("a/b").call(run_input={})
        assert rec.calls[0].cost_usd == pytest.approx(0.042)

    def test_instrument_actor_tags_stage_calls(self):
        rec = CallRecorder()

        class Dummy:
            def _enrich_user_author(self, docs, **kw):
                return rec.purpose

            def _enrich_comments(self, docs, **kw):
                return rec.purpose

        d = Dummy()
        instrument_actor(d, rec)
        assert d._enrich_user_author([]) == "profiles"
        assert d._enrich_comments([]) == "comments"
        assert rec.purpose == "search"


class TestFunnelCounters:
    def test_drop_reasons_counted(self, sample_facebook_posts_results):
        from src.models.facebook_post import FacebookPost

        actor = get_actor("facebook_page_posts", client=FakeApify({}))
        posts = [FacebookPost.from_facebook(i) for i in sample_facebook_posts_results]
        body_word = (posts[0].data.get("body") or "").split()[0]
        with _no_es([posts[1].data["url"]]):
            out = actor.process_documents(posts, not_keywords=[body_word], add_text_from_images=False)
        assert actor.funnel["docs_in"] == 4
        assert actor.funnel["dropped_keyword"] >= 1
        assert actor.funnel["dropped_existing_elasticsearch"] + actor.funnel["dropped_keyword"] + len(out) == 4
        assert actor.funnel_newest >= actor.funnel_oldest


class TestSpool:
    def test_failed_cost_write_is_spooled_and_replayed(self, tmp_path):
        db = CrawlDB("postgresql://nobody@127.0.0.1:1/none", spool_path=str(tmp_path / "spool.jsonl"))
        applied: List[tuple] = []
        state = {"down": True}

        def apply(op, payload):
            if state["down"]:
                raise ConnectionError("down")
            applied.append((op, payload))

        db._apply = apply
        db.insert_call({"run_id": 1, "apify_run_id": "r1", "purpose": "search", "apify_actor_id": "a",
                        "started_at": datetime(2026, 10, 6, tzinfo=timezone.utc)})
        db.finish_run(1, "succeeded", None, None, {"cost_usd": 0.01}, {"x": 1})
        lines = open(tmp_path / "spool.jsonl").read().splitlines()
        assert [json.loads(ln)["op"] for ln in lines] == ["insert_call", "finish_run"]
        assert db.replay_spool() == 0  # still down: kept
        assert len(open(tmp_path / "spool.jsonl").read().splitlines()) == 2
        state["down"] = False
        assert db.replay_spool() == 2
        assert [op for op, _ in applied] == ["insert_call", "finish_run"]
        assert open(tmp_path / "spool.jsonl").read() == ""

    def test_spooled_finish_never_fails_the_run(self, world, tmp_path):
        db, apify, deps, _ = world
        real_finish = db.finish_run
        spooled = []

        def failing_finish(*args, **kwargs):
            spooled.append(args)  # a real CrawlDB spools here instead of raising

        db.finish_run = failing_finish
        with _no_es():
            assert handle_message(json.dumps(_message()), deps) == ACK
        assert spooled and spooled[0][1] == "succeeded"
        db.finish_run = real_finish
