"""Crawl-run queue worker (src/worker/*, docs/db/crawl_tasks.md §6.1).

No network: Apify is a fake client (no paid calls), userdb is an in-memory fake, the
document publisher is a list. The DB-backed counterpart (real SQL as crawler_worker)
is test_crawl_worker_db.py.
"""
from __future__ import annotations

import copy
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

from src.actors import get_actor
from src.models.crawl_task import CrawlTask
from src.worker import runner as runner_mod
from src.worker.db import CrawlDB
from src.worker.recording import (
    CallRecorder,
    CostCapExceeded,
    ItemCapExceeded,
    RecordingApifyClient,
    instrument_actor,
)
from src.worker.runner import ACK, REQUEUE, WorkerDeps, build_crawl_kwargs, handle_message

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
                return {
                    "id": f"apifyrun-{actor_id.replace('/', '_')}-{n}-{uuid.uuid4().hex[:8]}",
                    "status": "SUCCEEDED",
                    "defaultDatasetId": f"ds-{n}",
                    "usageTotalUsd": fake.cost_per_call,
                    "usage": {"COMPUTE_UNITS": 0.001},
                    "startedAt": "2026-10-06T12:00:00.000Z",
                    "finishedAt": "2026-10-06T12:00:05.000Z",
                }

        return _A()

    def dataset(self, dataset_id):
        items = self.datasets.get(dataset_id, [])

        class _D:
            def get(self):
                return {"itemCount": len(items)}

            def iterate_items(self):
                return iter(items)

        return _D()


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
            return
        run.update(status=status, error=error, output_path=output_path, **counters)
        run["stats"] = {**run.get("stats", {}), **json.loads(json.dumps(stats, default=str))}
        self.finished.append(run_id)

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

    def test_schedule_bookkeeping(self, world):
        db, apify, deps, _ = world
        db.runs[1234]["trigger"] = "schedule"
        with _no_es():
            handle_message(json.dumps(_message()), deps)
        assert db.schedule_updates == [(159, 1234, True)]

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
        actor = get_actor("facebook_keyword_search", client=RecordingApifyClient(fake, rec))
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


class TestRecordingClient:
    def test_search_calls_get_apify_max_items_and_budget(self, sample_facebook_posts_results):
        rec = CallRecorder(search_item_budget=3)
        fake = FakeApify({"a/b": sample_facebook_posts_results})
        client = RecordingApifyClient(fake, rec)
        client.actor("a/b").call(run_input={})
        assert fake.calls[0]["kwargs"]["max_items"] == 3
        assert rec.calls[0].items == 3
        with pytest.raises(ItemCapExceeded):
            client.actor("a/b").call(run_input={})
        assert len(fake.calls) == 1

    def test_enrichment_calls_not_item_capped(self):
        rec = CallRecorder(search_item_budget=0)
        fake = FakeApify({"a/b": [{}]})
        rec.push("comments")
        RecordingApifyClient(fake, rec).actor("a/b").call(run_input={})
        assert "max_items" not in fake.calls[0]["kwargs"]

    def test_cost_budget(self):
        rec = CallRecorder(cost_budget_usd=0.015)
        fake = FakeApify({"a/b": [{}]}, cost_per_call=0.01)
        client = RecordingApifyClient(fake, rec)
        client.actor("a/b").call(run_input={})
        assert float(fake.calls[0]["kwargs"]["max_total_charge_usd"]) == pytest.approx(0.015)
        client.actor("a/b").call(run_input={})
        with pytest.raises(CostCapExceeded):
            client.actor("a/b").call(run_input={})

    def test_usage_runner_replaces_raw_call(self):
        rec = CallRecorder()
        fake = FakeApify({})
        fake.datasets["ds-u"] = [{}, {}]
        seen = []

        def runner(actor_id, run_input):
            seen.append((actor_id, run_input))
            return {"id": "u-1", "status": "SUCCEEDED", "defaultDatasetId": "ds-u", "usageTotalUsd": 0.5,
                    "usage_event_id": "apify-client:u-1:106:crawl:0"}

        RecordingApifyClient(fake, rec, usage_runner=runner).actor("a/b").call(run_input={"q": 1})
        assert seen == [("a/b", {"q": 1})] and fake.calls == []
        assert rec.calls[0].cost_usd == 0.5 and rec.calls[0].items == 2
        assert rec.calls[0].usage_event_id == "apify-client:u-1:106:crawl:0"

    def test_missing_usage_is_reread_once(self):
        rec = CallRecorder()

        class F(FakeApify):
            def run(self, run_id):
                class _R:
                    def get(self_inner):
                        return {"usageTotalUsd": 0.042}

                return _R()

        fake = F({"a/b": []}, cost_per_call=None)
        RecordingApifyClient(fake, rec).actor("a/b").call(run_input={})
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
