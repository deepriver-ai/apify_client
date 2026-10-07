"""Apify cost settling in the worker, the publish-failure tolerance, dropped attached
articles and the cost reconciliation script (2026-10-07 fixes). No network; recorded
Apify run payloads in src/tests/cache/apify_runs (GET /v2/actor-runs/{id}, trimmed)."""
from __future__ import annotations

import copy
import json
import os
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

from src.tests.test_crawl_worker import FakeApify, FakeLedger, _message, _meter, _no_es, world  # noqa: F401
from src.worker.recording import CallRecord, CallRecorder, RecordingApifyClient
from src.worker.runner import PublishResult, _publish_all, handle_message, run_outcome
from usage_client import apify_cost

RUNS_DIR = os.path.join(os.path.dirname(__file__), "cache", "apify_runs")
SEARCH_RUN = "Oy7YlPPjj7gF8Q203"      # scrapeforge/facebook-search-posts, 29 results, $0.07516
COMMENTS_RUN = "d1RU4R2koLkL2HYbE"    # apify/facebook-comments-scraper, 6 comments, $0.013


def recorded(run_id: str) -> Dict[str, Any]:
    with open(os.path.join(RUNS_DIR, f"run_{run_id}.json"), encoding="utf-8") as f:
        return json.load(f)


def lagging(run: Dict[str, Any]) -> Dict[str, Any]:
    """The run as read when call() returned on 2026-10-07: only the start charged."""
    r = copy.deepcopy(run)
    start = {k: v for k, v in r["chargedEventCounts"].items() if "start" in k}
    r["chargedEventCounts"] = r["accountedChargedEventCounts"] = start
    r["usageTotalUsd"] = apify_cost.cost_from_events(start, r["pricingInfo"])
    r.pop("eventUsage", None)
    return r


class ReplayApify:
    """``call()`` returns the lagging run; ``run(id).get()`` replays ``reads`` (last one repeats)."""

    def __init__(self, run: Dict[str, Any], reads: List[Dict[str, Any]], items: int):
        self.run_obj, self.reads, self.items, self.gets, self.calls = run, reads, items, 0, []

    def actor(self, actor_id):
        outer = self

        class _A:
            def call(self, run_input=None, **kw):
                outer.calls.append(kw)
                return copy.deepcopy(outer.run_obj)

        return _A()

    def run(self, run_id):
        outer = self

        class _R:
            def get(self):
                outer.gets += 1
                return copy.deepcopy(outer.reads[min(outer.gets - 1, len(outer.reads) - 1)])

        return _R()

    def dataset(self, ds):
        outer = self

        class _D:
            def get(self):
                return {"itemCount": outer.items}

        return _D()


# --------------------------------------------------------------------- worker recorder


class TestSettledCostInTheRecorder:
    def test_call_row_gets_the_settled_cost_not_the_start_charge(self):
        settled = recorded(SEARCH_RUN)
        lag = lagging(settled)
        fake = ReplayApify(lag, [lag, lag, settled, settled], items=29)
        rec, ledger = CallRecorder(), FakeLedger()
        RecordingApifyClient(fake, rec, _meter(ledger)).actor("scrapeforge/facebook-search-posts").call(run_input={})
        (call,) = rec.calls
        assert lag["usageTotalUsd"] == pytest.approx(0.00005)                 # what was recorded that night
        assert (call.cost_usd, call.estimated, call.items) == (pytest.approx(0.07516), False, 29)
        assert call.usage["chargedEventCounts"] == {"apify-actor-start": 1, "apify-default-dataset-item": 29}
        assert call.usage["settled"] is True
        (ev,) = [e for e in ledger.events.values()]
        assert ev.cost_usd == call.cost_usd and ev.event_id == call.usage_event_id    # one reading, two records

    def test_unsettled_call_is_estimated_and_spooled_for_reconciliation(self):
        lag = lagging(recorded(COMMENTS_RUN))
        fake = ReplayApify(lag, [lag], items=6)
        rec = CallRecorder(default_purpose="comments")
        RecordingApifyClient(fake, rec, _meter()).actor("apify/facebook-comments-scraper").call(run_input={})
        (call,) = rec.calls
        # our own figure: start $0.001 + 6 comments × $0.002 (the settled bill is $0.013)
        assert call.estimated and call.cost_usd == pytest.approx(0.013) and call.usage["settled"] is False
        assert [l["apify_run_id"] for l in apify_cost.read_reconcile_spool()] == [COMMENTS_RUN]

    def test_below_one_run_of_the_actor_is_not_attempted(self):
        """The 'budget left' guard uses the actor's start + one result price."""
        from src.worker.recording import CostCapExceeded

        rec = CallRecorder(cost_budget_usd=0.0025, default_purpose="comments")
        fake = ReplayApify(recorded(COMMENTS_RUN), [recorded(COMMENTS_RUN)], items=6)
        with pytest.raises(CostCapExceeded, match="start 0.001"):
            RecordingApifyClient(fake, rec, _meter()).actor("apify/facebook-comments-scraper").call(run_input={})
        assert fake.calls == []


# --------------------------------------------------------------------- publish tolerance


class _Doc:
    def __init__(self, url, attached=None, bad=False):
        self.data = {"url": url}
        self.attached_news = attached
        self.bad = bad

    def to_final_schema(self):
        if self.bad:
            raise ValueError("Missing required field: body")
        return {"type": "news", "message": self.data}


def _rec(status="SUCCEEDED", purpose="search") -> CallRecorder:
    rec = CallRecorder()
    rec.calls.append(CallRecord(purpose=purpose, apify_actor_id="a", apify_run_id="r", apify_status=status,
                                items=10, cost_usd=0.01))
    return rec


class TestPublishTolerance:
    def test_outcome_rule(self):
        assert run_outcome(_rec(), 0, 20) == ("succeeded", None)
        assert run_outcome(_rec(), 1, 3) == ("succeeded", None)          # max(1, 10%) = 1
        assert run_outcome(_rec(), 2, 20) == ("succeeded", None)         # 10% of 20
        status, error = run_outcome(_rec(), 3, 20)
        assert status == "partial" and error == "3 of 20 document(s) failed to publish"
        assert run_outcome(_rec(), 2, 3)[0] == "partial"
        assert run_outcome(_rec(status="FAILED"), 1, 20)[0] == "partial"  # the search itself didn't succeed

    def test_one_failed_document_keeps_the_run_succeeded(self, world):
        db, apify, deps, published = world
        failing = []

        def publish(doc):
            if not failing:
                failing.append(doc.data["url"])
                raise ValueError("schema says no")
            published.append(doc)

        deps.publish = publish
        with _no_es():
            handle_message(json.dumps(_message()), deps)
        run = db.runs[1234]
        assert run["status"] == "succeeded" and run["error"] is None
        stats = run["stats"]
        assert stats["documents_failed"] == 1 and stats["documents_failed_urls"] == failing
        assert stats["documents_to_publish"] == len(published) + 1
        assert run["items_new"] == len(published)

    def test_unserializable_attached_article_is_dropped_not_failed(self):
        sent = []
        docs = [_Doc("https://fb.com/p1", attached=_Doc("http://3.er/", bad=True)),
                _Doc("https://fb.com/p2", attached=_Doc("https://news.example/a"))]
        r = _publish_all(docs, sent.append)
        assert isinstance(r, PublishResult)
        assert (r.main_ok, r.attached_ok, r.failures, r.attached_dropped, r.to_publish) == (2, 1, 0, 1, 3)
        assert [d.data["url"] for d in sent] == ["https://fb.com/p1", "https://fb.com/p2", "https://news.example/a"]

    def test_failed_urls_are_truncated_and_capped(self):
        from src.worker import runner

        docs = [_Doc("https://x/" + "a" * 400 + str(i)) for i in range(30)]

        def boom(doc):
            raise ValueError("nope")

        r = _publish_all(docs, boom)
        assert r.failures == 30 and len(r.failed_urls) == runner.FAILED_URLS_KEPT
        assert all(len(u) <= runner.FAILED_URL_MAX_LEN for u in r.failed_urls)


# --------------------------------------------------------------------- attached article


class TestAttachedArticleWithEmptyBody:
    def _post(self):
        from src.models.facebook_post import FacebookPost

        post = FacebookPost()
        post.data.update(url="https://www.facebook.com/x/posts/1", body="Mira esto http://3.er/",
                         timestamp="2026-10-07T06:00:00")
        post._raw = {}
        return post

    def test_empty_body_article_is_not_attached(self):
        from src.models.news import News

        empty = News()
        empty.data.update(url="http://3.er/", type="news", title=None, body=None)
        post = self._post()
        with patch("src.models.news.News.from_url", return_value=empty):
            post.fetch_attached_url("http://3.er/")
        assert post.attached_news is None
        assert "attached_url_text" not in post.data["body"]

    def test_article_with_body_is_attached(self):
        from src.models.news import News

        article = News()
        article.data.update(url="https://news.example/a", type="news", title="T", body="Cuerpo " * 50)
        post = self._post()
        with patch("src.models.news.News.from_url", return_value=article):
            post.fetch_attached_url("https://news.example/a")
        assert post.attached_news is article and "attached_url_text" in post.data["body"]


# --------------------------------------------------------------------- reconcile script


class FakeRunsClient:
    def __init__(self, runs: Dict[str, Dict[str, Any]], items: Dict[str, int]):
        self.runs, self.items = runs, items

    def run(self, run_id):
        outer = self

        class _R:
            def get(self):
                return copy.deepcopy(outer.runs.get(run_id))

        return _R()

    def dataset(self, ds):
        outer = self

        class _D:
            def get(self):
                return {"itemCount": outer.items.get(ds)}

        return _D()


def _call_row(call_id, apify_run_id, cost, estimated=False, items=None):
    return {"call_id": call_id, "run_id": 1, "purpose": "search", "apify_actor_id": "a", "apify_run_id": apify_run_id,
            "items": items, "cost_usd": cost, "estimated": estimated, "usage_event_id": f"e:{apify_run_id}",
            "run_status": "succeeded"}


class TestReconcileCosts:
    def test_check_call_settles_the_start_only_row(self):
        from src.scripts.reconcile_costs import check_call

        run = recorded(SEARCH_RUN)
        client = FakeRunsClient({SEARCH_RUN: run}, {run["defaultDatasetId"]: 29})
        fix = check_call(_call_row(2, SEARCH_RUN, 0.00005, items=29), client)
        assert (fix.status, fix.new_cost_usd) == ("corrected", pytest.approx(0.07516))
        assert fix.delta_usd == pytest.approx(0.07511) and fix.usage["settled"] is True

    def test_check_call_ok_and_unsettled(self):
        from src.scripts.reconcile_costs import check_call

        run = recorded(SEARCH_RUN)
        client = FakeRunsClient({SEARCH_RUN: run, "RUNNING1": dict(run, status="RUNNING")},
                                {run["defaultDatasetId"]: 29})
        assert check_call(_call_row(2, SEARCH_RUN, 0.07516, items=29), client).status == "ok"
        assert check_call(_call_row(2, SEARCH_RUN, 0.07516, estimated=True, items=29), client).status == "corrected"
        assert check_call(_call_row(3, "RUNNING1", 0.0), client).status == "unsettled"
        assert check_call(_call_row(4, "GONE", 0.0), client).status == "missing_run"
