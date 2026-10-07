"""Replay republish (src/scripts/republish_run.py) and the publisher's reconnect (2026-10-07).

No network: a fake Apify client serves recorded runs/datasets; pika is faked."""
from __future__ import annotations

import importlib
import sys
import types

import pytest

from src.scripts.republish_run import ReplayApifyClient, ReplayMiss, replay_run


class FakeReal:
    """The real ApifyClient's read side: runs + datasets. Starting an actor is a test failure."""

    def __init__(self, runs, datasets):
        self.runs, self.datasets = runs, datasets

    def run(self, run_id):
        real = self

        class _R:
            def get(self):
                return real.runs.get(run_id)

        return _R()

    def dataset(self, ds):
        items = self.datasets.get(ds, [])

        class _D:
            def iterate_items(self):
                return iter(items)

        return _D()

    def actor(self, actor_id):  # pragma: no cover — must never be reached
        raise AssertionError("replay started a real actor")


CALLS = [{"call_id": 1, "purpose": "search", "apify_actor_id": "scrapeforge/facebook-search-posts", "apify_run_id": "r1"},
         {"call_id": 2, "purpose": "search", "apify_actor_id": "scrapeforge/facebook-search-posts", "apify_run_id": "r2"},
         {"call_id": 3, "purpose": "comments", "apify_actor_id": "apify/facebook-comments-scraper", "apify_run_id": "r3"}]
REAL = FakeReal({"r1": {"defaultDatasetId": "d1"}, "r2": {"defaultDatasetId": "d2"}, "r3": {"defaultDatasetId": "d3"}},
                {"d1": [{"n": 1}], "d2": [{"n": 2}], "d3": [{"c": 1}]})


def test_serves_recorded_runs_in_order_per_actor():
    c = ReplayApifyClient(REAL, CALLS)
    assert c.actor("scrapeforge~facebook-search-posts").call(run_input={})["defaultDatasetId"] == "d1"
    assert c.actor("apify/facebook-comments-scraper").call(run_input={})["defaultDatasetId"] == "d3"
    assert c.actor("scrapeforge/facebook-search-posts").call(run_input={})["defaultDatasetId"] == "d2"
    assert list(c.dataset("d2").iterate_items()) == [{"n": 2}]
    assert [s["apify_run_id"] for s in c.served] == ["r1", "r3", "r2"]


def test_never_starts_an_unrecorded_actor():
    c = ReplayApifyClient(REAL, CALLS[:1])
    c.actor("scrapeforge/facebook-search-posts").call()
    with pytest.raises(ReplayMiss):
        c.actor("scrapeforge/facebook-search-posts").call()
    with pytest.raises(ReplayMiss):
        c.actor("apify/facebook-pages-scraper").call()


def test_expired_dataset_is_a_miss():
    c = ReplayApifyClient(FakeReal({"r1": None}, {}), CALLS[:1])
    with pytest.raises(ReplayMiss):
        c.actor("scrapeforge/facebook-search-posts").call()


class FakeDB:
    def __init__(self, run, calls):
        self.run, self.calls = run, calls

    def get_run(self, run_id):
        return self.run

    def _fetchall(self, sql, params=()):
        return self.calls

    def get_actor_class(self, actor_class):
        return {"enabled": True, "run_kind": "crawl", "cap_scope": "per_param", "params_schema": {}}


def test_replay_runs_the_actor_on_recorded_datasets():
    spec = {"actor_class": "facebook_keyword_search", "search_params": ["a", "b"], "max_results": 5,
            "min_date": "2026-09-20T00:00:00+00:00", "get_comments": True, "max_comments": 30}
    run = {"run_id": 70, "kind": "adhoc", "task_id": None, "org_id": 102, "status": "partial", "max_items": 5,
           "params": {"spec": spec}, "window_since": None}

    class EchoActor:
        def __init__(self, client):
            self.client = client

        def search(self, params, **kw):
            out = []
            for _ in params:
                run_ = self.client.actor("scrapeforge/facebook-search-posts").call(run_input={})
                out += list(self.client.dataset(run_["defaultDatasetId"]).iterate_items())
            self.client.actor("apify/facebook-comments-scraper").call(run_input={})
            return [types.SimpleNamespace(data=i) for i in out]

    out = replay_run(70, FakeDB(run, CALLS), REAL, lambda ac, client: EchoActor(client))
    assert [d.data for d in out["docs"]] == [{"n": 1}, {"n": 2}]
    assert out["unused"] == {}


def test_publisher_reconnects_after_a_failed_reconnect(monkeypatch):
    """A reconnect that raised (handshake timeout) left channel=None; the next publish must
    reconnect instead of failing forever with AttributeError."""
    for k, v in {"RABBIT_PORT": "5672", "RABBIT_HOST": "h", "RABBIT_USER": "u", "RABBIT_PASSWORD": "p",
                 "RABBIT_EXCHANGE": "ex", "RABBIT_QUEUE": "q", "RABBIT_VIRTUAL_HOST": "v"}.items():
        monkeypatch.setenv(k, v)
    published, attempts = [], {"n": 0}

    class Chan:
        is_closed = False

        def exchange_declare(self, **kw): pass
        def queue_declare(self, **kw): pass
        def queue_bind(self, **kw): pass
        def close(self): pass

        def basic_publish(self, exchange, routing_key, body):
            published.append(body)

    class Conn:
        is_closed = False

        def __init__(self, params):
            attempts["n"] += 1
            if attempts["n"] == 2:  # the first reconnect fails (tunnel blip)
                raise RuntimeError("Timeout during AMQP handshake")

        def channel(self): return Chan()
        def close(self): pass

    import pika

    monkeypatch.setattr(pika, "BlockingConnection", Conn)
    monkeypatch.setattr(pika, "ConnectionParameters", lambda **kw: kw)
    monkeypatch.setattr(pika, "PlainCredentials", lambda u, p: (u, p))
    sys.modules.pop("src.helpers.rabbitmq", None)
    rabbitmq = importlib.import_module("src.helpers.rabbitmq")
    rmq = rabbitmq.rmq
    rmq.connection.is_closed = True          # the connection dropped
    with pytest.raises(RuntimeError):        # reconnect attempt #2 fails -> channel None
        rmq.publish(b"one")
    assert rmq.channel is None
    rmq.publish(b"two")                      # attempt #3 succeeds: no AttributeError, no stuck channel
    assert published == [b"two"]
    sys.modules.pop("src.helpers.rabbitmq", None)
