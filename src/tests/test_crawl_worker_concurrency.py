"""Worker concurrency: the Apify-limit cap on CRAWL_WORKER_CONCURRENCY and the shared
on-disk caches written by concurrent runs (merge + atomic replace under locks)."""
import json
import threading

import pytest

from src import run_crawl_worker as w
from src.helpers.atomic_json import atomic_write_json

STARTER = {"data": {"limits": {"maxConcurrentActorJobs": 32, "maxActorMemoryGbytes": 64}}}


def test_apify_limit_cap_starter_plan():
    assert w.apify_limit_cap(STARTER, 3, 4) == 5        # min(32 // 3 = 10, 64 // 12 = 5)
    assert w.apify_limit_cap(STARTER, 2, 1) == 16       # min(16, 32)
    assert w.apify_limit_cap({"data": {"limits": {}}}, 3, 4) is None
    assert w.apify_limit_cap({"data": {"limits": {"maxConcurrentActorJobs": 2}}}, 3, 4) == 1   # never 0


@pytest.mark.parametrize("env,fetch,expected", [
    ({"CRAWL_WORKER_CONCURRENCY": "4"}, lambda: STARTER, 4),
    ({"CRAWL_WORKER_CONCURRENCY": "8"}, lambda: STARTER, 5),
    ({"CRAWL_WORKER_CONCURRENCY": "8", "CRAWL_WORKER_APIFY_LIMIT_CHECK": "0"}, lambda: STARTER, 8),
    ({"CRAWL_WORKER_CONCURRENCY": "8"}, lambda: (_ for _ in ()).throw(RuntimeError("401")), 8),
    ({}, None, 1),                                       # default: no lookup at all
])
def test_worker_concurrency(monkeypatch, env, fetch, expected):
    for k in ("CRAWL_WORKER_CONCURRENCY", "CRAWL_WORKER_APIFY_LIMIT_CHECK"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    assert w.worker_concurrency(fetch_limits=fetch or (lambda: pytest.fail("no lookup at N=1"))) == expected


def test_concurrent_filter_cache_saves_merge(tmp_path, monkeypatch):
    from src.actors import actor as actor_mod

    path = tmp_path / "filter_cache.json"
    monkeypatch.setattr(actor_mod, "FILTER_CACHE_PATH", str(path))
    atomic_write_json(str(path), {"old": True})

    class A:   # just the cache methods of ApifyActor
        _load_filter_cache = staticmethod(actor_mod.ApifyActor._load_filter_cache)
        _save_filter_cache = actor_mod.ApifyActor._save_filter_cache

    actors = []
    for i in range(8):
        a = A()
        a._filter_cache = a._load_filter_cache()
        actors.append(a)
    for i, a in enumerate(actors):
        a._filter_cache[f"run{i}"] = bool(i % 2)
    threads = [threading.Thread(target=a._save_filter_cache) for a in actors]
    [t.start() for t in threads]
    [t.join() for t in threads]
    saved = json.loads(path.read_text())
    assert saved == {"old": True, **{f"run{i}": bool(i % 2) for i in range(8)}}   # nobody's entries lost


def test_users_cache_concurrent_updates_do_not_crash(tmp_path):
    from src.models.users_management import UsersManagement

    um = UsersManagement(cache_path=str(tmp_path / "users.json"))
    errors = []

    def work(i):
        try:
            for j in range(30):
                um.save_stats(f"https://x.com/u{i}_{j}", {"followers": j})
        except Exception as e:   # "dictionary changed size during iteration" without the lock
            errors.append(e)

    threads = [threading.Thread(target=work, args=(i,)) for i in range(6)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors
    assert len(json.loads((tmp_path / "users.json").read_text())) == 180
