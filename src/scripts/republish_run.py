"""Republish a crawl run's documents from its saved Apify datasets — no new Apify spend.

Why (2026-10-07): the tunnel to RabbitMQ dropped mid-run and the worker's publisher kept a
dead channel (fixed in ``src/helpers/rabbitmq.py``), so five paid runs fetched posts and
comments from Apify and then published nothing ("N of N document(s) failed to publish").
Apify keeps every run's dataset for days, and reading a run or a dataset is free.

How: the run is **replayed** through the same code path as the worker — ``plan_run`` builds
the actor kwargs from the ``crawl_runs`` row (task row, or ``params.spec`` for ad-hoc
runs), and the actor's ``search()`` runs the full pipeline (keyword/date/language/location
filters, the cached LLM filter decisions, comment attachment) against a ``ReplayApifyClient``
that answers each ``actor(id).call()`` with the next recorded Apify run of that actor
(``crawl_run_calls``, in call order) instead of starting a new one. A call with no recorded
run raises ``ReplayMiss`` — the replay can never buy anything. Documents already in ES are
dropped by the actor's own ``existing_elasticsearch`` filter, so a replay publishes only
what is missing (gp3 indexes by URL, so a duplicate would be idempotent anyway).

    python -m src.scripts.republish_run --run-id 70 [--run-id 72 ...] [--apply]

Dry run by default: prints the documents the replay would publish. ``--apply`` publishes
through ``publish_document`` (the worker's ``RABBIT_*`` document connection — run with the
crawl worker's env: ``RABBIT_VIRTUAL_HOST``/``RABBIT_QUEUE`` decide where documents go) and
records ``stats.republish`` on the run (``published``, ``failed``, ``at``); the run's status
and counters are left as the worker wrote them. Needs ``DATABASE_URI`` (reads
``crawl_runs`` / ``crawl_run_calls``; the stats update needs UPDATE on ``crawl_runs``) and
``APIFI_API_TOKEN`` (GETs only).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional

logger = logging.getLogger("republish_run")


class ReplayMiss(RuntimeError):
    """The replayed pipeline asked for an Apify run the original run never made."""


def _norm_actor(actor_id: str) -> str:
    return (actor_id or "").replace("~", "/").strip().lower()


class ReplayApifyClient:
    """Answers ``actor(id).call()`` with recorded Apify runs (in order, per actor); reads go
    to the real client. Never starts an actor."""

    def __init__(self, real_client: Any, recorded_calls: List[Dict[str, Any]]):
        self.real = real_client
        self.queues: Dict[str, Deque[str]] = defaultdict(deque)
        for c in recorded_calls:
            if c.get("apify_run_id"):
                self.queues[_norm_actor(c["apify_actor_id"])].append(c["apify_run_id"])
        self.served: List[Dict[str, str]] = []

    def actor(self, actor_id: str):
        replay = self

        class _Actor:
            def call(self, run_input: Any = None, **kwargs: Any) -> Dict[str, Any]:
                q = replay.queues.get(_norm_actor(actor_id))
                if not q:
                    raise ReplayMiss(f"no recorded Apify run left for actor {actor_id!r} — refusing to start one")
                apify_run_id = q.popleft()
                run = replay.real.run(apify_run_id).get()
                if not run or not run.get("defaultDatasetId"):
                    raise ReplayMiss(f"Apify run {apify_run_id} ({actor_id}) has no dataset (expired?)")
                replay.served.append({"actor_id": actor_id, "apify_run_id": apify_run_id})
                return run

        return _Actor()

    def dataset(self, dataset_id: str):
        return self.real.dataset(dataset_id)

    def run(self, run_id: str):
        return self.real.run(run_id)


def _recorded_calls(db: Any, run_id: int) -> List[Dict[str, Any]]:
    return db._fetchall(  # noqa: SLF001 — CrawlDB's own read helper
        "SELECT call_id, purpose, apify_actor_id, apify_run_id FROM public.crawl_run_calls "
        "WHERE run_id = %s ORDER BY call_id", (run_id,))


def replay_run(run_id: int, db: Any, real_client: Any, actor_factory: Any) -> Dict[str, Any]:
    """Rebuild the run's documents from its recorded Apify runs. Returns {docs, served, unused}."""
    from src.worker.runner import WorkerDeps, plan_run

    run = db.get_run(run_id)
    if not run:
        raise SystemExit(f"run {run_id} not found")
    calls = _recorded_calls(db, run_id)
    if not calls:
        raise SystemExit(f"run {run_id} has no crawl_run_calls — nothing to replay")
    deps = WorkerDeps(db=db, apify_client_factory=lambda: real_client, actor_factory=actor_factory)
    plan = plan_run(run, {}, deps)
    client = ReplayApifyClient(real_client, calls)
    actor = actor_factory(plan.actor_class, client)
    docs = list(actor.search(plan.search_params, **plan.kwargs) or [])
    unused = {a: list(q) for a, q in client.queues.items() if q}
    return {"run": run, "docs": docs, "served": client.served, "unused": unused}


def publish_docs(docs: List[Any], publish: Any) -> Dict[str, Any]:
    from src.worker.runner import _publish_all

    r = _publish_all(docs, publish)
    return {"to_publish": r.to_publish, "published": r.main_ok + r.attached_ok, "failed": r.failures,
            "failed_urls": r.failed_urls, "fatal": r.fatal}


def main(argv: Optional[List[str]] = None) -> int:
    import os

    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--run-id", type=int, action="append", required=True)
    ap.add_argument("--apply", action="store_true", help="publish (default: dry run)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    from src.actors import get_actor
    from src.helpers.metering import raw_apify_client
    from src.worker.db import CrawlDB

    db = CrawlDB(os.getenv("DATABASE_URI", ""))
    real = raw_apify_client()
    rc = 0
    for run_id in args.run_id:
        out = replay_run(run_id, db, real, lambda actor_class, client: get_actor(actor_class, client=client))
        docs = out["docs"]
        print(f"run {run_id} ({out['run'].get('status')}, {out['run'].get('kind')}): replayed {len(out['served'])} "
              f"Apify run(s), {len(docs)} document(s) to publish"
              + (f"; unused recorded runs: {out['unused']}" if out["unused"] else ""))
        for d in docs[:50]:
            data = getattr(d, "data", {}) or {}
            print(f"   {str(data.get('timestamp'))[:16]:16} {str(data.get('source'))[:28]:28} "
                  f"c={len(data.get('comments') or []):3} {str(data.get('url'))[:90]}")
        if not args.apply:
            continue
        from src.worker.runner import _default_publish

        res = publish_docs(docs, _default_publish)
        res["at"] = datetime.now(timezone.utc).isoformat()
        res["apify_runs_replayed"] = [s["apify_run_id"] for s in out["served"]]
        db._execute(  # noqa: SLF001
            "UPDATE public.crawl_runs SET stats = coalesce(stats, '{}'::jsonb) || jsonb_build_object('republish', %s::jsonb) "
            "WHERE run_id = %s", (json.dumps(res), run_id))
        print(f"run {run_id}: published {res['published']}/{res['to_publish']}, failed {res['failed']}"
              + (f" — {res['fatal']}" if res["fatal"] else ""))
        rc = rc or (1 if res["failed"] else 0)
    return rc


if __name__ == "__main__":
    sys.exit(main())
