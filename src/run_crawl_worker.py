"""Crawl-run queue worker: consumes ``crawl_run`` messages and executes them.

    python -m src.run_crawl_worker            # consume forever
    python -m src.run_crawl_worker --once     # exit after one message (smoke tests)

Design: docs/db/crawl_tasks.md §6.1. The backend (``POST /crawl/runs/request``) or the
scheduler inserts a ``crawl_runs`` row (``queued``) and enqueues its id through
``rabbit_enqueuer``; this process claims the row, re-reads the task from userdb, runs
the actor, records every Apify call in ``crawl_run_calls`` and the funnel/cost on the
run, publishes the documents to gp3's queue and finishes the run.

Two RabbitMQ connections (Q4):
- **runs** (consumed here): ``CRAWL_RUNS_RABBIT_VIRTUAL_HOST`` + ``CRAWL_RUNS_QUEUE``
  (host/port/user/password default to the ``RABBIT_*`` values, overridable with
  ``CRAWL_RUNS_RABBIT_*``). Locally: ``document_processing_dev`` + ``dev_crawl_runs``.
- **documents** (published): the existing ``publish_document`` path, configured by the
  ``RABBIT_*`` variables exactly as for ``run_searches.py``. Opened only when a
  non-dry run publishes.

Manual ack after the run is finished; each run executes on its own thread (with its own
``heartbeat_at`` beat) so the consuming connection keeps its heartbeats during long
actor runs. A message whose run is not ``queued`` (redelivery, cancelled) is acked and
dropped.

**Concurrency** (``CRAWL_WORKER_CONCURRENCY``, default 1, recommended 4): prefetch N, so
up to N runs execute in parallel in this process, each acked when it finishes. Threads
in one process rather than N processes: the on-disk caches (filter cache, users cache,
unknown sources) are shared in-process and saved under locks with merge + atomic
replace, and document publishing (one pika connection) is serialized; N processes would
each keep their own copy of those caches and overwrite each other's files. Sizing: the
Apify STARTER plan allows 32 concurrent actor runs and 64 GB actor memory; a run starts
up to 3 actors (search → comments → profiles) of 1–4 GB, so 4 runs ≈ 12 actors / ≤ 48 GB,
leaving room for hand runs (``run_searches.py``, comment scripts) on the same account.
When N > 1 the worker reads ``GET /v2/users/me/limits`` once at startup (free, no actor
run) and lowers N to ``min(maxConcurrentActorJobs // A, maxActorMemoryGbytes // (A × G))``
with A = ``CRAWL_WORKER_ACTORS_PER_RUN`` (3) and G = ``CRAWL_WORKER_GB_PER_ACTOR`` (4);
``CRAWL_WORKER_APIFY_LIMIT_CHECK=0`` skips it, and a failed lookup keeps N.

Metering: every Apify call goes through ``usage_client.apify.run`` (admit → cap → run →
one ``usage_events`` row per Apify run); see ``src/worker/runner.py`` for how an
admission refusal ends the run. Ledger: ``USAGE_DB_URI`` (default ``DATABASE_URI``),
spool ``USAGE_SPOOL_DIR`` (default ``cache/usage_spool``).
"""
from __future__ import annotations

import argparse
import functools
import logging
import os
import socket
import sys
import threading
import time

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("src.run_crawl_worker")


def _env(name: str, default: str | None = None, *, fallback: str | None = None) -> str | None:
    value = os.getenv(name)
    if value:
        return value
    if fallback and os.getenv(fallback):
        return os.getenv(fallback)
    return default


def _runs_connection_params():
    import pika

    vhost = _env("CRAWL_RUNS_RABBIT_VIRTUAL_HOST")
    if not vhost:
        raise SystemExit("CRAWL_RUNS_RABBIT_VIRTUAL_HOST is required (local: document_processing_dev)")
    return pika.ConnectionParameters(
        host=_env("CRAWL_RUNS_RABBIT_HOST", fallback="RABBIT_HOST"),
        port=int(_env("CRAWL_RUNS_RABBIT_PORT", "5672", fallback="RABBIT_PORT")),
        virtual_host=vhost,
        credentials=pika.PlainCredentials(
            _env("CRAWL_RUNS_RABBIT_USER", fallback="RABBIT_USER"),
            _env("CRAWL_RUNS_RABBIT_PASSWORD", fallback="RABBIT_PASSWORD"),
        ),
        heartbeat=int(_env("CRAWL_RUNS_RABBIT_HEARTBEAT", "60")),
        blocked_connection_timeout=300,
    )


def build_deps():
    from src.actors import get_actor
    from src.actors.actor import _get_apify_token
    from src.worker.db import DEFAULT_SPOOL_PATH, CrawlDB
    from src.worker.runner import WorkerDeps

    db = CrawlDB(os.getenv("DATABASE_URI", ""), spool_path=os.getenv("CRAWL_WORKER_SPOOL_PATH", DEFAULT_SPOOL_PATH))

    def apify_client_factory():
        from apify_client import ApifyClient

        return ApifyClient(_get_apify_token())

    return WorkerDeps(
        db=db,
        apify_client_factory=apify_client_factory,
        actor_factory=lambda actor_class, client: get_actor(actor_class, client=client),
        usage_ledger=configure_usage_ledger(),
        worker_name=f"{socket.gethostname()}:{os.getpid()}",
        output_dir=os.getenv("CRAWL_WORKER_OUTPUT_DIR", os.path.join("cache", "runs")),
        heartbeat_secs=float(os.getenv("CRAWL_WORKER_HEARTBEAT_SECS", "60")),
    )


DEFAULT_USAGE_SPOOL_DIR = os.path.join("cache", "usage_spool")


def configure_usage_ledger():
    """The process-wide ``usage_client`` ledger (rate_limiting.md §2.2, §2.7).

    ``USAGE_DB_URI`` (default: ``DATABASE_URI`` — the ``crawler`` role holds the usage
    grants, migration ``2026-10-07_crawler_usage_grants_userdb.sql``); failed
    ``usage_events`` writes spool to ``USAGE_SPOOL_DIR`` (default ``cache/usage_spool``)
    and are replayed. Every Apify call is admitted against it, so without a DSN nothing
    could ever be spent: refuse to start instead.
    """
    from usage_client import ledger as usage_ledger

    dsn = os.getenv("USAGE_DB_URI") or os.getenv("DATABASE_URI")
    if not dsn:
        raise SystemExit("USAGE_DB_URI (or DATABASE_URI) is required: every Apify call is admitted and "
                         "recorded through usage_client")
    return usage_ledger.configure(dsn, spool_dir=os.getenv("USAGE_SPOOL_DIR") or DEFAULT_USAGE_SPOOL_DIR)


def apify_limit_cap(limits: dict, actors_per_run: int, gb_per_actor: float) -> int | None:
    """Upper bound on concurrent runs from the Apify account limits
    (``GET /v2/users/me/limits`` → ``data.limits``); None when unknown."""
    data = (limits or {}).get("data", limits or {})
    lim = data.get("limits", data)
    caps = []
    if lim.get("maxConcurrentActorJobs"):
        caps.append(int(lim["maxConcurrentActorJobs"]) // max(actors_per_run, 1))
    if lim.get("maxActorMemoryGbytes"):
        caps.append(int(float(lim["maxActorMemoryGbytes"]) // max(actors_per_run * gb_per_actor, 1)))
    return max(min(caps), 1) if caps else None


def _fetch_apify_limits() -> dict:
    import requests

    from src.actors.actor import _get_apify_token

    resp = requests.get("https://api.apify.com/v2/users/me/limits",
                        headers={"Authorization": f"Bearer {_get_apify_token()}"}, timeout=15)
    resp.raise_for_status()
    return resp.json()


def worker_concurrency(fetch_limits=_fetch_apify_limits) -> int:
    n = max(int(os.getenv("CRAWL_WORKER_CONCURRENCY", "1") or 1), 1)
    if n == 1 or os.getenv("CRAWL_WORKER_APIFY_LIMIT_CHECK", "1").lower() in ("0", "false", "no"):
        return n
    try:
        cap = apify_limit_cap(fetch_limits(), int(os.getenv("CRAWL_WORKER_ACTORS_PER_RUN", "3")),
                              float(os.getenv("CRAWL_WORKER_GB_PER_ACTOR", "4")))
    except Exception as exc:
        logger.warning("Apify limits lookup failed (%s); keeping CRAWL_WORKER_CONCURRENCY=%d", exc, n)
        return n
    if cap is not None and cap < n:
        logger.warning("CRAWL_WORKER_CONCURRENCY=%d lowered to %d by the Apify account limits", n, cap)
        return cap
    return n


def consume(deps, queue: str, once: bool = False, concurrency: int = 1) -> None:
    import pika

    from src.worker.runner import REQUEUE, handle_message

    retry_secs = float(os.getenv("CRAWL_WORKER_DB_RETRY_SECS", "30"))
    params = _runs_connection_params()
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_qos(prefetch_count=max(concurrency, 1))   # N runs in flight, one thread each

    def settle(delivery_tag: int, outcome: str) -> None:
        if not channel.is_open:
            logger.error("channel closed before settling delivery %s (it will be redelivered)", delivery_tag)
            return
        if outcome == REQUEUE:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
        else:
            channel.basic_ack(delivery_tag=delivery_tag)
        if once:
            channel.stop_consuming()

    def work(delivery_tag: int, body: bytes) -> None:
        try:
            outcome = handle_message(body, deps)
        except Exception:
            logger.exception("unexpected worker error; acking to avoid a poison loop")
            outcome = "ack"
        if outcome == REQUEUE:
            time.sleep(retry_secs)  # don't hot-loop while userdb is unreachable
        connection.add_callback_threadsafe(functools.partial(settle, delivery_tag, outcome))

    def on_message(ch, method, properties, body) -> None:
        threading.Thread(target=work, args=(method.delivery_tag, body), daemon=True,
                         name=f"crawl-run-{method.delivery_tag}").start()

    channel.basic_consume(queue=queue, on_message_callback=on_message, auto_ack=False)
    logger.info("consuming %s on vhost %s, concurrency %d (documents publish to vhost %s, queue %s)",
                queue, params.virtual_host, concurrency, os.getenv("RABBIT_VIRTUAL_HOST"), os.getenv("RABBIT_QUEUE"))
    try:
        channel.start_consuming()
    finally:
        try:
            connection.close()
        except Exception:
            pass
        try:
            if "src.helpers.rabbitmq" in sys.modules:
                sys.modules["src.helpers.rabbitmq"].close_client()
        except Exception:
            pass


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--once", action="store_true", help="exit after settling one message")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    queue = os.getenv("CRAWL_RUNS_QUEUE")
    if not queue:
        raise SystemExit("CRAWL_RUNS_QUEUE is required (local: dev_crawl_runs)")

    deps = build_deps()
    deps.db.ping()  # fail fast (and loudly) if userdb is unreachable
    deps.db.replay_spool()
    consume(deps, queue, once=args.once, concurrency=1 if args.once else worker_concurrency())


if __name__ == "__main__":
    main()
