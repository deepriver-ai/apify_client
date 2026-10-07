"""userdb access for the crawl-run worker (role ``crawler_worker``, design Q5).

Short-lived connections, one per operation: a run can sit inside Apify for many
minutes, and the role has 60 s statement / idle-in-transaction timeouts, so nothing
holds a connection across actor work. Every statement here is within the role's grants:
SELECT crawl_*; INSERT/UPDATE crawl_runs; INSERT crawl_run_calls; column UPDATEs on
crawl_schedules / crawl_comment_requests / crawl_comment_request_posts. The usage ledger
(``usage_events`` / ``usage_blocks`` / ``usage_limits``) is written and read by
``usage_client`` on its own connection (``USAGE_DB_URI``, default ``DATABASE_URI``).

Cost writes (``crawl_run_calls`` inserts and the final ``crawl_runs`` stats/cost update)
never fail a run: on a DB error they go to a JSONL spool and are replayed later
(``replay_spool``) — design §6.1 worker rules, rate_limiting.md §6.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_SPOOL_PATH = os.path.join("cache", "crawl_worker_spool.jsonl")

# crawl_runs columns the worker writes when a run finishes.
FINISH_COLUMNS = (
    "items_paid", "items_cached_filtered", "items_dropped_keyword", "items_dropped_date",
    "items_already_seen", "items_dropped_language", "items_dropped_location",
    "items_dropped_llm", "items_new", "attached_news_published", "comments_fetched",
    "profiles_scraped", "newest_item_at", "oldest_item_at", "cost_usd",
)


def _json_default(o: Any) -> Any:
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    return str(o)


def _dumps(o: Any) -> str:
    return json.dumps(o, ensure_ascii=False, default=_json_default)


class CrawlDB:
    """Thin psycopg (v3) wrapper. ``dsn`` is the worker's ``DATABASE_URI``."""

    def __init__(self, dsn: str, spool_path: str = DEFAULT_SPOOL_PATH, connect_timeout: int = 10):
        if not dsn:
            raise RuntimeError("DATABASE_URI is not set (crawl worker needs the userdb, role crawler_worker)")
        self.dsn = dsn
        self.spool_path = spool_path
        self.connect_timeout = connect_timeout
        self._spool_lock = threading.Lock()

    # --- plumbing ---
    def _connect(self):
        import psycopg
        from psycopg.rows import dict_row

        return psycopg.connect(self.dsn, row_factory=dict_row, connect_timeout=self.connect_timeout,
                               application_name="apify_client.crawl_worker")

    def _fetchone(self, sql: str, params: Iterable[Any] = ()) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return cur.fetchone()

    def _fetchall(self, sql: str, params: Iterable[Any] = ()) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return list(cur.fetchall())

    def _execute(self, sql: str, params: Iterable[Any] = ()) -> int:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return cur.rowcount

    def ping(self) -> None:
        self._fetchone("SELECT 1 AS ok")

    # --- run lifecycle ---
    def get_run(self, run_id: int) -> Optional[Dict[str, Any]]:
        return self._fetchone("SELECT * FROM public.crawl_runs WHERE run_id = %s", (run_id,))

    def claim_run(self, run_id: int, worker: str) -> Optional[Dict[str, Any]]:
        """``queued`` → ``running`` (design §5.2). None when the run is not ``queued``."""
        return self._fetchone(
            """UPDATE public.crawl_runs
                  SET status = 'running', started_at = now(), heartbeat_at = now(), worker = %s
                WHERE run_id = %s AND status = 'queued'
            RETURNING *""",
            (worker, run_id),
        )

    def fail_queued_run(self, run_id: int, error: str) -> int:
        """Close a run that never started (bad message), without claiming it."""
        return self._execute(
            """UPDATE public.crawl_runs SET status = 'failed', finished_at = now(), error = %s
                WHERE run_id = %s AND status = 'queued'""",
            (error, run_id),
        )

    def release_run(self, run_id: int, error: str) -> int:
        """``running`` → ``queued`` (usage ledger unreachable before any Apify spend): the
        requeued message claims it again. ``error`` keeps the last reason visible."""
        return self._execute(
            """UPDATE public.crawl_runs
                  SET status = 'queued', started_at = NULL, heartbeat_at = NULL, worker = NULL, error = %s
                WHERE run_id = %s AND status = 'running'""",
            (error, run_id),
        )

    def heartbeat(self, run_id: int) -> int:
        return self._execute(
            "UPDATE public.crawl_runs SET heartbeat_at = now() WHERE run_id = %s AND status = 'running'",
            (run_id,),
        )

    def get_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        return self._fetchone("SELECT * FROM public.crawl_tasks WHERE task_id = %s", (task_id,))

    def get_actor_class(self, actor_class: str) -> Optional[Dict[str, Any]]:
        return self._fetchone("SELECT * FROM public.crawl_actor_classes WHERE actor_class = %s", (actor_class,))

    def get_comment_posts(self, run_id: int) -> List[Dict[str, Any]]:
        return self._fetchall(
            """SELECT row_id, request_id, post_url, tier, depth, stored_comments
                 FROM public.crawl_comment_request_posts WHERE run_id = %s ORDER BY row_id""",
            (run_id,),
        )

    def set_comment_post_fetched(self, row_id: int, comments_fetched: int) -> int:
        return self._execute(
            "UPDATE public.crawl_comment_request_posts SET comments_fetched = %s WHERE row_id = %s",
            (comments_fetched, row_id),
        )

    def mark_comment_request_running(self, request_id: int) -> int:
        return self._execute(
            "UPDATE public.crawl_comment_requests SET status = 'running' WHERE request_id = %s AND status = 'approved'",
            (request_id,),
        )

    def close_comment_request_if_done(self, request_id: int) -> int:
        """``running`` → ``done`` once none of the request's runs is still open."""
        return self._execute(
            """UPDATE public.crawl_comment_requests SET status = 'done', finished_at = now()
                WHERE request_id = %s AND status IN ('approved', 'running')
                  AND NOT EXISTS (SELECT 1 FROM public.crawl_runs r
                                   WHERE r.comment_request_id = %s
                                     AND r.status IN ('requested', 'queued', 'running'))""",
            (request_id, request_id),
        )

    def update_schedule_after_run(self, task_id: int, run_id: int, success: bool) -> int:
        """Immediate feedback only: ``last_run_id`` (+ ``last_success_at``). The failure
        response — ``consecutive_failures``, retry backoff (``retry_at``), auto-pause
        (``active``/``paused_reason``) — belongs to the poller's crawl scheduler, which
        settles every terminal scheduled run exactly once (reaped runs included), so the
        worker must not count failures too (design §6.2 "Split of schedule bookkeeping")."""
        if success:
            sql = """UPDATE public.crawl_schedules SET last_run_id = %s, last_success_at = now()
                      WHERE task_id = %s"""
        else:
            sql = "UPDATE public.crawl_schedules SET last_run_id = %s WHERE task_id = %s"
        return self._execute(sql, (run_id, task_id))

    # --- cost writes (spooled on failure) ---
    def insert_call(self, row: Dict[str, Any]) -> None:
        self._write_or_spool("insert_call", row)

    def finish_run(self, run_id: int, status: str, error: Optional[str], output_path: Optional[str],
                   counters: Dict[str, Any], stats: Dict[str, Any]) -> Optional[bool]:
        """True = written; False = the run was no longer ``running`` (reaped by the
        scheduler); None = the write failed and was spooled for replay."""
        payload = {"run_id": run_id, "status": status, "error": error, "output_path": output_path,
                   "counters": counters, "stats": stats}
        return self._write_or_spool("finish_run", payload)

    def _apply(self, op: str, payload: Dict[str, Any]) -> Optional[bool]:
        from psycopg.types.json import Jsonb

        if op == "insert_call":
            row = payload
            inserted = self._execute(
                """INSERT INTO public.crawl_run_calls
                       (run_id, purpose, apify_actor_id, apify_run_id, apify_status, items, cost_usd,
                        estimated, usage, usage_event_id, started_at, finished_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (apify_run_id) DO NOTHING""",
                (row["run_id"], row["purpose"], row["apify_actor_id"], row["apify_run_id"],
                 row.get("apify_status"), row.get("items"), row.get("cost_usd"), bool(row.get("estimated")),
                 Jsonb(row["usage"]) if row.get("usage") is not None else None, row.get("usage_event_id"),
                 row.get("started_at"), row.get("finished_at")),
            )
            if not inserted:
                logger.info("crawl_run_calls: apify run %s already recorded (redelivery/replay)", row["apify_run_id"])
        elif op == "finish_run":
            counters = payload["counters"]
            sets = ", ".join(f"{col} = %s" for col in FINISH_COLUMNS)
            values = [counters.get(col) for col in FINISH_COLUMNS]
            n = self._execute(
                f"""UPDATE public.crawl_runs
                       SET status = %s, finished_at = now(), heartbeat_at = now(), error = %s,
                           output_path = %s, {sets}, stats = stats || %s
                     WHERE run_id = %s AND status = 'running'""",
                [payload["status"], payload.get("error"), payload.get("output_path"), *values,
                 Jsonb(json.loads(_dumps(payload.get("stats") or {}))), payload["run_id"]],
            )
            if n == 0:
                logger.warning("finish_run: run %s was no longer 'running' (reaped?) — stats not written",
                               payload["run_id"])
            return n > 0
        else:
            raise ValueError(f"unknown spool op {op!r}")

    def _write_or_spool(self, op: str, payload: Dict[str, Any]) -> Optional[bool]:
        try:
            return self._apply(op, payload)
        except Exception as exc:
            logger.error("userdb write %s failed (%s) — spooling to %s", op, exc, self.spool_path)
            self._spool(op, payload)
            return None

    def _spool(self, op: str, payload: Dict[str, Any]) -> None:
        with self._spool_lock:
            os.makedirs(os.path.dirname(self.spool_path) or ".", exist_ok=True)
            with open(self.spool_path, "a", encoding="utf-8") as f:
                f.write(_dumps({"op": op, "payload": payload}) + "\n")

    def replay_spool(self) -> int:
        """Re-apply spooled writes in order; keep the ones that still fail. Returns #applied."""
        with self._spool_lock:
            if not os.path.exists(self.spool_path):
                return 0
            with open(self.spool_path, encoding="utf-8") as f:
                lines = [ln for ln in f if ln.strip()]
            remaining: List[str] = []
            applied = 0
            for i, line in enumerate(lines):
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    logger.error("spool: dropping unparseable line %r", line[:200])
                    continue
                try:
                    self._apply(entry["op"], entry["payload"])  # ISO datetimes cast server-side
                    applied += 1
                except Exception as exc:
                    logger.warning("spool replay still failing (%s); keeping %d entries", exc, len(lines) - i)
                    remaining.extend(lines[i:])
                    break
            tmp = self.spool_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.writelines(remaining)
            os.replace(tmp, self.spool_path)
            if applied:
                logger.info("spool: replayed %d writes, %d left", applied, len(remaining))
            return applied

