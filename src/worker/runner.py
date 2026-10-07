"""One crawl-run message → one claimed, executed and accounted ``crawl_runs`` row.

Design: docs/db/crawl_tasks.md §5.2 (run status), §6.1 (flow, message shape, worker
rules), §2.5 (funnel columns), §2.8 R2 (no window → no date filter), §7.3 / §7.4.

Metering (rate_limiting.md §2.1-§2.3, §2.7): every Apify call of a run goes through
``usage_client.apify.run`` (``UsageMeter`` in ``recording.py``) with the run's org, user
(``requested_by``) and search (``query_id``), ``event_key='crawl-run:<run_id>'``. When
admission lets nothing through (``AdmissionRefused``) the run ends as follows:

====================================  ========================  =========================
refusal                               no Apify call made yet    after a paid call
====================================  ========================  =========================
``over_limit`` / ``per_request_cap``  ``blocked`` (terminal;    ``partial`` (money spent,
                                      the schedule is left      stats + cost kept)
                                      alone, so the next tick
                                      retries = deferred)
``ledger_unreachable``                released to ``queued`` +  ``partial``
                                      message requeued (retry
                                      after
                                      ``CRAWL_WORKER_DB_RETRY_SECS``)
====================================  ========================  =========================

The ``usage_blocks`` row is written by ``usage_client`` (path ``worker:apify-client``);
``crawl_runs.error`` / ``stats.usage_refusal`` carry the reason. A message without a
valid org writes ``usage_blocks(reason='missing_org')`` itself (best effort).

Transport-free: ``handle_message(body, deps)`` returns ``"ack"`` or ``"requeue"`` and the
consumer (``src/run_crawl_worker.py``) acks accordingly, so this module is unit-testable
with a fake DB, a fake Apify client and a fake publisher.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from usage_client import AdmissionRefused

from src.models.crawl_task import CrawlTask
from src.worker.recording import SERVICE, CallRecorder, RecordingApifyClient, UsageMeter, instrument_actor

logger = logging.getLogger(__name__)

ACK = "ack"
REQUEUE = "requeue"
# execute_run() result when a ledger outage before any spend released the run to 'queued'.
RELEASED = "queued"
BLOCK_PATH = f"worker:{SERVICE}"

MESSAGE_TYPE = "crawl_run"

# Keys in actor_params that set the per-call result count; clamped to the run's max_items.
RESULT_COUNT_KEYS = ("results_limit", "max_articles")
# Keys inside a raw actor-input passthrough (tiktok ``apify_input``) that would lift the cap (§4).
CAP_LIFTING_INPUT_KEYS = ("maxItems", "max_items", "resultsPerPage", "resultsLimit", "maxPosts", "numberOfTweets")
# §2.8 R2: a task without min_date/period sends NO date filter. Actors whose default run
# input carries a window of its own get it neutralized here (unless the task sets it).
NO_WINDOW_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "google_news": {"timeframe": "all"},  # GoogleNewsActor defaults timeframe to "1d"
}

FUNNEL_TO_COLUMN = {
    "dropped_cached": "items_cached_filtered",
    "dropped_keyword": "items_dropped_keyword",
    "dropped_date": "items_dropped_date",
    "dropped_existing_elasticsearch": "items_already_seen",
    "dropped_language": "items_dropped_language",
    "dropped_location": "items_dropped_location",
    "dropped_llm_filter": "items_dropped_llm",
}


class RunRejected(ValueError):
    """The run cannot be executed as defined (bad params, missing task, …). No spend."""


@dataclass
class WorkerDeps:
    db: Any                                            # src.worker.db.CrawlDB (or a fake)
    apify_client_factory: Callable[[], Any]            # () -> ApifyClient
    actor_factory: Callable[[str, Any], Any]           # (actor_class, client) -> ApifyActor
    publish: Optional[Callable[[Any], None]] = None    # doc -> None; default publish_document
    usage_ledger: Any = None                           # usage_client Ledger; None = process default (USAGE_DB_URI)
    worker_name: str = "crawl-worker"
    output_dir: str = os.path.join("cache", "runs")
    heartbeat_secs: float = 60.0


# --------------------------------------------------------------------------- message


def parse_message(body: Any) -> Dict[str, Any]:
    if isinstance(body, (bytes, bytearray)):
        body = body.decode("utf-8")
    msg = json.loads(body) if isinstance(body, str) else body
    if not isinstance(msg, dict):
        raise ValueError("message is not a JSON object")
    if msg.get("type") != MESSAGE_TYPE:
        raise ValueError(f"message type {msg.get('type')!r} is not {MESSAGE_TYPE!r}")
    run_id = msg.get("run_id")
    if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
        raise ValueError(f"bad run_id {run_id!r}")
    return msg


def _valid_org(org_id: Any) -> bool:
    return isinstance(org_id, int) and not isinstance(org_id, bool) and org_id > 0


def handle_message(body: Any, deps: WorkerDeps) -> str:
    """Process one queue message. Returns ``ack`` or ``requeue`` (DB unreachable at claim)."""
    try:
        msg = parse_message(body)
    except Exception as exc:
        logger.error("crawl worker: dropping malformed message (%s): %r", exc, body[:500] if isinstance(body, (bytes, str)) else body)
        return ACK

    run_id = msg["run_id"]
    trace_id = msg.get("trace_id")
    log_ctx = f"run_id={run_id} trace_id={trace_id}"

    # Worker rule (§6.1, rate_limiting §2.3): no org → ack + error log +
    # usage_blocks(missing_org), never spend. The run is closed as failed so it doesn't
    # hold the task's one-live-run slot.
    if not _valid_org(msg.get("org_id")):
        logger.error("crawl worker: message without a valid org_id (%r) — acked, not run [%s]",
                     msg.get("org_id"), log_ctx)
        _block_missing_org(deps, msg)
        try:
            deps.db.fail_queued_run(run_id, "message missing org_id (worker:apify-client)")
        except Exception as exc:
            logger.error("could not mark run %s failed: %s", run_id, exc)
        return ACK

    try:
        deps.db.replay_spool()
    except Exception as exc:
        logger.warning("spool replay failed: %s", exc)

    try:
        run = deps.db.claim_run(run_id, deps.worker_name)
    except Exception as exc:
        logger.error("crawl worker: userdb unreachable at claim (%s) — requeue [%s]", exc, log_ctx)
        return REQUEUE

    if run is None:
        try:
            current = deps.db.get_run(run_id)
        except Exception:
            current = None
        status = current.get("status") if current else "missing"
        logger.warning("crawl worker: run not claimable (status=%s) — redelivery or stale message, acked [%s]",
                       status, log_ctx)
        return ACK

    if run.get("org_id") != msg.get("org_id"):
        logger.warning("crawl worker: message org_id %s != run org_id %s; using the DB row [%s]",
                       msg.get("org_id"), run.get("org_id"), log_ctx)

    logger.info("crawl worker: claimed run kind=%s task_id=%s org_id=%s [%s]",
                run.get("kind"), run.get("task_id"), run.get("org_id"), log_ctx)
    with _heartbeat(deps, run_id):
        status = execute_run(run, msg, deps)
    try:
        deps.db.replay_spool()
    except Exception as exc:
        logger.warning("spool replay failed: %s", exc)
    if status == RELEASED:
        logger.warning("crawl worker: usage ledger unreachable before any spend — run released, requeue [%s]",
                       log_ctx)
        return REQUEUE
    return ACK


def _block_missing_org(deps: WorkerDeps, msg: Dict[str, Any]) -> None:
    """``usage_blocks(path='worker:apify-client', reason='missing_org')``, best effort."""
    query_id = msg.get("query_id")
    if isinstance(query_id, bool) or not isinstance(query_id, int) or query_id <= 0:
        query_id = None
    kind = msg.get("kind") if isinstance(msg.get("kind"), str) and msg.get("kind") else "crawl"
    try:
        ledger = deps.usage_ledger
        if ledger is None:
            from usage_client.ledger import get_ledger

            ledger = get_ledger()
        ledger.block(path=BLOCK_PATH, tool="apify", kind=kind, reason="missing_org", search_id=query_id)
    except Exception as exc:  # never let the refusal bookkeeping fail the ack
        logger.error("could not write usage_blocks(missing_org) for run %s: %s", msg.get("run_id"), exc)


@contextmanager
def _heartbeat(deps: WorkerDeps, run_id: int) -> Iterator[None]:
    stop = threading.Event()

    def beat() -> None:
        while not stop.wait(deps.heartbeat_secs):
            try:
                deps.db.heartbeat(run_id)
            except Exception as exc:
                logger.warning("heartbeat for run %s failed: %s", run_id, exc)

    thread = threading.Thread(target=beat, name=f"crawl-heartbeat-{run_id}", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=5)


# --------------------------------------------------------------------------- params


def validate_actor_params(actor_class: str, actor_params: Dict[str, Any], params_schema: Optional[Dict[str, Any]]) -> None:
    """§4: reject keys outside ``crawl_actor_classes.params_schema`` (when the schema is
    seeded — an empty schema means "not yet generated", accept all) and any raw-input
    key that would lift the item cap."""
    schema = params_schema or {}
    allowed = schema.get("properties") if isinstance(schema.get("properties"), dict) else schema
    if allowed:
        unknown = sorted(k for k in actor_params if k not in allowed)
        if unknown:
            raise RunRejected(f"actor_params key(s) {unknown} not allowed for actor_class {actor_class}")
    raw_input = actor_params.get("apify_input")
    if isinstance(raw_input, dict):
        lifting = sorted(k for k in raw_input if k in CAP_LIFTING_INPUT_KEYS)
        if lifting:
            raise RunRejected(f"actor_params.apify_input key(s) {lifting} would lift the run's item cap")


def build_crawl_kwargs(task: CrawlTask, run: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
    """Actor kwargs for a crawl run: ``CrawlTask.to_actor_kwargs()`` (the single param
    path) + the run's window, ``override_filters`` and effective ``max_items``.

    Window: ``crawl_runs.window_since`` (set by the scheduler / endpoint) replaces the
    task's ``min_date``/``period``; otherwise the task's own window applies; with
    neither, NO date filter is sent (§2.8 R2).
    """
    kwargs = task.to_actor_kwargs()
    params = run.get("params") or {}

    if run.get("window_since") is not None:
        kwargs["min_date"] = run["window_since"]
        kwargs["period"] = None
    kwargs["override_filters"] = bool(params.get("override_filters", False))

    max_items = min(int(run["max_items"]), int(task.max_results))
    kwargs["max_results"] = max_items
    for key in RESULT_COUNT_KEYS:
        value = kwargs.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value > max_items:
            kwargs[key] = max_items

    if kwargs.get("min_date") is None and not kwargs.get("period"):
        kwargs["min_date"] = None
        kwargs["period"] = None
        for key, value in NO_WINDOW_OVERRIDES.get(task.actor_class, {}).items():
            if key not in task.actor_params:
                kwargs[key] = value
    return kwargs, max_items


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def diff_params(message_params: Dict[str, Any], effective: Dict[str, Any]) -> List[str]:
    """Keys where the message's convenience copy differs from what the worker runs."""
    diffs = []
    for key, value in (message_params or {}).items():
        if key not in effective or key in ("actor_class", "search_params"):
            continue
        a, b = _jsonable(value), _jsonable(effective[key])
        if key == "min_date" and isinstance(a, str) and isinstance(b, str):
            a, b = _instant(a), _instant(b)
        if a != b:
            diffs.append(key)
    return sorted(diffs)


def _instant(value: str) -> Any:
    """An ISO datetime string as a comparable instant: the backend sends ``min_date`` in
    UTC while the worker reads ``window_since`` in the DB session's time zone, so the
    same instant can print with different offsets. Naive values are compared as UTC;
    unparseable strings compare verbatim."""
    try:
        dt_value = datetime.fromisoformat(value)
    except ValueError:
        return value
    return dt_value if dt_value.tzinfo else dt_value.replace(tzinfo=timezone.utc)


# --------------------------------------------------------------------------- execution


@dataclass
class _Plan:
    actor_class: str
    search_params: List[str]
    kwargs: Dict[str, Any]
    dry_run: bool
    search_item_budget: Optional[int]
    cost_budget_usd: Optional[float]
    comment_posts: List[Dict[str, Any]] = field(default_factory=list)
    task_id: Optional[int] = None


def _min_budget(*values: Any) -> Optional[float]:
    nums = [float(v) for v in values if v is not None]
    return min(nums) if nums else None


def plan_run(run: Dict[str, Any], msg: Dict[str, Any], deps: WorkerDeps) -> _Plan:
    """Re-read the definition from userdb (source of truth) and resolve the run."""
    db = deps.db
    kind = run.get("kind") or "crawl"

    if kind == "comments":
        posts = db.get_comment_posts(run["run_id"])
        if not posts:
            raise RunRejected(f"comments run {run['run_id']} has no crawl_comment_request_posts linked")
        actor_class = (run.get("params") or {}).get("actor_class") or "facebook_post_comments"
        ac = db.get_actor_class(actor_class)
        if not ac or not ac.get("enabled", True) or ac.get("run_kind") != "comments":
            raise RunRejected(f"actor_class {actor_class!r} is not an enabled comments actor")
        depth = max(int(p["depth"]) for p in posts)
        kwargs = {"task_id": f"db-comments:{run.get('comment_request_id')}", "max_comments": depth}
        return _Plan(
            actor_class=actor_class,
            search_params=[p["post_url"] for p in posts],
            kwargs=kwargs,
            dry_run=bool(run.get("dry_run")),
            search_item_budget=None,
            cost_budget_usd=_min_budget(msg.get("max_cost_usd"), run.get("admitted_usd")),
            comment_posts=posts,
        )

    task_row = db.get_task(run["task_id"])
    if not task_row:
        raise RunRejected(f"crawl task {run.get('task_id')} not found")
    if task_row.get("org_id") != run.get("org_id"):
        raise RunRejected(f"crawl task {task_row['task_id']} org {task_row.get('org_id')} != run org {run.get('org_id')}")
    ac = db.get_actor_class(task_row["actor_class"])
    if not ac or not ac.get("enabled", True):
        raise RunRejected(f"actor_class {task_row['actor_class']!r} is not enabled")
    if ac.get("run_kind") != "crawl":
        raise RunRejected(f"actor_class {task_row['actor_class']!r} is a {ac.get('run_kind')} actor, not crawl")

    task = CrawlTask.from_db_row(task_row)
    validate_actor_params(task.actor_class, task.actor_params, ac.get("params_schema"))
    kwargs, max_items = build_crawl_kwargs(task, run)
    per_param = ac.get("cap_scope") == "per_param"
    return _Plan(
        actor_class=task.actor_class,
        search_params=task.search_params,
        kwargs=kwargs,
        dry_run=bool(run.get("dry_run")) or not task.publish,
        search_item_budget=max_items * (len(task.search_params) if per_param else 1),
        cost_budget_usd=_min_budget(msg.get("max_cost_usd"), task_row.get("max_cost_usd_per_run"), run.get("admitted_usd")),
        task_id=task_row["task_id"],
    )


def _expand(docs: List[Any]) -> Tuple[List[Any], int]:
    expanded, attached = [], 0
    for doc in docs:
        expanded.append(doc)
        extra = getattr(doc, "attached_news", None)
        if extra is not None:
            expanded.append(extra)
            attached += 1
    return expanded, attached


def _default_publish(doc: Any) -> None:
    from src.helpers.serialization import publish_document

    publish_document(doc)


def execute_run(run: Dict[str, Any], msg: Dict[str, Any], deps: WorkerDeps) -> str:
    """Run a claimed run to a terminal status; always writes the outcome. Returns the status."""
    run_id = run["run_id"]
    db = deps.db
    recorder = CallRecorder(
        on_call=lambda record: db.insert_call(record.as_row(run_id)),
        default_purpose="comments" if run.get("kind") == "comments" else "search",
    )
    status, error, output_path = "failed", None, None
    docs: List[Any] = []
    actor = None
    plan: Optional[_Plan] = None
    publish_failures = 0
    attached_out = 0
    main_out = 0
    stats: Dict[str, Any] = {"worker": deps.worker_name, "trace_id": msg.get("trace_id")}

    try:
        plan = plan_run(run, msg, deps)
        recorder.search_item_budget = plan.search_item_budget
        recorder.cost_budget_usd = plan.cost_budget_usd
        stats["effective_params"] = _jsonable(plan.kwargs)
        stats["dry_run"] = plan.dry_run
        diffs = diff_params(msg.get("params") or {}, plan.kwargs) if run.get("kind") != "comments" else []
        if diffs:
            logger.warning("run %s: message params differ from the DB on %s; running the DB version", run_id, diffs)
            stats["message_params_diff"] = diffs

        if plan.comment_posts:
            try:
                db.mark_comment_request_running(run["comment_request_id"])
            except Exception as exc:
                logger.warning("run %s: could not mark comment request running: %s", run_id, exc)

        if plan.kwargs.get("max_results") == 0:
            logger.info("run %s: max_items is 0 — nothing to buy", run_id)
        else:
            client = RecordingApifyClient(deps.apify_client_factory(), recorder, _meter_for(run, deps))
            actor = deps.actor_factory(plan.actor_class, client)
            instrument_actor(actor, recorder)
            docs = list(actor.search(plan.search_params, **plan.kwargs) or [])

        expanded, attached = _expand(docs)
        if plan.dry_run:
            output_path = _save_dry_run(deps.output_dir, plan.actor_class, run_id, expanded)
            main_out, attached_out = len(docs), attached
            logger.info("run %s: dry run — %d documents saved to %s (not published)", run_id, len(expanded), output_path)
        else:
            main_out, attached_out, publish_failures, publish_error = _publish_all(docs, deps.publish or _default_publish)
            if publish_error:
                raise RuntimeError(publish_error)
        status = "partial" if publish_failures else "succeeded"
        if publish_failures:
            error = f"{publish_failures} document(s) failed to publish"
    except Exception as exc:
        status = "partial" if recorder.calls else "failed"
        error = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, (RunRejected, AdmissionRefused)):
            logger.error("run %s %s: %s", run_id, "rejected" if isinstance(exc, RunRejected) else "refused", exc)
        else:
            logger.exception("run %s %s", run_id, status)

    refusal = recorder.refusal
    if refusal is not None:
        # Admission refused a call (the exception may have propagated or been swallowed
        # by an actor stage): the outcome follows the refusal, see the module docstring.
        stats["usage_refusal"] = _refusal_stats(recorder)
        error = (f"AdmissionRefused({refusal.reason}) on a {recorder.refusal_purpose} call: {refusal}"
                 + (f"; {error}" if error and "AdmissionRefused" not in error else ""))
        if recorder.calls:
            status = "partial"
        elif refusal.reason == "ledger_unreachable":
            if _release(db, run_id, error):
                return RELEASED
            status = "failed"
        else:
            status = "blocked"

    if plan is not None and plan.comment_posts and docs:
        _record_comment_posts(db, plan.comment_posts, docs)

    counters = _counters(run, actor, recorder, docs, main_out, attached_out)
    stats.update({
        "funnel": dict(getattr(actor, "funnel", {}) or {}),
        "calls": len(recorder.calls),
        "publish_failures": publish_failures,
        "cost_unknown_calls": sum(1 for c in recorder.calls if c.cost_usd is None),
    })
    db.finish_run(run_id, status, error, output_path, counters, stats)
    logger.info("run %s finished: status=%s items_paid=%s items_new=%s cost_usd=%s error=%s",
                run_id, status, counters.get("items_paid"), counters.get("items_new"), counters.get("cost_usd"), error)

    if run.get("trigger") == "schedule" and run.get("task_id") and status != "blocked":
        try:
            db.update_schedule_after_run(run["task_id"], run_id, status == "succeeded")
        except Exception as exc:
            logger.warning("run %s: schedule bookkeeping failed: %s", run_id, exc)
    if run.get("comment_request_id"):
        try:
            db.close_comment_request_if_done(run["comment_request_id"])
        except Exception as exc:
            logger.warning("run %s: comment request close failed: %s", run_id, exc)
    return status


def _meter_for(run: Dict[str, Any], deps: WorkerDeps) -> UsageMeter:
    return UsageMeter(
        org_id=run.get("org_id"),
        event_key=f"crawl-run:{run['run_id']}",
        user_id=run.get("requested_by"),
        search_id=run.get("query_id"),
        ledger=deps.usage_ledger,
    )


def _refusal_stats(recorder: CallRecorder) -> Dict[str, Any]:
    exc = recorder.refusal
    a = getattr(exc, "admission", None)
    return {
        "reason": exc.reason,
        "kind": exc.kind,
        "purpose": recorder.refusal_purpose,
        "asked_usd": getattr(a, "asked_usd", None),
        "allowed_usd": getattr(a, "allowed_usd", None),
        "limit_usd": getattr(a, "limit_usd", None),
        "spent_usd": getattr(a, "spent_usd", None),
        "remaining_usd": getattr(a, "remaining_usd", None),
        "limit_id": getattr(a, "limit_id", None),
    }


def _release(db: Any, run_id: int, error: str) -> bool:
    """``running`` → ``queued`` so the requeued message can claim it again."""
    try:
        return bool(db.release_run(run_id, error))
    except Exception as exc:
        logger.error("run %s: could not release the run back to queued (%s); failing it", run_id, exc)
        return False


def _publish_all(docs: List[Any], publish: Callable[[Any], None]) -> Tuple[int, int, int, Optional[str]]:
    """Publish docs + attached news through ``publish_document``. Returns
    (main published, attached published, failures, fatal error)."""
    from pika.exceptions import AMQPError

    main_ok = attached_ok = failures = 0
    for doc in docs:
        for item, is_attached in ((doc, False), (getattr(doc, "attached_news", None), True)):
            if item is None:
                continue
            try:
                publish(item)
            except AMQPError as exc:
                return main_ok, attached_ok, failures + 1, f"RabbitMQ publish failed: {exc}"
            except Exception as exc:
                failures += 1
                logger.error("Error publishing document %s: %s", getattr(item, "data", {}).get("url"), exc)
                continue
            if is_attached:
                attached_ok += 1
            else:
                main_ok += 1
    return main_ok, attached_ok, failures, None


def _save_dry_run(output_dir: str, actor_class: str, run_id: int, expanded: List[Any]) -> str:
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"{actor_class}_run_{run_id}_{ts}.json")
    results = []
    for doc in expanded:
        try:
            results.append(doc.to_final_schema())
        except Exception as exc:
            logger.error("dry run: could not serialize %s: %s", getattr(doc, "data", {}).get("url"), exc)
    from src.helpers.serialization import serialize_message

    with open(path, "w", encoding="utf-8") as f:
        f.write(serialize_message(results))
    return os.path.abspath(path)


def _record_comment_posts(db: Any, posts: List[Dict[str, Any]], docs: List[Any]) -> None:
    by_url = {d.data.get("url"): len(d.data.get("comments") or []) for d in docs if hasattr(d, "data")}
    for post in posts:
        if post["post_url"] in by_url:
            try:
                db.set_comment_post_fetched(post["row_id"], by_url[post["post_url"]])
            except Exception as exc:
                logger.warning("could not update comments_fetched for post row %s: %s", post["row_id"], exc)


def _counters(run: Dict[str, Any], actor: Any, recorder: CallRecorder, docs: List[Any],
              main_out: int, attached_out: int) -> Dict[str, Any]:
    funnel = getattr(actor, "funnel", None) or {}
    paid_purpose = "comments" if run.get("kind") == "comments" else "search"
    items_paid = recorder.items_for(paid_purpose)
    if items_paid is None and actor is not None:
        items_paid = funnel.get("docs_in")  # dataset count unknown: fall back to mapped items
    counters: Dict[str, Any] = {col: int(funnel.get(key, 0)) if actor is not None else None
                                for key, col in FUNNEL_TO_COLUMN.items()}
    counters.update({
        "items_paid": items_paid,
        "items_new": main_out if actor is not None else None,
        "attached_news_published": attached_out if actor is not None else None,
        "comments_fetched": sum(len(d.data.get("comments") or []) for d in docs if hasattr(d, "data")) if actor is not None else None,
        "profiles_scraped": recorder.items_for("profiles") or (0 if actor is not None else None),
        "newest_item_at": getattr(actor, "funnel_newest", None),
        "oldest_item_at": getattr(actor, "funnel_oldest", None),
        "cost_usd": round(recorder.cost_usd, 6) if recorder.calls else (0 if actor is not None else None),
    })
    return counters
