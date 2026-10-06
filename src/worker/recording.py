"""Recording Apify client: every Apify call of a crawl run is captured once.

Design: docs/db/crawl_tasks.md §2.6 (``crawl_run_calls``), §6.1, §7.3, §7.4.

``ApifyActor.run_actor()`` and the utility actors (profiles, comments, video) all go
through ``client.actor(id).call(run_input=...)``. The worker hands each actor a
``RecordingApifyClient`` instead of the raw ``ApifyClient``, so no actor code changes:

- each call's run object (``id``, ``status``, ``usageTotalUsd``, ``usage``) and dataset
  item count are passed to ``CallRecorder.on_call`` the moment the call returns, so the
  cost is persisted before anything else can fail (§5.2 ``partial``);
- calls are tagged with a ``purpose`` (search / profiles / comments / video / other) from
  the pipeline stage they run in (``instrument_actor``);
- paid search items are capped server-side with Apify's ``max_items`` (pay-per-result
  actors) to the run's remaining item budget, and every call carries
  ``max_total_charge_usd`` for the remaining cost budget when the run has one;
- with ``usage_client`` enabled, the call goes through ``usage_client.apify.run(actor,
  input, ctx=ctx)`` (rate_limiting.md §2.6) instead of the raw client.
"""
from __future__ import annotations

import functools
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

PURPOSES = ("search", "profiles", "comments", "video", "other")

# Pipeline stage -> crawl_run_calls.purpose for Apify calls made inside that stage.
STAGE_PURPOSE = {
    "_enrich_user_author": "profiles",
    "_enrich_comments": "comments",
    "_enrich_content": "other",      # attached-URL fetch is not Apify; video downloads are
    "_download_video": "video",
}


class CostCapExceeded(RuntimeError):
    """The run's cost budget is spent; no further Apify call is made."""


class ItemCapExceeded(RuntimeError):
    """The run's paid-item budget for search calls is spent; no further search call."""


@dataclass
class CallRecord:
    """One Apify call, shaped like a ``crawl_run_calls`` row (minus run_id/call_id)."""

    purpose: str
    apify_actor_id: str
    apify_run_id: str
    apify_status: Optional[str]
    items: Optional[int]
    cost_usd: Optional[float]
    estimated: bool = False
    usage: Optional[Dict[str, Any]] = None
    usage_event_id: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    def as_row(self, run_id: int) -> Dict[str, Any]:
        return {
            "run_id": run_id,
            "purpose": self.purpose,
            "apify_actor_id": self.apify_actor_id,
            "apify_run_id": self.apify_run_id,
            "apify_status": self.apify_status,
            "items": self.items,
            "cost_usd": self.cost_usd,
            "estimated": self.estimated,
            "usage": self.usage,
            "usage_event_id": self.usage_event_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


@dataclass
class CallRecorder:
    """Collects the calls of one run and enforces its item / cost budgets.

    ``search_item_budget``: max paid items across ``purpose='search'`` calls (None = no
    cap). ``cost_budget_usd``: max total cost across all calls (None = no cap).
    ``on_call(record)`` persists each call (the worker inserts ``crawl_run_calls``).
    """

    on_call: Callable[[CallRecord], None] = lambda record: None
    search_item_budget: Optional[int] = None
    cost_budget_usd: Optional[float] = None
    default_purpose: str = "search"
    calls: List[CallRecord] = field(default_factory=list)
    _purpose_stack: List[str] = field(default_factory=list)

    @property
    def purpose(self) -> str:
        return self._purpose_stack[-1] if self._purpose_stack else self.default_purpose

    def push(self, purpose: str) -> None:
        self._purpose_stack.append(purpose)

    def pop(self) -> None:
        if self._purpose_stack:
            self._purpose_stack.pop()

    # --- totals ---
    @property
    def cost_usd(self) -> float:
        return float(sum(c.cost_usd or 0 for c in self.calls))

    def items_for(self, purpose: str) -> Optional[int]:
        """Sum of known dataset item counts for ``purpose``; None when none is known."""
        counted = [c.items for c in self.calls if c.purpose == purpose and c.items is not None]
        return sum(counted) if counted else None

    def remaining_search_items(self) -> Optional[int]:
        if self.search_item_budget is None:
            return None
        used = sum(c.items or 0 for c in self.calls if c.purpose == "search")
        return max(self.search_item_budget - used, 0)

    def remaining_cost(self) -> Optional[float]:
        if self.cost_budget_usd is None:
            return None
        return max(self.cost_budget_usd - self.cost_usd, 0.0)

    def record(self, record: CallRecord) -> None:
        self.calls.append(record)
        try:
            self.on_call(record)
        except Exception:  # persistence has its own spool; never break the run here
            logger.exception("on_call failed for apify run %s", record.apify_run_id)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None or isinstance(value, datetime):
        return value
    try:
        from dateutil import parser as dateutil_parser

        return dateutil_parser.parse(str(value))
    except (ValueError, OverflowError, TypeError):
        return None


class _RecordingActorClient:
    def __init__(self, owner: "RecordingApifyClient", actor_id: str):
        self._owner = owner
        self._actor_id = actor_id

    def call(self, *, run_input: Any = None, **kwargs) -> Optional[Dict[str, Any]]:
        return self._owner._call(self._actor_id, run_input, kwargs)

    def __getattr__(self, name: str):
        return getattr(self._owner._client.actor(self._actor_id), name)


class RecordingApifyClient:
    """Drop-in for ``ApifyClient`` as used by the actors (``actor(id).call``, ``dataset``)."""

    def __init__(
        self,
        client: Any,
        recorder: CallRecorder,
        usage_runner: Optional[Callable[[str, Dict[str, Any]], Dict[str, Any]]] = None,
    ):
        self._client = client
        self._recorder = recorder
        self._usage_runner = usage_runner

    def actor(self, actor_id: str) -> _RecordingActorClient:
        return _RecordingActorClient(self, actor_id)

    def dataset(self, dataset_id: str):
        return self._client.dataset(dataset_id)

    def __getattr__(self, name: str):
        return getattr(self._client, name)

    # --- the one place an Apify run is started ---
    def _call(self, actor_id: str, run_input: Any, kwargs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        rec = self._recorder
        purpose = rec.purpose

        remaining_cost = rec.remaining_cost()
        if remaining_cost is not None and remaining_cost <= 0:
            raise CostCapExceeded(
                f"run cost budget {rec.cost_budget_usd} USD spent ({rec.cost_usd:.6f}); "
                f"skipping {purpose} call to {actor_id}"
            )
        if purpose == "search":
            remaining_items = rec.remaining_search_items()
            if remaining_items is not None and remaining_items <= 0:
                raise ItemCapExceeded(
                    f"run item budget {rec.search_item_budget} spent; skipping search call to {actor_id}"
                )
        else:
            remaining_items = None

        started = _now()
        if self._usage_runner is not None:
            # usage_client caps maxItems to the org's remaining budget and records the
            # usage_events row itself (rate_limiting.md §2.6/§2.7).
            run = self._usage_runner(actor_id, run_input)
        else:
            call_kwargs = dict(kwargs)
            if remaining_items is not None:
                prior = call_kwargs.get("max_items")
                call_kwargs["max_items"] = remaining_items if prior is None else min(prior, remaining_items)
            if remaining_cost is not None:
                call_kwargs.setdefault("max_total_charge_usd", Decimal(str(round(remaining_cost, 6))))
            run = self._client.actor(actor_id).call(run_input=run_input, **call_kwargs)
        finished = _now()

        if not run:
            logger.warning("Apify call to %s returned no run object (purpose=%s)", actor_id, purpose)
            return run

        run = self._ensure_usage(run)
        items = self._dataset_item_count(run)
        record = CallRecord(
            purpose=purpose if purpose in PURPOSES else "other",
            apify_actor_id=actor_id,
            apify_run_id=str(run.get("id")),
            apify_status=run.get("status"),
            items=items,
            cost_usd=_to_float(run.get("usageTotalUsd")),
            estimated=False,
            usage=run.get("usage"),
            usage_event_id=run.get("usage_event_id") or run.get("usageEventId"),
            started_at=_parse_ts(run.get("startedAt")) or started,
            finished_at=_parse_ts(run.get("finishedAt")) or finished,
        )
        rec.record(record)
        logger.info(
            "apify call recorded: purpose=%s actor=%s run=%s status=%s items=%s cost_usd=%s",
            record.purpose, actor_id, record.apify_run_id, record.apify_status, items, record.cost_usd,
        )
        return run

    def _ensure_usage(self, run: Dict[str, Any]) -> Dict[str, Any]:
        """``usageTotalUsd`` can lag the finished run; re-read the run object once."""
        if run.get("usageTotalUsd") is not None or not run.get("id"):
            return run
        try:
            fresh = self._client.run(run["id"]).get()
            if fresh:
                merged = dict(run)
                for key in ("usageTotalUsd", "usage", "status", "finishedAt"):
                    if fresh.get(key) is not None:
                        merged[key] = fresh[key]
                return merged
        except Exception as exc:
            logger.warning("could not re-read apify run %s for usage: %s", run.get("id"), exc)
        return run

    def _dataset_item_count(self, run: Dict[str, Any]) -> Optional[int]:
        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            return None
        try:
            info = self._client.dataset(dataset_id).get()
            count = (info or {}).get("itemCount")
            return int(count) if count is not None else None
        except Exception as exc:
            logger.warning("could not read item count of dataset %s: %s", dataset_id, exc)
            return None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def instrument_actor(actor: Any, recorder: CallRecorder) -> None:
    """Tag Apify calls made inside enrichment stages with their purpose.

    Wraps the stage methods on the *instance* (no actor code changes); calls made
    outside them keep ``recorder.default_purpose`` (``search``, or ``comments`` for
    comments runs).
    """
    for stage, purpose in STAGE_PURPOSE.items():
        method = getattr(actor, stage, None)
        if method is None or not callable(method):
            continue

        def wrapped(*args, __method=method, __purpose=purpose, **kwargs):
            recorder.push(__purpose)
            try:
                return __method(*args, **kwargs)
            finally:
                recorder.pop()

        functools.update_wrapper(wrapped, method)
        setattr(actor, stage, wrapped)
