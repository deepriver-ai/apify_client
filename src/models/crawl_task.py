from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List

import openpyxl

from dateutil import parser as dateutil_parser

logger = logging.getLogger(__name__)

# Ad-hoc runs (docs/db/crawl_tasks.md §6.4): fields of a stored spec that change no filter
# decision (volume, cost, publishing, notes) stay out of the filter-cache key. Must match
# media-backend-paid backend/app/crawl/crawl_helpers.py (SPEC_KEY_EXCLUDED, adhoc_cache_key);
# both test suites pin the same vector.
SPEC_KEY_EXCLUDED = frozenset({"name", "metadata", "max_cost_usd_per_run", "max_results", "publish"})
ADHOC_KEY_PREFIX = "adhoc:"
# The writable crawl_tasks columns a spec carries (crawl_runs.params.spec); anything else is refused.
SPEC_FIELDS = frozenset({
    "name", "actor_class", "search_params", "country_id", "language", "max_results", "min_date", "period",
    "publish", "get_comments", "max_comments", "not_keywords", "llm_filter_condition", "update_existing",
    "enrich_followers", "fetch_attached_url", "classify_users", "official_page", "actor_params",
    "max_cost_usd_per_run", "metadata",
})


def _spec_min_date_iso(value: Any) -> Any:
    """``min_date`` as the backend stores it in a spec: ISO 8601 in UTC (naive = UTC)."""
    if value is None:
        return None
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def adhoc_cache_key(spec: Dict[str, Any]) -> str:
    """``adhoc:<sha1>`` of the canonical spec minus ``SPEC_KEY_EXCLUDED`` (search_params
    sorted, ``min_date`` as UTC ISO; json sort_keys, compact separators, UTF-8)."""
    keyed = {k: v for k, v in spec.items() if k not in SPEC_KEY_EXCLUDED}
    if isinstance(keyed.get("search_params"), list):
        keyed["search_params"] = sorted({str(p).strip() for p in keyed["search_params"] if str(p).strip()})
    if "min_date" in keyed:
        keyed["min_date"] = _spec_min_date_iso(keyed["min_date"])
    blob = json.dumps(keyed, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return ADHOC_KEY_PREFIX + hashlib.sha1(blob.encode("utf-8")).hexdigest()


@dataclass
class CrawlTask:
    """Represents a single crawl task loaded from the tasks CSV."""

    task_id: str
    actor_class: str
    search_params: List[str]
    country_id: str | None = None
    language: str | None = None
    max_results: int = 30
    min_date: datetime | None = None
    period: str | None = None
    enabled: bool = True
    publish: bool = True
    get_comments: bool = False
    max_comments: int = 15
    not_keywords: List[str] = field(default_factory=list)
    llm_filter_condition: str | None = None
    override_filters: bool = False
    update_existing: bool = False
    enrich_followers: bool = False
    fetch_attached_url: bool = False
    # Post-run user classification (WS-4): opt-in per task — this task's scope
    # (page name for page tasks, search_params as phrases for keyword tasks)
    # feeds the theme's classify_users pass. official_page marks page tasks whose
    # account belongs to the customer (deterministic pagina_oficial bypass).
    classify_users: bool = False
    official_page: bool = False
    theme: str | None = None
    actor_params: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_csv_row(cls, row: Dict[str, str]) -> CrawlTask:
        """Parse a CSV DictReader row into a CrawlTask."""
        search_params = [k.strip() for k in row["search_params"].split(",") if k.strip()]

        actor_params_raw = row.get("actor_params", "").strip()
        actor_params = json.loads(actor_params_raw) if actor_params_raw else {}

        def parse_bool(value: str, default: bool = True) -> bool:
            if not value or not value.strip():
                return default
            return value.strip().lower() in ("true", "1", "yes", "t")

        def parse_min_date(value: str) -> datetime | None:
            raw = value.strip() if value else ""
            if not raw:
                return None
            try:
                return dateutil_parser.parse(raw)
            except (ValueError, OverflowError):
                logger.warning("Could not parse min_date '%s', ignoring", raw)
                return None

        min_date = parse_min_date(row.get("min_date", ""))

        period_raw = row.get("period", "").strip().lower() or None
        if period_raw and period_raw not in ("d", "w", "m"):
            logger.warning("Invalid period '%s', ignoring", period_raw)
            period_raw = None

        if min_date is not None and period_raw is not None:
            raise ValueError(
                "Task row has both min_date and period set. They are mutually exclusive."
            )

        not_keywords_raw = row.get("not_keywords", "").strip()
        not_keywords = [k.strip() for k in not_keywords_raw.split("|") if k.strip()] if not_keywords_raw else []

        llm_filter_condition = row.get("llm_filter_condition", "").strip() or None

        task_id = row.get("task_id", "").strip()
        if not task_id:
            # Auto-generate from actor_class + search_params
            task_id = f"{row['actor_class'].strip()}:{','.join(search_params)}"

        return cls(
            task_id=task_id,
            actor_class=row["actor_class"].strip(),
            search_params=search_params,
            country_id=row.get("country_id", "").strip() or None,
            language=row.get("language", "").strip() or None,
            max_results=int(row.get("max_results", "").strip() or 30),
            min_date=min_date,
            period=period_raw,
            enabled=parse_bool(row.get("enabled", "")),
            publish=parse_bool(row.get("publish", "")),
            get_comments=parse_bool(row.get("get_comments", ""), default=False),
            max_comments=int(row.get("max_comments", "").strip() or 15),
            not_keywords=not_keywords,
            llm_filter_condition=llm_filter_condition,
            override_filters=parse_bool(row.get("override_filters", ""), default=False),
            update_existing=parse_bool(row.get("update_existing", ""), default=False),
            enrich_followers=parse_bool(row.get("enrich_followers", ""), default=False),
            fetch_attached_url=parse_bool(row.get("fetch_attached_url", ""), default=False),
            classify_users=parse_bool(row.get("classify_users", ""), default=False),
            official_page=parse_bool(row.get("official_page", ""), default=False),
            theme=row.get("theme", "").strip().lower() or None,
            actor_params=actor_params,
        )

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> CrawlTask:
        """Build a CrawlTask from a userdb ``crawl_tasks`` row (dict, jsonb already decoded).

        Same field semantics as ``from_csv_row`` (docs/db/crawl_tasks.md §2.2 / §3), so
        ``to_actor_kwargs()`` stays the single param path for the xlsx hand runs and the
        queue worker. ``task_id`` is the filter-cache key: ``legacy_task_id`` for imported
        rows (keeps their cached decisions), else ``db:<task_id>``. ``override_filters``
        is a run option, not a task column, so it defaults to False here. ``enabled``
        mirrors ``status == 'active'``; the worker runs whatever run row it is handed.
        """
        legacy = row.get("legacy_task_id")
        return cls._from_columns(row, str(legacy) if legacy else f"db:{row['task_id']}",
                                 f"crawl task {row.get('task_id')}")

    @classmethod
    def from_spec(cls, spec: Dict[str, Any], cache_key: str | None = None) -> CrawlTask:
        """Build a CrawlTask from an ad-hoc run's stored spec (``crawl_runs.params.spec``,
        design §6.4): the writable task columns, validated by the backend with the task
        rules. Same column semantics as ``from_db_row`` (one shared constructor, then
        ``to_actor_kwargs()``); the filter-cache key is ``adhoc_cache_key(spec)``, so the
        same ad-hoc crawl reuses its cached filter decisions. ``min_date`` arrives as an
        ISO string. Unknown keys are refused rather than ignored.

        ``cache_key`` overrides the filter-cache key: a **batch** run (docs/db/crawl_tasks.md
        §6.5) stores its merged spec the same way and passes its ``batch:<sha1>`` key, read
        from ``crawl_runs.params.task_id`` (the backend's; not recomputed here)."""
        if not isinstance(spec, dict):
            raise ValueError("ad-hoc spec must be an object")
        unknown = sorted(set(spec) - SPEC_FIELDS)
        if unknown:
            raise ValueError(f"ad-hoc spec has unknown field(s) {unknown}")
        for required in ("actor_class", "search_params"):
            if not spec.get(required):
                raise ValueError(f"ad-hoc spec is missing {required!r}")
        if not isinstance(spec["search_params"], list):
            raise ValueError("ad-hoc spec search_params must be a list")
        row = dict(spec)
        if row.get("min_date") is not None:
            try:
                row["min_date"] = datetime.fromisoformat(_spec_min_date_iso(row["min_date"]))
            except (TypeError, ValueError):
                raise ValueError(f"ad-hoc spec min_date {spec['min_date']!r} is not ISO 8601") from None
        row["status"] = "active"
        return cls._from_columns(row, cache_key or adhoc_cache_key(spec), "batch spec" if cache_key else "ad-hoc spec")

    @classmethod
    def _from_columns(cls, row: Dict[str, Any], cache_key: str, what: str) -> CrawlTask:
        """The one column → CrawlTask mapping behind ``from_db_row`` and ``from_spec``."""
        search_params = [str(p).strip() for p in (row.get("search_params") or []) if str(p).strip()]
        if not search_params:
            raise ValueError(f"{what} has no search_params")
        period = row.get("period")
        if period is not None and period not in ("d", "w", "m"):
            raise ValueError(f"{what}: invalid period {period!r}")
        min_date = row.get("min_date")
        if min_date is not None and period is not None:
            raise ValueError(f"{what} has both min_date and period set. They are mutually exclusive.")
        actor_params = row.get("actor_params") or {}
        if not isinstance(actor_params, dict):
            raise ValueError(f"{what}: actor_params must be an object")
        return cls(
            task_id=cache_key,
            actor_class=row["actor_class"],
            search_params=search_params,
            country_id=row.get("country_id") or None,
            language=row.get("language") or None,
            max_results=int(row.get("max_results") or 30),
            min_date=min_date,
            period=period,
            enabled=row.get("status", "active") == "active",
            publish=bool(row.get("publish", True)),
            get_comments=bool(row.get("get_comments", False)),
            max_comments=int(row["max_comments"]) if row.get("max_comments") is not None else 15,
            not_keywords=[str(k) for k in (row.get("not_keywords") or []) if str(k).strip()],
            llm_filter_condition=row.get("llm_filter_condition") or None,
            override_filters=False,
            update_existing=bool(row.get("update_existing", False)),
            enrich_followers=bool(row.get("enrich_followers", False)),
            fetch_attached_url=bool(row.get("fetch_attached_url", False)),
            classify_users=bool(row.get("classify_users", False)),
            official_page=bool(row.get("official_page", False)),
            theme=row.get("legacy_theme") or None,
            actor_params=dict(actor_params),
        )

    def to_actor_kwargs(self) -> Dict[str, Any]:
        """Merge common params and actor-specific params into kwargs for the actor."""
        kwargs: Dict[str, Any] = {
            "task_id": self.task_id,
            "max_results": self.max_results,
            "country_id": self.country_id,
            "language": self.language,
            "min_date": self.min_date,
            "period": self.period,
            "get_comments": self.get_comments,
            "max_comments": self.max_comments,
            "not_keywords": self.not_keywords,
            "llm_filter_condition": self.llm_filter_condition,
            "override_filters": self.override_filters,
            "update_existing": self.update_existing,
            "enrich_followers": self.enrich_followers,
            "fetch_attached_url": self.fetch_attached_url,
        }
        kwargs.update(self.actor_params)
        return kwargs


def load_tasks(xlsx_path: str) -> List[CrawlTask]:
    """Read a tasks Excel (.xlsx) file and return only enabled tasks."""
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active

    rows = iter(ws.rows)
    header = [str(cell.value).strip() if cell.value is not None else "" for cell in next(rows)]

    tasks: List[CrawlTask] = []
    for excel_row in rows:
        row = {header[i]: (str(cell.value).strip() if cell.value is not None else "") for i, cell in enumerate(excel_row)}
        task = CrawlTask.from_csv_row(row)
        if task.enabled:
            tasks.append(task)
        else:
            logger.debug("Skipping disabled task: %s %s", task.actor_class, task.search_params)

    wb.close()
    return tasks
