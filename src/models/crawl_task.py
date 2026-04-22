from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List

import openpyxl

from dateutil import parser as dateutil_parser

logger = logging.getLogger(__name__)


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
    enrich_followers: bool = False
    fetch_attached_url: bool = False
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
            enrich_followers=parse_bool(row.get("enrich_followers", ""), default=False),
            fetch_attached_url=parse_bool(row.get("fetch_attached_url", ""), default=False),
            theme=row.get("theme", "").strip().lower() or None,
            actor_params=actor_params,
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
