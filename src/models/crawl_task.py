from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List

from dateutil import parser as dateutil_parser

logger = logging.getLogger(__name__)


@dataclass
class CrawlTask:
    """Represents a single crawl task loaded from the tasks CSV."""

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
            return value.strip().lower() in ("true", "1", "yes")

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

        return cls(
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
            actor_params=actor_params,
        )

    def to_actor_kwargs(self) -> Dict[str, Any]:
        """Merge common params and actor-specific params into kwargs for the actor."""
        kwargs: Dict[str, Any] = {
            "max_results": self.max_results,
            "country_id": self.country_id,
            "language": self.language,
            "min_date": self.min_date,
            "period": self.period,
            "get_comments": self.get_comments,
            "max_comments": self.max_comments,
        }
        kwargs.update(self.actor_params)
        return kwargs


def load_tasks(csv_path: str) -> List[CrawlTask]:
    """Read a tasks CSV file and return only enabled tasks."""
    tasks: List[CrawlTask] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            task = CrawlTask.from_csv_row(row)
            if task.enabled:
                tasks.append(task)
            else:
                logger.debug("Skipping disabled task: %s %s", task.actor_class, task.keywords)
    return tasks
