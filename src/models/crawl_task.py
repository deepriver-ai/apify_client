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
    keywords: List[str]
    country_id: str | None = None
    language: str | None = None
    max_results: int = 30
    method: str = "search"
    min_date: datetime | None = None
    enabled: bool = True
    publish: bool = True
    actor_params: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_csv_row(cls, row: Dict[str, str]) -> CrawlTask:
        """Parse a CSV DictReader row into a CrawlTask."""
        keywords = [k.strip() for k in row["keywords"].split(",") if k.strip()]

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

        return cls(
            actor_class=row["actor_class"].strip(),
            keywords=keywords,
            country_id=row.get("country_id", "").strip() or None,
            language=row.get("language", "").strip() or None,
            max_results=int(row.get("max_results", "").strip() or 30),
            method=row.get("method", "").strip() or "search",
            min_date=parse_min_date(row.get("min_date", "")),
            enabled=parse_bool(row.get("enabled", "")),
            publish=parse_bool(row.get("publish", "")),
            actor_params=actor_params,
        )

    def to_actor_kwargs(self) -> Dict[str, Any]:
        """Merge common params and actor-specific params into kwargs for the actor."""
        kwargs: Dict[str, Any] = {
            "max_results": self.max_results,
            "country_id": self.country_id,
            "language": self.language,
            "min_date": self.min_date,
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
