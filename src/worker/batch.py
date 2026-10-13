"""Batched crawl runs (``crawl_runs.kind = 'batch'``): members, filter-cache keys and the
approximate per-member attribution.

Design: docs/db/crawl_tasks.md §6.5; plan and decisions:
business_agent/docs/plans/crawl_batched_runs_2026-10.md (D6, D8, D9).

A batch run is several crawl tasks of one org that share every field but
``search_params``; the backend stores the merged definition in ``params.spec`` and the
members in ``crawl_run_tasks``. The actor runs once over the union of the params; this
module tells, for each document, which members it belongs to — **by text match**, because
a merged Apify call doesn't say which keyword found an item:

* the document's title + body (+ its attached article), lower-cased and without accents;
* a param matches when its words appear as a phrase (quotes and a leading ``#`` stripped);
  a URL param (page / profile actors) matches the document's URL, profile URL or source by
  the URL's first path segment (the page or account name);
* ``twitter_keyword_search`` members match when **all** their params match (the X actor
  ANDs a task's params into one clause); every other actor's members when **any** does.

A document can belong to several members (counted for each) or to none (``unmatched``).

Uses: the filter cache (a batch reads the batch key and every member's key, and writes a
decision under the batch key and the keys of the members the document belongs to), and
the per-member numbers written to ``crawl_run_tasks``: ``items_matched`` (paid items
entering the pipeline), ``items_new`` (documents to publish), ``newest_item_at`` and
``cost_usd`` (the run's cost split by matched items; unmatched items are split evenly).
The run row keeps the authoritative counters and cost.
"""
from __future__ import annotations

import re
import unicodedata
from urllib.parse import urlsplit
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dateutil import parser as dateutil_parser

_MX_FIXED_TZ = timezone(timedelta(hours=-6))   # same convention as ApifyActor._count_pipeline_input
ALL_PARAMS_ACTORS = frozenset({"twitter_keyword_search"})


def normalize(text: str) -> str:
    """Lower-case, accents stripped, every non-alphanumeric run collapsed to one space."""
    decomposed = unicodedata.normalize("NFKD", text or "")
    plain = "".join(c for c in decomposed if not unicodedata.combining(c)).lower()
    return " " + re.sub(r"[^0-9a-z]+", " ", plain).strip() + " "


def _is_url(param: str) -> bool:
    p = param.strip().lower()
    return p.startswith(("http://", "https://", "www.")) or ".com/" in p


def _param_needle(param: str) -> Tuple[str, str]:
    """(where, needle): ``("url", <page slug>)`` for a URL param, else ``("text", <phrase>)``."""
    p = str(param).strip()
    if _is_url(p):
        path = urlsplit(p if "://" in p else "https://" + p).path
        slug = next((seg for seg in path.split("/") if seg), "")
        return "url", normalize(slug).strip()
    return "text", normalize(p.strip('"').strip("'").lstrip("#")).strip()


def doc_text(doc: Any) -> str:
    data = getattr(doc, "data", None) or {}
    parts = [data.get("title") or "", data.get("body") or ""]
    attached = getattr(doc, "attached_news", None)
    if attached is not None:
        adata = getattr(attached, "data", None) or {}
        parts += [adata.get("title") or "", adata.get("body") or ""]
    return normalize(" ".join(str(p) for p in parts))


def doc_url_text(doc: Any) -> str:
    data = getattr(doc, "data", None) or {}
    return normalize(" ".join(str(data.get(k) or "") for k in ("url", "profile_url", "source", "author")))


def doc_timestamp(doc: Any) -> Optional[datetime]:
    raw = (getattr(doc, "data", None) or {}).get("timestamp")
    if not raw:
        return None
    try:
        ts = raw if isinstance(raw, datetime) else dateutil_parser.parse(str(raw))
    except (ValueError, OverflowError, TypeError):
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=_MX_FIXED_TZ)


@dataclass
class BatchMember:
    task_id: int
    search_params: List[str]
    max_items: int
    filter_cache_key: str
    needles: List[Tuple[str, str]] = field(default_factory=list)

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "BatchMember":
        params = [str(p) for p in (row.get("search_params") or [])]
        return cls(task_id=int(row["task_id"]), search_params=params, max_items=int(row.get("max_items") or 0),
                   filter_cache_key=str(row["filter_cache_key"]),
                   needles=[n for n in (_param_needle(p) for p in params) if n[1]])


@dataclass
class _Tally:
    matched: int = 0
    new: int = 0
    newest: Optional[datetime] = None


class BatchContext:
    """Attached to the actor as ``actor.batch`` by the worker for a batch run."""

    def __init__(self, actor_class: str, members: Iterable[BatchMember]):
        self.actor_class = actor_class
        self.members = list(members)
        self.require_all = actor_class in ALL_PARAMS_ACTORS
        self.tallies: Dict[int, _Tally] = {m.task_id: _Tally() for m in self.members}
        self.items_in = 0
        self.unmatched = 0

    @property
    def read_keys(self) -> List[str]:
        """Member filter-cache keys a batch also honours: same filters by the group key (D8)."""
        return [m.filter_cache_key for m in self.members]

    def members_of(self, doc: Any) -> List[BatchMember]:
        hay = {"text": doc_text(doc), "url": doc_url_text(doc)}
        out = []
        for m in self.members:
            hits = [f" {needle} " in hay[where] for where, needle in m.needles]
            if hits and (all(hits) if self.require_all else any(hits)):
                out.append(m)
        return out

    def doc_keys(self, doc: Any) -> List[str]:
        """Member keys a filter decision about ``doc`` is also written under."""
        return [m.filter_cache_key for m in self.members_of(doc)]

    def on_pipeline_input(self, documents: Iterable[Any]) -> None:
        """Paid items entering ``process_documents`` (``ApifyActor._count_pipeline_input``)."""
        for doc in documents:
            self.items_in += 1
            owners = self.members_of(doc)
            if not owners:
                self.unmatched += 1
            ts = doc_timestamp(doc)
            for m in owners:
                t = self.tallies[m.task_id]
                t.matched += 1
                if ts is not None and (t.newest is None or ts > t.newest):
                    t.newest = ts

    def attribution(self, published: Iterable[Any], run_cost_usd: Optional[float]) -> List[Dict[str, Any]]:
        """One ``crawl_run_tasks`` update per member. ``published`` = the run's main
        documents to publish (would-be-published in a dry run)."""
        for doc in published:
            for m in self.members_of(doc):
                self.tallies[m.task_id].new += 1
        n = len(self.members) or 1
        weights = {tid: t.matched + self.unmatched / n for tid, t in self.tallies.items()}
        total = sum(weights.values())
        rows = []
        for m in self.members:
            t = self.tallies[m.task_id]
            share = (weights[m.task_id] / total) if total else 1 / n
            rows.append({
                "task_id": m.task_id,
                "items_matched": t.matched,
                "items_new": t.new,
                "newest_item_at": t.newest,
                "cost_usd": None if run_cost_usd is None else round(float(run_cost_usd) * share, 6),
                "stats": {"attribution": "keyword_match", "match": "all_params" if self.require_all else "any_param",
                          "cost_share": round(share, 6), "run_items_in": self.items_in,
                          "run_unmatched": self.unmatched, "saturated": t.matched >= m.max_items > 0},
            })
        return rows
