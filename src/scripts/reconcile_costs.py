"""Reconcile ``crawl_run_calls`` / ``crawl_runs.cost_usd`` and their ledger rows with Apify.

Why (2026-10-07): every Apify call was recorded with the cost read the moment the call
returned — before pay-per-event charges settled — so the calls held only the actor-start
charge (9 calls: $0.0021 recorded vs $0.6727 billed). usage_client now settles the cost
before recording (``usage_client.apify_cost.settle_run``); a run that doesn't settle in
time is stored ``estimated=true`` with usage_client's own figure. This script re-reads
those runs later (or every run, ``--all``) and corrects them:

- ``crawl_run_calls``: ``cost_usd`` = Apify's settled ``usageTotalUsd``, ``estimated =
  false``, ``usage`` = the settled charges (``chargedEventCounts`` …), ``items`` = the
  dataset ``itemCount`` when it was unknown. These rows are the worker's own run log, so
  they are UPDATEd (``crawler`` has no UPDATE on ``crawl_run_calls``: run this with an
  owner / admin DSN, never as ``crawler_worker``).
- ``crawl_runs.cost_usd`` = Σ ``crawl_run_calls.cost_usd`` of the run — only for runs in a
  terminal status (a running run's worker writes its own total when it finishes; re-run
  this afterwards). ``stats.cost_reconciled_at`` records when.
- the matching ``usage_events`` rows (``crawl_run_calls.usage_event_id``) through
  ``usage_client.reconcile``: the ledger is append-only, so a difference becomes a
  ``<event_id>:correction`` row (see usage_client README § Reconciliation).

A call is corrected only when its run is settled now (terminal, has ``usageTotalUsd``,
charges cover the items or it finished ≥ ``--min-age-s`` ago); otherwise it is reported
``unsettled`` and left for a later pass.

    python -m src.scripts.reconcile_costs --db-uri <userdb> [--ledger-dsn <userdb>] \\
        [--run-id N ...] [--all] [--apply] [--min-age-s 600] [--json]

Dry run by default (read-only sessions; prints old vs settled per call, per run and the
ledger corrections). Apify: GETs only (runs and datasets) with ``APIFI_API_TOKEN``.
Hand runs (``run_searches.py``, scripts) have no ``crawl_run_calls`` rows: reconcile their
ledger rows with ``python -m usage_client.reconcile --service apify-client --since …``.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Sequence

from usage_client import apify_cost
from usage_client.apify_cost import as_dict

logger = logging.getLogger(__name__)

TERMINAL_RUN_STATUSES = ("succeeded", "partial", "failed", "blocked", "cancelled")
EPS_USD = 1e-9


@dataclass
class CallFix:
    call_id: int
    run_id: int
    purpose: str
    apify_actor_id: str
    apify_run_id: str
    usage_event_id: Optional[str]
    old_cost_usd: Optional[float]
    old_estimated: bool
    old_items: Optional[int]
    new_cost_usd: Optional[float] = None
    new_items: Optional[int] = None
    computed_usd: Optional[float] = None
    status: str = "pending"          # corrected | ok | unsettled | missing_run | error
    note: Optional[str] = None
    usage: Optional[Dict[str, Any]] = None

    @property
    def delta_usd(self) -> float:
        if self.new_cost_usd is None:
            return 0.0
        return round(self.new_cost_usd - (self.old_cost_usd or 0.0), 10)


def _f(v: Any) -> Optional[float]:
    return None if v is None else float(v)


def connect(dsn: str, *, read_only: bool):
    import psycopg
    from psycopg.rows import dict_row

    kw = {"options": "-c default_transaction_read_only=on"} if read_only else {}
    return psycopg.connect(dsn, row_factory=dict_row, connect_timeout=10, application_name="reconcile_costs", **kw)


def load_calls(conn, *, run_ids: Sequence[int] = (), all_calls: bool = False) -> List[Dict[str, Any]]:
    where, params = [], []
    if run_ids:
        where.append("c.run_id = ANY(%s)")
        params.append(list(run_ids))
    if not all_calls:
        where.append("(c.estimated OR c.cost_usd IS NULL)")
    sql = ("SELECT c.call_id, c.run_id, c.purpose, c.apify_actor_id, c.apify_run_id, c.items, c.cost_usd, "
           "c.estimated, c.usage_event_id, r.status AS run_status FROM public.crawl_run_calls c "
           "JOIN public.crawl_runs r USING (run_id)"
           + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY c.call_id")
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def check_call(row: Dict[str, Any], client: Any, *, min_age_s: float = 600.0, now=None) -> CallFix:
    fix = CallFix(call_id=row["call_id"], run_id=row["run_id"], purpose=row["purpose"],
                  apify_actor_id=row["apify_actor_id"], apify_run_id=row["apify_run_id"],
                  usage_event_id=row.get("usage_event_id"), old_cost_usd=_f(row.get("cost_usd")),
                  old_estimated=bool(row.get("estimated")), old_items=row.get("items"))
    try:
        run = as_dict(client.run(fix.apify_run_id).get())
    except Exception as exc:  # noqa: BLE001
        fix.status, fix.note = "error", f"{type(exc).__name__}: {exc}"
        return fix
    if not run:
        fix.status, fix.note = "missing_run", "Apify returned no run"
        return fix
    items = fix.old_items
    if run.get("defaultDatasetId"):
        try:
            info = as_dict(client.dataset(run["defaultDatasetId"]).get()) or {}
            if info.get("itemCount") is not None:
                items = int(info["itemCount"])
        except Exception as exc:  # noqa: BLE001
            logger.warning("dataset of run %s unreadable: %s", fix.apify_run_id, exc)
    fix.new_items = items
    fix.computed_usd = apify_cost.run_cost(run, items)
    if not apify_cost.is_settled_now(run, items, min_age_s=min_age_s, now=now):
        fix.status, fix.note = "unsettled", f"status={run.get('status')} usageTotalUsd={run.get('usageTotalUsd')}"
        return fix
    rc = apify_cost.evaluate(run, items, settled=True)   # logs the cost cross-check
    fix.new_cost_usd = rc.cost_usd
    fix.usage = rc.usage_snapshot()
    unchanged = (fix.old_cost_usd is not None and abs(fix.delta_usd) <= EPS_USD and not fix.old_estimated
                 and (fix.old_items is not None or items is None))
    fix.status = "ok" if unchanged else "corrected"
    return fix


def apply_call_fixes(conn, fixes: Sequence[CallFix]) -> List[int]:
    """UPDATE the corrected calls and re-total their terminal runs. Returns the re-totalled run ids."""
    touched = sorted({f.run_id for f in fixes if f.status == "corrected"})
    with conn.transaction():
        with conn.cursor() as cur:
            for f in fixes:
                if f.status != "corrected":
                    continue
                cur.execute(
                    "UPDATE public.crawl_run_calls SET cost_usd = %s, estimated = false, usage = %s::jsonb, "
                    "items = COALESCE(items, %s) WHERE call_id = %s",
                    (f.new_cost_usd, json.dumps(f.usage, default=str), f.new_items, f.call_id))
            retotalled = []
            for run_id in touched:
                cur.execute(
                    "UPDATE public.crawl_runs r SET cost_usd = s.total, "
                    "stats = COALESCE(r.stats, '{}'::jsonb) || jsonb_build_object('cost_reconciled_at', now()) "
                    "FROM (SELECT sum(cost_usd) AS total FROM public.crawl_run_calls WHERE run_id = %s) s "
                    "WHERE r.run_id = %s AND r.status = ANY(%s) RETURNING r.run_id",
                    (run_id, run_id, list(TERMINAL_RUN_STATUSES)))
                if cur.fetchone():
                    retotalled.append(run_id)
    return retotalled


def run_totals(conn, run_ids: Sequence[int]) -> List[Dict[str, Any]]:
    if not run_ids:
        return []
    with conn.cursor() as cur:
        cur.execute("SELECT r.run_id, r.status, r.cost_usd AS run_cost_usd, "
                    "(SELECT sum(cost_usd) FROM public.crawl_run_calls c WHERE c.run_id = r.run_id) AS calls_cost_usd "
                    "FROM public.crawl_runs r WHERE r.run_id = ANY(%s) ORDER BY r.run_id", (list(run_ids),))
        return list(cur.fetchall())


def reconcile_ledger(ledger_dsn: str, client: Any, event_ids: Sequence[str], *, apply: bool, min_age_s: float):
    from usage_client import reconcile as ureconcile
    from usage_client.ledger import Ledger

    if not event_ids:
        return []
    ledger = Ledger(ledger_dsn) if apply else ureconcile.read_only_ledger(ledger_dsn)
    try:
        events = ureconcile.load_events(ledger, event_ids=event_ids)
        return ureconcile.reconcile(ledger, client, events, apply=apply, min_age_s=min_age_s)
    finally:
        ledger.close()


def format_calls(fixes: Sequence[CallFix]) -> str:
    lines = [f"{'call':>5} {'run':>4} {'purpose':<9} {'apify run':<18} {'items':>6} {'old':>10} {'settled':>10} "
             f"{'delta':>10}  status"]
    for f in fixes:
        new = "" if f.new_cost_usd is None else f"{f.new_cost_usd:.6f}"
        old = "" if f.old_cost_usd is None else f"{f.old_cost_usd:.6f}"
        items = "" if f.new_items is None else str(f.new_items)
        lines.append(f"{f.call_id:>5} {f.run_id:>4} {f.purpose:<9} {f.apify_run_id:<18} {items:>6} {old:>10} "
                     f"{new:>10} {f.delta_usd:>+10.6f}  {f.status}{' — ' + f.note if f.note else ''}"
                     f"{' (was estimated)' if f.old_estimated else ''}")
    old_total = sum(f.old_cost_usd or 0 for f in fixes)
    new_total = sum((f.new_cost_usd if f.new_cost_usd is not None else (f.old_cost_usd or 0)) for f in fixes)
    lines.append(f"TOTAL calls={len(fixes)} recorded={old_total:.6f} settled={new_total:.6f} "
                 f"delta={new_total - old_total:+.6f}")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None, *, apify_client: Any = None) -> Dict[str, Any]:
    ap = argparse.ArgumentParser(description="Reconcile crawl_run_calls costs and ledger rows with Apify.")
    ap.add_argument("--db-uri", required=True, help="userdb DSN with UPDATE on crawl_run_calls / crawl_runs")
    ap.add_argument("--ledger-dsn", help="usage ledger DSN (default: --db-uri)")
    ap.add_argument("--run-id", type=int, action="append", default=[])
    ap.add_argument("--all", action="store_true", help="every call, not only estimated / cost-less ones")
    ap.add_argument("--min-age-s", type=float, default=600.0)
    ap.add_argument("--apply", action="store_true", help="write (default: dry run, read-only sessions)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s", stream=sys.stderr)

    if apify_client is None:
        from src.helpers.metering import raw_apify_client

        apify_client = raw_apify_client()   # GETs only: runs and datasets
    with connect(args.db_uri, read_only=not args.apply) as conn:
        rows = load_calls(conn, run_ids=args.run_id, all_calls=args.all)
        fixes = [check_call(r, apify_client, min_age_s=args.min_age_s) for r in rows]
        retotalled = apply_call_fixes(conn, fixes) if args.apply else []
        runs = run_totals(conn, sorted({f.run_id for f in fixes}))
    not_terminal = [r["run_id"] for r in runs if r["status"] not in TERMINAL_RUN_STATUSES]
    event_ids = [f.usage_event_id for f in fixes if f.usage_event_id]
    ledger = reconcile_ledger(args.ledger_dsn or args.db_uri, apify_client, event_ids, apply=args.apply,
                              min_age_s=args.min_age_s)

    report = {
        "mode": "apply" if args.apply else "dry-run",
        "calls": [asdict(f) | {"delta_usd": f.delta_usd} for f in fixes],
        "runs": [{k: (float(v) if hasattr(v, "is_finite") else v) for k, v in r.items()} for r in runs],
        "runs_retotalled": retotalled,
        "runs_not_terminal": not_terminal,
        "ledger": [c.as_dict() for c in ledger],
    }
    if ledger:
        from usage_client.reconcile import totals

        report["ledger_totals"] = totals(ledger)
    if args.json:
        print(json.dumps(report, indent=1, default=str))
    else:
        from usage_client.reconcile import format_table

        print(f"mode: {'APPLY' if args.apply else 'DRY RUN (read-only)'}")
        print(format_calls(fixes))
        if not_terminal:
            print(f"runs not terminal (crawl_runs.cost_usd left to their worker; re-run later): {not_terminal}")
        if args.apply:
            print(f"crawl_runs re-totalled: {retotalled}")
        if ledger:
            print("ledger (usage_events):")
            print(format_table(ledger))
    return report


if __name__ == "__main__":
    main()
