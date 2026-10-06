"""Import ``tasks.xlsx`` into the userdb ``crawl_tasks`` table (design:
``docs/db/crawl_tasks.md`` §9.2 step 2).

Plan/apply split, as in ``customer_ops``: the default is a **dry run** that
reads every xlsx row, maps theme → org, validates each row against the
``crawl_tasks`` constraints and the live database (orgs, actor classes,
already-imported ``legacy_task_id`` s, the live-dedupe key) and prints what
``--apply`` would do. ``--apply`` inserts in **one transaction** and is
idempotent: a row whose ``legacy_task_id`` already exists is skipped, so a
second apply inserts 0.

    python -m src.scripts.import_tasks_xlsx --db-uri postgresql://...            # dry run
    python -m src.scripts.import_tasks_xlsx --db-uri postgresql://... --apply
    python -m src.scripts.import_tasks_xlsx --db-uri ... --apply --write-ids \\
        --customers-dir /path/to/business_agent/customers

``--db-uri`` has no default on purpose (never an implicit stg). The dry run
opens a read-only session. ``tasks.xlsx`` is only read.

Decisions encoded here (Oscar, 2026-10-05/06; doc §9.1, §10 Q1/Q2/Q6):

* Kept orgs: TotalEnergies 62 (``totalenergies``, ``totalenergies_guadalupe``),
  Zitácuaro 102, Pinal de Amoles 106. Enabled → ``active``; disabled →
  ``paused`` (reversible, keeps the row in the live-dedupe index), except rows
  in ``ROW_STATUS_OVERRIDES`` that are clearly obsolete → ``retired``.
* Retired themes (disabled 2026-10-05) whose org is on record are imported as
  ``retired`` whatever their ``enabled`` cell says (Q2): keeps history and the
  ``legacy_task_id`` → filter-cache link.
* Themes with no org on record (Q1) and ``orizaba`` (new org pending) are
  skipped and listed.
* Duplicate legacy ids (35/36/37): the first row in file order keeps the id,
  later ones get ``35~2``, ``35~3`` … (Q6).
* ``ids.json`` ``crawl_task_ids`` is only cross-checked (warnings), never the key.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

REPO = Path(__file__).resolve().parents[2]
WORKSPACE = REPO.parent
DEFAULT_XLSX = REPO / "tasks.xlsx"
DEFAULT_CUSTOMERS_DIR = WORKSPACE / "business_agent" / "customers"


def _load_crawl_task_module():
    """Load ``src/models/crawl_task.py`` without running ``src/models/__init__``
    (which imports ``SourcesManagement`` and so opens a Mongo connection at
    import time — the importer needs neither)."""
    path = REPO / "src" / "models" / "crawl_task.py"
    spec = importlib.util.spec_from_file_location("_crawl_task_for_import", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = mod  # dataclasses resolves annotations via sys.modules
    spec.loader.exec_module(mod)
    return mod


_ct = _load_crawl_task_module()
CrawlTask = _ct.CrawlTask

sys.path.insert(0, str(REPO))
from src.helpers.language import normalize_language  # noqa: E402

# ---------------------------------------------------------------------------
# Theme → org decisions
# ---------------------------------------------------------------------------

#: themes with active work: enabled → active, disabled → paused.
KEEP_THEMES: Dict[str, int] = {
    "totalenergies": 62,
    "totalenergies_guadalupe": 62,  # Q1: same customer
    "zitacuaro": 102,
    "pinal_amoles": 106,
}

#: themes disabled 2026-10-05 whose org is on record → imported as retired (Q2).
RETIRED_THEMES: Dict[str, int] = {
    "sjdr": 101,
    "zona_fest": 101,
    "morelia_descifra": 105,
    "queretaro": 93,
}

#: themes not imported, with the reason printed in the report.
SKIP_THEMES: Dict[str, str] = {
    "orizaba": "new Orizaba org pending (today 47 Clicnews / 92 Orizaba) — import later",
    "mc": "no org on record (Q1)",
    "top_news": "no org on record (Q1)",
    "chepe_guerrero": "no org on record (Q1)",
    "felifer": "no org on record (Q1)",
    "efren_cuevas": "no org on record (Q1)",
}

#: per-row status overrides for kept themes: legacy_task_id → (status, reason).
ROW_STATUS_OVERRIDES: Dict[str, Tuple[str, str]] = {
    "145": ("retired", "one-off: V Informe de Gobierno Pinal de Amoles (21-sep-2026), past"),
}

RETIRED_THEME_REASON = "theme retired 2026-10-05 (xlsx import)"
PAUSED_REASON = "disabled in tasks.xlsx (xlsx import)"

MAX_RESULTS_CEILING = 1000  # crawl_tasks_max_results_chk (R1)

# Actor-specific actor_params keys (doc §4). crawl_actor_classes.params_schema is
# still '{}' everywhere (the generator has not run), so this table is the
# fallback; unknown keys are warnings, not errors, until params_schema is filled.
COMMON_PARAM_KEYS = {"check_existing_elasticsearch", "skip_existing_filter", "results_limit",
                     "stats_max_age_days", "snippet_max_len"}
ACTOR_PARAM_KEYS: Dict[str, set] = {
    "facebook_keyword_search": {"enrich_author_after_likes", "get_comments_after_likes",
                                "add_text_from_images", "location_uid", "recent_posts"},
    # page posts do NOT read the *_after_likes thresholds (kwargs.get in facebook/posts.py)
    "facebook_page_posts": {"add_text_from_images", "add_subtitles", "add_ai_transcription"},
    "instagram_hashtags": {"keyword_search", "results_type", "video_dir"},
    "instagram_profile_posts": {"keyword_search", "results_type", "video_dir"},
    "instagram_profile_queenlike": {"scrape_type", "output_mode"},
    "tiktok_posts": {"get_comments_after_likes", "apify_input"},
    "google_news": {"timeframe", "topics", "enrich", "region_language", "max_articles",
                    "decode_urls", "extract_images", "extract_descriptions"},
    "twitter_keyword_search": set(),
    "linkedin_keyword_search": set(),
    "facebook_post_comments": set(),
}
GOOGLE_NEWS_TIMEFRAMES = {"1h", "1d", "7d", "30d", "1y", "all"}
TIKTOK_FORBIDDEN_INPUT_KEYS = {"resultsPerPage", "maxItems"}

LIVE_STATUSES = {"draft", "active", "paused"}

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Planned:
    """One xlsx row and what the importer decided for it."""

    excel_row: int
    legacy_task_id: str
    theme: Optional[str]
    name: str
    actor_class: str
    action: str = "insert"  # insert | skip | exists | conflict | invalid
    reason: str = ""
    values: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def org_id(self) -> Optional[int]:
        return self.values.get("org_id")

    @property
    def status(self) -> Optional[str]:
        return self.values.get("status")

    def status_overridden(self) -> bool:
        return self.theme in KEEP_THEMES and self.legacy_task_id.split("~")[0] in ROW_STATUS_OVERRIDES

    def live_key(self) -> Tuple[int, str, Tuple[str, ...]]:
        return (self.values["org_id"], self.values["actor_class"],
                tuple(self.values["search_params"]))


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def _cell_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return str(v).strip()


def read_xlsx(path: Path) -> List[Tuple[int, Dict[str, str]]]:
    """Every data row (enabled or not) as ``(excel_row_number, row_dict)`` with
    the same string conversion ``load_tasks`` uses. Fully blank rows dropped."""
    import openpyxl  # noqa: PLC0415

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        ws = wb.active
        it = ws.iter_rows(values_only=True)
        header = [_cell_str(h) for h in next(it)]
        out = []
        for n, values in enumerate(it, start=2):
            if all(v is None or str(v).strip() == "" for v in values):
                continue
            out.append((n, {header[i]: _cell_str(v) for i, v in enumerate(values) if i < len(header)}))
        return out
    finally:
        wb.close()


# ---------------------------------------------------------------------------
# Planning (pure — unit-tested)
# ---------------------------------------------------------------------------


def split_duplicate_ids(rows: List[Tuple[int, Dict[str, str]]]) -> Dict[int, str]:
    """excel_row → legacy_task_id. First occurrence keeps the id, later ones get
    ``<id>~2``, ``<id>~3`` … (Q6). Empty ids get ``row<excel_row>``."""
    seen: Counter = Counter()
    out = {}
    for n, row in rows:
        tid = row.get("task_id", "").strip() or f"row{n}"
        seen[tid] += 1
        out[n] = tid if seen[tid] == 1 else f"{tid}~{seen[tid]}"
    return out


def decide_org_status(theme: Optional[str], enabled: bool, legacy_id: str
                      ) -> Tuple[Optional[int], Optional[str], str]:
    """(org_id, status, reason). org_id None → skip (reason says why)."""
    if theme in KEEP_THEMES:
        base_id = legacy_id.split("~")[0]
        if base_id in ROW_STATUS_OVERRIDES:
            status, reason = ROW_STATUS_OVERRIDES[base_id]
            return KEEP_THEMES[theme], status, reason
        if enabled:
            return KEEP_THEMES[theme], "active", ""
        return KEEP_THEMES[theme], "paused", PAUSED_REASON
    if theme in RETIRED_THEMES:
        return RETIRED_THEMES[theme], "retired", RETIRED_THEME_REASON
    if theme in SKIP_THEMES:
        return None, None, f"theme {theme!r}: {SKIP_THEMES[theme]}"
    return None, None, f"theme {theme!r}: not in the theme → org map"


def validate_actor_params(actor_class: str, params: Any, schema: Optional[dict] = None
                          ) -> Tuple[List[str], List[str]]:
    """(errors, warnings) for actor_params (doc §4)."""
    errors: List[str] = []
    warnings: List[str] = []
    if not isinstance(params, dict):
        return [f"actor_params is {type(params).__name__}, not an object"], warnings
    allowed: Optional[set] = None
    if schema:  # params_schema filled by the generator: {"key": type, …} or {"properties": {…}}
        props = schema.get("properties", schema)
        allowed = set(props) | COMMON_PARAM_KEYS
    elif actor_class in ACTOR_PARAM_KEYS:
        allowed = ACTOR_PARAM_KEYS[actor_class] | COMMON_PARAM_KEYS
    if allowed is not None:
        unknown = sorted(k for k in params if k not in allowed and not k.startswith("download_"))
        if unknown:
            warnings.append(f"actor_params keys not read by {actor_class}: {unknown}")
    if actor_class == "google_news" and "timeframe" in params \
            and params["timeframe"] not in GOOGLE_NEWS_TIMEFRAMES:
        errors.append(f"google_news timeframe {params['timeframe']!r} not in {sorted(GOOGLE_NEWS_TIMEFRAMES)}")
    if actor_class == "tiktok_posts" and isinstance(params.get("apify_input"), dict):
        bad = sorted(TIKTOK_FORBIDDEN_INPUT_KEYS & set(params["apify_input"]))
        if bad:
            errors.append(f"tiktok apify_input would lift the cap: {bad}")
    return errors, warnings


def build_values(task: Any, raw: Dict[str, str], org_id: int, status: str, reason: str,
                 legacy_id: str, excel_row: int) -> Tuple[Dict[str, Any], List[str], List[str]]:
    """Column values for one crawl_tasks row + (errors, warnings)."""
    errors: List[str] = []
    warnings: List[str] = []
    metadata: Dict[str, Any] = {"import": {"source": "tasks.xlsx", "excel_row": excel_row,
                                           "enabled_cell": raw.get("enabled", "")}}
    if raw.get("followers"):
        metadata["followers"] = raw["followers"]
    if legacy_id != raw.get("task_id", "").strip():
        metadata["import"]["xlsx_task_id"] = raw.get("task_id", "").strip()
    if task.override_filters:
        warnings.append("override_filters=true is a run option, not stored")

    params = sorted(dict.fromkeys(task.search_params))  # sorted, de-duplicated (R3)
    if not params:
        errors.append("search_params empty")

    max_results = task.max_results
    if max_results <= 0:
        errors.append(f"max_results {max_results} <= 0")
    elif max_results > MAX_RESULTS_CEILING:
        warnings.append(f"max_results {max_results} clamped to {MAX_RESULTS_CEILING}")
        metadata["import"]["max_results_xlsx"] = max_results
        max_results = MAX_RESULTS_CEILING
    if task.max_comments < 0:
        errors.append(f"max_comments {task.max_comments} < 0")

    period_raw = raw.get("period", "").strip().lower()
    if period_raw and period_raw not in ("d", "w", "m"):
        warnings.append(f"period {period_raw!r} invalid, stored NULL")
    if task.min_date is not None and task.period is not None:  # from_csv_row raises first
        errors.append("min_date and period both set")
    min_date = task.min_date
    if min_date is not None and min_date.tzinfo is None:
        min_date = min_date.replace(tzinfo=timezone.utc)

    if task.actor_class == "twitter_keyword_search":
        spaced = [p for p in params if " " in p and not (p.startswith('"') and p.endswith('"'))]
        if spaced:
            warnings.append(f"twitter params with spaces run as exact phrases: {spaced}")

    values = dict(
        org_id=org_id,
        name=(raw.get("name") or "").strip(),
        actor_class=task.actor_class,
        search_params=params,
        country_id=task.country_id,
        language=normalize_language(task.language),
        max_results=max_results,
        min_date=min_date,
        period=task.period,
        publish=task.publish,
        get_comments=task.get_comments,
        max_comments=task.max_comments,
        not_keywords=list(task.not_keywords),
        llm_filter_condition=task.llm_filter_condition,
        update_existing=task.update_existing,
        enrich_followers=task.enrich_followers,
        fetch_attached_url=task.fetch_attached_url,
        classify_users=task.classify_users,
        official_page=task.official_page,
        actor_params=task.actor_params,
        status=status,
        status_reason=reason or None,
        legacy_theme=task.theme,
        legacy_task_id=legacy_id,
        metadata=metadata,
    )
    return values, errors, warnings


def plan_rows(rows: List[Tuple[int, Dict[str, str]]], actor_classes: Dict[str, dict],
              existing_legacy: Iterable[str] = (), existing_live_keys: Optional[Dict] = None,
              known_orgs: Optional[Iterable[int]] = None) -> List[Planned]:
    """Decide every row. ``actor_classes`` is ``{actor_class: params_schema}``
    from ``crawl_actor_classes``; ``existing_legacy`` the legacy ids already in
    the table; ``existing_live_keys`` ``{(org, actor, params_tuple): legacy_id}``
    of live (non-retired) DB rows; ``known_orgs`` the org ids present in ``orgs``."""
    existing_legacy = set(existing_legacy)
    live = dict(existing_live_keys or {})
    orgs = set(known_orgs) if known_orgs is not None else None
    ids = split_duplicate_ids(rows)
    out: List[Planned] = []
    for n, raw in rows:
        legacy_id = ids[n]
        p = Planned(excel_row=n, legacy_task_id=legacy_id, theme=raw.get("theme", "").strip().lower() or None,
                    name=(raw.get("name") or "").strip(), actor_class=raw.get("actor_class", "").strip())
        out.append(p)
        if legacy_id != raw.get("task_id", "").strip():
            p.warnings.append(f"duplicate legacy id {raw.get('task_id')!r} → {legacy_id!r} (Q6)")
        try:
            task = CrawlTask.from_csv_row(raw)
        except (ValueError, KeyError, json.JSONDecodeError) as e:
            org_id, status, reason = decide_org_status(p.theme, True, legacy_id)
            if org_id is None:
                p.action, p.reason = "skip", reason
            else:
                p.action, p.reason = "invalid", f"unparseable row: {e}"
                p.errors.append(p.reason)
            continue
        org_id, status, reason = decide_org_status(task.theme, task.enabled, legacy_id)
        if org_id is None:
            p.action, p.reason = "skip", reason
            continue
        values, errors, warnings = build_values(task, raw, org_id, status, reason, legacy_id, n)
        p.values, p.errors, p.warnings = values, errors, p.warnings + warnings
        if task.actor_class not in actor_classes:
            p.errors.append(f"actor_class {task.actor_class!r} not in crawl_actor_classes")
        else:
            e, w = validate_actor_params(task.actor_class, task.actor_params,
                                         actor_classes.get(task.actor_class) or None)
            p.errors += e
            p.warnings += w
        if orgs is not None and org_id not in orgs:
            p.errors.append(f"org {org_id} missing from orgs")
        if p.errors:
            p.action, p.reason = "invalid", "; ".join(p.errors)
            continue
        if legacy_id in existing_legacy:
            p.action, p.reason = "exists", "legacy_task_id already imported"
            continue
        if status in LIVE_STATUSES:
            key = p.live_key()
            if key in live:
                p.action = "conflict"
                p.reason = (f"live-dedupe key (org {org_id}, {task.actor_class}, {list(key[2])}) "
                            f"already held by legacy {live[key]!r}")
                continue
            live[key] = legacy_id
    return out


# ---------------------------------------------------------------------------
# ids.json cross-check / write
# ---------------------------------------------------------------------------


def _customer_theme_org(cdir: Path) -> Tuple[str, Optional[int]]:
    theme, org = cdir.name, None
    yml = cdir / "customer.yaml"
    if yml.exists():
        try:
            import yaml  # noqa: PLC0415

            spec = yaml.safe_load(yml.read_text(encoding="utf-8")) or {}
            theme = spec.get("theme") or theme
            o = spec.get("org")
            org = o.get("id") if isinstance(o, dict) else o
        except Exception:  # noqa: BLE001 — cross-check only
            pass
    return str(theme).lower(), org


def check_ids_json(customers_dir: Path, rows: List[Tuple[int, Dict[str, str]]]) -> List[str]:
    """Warnings for ``crawl_task_ids`` entries that don't match the xlsx row
    with that id (theme or name), and for customer.yaml org ids that differ
    from the theme → org map."""
    by_id: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for _, r in rows:
        by_id[r.get("task_id", "").strip()].append(r)
    warns = []
    if not customers_dir.is_dir():
        return [f"customers dir {customers_dir} not found — ids.json not checked"]
    for cdir in sorted(d for d in customers_dir.iterdir() if d.is_dir()):
        theme, org = _customer_theme_org(cdir)
        mapped = KEEP_THEMES.get(theme) or RETIRED_THEMES.get(theme)
        if org is not None and mapped is not None and int(org) != mapped:
            warns.append(f"{cdir.name}: customer.yaml org {org} ≠ importer map {mapped} for theme {theme!r}")
        f = cdir / "ids.json"
        if not f.exists():
            continue
        ids = json.loads(f.read_text(encoding="utf-8")).get("crawl_task_ids") or {}
        bad = []
        for tid, name in ids.items():
            matches = by_id.get(str(tid), [])
            if not matches:
                bad.append(f"{tid} (not in xlsx)")
            elif not any(m.get("theme", "").lower() == theme for m in matches):
                bad.append(f"{tid} (xlsx theme {matches[0].get('theme')!r})")
            elif not any((m.get("name") or "").strip() == str(name).strip() for m in matches):
                bad.append(f"{tid} (xlsx name {matches[0].get('name')!r} ≠ {name!r})")
        if bad:
            warns.append(f"{cdir.name}: ids.json crawl_task_ids stale/mismatched ({len(bad)}): "
                         + ", ".join(bad))
    return warns


def write_ids(customers_dir: Path, db_ids: Dict[str, Dict[str, int]], dry_run: bool = False
              ) -> List[Path]:
    """Record ``crawl_task_db_ids`` ({legacy_task_id: task_id}) in each customer's
    ids.json, keyed by the customer's theme (customer_ops ``record_ids`` format).
    ``db_ids`` is ``{legacy_theme: {legacy_task_id: task_id}}``."""
    written = []
    for cdir in sorted(d for d in customers_dir.iterdir() if d.is_dir()):
        theme, _ = _customer_theme_org(cdir)
        payload = db_ids.get(theme)
        if not payload:
            continue
        f = cdir / "ids.json"
        if dry_run:
            print(f"[ids.json dry run] would record crawl_task_db_ids ({len(payload)}) -> {f}")
            continue
        ids = json.loads(f.read_text(encoding="utf-8")) if f.exists() else {}
        ids.setdefault("crawl_task_db_ids", {}).update({k: v for k, v in sorted(payload.items())})
        ids.setdefault("_updated", {})["crawl_task_db_ids"] = datetime.now(timezone.utc).isoformat()
        tmp = f.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(ids, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, f)
        print(f"[ids.json] crawl_task_db_ids ({len(payload)}) -> {f}")
        written.append(f)
    return written


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

INSERT_COLS = ["org_id", "name", "actor_class", "search_params", "country_id", "language",
               "max_results", "min_date", "period", "publish", "get_comments", "max_comments",
               "not_keywords", "llm_filter_condition", "update_existing", "enrich_followers",
               "fetch_attached_url", "classify_users", "official_page", "actor_params", "status",
               "status_reason", "legacy_theme", "legacy_task_id", "metadata"]
JSON_COLS = {"search_params", "not_keywords", "actor_params", "metadata"}


def fetch_db_state(cur) -> Dict[str, Any]:
    cur.execute("SELECT actor_class, params_schema FROM crawl_actor_classes")
    actor_classes = {a: s for a, s in cur.fetchall()}
    cur.execute("SELECT id FROM orgs")
    orgs = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT legacy_task_id FROM crawl_tasks WHERE legacy_task_id IS NOT NULL")
    legacy = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT org_id, actor_class, crawl_params_key(search_params), "
                "coalesce(legacy_task_id, 'db:' || task_id) FROM crawl_tasks WHERE status <> 'retired'")
    live = {(o, a, tuple(p)): lid for o, a, p, lid in cur.fetchall()}
    return dict(actor_classes=actor_classes, orgs=orgs, legacy=legacy, live=live)


def insert_rows(cur, planned: List[Planned]) -> List[Tuple[str, int]]:
    from psycopg2.extras import Json  # noqa: PLC0415

    sql = (f"INSERT INTO crawl_tasks ({', '.join(INSERT_COLS)}, retired_at) VALUES "
           f"({', '.join('%s' for _ in INSERT_COLS)}, "
           f"CASE WHEN %s = 'retired' THEN now() END) RETURNING legacy_task_id, task_id")
    out = []
    for p in planned:
        if p.action != "insert":
            continue
        vals = [Json(p.values[c]) if c in JSON_COLS else p.values[c] for c in INSERT_COLS]
        cur.execute(sql, vals + [p.values["status"]])
        out.append(cur.fetchone())
    return out


def fetch_db_ids(cur) -> Dict[str, Dict[str, int]]:
    cur.execute("SELECT legacy_theme, legacy_task_id, task_id FROM crawl_tasks "
                "WHERE legacy_task_id IS NOT NULL AND legacy_theme IS NOT NULL")
    out: Dict[str, Dict[str, int]] = defaultdict(dict)
    for theme, lid, tid in cur.fetchall():
        out[theme][lid] = tid
    return dict(out)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def report(planned: List[Planned], ids_warnings: List[str], apply: bool) -> None:
    by_action = Counter(p.action for p in planned)
    print(f"\n== tasks.xlsx: {len(planned)} rows | " +
          " ".join(f"{k}={v}" for k, v in sorted(by_action.items())))

    print("\n-- to insert: org × status")
    grid: Dict[int, Counter] = defaultdict(Counter)
    themes: Dict[int, set] = defaultdict(set)
    for p in planned:
        if p.action == "insert":
            grid[p.org_id][p.status] += 1
            themes[p.org_id].add(p.theme)
    statuses = ["active", "paused", "retired"]
    print(f"   {'org':>5}  " + "  ".join(f"{s:>7}" for s in statuses) + "    total  themes")
    for org in sorted(grid):
        c = grid[org]
        print(f"   {org:>5}  " + "  ".join(f"{c[s]:>7}" for s in statuses)
              + f"  {sum(c.values()):>7}  {', '.join(sorted(themes[org]))}")
    tot = Counter()
    for c in grid.values():
        tot.update(c)
    print(f"   {'all':>5}  " + "  ".join(f"{tot[s]:>7}" for s in statuses) + f"  {sum(tot.values()):>7}")

    overridden = [p for p in planned if p.action == "insert" and p.status_overridden()]
    if overridden:
        print("\n-- status overrides")
        for p in overridden:
            print(f"   row {p.excel_row} legacy {p.legacy_task_id} → {p.status}: {p.values['status_reason']}")

    skipped = defaultdict(list)
    for p in planned:
        if p.action == "skip":
            skipped[p.reason].append(p.legacy_task_id)
    if skipped:
        print("\n-- skipped (not imported)")
        for reason, lids in sorted(skipped.items()):
            print(f"   {len(lids):>3}  {reason}: legacy ids {', '.join(lids)}")

    for action, title in (("exists", "already imported (legacy_task_id present)"),
                          ("conflict", "live-dedupe conflicts (skipped)"),
                          ("invalid", "constraint failures (skipped)")):
        rows = [p for p in planned if p.action == action]
        if rows:
            print(f"\n-- {title}: {len(rows)}")
            for p in rows if action != "exists" else rows[:5]:
                print(f"   row {p.excel_row} legacy {p.legacy_task_id} [{p.theme}] {p.actor_class}: {p.reason}")
            if action == "exists" and len(rows) > 5:
                print(f"   … and {len(rows) - 5} more")

    grouped: Dict[str, List[str]] = defaultdict(list)
    for p in planned:
        if p.action == "skip":
            continue
        for w in p.warnings:
            grouped[re.sub(r": \[.*\]$", "", w)].append(p.legacy_task_id)
    if grouped:
        print(f"\n-- row warnings (not blocking)")
        for w, lids in sorted(grouped.items()):
            print(f"   {len(lids):>3}  {w}: legacy ids {', '.join(lids)}")
    if ids_warnings:
        print("\n-- ids.json / customer.yaml cross-check")
        for w in ids_warnings:
            print(f"   {w}")
    n = by_action.get("insert", 0)
    print(f"\n{'APPLY' if apply else 'DRY RUN'}: {n} rows {'to insert' if not apply else 'inserting'}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--db-uri", required=True, help="userdb URI (no default — never implicit stg)")
    ap.add_argument("--xlsx", type=Path, default=DEFAULT_XLSX)
    ap.add_argument("--apply", action="store_true", help="insert (one transaction); default dry run")
    ap.add_argument("--write-ids", action="store_true",
                    help="with --apply: record crawl_task_db_ids in each customer's ids.json")
    ap.add_argument("--customers-dir", type=Path, default=DEFAULT_CUSTOMERS_DIR)
    args = ap.parse_args(argv)

    import psycopg2  # noqa: PLC0415

    rows = read_xlsx(args.xlsx)
    ids_warnings = check_ids_json(args.customers_dir, rows)

    conn = psycopg2.connect(args.db_uri)
    try:
        if not args.apply:
            conn.set_session(readonly=True)
        with conn:  # one transaction; commits on success, rolls back on error
            with conn.cursor() as cur:
                if args.apply:
                    # serialize concurrent writers so the pre-checks stay true until commit
                    cur.execute("LOCK TABLE crawl_tasks IN SHARE ROW EXCLUSIVE MODE")
                st = fetch_db_state(cur)
                planned = plan_rows(rows, st["actor_classes"], st["legacy"], st["live"], st["orgs"])
                report(planned, ids_warnings, args.apply)
                if args.apply:
                    inserted = insert_rows(cur, planned)
                    print(f"inserted {len(inserted)} rows")
                db_ids = fetch_db_ids(cur) if args.write_ids else {}
        # after commit: ids.json only records rows that really exist
        if args.write_ids:
            write_ids(args.customers_dir, db_ids, dry_run=not args.apply)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
