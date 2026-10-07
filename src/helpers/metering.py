"""Metered Apify clients for everything outside the crawl-run worker (rate_limiting.md §2.1, §2.3, §2.7).

The worker (``src/run_crawl_worker.py`` + ``src/worker/``) and the hand runs
(``src/run_searches.py``) already send every Apify call through ``usage_client.apify.run``
via ``RecordingApifyClient`` / ``UsageMeter``. This module gives the standalone scripts
(``run_comment_tasks``, ``enrich_page_bios``, ``ips/run_instagram_profile_posts``,
``src/sandbox.py``) the same path, so no code outside ``usage_client`` and this module
builds a raw ``ApifyClient`` (``src/tests/test_usage_adoption.py`` enforces it; the
worker's entry point is the one allow-listed exception, it wraps its client the same way).

**No org, no spend.** ``resolve_org`` picks who pays, in this order:

1. an explicit org id (``--org``, or an ``org_id`` carried by the input file);
2. the theme's org (``--theme``; the userdb importer's decided theme → org map,
   ``import_tasks_xlsx.KEEP_THEMES`` / ``RETIRED_THEMES`` — the same map ``run_searches``
   uses);
3. the platform org, **only** with an explicit ``--platform`` (internal / dev work,
   ``scope='global'``, ``usage_client.platform.platform_org_id``; ``PLATFORM_ORG_ID``
   overrides the lookup).

Nothing found → a best-effort ``usage_blocks(reason='missing_org')`` row and
``usage_client.MissingOrg`` before any Apify call.

``metered_client`` wraps the raw client in a ``RecordingApifyClient`` whose ``UsageMeter``
admits each call against the org's ``usage_limits``, caps Apify ``max_items`` /
``max_total_charge_usd``, runs it and records one ``usage_events`` row per Apify run
(``event_id = apify-client:<event_key>:<org>:<kind>:<apify run id>``, Q32). The usage kind
follows the call's purpose (``search`` → ``crawl``, ``comments``, ``profiles``; video and
other utility calls → ``crawl``): ``default_purpose`` sets it for calls made outside an
instrumented pipeline stage. Caps: ``search_item_budget`` (search calls),
``cost_budget_usd`` (every call) and ``max_items_per_call`` (every call, also non-search).
"""
from __future__ import annotations

import argparse
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Optional, Tuple

from src.worker.recording import SERVICE, CallRecorder, RecordingApifyClient, UsageMeter, instrument_actor

logger = logging.getLogger(__name__)

DEFAULT_USAGE_SPOOL_DIR = os.path.join("cache", "usage_spool")


def new_session() -> str:
    """A per-invocation id for event keys: a second run of the same script records anew."""
    return datetime.now().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]


def theme_org(theme: Optional[str]) -> Optional[int]:
    """The org a theme's work spends for (the importer's decided map), or None."""
    from src.scripts.import_tasks_xlsx import KEEP_THEMES, RETIRED_THEMES

    return KEEP_THEMES.get(theme) or RETIRED_THEMES.get(theme)


def _block_missing_org(ledger: Any, kind: str) -> None:
    try:
        if ledger is None:
            from usage_client.ledger import get_ledger

            ledger = get_ledger()
        ledger.block(path=f"worker:{SERVICE}", tool="apify", kind=kind, reason="missing_org")
    except Exception as exc:  # best effort: the refusal itself must still happen
        logger.error("could not write usage_blocks(missing_org): %s", exc)


def resolve_org(*, org_id: Optional[int] = None, theme: Optional[str] = None, platform: bool = False,
                ledger: Any = None, kind: str = "crawl", what: str = "this run") -> Tuple[int, str]:
    """``(org_id, scope)`` the work spends for; ``MissingOrg`` (after a block row) if none.

    ``LedgerUnavailable`` if ``platform`` needs the platform org and userdb can't be read.
    """
    from usage_client import MissingOrg

    if org_id is not None:
        if isinstance(org_id, bool) or not isinstance(org_id, int) or org_id <= 0:
            _block_missing_org(ledger, kind)
            raise MissingOrg(f"{what}: org_id={org_id!r} is not a real org; nothing bought")
        return org_id, "org"
    if theme:
        found = theme_org(theme)
        if found is not None:
            return found, "org"
    if platform:
        from usage_client.platform import platform_org_id

        return platform_org_id(ledger), "global"
    _block_missing_org(ledger, kind)
    raise MissingOrg(f"{what} has no org (theme {theme!r}): refused, nothing bought "
                     f"(pass --org / --theme with an org, or --platform for internal work)")


def meter(*, event_key: str, org_id: Optional[int] = None, theme: Optional[str] = None,
          platform: bool = False, ledger: Any = None, kind: str = "crawl",
          what: str = "this run") -> UsageMeter:
    """The ``UsageMeter`` a script's Apify calls spend through (see ``resolve_org``)."""
    org, scope = resolve_org(org_id=org_id, theme=theme, platform=platform, ledger=ledger, kind=kind, what=what)
    return UsageMeter(org_id=org, event_key=event_key, scope=scope, ledger=ledger)


def raw_apify_client() -> Any:
    """The one raw ``ApifyClient`` construction outside the worker; callers wrap it at once."""
    from apify_client import ApifyClient

    from src.actors.actor import _get_apify_token

    return ApifyClient(_get_apify_token())


class _CappedActorClient:
    def __init__(self, inner: Any, max_items: int):
        self._inner = inner
        self._max_items = max_items

    def call(self, *, run_input: Any = None, **kwargs: Any):
        given = kwargs.get("max_items")
        kwargs["max_items"] = self._max_items if given is None else min(int(given), self._max_items)
        return self._inner.call(run_input=run_input, **kwargs)

    def __getattr__(self, name: str):
        return getattr(self._inner, name)


class _PerCallItemCap:
    """Adds ``max_items`` to every ``actor(id).call`` (the recorder caps items only for search)."""

    def __init__(self, client: RecordingApifyClient, max_items: int):
        self._client = client
        self._max_items = max_items

    def actor(self, actor_id: str) -> _CappedActorClient:
        return _CappedActorClient(self._client.actor(actor_id), self._max_items)

    def __getattr__(self, name: str):
        return getattr(self._client, name)


def metered_client(usage_meter: UsageMeter, *, apify_client: Any = None, default_purpose: str = "search",
                   search_item_budget: Optional[int] = None, cost_budget_usd: Optional[float] = None,
                   max_items_per_call: Optional[int] = None) -> Tuple[Any, CallRecorder]:
    """``(client, recorder)``: a drop-in Apify client whose every call goes through usage_client."""
    if max_items_per_call is not None and int(max_items_per_call) <= 0:
        raise ValueError("max_items_per_call must be > 0")
    recorder = CallRecorder(default_purpose=default_purpose, search_item_budget=search_item_budget,
                            cost_budget_usd=cost_budget_usd)
    client: Any = RecordingApifyClient(apify_client if apify_client is not None else raw_apify_client(),
                                       recorder, usage_meter)
    if max_items_per_call is not None:
        client = _PerCallItemCap(client, int(max_items_per_call))
    return client, recorder


def metered_actor(actor_class: str, usage_meter: UsageMeter, **client_kw: Any):
    """A registry actor on a metered client, its pipeline stages tagged (comments / profiles kinds)."""
    from src.actors import get_actor

    client, recorder = metered_client(usage_meter, **client_kw)
    actor = get_actor(actor_class, client=client)
    instrument_actor(actor, recorder)
    return actor, recorder


def configure_usage_ledger():
    """Configure usage_client's ledger from ``USAGE_DB_URI`` (else ``DATABASE_URI``)."""
    from usage_client import ledger as usage_ledger

    dsn = os.getenv("USAGE_DB_URI") or os.getenv("DATABASE_URI")
    if not dsn:
        logger.error("USAGE_DB_URI (or DATABASE_URI) is not set: admission fails closed, nothing will be bought")
    return usage_ledger.configure(dsn, spool_dir=os.getenv("USAGE_SPOOL_DIR") or DEFAULT_USAGE_SPOOL_DIR)


def add_usage_args(ap: argparse.ArgumentParser, *, cost_help: str = "") -> None:
    """``--org`` / ``--theme`` / ``--platform`` / ``--max-cost-usd`` for a script that buys Apify items."""
    g = ap.add_argument_group("usage metering (who pays; no org, no spend)")
    g.add_argument("--org", type=int, default=None, help="org id the Apify spend is attributed to")
    g.add_argument("--theme", default=None, help="tasks.xlsx theme: spend for the theme's org (importer map)")
    g.add_argument("--platform", action="store_true",
                   help="internal/dev work: spend under the platform org (scope global)")
    g.add_argument("--max-cost-usd", type=float, default=None,
                   help="cost cap for the whole invocation (Apify max_total_charge_usd)" + cost_help)
