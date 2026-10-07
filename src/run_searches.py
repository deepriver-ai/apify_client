"""Main orchestrator: loads tasks from Excel (.xlsx), runs actors, and publishes to RabbitMQ.

    python -m src.run_searches [tasks.xlsx] [--platform]

Metering (rate_limiting.md §2.1, §2.7): every Apify call goes through
``usage_client.apify.run`` (admit → cap → run → one ``usage_events`` row per Apify run),
attributed to the task's org. An xlsx row has no org column: the org is the theme's, from
the decided theme → org map of the userdb importer (``import_tasks_xlsx.KEEP_THEMES`` /
``RETIRED_THEMES``). A theme with no org is **refused** (no org, no spend; best-effort
``usage_blocks(reason='missing_org')``) unless ``--platform`` is given, which runs it as
internal work under the platform org (``usage_client.platform.platform_org_id``, scope
``global`` — what ``platform_context`` builds, one context per usage kind; set
``PLATFORM_ORG_ID`` if the ledger role can't read ``orgs``). Needs ``USAGE_DB_URI`` (or ``DATABASE_URI``) — without a ledger admission
fails closed and nothing is bought.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Optional

from pika.exceptions import AMQPError

from src.actors import get_actor
from src.models.crawl_task import CrawlTask, load_tasks
from src.worker.recording import SERVICE, CallRecorder, RecordingApifyClient, UsageMeter, instrument_actor

logger = logging.getLogger(__name__)

DEFAULT_TASKS_CSV = "tasks.xlsx"
#CURRENT_THEME = "queretaro"
#CURRENT_THEME = "top_news"
#CURRENT_THEME = "felifer"
CURRENT_THEME = "chepe_guerrero"
CURRENT_THEME = "efren_cuevas"
CURRENT_THEME = "mc"
CURRENT_THEME = "zona_fest"
CURRENT_THEME = "sjdr"
CURRENT_THEME = "totalenergies"
CURRENT_THEME = "morelia_descifra"
# Env override (2026-10-05): CURRENT_THEME=<theme> python -m src.run_searches
CURRENT_THEME = os.getenv("CURRENT_THEME", CURRENT_THEME)

# Global override: when True, dismiss the cached filtered-out documents for every
# task (all actors), re-running all filters from scratch regardless of each task's
# own `override_filters` value. Equivalent to forcing override_filters=True everywhere.
OVERRIDE_FILTERED_CACHE = False

DEFAULT_USAGE_SPOOL_DIR = os.path.join("cache", "usage_spool")


def theme_org(theme: Optional[str]) -> Optional[int]:
    """The org a theme's tasks spend for (the importer's decided map), or None."""
    from src.scripts.import_tasks_xlsx import KEEP_THEMES, RETIRED_THEMES

    return KEEP_THEMES.get(theme) or RETIRED_THEMES.get(theme)


def task_meter(task: CrawlTask, *, session: str, platform: bool = False, ledger: Any = None) -> UsageMeter:
    """The ``UsageMeter`` a hand-run task spends through. Raises ``usage_client.MissingOrg``
    (after a best-effort ``usage_blocks`` row) when the task has no org and ``platform``
    is off; ``LedgerUnavailable`` if the platform org can't be resolved."""
    from usage_client import MissingOrg

    event_key = f"run-searches:{session}:{task.task_id}"
    org_id = theme_org(task.theme)
    if org_id is not None:
        return UsageMeter(org_id=org_id, event_key=event_key, ledger=ledger)
    if platform:
        from usage_client.platform import platform_org_id

        return UsageMeter(org_id=platform_org_id(ledger), event_key=event_key, scope="global", ledger=ledger)
    try:
        if ledger is None:
            from usage_client.ledger import get_ledger

            ledger = get_ledger()
        ledger.block(path=f"worker:{SERVICE}", tool="apify", kind="crawl", reason="missing_org")
    except Exception as exc:
        logger.error("could not write usage_blocks(missing_org): %s", exc)
    raise MissingOrg(f"task {task.task_id} (theme {task.theme!r}) has no org: refused, nothing bought "
                     f"(--platform runs it under the platform org)")


def build_actor(task: CrawlTask, *, session: str, platform: bool = False, ledger: Any = None,
                apify_client: Any = None):
    """The task's actor on a client that meters every Apify call through usage_client."""
    meter = task_meter(task, session=session, platform=platform, ledger=ledger)
    if apify_client is None:
        from apify_client import ApifyClient

        from src.actors.actor import _get_apify_token

        apify_client = ApifyClient(_get_apify_token())
    recorder = CallRecorder()
    actor = get_actor(task.actor_class, client=RecordingApifyClient(apify_client, recorder, meter))
    instrument_actor(actor, recorder)  # purpose → usage kind (crawl / comments / profiles)
    return actor


def configure_usage_ledger():
    from usage_client import ledger as usage_ledger

    dsn = os.getenv("USAGE_DB_URI") or os.getenv("DATABASE_URI")
    if not dsn:
        logger.error("USAGE_DB_URI (or DATABASE_URI) is not set: admission fails closed, no task will run")
    return usage_ledger.configure(dsn, spool_dir=os.getenv("USAGE_SPOOL_DIR") or DEFAULT_USAGE_SPOOL_DIR)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the xlsx crawl tasks (metered through usage_client).")
    parser.add_argument("xlsx_path", nargs="?", default=DEFAULT_TASKS_CSV)
    parser.add_argument("--platform", action="store_true",
                        help="run tasks whose theme has no org under the platform org (internal work)")
    args = parser.parse_args()
    xlsx_path = args.xlsx_path

    # Imported here, after argument parsing: src.helpers.rabbitmq connects to the broker
    # at import time (and the helpers above stay importable without one, e.g. by tests).
    from src.helpers.rabbitmq import close_client
    from src.helpers.serialization import publish_document

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    usage_ledger = configure_usage_ledger()
    session = datetime.now().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]

    tasks = load_tasks(xlsx_path)
    logger.info("Loaded %d enabled tasks from %s", len(tasks), xlsx_path)

    if CURRENT_THEME:
        tasks = [t for t in tasks if t.theme == CURRENT_THEME]
        logger.info("Filtered to %d tasks with theme=%s", len(tasks), CURRENT_THEME)

    all_actors = []
    all_documents = []
    for task in tasks:
        logger.info("Running task: %s %s", task.actor_class, task.search_params)
        try:
            actor = build_actor(task, session=session, platform=args.platform, ledger=usage_ledger)
            kwargs = task.to_actor_kwargs()
            if OVERRIDE_FILTERED_CACHE:
                kwargs["override_filters"] = True
            documents = actor.search(task.search_params, **kwargs)
            logger.info("Got %d documents from %s (post-filter)", len(documents), task.actor_class)

            all_actors.append(actor)
            # Expand each document into itself + its attached_news (if any),
            # so linked articles attached to social posts are also published/saved.
            expanded = []
            for doc in documents:
                expanded.append(doc)
                attached = getattr(doc, "attached_news", None)
                if attached is not None:
                    expanded.append(attached)

            if task.publish:
                for doc in expanded:
                    try:
                        publish_document(doc)
                    except AMQPError:
                        raise
                    except Exception as e:

                        logger.error("Error publishing document: %s", e)
                        logger.error("Document data: %s\n\n", doc.data)
                logger.info("Published %d documents to RabbitMQ", len(expanded))
            
                all_documents.extend(expanded)
            
            else:
                runs_dir = os.path.join("cache", "runs")
                os.makedirs(runs_dir, exist_ok=True)
                search_label = task.task_id

                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{task.actor_class}_id_{search_label}_{ts}.json"

                filepath = os.path.join(runs_dir, filename)
                results = [doc.to_final_schema() for doc in expanded]

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(results, f, ensure_ascii=False, indent=2, default=str)

                logger.info("Saved %d documents to %s", len(expanded), filepath)

        except AMQPError:
            logger.error("RabbitMQ connection lost — aborting run")
            raise
        except Exception:
            logger.exception("Task failed: actor=%s search_params=%s", task.actor_class, task.search_params)

    close_client()
    logger.info("All tasks complete")

    # --- Post-run user classification (WS-4, opt-in per task) ----------------
    # Tasks flagged classify_users contribute scope: page tasks their page's
    # source name (read from the docs this run just produced, matched by profile
    # URL slug), keyword tasks their search_params as phrases. official_page
    # tasks feed the deterministic pagina_oficial bypass. One aggregated pass
    # per run, --apply semantics. Failure never affects the crawl results.
    flagged = [t for t in tasks if getattr(t, "classify_users", False)]
    if flagged:
        import argparse as _argparse
        from src.scripts.classify_users import run as classify_run

        PAGE_ACTORS = {"facebook_page_posts", "instagram_profile_posts",
                       "instagram_profile_queenlike"}

        def _slug(url: str) -> str:
            return url.rstrip("/").split("/", 3)[-1].lower() if url else ""

        pages, phrases, official = set(), set(), set()
        for t in flagged:
            if t.actor_class in PAGE_ACTORS:
                slugs = {_slug(p) for p in t.search_params}
                names = {d.data.get("source") for d in all_documents
                         if d.data.get("source") and _slug(d.data.get("profile_url") or "") in slugs}
                pages |= names
                if t.official_page:
                    official |= names
            else:
                phrases |= set(t.search_params)
        if pages or phrases:
            logger.info("classify_users pass: %d pages, %d phrases (%d official)",
                        len(pages), len(phrases), len(official))
            try:
                classify_run(_argparse.Namespace(
                    pages=sorted(pages) or None, phrases=sorted(phrases) or None,
                    networks=None, days=60, org=None, entities=None,
                    min_activity=2, official_pages=sorted(official) or None,
                    model=None, override=False, table_rows=20, apply=True,
                ))
            except Exception:
                logger.exception("classify_users pass failed (crawl results unaffected)")
