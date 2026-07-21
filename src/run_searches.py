"""Main orchestrator: loads tasks from Excel (.xlsx), runs actors, and publishes to RabbitMQ."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime

from pika.exceptions import AMQPError

from src.actors import get_actor
from src.helpers.rabbitmq import close_client
from src.helpers.serialization import publish_document
from src.models.crawl_task import CrawlTask, load_tasks

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

# Global override: when True, dismiss the cached filtered-out documents for every
# task (all actors), re-running all filters from scratch regardless of each task's
# own `override_filters` value. Equivalent to forcing override_filters=True everywhere.
OVERRIDE_FILTERED_CACHE = False


if __name__ == "__main__":
    xlsx_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TASKS_CSV

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

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
            actor = get_actor(task.actor_class)
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
