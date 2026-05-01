"""Main orchestrator: loads tasks from Excel (.xlsx), runs actors, and publishes to RabbitMQ."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime

from pika.exceptions import AMQPError

from src.actors import get_actor
from src.helpers.rabbitmq import close_client, publish
from src.models.crawl_task import CrawlTask, load_tasks

logger = logging.getLogger(__name__)

DEFAULT_TASKS_CSV = "tasks.xlsx"
CURRENT_THEME = "queretaro"



if __name__ == "__main__":
    xlsx_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TASKS_CSV

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    tasks = load_tasks(xlsx_path)
    logger.info("Loaded %d enabled tasks from %s", len(tasks), xlsx_path)

    if CURRENT_THEME:
        tasks = [t for t in tasks if t.theme == CURRENT_THEME]
        logger.info("Filtered to %d tasks with theme=%s", len(tasks), CURRENT_THEME)

    for task in tasks:
        logger.info("Running task: %s %s", task.actor_class, task.search_params)
        try:
            actor = get_actor(task.actor_class)
            kwargs = task.to_actor_kwargs()
            documents = actor.search(task.search_params, **kwargs)
            logger.info("Got %d documents from %s (post-filter)", len(documents), task.actor_class)

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
                        final = doc.to_final_schema()
                        publish(json.dumps(final, default=lambda o: o.isoformat() if hasattr(o, "isoformat") else str(o)))
                    except AMQPError:
                        raise
                    except Exception as e:

                        logger.error("Error publishing document: %s", e)
                        logger.error("Document data: %s\n\n", doc.data)
                logger.info("Published %d documents to RabbitMQ", len(expanded))

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

