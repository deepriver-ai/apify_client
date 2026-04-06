"""Main orchestrator: loads tasks from Excel (.xlsx), runs actors, and publishes to RabbitMQ."""

from __future__ import annotations

import json
import logging
import sys

from src.actors import get_actor
from src.helpers.rabbitmq import close_client, publish
from src.models.crawl_task import CrawlTask, load_tasks

logger = logging.getLogger(__name__)

DEFAULT_TASKS_CSV = "tasks.xlsx"



if __name__ == "__main__":
    xlsx_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TASKS_CSV
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    tasks = load_tasks(xlsx_path)
    logger.info("Loaded %d enabled tasks from %s", len(tasks), xlsx_path)

    for task in tasks:

        try:
            actor = get_actor(task.actor_class)
            kwargs = task.to_actor_kwargs()
            documents = actor.search(task.search_params, **kwargs)
            logger.info("Got %d documents from %s (post-filter)", len(documents), task.actor_class)

            if task.publish:
                for doc in documents:
                    final = doc.to_final_schema()
                    publish(json.dumps(final))
                logger.info("Published %d documents to RabbitMQ", len(documents))
            else:
                logger.info("Publish disabled for this task, skipping RabbitMQ")

        except Exception:
            logger.exception("Task failed: actor=%s search_params=%s", task.actor_class, task.search_params)

    close_client()
    logger.info("All tasks complete")

