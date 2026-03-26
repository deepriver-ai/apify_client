"""Main orchestrator: loads tasks from CSV, runs actors, and publishes to RabbitMQ."""

from __future__ import annotations

import json
import logging
import sys

from src.actors import get_actor
from src.helpers.rabbitmq import close_client, publish
from src.models.crawl_task import CrawlTask, load_tasks

logger = logging.getLogger(__name__)

DEFAULT_TASKS_CSV = "tasks.csv"


def run_task(task: CrawlTask) -> None:
    """Execute a single crawl task: run actor and optionally publish."""
    logger.info("Running task: actor=%s keywords=%s method=%s", task.actor_class, task.keywords, task.method)

    actor = get_actor(task.actor_class)
    kwargs = task.to_actor_kwargs()

    method_fn = getattr(actor, task.method, None)
    if method_fn is None:
        logger.error("Actor %s has no method '%s'", task.actor_class, task.method)
        return

    documents = method_fn(task.keywords, **kwargs)
    logger.info("Got %d documents from %s (post-filter)", len(documents), task.actor_class)

    if task.publish:
        for doc in documents:
            final = doc.to_final_schema()
            publish(json.dumps(final))
        logger.info("Published %d documents to RabbitMQ", len(documents))
    else:
        logger.info("Publish disabled for this task, skipping RabbitMQ")


def main(csv_path: str = DEFAULT_TASKS_CSV) -> None:
    """Load tasks from CSV and execute each one."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    tasks = load_tasks(csv_path)
    logger.info("Loaded %d enabled tasks from %s", len(tasks), csv_path)

    for task in tasks:
        try:
            run_task(task)
        except Exception:
            logger.exception("Task failed: actor=%s keywords=%s", task.actor_class, task.keywords)

    close_client()
    logger.info("All tasks complete")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TASKS_CSV
    main(path)
