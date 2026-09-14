"""Run the comment tasks a producer file describes — the standalone comments
actor entry (Descifra plan 2.10 / track B).

Reads a task file written by ``select_posts_for_comments.py`` and runs one
``facebook_post_comments`` actor call per tier (the actor's ``resultsLimit`` is
global per call, so tiers cannot mix). Dry run by default: prints the runs and
their cost and does nothing else. ``--apply`` calls Apify and publishes the
comment-update documents to RabbitMQ (``publish_document``; gp3 merges the
comments into the existing ES document by exact URL) — or, with
``--no-publish``, saves them under ``cache/runs/`` like ``run_searches.py``.

    python -m src.scripts.run_comment_tasks cache/comment_tasks/morelia.json
    python -m src.scripts.run_comment_tasks cache/comment_tasks/morelia.json --apply
    python -m src.scripts.run_comment_tasks ... --apply --tiers deep --no-publish

Environment: APIFY_TOKEN (actor), RABBIT_* (publish; check the vhost — a local
run must point at ``document_processing_dev``).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def load_task_file(path: str) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        task = json.load(f)
    if "tiers" not in task or "depth" not in task:
        raise SystemExit(f"{path}: not a comment task file (missing tiers/depth)")
    return task


def plan_runs(task: Dict[str, Any], tiers: List[str]) -> List[Dict[str, Any]]:
    runs = []
    for tier in tiers:
        rows = task["tiers"].get(tier) or []
        if not rows:
            continue
        runs.append({"tier": tier, "depth": int(task["depth"][tier]),
                     "urls": [r["post_url"] for r in rows],
                     "comments_expected": sum(min(int(r.get("n_comments") or 0),
                                                  int(task["depth"][tier])) for r in rows)})
    return runs


def execute(run: Dict[str, Any], task_id: str, publish: bool) -> int:
    from src.actors import get_actor
    from src.helpers.serialization import publish_document

    actor = get_actor("facebook_post_comments")
    docs = actor.search(run["urls"], max_comments=run["depth"], task_id=task_id)
    total = sum(len(d.data.get("comments") or []) for d in docs)
    if publish:
        for doc in docs:
            publish_document(doc)
        logger.info("published %d comment-update docs (%d comments) tier=%s",
                    len(docs), total, run["tier"])
    else:
        runs_dir = os.path.join("cache", "runs")
        os.makedirs(runs_dir, exist_ok=True)
        path = os.path.join(runs_dir, f"facebook_post_comments_{task_id}_{run['tier']}_"
                                      f"{datetime.now():%Y%m%d_%H%M%S}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump([d.to_final_schema() for d in docs], f, ensure_ascii=False, indent=2, default=str)
        logger.info("saved %d docs (%d comments) to %s", len(docs), total, path)
    return total


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("task_file")
    ap.add_argument("--tiers", nargs="*", default=["deep", "sample"])
    ap.add_argument("--apply", action="store_true", help="actually call Apify")
    ap.add_argument("--no-publish", action="store_true",
                    help="with --apply: save to cache/runs instead of publishing to RabbitMQ")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from dotenv import load_dotenv
    load_dotenv()

    task = load_task_file(args.task_file)
    runs = plan_runs(task, args.tiers)
    usd_per_post = float((task.get("cost_estimate") or {}).get("usd_per_post") or 0.02)
    print(f"task file {args.task_file} (generated {task.get('generated_at')})")
    for r in runs:
        print(f"  [{r['tier']}] {len(r['urls'])} posts × depth {r['depth']} "
              f"≈ {r['comments_expected']} comments ≈ ${len(r['urls']) * usd_per_post:.2f}")
    if not runs:
        print("nothing to run"); return
    if not args.apply:
        print("dry-run — re-run with --apply to call Apify"
              + ("" if args.no_publish else f" and publish to vhost "
                 f"{os.getenv('RABBIT_VIRTUALHOST') or os.getenv('RABBIT_VIRTUAL_HOST') or '?'}"))
        return

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    total = 0
    for r in runs:
        total += execute(r, task_id=f"comments_{stamp}_{r['tier']}", publish=not args.no_publish)
    if not args.no_publish:
        from src.helpers.rabbitmq import close_client
        close_client()
    print(f"done: {total} comments across {len(runs)} run(s)")


if __name__ == "__main__":
    main()
