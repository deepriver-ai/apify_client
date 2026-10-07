# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project Snapshot

This is an Apify-based media ingestion and normalization pipeline. It scrapes news and social media content, maps raw actor output into a common intermediate `Document.data` shape, filters/enriches it in staged actor pipelines, normalizes it through `src/schema/`, and publishes final envelopes to RabbitMQ.

Primary entry point: `src/run_searches.py` (xlsx hand runs).

Queue entry point: `src/run_crawl_worker.py` — consumes `crawl_run` messages, executes userdb `crawl_runs` (design `docs/db/crawl_tasks.md` §6.1; see CLAUDE.md §Entry Point).

Current task file: `tasks.xlsx`.

Current runtime theme filter: `CURRENT_THEME = "morelia_descifra"` in `src/run_searches.py` (the last assignment wins; the `CURRENT_THEME` env var overrides it).

Rate limiting: this repo **records** Apify spend and admits each call at run time through `usage_client.apify.run`; the backend admits crawl runs at request time. Spec `business_agent/docs/plans/rate_limiting.md` (media workspace, §2.7); summary of role, contract, refusals, event ids, env, packaging, tests and known gaps in CLAUDE.md §Usage metering.

## Setup And Commands

Use Python 3. Dependencies are listed in `requirements.txt`.

```bash
pip install -r requirements.txt
pip install -e ../usage_client            # required: every Apify call is metered through it (images: Q26, see CLAUDE.md §Usage metering — the Dockerfile does not install it yet)
source setup_local.sh
python -m pytest src/tests/ -v
python -m src.run_searches
python -m src.run_searches custom.xlsx
python -m src.run_searches --platform     # themes with no org run under the platform org
python -m src.run_crawl_worker            # queue worker (see env below); --once for a single message
```

`setup_local.sh` adds this repo and `/Users/oscarcuellar/ocn/media/elastic_client` to `PYTHONPATH`. The Elasticsearch existing-document filter is best-effort: if the package or connection is unavailable, the pipeline logs a warning and keeps documents.

Do not run live Apify/RabbitMQ/OpenRouter/Mongo flows casually. Prefer unit tests with mocked data unless the task explicitly requires an integration run.

## Environment

Configuration is environment-driven, commonly via `.env`.

Important variables:

- `APIFI_API_TOKEN` for Apify client access. The code currently uses this spelling.
- `RABBIT_HOST`, `RABBIT_PORT`, `RABBIT_USER`, `RABBIT_PASSWORD`, `RABBIT_EXCHANGE`, `RABBIT_QUEUE`, `RABBIT_VIRTUAL_HOST` for RabbitMQ.
- `MONGO_USER`, `MONGO_PASSWORD`, `MONGO_HOST`, `MONGO_PORT`, `MONGO_AUTHDB` for MongoDB.
- `OPENROUTER_API_KEY`, `OPENROUTER_MODEL`, `LLM_CACHE_PATH` for LLM extraction/filtering.
- `USAGE_DB_URI` — the `usage_client` ledger (userdb: `usage_limits` / `usage_events` / `usage_blocks`); defaults to `DATABASE_URI`. **Every Apify call** (worker, `run_searches.py` and the scripts: `run_comment_tasks`, `enrich_page_bios`, `ips/run_instagram_profile_posts`, `src/sandbox.py`) goes through `usage_client.apify.run`: admitted against the org's limits (Apify $15/day per org by default), capped, recorded as one `usage_events` row per Apify run. No ledger → admission fails closed → nothing is bought. `USAGE_SPOOL_DIR` (`cache/usage_spool`) holds `usage_events` writes that failed, replayed later. `PLATFORM_ORG_ID` (optional) overrides the platform org lookup (`public_id='deepriver-platform'`) for `run_searches.py --platform`. `USAGE_SPOOL_DIR` must be a persistent volume in k8s. Event ids: `apify-client:<event_key>:<org>:<kind>:<Apify run id>` (Q32).

Hand runs (`run_searches.py`): each task spends for its theme's org (the importer's theme → org map, `src/scripts/import_tasks_xlsx.py` `KEEP_THEMES` / `RETIRED_THEMES`). A theme with no org is refused (no org, no spend; `usage_blocks(missing_org)`) unless `--platform`. Scripts that buy Apify items take `--org` / `--theme` / `--platform` / `--max-cost-usd` (`src/helpers/metering.py`; refused without an org) — table of paths, orgs and kinds in CLAUDE.md §Usage metering. A bare actor (no client) can't spend. apify_client's own OpenRouter calls (`src/oai/`) are **not metered yet** (no `usage_tools` kind; pending, see CLAUDE.md); `src/tests/test_usage_adoption.py` fails on any new raw `ApifyClient` or direct OpenRouter use.

Crawl-run queue worker (`src/run_crawl_worker.py`) — in addition to `APIFI_API_TOKEN` and the `RABBIT_*` document connection (where documents are published; check `RABBIT_VIRTUAL_HOST`):

- `DATABASE_URI` — userdb as role `crawler_worker` (least privilege; `media-backend-paid/docs/db/migrations/2026-10-06_crawl_worker_role_userdb.sql`). Local runs: a throwaway/local DB, never stg.
- `CRAWL_RUNS_RABBIT_VIRTUAL_HOST`, `CRAWL_RUNS_QUEUE` (required) — the runs queue it consumes. Local: `document_processing_dev` + `dev_crawl_runs`. `CRAWL_RUNS_RABBIT_HOST/PORT/USER/PASSWORD` default to the `RABBIT_*` values; `CRAWL_RUNS_RABBIT_HEARTBEAT` (60).
- `CRAWL_WORKER_HEARTBEAT_SECS` (60), `CRAWL_WORKER_DB_RETRY_SECS` (30, delay before requeue when userdb is unreachable), `CRAWL_WORKER_SPOOL_PATH` (`cache/crawl_worker_spool.jsonl`), `CRAWL_WORKER_OUTPUT_DIR` (`cache/runs`, dry runs).
- `USAGE_DB_URI` / `USAGE_SPOOL_DIR` as above (the worker refuses to start with neither `USAGE_DB_URI` nor `DATABASE_URI`). The `crawler` role holds the ledger grants (`2026-10-07_crawler_usage_grants_userdb.sql`).
- Admission refusals (`AdmissionRefused`, `src/worker/runner.py` docstring): `over_limit` / `per_request_cap` before any Apify call → run `blocked` (schedule untouched, so the next tick retries); `ledger_unreachable` before any call → run released to `queued` and the message nacked with requeue after `CRAWL_WORKER_DB_RETRY_SECS`; any refusal after a paid call → `partial`. A message without a valid `org_id` → `usage_blocks(path='worker:apify-client', reason='missing_org')` + run `failed` + ack.

Never point a local worker at the repo `.env` as-is: it sets `RABBIT_VIRTUAL_HOST=document_processing_prod`.

Tests mock MongoDB before importing `src` modules in `src/tests/conftest.py`, so a running MongoDB is not required for pytest.

## Repository Map

- `src/actors/actor.py` contains `ApifyActor`, the base class and staged filtering/enrichment pipeline.
- `src/actors/__init__.py` contains `ACTOR_REGISTRY` and `get_actor()`.
- `src/actors/news/`, `instagram/`, `facebook/`, `twitter/`, `linkedin/`, `tiktok/` contain actor wrappers and platform-specific enrichment.
- `src/models/document.py` defines the intermediate schema and final envelope normalization.
- `src/models/news.py` and `src/models/post.py` are the news and social base models.
- `src/models/*_post.py` map raw platform records into the intermediate schema.
- `src/models/sources_management.py` loads source metadata from MongoDB and tracks unknown news domains in `cache/unknown_sources.json`.
- `src/models/users_management.py` caches social author stats/location in `cache/users.json`.
- `src/models/crawl_task.py` parses task rows from `tasks.xlsx` (`from_csv_row`) and from userdb `crawl_tasks` (`from_db_row`).
- `src/worker/` is the crawl-run queue worker: `runner.py` (message → claimed run → actor → funnel/cost → terminal status), `recording.py` (recording Apify client: every call through `usage_client.apify.run` via `UsageMeter`, one `crawl_run_calls` row per call with its `usage_event_id`, item/cost budgets), `db.py` (userdb SQL as `crawler_worker`, JSONL spool for cost writes).
- `src/models/news_parser/` fetches and extracts article HTML content.
- `src/schema/` contains declarative normalization schemas and parsers.
- `src/helpers/` contains integration helpers for language, geocoding, MongoDB, RabbitMQ, HTML cleaning, and strings.
- `src/tests/` contains pytest coverage and committed Apify fixture JSON under `src/tests/cache/`.

## Actor Registry

Registered task actor keys:

- `google_news`
- `instagram_hashtags`
- `instagram_profile_posts`
- `instagram_profile_queenlike`
- `facebook_page_posts`
- `facebook_keyword_search`
- `twitter_keyword_search`
- `linkedin_keyword_search`
- `tiktok_posts`

Utility actors such as Facebook comments/profiles are not task registry entries unless added intentionally.

When adding an actor, update:

- `src/actors/__init__.py`
- `README.md`
- `CLAUDE.md`
- tests under `src/tests/`
- task documentation or examples if new task fields or `actor_params` are introduced

## Data Flow

The normal flow is:

```text
raw Apify results
  -> platform model factory, pure mapping into Document.data
  -> ApifyActor.process_documents()
  -> Document.to_final_schema()
  -> RabbitMQ publish or cache/runs JSON output
```

`process_documents()` in the base actor currently runs:

1. filter cached-out documents
2. `_filter_keywords`
3. `_filter_date`
4. `_filter_existing_in_elasticsearch`
5. `_enrich_content`
6. `_filter_language`
7. `_enrich_user_author`
8. `_enrich_location`
9. `_filter_location`
10. `_filter_llm`
11. `_filter_llm` again with a longer snippet
12. `_enrich_comments`

Some actors override the order to reduce cost. For example, Facebook keyword search applies LLM filtering before expensive author/location/comment enrichment, and Twitter skips author enrichment because tweet payloads already include profile metadata.

Keep cheap filters before expensive network or LLM work where possible.

Like thresholds gate the paid per-post enrichments: `get_comments_after_likes` (comments)
and `enrich_author_after_likes` (author-profile scrape) — only posts whose `likes` exceed
the threshold (strictly greater) get the extra Apify call; unset means every post. One
helper implements it, `ApifyActor._gate_by_likes`, used by `facebook_page_posts`,
`facebook_keyword_search` (both through the shared `enrich_page_authors` /
`enrich_post_comments` in `facebook/posts.py`) and `tiktok_posts` (comments). Skipped posts
are counted in the run funnel as `comments_skipped_likes` / `profiles_skipped_likes`; with
no qualifying post no call is made (so nothing is recorded in `crawl_run_calls`). This
applies to both entry points (`run_searches.py` and the crawl-run worker).

## Schema Conventions

All models first populate the intermediate dict from `Document._empty_data()`. Keep new platform mappings as pure data mapping; enrichment belongs in actor pipeline stages or model enrichment methods.

Final publishing always returns:

```python
{"type": "news", "message": parsed}
```

The outer envelope type is always `"news"`. The inner `message.type` carries the platform, such as `news`, `facebook`, `instagram`, `x`, `linkedin`, or `tiktok`.

Author location is stored in flat intermediate keys such as `location_author_geoid`, then `Document.to_final_schema()` nests it under `location_author`.

If you add or rename intermediate fields, update:

- `Document._empty_data()`
- `src/schema/schemas/news.py`
- model factories that should populate the field
- tests in `src/tests/`
- `README.md` and `CLAUDE.md`

## Task System

`CrawlTask.from_csv_row()` reads rows from the active sheet in an `.xlsx` file. Important task controls:

- `task_id` is used in filter-cache keys. If omitted, it is generated from actor class and search params.
- `search_params` are comma-separated.
- `not_keywords` are pipe-separated.
- `min_date` and `period` are mutually exclusive.
- `period` supports `d`, `w`, and `m`.
- `actor_params` is JSON and overrides/extends actor kwargs.
- `enabled=false` rows are skipped.
- `publish=false` writes output JSON under `cache/runs/`.
- `update_existing=true` lets existing Elasticsearch URLs continue through enrichment/publish.
- `override_filters=true` ignores prior filter-cache decisions.

Do not modify `tasks.xlsx` unless the user explicitly asks. It is a live input file and may have local edits.

## Caches And Generated Files

Runtime caches are under `cache/`:

- `cache/filter_cache.json`
- `cache/llm_core/`
- `cache/users.json`
- `cache/unknown_sources.json`
- `cache/runs/`
- `cache/media/instagram/`

Treat these as runtime artifacts unless a test intentionally depends on a committed fixture. Test fixtures in `src/tests/cache/` are committed and should be updated deliberately when actor mappings change.

## Testing Guidance

Run focused tests for the area you changed, then broaden when behavior crosses shared model/schema/pipeline boundaries.

Useful commands:

```bash
python -m pytest src/tests/test_crawl_task.py -v
python -m pytest src/tests/test_schema.py -v
python -m pytest src/tests/test_google_news_actor.py -v
python -m pytest src/tests/test_instagram_actor.py -v
python -m pytest src/tests/test_facebook_actor.py -v
python -m pytest src/tests/test_tiktok_actor.py -v
python -m pytest src/tests/ -v
```

Tests mock geocoding and MongoDB. Prefer adding fixture-backed tests instead of making live network calls.

## Coding Rules

- Use `from __future__ import annotations` in Python modules, matching the existing style.
- Keep type hints on public helpers, model factories, and actor methods.
- Use standard `logging.getLogger(__name__)`; avoid print statements in library code.
- Preserve the staged pipeline shape. Override individual stages for actor-specific behavior instead of duplicating the whole pipeline unless order must change.
- Keep platform model factories deterministic and side-effect free.
- Use `SourcesManagement` for news-domain source metadata; news articles should not geocode.
- Use `UsersManagement` for reusable social author stats/location caching.
- Normalize language through `src.helpers.language.normalize_language()`.
- Use geoid prefix semantics for location filters, e.g. `_48416053` matches `_484`.
- Keep `Document.to_final_schema()` as the final schema boundary.
- Update docs after feature changes, especially actor params, task fields, schemas, and pipeline order.

## Safety Notes

This repo can call paid/external services: Apify, OpenRouter, MongoDB, Elasticsearch, and RabbitMQ. Be explicit before running commands that publish messages, scrape live data, or make LLM calls.

Avoid destructive cleanup of `cache/`, `tasks.xlsx`, or fixture files unless specifically requested. The working tree may contain user edits.
