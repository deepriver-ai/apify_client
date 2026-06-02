# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project Snapshot

This is an Apify-based media ingestion and normalization pipeline. It scrapes news and social media content, maps raw actor output into a common intermediate `Document.data` shape, filters/enriches it in staged actor pipelines, normalizes it through `src/schema/`, and publishes final envelopes to RabbitMQ.

Primary entry point: `src/run_searches.py`.

Current task file: `tasks.xlsx`.

Current runtime theme filter: `CURRENT_THEME = "queretaro"` in `src/run_searches.py`.

## Setup And Commands

Use Python 3. Dependencies are listed in `requirements.txt`.

```bash
pip install -r requirements.txt
source setup_local.sh
python -m pytest src/tests/ -v
python -m src.run_searches
python -m src.run_searches custom.xlsx
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
- `src/models/crawl_task.py` parses task rows from `tasks.xlsx`.
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
