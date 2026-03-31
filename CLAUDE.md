# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apify Media Data Ingestion & Normalization Pipeline. Scrapes media/social data via Apify actors, normalizes it through a declarative schema system, and sends it to downstream services (RabbitMQ).

## Architecture

### Schema System (`src/schema/`)
Declarative, multi-stage data normalization framework:
- **`parse_object.py`** — Core `Parser` class that orchestrates: structure parsing (flatten/nest) → type conversion → default application → validation
- **`types.py`** — Type-specific parsers (`IntParser`, `FloatParser`, `StrParser`, `BoolParser`, `DateTimeParser`, `UrlParser`, `EnumStrParser`, `ListParser`) with parse/validate methods
- **`schemas/news.py`** — NEWS_SCHEMA for articles/posts (news, X, Facebook, Instagram, Radio, TV, impreso). Includes `location_ids` field (type list) for geocoded geoids
- **`schemas/source.py`** — SOURCE_SCHEMA for news sources/websites with crawl configuration

Key patterns: schemas support nested objects, context-aware default callbacks, conditional validation (e.g., URL required only when type != "impreso"), and timezone-aware datetime handling (Mexico City timezone).

### Models (`src/models/`)
Document hierarchy and source management:
- **`document.py`** — Base `Document` class. Holds intermediate schema data as `self.data` dict with `language` and `comments` fields. Provides `_empty_data()` (all intermediate fields as None/[], including `comments: []`), `detect_language()` (detects via `langdetect` and caches in `data["language"]`), `matches_language(code)`, `matches_min_date(dt)`, `matches_location(country_id)` (checks `location_ids` first — any geoid prefix match keeps doc; falls back to `author_location_id` when `location_ids` is empty; docs with no location data are kept), and `add_locations()` (writes geocoding results to `location_ids`; only writes author location fields if currently None, preserving SourcesManagement data) for pipeline filtering. Abstract `to_final_schema()`.
- **`news.py`** — `News(Document)`. Represents a news article. `from_google_news(item)` class method creates a News from raw Apify dict (pure data mapping, no enrichment). `fetch_and_parse()` downloads HTML via `fetch_html()`, extracts content via `extract_article()`, updates `self.data`. `enrich_location(sources)` sets ALL author location fields (`location_author_formatted_name`, `location_author_geoid`, `location_author_coords`, `location_author_precision_level`, `location_author_level_1`/`level_2`/`level_3` and their IDs, plus `author_location_text`/`author_location_id`) from `SourcesManagement` domain lookup and tracks unknown sources. News articles do NOT geocode — location comes from SourcesManagement only. `to_final_schema()` normalizes via `normalize_record(data, "MessageWrapper")`.
- **`post.py`** — `Post(Document)`. Represents a social media post. `from_instagram(item)` class method creates a Post from a raw Instagram Apify result, mapping caption, media URLs (via `_collect_media_urls`), engagement metrics, and profile URL. `to_final_schema()` normalizes via `normalize_record(data, "MessageWrapper")`.
- **`sources_management.py`** — `SourcesManagement` class. Single interface for all source-related operations. Reads sources from MongoDB (via `src/helpers/mongoconnection.py`) at module load and builds `_known_sources` set and `_domain_country_id` dict. Methods: `get_domain(url)` extracts domain, `is_known(domain)` checks known sources, `get_country_id(domain)` returns country_id (first 4 chars of `geoid`), `get_location(domain)` and `_build_domain_location()` return a full location dict with all author location fields (`location_author_formatted_name`, `location_author_geoid`, `location_author_coords`, `location_author_precision_level`, `location_author_level_1`/`level_2`/`level_3` and their IDs), `filter_by_country(items, country_id)` filters by country, `check_source(url, source_name)` tracks unknowns, `save()` persists unknowns to `cache/unknown_sources.json`.
- **`news_parser/`** — Article enrichment pipeline (moved from `src/actors/news/news_parser/`):
  - **`load_url.py`** — `fetch_html(url)`: streaming HTTP download with size guard (10MB), retry, and browser UA headers. Returns decoded HTML or None.
  - **`parser.py`** — `extract_article(html, url)`: three-tier content extraction. First tries NewsPlease, fills gaps with newspaper4k, falls back to LLM (via `src/oai/llm_core.py`) when both parsers fail. Returns `{title, body, author, media_urls}` or None. Requires body >= 200 chars for meaningful content.

### Actors System (`src/actors/`)
Apify actor integrations for data scraping:
- **`actor.py`** — Base `ApifyActor` class. Wraps `ApifyClient`, provides `run_actor()` for executing actors, and `process_documents(docs, **kwargs)` — a staged pipeline that runs: `_filter_date` → `_enrich_content` → `_filter_language` → `_enrich_location` → `_filter_location` → `_enrich_comments`. Stages are ordered cheapest-first; subclasses override individual stages. `_filter_date` resolves `period` (d/w/m) to `min_date` if no explicit `min_date` is given. `_enrich_comments` is no-op by default; subclasses override to scrape comments via dedicated actors. Defines the interface: `search(search_params, **kwargs)`. Subclasses must set `actor_id`.
- **`instagram/hashtags.py`** — `InstagramHashtagActor` extending `ApifyActor`. Implements `search(search_params)` to scrape hashtags via Apify actor `reGe1ST3OBgYZSsZJ`. Creates `Post` objects via `Post.from_instagram(item)`, then calls `process_documents()`. Overrides `_enrich_comments` to scrape comments via `apify/instagram-comment-scraper` when `get_comments=True`. Returns `List[Post]`.
- **`news/news_scraper.py`** — `GoogleNewsActor` extending `ApifyActor`. Implements `search(search_params)` to scrape Google News via actor `3Z6SK7F2WoPU3t2sg`. Defaults to Mexico/Spanish (`MX:es-419`). Creates `News` objects via `News.from_google_news(item)` (pure data mapping), then calls `process_documents()`. Overrides `_enrich_content` (delegates to `News.fetch_and_parse()`) and `_enrich_location` (delegates to `News.enrich_location(sources_manager)`). Returns `List[News]`.

### Helpers (`src/helpers/`)
- **`language.py`** — `normalize_language(raw)`: maps language identifiers to ISO 639-1 codes. Handles full names (`"spanish"` → `"es"`), BCP 47 (`"es-MX"` → `"es"`), Google News format (`"MX:es-419"` → `"es"`), and passthrough (`"es"` → `"es"`).
- **`html_cleaner.py`** — `clean_html(html)`: single-pass HTML cleaner that strips script/style/noscript/SVG/iframe/head/comments/base64 data URIs, removes non-essential attributes (keeps href/src/alt/title/datetime/content/name/property), unwraps nav/footer/aside/form tags, and collapses whitespace. Designed to reduce token count before LLM parsing.
- **`mongoconnection.py`** — MongoDB connection setup. Reads credentials from env vars (`MONGO_USER`, `MONGO_PASSWORD`, `MONGO_HOST`, `MONGO_PORT`, `MONGO_AUTHDB`), provides `mongoconn` singleton client and `get_mongo_connection()`. Used by `SourcesManagement` to load source data.
- **`rabbitmq.py`** — `RMQ` class wrapping `pika.BlockingConnection` for RabbitMQ publishing. Handles connection/channel lifecycle, auto-reconnect on AMQP errors, and exposes module-level `publish()` and `close_client()` functions via a singleton instance. Config from env vars (`RABBIT_HOST`, `RABBIT_PORT`, `RABBIT_USER`, `RABBIT_PASSWORD`, `RABBIT_EXCHANGE`, `RABBIT_QUEUE`, `RABBIT_VIRTUAL_HOST`).

### Task System
- **`src/models/crawl_task.py`** — `CrawlTask` dataclass representing a single crawl job. Fields include `period` (d/w/m, mutually exclusive with `min_date`), `get_comments` (bool), and `max_comments` (int, default 15). `from_csv_row()` parses a CSV row (search_params as comma-separated, actor_params as JSON). `to_actor_kwargs()` merges common + actor-specific params. `load_tasks(csv_path)` reads CSV and returns enabled tasks.
- **`tasks.csv`** — CSV file defining all crawl jobs. Columns: `actor_class`, `search_params`, `country_id`, `language`, `min_date`, `period`, `max_results`, `enabled`, `publish`, `get_comments`, `max_comments`, `actor_params`.
- **`src/actors/__init__.py`** — Actor registry mapping string keys (`google_news`, `instagram_hashtags`) to actor classes. `get_actor(actor_class)` instantiates by key.

### Entry Point
- **`src/run_searches.py`** — Main orchestrator: loads tasks from `tasks.csv`, dispatches each to the appropriate actor by calling `actor.search()` directly, and publishes to RabbitMQ. All filtering and enrichment is handled by actors via `ApifyActor.process_documents()`.

### Data Flow (staged pipeline)
```
Raw Apify actor output
  → create Documents (pure data mapping, no enrichment)
  → process_documents() pipeline:
      1. _filter_date         — cheap, timestamp from API (resolves period to min_date)
      2. _enrich_content      — expensive (fetch_and_parse for news, no-op for social)
      3. _filter_language      — cheap, needs body text from step 2
      4. _enrich_location      — news: SourcesManagement domain lookup (all author location fields); social: geocoding via add_locations() → location_ids
      5. _filter_location      — cheap, checks location_ids first (any geoid prefix match), falls back to author_location_id; keeps docs with no location data
      6. _enrich_comments      — if get_comments: scrape via dedicated actor per platform
  → to_final_schema → publish to RabbitMQ
```

## Dependencies (`requirements.txt`)
`apify_client`, `newsplease`, `newspaper4k`, `openai`, `pika`, `python-dateutil`, `python-dotenv`, `pytz`, `requests`, `tldextract`, `pymongo`, `langdetect`.

## Development Rules

After every change/feature implementation, modify relevant documentation accordingly

Tests should be updated when schemas are changed, new Actors are implemented, or new parameters/filtering/processing config are added to the task system/actors

## Testing

- Tests live in `src/tests/` and use pytest: `python -m pytest src/tests/ -v`
- Test cache fixtures (real Apify responses) are in `src/tests/cache/` and are committed to the repo
- MongoDB is mocked at import time via `conftest.py` (no running MongoDB required for tests)

## Development Notes

- Python 3 with extensive type hints (`from __future__ import annotations`)
- Python 3 with extensive type hints (`from __future__ import annotations`)
- Environment config via `.env` (Apify token, service endpoints)
- MCP server configured in `.mcp.json` for Apify actor interaction
