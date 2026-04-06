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
- **`news.py`** — `News(Document)`. Represents a news article. `from_url(url)` class method creates a News from any URL, fetches and parses it, returns News or None. `from_google_news(item)` class method creates a News from raw Apify dict (pure data mapping, no enrichment). `fetch_and_parse()` downloads HTML via `fetch_html()` (unpacking the `(html, final_url)` tuple), updates `self.data["url"]` to the final URL when it differs from the original (handling shortened/redirected URLs), then extracts content via `extract_article()` using the final URL, and updates `self.data`. `enrich_location(sources)` sets ALL author location fields (`location_author_formatted_name`, `location_author_geoid`, `location_author_coords`, `location_author_precision_level`, `location_author_level_1`/`level_2`/`level_3` and their IDs, plus `author_location_text`/`author_location_id`) from `SourcesManagement` domain lookup and tracks unknown sources. News articles do NOT geocode — location comes from SourcesManagement only. `to_final_schema()` normalizes via `normalize_record(data, "MessageWrapper")`.
- **`post.py`** — Base `Post(Document)` class for social media posts. Provides `to_final_schema()` which normalizes via `normalize_record(data, "MessageWrapper")`. Platform-specific subclasses handle data mapping. Shared `UsersManagement` singleton caches user profile data. Attribute: `attached_news: News | None` (initialized to `None`) stores the `News` object created when a linked article is fetched. Methods: `fetch_attached_url(sources_manager)` first checks `self._raw.get("link")` for an explicit attached URL (e.g. Facebook's `link` field), then falls back to extracting the first external URL from the post body; fetches and parses it via `News.from_url()`, appends article text as `\n\n attached_url_text: <text>`, optionally copies source location to the post, and stores the resulting `News` object on `self.attached_news`. `_extract_first_external_url()` finds the first non-platform URL in the body. `enrich_location()` (geocode with UsersManagement cache), `needs_user_author_update(max_age_days)`, `apply_cached_user_author()` (applies cached `website_visits`, `author_full_name`, `author_profile_bio`, and `author_location_text`), `save_user_author_stats(stats)` (also writes `author_location_text` when present in stats). Intermediate schema includes `author_full_name` and `author_profile_bio` fields.
- **`instagram_post.py`** — `InstagramPost(Post)`. `from_instagram(item)` class method creates from raw Instagram Apify result, mapping caption, media URLs (via `_collect_media_urls`), engagement metrics, profile URL, and post type (Image, Sidecar, Story, Reel).
- **`facebook_post.py`** — `FacebookPost(Post)`. `from_facebook(item)` class method creates from raw Facebook Posts Scraper result, mapping text, media (with OCR text appended as `image_text_N`), engagement metrics, and page name extracted from URL. Facebook post types: Photo, Video, Reel, Status.
- **`sources_management.py`** — `SourcesManagement` class. Single interface for all source-related operations. Reads sources from MongoDB (via `src/helpers/mongoconnection.py`) at module load and builds `_known_sources` set and `_domain_country_id` dict. Methods: `get_domain(url)` extracts domain, `is_known(domain)` checks known sources, `get_country_id(domain)` returns country_id (first 4 chars of `geoid`), `get_location(domain)` and `_build_domain_location()` return a full location dict with all author location fields (`location_author_formatted_name`, `location_author_geoid`, `location_author_coords`, `location_author_precision_level`, `location_author_level_1`/`level_2`/`level_3` and their IDs), 
- **`news_parser/`** — Article enrichment pipeline (moved from `src/actors/news/news_parser/`):
  - **`load_url.py`** — `fetch_html(url)`: streaming HTTP download with size guard (10MB), retry, and browser UA headers. Returns a tuple `(html, final_url)` where `final_url` is the URL after following redirects (`r.url` from requests). Returns `(None, None)` on failure.
  - **`parser.py`** — `extract_article(html, url)`: three-tier content extraction. First tries NewsPlease, fills gaps with newspaper4k, falls back to LLM (via `src/oai/llm_core.py`) when both parsers fail. Returns `{title, body, author, media_urls}` or None. Requires body >= 200 chars for meaningful content.

### Actors System (`src/actors/`)
Apify actor integrations for data scraping:
- **`actor.py`** — Base `ApifyActor` class. Wraps `ApifyClient`, provides `run_actor()` for executing actors, and `process_documents(docs, **kwargs)` — a staged pipeline that runs: `_filter_keywords` → `_filter_date` → `_enrich_content` → `_filter_language` → `_enrich_location` → `_filter_location` → `_filter_llm` → `_enrich_user_author` → `_enrich_comments`. Stages are ordered cheapest-first; subclasses override individual stages. `_filter_keywords` filters out documents matching any `not_keywords` (case-insensitive substring on body+title). `_filter_date` resolves `period` (d/w/m) to `min_date` if no explicit `min_date` is given. `_filter_llm` sends batched text snippets (keyword-context or first 250 chars) to an LLM with a Spanish-language `llm_filter_condition`; LLM returns indices to keep. Uses `llm_cached_call` from `src/oai/llm_core.py`. `search_params` is stored as `self.search_params` by each actor's `search()` for keyword-context snippet extraction. `_enrich_user_author` is no-op by default; Instagram overrides to enrich posts with author profile data (followers, bio, full name) via `apify/instagram-profile-scraper`. `_enrich_comments` is no-op by default; subclasses override to scrape comments via dedicated actors. Defines the interface: `search(search_params, **kwargs)`. Subclasses must set `actor_id`.
- **`instagram/hashtags.py`** — `InstagramHashtagActor` extending `ApifyActor`. Implements `search(search_params)` to scrape hashtags via Apify actor `reGe1ST3OBgYZSsZJ`. Creates `InstagramPost` objects via `InstagramPost.from_instagram(item)`, then calls `process_documents()`. Overrides `_enrich_content`: loops over posts calling `doc.fetch_attached_url(self.sources_manager)` directly (URL extraction, fetch/parse via `News.from_url()`, text append, location copy, storing the `News` object on `post.attached_news` — all handled by `Post.fetch_attached_url()`), plus Instagram-specific stubs: `_download_images`, `_download_video`, `_add_text_from_images`, `_add_subtitles`, `_add_ai_transcription`. Overrides `_enrich_user_author` to enrich posts with author profile data (bio, followers, full name) via `apify/instagram-profile-scraper` when `enrich_followers=True`. Overrides `_enrich_comments` to scrape comments via `apify/instagram-comment-scraper` when `get_comments=True`. Returns `List[InstagramPost]`.
- **`facebook/posts.py`** — `FacebookPagePostsActor` extending `ApifyActor`. Implements `search(search_params)` to scrape Facebook page posts via `apify/facebook-posts-scraper`. `search_params` are Facebook page URLs. Creates `FacebookPost` objects via `FacebookPost.from_facebook(item)`, then calls `process_documents()`. Overrides `_enrich_content`: loops over posts calling `doc.fetch_attached_url(self.sources_manager)` directly, plus Facebook-specific stubs: `_download_images`, `_download_video`, `_add_text_from_images`, `_add_subtitles`, `_add_ai_transcription`. Overrides `_enrich_user_author` to enrich posts with page profile data (page likes as follower proxy, full name, bio, location text) via `FacebookProfileActor` when `enrich_followers=True`. Follows the same pattern as Instagram: apply cached stats → collect stale profiles → bulk scrape → index by page name → map results back → `save_user_author_stats()`. `stats_max_age_days` configurable (default 90). Overrides `_enrich_comments` to scrape comments via `FacebookCommentsActor` (`apify/facebook-comments-scraper`) when `get_comments=True`. Returns `List[FacebookPost]`.
- **`facebook/profiles.py`** — `FacebookProfileActor`, a lightweight utility class (does NOT inherit from `ApifyActor`). Wraps `apify/facebook-pages-scraper` for page profile lookups. Not a search-task actor and NOT in the actor registry. `__init__(self, client)` accepts an existing `ApifyClient`. `scrape_pages(page_urls)` calls the actor with `startUrls` and returns raw results. `map_profile(raw)` (static method) maps raw output to a stats dict: `likes` → `website_visits`, `title` → `author_full_name`, `info` (list joined with newline) → `author_profile_bio`, `address` → `author_location_text`. Also stores `categories`, `rating`, `email`, `phone`, `website`, and the full raw response as `_raw_profile`.
- **`facebook/comments.py`** — `FacebookCommentsActor`, a lightweight utility class (does NOT inherit from `ApifyActor`). Wraps `apify/facebook-comments-scraper` for post comment scraping. Not a search-task actor and NOT in the actor registry. `__init__(self, client)` accepts an existing `ApifyClient`. `scrape_comments(post_urls, max_comments)` calls the actor with `startUrls` and `resultsLimit` and returns raw results. `map_comment(raw)` (static method) maps raw output to the common comments schema: `text` → `comment_text`, `profileName` → `comment_author`, `date` → `comment_timestamp`, `likesCount` → `comment_likes`. `group_by_post_url(raw_comments)` groups mapped comments by post URL (falls back from `postUrl` to `facebookUrl`).
- **`news/news_scraper.py`** — `GoogleNewsActor` extending `ApifyActor`. Implements `search(search_params)` to scrape Google News via actor `3Z6SK7F2WoPU3t2sg`. Defaults to Mexico/Spanish (`MX:es-419`). Creates `News` objects via `News.from_google_news(item)` (pure data mapping), then calls `process_documents()`. Overrides `_enrich_content` (delegates to `News.fetch_and_parse()`) and `_enrich_location` (delegates to `News.enrich_location(sources_manager)`). Returns `List[News]`.

### Helpers (`src/helpers/`)
- **`language.py`** — `normalize_language(raw)`: maps language identifiers to ISO 639-1 codes. Handles full names (`"spanish"` → `"es"`), BCP 47 (`"es-MX"` → `"es"`), Google News format (`"MX:es-419"` → `"es"`), and passthrough (`"es"` → `"es"`).
- **`html_cleaner.py`** — `clean_html(html)`: single-pass HTML cleaner that strips script/style/noscript/SVG/iframe/head/comments/base64 data URIs, removes non-essential attributes (keeps href/src/alt/title/datetime/content/name/property), unwraps nav/footer/aside/form tags, and collapses whitespace. Designed to reduce token count before LLM parsing.
- **`mongoconnection.py`** — MongoDB connection setup. Reads credentials from env vars (`MONGO_USER`, `MONGO_PASSWORD`, `MONGO_HOST`, `MONGO_PORT`, `MONGO_AUTHDB`), provides `mongoconn` singleton client and `get_mongo_connection()`. Used by `SourcesManagement` to load source data.
- **`rabbitmq.py`** — `RMQ` class wrapping `pika.BlockingConnection` for RabbitMQ publishing. Handles connection/channel lifecycle, auto-reconnect on AMQP errors, and exposes module-level `publish()` and `close_client()` functions via a singleton instance. Config from env vars (`RABBIT_HOST`, `RABBIT_PORT`, `RABBIT_USER`, `RABBIT_PASSWORD`, `RABBIT_EXCHANGE`, `RABBIT_QUEUE`, `RABBIT_VIRTUAL_HOST`).

### Task System
- **`src/models/crawl_task.py`** — `CrawlTask` dataclass representing a single crawl job. Fields include `period` (d/w/m, mutually exclusive with `min_date`), `get_comments` (bool), `max_comments` (int, default 15), `not_keywords` (list, pipe-separated), and `llm_filter_condition` (str, Spanish-language filtering condition for LLM-based filtering). `from_csv_row()` parses a row dict (search_params as comma-separated, not_keywords as pipe-separated, actor_params as JSON). `to_actor_kwargs()` merges common + actor-specific params. `load_tasks(xlsx_path)` reads Excel (.xlsx) and returns enabled tasks.
- **`tasks.xlsx`** — Excel file defining all crawl jobs. Columns: `actor_class`, `search_params`, `country_id`, `language`, `min_date`, `period`, `max_results`, `enabled`, `publish`, `get_comments`, `max_comments`, `not_keywords`, `llm_filter_condition`, `actor_params`.
- **`src/actors/__init__.py`** — Actor registry mapping string keys (`google_news`, `instagram_hashtags`, `facebook_page_posts`) to actor classes. `get_actor(actor_class)` instantiates by key.

### Entry Point
- **`src/run_searches.py`** — Main orchestrator: loads tasks from `tasks.csv`, dispatches each to the appropriate actor by calling `actor.search()` directly, and publishes to RabbitMQ. All filtering and enrichment is handled by actors via `ApifyActor.process_documents()`.

### Data Flow (staged pipeline)
```
Raw Apify actor output
  → create Documents (pure data mapping, no enrichment)
  → process_documents() pipeline:
      1. _filter_keywords      — cheapest, case-insensitive substring match on body+title against not_keywords
      2. _filter_date          — cheap, timestamp from API (resolves period to min_date)
      3. _enrich_content       — expensive (fetch_and_parse for news; social: fetch_attached_url via Post.fetch_attached_url() — fetches linked article, appends text, stores News on post.attached_news; download media stubs, OCR/transcription stubs)
      4. _filter_language      — cheap, needs body text from step 3
      5. _enrich_location      — news: SourcesManagement domain lookup (all author location fields); social: geocoding via add_locations() → location_ids
      6. _filter_location      — cheap, checks location_ids first (any geoid prefix match), falls back to author_location_id; keeps docs with no location data
      7. _filter_llm           — LLM-based filtering: sends batched text snippets (keyword context or first 250 chars) to LLM with a Spanish-language condition, keeps only indices returned. Skipped if llm_filter_condition not set
      8. _enrich_user_author     — user profile enrichment (bio, followers, full name, location text); Instagram uses apify/instagram-profile-scraper; Facebook uses FacebookProfileActor (apify/facebook-pages-scraper, page likes as follower proxy)
      9. _enrich_comments      — if get_comments: scrape via dedicated actor per platform
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
