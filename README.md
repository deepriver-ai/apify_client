# Apify Media Data Ingestion & Normalization Pipeline

Scrapes media and social media data via Apify actors, normalizes it through a declarative schema system, and publishes to RabbitMQ for downstream processing.

Supported sources: News, Instagram, Facebook, X, LinkedIn (and others to be implemented).


# Models (`src/models/`)

## Document

Base class for all media types (`src/models/document.py`).

- `Document(data)` — accepts an optional dict; defaults to `_empty_data()` (all intermediate fields set to `None` or `[]`, including `language`)
- `Document.data` — the intermediate schema dict (see below)
- `Document.detect_language()` — detects language from body/title via `langdetect`, caches result in `data["language"]`. Returns ISO 639-1 code or None
- `Document.matches_language(code)` — checks if document matches a language code (normalizes both sides via `normalize_language()`)
- `Document.matches_min_date(dt)` — checks if document's timestamp is >= the given datetime
- `Document.matches_location(country_id)` — geoid prefix match on `data["location_id"]`. Non-geoid IDs (e.g. Instagram numeric) pass through
- `Document.to_final_schema()` — abstract method; subclasses normalize `self.data` to the final output schema

## News

News article model (`src/models/news.py`). Inherits from `Document`.

- `News.from_google_news(item)` — class method that creates a `News` from a raw Google News Apify result, mapping `link`/`url`, `title`, `description`, `image`, `publishedAt`, and `source` into the intermediate schema. Pure data mapping — no enrichment or source lookups
- `News.fetch_and_parse()` — downloads the article HTML via `fetch_html(url)` and extracts content via `extract_article(html, url)`. Updates `title`, `body`, `author`, and `media_urls` in `self.data`. Returns `True` on success, `False` on failure
- `News.enrich_location(sources)` — sets `location_text` and `location_id` from `SourcesManagement` domain lookup. Also calls `check_source()` to track unknown domains
- `News.to_final_schema()` — normalizes `self.data` via `normalize_record(data, "MessageWrapper")`

## Post

Social media post model (`src/models/post.py`). Inherits from `Document`.

- `Post.from_instagram(item)` — class method that creates a `Post` from a raw Instagram Apify result, mapping `caption`, `ownerUsername`/`ownerFullName`, engagement metrics (`likesCount`, `commentsCount`, `reshareCount`, `videoPlayCount`), media URLs (recursively collected from `displayUrl`, `videoUrl`, `images`, and `childPosts`), and post type
- `Post.to_final_schema()` — normalizes `self.data` via `normalize_record(data, "MessageWrapper")`

## SourcesManagement

A source refers to the publisher of a news article, it is the news media site and includes metadata such as domain, name, location, etc. Domain is used as identifier
Single interface for all source-related operations (`src/models/sources_management.py`). 
Sources are stored in a MongoDB database,  (connection via `src/helpers/mongoconnection.py`) at module load and builds lookup dicts internally.

**Unknown source tracking:**
- `check_source(url, source_name)` — returns `True` if the URL's domain is known (exists in MongoDB); otherwise records it as unknown
- `save()` — loads existing unknowns from `cache/unknown_sources.json`, prunes any that became known, merges new entries, and writes back

**Source lookups:**
- `get_domain(url)` — extracts the domain from a URL (delegates to `sources.get_domain`)
- `is_known(domain)` — returns `True` if the domain is in the known sources set (from MongoDB)
- `get_country_id(domain)` — returns the country_id (first 4 chars of `location_author_geoid`) for a domain, or `None`
- `get_location(domain)` — returns `{location_text, location_id}` (`location_author_formatted_name` and `location_author_geoid`) for a domain, or both `None`
- `filter_by_country(items, country_id)` — filters a list of raw result dicts by country. Keeps items from unknown domains, domains with no country mapping, or domains matching the given `country_id`. Discards the rest.


# Intermediate schema

All data fetched from Apify is first translated into this common intermediate schema before final normalization:

| Field | Description |
|---|---|
| `timestamp` | Published timestamp of the article/post |
| `source` | Name of the source (media outlet or social network) |
| `title` | Title of the publication |
| `body` | Text body (or caption for posts) |
| `url` | URL of the publication |
| `media_urls` | URLs for media (images, testigos) |
| `type` | One of: `news`, `X`, `Facebook`, `impreso`, `Instagram`, `Radio`, `TV` |
| `author` | Author name (journalist for news, username for social) |
| `article_value` | Estimated cost if the publication were paid for |
| `website_visits` | Monthly visitors (news) or followers (social) |
| `likes` | Like count (social media) |
| `shares` | Share count (social media) |
| `views` | View count (social media) |
| `n_comments` | Comment count (social media) |
| `profile_url` | Publisher profile URL (social media) |
| `post_type` | Post type, e.g. Image, Sidecar, Video, Story, Reel (social media) |
| `location_text` | Location name — for news: `location_author_formatted_name` from source stats; for social: `locationName` from post |
| `location_id` | Location identifier — for news: `location_author_geoid` from source stats; for social: platform location ID (e.g. Instagram `locationId`) |
| `language` | Detected language as ISO 639-1 code (e.g. `es`, `en`). Set by `detect_language()` or by actor from metadata |

Final normalization uses the schema engine in `src/schema/` (see `src/schema/readme_schema.md`):

```python
from src.schema import normalize_record
normalized = normalize_record(raw_record, "MessageWrapper")
```


# Task System

All crawl jobs are defined in `tasks.csv` and dispatched through `src/run_searches.py`.

## CSV Schema (`tasks.csv`)

| Column | Type | Required | Description |
|---|---|---|---|
| `actor_class` | str | yes | Registry key: `google_news`, `instagram_hashtags` |
| `keywords` | str | yes | Comma-separated keywords (quoted in CSV) |
| `country_id` | str | no | Country filter, e.g. `_484` |
| `language` | str | no | Language code, e.g. `es`, `en`. Filtered at Document level via detection |
| `min_date` | str | no | ISO date/datetime, e.g. `2026-03-15`. Publications older than this are dropped at Document level. Should be enforced by Apify via each Actor params if possible, otherwise by the actor class |
| `max_results` | int | no | Max items to retrieve (default 30) |
| `method` | str | no | `search` (default), `get_data`, `get_page_data` |
| `enabled` | bool | no | Default true. Disabled rows skipped |
| `publish` | bool | no | Default true. Whether to publish to RabbitMQ |
| `actor_params` | JSON str | no | Actor-specific overrides as JSON |


## Actor-specific params (via `actor_params`)

- **GoogleNewsActor**: `timeframe`, `region_language`, `decode_urls`, `extract_descriptions`, `extract_images`, `enrich`
- **InstagramHashtagActor**: `keyword_search`, `results_type`

## Processing pipeline (`ApifyActor.process_documents`)

After creating Documents from raw API results, actors call `process_documents()` which runs a staged pipeline ordered by processing cost (cheapest first):

```
Documents (created from raw results, no enrichment)
  → 1. _filter_date         — cheap, timestamp available from API
  → 2. _enrich_content      — expensive (fetch_and_parse for news, no-op for social)
  → 3. _filter_language      — cheap, needs body text from step 2
  → 4. _enrich_location      — news: SourcesManagement domain lookup; social: geocoding (future)
  → 5. _filter_location      — cheap, geoid prefix match on location_id
  → to_final_schema → publish to RabbitMQ
```

Each stage is a method on `ApifyActor` that subclasses can override. For example, `GoogleNewsActor` overrides `_enrich_content` (delegates to `News.fetch_and_parse()`) and `_enrich_location` (delegates to `News.enrich_location(sources_manager)`).

**Cost savings:** date filtering runs before content enrichment, so articles that fail the date check skip the expensive HTTP fetch + parse. Language filtering runs before location enrichment, so documents in the wrong language skip geocoding.

Language codes are normalized to ISO 639-1 via `normalize_language()` — accepts `"es"`, `"spanish"`, `"MX:es-419"`, `"es-MX"`, etc. Location filtering uses geoid prefix matching: `location_id="_48416053"` matches `country_id="_484"` (Mexico). Non-geoid location IDs (e.g. Instagram numeric IDs) pass through. Documents with missing data are kept.

## Example rows

```csv
actor_class,keywords,country_id,language,min_date,max_results,method,enabled,publish,actor_params
google_news,"crimen,seguridad",_484,es,2026-03-15,30,search,true,true,"{""timeframe"":""1d"",""enrich"":true}"
instagram_hashtags,"#seguridad,#mexico",,es,,50,search,true,true,"{""keyword_search"":false}"
```

## Running

```bash
python -m src.run_searches                # uses tasks.csv by default
python -m src.run_searches custom.csv     # custom task file
```

## Actor registry (`src/actors/__init__.py`)

Maps string keys to actor classes:

| Key | Class |
|---|---|
| `google_news` | `GoogleNewsActor` |
| `instagram_hashtags` | `InstagramHashtagActor` |


# Pipelines

## News ingestion pipeline

Entry point: `src/run_searches.py` → `GoogleNewsActor.search()`

```
keywords
  │
  ▼
GoogleNewsActor.search(keywords, **kwargs)
  │
  ├─ 1. run_actor() ──► Apify Google News Scraper (actor 3Z6SK7F2WoPU3t2sg)
  │                      returns raw results: {title, link, source, description, image, publishedAt}
  │
  ├─ 2. News.from_google_news(item) for each result
  │      pure data mapping → intermediate schema (no enrichment)
  │
  └─ 3. process_documents(articles, **kwargs)
         │
         ├─ _filter_date          — drop articles older than min_date (cheap, uses timestamp)
         │
         ├─ _enrich_content       — news.fetch_and_parse() for each surviving article
         │      │
         │      ├─ fetch_html(url)          streaming download, 10MB guard, 3 retries
         │      │
         │      └─ extract_article(html, url)
         │           ├─ Tier 1: NewsPlease
         │           ├─ Tier 2: newspaper4k   (fallback / gap-fill)
         │           └─ Tier 3: LLM           (if body < 200 chars)
         │
         ├─ _filter_language      — drop articles not matching language (needs body text)
         │
         ├─ _enrich_location      — news.enrich_location(sources_manager) for each surviving article
         │      sets location_text/location_id from domain lookup
         │      tracks unknown sources via check_source()
         │
         └─ _filter_location      — drop articles not matching country_id (geoid prefix match)
```

**Defaults:** region `MX:es-419`, timeframe `1d`, max 30 articles, URL decoding enabled.

## Instagram hashtag pipeline (partial)

Entry point: `InstagramHashtagActor.search(keywords)`

Scrapes hashtag results via Apify actor `reGe1ST3OBgYZSsZJ`, translates raw results to the intermediate schema. Final normalization and RabbitMQ publishing follow the same pattern as news.


# Data ingest — actors (`src/actors/`)

Each actor lives in its own subfolder (one per platform). Actor classes inherit from `ApifyActor` (`src/actors/actor.py`) and implement:

| Method | Input | Output |
|---|---|---|
| `search(keywords)` | list of search keywords | `List[Document]` (e.g. `List[News]`) |
| `get_data(urls)` | list of article/post URLs | `List[Document]` |
| `get_page_data(profile_urls)` | list of profile/page URLs | `List[Document]` |

The base class provides `process_documents(docs, **kwargs)` — a staged pipeline (date filter → content enrichment → language filter → location enrichment → location filter). Subclasses call it after creating Documents and override individual stages (`_enrich_content`, `_enrich_location`, etc.) to provide actor-specific behavior.

Current actors:
- **`news/news_scraper.py`** — `GoogleNewsActor` (actor `3Z6SK7F2WoPU3t2sg`)
- **`instagram/hashtags.py`** — `InstagramHashtagActor` (actor `reGe1ST3OBgYZSsZJ`)


# Article parsing (`src/models/news_parser/`)

Used by `News.fetch_and_parse()` to download and extract full article content.

- **`load_url.py`** — `fetch_html(url)`: streaming HTTP download, 10 MB size guard, 15s timeout, 3 retries, browser UA headers. Returns HTML string or `None`.
- **`parser.py`** — `extract_article(html, url)`: three-tier extraction (NewsPlease → newspaper4k → LLM). Returns `{title, body, author, media_urls}` or `None`. Requires body >= 200 chars.

### HTML cleaner (`src/helpers/html_cleaner.py`)

Single-pass HTML cleaner that reduces token count before LLM parsing:

- **Strips entirely:** `script`, `style`, `noscript`, `svg`, `math`, `iframe`, `object`, `embed`, `applet`, `head`, `header`, `footer`, `aside`, `nav`
- **Unwraps:** `button`, `input`, `select`, `textarea`, `form`, `menu`
- **Drops markup:** `span`, `div`, `section`, `article`, `figure`, `ul/ol/li`, `table/*`, `main`, `details`, `summary`
- **Removes:** HTML comments, base64 data URIs, social share links, placeholder images, empty tags, non-essential attributes
- **Preserves:** `href`, `src`, `alt`, `datetime`, `content`, `name`, `property` attributes; article text; heading structure


# Supporting modules

- **`src/oai/`** — LLM client, call wrappers, and caching (`llm_core.py`, `llm_call.py`). LLM annotation outputs (relevance filtering, stance extraction) are cached; data layout files (`filtered_posts.json`, `analyzed_posts.json`, etc.) reflect cached annotation outputs.
- **`src/helpers/mongoconnection.py`** — MongoDB connection setup. Reads credentials from env vars, provides `mongoconn` singleton client. Used by `SourcesManagement` to load source data.
- **`src/helpers/rabbitmq.py`** — `RMQ` class wrapping `pika.BlockingConnection`. Auto-reconnect, module-level `publish()` and `close_client()` singleton. Config via env vars.
- **`src/helpers/text_fn.py`** — Text normalization utilities.
- **`src/helpers/word_embeddings.py`** — Embedding utilities.
- **`src/schema/`** — Declarative multi-stage normalization framework (see `src/schema/readme_schema.md`).


# TODO

- **`SourcesManagement` Mongo create/update** — `SourcesManagement` currently reads sources from MongoDB via `src/helpers/sources.py` but has no ability to create or update source records in Mongo. Add methods to create new sources and update existing ones directly through `SourcesManagement`, so that unknown sources can be promoted to known sources without manual DB intervention.
