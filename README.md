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
- `Document.matches_location(country_id)` — checks `location_ids` first: if non-empty, any geoid prefix match keeps the doc. Falls back to `author_location_id` when `location_ids` is empty. Non-geoid IDs (e.g. Instagram numeric) pass through. Documents with no location data are kept
- `Document.enrich_location(**kwargs)` — abstract method; subclasses implement platform-specific location enrichment (geocoding for posts, SourcesManagement lookup for news)
- `Document.to_final_schema()` — parses `self.data` against `NEWS_SCHEMA` and returns `{"type": "news", "message": parsed}`. The envelope `type` is always `"news"`; the inner `message.type` carries the actual platform (one of `news`, `x`, `facebook`, `instagram`, `linkedin`, `impreso`, `radio`, `tv`). Subclasses can override to add preprocessing, then call `super().to_final_schema()`

## News

News article model (`src/models/news.py`). Inherits from `Document`.

- `News.from_url(url)` — class method that creates a `News` from any URL, fetches and parses it. Returns the `News` object if successful, `None` if fetch/parse fails. Used by `fetch_attached_url` to parse linked articles from social media posts
- `News.from_google_news(item)` — class method that creates a `News` from a raw Google News Apify result, mapping `link`/`url`, `title`, `description`, `image`, `publishedAt`, and `source` into the intermediate schema. Pure data mapping — no enrichment or source lookups
- `News.fetch_and_parse()` — downloads the article HTML via `fetch_html(url)`, unpacking the `(html, final_url)` tuple. Updates `self.data["url"]` to the final URL when it differs from the original (handling shortened/redirected URLs). Passes the final URL to `extract_article(html, final_url)`. Updates `title`, `body`, `author`, `media_urls`, and `timestamp` in `self.data` (only overwriting when the parser returned a non-empty value). Returns `True` on success, `False` on failure
- `News.enrich_location(sources)` — sets all author location fields (`author_location_text`, `author_location_id`, `location_author_formatted_name`, `location_author_geoid`, `location_author_coords`, `location_author_precision_level`, `location_author_level_1`/`level_2`/`level_3` and their IDs) from `SourcesManagement` domain lookup. Also calls `check_source()` to track unknown domains
- `News` uses the inherited `Document.to_final_schema()` — `data["type"]` is `"news"`, so the envelope wraps as `{"type": "news", "message": parsed_news}`

## Post

Base social media post model (`src/models/post.py`). Inherits from `Document`. Platform-specific subclasses handle data mapping and set `data["type"]` to the real platform (`instagram`, `facebook`, `x`, `linkedin`).

Posts share the same final envelope as News: `{"type": "news", "message": parsed}` where `parsed` is validated against `NEWS_SCHEMA`.

- `Post.attached_news` — `News | None` attribute (initialized to `None`). Stores the `News` object created when a linked article is successfully fetched via `fetch_attached_url()`
- `Post.fetch_attached_url(sources_manager)` — extracts the first external URL from the post body, fetches and parses it via `News.from_url()`, appends article text as `\n\n attached_url_text: <text>`, optionally copies the source location to the post, stores the resulting `News` object on `self.attached_news`, and falls back to the parent post's `timestamp` when the fetched article still has no `timestamp` after parsing (the parser surfaces a date when one is present in the HTML, but some articles lack it) so the attached news isn't published with a null date
- `Post._extract_first_external_url()` — finds the first non-platform URL in the post body
- `Post.enrich_location(**kwargs)` — geocodes body text to populate `location_ids` and `location_author_*` fields. If a `users_manager` is provided in kwargs and already has cached location data for the user (by `profile_url`), applies cached data and skips geocoding. When geocoding finds a city-level location, saves it to `UsersManagement` for future reuse
- `Post.apply_cached_user_author()` — applies cached `website_visits`, `author_full_name`, `author_profile_bio`, and `author_location_text` from `UsersManagement` to the post's intermediate schema
- `Post.save_user_author_stats(stats)` — writes profile stats (follower count, full name, bio, and `author_location_text` when present) to the intermediate schema and persists to `UsersManagement`
- `Post.to_final_schema()` — fills fallback `body`, `title`, and `fb_likes`, then delegates to `Document.to_final_schema()` for schema parsing and envelope wrapping. Returns `None` on validation error (logged)

### InstagramPost

Instagram post model (`src/models/instagram_post.py`). Inherits from `Post`.

- `InstagramPost.from_instagram(item)` — class method mapping `caption`, `ownerUsername`/`ownerFullName`, engagement metrics (`likesCount`, `commentsCount`, `reshareCount`, `videoPlayCount`), media URLs (recursively collected from `displayUrl`, `videoUrl`, `images`, and `childPosts`), and post type. Instagram post types: `Image`, `Sidecar`, `Story`, `Reel`

### FacebookPost

Facebook post model (`src/models/facebook_post.py`). Inherits from `Post`.

- `FacebookPost.from_facebook(item)` — class method mapping `text`, `likes`, `shares`, `comments` (count), media URLs (from `photo_image.uri` for photos, `browser_native_hd_url`/`browser_native_sd_url` for videos), and page name (extracted from URL). OCR text from media `ocrText` fields is appended to body as `\n\n image_text_1: <text>`, `\n\n image_text_2: <text>`, etc. (skips generic "May be an image of" descriptions). Facebook post types: `Photo`, `Video`, `Reel`, `Status`

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
- `get_country_id(domain)` — returns the country_id (first 4 chars of `geoid`) for a domain, or `None`
- `get_location(domain)` — returns a full location dict with all author location fields (`location_author_formatted_name`, `location_author_geoid`, `location_author_coords`, `location_author_precision_level`, `location_author_level_1`/`level_2`/`level_3` and their IDs) for a domain, or `None` for each field when not found. Built by `_build_domain_location()`

## UsersManagement

Caches social media user profile data (`src/models/users_management.py`). Similar pattern to `SourcesManagement` but backed by a JSON file (`cache/users.json`) — no MongoDB collection yet (TODO).

Keyed by `profile_url` (e.g. `https://www.instagram.com/username/`). Stores per-user: follower count (`website_visits`), author name, geocoded location fields (`location_author_*`), and `date_stats_updated` timestamp.

**User lookups:**
- `get_user(profile_url)` — return full cached user dict or None
- `is_known(profile_url)` — True if any data exists for this user
- `has_location(profile_url)` — True if geocoded location (geoid) is cached
- `get_location(profile_url)` — return cached location fields dict, or None
- `get_stats(profile_url)` — return cached stats dict (`website_visits`, `author`, `author_full_name`, `author_profile_bio`, `author_location_text`, `date_stats_updated`), or None

**Staleness tracking:**
- `needs_stats_update(profile_url, max_age_days=90)` — True if stats are missing or `date_stats_updated` is older than `max_age_days`. Used by `_enrich_user_author` to skip users with fresh stats
- `stats_max_age_days` is configurable per task via `actor_params`

**Saving data:**
- `save_stats(profile_url, stats)` — update user with profile stats (follower count, bio, full name) + set `date_stats_updated` to now, persist to disk
- `save_location(profile_url, location)` — update user with geocoded location fields, persist to disk. Called by `Post.enrich_location()` when geocoding finds a city-level result
- `save()` — persist full cache to `cache/users.json`

**Integration points:**
- `Post.enrich_location(**kwargs)` — checks `UsersManagement` for cached user location before geocoding; saves new geocoded locations back
- `InstagramHashtagActor._enrich_user_author()` — uses `UsersManagement` to skip fresh profiles and save scraped profile data (followers, bio, full name)
- `FacebookPagePostsActor._enrich_user_author()` — uses `UsersManagement` to skip fresh profiles and save scraped page profile data (followers, bio, full name, location text via `FacebookProfileActor`)
- `FacebookKeywordSearchActor._enrich_user_author()` — same pattern as `FacebookPagePostsActor`: applies cached stats always, and when `enrich_followers=True` bulk-scrapes stale pages via `FacebookProfileActor` and persists to `UsersManagement`
- `Post` class-level singleton `users_manager` provides access to all pipeline stages


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
| `author_full_name` | Full name of the author (from profile scraper, social media) |
| `author_profile_bio` | Author's profile biography (from profile scraper, social media) |
| `author_location_text` | Location display name — for news: `formatted_name` from source stats; for social: `locationName` from post's user, or similar field |
| `author_location_id` | Location identifier — for news: `geoid` from source stats; for social: platform location ID (e.g. Instagram `locationId`) |
| `location_author_formatted_name` | Formatted location name from geocoding/source stats. Emitted as `location_author.formatted_name` in the final schema |
| `location_author_geoid` | Geoid of the author location (e.g. `_48416053`). Emitted as `location_author.geoid` |
| `location_author_coords` | Coordinates of the author location. Emitted as `location_author.coords` |
| `location_author_precision_level` | Precision level of the geocoded location. Emitted as `location_author.precision_level` |
| `location_author_level_1` | Top-level administrative division name. Emitted as `location_author.level_1` |
| `location_author_level_1_id` | Top-level administrative division geoid. Emitted as `location_author.level_1_id` |
| `location_author_level_2` | Second-level administrative division name. Emitted as `location_author.level_2` |
| `location_author_level_2_id` | Second-level administrative division geoid. Emitted as `location_author.level_2_id` |
| `location_author_level_3` | Third-level administrative division name. Emitted as `location_author.level_3` |
| `location_author_level_3_id` | Third-level administrative division geoid. Emitted as `location_author.level_3_id` |
| `location_ids` | List of geoids from geocoding (used by `matches_location()` before falling back to `author_location_id`) |
| `language` | Detected language as ISO 639-1 code (e.g. `es`, `en`). Set by `detect_language()` or by actor from metadata |
| `comments` | List of comment dicts, each with `comment_text`, `comment_author`, `comment_timestamp`, `comment_likes`. Empty list if comments not scraped |
| `video_filename` | Local path to downloaded video file (set by `InstagramProfilePostsActor._download_video()`). Default directory: `cache/media/instagram` |

Final normalization uses the schema engine in `src/schema/` (see `src/schema/readme_schema.md`). `Document.to_final_schema()` parses against `NEWS_SCHEMA` and wraps the result:

```python
from src.schema import normalize_record
parsed = normalize_record(raw_record, "News")
envelope = {"type": "news", "message": parsed}
```


# Task System

All crawl jobs are defined in `tasks.xlsx` and dispatched through `src/run_searches.py`.

## CrawlTask (`src/models/crawl_task.py`)

A `CrawlTask` dataclass represents a single crawl job. Each row in `tasks.csv` becomes one `CrawlTask`.

**Fields:**

| Field | Type | Default | Description |
|---|---|---|---|
| `task_id` | `str` | auto-generated | Unique task identifier. Auto-generated as `actor_class:search_params` if not provided. Used as part of filter cache keys so the same document can have different filtering outcomes for different tasks |
| `actor_class` | `str` | *(required)* | Registry key identifying the actor (e.g. `google_news`, `instagram_hashtags`) |
| `search_params` | `List[str]` | *(required)* | Search terms, parsed from comma-separated CSV value |
| `country_id` | `str \| None` | `None` | Country filter for location matching (e.g. `_484` for Mexico) |
| `language` | `str \| None` | `None` | Language filter code (e.g. `es`, `en`) |
| `max_results` | `int` | `30` | Maximum items to retrieve from Apify |
| `min_date` | `datetime \| None` | `None` | Absolute date cutoff — publications older than this are dropped. Mutually exclusive with `period` |
| `period` | `str \| None` | `None` | Relative date filter: `d` (1 day), `w` (7 days), `m` (30 days). Mutually exclusive with `min_date` |
| `enabled` | `bool` | `True` | Disabled tasks are skipped by `load_tasks()` |
| `publish` | `bool` | `True` | Whether to publish results to RabbitMQ |
| `get_comments` | `bool` | `False` | Whether to scrape comments per post via a dedicated comment actor |
| `max_comments` | `int` | `15` | Max comments per post (only used when `get_comments` is true) |
| `not_keywords` | `List[str]` | `[]` | Keywords to exclude — documents containing any are filtered out. Pipe-separated in CSV |
| `llm_filter_condition` | `str \| None` | `None` | Spanish-language LLM filtering condition. If set, documents are filtered via batched LLM calls |
| `override_filters` | `bool` | `False` | If true, ignores the filter cache and re-runs all filters from scratch. Useful for reprocessing after changing filter conditions |
| `theme` | `str \| None` | `None` | Theme tag grouping tasks (e.g. `orizaba`). `run_searches.py` filters tasks to a single `CURRENT_THEME` constant at runtime |
| `actor_params` | `Dict[str, Any]` | `{}` | Actor-specific overrides (parsed from JSON in CSV) |

**Key methods:**

- `CrawlTask.from_csv_row(row)` — class method that parses a row dict into a `CrawlTask`. Handles type conversions (bools, dates, JSON), validates `period` values (`d`/`w`/`m`), and raises `ValueError` if both `min_date` and `period` are set.
- `CrawlTask.to_actor_kwargs()` — merges common parameters (`task_id`, `max_results`, `country_id`, `language`, `min_date`, `period`, `get_comments`, `max_comments`, `not_keywords`, `llm_filter_condition`, `override_filters`) with `actor_params` into a single kwargs dict passed to the actor's method.
- `load_tasks(xlsx_path)` — reads the Excel (.xlsx) file, creates `CrawlTask` objects, and returns only enabled tasks.

**Data flow:** `tasks.xlsx` → `load_tasks()` → list of `CrawlTask` → for each task: `get_actor(task.actor_class)` → `actor.search(task.search_params, **task.to_actor_kwargs())` → documents → optionally publish to RabbitMQ.

## Excel Schema (`tasks.xlsx`)

| Column | Type | Required | Description |
|---|---|---|---|
| `task_id` | str | no | Unique task identifier. Auto-generated from `actor_class:search_params` if empty |
| `actor_class` | str | yes | Registry key: `google_news`, `instagram_hashtags`, `facebook_page_posts` |
| `search_params` | str | yes | Comma-separated search terms (quoted in CSV) |
| `country_id` | str | no | Country filter, e.g. `_484` |
| `language` | str | no | Language code, e.g. `es`, `en`. Filtered at Document level via detection |
| `min_date` | str | no | ISO date/datetime, e.g. `2026-03-15`. Publications older than this are dropped at Document level. Mutually exclusive with `period`. Should be enforced by Apify via each Actor params if possible, otherwise by the actor class |
| `period` | str | no | Relative date filter: `d` (1 day), `w` (7 days), `m` (30 days). Mutually exclusive with `min_date`. Resolved to a `min_date` datetime in `ApifyActor._filter_date()` |
| `max_results` | int | no | Max items to retrieve (default 30) |
| `enabled` | bool | no | Default true. Disabled rows skipped |
| `publish` | bool | no | Default true. Whether to publish to RabbitMQ |
| `get_comments` | bool | no | Default false. Whether to scrape comments for each post. Uses a dedicated comment actor per platform (e.g. `apify/instagram-comment-scraper` for Instagram). Ignored by actors that don't support comments (e.g. Google News) |
| `max_comments` | int | no | Max comments per post (default 15). Only used when `get_comments` is true |
| `not_keywords` | str | no | Pipe-separated keywords to exclude (e.g. `spam\|ads`). Documents containing any keyword (case-insensitive, substring match on body+title) are filtered out as the first pipeline step |
| `llm_filter_condition` | str | no | Spanish-language filtering condition for LLM-based filtering. The LLM receives text snippets and applies this condition to decide which to keep. Example: `"elimina publicaciones que no estén relacionadas con lubricantes"` |
| `override_filters` | bool | no | Default false. If true, ignores filter cache and re-runs all filters from scratch |
| `theme` | str | no | Theme tag (e.g. `orizaba`). Only tasks matching `CURRENT_THEME` in `src/run_searches.py` are executed |
| `actor_params` | JSON str | no | Actor-specific overrides as JSON |


## Actor-specific params (via `actor_params`)

- **GoogleNewsActor**: `timeframe`, `region_language`, `decode_urls`, `extract_descriptions`, `extract_images`, `enrich`
- **InstagramHashtagActor**: `keyword_search`, `results_type`, `fetch_attached_url`, `download_images`, `download_video`, `add_text_from_images`, `add_subtitles`, `add_ai_transcription`, `enrich_followers`, `stats_max_age_days` (default 90)
- **InstagramProfilePostsActor**: `results_type`, `fetch_attached_url`, `download_images`, `download_video`, `video_dir` (default `cache/media/instagram`), `add_text_from_images`, `add_subtitles`, `add_ai_transcription`, `enrich_followers`, `stats_max_age_days` (default 90)
- **FacebookPagePostsActor**: `fetch_attached_url`, `download_images`, `download_video`, `add_text_from_images`, `add_subtitles`, `add_ai_transcription`, `enrich_followers`, `stats_max_age_days` (default 90)

## Processing pipeline (`ApifyActor.process_documents`)

After creating Documents from raw API results, actors call `process_documents()` which runs a staged pipeline ordered by processing cost (cheapest first):

```
Documents (created from raw results, no enrichment)
  → 1. _filter_keywords      — cheapest, case-insensitive substring match on body+title against not_keywords
  → 2. _filter_date          — cheap, timestamp available from API (supports min_date or period)
  → 3. _enrich_content       — expensive (fetch_and_parse for news; social: fetch_attached_url via Post.fetch_attached_url() — fetches linked article, appends text, stores News on post.attached_news; download media stubs, OCR/transcription stubs)
  → 4. _filter_language      — cheap, needs body text from step 3
  → 5. _enrich_location      — news: SourcesManagement domain lookup; social: geocoding via Post.enrich_location() with UsersManagement caching (checks cached user location first, saves new geocoded locations back)
  → 6. _filter_location      — cheap, checks location_ids first (any geoid prefix match), falls back to author_location_id; keeps docs with no location data
  → 7. _filter_llm           — LLM-based: sends batched text snippets (with user_name, user_location, user_bio metadata) to LLM with llm_filter_condition, keeps only returned indices. Skipped if condition not set
  → 8. _enrich_user_author   — Instagram: bulk scrapes profiles via apify/instagram-profile-scraper, maps followersCount → website_visits, fullName → author_full_name, biography → author_profile_bio, caches in UsersManagement (skips fresh profiles); Facebook: bulk scrapes page profiles via FacebookProfileActor (apify/facebook-pages-scraper), maps likes → website_visits, title → author_full_name, info → author_profile_bio, address → author_location_text
  → 9. _enrich_comments      — if get_comments: scrape comments via dedicated actor per platform
  → to_final_schema → publish to RabbitMQ
```

Each stage is a method on `ApifyActor` that subclasses can override. For example, `GoogleNewsActor` overrides `_enrich_content` (delegates to `News.fetch_and_parse()`) and `_enrich_location` (delegates to `News.enrich_location(sources_manager)`).

Social actors (Instagram, Facebook) override `_enrich_content` to dispatch enrichment steps based on kwargs flags: `fetch_attached_url`, `download_images`, `download_video`, `add_text_from_images`, `add_subtitles`, `add_ai_transcription`. The `fetch_attached_url` flag causes each actor to loop over posts and call `doc.fetch_attached_url(self.sources_manager)` directly — the per-post logic (URL extraction via `Post._extract_first_external_url()`, fetch/parse via `News.from_url()`, text append, location copy, storing the `News` object on `post.attached_news`) lives in `Post.fetch_attached_url()` (`src/models/post.py`). The remaining stubs raise `NotImplementedError`.

**Filter cache** (`cache/filter_cache.json`): Filtering results are cached per document per task (keyed by `filtered:{task_id}:{url}`). On subsequent runs, documents previously filtered out are skipped before entering the pipeline. The same document can have different outcomes for different tasks. Set `override_filters=true` to ignore the cache and re-run all filters. The LLM filter has its own per-document cache (keyed by `llm_filter:{condition}:{url}`) so changing the condition automatically re-evaluates.

TODO: Replace file-based filter cache with Redis for multi-process/distributed support.

**Cost savings:** date filtering runs before content enrichment, so articles that fail the date check skip the expensive HTTP fetch + parse. Language filtering runs before location enrichment, so documents in the wrong language skip geocoding.

Language codes are normalized to ISO 639-1 via `normalize_language()` — accepts `"es"`, `"spanish"`, `"MX:es-419"`, `"es-MX"`, etc. Location filtering uses geoid prefix matching: `geoid="_48416053"` matches `country_id="_484"` (Mexico). `_filter_location` checks `location_ids` first (if non-empty, any match passes); when `location_ids` is empty it falls back to `author_location_id`. Non-geoid location IDs (e.g. Instagram numeric IDs) pass through. Documents with no location data are kept.

## Example rows

| task_id | actor_class | search_params | country_id | language | min_date | period | max_results | enabled | publish | get_comments | max_comments | not_keywords | llm_filter_condition | override_filters | actor_params |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | google_news | totalenergies | _484 | es | | d | 30 | true | true | false | | | | | {"timeframe":"1d","enrich":true} |
| 2 | instagram_hashtags | totalenergies | _484 | es | | d | 50 | true | true | false | 15 | | elimina publicaciones no relacionadas con lubricantes | | {"keyword_search":false,"enrich_followers":true} |
| 3 | facebook_page_posts | https://facebook.com/SomePage | _484 | es | | w | 20 | true | true | false | | spam\|ads | | | {"fetch_attached_url":true,"add_text_from_images":true} |

## Running

```bash
python -m src.run_searches                 # uses tasks.xlsx by default
python -m src.run_searches custom.xlsx     # custom task file
```

## Actor registry (`src/actors/__init__.py`)

Maps string keys to actor classes:

| Key | Class |
|---|---|
| `google_news` | `GoogleNewsActor` |
| `instagram_hashtags` | `InstagramHashtagActor` |
| `instagram_profile_posts` | `InstagramProfilePostsActor` |
| `facebook_page_posts` | `FacebookPagePostsActor` |


# Pipelines

## News ingestion pipeline

Entry point: `src/run_searches.py` → `GoogleNewsActor.search()`

```
search_params
  │
  ▼
GoogleNewsActor.search(search_params, **kwargs)
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
         │      │      returns (html, final_url); updates self.data["url"] if redirected
         │      │
         │      └─ extract_article(html, final_url)
         │           ├─ Tier 1: NewsPlease
         │           ├─ Tier 2: newspaper4k   (fallback / gap-fill)
         │           └─ Tier 3: LLM           (if body < 200 chars)
         │
         ├─ _filter_language      — drop articles not matching language (needs body text)
         │
         ├─ _enrich_location      — news.enrich_location(sources_manager) for each surviving article
         │      sets all author location fields (location_author_formatted_name, location_author_geoid, location_author_coords, location_author_precision_level,
         │      location_author_level_1/2/3 and their IDs, author_location_text, author_location_id)
         │      from SourcesManagement domain lookup; tracks unknown sources via check_source()
         │      NOTE: news articles do NOT geocode — location comes from SourcesManagement only
         │
         └─ _filter_location      — drop articles not matching country_id
                checks location_ids first (any geoid prefix match), falls back to author_location_id
```

**Defaults:** region `MX:es-419`, timeframe `1d`, max 30 articles, URL decoding enabled.

## Instagram hashtag pipeline

Entry point: `InstagramHashtagActor.search(search_params)`

Scrapes hashtag results via Apify actor `reGe1ST3OBgYZSsZJ`, translates raw results to the intermediate schema via `InstagramPost.from_instagram()`. The pipeline includes:

- **Content enrichment** — `fetch_attached_url` loops over posts calling `doc.fetch_attached_url(self.sources_manager)` directly (`Post.fetch_attached_url()` in `src/models/post.py`) — fetches the linked article, appends its text, and stores the `News` object on `post.attached_news`. Plus platform-specific stubs for `download_images`, `download_video`, `add_text_from_images`, `add_subtitles`, `add_ai_transcription`
- **Location enrichment** — geocodes post body text via `Post.enrich_location()`. Caches user locations in `UsersManagement` — on subsequent runs, known user locations are applied without re-geocoding
- **User author enrichment** — when `enrich_followers=true`, bulk-scrapes unique author profiles via `apify/instagram-profile-scraper`. Maps `followersCount` → `website_visits`, `fullName` → `author_full_name`, `biography` → `author_profile_bio`. Skips profiles with stats fresher than `stats_max_age_days` (default 90). Results cached in `UsersManagement`
- **Comment enrichment** — when `get_comments=true`, bulk-scrapes comments via `apify/instagram-comment-scraper`

Final normalization and RabbitMQ publishing follow the same pattern as news.


# Data ingest — actors (`src/actors/`)

Each Apify actor lives in its own class. Actor classes inherit from `ApifyActor` (`src/actors/actor.py`) and implement:

| Method | Input | Output |
|---|---|---|
| `search(search_params, **kwargs)` | list of search terms | `List[Document]` (e.g. `List[News]`) |

The base class provides `process_documents(docs, **kwargs)` — a staged pipeline (keyword filter → date filter → content enrichment → language filter → location enrichment → location filter → LLM filter → follower enrichment → comment enrichment). Subclasses call it after creating Documents and override individual stages. The pipeline supports a per-document per-task filter cache (`cache/filter_cache.json`) and an `override_filters` flag to force re-evaluation.

Current actors:
- **`news/news_scraper.py`** — `GoogleNewsActor` (actor `3Z6SK7F2WoPU3t2sg`)
- **`instagram/hashtags.py`** — `InstagramHashtagActor` (actor `reGe1ST3OBgYZSsZJ`)
- **`instagram/profile_posts.py`** — `InstagramProfilePostsActor` (actor `shu8hvrXbJbY3Eb9W`). Scrapes posts from profile URLs, downloads videos via `igview-owner/instagram-video-downloader` with URL-hash caching to `cache/media/instagram/`
- **`facebook/posts.py`** — `FacebookPagePostsActor` (actor `apify/facebook-posts-scraper`)
- **`facebook/profiles.py`** — `FacebookProfileActor`, utility class wrapping `apify/facebook-pages-scraper`. Used internally by `FacebookPagePostsActor` for author enrichment. Not in the actor registry
- **`facebook/comments.py`** — `FacebookCommentsActor`, utility class wrapping `apify/facebook-comments-scraper`. Used internally by `FacebookPagePostsActor` for comment enrichment. Not in the actor registry


# Article parsing (`src/models/news_parser/`)

Used by `News.fetch_and_parse()` to download and extract full article content.

- **`load_url.py`** — `fetch_html(url)`: streaming HTTP download, 10 MB size guard, 15s timeout, 3 retries, browser UA headers. Returns a tuple `(html, final_url)` where `final_url` is the URL after following redirects (`r.url` from requests). Returns `(None, None)` on failure.
- **`parser.py`** — `extract_article(html, url)`: three-tier extraction (NewsPlease → newspaper4k → LLM). Returns `{title, body, author, media_urls, timestamp}` or `None`. `timestamp` comes from NewsPlease's `date_publish`, newspaper's `publish_date`, or the LLM's `published_at` (ISO 8601). Requires body >= 200 chars.

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


# Tests

Tests live in `src/tests/` and use pytest. Test cache fixtures (real Apify responses) are in `src/tests/cache/`.

```bash
python -m pytest src/tests/ -v
```

## Test modules

| Module | Coverage |
|---|---|
| `test_crawl_task.py` | CrawlTask parsing: task_id, period, get_comments, max_comments, not_keywords, llm_filter_condition, override_filters, mutual exclusivity, to_actor_kwargs |
| `test_document.py` | Document._empty_data fields, matches_min_date |
| `test_google_news_actor.py` | GoogleNewsActor: document creation, Apify params, date/language/period filters, final schema, get_comments no-op |
| `test_instagram_actor.py` | InstagramHashtagActor: document creation, Apify params, date/period filters, comment enrichment, final schema with/without comments |
| `test_facebook_actor.py` | FacebookPagePostsActor: FacebookPost.from_facebook mapping (photo/video/OCR/comments), search params, keyword filtering, _add_text_from_images, final schema; FacebookProfileActor: map_profile field mapping (likes → website_visits, title → author_full_name, info → author_profile_bio, address → author_location_text); _enrich_user_author pipeline (apply cached → scrape stale → save); FacebookCommentsActor: map_comment field mapping, group_by_post_url grouping/error-skipping/fallback; _enrich_comments pipeline (scrape → map → assign to posts) |
| `test_social_enrichment.py` | Social enrichment: URL extraction, fetch_attached_url (article text append, location copy, no-overwrite), platform-specific NotImplementedError stubs |
| `test_llm_filter.py` | LLM filtering: snippet building (keyword context vs first chars), batching, cache integration, override_filters, per-task filter cache |
| `test_users_management.py` | UsersManagement: save/get stats and location, needs_stats_update staleness, persistence to disk |
| `test_schema.py` | NEWS_SCHEMA normalization: comments list, empty list, None handling, expected fields |

## Test data

Cache fixtures in `src/tests/cache/` are generated by running actual Apify calls with small limits. They are committed to the repo (the `.gitignore` has a negation for `src/tests/cache/`). MongoDB is mocked at import time via `conftest.py`.


# TODO

- **`SourcesManagement` Mongo create/update** — `SourcesManagement` currently reads sources from MongoDB via `src/helpers/sources.py` but has no ability to create or update source records in Mongo. Add methods to create new sources and update existing ones directly through `SourcesManagement`, so that unknown sources can be promoted to known sources without manual DB intervention.
- **`UsersManagement` MongoDB backing** — `UsersManagement` is currently file-based (`cache/users.json`). Add a MongoDB collection for persistent, shared storage across instances.
- **Redis filter cache** — Replace the file-based filter cache (`cache/filter_cache.json`) with Redis for multi-process/distributed support.
- **Platform enrichment stubs** — Implement `download_images`, `download_video`, `add_text_from_images` (Instagram), `add_subtitles`, `add_ai_transcription` for each social platform via platform-specific Apify actors.
