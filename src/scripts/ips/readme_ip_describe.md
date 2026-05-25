# IP-analysis pipeline for Instagram videos

## Goal

Take Instagram posts (mostly Reels / single videos) scraped via the Apify Instagram-profile actors and produce a structured "IP analysis" record per post: ~70 attributes covering caption metadata, context, characters, plot, drawing/visual style, messaging/text overlays, and comment insights.

The end record matches the `InstagramPostAnalysis` schema defined in `schema_tools` (separate repo: `/Users/oscarcuellar/ocn/media/schema_tools`), so downstream consumers get a typed, validated payload regardless of which section the LLM is filling.

## Two scripts cooperate

### `run_instagram_profile_posts.py` — scrape + drive the analysis

1. Picks an actor:
   - `"default"` → `InstagramProfilePostsActor` (Apify `shu8hvrXbJbY3Eb9W`; takes profile / post URLs)
   - `"queenlike"` → `InstagramProfileQueenlikeActor` (`queenlike_xystos/instagram-posts-reels-scraper---no-cookies`; takes bare usernames, also returns `reshare_count` and author `follower_count` so no extra profile scrape)
2. Calls `actor.search(search_params, **kwargs)` which runs the full `ApifyActor` pipeline (`filter_keywords` → `filter_date` → `enrich_content` → `filter_language` → `enrich_user_author` → `enrich_location` → `filter_location` → `filter_llm` → `enrich_comments`). With `download_video=True` (default in this script) each Video/Reel is downloaded to `cache/media/instagram/<sha>.<ext>` and the local path is stored on `doc.data["video_filename"]`.
3. Saves the scraped, normalized posts to `cache/runs/instagram_profile_posts_id_<task_id>_<ts>.json` (or publishes to RabbitMQ if `publish=True`).
4. For each scraped doc, calls `analyse_post(gemini_client, doc.data)` from `ai_describe`. Results are saved as JSON (`instagram_profile_posts_analysis_<task_id>_<ts>.json`) and flattened to Excel for human review (`~/Downloads/instagram_profile_posts_analysis_<task_id>_<ts>.xlsx`).

### `ai_describe.py` — the section-by-section LLM extraction

1. Loads the question catalogue from `resources/ip_fields.xlsx` (sheet `Fields`): each row has a question number, the question text, allowed responses, datatype (`Catalogue`, `Multi-catalogue`, `Extensible …`, `Boolean`, `List[…]`), and the target JSON field name.
2. The xlsx is treated as a **human-facing reference only**. The section→question grouping and question→json-field mapping live in this module (constants `SECTIONS` and `FIELD_FOR_Q`) so reorganising the xlsx never breaks the code. Meta-derived rows (Q1, Q5, Q7, Q10, Q11, Q13, Q68) and out-of-scope rows (Q3, Q4, Q44, Q72) are excluded on purpose — they are filled by `fill_meta_fields()` from the Apify-scraped Document data (timestamp, post_type, hashtag_count, mention_count, caption_word_count, emoji_pct, likes, views, shares, comment_count).
3. Uploads the video to Gemini (`gemini-2.5-pro`) exactly once per content sha256 via `upload_video_cached()` and waits for the file to become `ACTIVE`.
4. For each of the 7 logical sections — `post_info`, `context`, `character`, `plot`, `drawing`, `messaging`, `comments` — builds a focused prompt (`build_section_prompt`) and calls `client.models.generate_content` with `response_mime_type="application/json"` and the section's Pydantic model as `response_schema`. Gemini honors `Literal[...]` enums and required-field shapes, so each call returns a JSON object guaranteed to match its section model. Per-section failures are caught and stored as `{"_error": str(exc)}` so one bad section doesn't kill the whole post.
5. `build_record()` merges all section outputs + meta fields into the `InstagramPostAnalysis` shape (the `character` section is split into top-level fields and a `characters[]` list) and runs the result through `schema_tools.normalize_record(record, "InstagramPostAnalysis")` for final typing/validation.

## Why split into sections (rather than one big call)?

- Smaller, focused prompts give more consistent answers on closed enums and reduce hallucination across unrelated dimensions.
- One transient failure isolates to one section instead of nuking the whole record.
- Each section's Pydantic model is small enough that Gemini reliably honors the schema (Literal enums in particular).

## Why the xlsx is reference-only

The xlsx is meant to be edited by humans (questions can be reworded, rows reordered, response lists tweaked). Keeping the section grouping and field map in code means none of that editing breaks the pipeline.

## Data flow (single post)

```
Apify result
  → InstagramPost.from_instagram(_queenlike)(item)
  → InstagramProfilePostsActor.process_documents([...])
       ├─ filters (keywords, date, language, location, optional LLM)
       ├─ enrich_content
       │    ├─ fetch_attached_url (if enabled)
       │    └─ _download_video → cache/media/instagram/<hash>.<ext>
       │                          → doc.data["video_filename"]
       ├─ enrich_user_author (cached stats; profile scrape if enabled)
       ├─ enrich_location (geocode body or user cache)
       └─ enrich_comments (if get_comments=True)
  → doc.data is now the intermediate-schema dict
  → analyse_post(client, doc.data):
       ├─ upload_video_cached(video_filename)        (Gemini Files API)
       ├─ describe_section(...) × 7                  (one Gemini call each)
       ├─ fill_meta_fields(doc.data, ai_results)
       └─ normalize_record(merged, "InstagramPostAnalysis")
  → record (typed dict)
```

## Inputs / outputs at a glance

**Inputs**

- `search_params`: profile URLs / post URLs (default actor) or usernames (queenlike actor). Configured in `run_instagram_profile_posts.py`.
- `resources/ip_fields.xlsx`: question catalogue.
- Environment: `APIFY_TOKEN`, `GEMINI_API_KEY`, plus the usual Mongo / RabbitMQ / OpenRouter vars consumed by the main client.

**Outputs**

- `cache/media/instagram/<sha>.<ext>` — downloaded videos (sha-cached, so re-runs don't re-download).
- `cache/runs/instagram_profile_posts_id_<task_id>_<ts>.json` — scraped + filtered posts in final schema.
- `cache/runs/instagram_profile_posts_analysis_<task_id>_<ts>.json` — IP-analysis records.
- `~/Downloads/instagram_profile_posts_analysis_<task_id>_<ts>.xlsx` — flattened analysis for human review (one row per post; nested dicts become dotted keys; lists of dicts get numbered subkeys like `characters.1.outlook`; scalar lists are joined with ` | `).

## Key configuration knobs (`run_instagram_profile_posts.py`)

| Knob | Description |
|---|---|
| `actor_choice` | `"default"` or `"queenlike"` |
| `search_params_*` | URLs (default) or usernames (queenlike) |
| `task_id` | filter-cache key |
| `download_video` | must be `True` to feed `analyse_post` (Gemini needs the local mp4) |
| `enrich_followers` | gates the extra profile-scraper call |
| `get_comments` | enables comment scraping (per platform) |
| `publish` | `True` → RabbitMQ; `False` → save JSON locally |
| Gemini model | `GEMINI_MODEL = "gemini-2.5-pro"` in `ai_describe` |

## Key constants (`ai_describe.py`)

| Constant | Purpose |
|---|---|
| `SECTIONS` | section name → list of question numbers |
| `FIELD_FOR_Q` | question number → target JSON field (`characters[].<field>` for per-character fields, comma-separated for composite questions like Q45) |
| `SECTION_MODEL` | section name → Pydantic response model (used as Gemini `response_schema`) |
| `APIFY_POST_TYPE_TO_FORMAT` | Apify `post_type` → schema_tools `post_format` |

## Notes & gotchas

- `analyse_post` returns `None` (and logs a warning) when `doc.data["video_filename"]` is missing or the file doesn't exist on disk — i.e. it silently skips Image / Sidecar posts in the current pipeline because only Video / Reel posts trigger `_download_video`. If you want to analyse static posts, you need a different ingest path for Gemini (image upload) — not implemented yet.
- The Gemini Files API expires uploads after ~48h; `upload_video_cached` re-uploads transparently if `client.files.get(name=...)` fails.
- The pipeline relies on `schema_tools` being importable. The runner script inserts `/Users/oscarcuellar/ocn/media/schema_tools/src` onto `sys.path` at runtime, so make sure that repo is checked out alongside this one.
- `ai_describe.py` is intentionally **not** wired into `process_documents()` — it runs as a post-step in the runner script so it stays opt-in and cheap to comment out when iterating on scraping.

## TODOs surfaced in the code

- Merge some sections into single Gemini calls (fewer calls, but watch for consistency regressions).
- Push "proposed" values from extensible catalogues back into the xlsx / a Mongo catalogue so the universe of allowed values grows over time.
