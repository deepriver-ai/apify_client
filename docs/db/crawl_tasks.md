# Crawl tasks in userdb — design (draft)

Status: **drafted 2026-10-05 for Oscar's review; since implemented.** The crawl tables are in
`media-backend-paid/db/user_db/schema.sql`, the queue worker is described in §6.1.1, and every
Apify call is metered through `usage_client` since 2026-10-07 (§6.1.1). The original DDL is in
[`media-backend-paid/docs/db/migrations/2026-10-05_crawl_tasks_userdb.sql`](../../../media-backend-paid/docs/db/migrations/2026-10-05_crawl_tasks_userdb.sql).
The text proposed for `media-backend-paid/docs/DATABASE_POSTGRES.md` is in
[Appendix A](#appendix-a--proposed-section-for-media-backend-paiddocsdatabase_postgresmd).

Decision record: `business_agent/docs/plans/sprint_2026-10_demos_access.md` §3 Line C
("Crawl tasks in the DB") and §8 item 5. Gap: `business_agent/docs/agents/agent_framework.md`
§5, §5.2 and §10 ("Crawl tasks: Excel → DB/API"). Customer plan: `business_agent/customers/sjdr/plan.md`
WS-7a ("queue tasks — unblocks recurrence everywhere").

---

## 1. Goals and non-goals

**Goals**
1. Replace `apify_client/tasks.xlsx` + `CURRENT_THEME` (`src/run_searches.py:20-32`)
   with userdb tables owned by `media-backend-paid` (schema-first; workspace `CLAUDE.md`
   §Cross-cutting conventions).
2. Every task and every run belongs to an **org** (`org_id NOT NULL`). Tasks can also
   be linked to saved searches (`query_id`, one search → N tasks; agent_framework §5).
3. Runs start from a **backend request**: endpoint → RabbitMQ (`rabbit_enqueuer`) →
   `apify_client` worker. A scheduler handles recurring tasks (agent_framework §5.2).
4. Each run records **what it paid for and what it got**: `items_paid / items_new /
   items_already_seen / …` and cost. That gives the waste rate and the posting rate
   (agent_framework §5.1). Each Apify run also writes its cost to the `usage_events`
   ledger through `usage_client.apify.run` (`rate_limiting.md` §2.7; §6.1.1).
5. Hooks for **pre-run range estimates** and **caps**: admission plus `maxItems`
   capped to the remaining budget (`rate_limiting.md` §2.5 admission paths, §2.6–§2.7
   `usage_client.apify`).
6. Comments-on-demand as a first-class request: post selection, an exact estimate,
   then purchase (agent_framework §5.3; `src/scripts/select_posts_for_comments.py`,
   `src/scripts/run_comment_tasks.py`).

**Non-goals**
- The metering tables themselves (`usage_events`, `usage_limits`, `usage_blocks`) and
  the `usage_client` library. Those are specified in `rate_limiting.md` §2.2 and §2.6; this
  doc only references them.
- Changing actor behavior, the `process_documents` pipeline, or the gp3 contract.
- The WS-4 `classify_users` pass. Today it runs after the task loop, aggregated per
  theme (`run_searches.py:113-152`); where it runs in the new design is open (Q9).
- Engagement refresh policy (agent_framework §5.4). `update_existing` stays a per-task
  flag.
- The pfbid canonical-URL dedupe (`documentation_gaps.md` #10). It is a separate
  ingestion fix. Until it lands, it inflates `items_new` (§7.3).

---

## 2. Entities

Prefixes: `crawl_*`. IDs follow userdb conventions (`DATABASE_POSTGRES.md` "Surrogate-key
convention"): an `integer`/`bigint GENERATED ALWAYS AS IDENTITY` surrogate key, natural
keys as `UNIQUE`, FKs to `orgs(id)` / `users(id)` / `user_searches(query_id)` as in
`stance_assignments` / `entities_documents_sentiments_org`, and `jsonb`, as in the
recent tags tables (`stance_entries.aliases`). The older `json` columns
(`user_searches.query_json`, `tasks_run`) are not the model.

```
orgs ─┬─< crawl_tasks >─ crawl_actor_classes          user_searches
      │        │  ├──1:1── crawl_schedules                   │
      │        │  └──<── crawl_task_searches >───────────────┘
      │        └──< crawl_runs ──< crawl_run_calls ──(event_id)──> usage_events (rate_limiting §2.2)
      └─< crawl_comment_requests ──< crawl_comment_request_posts ──> crawl_runs (kind='comments')
```

### 2.1 `crawl_actor_classes` (lookup, seeded)

One row per key in the actor registry (`src/actors/__init__.py:16-25`). It holds the
capability flags used by validation, scheduling and estimation. Prices are **not**
stored here; they belong to `usage_client.prices` (`rate_limiting.md` §2.4, §2.6).

| Column | Type | Notes |
|---|---|---|
| `actor_class` | `text` PK | registry key |
| `network` | `text NOT NULL` | `facebook` / `instagram` / `x` / `linkedin` / `tiktok` / `news` |
| `apify_actor_id` | `text NOT NULL` | the main actor, e.g. `scrapeforge/facebook-search-posts` |
| `run_kind` | `text NOT NULL` CHECK `crawl`/`comments` | `facebook_post_comments` = `comments` |
| `window_support` | `text NOT NULL` CHECK | `server_min_date` / `server_coarse` / `client_only` (§6.2) |
| `cap_scope` | `text NOT NULL` CHECK `per_call`/`per_param` | whether `max_results` applies once or per search param (§7.1) |
| `params_schema` | `jsonb NOT NULL` | allowed `actor_params` keys and types (§4) |
| `enabled` | `boolean NOT NULL DEFAULT true` | |

### 2.2 `crawl_tasks` (task definition)

Each row is one `tasks.xlsx` row. Column semantics are the same as in `CrawlTask`
(`src/models/crawl_task.py:16-45`).

| Column | Type / constraint | Notes |
|---|---|---|
| `task_id` | `integer` identity PK | new id space; legacy ids go in `legacy_task_id` |
| `org_id` | `integer NOT NULL` FK `orgs(id)` | replaces `theme` |
| `name` | `text NOT NULL DEFAULT ''` | xlsx `name` (not parsed by `CrawlTask` today) |
| `actor_class` | `text NOT NULL` FK `crawl_actor_classes` | |
| `search_params` | `jsonb NOT NULL`, non-empty array of strings | xlsx comma-separated (`crawl_task.py:50`) |
| `country_id` | `text NULL` | geoid prefix, e.g. `_484` |
| `language` | `text NULL` | normalized via `normalize_language()` |
| `max_results` | `integer NOT NULL DEFAULT 30 CHECK > 0` | hard ceiling. Adaptive caps go *under* it (§6.3) |
| `min_date` | `timestamptz NULL` | |
| `period` | `text NULL CHECK IN ('d','w','m')` | `CHECK (min_date IS NULL OR period IS NULL)` (`crawl_task.py:77-80`) |
| `publish` | `boolean NOT NULL DEFAULT true` | false → dry run (§5.4) |
| `get_comments` | `boolean NOT NULL DEFAULT false` | |
| `max_comments` | `integer NOT NULL DEFAULT 15 CHECK >= 0` | |
| `not_keywords` | `jsonb NOT NULL DEFAULT '[]'` | xlsx pipe-separated (`crawl_task.py:82-83`) |
| `llm_filter_condition` | `text NULL` | Spanish condition |
| `update_existing` | `boolean NOT NULL DEFAULT false` | skips `_filter_existing_in_elasticsearch` |
| `enrich_followers` | `boolean NOT NULL DEFAULT false` | gates the paid profile scrape |
| `fetch_attached_url` | `boolean NOT NULL DEFAULT false` | `CLAUDE.md` §Task System warning |
| `classify_users`, `official_page` | `boolean NOT NULL DEFAULT false` | WS-4 opt-ins |
| `actor_params` | `jsonb NOT NULL DEFAULT '{}'`, object | validated against `params_schema` (§4) |
| `max_cost_usd_per_run` | `numeric(10,4) NULL` | per-task breaker (agent_framework §5.1) |
| `status` | `text NOT NULL DEFAULT 'draft'` CHECK | §5.1 |
| `status_reason` | `text NULL` | e.g. "search 229 untracked" |
| `legacy_theme`, `legacy_task_id` | `text NULL` | import provenance. `legacy_task_id` is partial `UNIQUE` |
| `metadata` | `jsonb NOT NULL DEFAULT '{}'` | xlsx `followers`, free notes |
| `created_by` | `integer NULL` FK `users(id)` | |
| `created_at`, `updated_at` | `timestamptz NOT NULL DEFAULT now()` | |
| `retired_at` | `timestamptz NULL` | |

Indexes:
- `(org_id, status)`.
- Partial `UNIQUE (org_id, actor_class, search_params) WHERE status <> 'retired'`. This
  is the same dedupe key `customer_ops` uses today, `(theme, actor_class, search_params)`
  in `customer_ops/src/customer_ops/crawl_tasks.py:44-45`, with `theme` replaced by
  `org_id`.

`override_filters` is **not** a task column. It is a per-run option (§3) because it
means "re-run the filters this once" (`AGENTS.md` §Task System).

**Filter-cache key.** The actor cache key is `filtered:{task_id}:{url}`
(`src/actors/actor.py:89-92`). The worker passes `task_id = legacy_task_id or
f"db:{task_id}"`, so imported tasks keep their cached decisions.

### 2.3 `crawl_task_searches` (task ↔ search link, optional)

The relation is N:M. One search maps to N tasks (agent_framework §5), and one page
crawl can feed several searches (e.g. municipio and alcalde).

| Column | Type | Notes |
|---|---|---|
| `row_id` | `integer` identity PK | |
| `task_id` | FK `crawl_tasks` `ON DELETE CASCADE` | |
| `query_id` | FK `user_searches(query_id)` `ON DELETE CASCADE` | |
| `created_at` | `timestamptz NOT NULL DEFAULT now()` | |

Constraints:
- `UNIQUE (task_id, query_id)`, plus an index on `query_id`.
- Same-org rule (`crawl_tasks.org_id = user_searches.org_id`) is enforced in the
  endpoint, not by an FK.
- **Teardown**: when a search is deleted or untracked, every task with no remaining
  active link goes to `paused` with a `status_reason` (agent_framework §5 "teardown
  cascades").

### 2.4 `crawl_schedules` (recurrence, 1:1 optional)

This is a separate table, not columns on the task row, so the scheduler scans a small
partial index. A task with no row is manual-only.

| Column | Type | Notes |
|---|---|---|
| `task_id` | `integer` PK, FK `crawl_tasks` `ON DELETE CASCADE` | |
| `active` | `boolean NOT NULL DEFAULT true` | |
| `every` | `interval NOT NULL CHECK >= '1 hour'` | frequency |
| `window_overlap` | `interval NOT NULL DEFAULT '1 day'` | lookback past the last success (§6.2) |
| `next_run_at` | `timestamptz NOT NULL` | |
| `last_run_id` | `bigint NULL` FK `crawl_runs` | |
| `last_success_at` | `timestamptz NULL` | |
| `adaptive_cap` | `boolean NOT NULL DEFAULT false` | §6.3 |
| `cap_floor` | `integer NULL` | lowest adaptive cap (probe size) |
| `target_overlap_rate` | `numeric(4,3) NOT NULL DEFAULT 0.300` | §6.3 |
| `consecutive_failures` | `integer NOT NULL DEFAULT 0` | auto-pause after N |
| `paused_reason` | `text NULL` | |
| `updated_at` | `timestamptz NOT NULL DEFAULT now()` | |

Index: `(next_run_at) WHERE active`.

### 2.5 `crawl_runs` (one execution, with stats and cost)

The per-run stats sit on the run row (1:1). The per-provider-call costs go in
`crawl_run_calls` (§2.6), because one task run makes several Apify calls: a keyword
search loops per param (`facebook/keyword_search.py:46`), plus profile scrapes, comment
scrapes and video downloads.

| Column | Type | Notes |
|---|---|---|
| `run_id` | `bigint` identity PK | |
| `kind` | `text NOT NULL` CHECK `crawl`/`comments`/`adhoc`/`batch` | `adhoc` (§6.4) pending migration `2026-10-07_crawl_runs_adhoc_userdb.sql`; `batch` (§6.5) pending `2026-10-13_crawl_batch_runs_userdb.sql` |
| `task_id` | FK `crawl_tasks NULL` | set when `kind='crawl'`; NULL for `comments`, `adhoc` and `batch` (members in `crawl_run_tasks`, §6.5) |
| `comment_request_id` | FK `crawl_comment_requests NULL` | set when `kind='comments'`. A CHECK ties each kind to its FK |
| `org_id` | `integer NOT NULL` FK `orgs` | attribution (`rate_limiting.md` §2.3) |
| `query_id` | FK `user_searches NULL ON DELETE SET NULL` | → `usage_events.search_id` |
| `trigger` | `text NOT NULL` CHECK `api`/`schedule`/`cli` | |
| `requested_by` | FK `users NULL` | |
| `status` | `text NOT NULL DEFAULT 'requested'` CHECK | §5.2 |
| `params` | `jsonb NOT NULL` | resolved kwargs snapshot (`to_actor_kwargs()` + run window). Reproducible |
| `window_since`, `window_until` | `timestamptz NULL` | §6.2 |
| `max_items` | `integer NOT NULL` | effective cap after admission (≤ task `max_results`) |
| `estimate_usd_low`, `estimate_usd_high` | `numeric(12,6)` | §7 |
| `admitted_usd` | `numeric(12,6) NULL` | `Admission.allowed_usd` |
| `block_id` | `bigint NULL` | `usage_blocks.block_id` when status is `blocked`. Not filled yet by the backend or the worker (usage_client doesn't return the block id; known gap, `rate_limiting.md` §3.5) |
| `dry_run` | `boolean NOT NULL DEFAULT false` | |
| `trace_id` | `text NOT NULL` | `dev/trace.sh` |
| `requested_at`, `queued_at`, `started_at`, `heartbeat_at`, `finished_at` | `timestamptz` | |
| `worker`, `error`, `output_path` | `text NULL` | `output_path` for dry runs |
| `items_paid` | `integer` | raw dataset items across *search* calls |
| `items_cached_filtered` | `integer` | filter reason `cached` |
| `items_dropped_keyword`, `items_dropped_date` | `integer` | reasons `keyword`, `date` |
| `items_already_seen` | `integer` | reason `existing_elasticsearch` |
| `items_dropped_language`, `items_dropped_location`, `items_dropped_llm` | `integer` | LLM needs new counting (§7.3) |
| `items_new` | `integer` | docs published (excluding attached articles) |
| `attached_news_published`, `comments_fetched`, `profiles_scraped` | `integer` | |
| `newest_item_at`, `oldest_item_at` | `timestamptz` | posting rate = paid items / span |
| `cost_usd` | `numeric(12,6)` | Σ `crawl_run_calls.cost_usd` |
| `overlap_rate` | generated: `items_already_seen / NULLIF(items_paid,0)` | §6.3 |
| `yield_rate` | generated: `items_new / NULLIF(items_paid,0)` | waste = 1 − yield |
| `stats` | `jsonb NOT NULL DEFAULT '{}'` | anything else, e.g. per-param counts |

Indexes:
- `(task_id, requested_at DESC)` and `(org_id, requested_at DESC)`.
- Partial `UNIQUE (task_id) WHERE status IN ('queued','running')`: one live run per
  task, so a schedule tick and a manual request can't double-buy.

### 2.6 `crawl_run_calls` (per Apify call cost → ledger)

| Column | Type | Notes |
|---|---|---|
| `call_id` | `bigint` identity PK | |
| `run_id` | FK `crawl_runs ON DELETE CASCADE` | |
| `purpose` | `text NOT NULL` CHECK | `search` / `profiles` / `comments` / `video` / `other` |
| `apify_actor_id` | `text NOT NULL` | |
| `apify_run_id` | `text NOT NULL UNIQUE` | idempotency on redelivery |
| `apify_status` | `text` | |
| `items` | `integer` | dataset items |
| `cost_usd` | `numeric(12,6) NULL` | Apify run `usageTotalUsd` **once settled** (§7.5); our own computed figure while not |
| `estimated` | `boolean NOT NULL DEFAULT false` | true while the cost is our own figure (Apify's charges not settled in time) or a list-price fallback; `reconcile_costs` sets it false |
| `usage` | `jsonb NULL` | Apify `usage` object (platform-usage actors) + pay-per-event `chargedEventCounts` / `accountedChargedEventCounts` / `eventUsage` + `settled` / `computed_usd` |
| `usage_event_id` | `text NULL` | `usage_events.event_id`. No FK, so this table doesn't depend on P0.3 ordering |
| `started_at`, `finished_at` | `timestamptz` | |

Today `ApifyActor.run_actor()` throws away the run object (`actor.py:79-85`). The
utility actors (`FacebookProfileActor`, `FacebookCommentsActor`, …) call
`ApifyClient` directly. Capturing every call is the `usage_client.apify.run()` wrapper
(`rate_limiting.md` §2.6–§2.7): the worker's `RecordingApifyClient` routes every
`actor(id).call()` through it and stores the returned `usage_event_id` (§6.1.1).

### 2.7 `crawl_comment_requests` + `crawl_comment_request_posts` (comments-on-demand)

These persist what `select_posts_for_comments.py` writes to `cache/comment_tasks/*.json`
today: tiers, depth and `cost_estimate`. They also persist what
`run_comment_tasks.py` executes: one `facebook_post_comments` run per tier, because the
actor's `resultsLimit` is global per call (`run_comment_tasks.py:1-10`).

`crawl_comment_requests`:

| Column | Type | Notes |
|---|---|---|
| `request_id` | `integer` identity PK | |
| `org_id` | FK `orgs NOT NULL` | |
| `query_id` | FK `user_searches NULL` | |
| `source_task_id` | FK `crawl_tasks NULL` | |
| `requested_by` | FK `users NULL` | |
| `selector` | `text NOT NULL` CHECK `producer`/`rule`/`hand` | §5.3 tiering, or a human's pick |
| `selector_params` | `jsonb NOT NULL` | producer args (`--pages/--phrases/--days/--deep/--sample/…`, `PRODUCER_VERSION`) |
| `status` | `text NOT NULL DEFAULT 'proposed'` CHECK | §5.3 |
| `posts_count`, `comments_expected` | `integer` | |
| `estimate_usd` | `numeric(12,6) NOT NULL` | exact (§7.2) |
| `approved_by` | FK `users NULL` | |
| `approved_at`, `created_at`, `finished_at` | `timestamptz` | |

`crawl_comment_request_posts`:

| Column | Type | Notes |
|---|---|---|
| `row_id` | `bigint` identity PK | |
| `request_id` | FK `ON DELETE CASCADE` | |
| `post_url` | `text NOT NULL` | **exact ES `_id`, never normalized**; gp3 merges by it (`CLAUDE.md` `facebook/post_comments.py`) |
| `tier` | `text NOT NULL` CHECK `deep`/`sample`/`custom` | |
| `depth` | `integer NOT NULL CHECK > 0` | |
| `n_comments_declared`, `stored_comments`, `comments_expected`, `comments_fetched` | `integer` | |
| `signals` | `jsonb` | place / theme / official / traction |
| `run_id` | FK `crawl_runs NULL` | |

Constraints: `UNIQUE (request_id, post_url)`, plus an index on `post_url` (per-post
coverage for the availability manifest, agent_framework §7.5).

---

### 2.8 Review decisions (Oscar, 2026-10-06) — folded into the migration

Smoke-tested on the local userdb (14 checks, rolled back); **applied to stg 2026-10-06** with the RBAC rows and the worker role, and folded into `media-backend-paid/db/user_db/schema.sql`.

| # | Decision | Where |
|---|---|---|
| R1 | `max_results` hard ceiling **1000** per task (`CHECK`), whatever the caller; budgets/admission sit on top | `crawl_tasks_max_results_chk` |
| R2 | A task may have **no time window** (`min_date` and `period` both NULL): the worker then sends **no** date filter to the actor; `max_results` is the only bound. `min_date` XOR `period` still holds | §6.2; `crawl_tasks_min_date_xor_period_chk` |
| R3 | Dedupe is **order-insensitive** on `search_params` (params run as independent queries): unique index over `crawl_params_key(search_params)` (sorted jsonb array); the backend also sorts on write | `crawl_tasks_live_scope_uniq` |
| R4 | **Org consistency in the DB**, not only in endpoints: triggers check task ↔ search (`crawl_task_searches`), run ↔ task / comment request / search (`crawl_runs`), comment request ↔ source task / search. No change to `user_searches`. Endpoints keep the check + tests | `crawl_*_same_org` triggers |
| R5 | Several `requested` runs may exist for one task; the `requested → queued` transition hits `crawl_runs_one_live_per_task` for all but one — the caller catches the unique violation and sets that run `cancelled` (`error = 'duplicate live run'`) | §5.2, §6.1 |
| R6 | `updated_at` maintained by trigger on `crawl_tasks` / `crawl_schedules`; a `comments` run must have `task_id IS NULL`; a comment request in `approved`/`running`/`done` must carry `approved_by` + `approved_at`; Instagram `cap_scope = per_param` verified (resultsLimit per hashtag/URL, queenlike one call per username) | constraints in the migration |

Note for Q5 (worker role): the org triggers run as the invoking role, so the worker role
needs `SELECT` on `crawl_tasks`, `crawl_comment_requests` and `user_searches` besides its DML.

## 3. `tasks.xlsx` column mapping

The live header (2026-10-05) has 24 columns and 185 rows. `task_id` is filled on every
row, but it has duplicates `35`, `36` and `37` (two rows each) and mixed int/str values
(`34.a`).

| xlsx column | Parsed by `CrawlTask`? | → DB |
|---|---|---|
| `task_id` | yes (`crawl_task.py:87-90`) | `crawl_tasks.legacy_task_id` (text). New `task_id` is identity. Duplicates: §9 step 2 |
| `actor_class` | yes | `actor_class` (FK). `GROUPS` (3 rows, all disabled, not in the registry) → not imported |
| `theme` | yes, lowercased | `org_id` via §9.1, plus `legacy_theme` |
| `name` | **no** | `name` |
| `search_params` | yes, comma-split | `search_params` jsonb array |
| `followers` | **no** | `metadata.followers` |
| `country_id`, `language`, `max_results`, `min_date`, `period` | yes | same name |
| `enabled` | yes (0/1/True/'TRUE' mixed) | `status`: true → `active`, false → `paused` (or `retired` for dropped themes, §9.1) |
| `publish` | yes | `publish` |
| `get_comments`, `max_comments` | yes | same |
| `not_keywords` | yes, pipe-split | jsonb array |
| `llm_filter_condition` | yes | same |
| `override_filters` | yes | **not stored**. It is a run option (`params.override_filters`). The global `OVERRIDE_FILTERED_CACHE` (`run_searches.py:37`) becomes the same run option |
| `enrich_followers`, `fetch_attached_url`, `update_existing`, `classify_users`, `official_page` | yes | same |
| `actor_params` | yes, JSON | `actor_params` jsonb, validated (§4) |
| (`CURRENT_THEME` constant/env) | — | replaced by `org_id` scoping on the request |

---

## 4. Actor-specific params

`actor_params` stays a free-form object merged last into the kwargs
(`to_actor_kwargs()`, `crawl_task.py:117-136`). That keeps new actor knobs out of the
schema. The defense is validation against `crawl_actor_classes.params_schema` in **two
places**: the backend endpoint on create/update, and the worker on consume (it rejects
the run as `failed` with the bad key). The schema is generated from the actor modules,
so `apify_client` remains the owner. The actor gotchas in `documentation_gaps.md` #9
become validation rules at save time:
- `google_news.timeframe` accepts only `1h/1d/7d/30d/1y/all`.
- A `twitter_keyword_search` param containing a space is an exact phrase. This is a
  warning, not an error.

Keys found in the live xlsx: `enrich_author_after_likes` (47 rows),
`get_comments_after_likes` (46), `keyword_search` (18), `timeframe` (7), `enrich` (7),
`topics` (4), `recent_posts` (1).

Keys each actor reads (`kwargs.get` in `src/actors/**`), excluding the common ones:

| actor_class | actor-specific keys |
|---|---|
| `facebook_keyword_search` | `enrich_author_after_likes`, `get_comments_after_likes`, `add_text_from_images`, `location_uid`, `recent_posts`, `snippet_max_len`, `stats_max_age_days`, `results_limit` |
| `facebook_page_posts` | `enrich_author_after_likes`, `get_comments_after_likes` (read since 2026-10-06; before, the page actor fetched comments/profiles for every post), `add_text_from_images`, `download_*`, `add_subtitles`, `add_ai_transcription`, `stats_max_age_days`, `results_limit` |
| `instagram_hashtags` / `instagram_profile_posts` | `keyword_search`, `results_type`, `stats_max_age_days`, `download_*`, `video_dir`, `results_limit` |
| `instagram_profile_queenlike` | `scrape_type`, `output_mode`, `results_limit` |
| `tiktok_posts` | `get_comments_after_likes`, `apify_input`, `results_limit` |
| `google_news` | `timeframe`, `topics`, `enrich`, `region_language`, `max_articles`, `decode_urls`, `extract_images`, `extract_descriptions` |
| `twitter_keyword_search`, `linkedin_keyword_search` | — (common keys only) |
| all (base pipeline) | `check_existing_elasticsearch`, `skip_existing_filter` (`actor.py`) |

**Thresholds that drive cost.** `get_comments_after_likes` and
`enrich_author_after_likes` gate the two paid enrichment calls (one shared gate,
`ApifyActor._gate_by_likes`: only posts whose `likes` **exceed** the threshold — strictly
greater — get the call; unset = every post; skipped posts land in the run funnel as
`comments_skipped_likes` / `profiles_skipped_likes`). They are inputs to the
estimate's high end (§7.1). `customer_ops` defaults are 100/100
(`customer_ops/src/customer_ops/crawl_tasks.py:18-27`). `apify_input` (TikTok) is a raw
passthrough to the actor input, so validation must reject any key in it that would
lift the cap (`resultsPerPage`, `maxItems`).

---

## 5. Lifecycle

### 5.1 Task status

`draft → active ⇄ paused → retired`

- `draft`: proposed by a customer or agent ("read and propose", `rate_limiting.md` §0;
  sprint §3 Line B "Propose" tools such as `plan_crawl`). Never runs. It becomes
  `active` when we approve it.
- `paused`: the old `enabled=false`, or an untracked linked search (§2.3). Repeated
  failures auto-pause the **schedule** (`crawl_schedules.active = false` +
  `paused_reason`, §6.2.2), not the task.
- `retired`: soft-delete (`retired_at`). Its runs stay queryable. This mirrors the
  soft-retire convention of `stance_entries`.

### 5.2 Run status

```
requested ─admit refused─> blocked              (terminal; usage_blocks row)
    │
    └─admit ok─> queued ─worker claims─> running ─> succeeded | partial | failed
                   │                        └─ heartbeat stale > N min ─> failed (reaper)
                   └─> cancelled
```

- `partial`: some Apify calls succeeded and their cost was recorded, but a later stage
  failed, the broker went away, or publishing broke broadly. The money is spent, so the
  stats and cost are still written. A few unpublishable documents don't make a run
  partial (2026-10-07): with the search call(s) succeeded and failed publishes ≤ max(1,
  10% of the documents to publish) the run is `succeeded`, with `stats.documents_failed`
  / `documents_failed_urls`. Attached articles that can't be serialized are dropped
  (`stats.attached_news_dropped`), never counted against their post.
- The worker claims a run with `UPDATE crawl_runs SET status='running' … WHERE run_id=$1
  AND status='queued' RETURNING`. A RabbitMQ redelivery of a run that is already
  `running` or terminal is acked and dropped.

### 5.3 Comment-request status

`proposed → approved → running → done`, or `proposed → rejected/cancelled`.
`approved` creates one `crawl_runs` row (`kind='comments'`) per non-empty tier.

### 5.4 `publish=false`

The run is created with `dry_run=true`. The worker saves results under `cache/runs/`
as it does today (`run_searches.py:88-102`) and records `output_path`. Apify cost is
still incurred and still metered.

---

## 6. Run flow and scheduling

### 6.1 Flow

```
POST /crawl/runs/request {task_id, options}        (or the scheduler, §6.2)
  backend: RBAC → org check → resolve params/window → estimate (§7)
         → admit_api(tool='apify', kind='crawl', est_usd=high)   (backend.helpers.usage → usage_client admit; partial allowed → admitted_usd)
         → INSERT crawl_runs(status='requested'→'queued' | 'blocked')
         → POST ENQUEUER_URL {"queue": CRAWL_RUNS_QUEUE, "message": {...}}   (pattern: event_report_helpers.py:198-240)
rabbit_enqueuer → queue crawl_runs
apify_client worker:
  claim run → for each Apify call: usage_client.apify.run(actor, input{maxItems≤max_items}, ctx)
            → INSERT crawl_run_calls + usage_events(tool='apify', kind='crawl'|'comments'|'profiles', unit='items',
                                                     cost_usd=usageTotalUsd,
                                                     event_id='apify-client:crawl-run:<run_id>:<org_id>:<kind>:<apify_run_id>')
            → process_documents() → publish_document() to the document queue (gp3)
            → UPDATE crawl_runs stats/cost/status; UPDATE crawl_schedules
```

Message shape (`trace_id` goes at the top level, per the workspace `CLAUDE.md`
§Local cross-service dev loop):

```json
{
  "type": "crawl_run",
  "run_id": 1234, "kind": "crawl", "task_id": 159, "comment_request_id": null,
  "org_id": 106, "user_id": 23, "query_id": 228,
  "trace_id": "crawl-1234-…",
  "max_items": 50, "max_cost_usd": 0.20, "dry_run": false,
  "params": { "actor_class": "facebook_keyword_search", "search_params": ["pinal de amoles"],
              "min_date": "2026-10-01T00:00:00-06:00", "period": null, "...": "to_actor_kwargs() snapshot",
              "override_filters": false }
}
```

Worker rules:
- No `org_id` → ack, error log, `usage_blocks(reason=missing_org,
  path=worker:apify-client)` (`rate_limiting.md` §2.3).
- If the cost write fails, the worker spools it and replays later. It never fails the
  run because of it (`rate_limiting.md` §2.5: workers record with spool).
- The DB row is the source of truth. `params` in the message is a convenience copy;
  the worker re-reads the row when they differ.

New wiring in `apify_client`:
- A queue consumer entry point next to `run_searches.py`.
- A userdb connection (`DATABASE_URI`; `psycopg` is not in `requirements.txt` today).
- Two RabbitMQ connections: one consuming runs on the user-logic vhost, one publishing
  documents to the document-processing vhost (Q4).
- Local runs use `document_processing_dev` + `dev_crawl_runs` (workspace `CLAUDE.md`
  §Environments).

#### 6.1.1 Worker as implemented (2026-10-06, `src/run_crawl_worker.py`, `src/worker/`)

Points the code had to settle. The message shape above is unchanged.
- **Missing org**: ack + error log + `usage_blocks(path='worker:apify-client',
  tool='apify', kind=<message kind>, reason='missing_org', search_id=<message query_id>)`
  through the `usage_client` ledger (best effort; grant in
  `2026-10-07_crawler_usage_grants_userdb.sql`), and the `queued` run is also set
  `failed` ("message missing org_id") so it does not hold the one-live-run slot.
- **Metering (2026-10-07)**: every Apify call goes through `usage_client.apify.run`, no
  flag. Context per call: `org_id` = `crawl_runs.org_id`, `user_id` = `requested_by`,
  `search_id` = `query_id`, `service='apify-client'`, `event_key='crawl-run:<run_id>'`,
  `kind` from the call's purpose (search → `crawl`, comments → `comments`, profiles →
  `profiles`, video/other → `crawl`). It admits each call against the org's
  `usage_limits`, passes Apify `max_items` / `max_total_charge_usd` = the smaller of the
  run's caps (below) and what was admitted, forwards the actor's own call kwargs, and
  records one `usage_events` row per Apify run; its `event_id` is stored in
  `crawl_run_calls.usage_event_id`. Ledger: `USAGE_DB_URI` (default `DATABASE_URI`),
  spool `USAGE_SPOOL_DIR` (default `cache/usage_spool`).
- **Admission refused in the worker** (`AdmissionRefused`: the org's budget was used up
  between the backend's admission and the run, or by the run's own earlier calls):

  | Refusal | No Apify call made yet | After a paid call |
  |---|---|---|
  | `over_limit` / `per_request_cap` | `blocked` (terminal, §5.2). The schedule is not touched (no failure counted, window not advanced), so the next tick retries: deferred | `partial` (cost kept) |
  | `ledger_unreachable` | run released `running → queued`, message requeued after `CRAWL_WORKER_DB_RETRY_SECS` (retry later); if the release fails → `failed` | `partial` |

  The reason goes to `error` and `stats.usage_refusal`; usage_client writes the
  `usage_blocks` row (path `worker:apify-client`). `crawl_runs.block_id` stays NULL:
  `usage_client` doesn't return the block id. Later calls of a refused run are refused
  locally (no second block row, no spend).
- **What wins**: the run row's `max_items` (≤ task `max_results`), `window_since`
  (replaces the task's `min_date`/`period`), `dry_run` and `params.override_filters`;
  everything else comes from the task row through `CrawlTask.from_db_row()` +
  `to_actor_kwargs()`. Message `params` are only diffed (`stats.message_params_diff`).
- **R2**: with no window, `google_news` gets `timeframe="all"` (its built-in default
  is `"1d"`) unless the task sets `timeframe` in `actor_params`. **With** a window
  (`window_since`, `min_date` or `period`) and no explicit `timeframe`, the actor
  derives the smallest bucket ≥ the window (`GoogleNewsActor._timeframe_for_window`,
  2026-10-07). Before that fix the window only fed the client-side date filter while
  Google News searched `1d`: task 172, runs 272/274.
- **Item cap**: the paid-item budget across `search` calls is
  `max_items × len(search_params)` for `cap_scope='per_param'`, else `max_items`. It is
  passed to Apify as `max_items` on each search call, and a spent budget stops further
  calls. Enrichment calls (profiles, comments) are not capped by this budget; they still
  carry the `max_items` usage_client admits (at most 1,000 when nothing tighter is known).
- **Cost cap**: min(message `max_cost_usd`, task `max_cost_usd_per_run`, run
  `admitted_usd`). It is passed as `max_total_charge_usd` (honoured by pay-per-event
  actors; usage_client lowers it to the whole items admitted), and further calls stop
  once it is spent or below one item's list price.
- **`crawl_run_calls.purpose`** comes from the pipeline stage the call runs in
  (`_enrich_user_author` → profiles, `_enrich_comments` → comments, `_download_video` →
  video, other enrichment → other, else search). `items` is the dataset `itemCount`.
  `items_paid` sums the search calls (comments calls for `kind='comments'`).
- **Dry runs** still fill `items_new` / `attached_news_published` with the
  would-be-published counts, so the §9.2 parallel run can compare yields.
- **Comments runs** read posts by `crawl_comment_request_posts.run_id`, so **the
  approve endpoint must set `run_id` on each post**. `max_comments` is the tier depth.
- **Schedules**: only `trigger='schedule'` runs update `crawl_schedules`, and only
  `last_run_id` / `last_success_at` (2026-10-06, branch `crawl-scheduler`: the worker no
  longer touches `consecutive_failures`, and skips the update when its run was reaped
  meanwhile — `finish_run` then reports the row was no longer `running`). `partial`
  counts as a failure, so the window does not advance. The failure response is the
  scheduler's (§6.2.2).
- **Not in the worker**: the stale-heartbeat reaper (§5.2, now in the poller, §6.2.1) and
  the WS-4 `classify_users` pass (Q9).

### 6.2 Scheduling and newest-first incrementality

The scheduler ticks every minute ("the poller pattern", agent_framework §5.2):

```sql
SELECT … FROM crawl_schedules s JOIN crawl_tasks t USING (task_id)
WHERE s.active AND t.status = 'active' AND s.next_run_at <= now()
FOR UPDATE OF s SKIP LOCKED
```

For each due task it runs the same estimate → admit → insert → enqueue path, then sets
`next_run_at += every`. The scheduler lives in the poller (Q3, decided 2026-10-06; see
§6.2.1).

Run window: `window_since = last_success.newest_item_at − window_overlap`, or
`now() − period` on the first run. Scheduled runs pass `min_date = window_since` and
`period = NULL`; the two are mutually exclusive.

#### 6.2.1 As implemented (2026-10-06, branch `crawl-scheduler` in three repos)

- **Where.** `search-sentiment-poller-tasker`, `runsearches/crawl_scheduler.py`, a thread
  of its own (`CRAWL_SCHEDULER_ENABLED=1`, default off; tick `CRAWL_SCHEDULER_TICK_SECONDS`
  = 60) so a long search pass never delays it. It connects to userdb as `backend` (UPDATE
  on `crawl_runs`, `crawl_schedules`, `crawl_comment_requests`). Each tick does three
  steps, in order: reap, settle, dispatch.
- **Dispatch goes through the backend.** The poller never inserts `crawl_runs` or
  enqueues. For each due schedule (the query above, one row per transaction) it calls
  `POST /crawl/runs/request {task_id, trigger: "schedule", trace_id: "sched-<task>-…"}`
  as a dedicated machine user (`service@deepriver.ai` on stg, member of every scheduling
  org, with a role/plan that has the `/crawl/*` RBAC rows). Login returns one JWT per
  org; tokens last 12 h, so the client re-logs in 30 min before `exp`, when an org is
  missing, and once on an auth failure. Estimate, admission (rate limits), the
  one-live-run rule, insert and enqueue all stay in the endpoint. Then
  `next_run_at += every`, repeated past missed slots (a poller that was down fires once,
  not once per slot), **whatever the outcome**: queued, blocked, duplicate or error.
- **Window (backend, `trigger: "schedule"`).** `window_since` = the newest `succeeded`
  crawl run of the task (any trigger) `.newest_item_at − window_overlap`. When that run
  found nothing, or the result would fall before that run's own `window_since`, its
  `window_since` is carried forward, so the window never moves backwards. With no success
  yet, the task's own window is used. A task `min_date` stays a floor. `failed` /
  `partial` / `blocked` / `cancelled` runs never move it, so a retry re-covers everything
  since the last success. The endpoint refuses `trigger: "schedule"` for a task with no
  schedule row or a paused one (409), and admits it exactly like an `api` run (endpoint `/crawl/runs/request`, so the
  same per-request limits apply). `trigger: "schedule"` is open to every org role that may launch runs
  (Oscar 2026-10-07): attribution is by `org_id` + `requested_by` anyway.
- **Reaper** (§5.2).
  - `running` with `heartbeat_at` older than 5 min → `failed`,
    `error='stale heartbeat'` (a crashed worker; the worker heartbeat is 60 s).
  - `queued` → `failed`, `error='not claimed'`, **only when the workers are down**
    (Oscar 2026-10-07). The run waited more than 5 min **and** no run was claimed
    (`started_at`) **and** no running run heartbeated in those 5 min.
  - A backlog behind busy workers is never failed, however long it waits.
  - Thresholds: `CRAWL_REAPER_STALE_HEARTBEAT_MINUTES`, `CRAWL_REAPER_UNCLAIMED_MINUTES`.
  - The reaper covers every trigger and kind. A reaped comments run closes its comment
    request once none of its runs is open.
  - A message for a reaped run that reaches a worker later is acked and dropped (the
    claim only takes `queued` rows).
- **Regular slot and stagger** (Oscar 2026-10-07).
  - `next_run_at` is the regular, anchored slot (e.g. 08:00). It fires at
    `next_run_at − offset(task)`, where `offset = (task_id × 2654435761) mod
    CRAWL_SCHEDULER_SPREAD_MINUTES` in seconds (default 0).
  - With 30, a batch anchored at 08:00 fires over 07:30–08:00, the same minute every day.
  - Advancing adds `every` to the anchor, so the cadence is stable.
  - Retries live in `crawl_schedules.retry_at` (migration
    `media-backend-paid/docs/db/migrations/2026-10-07_crawl_schedules_retry_at_userdb.sql`,
    pending), never in `next_run_at`. Every dispatch clears it.
- **Worker concurrency** (`CRAWL_WORKER_CONCURRENCY`, default 1, recommended 4). The
  worker runs N parallel runs in one process: prefetch N, a thread and heartbeat per
  run, manual ack per message.
  - Shared caches are locked, merged and atomically replaced. Publishing is serialized.
  - It is not N processes, because separate processes would overwrite each other's cache
    files.
  - Apify STARTER allows 32 concurrent actor runs and 64 GB. A run starts up to 3 actors
    of 1–4 GB. With N > 1, N is capped once at startup by `GET /v2/users/me/limits`:
    `min(jobs // 3, GB // 12)`, which is 5 on STARTER.

#### 6.2.2 Failure → retry, backoff, auto-pause

- Every terminal scheduled run (`succeeded` / `partial` / `failed`, reaped ones
  included) is **settled once** by the poller. The marker is
  `crawl_runs.stats.scheduler = {outcome, consecutive_failures, retry_at, …}`.
- **Success** sets `consecutive_failures = 0` (plus `last_run_id` and `last_success_at`).
- **Failure** increments `consecutive_failures` and retries instead of waiting for the
  next slot: `retry_at = now + backoff`, when that is earlier than the next regular fire
  time.
  - Backoff is 15 min, 30 min, 60 min… (`CRAWL_SCHEDULER_BACKOFF_MINUTES` ×
    2^(n−1)), capped at `every`.
  - The retry's window still starts from the last success, so nothing is lost.
  - Failures are:
    - a scheduled run ending `failed` / `partial`;
    - (Oscar 2026-10-07) a request the backend refused **without a run row**: 403
      RBAC/plan, 400/404 validation, 409 task inactive or actor disabled, login or
      membership errors;
    - transient errors (no HTTP answer, 5xx without a run). These back off the same way
      but never pause.
- **3 consecutive failures** (`CRAWL_SCHEDULER_MAX_FAILURES`) set `active = false` and
  `paused_reason = 'auto-paused <ts> after 3 consecutive failed scheduled runs (last: run
  N <status>: <error>)'`, logged as `!!! SCHEDULE PAUSED`. The task's own `status` is
  left alone. `/crawl/tasks/schedule {active: true}` re-enables the schedule and resets
  the counter and the reason.
- A run **blocked by admission** (budget) is not a failure. Neither is a `cancelled`
  duplicate. Neither is settled, and the normal slot stands.

**Split of schedule bookkeeping** (avoids double counting):

| `crawl_schedules` column | Written by |
|---|---|
| `last_run_id`, `last_success_at` | worker at finish, for immediate feedback (skipped if its run was reaped), and poller on settle (covers reaped runs) |
| `consecutive_failures` | poller only (settle and dispatch). The worker's role keeps the column grant, but the worker no longer writes it |
| `next_run_at`, `retry_at`, `active`, `paused_reason` | poller only. The worker's role has no grant on `next_run_at` / `retry_at` / `active`. `/crawl/tasks/schedule` clears `retry_at` on `active: true` or a new `next_run_at` |
| everything else | backend `/crawl/tasks/schedule` |

Tests: poller unit tests (policy, dispatch, backend client; HTTP and store mocked) and
DB tests (reaper / settle / dispatch SQL on a throwaway userdb with an injected clock);
backend endpoint tests for `trigger`. There is also a local e2e: poller tick → backend
over HTTP (admission faked) → local `rabbit_enqueuer` → `document_processing_dev` /
`dev_crawl_runs` → worker (Apify mocked, dry runs). It covered a schedule advancing, a
blocked run, a worker killed mid-run → reaped → 15 min retry, then not claimed twice →
30 min → auto-pause.

How far the window helps depends on `crawl_actor_classes.window_support` (from
agent_framework §5.1 and the actor code):

| window_support | Actors | Effect |
|---|---|---|
| `server_min_date` | `facebook_keyword_search` (`start_date`/`end_date`), `twitter_keyword_search` (`timeSince`), `facebook_page_posts` (`onlyPostsNewerThan`, day granularity, `facebook/posts.py:96-113`) | increments don't re-pay (only the overlap day) |
| `server_coarse` | `linkedin_keyword_search` (`postedLimit` 24h/week/month), `google_news` (`timeframe`, derived from the window since 2026-10-07; before, a fixed `1d` unless set) | the window rounds up to the coarse bucket. Verify the bucket in the actor's run log on a task's first run |
| `client_only` | `instagram_*`, `tiktok_posts` | date filter runs after paying. Rely on newest-first + cap (§6.3) |

### 6.3 Adaptive caps (optional per schedule)

From the last *k* successful runs of the task:
- posting rate `r` = `items_paid` / span between `oldest_item_at` and `newest_item_at`
- `overlap_rate` = `items_already_seen / items_paid`

Then `max_items = clamp(r × every × (1 + margin), cap_floor, crawl_tasks.max_results)`,
nudged down while `overlap_rate > target_overlap_rate`. The first run is a probe at
`cap_floor` (agent_framework §5.1). Spikes are non-stationary: the circuit breakers
(§7.4) own the tail, not the learner.

### 6.4 Ad-hoc runs (Oscar, 2026-10-06; branch `crawl-adhoc` in media-backend-paid + apify_client)

Launch a crawl on demand **without a `crawl_tasks` row** — a one-off probe, a customer's
"just look this up once" — with the same money controls as a task run.

- **Request.** `POST /crawl/runs/request` takes an inline `task` spec instead of `task_id`
  (exactly one). The spec is the writable task columns, validated with the same rules as
  `/crawl/tasks/create`: actor in `crawl_actor_classes` (crawl kind, enabled), `params_schema`,
  reserved `actor_params` keys and cap-lifting `apify_input` keys refused, `max_results` ≤ 1000,
  `min_date` XOR `period` or neither (neither = no date filter, §2.8 R2), `search_params`
  sorted. Org from the JWT only; `trigger` is `api` only (no schedule without a task).
- **Admission.** The same path as task runs, not a fork: estimate (cold — no history) →
  `admit_api(tool='apify', kind='crawl', path='api:/crawl/runs/request')` → 429 / 503 with a
  `blocked` row, partial admission scales `max_items`.
- **Row.** A `crawl_runs` row is still written (audit, cost, funnel): `kind = 'adhoc'`,
  `task_id` NULL, `comment_request_id` NULL, `params` = the usual `to_actor_kwargs()`
  snapshot **plus `params.spec`**, the complete canonical spec (every writable column with
  defaults, `min_date` as UTC ISO). The DB row is the source of truth.
- **Message.** The §6.1 shape unchanged: `kind: "adhoc"`, `task_id: null`, `params` = the
  row's `params`; `max_cost_usd` = the spec's `max_cost_usd_per_run`.
- **Worker.** `plan_run` builds the task with `CrawlTask.from_spec(run.params.spec)`, which
  shares `from_db_row`'s column mapping and `to_actor_kwargs()` (no second param path); from
  there it is a crawl run (actor checks, validation, window = `window_since`, budgets, metering
  as `crawl`, funnel, `crawl_run_calls`, `usage_events`). No schedule bookkeeping.
- **Filter-cache key.** `adhoc:<sha1>` of the canonical spec without the fields that change no
  filter decision (`name`, `metadata`, `max_cost_usd_per_run`, `max_results`, `publish`),
  JSON with sorted keys and compact separators. Repeating the same ad-hoc crawl — bigger or
  smaller — reuses its cached drops; any filter change starts a new scope. Backend
  (`crawl_helpers.adhoc_cache_key`) and worker (`crawl_task.adhoc_cache_key`) both compute
  it; a shared test vector pins them together.
- **Live runs** (Oscar, 2026-10-06). One live run per spec, mirroring
  `crawl_runs_one_live_per_task`: the partial unique index `crawl_runs_one_live_adhoc_spec
  (org_id, params->>'task_id') WHERE kind = 'adhoc' AND status IN ('queued','running')`. An
  identical spec of the same org already queued/running → the new run is `cancelled`
  (`duplicate live run`) and the request answers `409 {live_run_id}` (R5). Different orgs or
  specs don't collide; a terminal run frees the slot.
- **Missing migration.** On a userdb without the migration the backend answers ad-hoc requests
  `503 "migration 2026-10-07_crawl_runs_adhoc_userdb.sql not applied"` (catalog check before
  admission, cached once true; a kind-check violation at insert maps to the same 503). Task runs
  keep working.
- **Save as task.** `POST /crawl/tasks/create` (and `propose`) take `from_run_id`: the ad-hoc
  run's spec, the body's fields on top (a body window replaces the spec's), validated like any
  create, `metadata.from_run_id` set. No new endpoint, so no RBAC rows. The task's cache key
  is `db:<task_id>` (the ad-hoc cached drops are not carried over).
- **Schema.** `media-backend-paid/docs/db/migrations/2026-10-07_crawl_runs_adhoc_userdb.sql`
  (draft, not applied to stg): `crawl_runs_kind_chk` gains `adhoc`; `crawl_runs_kind_ref_chk`
  requires `task_id IS NULL AND comment_request_id IS NULL` for it; plus the live-spec unique
  index above. The one-live-run index
  (`task_id IS NOT NULL`) and the `crawl_runs_same_org` trigger (every check guarded by `IS NOT
  NULL`; a `query_id` is still checked) need no change; the backend / `crawler_worker` grants
  already cover the new kind. The poller's reaper covers every kind; its settle pass only reads
  `kind = 'crawl'` scheduled runs.
- **Tests.** Backend unit (spec canonicalization, key vector) and DB endpoint tests; worker unit
  (`TestFromSpec`, `TestAdhocRuns`) and DB tests (as `crawler_worker`); a local e2e (backend →
  local rabbit_enqueuer → `document_processing_dev`/`dev_crawl_runs` → worker with Apify
  mocked, dry runs, real usage ledger): ad-hoc run `succeeded` with cost and funnel, the same
  spec again reusing the cached drop, a refused ad-hoc request (429, `blocked`, nothing
  enqueued), an identical spec while one is live (409, `cancelled`), and "save as task" followed by a run of the promoted task.

### 6.5 Batched runs (Oscar, 2026-10-13; branch `crawl-batch` in media-backend-paid + apify_client + poller)

Tasks of one org that share every field but `search_params` run as **one** run whose actor
call gets the merged list: fewer Apify starts, shorter rounds, one row per group. Plan and the
13 decisions (with the per-actor analysis):
`business_agent/docs/plans/crawl_batched_runs_2026-10.md`. Endpoint contract:
`media-backend-paid/docs/endpoints/crawl.md` § Batched runs.

- **Request.** `POST /crawl/runs/request {task_ids: [...]}` (the poller sends it for an org's
  due schedules when `CRAWL_SCHEDULER_BATCH=1`). The backend groups by the group key (every
  writable field but `search_params` / `name` / `metadata`, plus `max_results` only for
  `per_param` actors), only for actors with `crawl_actor_classes.batch_max_params`
  (X, `instagram_hashtags`, `instagram_profile_posts`, `facebook_page_posts`; not the loop
  actors `facebook_keyword_search` / `instagram_profile_queenlike`, nor `google_news`,
  `linkedin_keyword_search`, `tiktok_posts` until their cap semantics are checked). A `per_call`
  task whose last result hit its cap runs alone (saturation split). Singletons take the
  single-task path unchanged.
- **Row.** `kind = 'batch'`, `task_id` NULL, `max_items` = Σ member caps (`per_call`) or the
  common per-param cap (`per_param`), `params` = the usual snapshot of the merged definition +
  `params.spec` (merged canonical spec) + `params.batch` (members); `params.task_id` = the
  filter-cache key `batch:<sha1>` (the spec without `search_params` and the volume/notes
  fields: the group's filter signature, whoever is in it). Members: **`crawl_run_tasks`**
  (`search_params`, `max_items` = the member's share, `window_since`, `filter_cache_key`,
  attribution columns, `live`). One live run per task holds across single and batch runs
  (DB triggers, advisory locks).
- **Worker.** `plan_run` builds the task with `CrawlTask.from_spec(params.spec,
  cache_key=params.task_id)` — the spec is **frozen at request time** (a single run re-reads
  its task row; a batch doesn't) — and reads the members (`CrawlDB.get_run_tasks`); a member
  param outside the spec, a non-`batch:` key, no members or a `task_id` fail the run without
  spend. From there it is a crawl run (actor checks, budgets: items = `max_items × len(union)`
  for `per_param`, else `max_items`; cost = min(message `max_cost_usd`, spec breaker,
  `admitted_usd`); metering as `crawl`).
- **X.** The actor ANDs a task's params (run log 2026-10-13: `["guadalupe","huachicol"]` →
  `guadalupe huachicol`), so a batch sends `query = ((clause₁) OR (clause₂) …)`, each clause
  exactly what that task sends alone (`TwitterKeywordSearchActor.member_clause`), with
  `contentLanguage` / `timeSince` / `timeWithinTime` as before. Verify the query in the first
  batched run's Apify log.
- **Filter cache** (`ApifyActor.batch` = `src/worker/batch.py::BatchContext`): a URL is skipped
  when it is cached as dropped under the batch key **or any member's key** (same filters); a
  decision is written under the batch key and the keys of the members the document belongs
  to (`_remember_filter_decisions`, the one write path of every `process_documents`).
- **Attribution** (approximate, by text match, `src/worker/batch.py`): title + body (+ the
  attached article), lower-cased, accents stripped, whole words; X members match when all
  their params match, others when any does; URL params match the post's URL / profile /
  source by the page or account name. Written before the run's finish (`CrawlDB.update_run_tasks`,
  spooled like the cost writes): `items_matched` (paid items in), `items_new` (documents to
  publish), `newest_item_at`, `cost_usd` (run cost × matched share, unmatched split evenly),
  `stats` (`cost_share`, `run_unmatched`, `saturated`). The run row stays authoritative.
- **Schedules.** `trigger='schedule'` batch runs update `last_run_id` / `last_success_at` of
  **every** member; the poller settles batch runs per member. A member's next window starts
  from its attributed `newest_item_at` (none → its own `window_since` carried); a batch takes
  the earliest member window.
- **Release order.** Worker first (an older worker fails a `batch` message as an unknown kind,
  no spend), then the migration, the backend, the poller flag.

---

## 7. Estimates and caps (hooks only)

The price table and `admit()` belong to `usage_client` (`rate_limiting.md` §2.4, §2.6). This
design supplies the inputs and stores the outputs.

### 7.1 Crawl estimate (a range, never a point; agent_framework §5.1)

- `items_high = max_items × (len(search_params) if cap_scope='per_param' else 1)`, plus
  the enrichment calls:
  - comments: `get_comments` → ≤ `max_comments` × posts above
    `get_comments_after_likes`
  - profiles: `enrich_followers` → ≤ posts above `enrich_author_after_likes`, minus the
    90-day cache (`stats_max_age_days`)
- `items_low`: cold, the `cap_floor` probe. Warm (≥ 3 runs), the p10 of the task's
  `items_paid`.
- `usd = starts × start price + items × price per result`, per actor, from
  `usage_client.prices.apify_unit(actor_id, kind)` — the actor's current Apify pricing
  (`GET /v2/acts/{id}` → `pricingInfos`, 24 h cache, static table when unreachable;
  usage_client README § Apify actor prices), falling back to the per-kind
  `UNIT_USD` only for an actor it doesn't know. 2026-10-07 BRONZE prices:
  `scrapeforge/facebook-search-posts` $0.00005 / start + $0.00259 / result (one start per
  search param), `apify/facebook-posts-scraper` $0.001 + $0.004 / post (+ $0.001 date
  filter), comments `apify/facebook-comments-scraper` $0.001 + $0.002 / comment, profiles
  `apify/facebook-pages-scraper` $0.0082 / page. Older measured magnitudes: $2.25–4 / 1,000
  paid items, profiles ~$10 / 1,000 (`business_agent/docs/platform.md` §Cost model).
- Admission uses `estimate_usd_high` (agent_framework §5.1: "the budget gate applies to
  the high end").
- Endpoint: `POST /crawl/tasks/estimate`. This is the backend half of `estimate_crawl`
  (sprint §3 Line C).

### 7.2 Comment estimate (exact; agent_framework §5.3)

`Σ min(n_comments_declared, depth) × price`, or posts × `usd_per_post`. The producer's
default is $0.02/post (`select_posts_for_comments.py:62-64`). The measured figure is
~$0.01/post (`platform.md`). It is stored on the request before approval.

### 7.3 Stats the worker must start counting

- `_record_filtered_documents` covers the reasons `cached`, `keyword`, `date`,
  `existing_elasticsearch`, `language` and `location`. **`_filter_llm` and the
  per-actor dataset size are not recorded**, so `items_dropped_llm` and `items_paid`
  need counters at the stage boundaries in `process_documents` (`actor.py:145-185`).
- pfbid URL variants (`documentation_gaps.md` #10) count as `items_new`, so `yield_rate`
  reads high until that is fixed.

### 7.4 Caps and breakers

| Layer | Mechanism | Source |
|---|---|---|
| Per run | `max_items` → Apify `maxItems` / `resultsLimit`. Optional Apify run charge limit (verify the actor/client supports it) | agent_framework §5.1 |
| Per task | `crawl_tasks.max_cost_usd_per_run`; `max_results` as hard ceiling | this doc |
| Per org / day | `usage_limits` default `apify` $15/day; `maxItems` capped to remaining budget | `rate_limiting.md` §2.4, §2.5, §2.7 |
| Per request | $2 / 2,000 docs rule applies to API-triggered runs | `rate_limiting.md` §2.4 (decided 2026-10-05) |
| Scheduler | auto-pause after `consecutive_failures`; one live run per task (§2.5 unique index) | this doc |

### 7.5 Recording the real cost (2026-10-07)

Found on the local stack: every call was recorded with only the actor-start charge — 9
calls, $0.0021 recorded vs $0.6727 billed (e.g. run `Oy7YlPPjj7gF8Q203`: $0.00005
recorded, `usageTotalUsd` $0.07516 = 1 × `apify-actor-start` $0.00005 + 29 ×
`apify-default-dataset-item` $0.00259; comment and profile calls recorded $0). Pay-per-event
actors charge their per-result events seconds after the run turns `SUCCEEDED`; the run
object `call()` returns, and an immediate re-read, still show only the start.

- **Settle** (`usage_client.apify_cost.settle_run`, used by `usage_client.apify.run`, so
  the worker, `run_searches.py` and every script share it): re-read the run until it is
  terminal, `usageTotalUsd` and the charged event counts are equal on two consecutive
  reads, and the per-result event count covers the dataset items (or the run's charge cap
  is reached); ≤ 60 s, backoff 1 → 8 s. The worker's `crawl_run_calls` row and the
  `usage_events` row take that one reading.
- **Our own figure** (`cost_from_events`): Σ billed event counts × event price from the
  run's `pricingInfo` (`PAY_PER_EVENT`), items × price (`PRICE_PER_DATASET_ITEM`), Apify's
  total for rental / platform-usage actors. Cross-check when settled (ERROR log on a
  mismatch > $0.005 and 5%); the recorded value with `estimated=true` when Apify hasn't
  settled in time (max of the partial total, the billed events, start + items × price,
  capped at the charge cap) — those runs are spooled for reconciliation.
- **Reconcile** later: `src/scripts/reconcile_costs.py` (`crawl_run_calls` + terminal
  `crawl_runs.cost_usd` + the ledger) and `python -m usage_client.reconcile` (ledger only,
  e.g. hand runs). The ledger is append-only: a difference becomes a
  `<event_id>:correction` row, never an UPDATE.

---

## 8. RBAC rows and endpoints (list only)

RBAC matches `request.path` **exactly** (`UserRolesVsOrgPlanVsEndpoints.IsEndpointAllowed`,
`backend/models/rbac.py:118-124`), so ids travel in the body, not the path. Each
endpoint needs `user_roles_vs_org_plan_vs_endpoints` rows (`docs/TECHNICAL.md`
§Authentication & RBAC). Each row set is cloned from a source endpoint, as in
`docs/db/migrations/social_sentiment_stances_rbac_update_userdb.sql`. The source
endpoints are Q7.

| Endpoint | Purpose | Write? |
|---|---|---|
| `/crawl/actors` | list `crawl_actor_classes` + params schema | read |
| `/crawl/tasks/list` | org's tasks (+ schedule, last run, waste) | read |
| `/crawl/tasks/propose` | create `draft` (customers/agents) | write (draft only) |
| `/crawl/tasks/create`, `/crawl/tasks/update`, `/crawl/tasks/set_status` | operator-level CRUD / approve / pause / retire | write |
| `/crawl/tasks/link_search`, `/crawl/tasks/unlink_search` | `crawl_task_searches` | write |
| `/crawl/tasks/schedule` | upsert `crawl_schedules` | write |
| `/crawl/tasks/estimate` | §7.1 range | read |
| `/crawl/runs/request` | launch (admit → enqueue) | write, paid |
| `/crawl/runs/list`, `/crawl/runs/get` | status + stats + cost (request-status layer, sprint Line C) | read |
| `/crawl/runs/cancel` | `requested`/`queued` → `cancelled` | write |
| `/crawl/comments/estimate`, `/crawl/comments/request`, `/crawl/comments/approve` | §2.7 | write, paid on approve |

Grants: `SELECT, INSERT, UPDATE` on the `crawl_*` tables to `backend`, following the
pattern of the tags tables. The worker's DB role is Q5.

---

## 9. Migration from Excel

### 9.1 Theme → org mapping

Sources: `org.id` + `theme` in `business_agent/customers/<slug>/customer.yaml`, the org
table of the 2026-10-05 `rate_limiting.md` draft ("rl draft" below; the spec has since been
restructured and no longer carries it), and the keep-list in sprint §8 item 7. The decided
map now lives in `src/scripts/import_tasks_xlsx.py` (`KEEP_THEMES` / `RETIRED_THEMES`). Counts are xlsx
rows (enabled/total) on 2026-10-05.

| theme | rows | org_id | source / note |
|---|---|---|---|
| `totalenergies` | 52/67 | 62 | rl draft "62 TotalEnergies" (no `customers/` folder) |
| `totalenergies_guadalupe` | 5/5 | 62? | Q1 |
| `pinal_amoles` | 17/18 | 106 | `customers/pinal_amoles/customer.yaml` |
| `zitacuaro` | 13/13 | 102 | `customers/zitacuaro/customer.yaml` |
| `sjdr` | 0/18 | 101 | `customers/sjdr/customer.yaml`. Dropped per sprint §8.7 → `retired` |
| `morelia_descifra` | 0/20 | 105 | rl draft. Dropped per sprint §8.7 → `retired` |
| `orizaba` | 0/15 | 47 today → new Orizaba org | `customers/orizaba/customer.yaml` (reuses rows 9–16, 18, 80, 86). Sprint §2.3/§8.7 → import once the new org exists (Q1) |
| `queretaro` | 0/8 | 93 | rl draft |
| `mc`, `zona_fest`, `top_news`, `chepe_guerrero`, `felifer`, `efren_cuevas` | 0/21 | — | no org on record. Leave in the frozen xlsx (Q1) |

**Cross-check against `ids.json` `crawl_task_ids`**:
- `sjdr` 106–116, `pinal_amoles` 145 + 159–175, `orizaba` 124 and `zitacuaro` 146–158
  match the xlsx (theme + name).
- `zitacuaro` **124–136 are stale**: in the xlsx those ids are now `orizaba` (124) and
  `morelia_descifra` (125–136). Sprint §2.2 inherits the stale range ("crawl rows
  124–136").

So the importer maps by `theme` and uses `ids.json` only to warn, never as the key.

### 9.2 Steps

1. **DDL locally.** Apply the migration to a throwaway DB (`dev/testdb.sh
   proto06_test_apify_client`, `rate_limiting.md` §5) and to local userdb `:5334`
   (`dev/docs/db/userdb_runbook.md`). Seed `crawl_actor_classes` from the registry.
   Applying it to stg proto06 needs Oscar's go.
2. **Import script** `apify_client/src/scripts/import_tasks_xlsx.py` — **written
   2026-10-06** (branch `crawl-import`; tested on a throwaway userdb, stg dry run only).
   Decisions applied (Oscar 2026-10-05/06): kept orgs 62 (`totalenergies` +
   `totalenergies_guadalupe`), 102, 106 → enabled `active`, disabled `paused`, except
   legacy 145 (past V Informe) → `retired`; `sjdr`/`zona_fest` → 101,
   `morelia_descifra` → 105, `queretaro` → 93 imported `retired` (Q2); `orizaba` and
   the five org-less legacy themes skipped (Q1). Stg dry run 2026-10-06: 154 to insert
   (87 active / 15 paused / 52 retired), 31 skipped, 0 conflicts, 0 constraint failures.
   It follows the plan/apply split of `customer_ops`:
   - Read **all** rows with `CrawlTask.from_csv_row()`. `load_tasks()` drops disabled
     rows (`crawl_task.py:139-157`).
   - Map theme → org (§9.1) and validate `actor_params` (§4).
   - Dry-run prints per-org counts, conflicts with the unique key and `ids.json`
     mismatches.
   - `--apply` inserts `crawl_tasks` with `legacy_task_id`/`legacy_theme`, and writes
     `crawl_task_db_ids` into each customer's `ids.json` via `record_ids`.
   - Duplicate legacy ids 35/36/37: the first row keeps the key, and the second gets
     `35~2`, etc. This ends the existing filter-cache collision at the cost of one
     re-evaluation (Q6).
   - Search links are only created where `customer.yaml` names them. Otherwise links
     are added by hand.
3. **customer_ops**: the `tasks` step (`crawl_tasks.py`) writes through
   `/crawl/tasks/create` (or the DB, Q8) instead of appending xlsx rows. Its `DEFAULTS`
   become the endpoint's per-actor defaults.
4. **Parallel run (one demo org, e.g. 106).** Run the worker on
   `document_processing_dev` with `publish=false` dry runs, and compare against the same
   tasks run from xlsx (counts, cost). Then switch that org to live publishing.
5. **Cut-over.**
   - Freeze `tasks.xlsx`: rename it to `tasks_frozen_<date>.xlsx`, keeping the existing
     `tasks_backup_*.xlsx`.
   - `run_searches.py` refuses to run for orgs that were migrated. This also closes
     `documentation_gaps.md` #11; the env override was added 2026-10-05,
     `run_searches.py:31-32`.
   - Update `apify_client/CLAUDE.md`/`AGENTS.md` §Task System, `customers/*/README.md`
     and `sjdr/plan.md` WS-7a.
6. **Fold** the DDL into `media-backend-paid/db/user_db/schema.sql` once applied, and
   add Appendix A to `DATABASE_POSTGRES.md`.

**Rollback.**
- The DDL is purely additive: new tables only, no change to existing ones.
- To roll back: stop the worker and scheduler, export active tasks back to xlsx (a
  small `export_tasks_xlsx.py`), unfreeze `tasks.xlsx`, and resume `run_searches.py`
  with `CURRENT_THEME`. The filter cache is intact because `legacy_task_id` was kept.
- The tables can be dropped with the commented `DROP` block at the end of the
  migration. Runs, costs and `usage_events` rows stay as the record of spend.

---

## 10. Open questions for Oscar

| # | Question | Proposal |
|---|---|---|
| Q1 | Orgs for `totalenergies_guadalupe`, the six theme-less legacy themes, and Orizaba (47 vs the new org) | `_guadalupe` → 62. Don't import the legacy themes. Import Orizaba when the new org exists |
| Q2 | Import disabled rows of dropped themes (sjdr, morelia_descifra, queretaro) as `retired`, or skip them? | import as `retired` (keeps history and the `legacy_task_id` → filter-cache link) |
| Q3 | Where does the scheduler live: a tick in `search-sentiment-poller-tasker` (has userdb + 1-min loop), the backend, or the `apify_client` worker? | **Decided 2026-10-06 (Oscar)**: in the poller, which requests each due run **through the backend** (`POST /crawl/runs/request`, `trigger: "schedule"`, as a machine user), so estimate, admission, insert and enqueue stay in one place. The same tick reaps stale runs (heartbeat > 5 min → `failed` 'stale heartbeat'; queued > 5 min → `failed` 'not claimed'). It retries failures with backoff (15/30/60 min… capped at `every`, window still from the last success). After 3 consecutive failures it auto-pauses the schedule. Blocked runs are not failures. Worker/poller bookkeeping split: §6.2.2 |
| Q4 | Run queue vhost: `document_processing_stg`/prod (user logic, touches userdb) while documents still publish to the document vhost — OK to have two connections? | yes, consume user-logic vhost, publish documents vhost; dev = `document_processing_dev` for both |
| Q5 | DB role for the worker | **Decided 2026-10-06**: dedicated `crawler` group role (NOLOGIN, holds grants) + `crawler_worker` login (conn limit 10, 60 s statement/idle timeouts): SELECT on `crawl_*` + `user_searches(query_id, org_id)`; INSERT/UPDATE `crawl_runs`; INSERT `crawl_run_calls`; column UPDATEs on schedules / comment requests / posts; INSERT `usage_events` once it exists. Script: `media-backend-paid/docs/db/migrations/2026-10-06_crawl_worker_role_userdb.sql` (password via `-v crawler_password=`) |
| Q6 | Split the duplicate legacy ids 35/36/37 (lose one cache pass) or keep the shared key? | split |
| Q7 | RBAC source endpoints to clone | **Decided 2026-10-06 (Oscar)**: everyone in the org may read, propose and launch paid work — all 17 `/crawl/*` endpoints mirror `/savedsearches/get` (reads) or `/savedsearches/save` (propose + operate) for every role; the plan decides (Premium/Empresarial yes, Básico no). Spend is bounded by per-org caps/admission (rate-limiting spec), so the operate endpoints ship only with admission in place. Script: `media-backend-paid/docs/db/migrations/2026-10-06_crawl_rbac_update_userdb.sql` |
| Q8 | `customer_ops` writes via API (as an org user, sprint §8 item 3) or direct SQL? | API |
| Q9 | Where does the WS-4 `classify_users` pass run once there is no per-theme batch? | separate scheduled job per org, scope = flagged tasks |
| Q10 | One schedule per task enough, or multiple windows (e.g. hourly during an event, daily otherwise)? | one, edited for event periods. Revisit with Feria (WS-5) |

---

## 11. Follow-ups (later)

- **F1 — every run saved and linked to the usage ledger** (Oscar, 2026-10-06). Every
  crawl execution gets a `crawl_runs` row — API, schedule *and* hand runs
  (`run_searches.py`, `trigger='cli'`), including `blocked` / `failed` / `cancelled` /
  dry runs — and is linked to the rate-limiting ledger `usage_events`
  ([`business_agent/docs/plans/rate_limiting.md`](../../../business_agent/docs/plans/rate_limiting.md)
  §2.2): `crawl_run_calls.usage_event_id` becomes a real FK once the `usage_events`
  migration lands, and each `usage_events` row carries the `run_id` (column or `raw`)
  so cost rolls up per run, task, search and org. Lands after the `usage_events` table
  exists (rate-limiting P0/P2).
  - **Done (2026-10-07):** every Apify run of a queued crawl run is linked:
    `crawl_run_calls.usage_event_id` = `usage_events.event_id`, and the event carries
    the run in its id (`apify-client:crawl-run:<run_id>:…`) and the Apify run id in
    `raw.run_id`. Hand runs (`run_searches.py`) are metered too (`usage_events` with
    `event_key='run-searches:<session>:<task_id>'`, org = the theme's org; no org →
    refused unless `--platform`).
  - **Still open:** hand runs don't create a `crawl_runs` row (`trigger='cli'`) and
    don't read tasks from `crawl_tasks`; the `usage_event_id` FK; `crawl_runs.block_id`
    for worker-side blocks.

## Appendix A — proposed section for `media-backend-paid/docs/DATABASE_POSTGRES.md`

*Insert under `## userdb (default bind)`, after "### Tags subsystem". Not yet applied.*

> ### Crawl tasks (Apify acquisition)
>
> Org-scoped definitions and runs for `apify_client` crawls. They replace
> `apify_client/tasks.xlsx` + `CURRENT_THEME`. Design:
> [`apify_client/docs/db/crawl_tasks.md`](../../apify_client/docs/db/crawl_tasks.md).
> DDL: [`./db/migrations/2026-10-05_crawl_tasks_userdb.sql`](./db/migrations/2026-10-05_crawl_tasks_userdb.sql).
> Writers: the backend `/crawl/*` endpoints (definitions, run requests) and the
> `apify_client` worker (run status, stats, cost). Same surrogate-key convention as the
> tags tables.
>
> | Table | Purpose |
> |---|---|
> | `crawl_actor_classes` | Lookup of actor registry keys: network, Apify actor id, `run_kind`, `window_support`, `cap_scope`, `params_schema` (validates `crawl_tasks.actor_params`). Prices live in `usage_client.prices`, not here |
> | `crawl_tasks` | One crawl definition per row. `org_id NOT NULL` (FK `orgs`), `actor_class`, `search_params` (jsonb array), filters (`country_id`, `language`, `min_date` xor `period`, `not_keywords`, `llm_filter_condition`), enrichment flags, `actor_params` (jsonb), `max_results` (hard ceiling), `max_cost_usd_per_run`, `status` (`draft`/`active`/`paused`/`retired`, soft-retire), `legacy_task_id`/`legacy_theme` (Excel provenance; `legacy_task_id` is the actor filter-cache key for imported rows). Partial unique `(org_id, actor_class, search_params) WHERE status <> 'retired'` |
> | `crawl_task_searches` | N:M link task ↔ `user_searches.query_id` (one search → N tasks). Cascades on delete of either side. Untracking a search pauses tasks left without links (application rule) |
> | `crawl_schedules` | Optional 1:1 recurrence per task: `every`, `window_overlap`, `next_run_at` (partial index `WHERE active`), adaptive-cap settings, failure counter |
> | `crawl_runs` | One execution (`kind` = `crawl` or `comments`). `org_id NOT NULL`, optional `query_id` (→ `usage_events.search_id`), `status` (`requested`→`queued`→`running`→`succeeded`/`partial`/`failed`; `blocked`, `cancelled`), resolved `params` snapshot, run window, effective `max_items`, estimate range, the funnel counters (`items_paid`, `items_already_seen`, `items_new`, per-filter drops) and generated `overlap_rate` / `yield_rate`, `cost_usd`. Partial unique `(task_id) WHERE status IN ('queued','running')` = one live run per task |
> | `crawl_run_calls` | One row per Apify actor call of a run: `apify_run_id` (unique, idempotency), `purpose`, `items`, `cost_usd` (Apify `usageTotalUsd`), raw `usage`, and `usage_event_id`, the matching `usage_events.event_id` (no FK) |
> | `crawl_comment_requests` / `crawl_comment_request_posts` | Comments-on-demand: an org-scoped request (selector, params, exact `estimate_usd`, approval) and its posts (`post_url` = exact ES `_id`, never normalized; `tier`, `depth`, declared/fetched counts). Approval creates `crawl_runs` with `kind='comments'`, one per tier |
>
> **Metering.** Every Apify run writes one `usage_events` row (`service='apify-client'`,
> `tool='apify'`, `kind` = `crawl`|`comments`|`profiles`, `unit='items'`, `event_id =
> 'apify-client:crawl-run:<run_id>:<org_id>:<kind>:<apify_run_id>'`), linked from
> `crawl_run_calls.usage_event_id`. Admission (`usage_limits`) happens
> before the run row is queued (backend) and again on every Apify call (worker). See the rate-limiting spec
> (`business_agent/docs/plans/rate_limiting.md` §2).
