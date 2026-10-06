# Crawl tasks in userdb — design (draft)

Status: **draft for Oscar's review, 2026-10-05.** Docs only: no DDL applied, no DB
writes, no code changes. The proposed DDL is in
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
   (agent_framework §5.1). Each run also writes its cost to the `usage_events` ledger
   (`rate_limiting.md` §4, P2).
5. Hooks for **pre-run range estimates** and **caps**: admission plus `maxItems`
   capped to the remaining budget (`rate_limiting.md` §6 "Apify runs", §7
   `usage_client.apify`).
6. Comments-on-demand as a first-class request: post selection, an exact estimate,
   then purchase (agent_framework §5.3; `src/scripts/select_posts_for_comments.py`,
   `src/scripts/run_comment_tasks.py`).

**Non-goals**
- The metering tables themselves (`usage_events`, `usage_limits`, `usage_blocks`) and
  the `usage_client` library. Those are specified in `rate_limiting.md` §4 and §7; this
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
      │        └──< crawl_runs ──< crawl_run_calls ──(event_id)──> usage_events (rate_limiting §4)
      └─< crawl_comment_requests ──< crawl_comment_request_posts ──> crawl_runs (kind='comments')
```

### 2.1 `crawl_actor_classes` (lookup, seeded)

One row per key in the actor registry (`src/actors/__init__.py:16-25`). It holds the
capability flags used by validation, scheduling and estimation. Prices are **not**
stored here; they belong to `usage_client.prices` (`rate_limiting.md` §7).

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
| `kind` | `text NOT NULL` CHECK `crawl`/`comments` | |
| `task_id` | FK `crawl_tasks NULL` | set when `kind='crawl'` |
| `comment_request_id` | FK `crawl_comment_requests NULL` | set when `kind='comments'`. A CHECK ties each kind to its FK |
| `org_id` | `integer NOT NULL` FK `orgs` | attribution (`rate_limiting.md` §4.1) |
| `query_id` | FK `user_searches NULL ON DELETE SET NULL` | → `usage_events.search_id` |
| `trigger` | `text NOT NULL` CHECK `api`/`schedule`/`cli` | |
| `requested_by` | FK `users NULL` | |
| `status` | `text NOT NULL DEFAULT 'requested'` CHECK | §5.2 |
| `params` | `jsonb NOT NULL` | resolved kwargs snapshot (`to_actor_kwargs()` + run window). Reproducible |
| `window_since`, `window_until` | `timestamptz NULL` | §6.2 |
| `max_items` | `integer NOT NULL` | effective cap after admission (≤ task `max_results`) |
| `estimate_usd_low`, `estimate_usd_high` | `numeric(12,6)` | §7 |
| `admitted_usd` | `numeric(12,6) NULL` | `Admission.allowed_usd` |
| `block_id` | `bigint NULL` | `usage_blocks.block_id` when status is `blocked` |
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
| `cost_usd` | `numeric(12,6) NULL` | Apify run `usageTotalUsd` |
| `estimated` | `boolean NOT NULL DEFAULT false` | true if computed from list price |
| `usage` | `jsonb NULL` | Apify `usage` object, verbatim |
| `usage_event_id` | `text NULL` | `usage_events.event_id`. No FK, so this table doesn't depend on P0.3 ordering |
| `started_at`, `finished_at` | `timestamptz` | |

Today `ApifyActor.run_actor()` throws away the run object (`actor.py:79-85`). The
utility actors (`FacebookProfileActor`, `FacebookCommentsActor`, …) call
`ApifyClient` directly. Capturing every call is the `usage_client.apify.run()` wrapper
(`rate_limiting.md` §7).

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

Applied to the **local** userdb only (localhost:5334) and smoke-tested (14 checks, rolled
back); **not** on stg.

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
| `facebook_page_posts` | `add_text_from_images`, `download_*`, `add_subtitles`, `add_ai_transcription`, `stats_max_age_days`, `results_limit` |
| `instagram_hashtags` / `instagram_profile_posts` | `keyword_search`, `results_type`, `stats_max_age_days`, `download_*`, `video_dir`, `results_limit` |
| `instagram_profile_queenlike` | `scrape_type`, `output_mode`, `results_limit` |
| `tiktok_posts` | `get_comments_after_likes`, `apify_input`, `results_limit` |
| `google_news` | `timeframe`, `topics`, `enrich`, `region_language`, `max_articles`, `decode_urls`, `extract_images`, `extract_descriptions` |
| `twitter_keyword_search`, `linkedin_keyword_search` | — (common keys only) |
| all (base pipeline) | `check_existing_elasticsearch`, `skip_existing_filter` (`actor.py`) |

**Thresholds that drive cost.** `get_comments_after_likes` and
`enrich_author_after_likes` gate the two paid enrichment calls. They are inputs to the
estimate's high end (§7.1). `customer_ops` defaults are 100/100
(`customer_ops/src/customer_ops/crawl_tasks.py:18-27`). `apify_input` (TikTok) is a raw
passthrough to the actor input, so validation must reject any key in it that would
lift the cap (`resultsPerPage`, `maxItems`).

---

## 5. Lifecycle

### 5.1 Task status

`draft → active ⇄ paused → retired`

- `draft`: proposed by a customer or agent ("read and propose", `rate_limiting.md` §3;
  sprint §3 Line B "Propose" tools such as `plan_crawl`). Never runs. It becomes
  `active` when we approve it.
- `paused`: the old `enabled=false`, an untracked linked search (§2.3), or automatic
  after `consecutive_failures`.
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
  or publish failed. The money is spent, so the stats and cost are still written.
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
         → usage_client.admission.admit(ctx, tool='apify', kind='crawl', est_usd=high)
         → INSERT crawl_runs(status='requested'→'queued' | 'blocked')
         → POST ENQUEUER_URL {"queue": CRAWL_RUNS_QUEUE, "message": {...}}   (pattern: event_report_helpers.py:198-240)
rabbit_enqueuer → queue crawl_runs
apify_client worker:
  claim run → for each Apify call: usage_client.apify.run(actor, input{maxItems≤max_items}, ctx)
            → INSERT crawl_run_calls + usage_events(tool='apify', kind='crawl'|'comments', unit='items',
                                                     cost_usd=usageTotalUsd, event_id='apify-client:<apify_run_id>:<org_id>:<kind>:0')
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
  path=worker:apify-client)` (`rate_limiting.md` §4.1).
- If the cost write fails, the worker spools it and replays later. It never fails the
  run because of it (`rate_limiting.md` §6 rules).
- The DB row is the source of truth. `params` in the message is a convenience copy;
  the worker re-reads the row when they differ.

New wiring in `apify_client`:
- A queue consumer entry point next to `run_searches.py`.
- A userdb connection (`DATABASE_URI`; `psycopg` is not in `requirements.txt` today).
- Two RabbitMQ connections: one consuming runs on the user-logic vhost, one publishing
  documents to the document-processing vhost (Q4).
- Local runs use `document_processing_dev` + `dev_crawl_runs` (workspace `CLAUDE.md`
  §Environments).

### 6.2 Scheduling and newest-first incrementality

The scheduler ticks every minute ("the poller pattern", agent_framework §5.2):

```sql
SELECT … FROM crawl_schedules s JOIN crawl_tasks t USING (task_id)
WHERE s.active AND t.status = 'active' AND s.next_run_at <= now()
FOR UPDATE OF s SKIP LOCKED
```

For each due task it runs the same estimate → admit → insert → enqueue path, then sets
`next_run_at += every`. Where the scheduler lives is open (Q3).

Run window: `window_since = last_success.newest_item_at − window_overlap`, or
`now() − period` on the first run. Scheduled runs pass `min_date = window_since` and
`period = NULL`; the two are mutually exclusive.

How far the window helps depends on `crawl_actor_classes.window_support` (from
agent_framework §5.1 and the actor code):

| window_support | Actors | Effect |
|---|---|---|
| `server_min_date` | `facebook_keyword_search` (`start_date`/`end_date`), `twitter_keyword_search` (`timeSince`), `facebook_page_posts` (`onlyPostsNewerThan`, day granularity, `facebook/posts.py:96-113`) | increments don't re-pay (only the overlap day) |
| `server_coarse` | `linkedin_keyword_search` (`postedLimit` 24h/week/month), `google_news` (`timeframe`) | the window rounds up to the coarse bucket |
| `client_only` | `instagram_*`, `tiktok_posts` | date filter runs after paying. Rely on newest-first + cap (§6.3) |

### 6.3 Adaptive caps (optional per schedule)

From the last *k* successful runs of the task:
- posting rate `r` = `items_paid` / span between `oldest_item_at` and `newest_item_at`
- `overlap_rate` = `items_already_seen / items_paid`

Then `max_items = clamp(r × every × (1 + margin), cap_floor, crawl_tasks.max_results)`,
nudged down while `overlap_rate > target_overlap_rate`. The first run is a probe at
`cap_floor` (agent_framework §5.1). Spikes are non-stationary: the circuit breakers
(§7.4) own the tail, not the learner.

---

## 7. Estimates and caps (hooks only)

The price table and `admit()` belong to `usage_client` (`rate_limiting.md` §7). This
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
- `usd = items × usage_client.prices` per actor. Measured magnitudes:
  $2.25–4 / 1,000 paid items, profiles ~$10 / 1,000 (`business_agent/docs/platform.md`
  §Cost model).
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
| Per org / day | `usage_limits` default `apify` $15/day; `maxItems` capped to remaining budget | `rate_limiting.md` §5, §6, P2 |
| Per request | $2 / 2,000 docs rule applies to API-triggered runs | `rate_limiting.md` §5 (decided 2026-10-05) |
| Scheduler | auto-pause after `consecutive_failures`; one live run per task (§2.5 unique index) | this doc |

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
table in `rate_limiting.md` §5, and the keep-list in sprint §8 item 7. Counts are xlsx
rows (enabled/total) on 2026-10-05.

| theme | rows | org_id | source / note |
|---|---|---|---|
| `totalenergies` | 52/67 | 62 | `rate_limiting.md` §5 "62 TotalEnergies" (no `customers/` folder) |
| `totalenergies_guadalupe` | 5/5 | 62? | Q1 |
| `pinal_amoles` | 17/18 | 106 | `customers/pinal_amoles/customer.yaml` |
| `zitacuaro` | 13/13 | 102 | `customers/zitacuaro/customer.yaml` |
| `sjdr` | 0/18 | 101 | `customers/sjdr/customer.yaml`. Dropped per sprint §8.7 → `retired` |
| `morelia_descifra` | 0/20 | 105 | `rate_limiting.md` §5. Dropped per sprint §8.7 → `retired` |
| `orizaba` | 0/15 | 47 today → new Orizaba org | `customers/orizaba/customer.yaml` (reuses rows 9–16, 18, 80, 86). Sprint §2.3/§8.7 → import once the new org exists (Q1) |
| `queretaro` | 0/8 | 93 | `rate_limiting.md` §5 |
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
   proto06_test_apify_client`, `rate_limiting.md` §10) and to local userdb `:5334`
   (`dev/docs/db/userdb_runbook.md`). Seed `crawl_actor_classes` from the registry.
   Applying it to stg proto06 needs Oscar's go.
2. **Import script** `apify_client/src/scripts/import_tasks_xlsx.py` (to be written).
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
| Q3 | Where does the scheduler live: a tick in `search-sentiment-poller-tasker` (has userdb + 1-min loop), the backend, or the `apify_client` worker? | poller tick, admitting through `usage_client.admission` (same code as the backend) |
| Q4 | Run queue vhost: `document_processing_stg`/prod (user logic, touches userdb) while documents still publish to the document vhost — OK to have two connections? | yes, consume user-logic vhost, publish documents vhost; dev = `document_processing_dev` for both |
| Q5 | DB role for the worker (`backend` or a dedicated `crawler` role with DML on `crawl_*` + INSERT on `usage_events`)? | dedicated role (+ SELECT on `user_searches` for the org triggers, §2.8) |
| Q6 | Split the duplicate legacy ids 35/36/37 (lose one cache pass) or keep the shared key? | split |
| Q7 | RBAC source endpoints to clone | **Decided 2026-10-06**: READ mirrors `/savedsearches/get`; PROPOSE (`/crawl/tasks/propose`, `/crawl/comments/request`) mirrors `/savedsearches/save`; OPERATE (create/update/set_status/link/unlink/schedule, `/crawl/runs/request`, `/crawl/runs/cancel`, `/crawl/comments/approve`) = `/savedsearches/save` owner rows only — the owner seat of every customer org is a deepriver.ai service user. Script: `media-backend-paid/docs/db/migrations/2026-10-06_crawl_rbac_update_userdb.sql` |
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
> **Metering.** Every Apify call writes one `usage_events` row (`service='apify-client'`,
> `tool='apify'`, `kind` = `crawl`|`comments`, `unit='items'`, `event_id =
> 'apify-client:<apify_run_id>:<org_id>:<kind>:0'`). Admission (`usage_limits`) happens
> before the run row is queued. See the rate-limiting spec
> (`business_agent/docs/plans/rate_limiting.md` §4–§7).
