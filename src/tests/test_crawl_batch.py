"""Batched crawl runs in the worker (crawl_runs.kind = 'batch', docs/db/crawl_tasks.md §6.5).

No network: the fakes of test_crawl_worker.py (Apify, userdb, usage ledger, publisher), plus
the members table. Covers: the run built from the merged spec + crawl_run_tasks, the X OR
query of the members' clauses, the filter cache over the batch key and the members' keys,
per-member attribution, schedule feedback for every member, and bad batch runs failing
without spend. The SQL side (as crawler_worker) is in test_crawl_worker_db.py.
"""
from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest

from src.actors.twitter.keyword_search import TwitterKeywordSearchActor
from src.models.crawl_task import CrawlTask
from src.tests.test_crawl_worker import (  # noqa: F401  (fixtures by name)
    ACTOR_CLASSES, FB_PAGE, FakeApify, FakeDB, FakeLedger, _isolated_caches, _message, _no_es, _run_row, world,
)
from src.worker import runner as runner_mod
from src.worker.batch import BatchContext, BatchMember, normalize
from src.worker.runner import ACK, handle_message

X = "api-ninja/x-twitter-advanced-search"
BATCH_KEY = "batch:" + "b" * 40
MEMBERS = [
    {"task_id": 43, "search_params": ["shell"], "max_items": 20, "filter_cache_key": "db:43"},
    {"task_id": 49, "search_params": ["castrol"], "max_items": 20, "filter_cache_key": "49"},   # legacy key
    {"task_id": 103, "search_params": ["huachicol", "nuevo león"], "max_items": 30, "filter_cache_key": "db:103"},
]


def _spec(**over) -> dict:
    spec = {"name": "batch of tasks 43, 49, 103", "actor_class": "twitter_keyword_search",
            "search_params": sorted(["shell", "castrol", "huachicol", "nuevo león"]), "country_id": None,
            "language": None, "max_results": 70, "min_date": None, "period": "d", "publish": True,
            "get_comments": False, "max_comments": 15, "not_keywords": [], "llm_filter_condition": None,
            "update_existing": False, "enrich_followers": False, "fetch_attached_url": False,
            "classify_users": False, "official_page": False, "actor_params": {}, "max_cost_usd_per_run": None,
            "metadata": {"batch_task_ids": [43, 49, 103]}}
    spec.update(over)
    return spec


def tweet(n: int, text: str, minutes_ago: int = 10) -> dict:
    ts = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return {"id": str(n), "url": f"https://x.com/u{n}/status/{n}", "text": text,
            "createdAt": ts.strftime("%a %b %d %H:%M:%S +0000 %Y"),
            "author": {"userName": f"u{n}", "name": f"User {n}"}}


TWEETS = [tweet(1, "Sube el precio de Shell en CDMX", 5), tweet(2, "Castrol y shell compiten", 30),
          tweet(3, "Operativo contra el huachicol en Nuevo León", 60), tweet(4, "Nada que ver con nada", 90)]


class BatchDB(FakeDB):
    def __init__(self):
        super().__init__()
        self.run_tasks: Dict[int, List[dict]] = {}
        self.attribution: Dict[int, List[dict]] = {}

    def get_run_tasks(self, run_id):
        return [dict(r) for r in self.run_tasks.get(run_id, [])]

    def update_run_tasks(self, run_id, rows):
        self.attribution[run_id] = json.loads(json.dumps(rows, default=str))
        return True


@pytest.fixture
def batch_world(world):
    db0, apify0, deps, published = world
    db = BatchDB()
    db.actor_classes = copy.deepcopy(ACTOR_CLASSES)
    db.actor_classes["twitter_keyword_search"] = {"actor_class": "twitter_keyword_search", "run_kind": "crawl",
                                                  "enabled": True, "cap_scope": "per_call", "params_schema": {},
                                                  "batch_max_params": 25}
    db.runs[1234] = _run_row(kind="batch", task_id=None, max_items=70, trigger="schedule",
                             params={"override_filters": False, "task_id": BATCH_KEY, "spec": _spec(),
                                     "batch": {"task_ids": [43, 49, 103]}})
    db.run_tasks[1234] = copy.deepcopy(MEMBERS)
    apify = FakeApify({X: copy.deepcopy(TWEETS)}, cost_per_call=0.04)
    deps.db = db
    deps.apify_client_factory = lambda: apify
    msg = _message(kind="batch", task_id=None, max_items=70, max_cost_usd=None)
    msg["params"] = copy.deepcopy(db.runs[1234]["params"])
    return db, apify, deps, published, msg


# ---------------------------------------------------------------- the run

class TestBatchRun:
    def test_one_apify_call_with_the_members_or_query(self, batch_world):
        db, apify, deps, published, msg = batch_world
        with _no_es():
            assert handle_message(json.dumps(msg), deps) == ACK
        run = db.runs[1234]
        assert run["status"] == "succeeded", run.get("error")
        (call,) = apify.calls
        assert call["actor_id"] == X
        ri = call["run_input"]
        assert ri["query"] == '((shell) OR (castrol) OR (huachicol "nuevo león"))'
        assert not {"contentKeywords", "contentExactPhrases", "contentHashtags"} & set(ri)
        assert ri["numberOfTweets"] == 70 and ri["timeWithinTime"] == "1d"
        # per_call: the run's item budget is Σ member caps (70), sent as numberOfTweets. (Apify
        # max_items itself comes out a little higher: usage_client v0.2.1 admits start/unit extra
        # units — reported, not changed here.)
        assert call["kwargs"]["max_items"] >= 70
        assert run["items_paid"] == 4 and run["items_new"] == len(published) == 4
        assert run["stats"]["effective_params"]["task_id"] == BATCH_KEY
        assert run["stats"]["batch"] == {"members": 3, "items_in": 4, "unmatched": 1}
        assert "message_params_diff" not in run["stats"]
        # metered once, as a crawl, under the run's org
        (event,) = deps.usage_ledger.recorded()
        assert event.kind == "crawl" and event.event_id.startswith("apify-client:crawl-run:1234:106:crawl:")

    def test_member_attribution(self, batch_world):
        db, apify, deps, published, msg = batch_world
        with _no_es():
            handle_message(json.dumps(msg), deps)
        rows = {r["task_id"]: r for r in db.attribution[1234]}
        assert {t: (r["items_matched"], r["items_new"]) for t, r in rows.items()} == {43: (2, 2), 49: (1, 1), 103: (1, 1)}
        # cost 0.04 split by matched items, the unmatched tweet spread evenly: weights 2⅓, 1⅓, 1⅓
        assert sum(r["cost_usd"] for r in rows.values()) == pytest.approx(0.04, abs=1e-5)   # rounded to 6 places
        assert rows[43]["cost_usd"] == pytest.approx(0.04 * (7 / 3) / 5, abs=1e-6)
        newest_43 = datetime.fromisoformat(rows[43]["newest_item_at"])
        assert abs(newest_43 - (datetime.now(timezone.utc) - timedelta(minutes=5))) < timedelta(minutes=2)
        assert rows[103]["stats"]["match"] == "all_params" and rows[43]["stats"]["run_unmatched"] == 1

    def test_every_member_schedule_gets_feedback(self, batch_world):
        db, apify, deps, published, msg = batch_world
        with _no_es():
            handle_message(json.dumps(msg), deps)
        assert sorted(db.schedule_updates) == [(43, 1234, True), (49, 1234, True), (103, 1234, True)]

    def test_api_trigger_has_no_schedule_bookkeeping(self, batch_world):
        db, apify, deps, published, msg = batch_world
        db.runs[1234]["trigger"] = "api"
        with _no_es():
            handle_message(json.dumps(msg), deps)
        assert db.schedule_updates == []

    def test_cache_reads_member_keys_and_writes_batch_and_owner_keys(self, batch_world, tmp_path):
        db, apify, deps, published, msg = batch_world
        cache_path = tmp_path / "filter_cache.json"
        # task 49 alone dropped tweet 2 last time (its legacy key): the batch honours it
        cache_path.write_text(json.dumps({"filtered:49:https://x.com/u2/status/2": False}))
        with _no_es(["https://x.com/u1/status/1"]):                   # tweet 1 already in ES
            handle_message(json.dumps(msg), deps)
        run = db.runs[1234]
        assert run["items_cached_filtered"] == 1 and run["items_already_seen"] == 1 and run["items_new"] == 2
        cache = json.loads(cache_path.read_text())
        assert cache[f"filtered:{BATCH_KEY}:https://x.com/u1/status/1"] is False
        assert cache["filtered:db:43:https://x.com/u1/status/1"] is False      # tweet 1 is shell's
        assert "filtered:49:https://x.com/u1/status/1" not in cache           # not castrol's
        assert cache[f"filtered:{BATCH_KEY}:https://x.com/u4/status/4"] is True
        assert not any(k.endswith("/status/4") and not k.startswith(f"filtered:{BATCH_KEY}") for k in cache)

    def test_dry_run_attribution_counts_would_be_published(self, batch_world):
        db, apify, deps, published, msg = batch_world
        db.runs[1234]["dry_run"] = True
        with _no_es():
            handle_message(json.dumps(msg), deps)
        assert published == [] and db.runs[1234]["output_path"]
        assert sum(r["items_new"] for r in db.attribution[1234]) == 4


class TestBadBatchRuns:
    @pytest.mark.parametrize("mutate,why", [
        (lambda db: db.run_tasks.pop(1234), "no crawl_run_tasks members"),
        (lambda db: db.runs[1234]["params"].update(task_id="db:43"), "not a batch cache key"),
        (lambda db: db.runs[1234]["params"].pop("spec"), "bad params.spec"),
        (lambda db: db.run_tasks[1234].append({"task_id": 9, "search_params": ["bp"], "max_items": 20,
                                               "filter_cache_key": "db:9"}), "not in the merged spec"),
        (lambda db: db.runs[1234].update(task_id=43), "carries task_id"),
    ])
    def test_fails_without_spend(self, batch_world, mutate, why):
        db, apify, deps, published, msg = batch_world
        mutate(db)
        handle_message(json.dumps(msg), deps)
        run = db.runs[1234]
        assert run["status"] == "failed" and why in run["error"], run["error"]
        assert apify.calls == [] and db.calls == {}

    def test_missing_org_block_uses_the_crawl_usage_kind(self, batch_world):
        db, apify, deps, published, msg = batch_world
        handle_message(json.dumps({**msg, "org_id": None}), deps)
        (block,) = deps.usage_ledger.blocks
        assert block["kind"] == "crawl"

    def test_over_limit_blocks_and_leaves_schedules_alone(self, batch_world):
        db, apify, deps, published, msg = batch_world
        deps.usage_ledger.spent(106, 15.0)
        handle_message(json.dumps(msg), deps)
        assert db.runs[1234]["status"] == "blocked" and apify.calls == [] and db.schedule_updates == []


class TestBatchPlan:
    def test_per_param_budget_counts_the_union(self, batch_world):
        db, apify, deps, published, msg = batch_world
        db.runs[1234]["params"]["spec"] = _spec(actor_class="facebook_page_posts", max_results=10,
                                                search_params=[FB_PAGE, FB_PAGE + "x"],
                                                actor_params={"add_text_from_images": False}, period=None)
        db.run_tasks[1234] = [{"task_id": 1, "search_params": [FB_PAGE], "max_items": 10, "filter_cache_key": "db:1"},
                              {"task_id": 2, "search_params": [FB_PAGE + "x"], "max_items": 10,
                               "filter_cache_key": "db:2"}]
        db.runs[1234]["max_items"] = 10
        plan = runner_mod.plan_run(db.runs[1234], msg, deps)
        assert plan.search_item_budget == 20 and plan.kwargs["max_results"] == 10
        assert "batch_clauses" not in plan.kwargs and [m.task_id for m in plan.batch_members] == [1, 2]

    def test_from_spec_takes_the_batch_key(self):
        assert CrawlTask.from_spec(_spec(), cache_key=BATCH_KEY).task_id == BATCH_KEY


# ---------------------------------------------------------------- pieces

class TestTwitterQuery:
    def test_member_clause_matches_what_the_actor_sends_for_one_task(self):
        assert TwitterKeywordSearchActor.member_clause(["huachicol", "nuevo león"]) == 'huachicol "nuevo león"'
        assert TwitterKeywordSearchActor.member_clause(['"British Petroleum"', "#TotalEnergies"]) == \
            '"British Petroleum" #TotalEnergies'

    def test_batch_query_ors_clauses(self):
        q = TwitterKeywordSearchActor.batch_query([["bp"], ["exxon aceite"], ["#Shell", "precio"]])
        assert q == '((bp) OR ("exxon aceite") OR (precio #Shell))'

    def test_single_task_input_is_unchanged(self):
        captured = {}

        class _A(TwitterKeywordSearchActor):
            def run_actor(self, run_input):
                captured.update(run_input)
                return []

        _A().search(["huachicol", "nuevo león"], max_results=30, language="es", period="d")
        assert captured == {"search_type": "Latest", "numberOfTweets": 30, "contentKeywords": ["huachicol"],
                            "contentExactPhrases": ["nuevo león"], "contentLanguage": "es", "timeWithinTime": "1d"}


class TestMatcher:
    class Doc:
        def __init__(self, attached=None, **data):
            self.data = data
            self.attached_news = attached

    def ctx(self, actor="instagram_hashtags"):
        return BatchContext(actor, [BatchMember.from_row(r) for r in (
            {"task_id": 1, "search_params": ["huachicol", "nuevo león"], "max_items": 20, "filter_cache_key": "a"},
            {"task_id": 2, "search_params": ["#TotalEnergies"], "max_items": 20, "filter_cache_key": "b"},
            {"task_id": 3, "search_params": ["https://www.instagram.com/pemex/"], "max_items": 20,
             "filter_cache_key": "c"},
        )])

    def ids(self, ctx, doc):
        return [m.task_id for m in ctx.members_of(doc)]

    def test_any_param_accent_and_case_insensitive(self):
        assert self.ids(self.ctx(), self.Doc(body="Nuevo Leon hoy")) == [1]
        assert self.ids(self.ctx(), self.Doc(title="Gran #totalenergies", body="")) == [2]
        assert self.ids(self.ctx(), self.Doc(body="leonor")) == []                  # whole words only

    def test_all_params_for_x(self):
        ctx = self.ctx("twitter_keyword_search")
        assert self.ids(ctx, self.Doc(body="huachicol en nuevo león")) == [1]
        assert self.ids(ctx, self.Doc(body="huachicol a secas")) == []

    def test_url_params_match_the_account(self):
        doc = self.Doc(body="post", url="https://www.instagram.com/p/xyz/", profile_url="https://www.instagram.com/pemex")
        assert self.ids(self.ctx(), doc) == [3]

    def test_attached_article_counts(self):
        art = self.Doc(title="Huachicol", body="")
        assert self.ids(self.ctx(), self.Doc(body="mira esto", attached=art)) == [1]

    def test_normalize(self):
        assert normalize("¡Nuevo León!") == " nuevo leon "
