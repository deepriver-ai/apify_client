"""Standalone scripts meter every Apify call through usage_client (src/helpers/metering.py).

No network: fake Apify client and in-memory usage ledger (from test_crawl_worker).
Covers org resolution (org > task file > theme > --platform, refusal without an org),
run_comment_tasks (kind comments), enrich_page_bios (kind profiles), src/sandbox.py
(kind crawl) and the bare-actor guard.
"""
from __future__ import annotations

from unittest.mock import patch

from usage_client.apify import APIFY_MIN_CHARGE_CAP_USD  # Apify refuses a lower charge cap
import pytest
from usage_client import AdmissionRefused, MissingOrg

from src.helpers import metering
from src.tests.test_crawl_worker import FakeApify, FakeLedger

POSTS = ["https://www.facebook.com/p/1", "https://www.facebook.com/p/2"]


@pytest.fixture(autouse=True)
def _isolated_caches(tmp_path, monkeypatch):
    monkeypatch.setattr("src.actors.actor.FILTER_CACHE_PATH", str(tmp_path / "filter_cache.json"))
    monkeypatch.delenv("PLATFORM_ORG_ID", raising=False)


# --------------------------------------------------------------------------- org resolution


class TestResolveOrg:
    def test_explicit_org_wins_over_theme(self):
        assert metering.resolve_org(org_id=62, theme="pinal_amoles", ledger=FakeLedger()) == (62, "org")

    def test_theme_maps_to_its_org(self):
        assert metering.resolve_org(theme="pinal_amoles", ledger=FakeLedger()) == (106, "org")

    def test_platform_only_when_asked(self, monkeypatch):
        monkeypatch.setenv("PLATFORM_ORG_ID", "107")
        assert metering.resolve_org(theme="mc", platform=True, ledger=FakeLedger()) == (107, "global")

    @pytest.mark.parametrize("kw", [{}, {"theme": "mc"}, {"org_id": 0}, {"org_id": True}])
    def test_no_org_is_refused_with_a_block_row(self, kw):
        ledger = FakeLedger()
        with pytest.raises(MissingOrg):
            metering.resolve_org(ledger=ledger, kind="comments", **kw)
        assert ledger.blocks == [{"path": "worker:apify-client", "tool": "apify", "kind": "comments",
                                  "reason": "missing_org"}]

    def test_bad_per_call_cap(self):
        with pytest.raises(ValueError):
            metering.metered_client(metering.meter(event_key="k", org_id=1, ledger=FakeLedger()),
                                    apify_client=FakeApify({}), max_items_per_call=0)


def test_bare_actor_cannot_spend():
    from src.actors import get_actor

    actor = get_actor("facebook_page_posts")
    with pytest.raises(MissingOrg):
        actor.client.actor("apify/facebook-posts-scraper").call(run_input={})
    with pytest.raises(MissingOrg):
        actor.run_actor({"startUrls": []})


# --------------------------------------------------------------------------- run_comment_tasks


def _task_file(**over):
    task = {"generated_at": "2026-10-07T10:00:00", "depth": {"deep": 50, "sample": 15},
            "tiers": {"deep": [{"post_url": POSTS[0], "n_comments": 80}],
                      "sample": [{"post_url": POSTS[1], "n_comments": 4}]}}
    task.update(over)
    return task


class TestRunCommentTasks:
    def test_tier_is_metered_as_comments_under_the_org(self, sample_facebook_comments):
        from src.scripts import run_comment_tasks as rct

        apify = FakeApify({"apify/facebook-comments-scraper": sample_facebook_comments})
        ledger = FakeLedger()
        (run,) = rct.plan_runs(_task_file(), ["deep"])
        meter = rct.tier_meter(_task_file(), "deep", session="s1", org_id=105, ledger=ledger)
        n, cost = rct.execute(run, "comments_x_deep", publish=False, usage_meter=meter,
                              cost_budget_usd=1.0, apify_client=apify)
        assert n > 0 and cost == pytest.approx(0.01)
        (ev,) = ledger.recorded()
        assert (ev.org_id, ev.kind, ev.scope, ev.service) == (105, "comments", "org", "apify-client")
        assert ev.event_id.startswith("apify-client:comment-tasks:s1:deep:105:comments:apifyrun-")
        (call,) = apify.calls
        assert call["kwargs"]["max_items"] == 1 * 50            # posts × depth
        # admitted = 50 comments × $0.002 + the $0.001 start (apify/facebook-comments-scraper), below the $1 budget
        assert float(call["kwargs"]["max_total_charge_usd"]) == pytest.approx(0.101)

    def test_cost_budget_caps_items(self, sample_facebook_comments):
        from src.scripts import run_comment_tasks as rct

        apify = FakeApify({"apify/facebook-comments-scraper": sample_facebook_comments})
        (run,) = rct.plan_runs(_task_file(), ["deep"])
        meter = rct.tier_meter(_task_file(), "deep", session="s1", org_id=105, ledger=FakeLedger())
        rct.execute(run, "t", publish=False, usage_meter=meter, cost_budget_usd=0.01, apify_client=apify)
        (call,) = apify.calls
        assert call["kwargs"]["max_items"] == 4                 # ($0.01 - $0.001 start) / $0.002 per comment
        assert float(call["kwargs"]["max_total_charge_usd"]) == pytest.approx(max(0.009, APIFY_MIN_CHARGE_CAP_USD))

    def test_task_file_org_is_used(self):
        from src.scripts import run_comment_tasks as rct

        meter = rct.tier_meter(_task_file(org_id=101), "deep", session="s1", ledger=FakeLedger())
        assert meter.org_id == 101 and meter.scope == "org"

    def test_no_org_refused_before_any_call(self):
        from src.scripts import run_comment_tasks as rct

        ledger = FakeLedger()
        with pytest.raises(MissingOrg):
            rct.tier_meter(_task_file(), "deep", session="s1", ledger=ledger)
        assert ledger.blocks and ledger.blocks[0]["kind"] == "comments"

    def test_over_limit_buys_nothing(self, sample_facebook_comments):
        from src.scripts import run_comment_tasks as rct

        apify = FakeApify({"apify/facebook-comments-scraper": sample_facebook_comments})
        ledger = FakeLedger()
        ledger.spent(105, 15.0, kind="comments")
        (run,) = rct.plan_runs(_task_file(), ["deep"])
        meter = rct.tier_meter(_task_file(), "deep", session="s1", org_id=105, ledger=ledger)
        with pytest.raises(AdmissionRefused):
            rct.execute(run, "t", publish=False, usage_meter=meter, apify_client=apify)
        assert apify.calls == []


# --------------------------------------------------------------------------- enrich_page_bios


class _FakeUsers:
    saved = {}

    def get_stats(self, url):
        return None

    def save_stats(self, url, stats):
        _FakeUsers.saved[url] = stats


class TestEnrichPageBios:
    def _run(self, argv, apify, ledger):
        from src.scripts import enrich_page_bios

        with patch("src.models.users_management.UsersManagement", _FakeUsers):
            return enrich_page_bios.main(argv, apify_client=apify, ledger=ledger)

    def test_scrape_is_metered_as_profiles(self):
        items = [{"facebookUrl": "https://www.facebook.com/pageA/", "intro": "Gobierno municipal",
                  "followers": 10}]
        apify = FakeApify({"apify/facebook-pages-scraper": items})
        ledger = FakeLedger()
        out = self._run(["--urls", "facebook.com/pageA", "facebook.com/pageB", "--theme", "zitacuaro",
                         "--max-cost-usd", "0.015"], apify, ledger)
        assert out["scraped"] == 1
        (ev,) = ledger.recorded()
        assert (ev.org_id, ev.kind) == (102, "profiles")
        assert ev.event_id.startswith("apify-client:page-bios:")
        (call,) = apify.calls
        assert call["kwargs"]["max_items"] == 1                 # 2 asked, $0.015 buys 1 at $0.0082
        assert float(call["kwargs"]["max_total_charge_usd"]) == pytest.approx(max(0.0082, APIFY_MIN_CHARGE_CAP_USD))

    def test_items_capped_to_profiles_asked_for(self):
        apify = FakeApify({})
        self._run(["--urls", "facebook.com/pageA", "facebook.com/pageB", "--org", "62"], apify, FakeLedger())
        (call,) = apify.calls
        assert call["kwargs"]["max_items"] == 2

    def test_without_org_nothing_is_scraped(self):
        apify = FakeApify({})
        with pytest.raises(MissingOrg):
            self._run(["--urls", "facebook.com/pageA"], apify, FakeLedger())
        assert apify.calls == []

    def test_dry_run_needs_no_org(self):
        apify = FakeApify({})
        assert self._run(["--urls", "facebook.com/pageA", "--dry-run"], apify, FakeLedger())["scraped"] == 0
        assert apify.calls == []


# --------------------------------------------------------------------------- sandbox


class TestSandbox:
    def test_platform_run_meters_both_actors(self, monkeypatch):
        from src import sandbox

        monkeypatch.setenv("PLATFORM_ORG_ID", "107")
        apify = FakeApify({})
        ledger = FakeLedger()
        sandbox.main(["--platform"], apify_client=apify, ledger=ledger)
        events = ledger.recorded()
        assert len(events) == len(apify.calls) == 2
        assert {(e.org_id, e.scope, e.kind) for e in events} == {(107, "global", "crawl")}
        assert [c["kwargs"]["max_items"] for c in apify.calls] == [20, 30]

    def test_refused_without_org(self):
        from src import sandbox

        apify = FakeApify({})
        with pytest.raises(MissingOrg):
            sandbox.main([], apify_client=apify, ledger=FakeLedger())
        assert apify.calls == []
