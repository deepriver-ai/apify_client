"""The hand-run path (src/run_searches.py) meters every Apify call through usage_client.

No network: fake Apify client and in-memory usage ledger (from test_crawl_worker).
"""
from __future__ import annotations

import pytest
from usage_client import MissingOrg

from src import run_searches
from src.models.crawl_task import CrawlTask
from src.tests.test_crawl_worker import FB_PAGE, FakeApify, FakeLedger, _no_es


@pytest.fixture(autouse=True)
def _isolated_caches(tmp_path, monkeypatch):
    monkeypatch.setattr("src.actors.actor.FILTER_CACHE_PATH", str(tmp_path / "filter_cache.json"))


def _task(theme, **over) -> CrawlTask:
    kw = dict(task_id="t-1", actor_class="facebook_page_posts", search_params=[FB_PAGE], theme=theme,
              actor_params={"add_text_from_images": False})
    kw.update(over)
    return CrawlTask(**kw)


@pytest.fixture
def apify(sample_facebook_posts_results, sample_facebook_comments):
    return FakeApify({"apify/facebook-posts-scraper": sample_facebook_posts_results,
                      "apify/facebook-comments-scraper": sample_facebook_comments})


def test_theme_org_comes_from_the_importer_map():
    assert run_searches.theme_org("pinal_amoles") == 106
    assert run_searches.theme_org("sjdr") == 101          # retired theme, org on record
    assert run_searches.theme_org("mc") is None           # no org on record (Q1)
    assert run_searches.theme_org(None) is None


def test_task_with_org_is_metered_under_it(apify):
    ledger = FakeLedger()
    actor = run_searches.build_actor(_task("pinal_amoles", get_comments=True), session="s1",
                                     ledger=ledger, apify_client=apify)
    with _no_es():
        actor.search([FB_PAGE], **_task("pinal_amoles", get_comments=True).to_actor_kwargs())
    events = ledger.recorded()
    assert len(events) == len(apify.calls) == 2
    assert {e.kind for e in events} == {"crawl", "comments"}
    for ev in events:
        assert ev.org_id == 106 and ev.scope == "org" and ev.service == "apify-client"
        assert ev.event_id.startswith(f"apify-client:run-searches:s1:t-1:106:{ev.kind}:")
    # every call carried usage_client's caps
    assert all("max_items" in c["kwargs"] and "max_total_charge_usd" in c["kwargs"] for c in apify.calls)


def test_task_without_org_is_refused_before_any_call(apify):
    ledger = FakeLedger()
    with pytest.raises(MissingOrg):
        run_searches.build_actor(_task("mc"), session="s1", ledger=ledger, apify_client=apify)
    assert apify.calls == []
    assert ledger.blocks == [{"path": "worker:apify-client", "tool": "apify", "kind": "crawl",
                              "reason": "missing_org"}]


def test_platform_flag_runs_orgless_task_under_the_platform_org(apify, monkeypatch):
    monkeypatch.setenv("PLATFORM_ORG_ID", "107")
    ledger = FakeLedger()
    actor = run_searches.build_actor(_task("mc"), session="s1", platform=True, ledger=ledger, apify_client=apify)
    with _no_es():
        actor.search([FB_PAGE], **_task("mc").to_actor_kwargs())
    (ev,) = ledger.recorded()
    assert ev.org_id == 107 and ev.scope == "global" and ev.kind == "crawl"


def test_over_limit_org_buys_nothing(apify):
    ledger = FakeLedger()
    ledger.spent(106, 15.0)
    actor = run_searches.build_actor(_task("pinal_amoles"), session="s1", ledger=ledger, apify_client=apify)
    from usage_client import AdmissionRefused

    with _no_es(), pytest.raises(AdmissionRefused):
        actor.search([FB_PAGE], **_task("pinal_amoles").to_actor_kwargs())
    assert apify.calls == [] and [b["reason"] for b in ledger.blocks] == ["over_limit"]
