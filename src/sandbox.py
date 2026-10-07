"""Scratch run of two actors (Instagram hashtags, Google News) — prints what comes back.

    python -m src.sandbox --platform          # internal/dev: the platform org pays
    python -m src.sandbox --org 62            # or a customer org / --theme totalenergies

Every Apify call goes through ``usage_client.apify.run`` (rate_limiting.md §2.1, §2.3):
admitted against the org's limits, capped (``max_items`` = the results asked for,
``--max-cost-usd``), one ``usage_events`` row per Apify run (event key
``sandbox:<session>``). No org → refused before any Apify call. Needs ``USAGE_DB_URI`` (or
``DATABASE_URI``) and ``APIFI_API_TOKEN``.
"""
import argparse

from dotenv import load_dotenv


def main(argv=None, *, apify_client=None, ledger=None):
    load_dotenv()
    from src.helpers import metering

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    metering.add_usage_args(ap)
    args = ap.parse_args(argv)
    if ledger is None:
        ledger = metering.configure_usage_ledger()
    usage_meter = metering.meter(event_key=f"sandbox:{metering.new_session()}", org_id=args.org,
                                 theme=args.theme, platform=args.platform, ledger=ledger, what="sandbox")

    # Instagram
    ig_actor, ig_rec = metering.metered_actor("instagram_hashtags", usage_meter, apify_client=apify_client,
                                              search_item_budget=20, cost_budget_usd=args.max_cost_usd)
    #ig_results = ig_actor.search(["valvoline"], results_limit=20, keyword_search=False)
    ig_results = ig_actor.search(["totalenergies"], results_limit=20, keyword_search=False)

    for post in sorted(ig_results, key=lambda x: x.data['timestamp'], reverse=True):
        body = post.data["body"]
        print(f"{post.data['timestamp']} {post.data['author']}: {body[:80] if body else '(no caption)'}...")
        print(f"  url: {post.data['url']}")
        print(f"  likes: {post.data['likes']} | comments: {post.data['n_comments']} | shares: {post.data['shares']}")
        print()

    # Google News
    budget = None if args.max_cost_usd is None else max(args.max_cost_usd - ig_rec.cost_usd, 0.0)
    news_actor, _ = metering.metered_actor("google_news", usage_meter, apify_client=apify_client,
                                           search_item_budget=30, cost_budget_usd=budget)
    news_results = news_actor.search(["GNP"], max_articles=30, timeframe="1d")

    for article in news_results:
        print(f"{article.data['source']}: {article.data['title']}")
        print(f"  url: {article.data['url']}")
        print(f"  date: {article.data['timestamp']}")
        print()
    return ig_results, news_results


if __name__ == "__main__":
    main()
