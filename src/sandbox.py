from dotenv import load_dotenv
load_dotenv()

from src.actors.instagram.hashtags import InstagramHashtagActor
from src.actors.news.news_scraper import GoogleNewsActor

# Instagram
ig_actor = InstagramHashtagActor()
#ig_results = ig_actor.search(["valvoline"], results_limit=20, keyword_search=False)
ig_results = ig_actor.search(["totalenergies"], results_limit=20, keyword_search=False)

for post in sorted(ig_results, key=lambda x: x.data['timestamp'], reverse=True):
    body = post.data["body"]
    print(f"{post.data['timestamp']} {post.data['author']}: {body[:80] if body else '(no caption)'}...")
    print(f"  url: {post.data['url']}")
    print(f"  likes: {post.data['likes']} | comments: {post.data['n_comments']} | shares: {post.data['shares']}")
    print()


# Google News
news_actor = GoogleNewsActor()
news_results = news_actor.search(["GNP"], max_articles=30, timeframe="1d")

for article in news_results:
    print(f"{article.data['source']}: {article.data['title']}")
    print(f"  url: {article.data['url']}")
    print(f"  date: {article.data['timestamp']}")
    print()
