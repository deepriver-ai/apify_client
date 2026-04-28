"""
Run InstagramProfilePostsActor with a sample task.

Usage (from project root):
    ipython -i src/scripts/run_instagram_profile_posts.py
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

from src.actors.instagram.profile_posts import InstagramProfilePostsActor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

# ------------------------------------------------------------------
# Task parameters — edit these before running
# ------------------------------------------------------------------

# Profile URLs to scrape posts from
search_params = [
    "https://www.instagram.com/luluthepiggy_official/",
    "https://www.instagram.com/shuya_official/"
]

search_params = [
'https://www.instagram.com/p/DWVIVAIjFQf/',
'https://www.instagram.com/p/DWEBN3_EaxR/',
'https://www.instagram.com/reel/DVqHLokkT33/',
'https://www.instagram.com/reel/DCLtx_SPSf7/',
'https://www.instagram.com/reel/DP1R6zckbzk/',
'https://www.instagram.com/reel/DWncabjAOsj/',
'https://www.instagram.com/reel/DW4iBu_Ewxb/',
'https://www.instagram.com/reel/DUqFjjBDL5H/',
'https://www.instagram.com/reel/DXUMwjJkfG9/',
'https://www.instagram.com/reel/DUVn6KdjE4-/'
]

# Filtering
task_id = "ig_post_tst"          # unique task identifier (used for filter cache)
country_id = None  #"_484"         # Mexico (geoid prefix); None to skip location filter
language = None  #"es"             # ISO 639-1 code; None to skip language filter
period = None                      # "d" (1 day), "w" (7 days), "m" (30 days); None to skip
min_date = None                    # absolute datetime cutoff; mutually exclusive with period
max_results = 20                   # max posts to fetch from Apify
not_keywords = []                  # posts containing any keyword (case-insensitive) are dropped
llm_filter_condition = None        # Spanish-language LLM filter; None to skip
override_filters = False           # True to ignore filter cache and re-run all filters

# Content enrichment
fetch_attached_url = False         # fetch and parse URLs found in post captions
download_video = True              # download videos for Video/Reel posts
video_dir = "cache/media/instagram"  # directory to save downloaded videos
download_images = False            # not yet implemented
add_text_from_images = False       # not yet implemented (OCR)
add_subtitles = False              # not yet implemented
add_ai_transcription = False       # not yet implemented

# User author enrichment
enrich_followers = False           # scrape profile data (followers, bio, full name)
stats_max_age_days = 90            # skip profiles with stats fresher than this

# Comments
get_comments = True                # scrape comments for each post
max_comments = 5                   # max comments per post

# Apify actor params
results_type = "posts"             # "posts", "reels", "tagged"

# Whether to publish to RabbitMQ (False saves to cache/runs/ instead)
publish = False

# ------------------------------------------------------------------
# Run
# ------------------------------------------------------------------

actor = InstagramProfilePostsActor()

kwargs = {
    "task_id": task_id,
    "country_id": country_id,
    "language": language,
    "period": period,
    "min_date": min_date,
    "max_results": max_results,
    "not_keywords": not_keywords,
    "llm_filter_condition": llm_filter_condition,
    "override_filters": override_filters,
    "fetch_attached_url": fetch_attached_url,
    "download_video": download_video,
    "video_dir": video_dir,
    "download_images": download_images,
    "add_text_from_images": add_text_from_images,
    "add_subtitles": add_subtitles,
    "add_ai_transcription": add_ai_transcription,
    "enrich_followers": enrich_followers,
    "stats_max_age_days": stats_max_age_days,
    "get_comments": get_comments,
    "max_comments": max_comments,
    "results_type": results_type,
}

documents = actor.search(search_params, **kwargs)
print(f"\nGot {len(documents)} documents (post-filter)")


#with open("/Users/oscarcuellar/Downloads/lulupiggie_shuya_20.json", "w", encoding="utf-8") as f:
#    json.dump([d.data for d in documents],f)


# ------------------------------------------------------------------
# Save results
# ------------------------------------------------------------------

ts = datetime.now().strftime("%Y%m%d_%H%M%S")
runs_dir = os.path.join("cache", "runs")
os.makedirs(runs_dir, exist_ok=True)

if publish:
    from src.helpers.rabbitmq import close_client, publish as rmq_publish
    for doc in documents:
        rmq_publish(json.dumps(doc.to_final_schema()))
    close_client()
    print(f"Published {len(documents)} documents to RabbitMQ")
else:
    filename = f"instagram_profile_posts_id_{task_id}_{ts}.json"
    filepath = os.path.join(runs_dir, filename)
    results = [doc.to_final_schema() for doc in documents]
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print(f"Saved {len(documents)} documents to {filepath}")

# ------------------------------------------------------------------
# Post-processing: describe videos with Gemini, section by section
# ------------------------------------------------------------------

# When running interactively against a previously saved set of docs, swap the
# block above for a json.load() of cache/runs/*.json. By default we operate on
# the in-memory `documents` produced by `actor.search()` above.

import sys                                                                           
sys.path.insert(0, '/Users/oscarcuellar/ocn/media/schema_tools/src')

from google import genai

from src.scripts.ips.ai_describe import analyse_post

gemini_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

records = []
for doc in documents:
    doc_data = doc if isinstance(doc, dict) else getattr(doc, "data", {})
    record = analyse_post(gemini_client, doc_data)
    if record is not None:
        records.append(record)



analysis_path = os.path.join(runs_dir, f"instagram_profile_posts_analysis_{task_id}_{ts}.json")
with open(analysis_path, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2, default=str)
print(f"Saved {len(records)} analyses to {analysis_path}")

# Excel export — flatten nested dicts with dotted paths; lists of dicts (e.g.
# `characters`) become numbered subkeys: characters.1.outlook, characters.2.outlook, …
# Scalar lists are joined with " | ". One row per record, single sheet.
import pandas as pd
from datetime import datetime


def _excel_safe(v):
    if isinstance(v, datetime) and v.tzinfo is not None:
        return v.replace(tzinfo=None)
    return v


def _flatten_record(d, prefix="", out=None):
    if out is None:
        out = {}
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            _flatten_record(v, key, out)
        elif isinstance(v, list):
            if v and isinstance(v[0], dict):
                for i, item in enumerate(v, start=1):
                    _flatten_record(item, f"{key}.{i}", out)
            else:
                out[key] = " | ".join("" if x is None else str(x) for x in v)
        else:
            out[key] = _excel_safe(v)
    return out


flat_records = [_flatten_record(r) for r in records]

xlsx_path = os.path.join('~/Downloads', f"instagram_profile_posts_analysis_{task_id}_{ts}.xlsx")
df = pd.DataFrame(flat_records)
df.to_excel(xlsx_path, sheet_name="posts", index=False)
print(f"Saved {len(records)} analyses to {xlsx_path}")

# `documents` and `records` are available for interactive inspection in ipython.
