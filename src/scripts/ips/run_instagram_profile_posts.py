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

from openai import OpenAI

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

# Filtering
task_id = "ig_profile_te"          # unique task identifier (used for filter cache)
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
max_comments = 25                  # max comments per post

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

if publish:
    from src.helpers.rabbitmq import close_client, publish as rmq_publish
    for doc in documents:
        rmq_publish(json.dumps(doc.to_final_schema()))
    close_client()
    print(f"Published {len(documents)} documents to RabbitMQ")
else:
    runs_dir = os.path.join("cache", "runs")
    os.makedirs(runs_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"instagram_profile_posts_id_{task_id}_{ts}.json"
    filepath = os.path.join(runs_dir, filename)

    results = [doc.to_final_schema() for doc in documents]
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)

    print(f"Saved {len(documents)} documents to {filepath}")

# ------------------------------------------------------------------
# Post-processing: describe videos with LLM
# ------------------------------------------------------------------

#with open("/Users/oscarcuellar/Downloads/lulupiggie_test.json", "r", encoding="utf-8") as f:
with open("/Users/oscarcuellar/Downloads/lulupiggie_shuya_20.json", "r", encoding="utf-8") as f:
    documents = json.load(f)


VIDEO_DESCRIPTION_PROMPT = """You are analyzing an Instagram video post. Describe the video content in detail, including:
What is shown:
    - label: theme of the video, list of keywords (e.g. 'romantic love', 'friendship', 'enjoyment oneself', 'tricking others', 'funny')
    - objects: list of objects
    - setting: settings of scenes
    - actions
    - text_overlays
    - plot: Describe the plot/image of the post within 100 words
    - interactions: Describe the interactions between the characters if there are more than one
    - visual_style: list of keywords including Digital illustration, animations, hand painting, mascot, product photo, animation, open to other suggested keywords that you see
    - illustration: what art/drawing style is it? (concrete line / no line etc)
    - background_image: What is the background image? Pure color, motif, scene etc
    - message: What is the main character's positioning or messaging?
    
- For each character, describe it (list of characters):
    - description: description of the character
    - actions
    - personality
    - theme: Is there a common theme underlying the content of the character?
    - emotions: (happy, lazy, gloomy, etc)
    - key_descriptives: Key descriptives of the IP: Cute/Kawaii, lazy, chilled, funny, moody, dumb, etc.
    - iconic_elements: What is visually iconic about this IP compared to others
    - colors: Primary and secondary colors

Respond in English.

Caption: {caption}

Return a json with a description for each of the keys
Answer only with the json, no other text or interaction.
Describe the video:"""

import time

from google import genai
from google.genai import types as genai_types

gemini_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
GEMINI_MODEL = "gemini-2.5-pro"


def _wait_for_active(file_obj, timeout_s: int = 300, poll_s: float = 2.0):
    """Poll the Gemini file until state is ACTIVE (or FAILED/timeout)."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        f = gemini_client.files.get(name=file_obj.name)
        if f.state.name == "ACTIVE":
            return f
        if f.state.name == "FAILED":
            raise RuntimeError(f"Gemini file processing failed: {f.name}")
        time.sleep(poll_s)
    raise TimeoutError(f"Gemini file {file_obj.name} did not become ACTIVE within {timeout_s}s")

video_descriptions = {}

for doc in documents:
    video_path = doc.get("video_filename")
    if post_url in video_descriptions or not video_path or not os.path.exists(video_path):
        continue

    post_url = doc.get("url", "unknown")
    caption = doc.get("body") or ""
    print(f"\nDescribing video for {post_url} ...")

    # Upload the video file (Gemini natively supports video input)
    uploaded = gemini_client.files.upload(file=video_path)
    uploaded = _wait_for_active(uploaded)

    response = gemini_client.models.generate_content(
        model=GEMINI_MODEL,
        contents=[
            uploaded,
            VIDEO_DESCRIPTION_PROMPT.format(caption=caption),
        ],
        config=genai_types.GenerateContentConfig(temperature=0.3),
    )

    description = response.text
    video_descriptions[post_url] = description
    print(f"  -> {description[:200]}...")

print(f"\nDescribed {len(video_descriptions)} videos")

# The `documents` and `video_descriptions` dicts are available for interactive inspection in ipython
