"""Comment-task producer — select the posts whose comments are worth buying.

The ``facebook_post_comments`` actor consumes a list of post URLs; nothing
produced those lists until now (Descifra plan 2.10). This script reads a scope's
posts already indexed in Elasticsearch, scores each post on four signals, and
writes a **task file** grouped by depth tier that ``run_comment_tasks.py`` turns
into actor runs. It never calls Apify itself — the cost is printed here, the
purchase happens in the runner (plan/apply split).

Signals per post:
  place    — the NER tagger (``NLP_URL``) finds COL / CALLE / LUG mentions in the
             post text (OCR image text included, it lives in the body).
  theme    — an LLM class over the snippet: obra · servicios · seguridad ·
             movilidad · gobierno · politica · otro (social docs carry no
             ``custom_categories``). Cached through ``llm_cached_call``.
  official — the post's ``source.name`` is one of ``--official-pages``.
  traction — ``fb_likes`` and the post's comment count.

Tiering (depth = comments per post, one actor run per tier because the actor's
``resultsLimit`` is global per call):
  deep (default 50): place AND theme ∈ {obra, servicios, seguridad, movilidad},
                     or an official page's obra/servicios post (with or without
                     place) — the "works announcement → colonia complaints" case.
  sample (default 15): theme match without place, place without theme, or a
                     top-traction post — the hypothesis test that comments on
                     these bring colonias too.
Posts whose stored comments already reach the tier depth are skipped, and posts
under ``--min-comments`` declared comments are skipped (nothing to buy).

Invocation (dry run = the default and the only mode; the file IS the output)::

    python -m src.scripts.select_posts_for_comments \
        --pages "Gobierno de Morelia" "Mi Morelia" [--phrases morelia] \
        --days 60 --official-pages "Gobierno de Morelia" \
        --context "Morelia, Michoacán" [--max-deep 60 --max-sample 120] \
        [--no-llm] [--out cache/comment_tasks/morelia.json]

Environment: ELASTIC_HOST/PORT/AUTH, NLP_URL (tagger), OPENROUTER_API_KEY.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

PRODUCER_VERSION = "v1"
THEMES = ("obra", "servicios", "seguridad", "movilidad", "gobierno", "politica", "otro")
PLACE_THEMES = {"obra", "servicios", "seguridad", "movilidad"}
WORKS_THEMES = {"obra", "servicios"}
PLACE_LEVELS = ("COL", "CALLE", "LUG")
LLM_BATCH_SIZE = 20
SNIPPET_LEN = 400
TAGGER_MAX_CHARS = 2000
# Default unit cost per post opened — the planning figure of the Descifra cost
# sheet (`alcance_geopolis.xlsx` «Costo por scope»); override with --usd-per-post.
DEFAULT_USD_PER_POST = 0.02

ES_FIELDS = ["source", "news_type", "url", "title", "text", "fb_likes", "fb_shares",
             "n_comments", "comments", "date_created", "author_name",
             "author_location_text"]

# Cheap lexical prior for works/services talk — used to rank inside a tier and
# as the theme when --no-llm is given.
_WORKS_RE = re.compile(
    r"\b(obra|obras|pavimenta|bache|bacheo|repavimenta|rehabilita|drenaje|agua potable|"
    r"sin agua|fuga|alumbrado|luminaria|basura|recolecci|colector|banqueta|puente|"
    r"vialidad|calle|colonia|comunidad|tenencia|ooapas|servicios públicos)\b", re.I)
_SECURITY_RE = re.compile(r"\b(balacera|asalt|robo|homicid|extorsi|policía|seguridad|"
                          r"detenid|violencia|secuestr)\b", re.I)
_MOBILITY_RE = re.compile(r"\b(tránsito|transporte|combi|ruta|semáforo|cierre|"
                          r"bloqueo|movilidad|ciclovía|estacionamiento)\b", re.I)


@dataclass
class Post:
    url: str
    source: str
    network: str
    date: str
    title: str
    text: str
    likes: int
    n_comments: int
    stored_comments: int
    author_location: Optional[str] = None
    places: List[Dict[str, Any]] = field(default_factory=list)
    theme: Optional[str] = None
    theme_conf: float = 0.0
    official: bool = False
    tier: Optional[str] = None
    reasons: List[str] = field(default_factory=list)

    @property
    def snippet(self) -> str:
        body = " ".join(x for x in [self.title, self.text] if x)
        return " ".join(body.split())[:SNIPPET_LEN]

    @property
    def has_place(self) -> bool:
        return bool(self.places)


# --- Stage A: harvest posts from ES ------------------------------------------

def get_es_client():
    from elasticsearch import Elasticsearch
    import urllib3
    urllib3.disable_warnings()
    host = os.getenv("ELASTIC_HOST", "localhost")
    port = int(os.getenv("ELASTIC_PORT", "9200"))
    auth = os.getenv("ELASTIC_AUTH", "")
    http_auth = tuple(auth.split(":", 1)) if ":" in auth else None
    return Elasticsearch([{"host": host, "port": port, "scheme": "https"}],
                         http_auth=http_auth, verify_certs=False)


def harvest_posts(es, pages: Optional[List[str]], phrases: Optional[List[str]],
                  networks: List[str], days: int, index: str = "news") -> List[Post]:
    """Posts in scope = pages (source.name) OR phrases (text/title), last ``days``."""
    from elasticsearch.helpers import scan

    if not pages and not phrases:
        raise ValueError("give --pages and/or --phrases")
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    scope: List[Dict[str, Any]] = []
    if pages:
        scope.append({"terms": {"source.name": pages}})
    for ph in phrases or []:
        scope.append({"match_phrase": {"text": ph}})
        scope.append({"match_phrase": {"title": ph}})
    query = {"query": {"bool": {"filter": [
        {"bool": {"should": scope, "minimum_should_match": 1}},
        {"range": {"date_created": {"gte": since}}},
        {"terms": {"news_type": [n.lower() for n in networks]}},
    ]}}}
    posts: List[Post] = []
    for hit in scan(es, index=index, query=query, _source=ES_FIELDS, size=500):
        s = hit.get("_source", {})
        url = s.get("url") or hit.get("_id")
        if not url:
            continue
        comments = s.get("comments") or []
        # The scraper's declared count lives at source.stats.n_comments on the ES
        # doc (top-level n_comments is never populated) — 2026-09-13 fix: reading
        # only the top level skipped every post as "< min comments".
        stats = (s.get("source") or {}).get("stats") or {}
        declared = s.get("n_comments")
        if declared is None:
            declared = stats.get("n_comments")
        try:
            declared = int(declared) if declared is not None else None
        except (TypeError, ValueError):
            declared = None
        posts.append(Post(
            url=url,
            source=(s.get("source") or {}).get("name") or "?",
            network=(s.get("news_type") or "").lower(),
            date=str(s.get("date_created") or "")[:10],
            title=s.get("title") or "",
            text=s.get("text") or "",
            likes=int(s.get("fb_likes") or 0),
            # declared count when the scraper carried it; else what we hold.
            n_comments=declared if declared is not None else len(comments),
            stored_comments=len(comments),
            author_location=s.get("author_location_text"),
        ))
    return posts


# --- Stage B: place signal (tagger) ------------------------------------------

def tag_places(posts: List[Post], nlp_url: str) -> int:
    """COL/CALLE/LUG mentions per post via the NER tagger. Returns tagger calls."""
    import requests

    calls = 0
    for p in posts:
        text = p.snippet if len(p.text) < 40 else " ".join(x for x in [p.title, p.text] if x)
        text = text[:TAGGER_MAX_CHARS]
        if not text.strip():
            continue
        try:
            r = requests.post(nlp_url, json={"text": text, "entities": 1}, timeout=30)
            r.raise_for_status()
            ents = (r.json() or {}).get("entities") or {}
            calls += 1
        except Exception as exc:  # tagger down → no place signal, keep going
            logger.warning("tagger failed for %s: %s", p.url, exc)
            continue
        for lvl in PLACE_LEVELS:
            for item in ents.get(lvl) or []:
                name = item[0] if isinstance(item, (list, tuple)) else str(item)
                if name and len(name) >= 3:
                    p.places.append({"level": lvl, "text": name})
    return calls


# --- Stage C: theme signal (LLM, cached) --------------------------------------

_THEME_SYSTEM = (
    "Clasifica publicaciones de redes sociales de un municipio mexicano por su tema "
    "principal. Responde SOLO con un arreglo JSON, un objeto por publicación, en el mismo "
    "orden, con la forma {\"i\": n, \"theme\": t, \"confidence\": c, \"names_place\": b}. "
    "t ∈ [obra, servicios, seguridad, movilidad, gobierno, politica, otro]: "
    "obra = obra pública, construcción, pavimentación, rehabilitación de calles/espacios; "
    "servicios = agua, drenaje, basura, alumbrado, parques, servicios municipales; "
    "seguridad = delitos, policía, violencia; movilidad = tránsito, transporte, cierres; "
    "gobierno = actos, anuncios o gestión del gobierno sin obra/servicio concreto; "
    "politica = partidos, candidatos, elecciones; otro = todo lo demás. "
    "names_place = true si la publicación nombra una colonia, calle, comunidad o lugar "
    "concreto dentro del municipio. c ∈ [0,1]."
)


def _lexical_theme(p: Post) -> Optional[str]:
    body = p.snippet
    if _WORKS_RE.search(body):
        return "obra" if re.search(r"obra|pavimenta|bache|rehabilita|puente|banqueta", body, re.I) else "servicios"
    if _SECURITY_RE.search(body):
        return "seguridad"
    if _MOBILITY_RE.search(body):
        return "movilidad"
    return None


def classify_themes(posts: List[Post], use_llm: bool, override: bool = False) -> int:
    """Fill ``theme``/``theme_conf``; lexical prior when the LLM is off. Returns LLM calls."""
    if not use_llm:
        for p in posts:
            p.theme = _lexical_theme(p) or "otro"
            p.theme_conf = 0.5 if p.theme != "otro" else 0.3
        return 0

    from src.oai.llm_core import get_text_content, llm_cached_call, parse_json_response

    calls = 0
    for start in range(0, len(posts), LLM_BATCH_SIZE):
        batch = posts[start:start + LLM_BATCH_SIZE]
        lines = [f"[{i}] ({p.source}) {p.snippet}" for i, p in enumerate(batch, start=1)]
        messages = [{"role": "system", "content": _THEME_SYSTEM},
                    {"role": "user", "content": "\n".join(lines)}]
        cache_key = hash(json.dumps([p.snippet for p in batch], ensure_ascii=False))

        def parse_fn(response):
            data = parse_json_response(get_text_content(response))
            if not isinstance(data, list):
                raise ValueError("expected a JSON array")
            return data

        def fallback_fn(_response):
            return [{"i": i + 1, "theme": _lexical_theme(p) or "otro", "confidence": 0.3}
                    for i, p in enumerate(batch)]

        result = llm_cached_call(
            cache_tag=f"comment_producer_theme_{PRODUCER_VERSION}",
            request_id=cache_key, cache_field="themes",
            messages_builder=lambda: messages, parse_fn=parse_fn,
            override=override, fallback_fn=fallback_fn,
        )
        calls += 1
        by_i = {int(r.get("i", 0)): r for r in result if isinstance(r, dict)}
        for i, p in enumerate(batch, start=1):
            r = by_i.get(i) or {}
            theme = str(r.get("theme") or "otro")
            p.theme = theme if theme in THEMES else "otro"
            try:
                p.theme_conf = max(0.0, min(1.0, float(r.get("confidence", 0.0))))
            except (TypeError, ValueError):
                p.theme_conf = 0.0
            if r.get("names_place") and not p.places:
                # the LLM saw a place the tagger missed — keep it as a weak place signal
                p.places.append({"level": "LLM", "text": "(names_place)"})
    return calls


# --- Stage D: tiering ---------------------------------------------------------

def assign_tiers(posts: List[Post], official_pages: List[str], deep: int, sample: int,
                 min_comments: int, top_traction: int) -> None:
    official = {o.casefold() for o in official_pages}
    ranked = sorted(posts, key=lambda p: (p.likes, p.n_comments), reverse=True)
    traction_urls = {p.url for p in ranked[:top_traction]}
    for p in posts:
        p.official = p.source.casefold() in official
        strong_place = any(pl["level"] in PLACE_LEVELS for pl in p.places)
        place_theme = p.theme in PLACE_THEMES
        if p.n_comments < min_comments:
            p.tier = None
            p.reasons.append(f"skip: {p.n_comments} comments < {min_comments}")
            continue
        if strong_place and place_theme:
            p.tier, r = "deep", f"place+{p.theme}"
        elif p.official and p.theme in WORKS_THEMES:
            p.tier, r = "deep", f"official {p.theme} post"
        elif place_theme or strong_place or p.url in traction_urls:
            p.tier, r = "sample", ("theme " + p.theme if place_theme else
                                   "place w/o theme" if strong_place else "top traction")
        else:
            p.tier = None
            p.reasons.append(f"skip: {p.theme}, no place, low traction")
            continue
        depth = deep if p.tier == "deep" else sample
        if p.stored_comments >= depth:
            p.reasons.append(f"skip: already hold {p.stored_comments} ≥ {depth}")
            p.tier = None
            continue
        p.reasons.append(r)


def _rank_key(p: Post):
    return (1 if any(pl["level"] in PLACE_LEVELS for pl in p.places) else 0,
            p.theme_conf, p.n_comments, p.likes)


# --- Stage E: output ----------------------------------------------------------

def build_task_file(posts: List[Post], args, stats: Dict[str, Any]) -> Dict[str, Any]:
    tiers: Dict[str, List[Dict[str, Any]]] = {"deep": [], "sample": []}
    for p in sorted(posts, key=_rank_key, reverse=True):
        if p.tier is None:
            continue
        if p.tier == "deep" and len(tiers["deep"]) >= args.max_deep:
            continue
        if p.tier == "sample" and len(tiers["sample"]) >= args.max_sample:
            continue
        tiers[p.tier].append({
            "post_url": p.url,                      # exact ES _id — never normalized
            "source": p.source, "network": p.network, "date": p.date,
            "n_comments": p.n_comments, "stored_comments": p.stored_comments,
            "likes": p.likes, "official": p.official,
            "theme": p.theme, "theme_conf": p.theme_conf,
            "places": [pl["text"] for pl in p.places if pl["level"] in PLACE_LEVELS],
            "reason": p.reasons[-1] if p.reasons else "",
            "snippet": p.snippet[:160],
        })
    depth = {"deep": args.deep, "sample": args.sample}
    n_posts = sum(len(v) for v in tiers.values())
    est_comments = sum(min(t["n_comments"], depth[k]) for k, v in tiers.items() for t in v)
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "producer_version": PRODUCER_VERSION,
        "scope": {"pages": args.pages, "phrases": args.phrases, "networks": args.networks,
                  "days": args.days, "context": args.context,
                  "official_pages": args.official_pages},
        "depth": depth,
        "stats": stats,
        "cost_estimate": {"posts": n_posts, "comments_expected": est_comments,
                          "usd_per_post": args.usd_per_post,
                          "usd": round(n_posts * args.usd_per_post, 2)},
        "tiers": tiers,
    }


def print_summary(task: Dict[str, Any]) -> None:
    st = task["stats"]
    print(f"\nposts harvested {st['harvested']} · with place {st['with_place']} · "
          f"themes {st['themes']} · tagger calls {st['tagger_calls']} · llm calls {st['llm_calls']}")
    for tier, rows in task["tiers"].items():
        print(f"\n[{tier}] depth {task['depth'][tier]} — {len(rows)} posts")
        for r in rows[:12]:
            places = ", ".join(r["places"][:3])
            print(f"  {r['date']} {r['source'][:28]:28} c={r['n_comments']:<4} l={r['likes']:<5} "
                  f"{r['theme']:<10} {places[:40]:40} {r['reason']}")
        if len(rows) > 12:
            print(f"  … {len(rows) - 12} more")
    ce = task["cost_estimate"]
    print(f"\ncost estimate: {ce['posts']} posts × ${ce['usd_per_post']} ≈ ${ce['usd']} "
          f"(≈{ce['comments_expected']} comments)")


# --- Orchestration ----------------------------------------------------------

def run(args: argparse.Namespace) -> Dict[str, Any]:
    from dotenv import load_dotenv
    load_dotenv()

    es = get_es_client()
    posts = harvest_posts(es, args.pages, args.phrases, args.networks, args.days)
    logger.info("harvested %d posts", len(posts))

    nlp_url = os.getenv("NLP_URL", "http://localhost:8210/tag")
    tagger_calls = tag_places(posts, nlp_url)
    llm_calls = classify_themes(posts, use_llm=not args.no_llm, override=args.override)
    assign_tiers(posts, args.official_pages or [], args.deep, args.sample,
                 args.min_comments, args.top_traction)

    themes: Dict[str, int] = {}
    for p in posts:
        themes[p.theme or "?"] = themes.get(p.theme or "?", 0) + 1
    stats = {"harvested": len(posts),
             "with_place": sum(1 for p in posts if any(pl["level"] in PLACE_LEVELS for pl in p.places)),
             "themes": themes, "tagger_calls": tagger_calls, "llm_calls": llm_calls,
             "skipped": sum(1 for p in posts if p.tier is None)}
    task = build_task_file(posts, args, stats)

    out = args.out or os.path.join("cache", "comment_tasks",
                                   f"comment_tasks_{datetime.now():%Y%m%d_%H%M%S}.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(task, f, ensure_ascii=False, indent=2)
    print_summary(task)
    print(f"\ntask file: {out}\nnext: python -m src.scripts.run_comment_tasks {out} [--apply]")
    return task


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pages", nargs="*", help="source.name values (page crawls)")
    ap.add_argument("--phrases", nargs="*", help="text/title phrases (keyword crawls)")
    ap.add_argument("--networks", nargs="*", default=["facebook"])
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--context", default=None, help="scope context for the record, e.g. 'Morelia, Michoacán'")
    ap.add_argument("--official-pages", nargs="*", default=[])
    ap.add_argument("--deep", type=int, default=50, help="comments per post, deep tier")
    ap.add_argument("--sample", type=int, default=15, help="comments per post, sample tier")
    ap.add_argument("--max-deep", type=int, default=60)
    ap.add_argument("--max-sample", type=int, default=120)
    ap.add_argument("--min-comments", type=int, default=3, help="skip posts with fewer declared comments")
    ap.add_argument("--top-traction", type=int, default=30, help="top-N posts by likes/comments enter the sample tier")
    ap.add_argument("--usd-per-post", type=float, default=DEFAULT_USD_PER_POST)
    ap.add_argument("--no-llm", action="store_true", help="lexical theme prior instead of the LLM class")
    ap.add_argument("--override", action="store_true", help="bypass the LLM cache")
    ap.add_argument("--out", default=None, help="task file path (default cache/comment_tasks/…)")
    return ap


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(build_arg_parser().parse_args())
