"""Batch social-account classifier (WS-4).

Builds per-account evidence from Elasticsearch (post authors + comment authors on
the given pages), computes deterministic activity/repetition features, optionally
joins userdb sentiment, and LLM-classifies only accounts with enough activity.
Results feed the ``SocialUsers`` Mongo store (``src/models/social_users.py``),
which reports join at read time — nothing is written to Elasticsearch.

Invocation::

    python -m src.scripts.classify_users \
        --pages "Roberto Cabrera Valencia" "Presidencia Municipal San Juan del Río" \
        [--networks facebook tiktok] [--days 60] \
        [--org 101 --entities 82 83] [--min-activity 2] \
        [--official-pages "Roberto Cabrera Valencia"] [--apply]

Default is a dry run: it prints a human-review table and writes a JSON report next
to ``cache/`` but does NOT write to Mongo. Pass ``--apply`` to upsert.

Environment (read from the process env / repo ``.env``; documented in CLAUDE.md):
    ELASTIC_HOST (default localhost), ELASTIC_PORT (default 9200),
    ELASTIC_AUTH ("user:pass") — connection is https with verify_certs=False.
    DATABASE_URI — optional; only needed for the sentiment join (--org/--entities).
    OPENROUTER_API_KEY — for the LLM classification stage.
    MONGO_* — for --apply (SocialUsers upsert).

Stages: A harvest (ES) → B deterministic features → C LLM classify (cost-gated)
→ D output (table + JSON report; upsert on --apply).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Bump to invalidate cached LLM classifications after a prompt/feature change.
CLASSIFIER_VERSION = "v1"

# LLM cost gates / batching.
LLM_BATCH_SIZE = 15
LLM_SAMPLE_TEXTS = 8
# Cap comment count fed into the O(n^2) pairwise-similarity feature.
SIMILARITY_SAMPLE_CAP = 30
# Burstiness: two of an account's comments on *different* posts within this many
# seconds of each other count as a burst pair (near-impossible for a human).
BURST_WINDOW_SECONDS = 120

ES_FIELDS = [
    "source", "news_type", "author_name", "url", "fb_likes",
    "comments", "date_created", "title", "text",
]


# --- Account model ----------------------------------------------------------

@dataclass
class Account:
    """Accumulated evidence for one social account across the harvest window."""

    tier: str
    network: str
    display_name: str
    key: str  # deterministic SocialUsers _id
    profile_url: Optional[str] = None
    followers: Optional[int] = None
    bio: Optional[str] = None
    n_posts: int = 0
    n_comments: int = 0
    post_texts: List[str] = field(default_factory=list)
    # each: {text, likes, timestamp, parent_doc_id, page}
    comment_items: List[Dict[str, Any]] = field(default_factory=list)
    pages: set = field(default_factory=set)
    parent_docs: set = field(default_factory=set)
    latest_ts: Optional[str] = None
    # filled downstream
    features: Dict[str, Any] = field(default_factory=dict)
    automation_score: float = 0.0
    automation_evidence: List[str] = field(default_factory=list)
    classification: str = "organico"
    confidence: float = 0.0
    anonymity: float = 0.0
    evidence: List[str] = field(default_factory=list)
    llm_used: bool = False

    @property
    def appearances(self) -> int:
        return self.n_posts + self.n_comments

    def note_ts(self, ts: Optional[str]) -> None:
        if ts and (self.latest_ts is None or str(ts) > str(self.latest_ts)):
            self.latest_ts = str(ts)

    def texts(self) -> List[str]:
        """All verbatim texts (comments first — they carry the automation signal)."""
        return [c["text"] for c in self.comment_items if c.get("text")] + \
               [t for t in self.post_texts if t]


# --- Stage A: harvest evidence from Elasticsearch ---------------------------

def get_es_client():
    """Build an Elasticsearch client from the process environment.

    host/port default to localhost:9200; scheme is https, verify_certs=False,
    basic auth from ``ELASTIC_AUTH`` ("user:pass").
    """
    from elasticsearch import Elasticsearch
    import urllib3
    urllib3.disable_warnings()

    host = os.getenv("ELASTIC_HOST", "localhost")
    port = int(os.getenv("ELASTIC_PORT", "9200"))
    auth = os.getenv("ELASTIC_AUTH", "")
    http_auth = tuple(auth.split(":", 1)) if ":" in auth else None
    return Elasticsearch(
        [{"host": host, "port": port, "scheme": "https"}],
        http_auth=http_auth,
        verify_certs=False,
    )


def _upsert_profile_account(accounts: Dict[str, Account], src: Dict[str, Any],
                            network: str,
                            author_name: Optional[str] = None) -> Optional[Account]:
    """Get-or-create the post-author account for an ES source object.

    ``author_name`` is the doc's top-level author_name — the most reliable
    display name across networks (populated even where source.name is the
    platform string, e.g. legacy TikTok docs)."""
    from src.models.social_users import (
        TIER_PROFILE, TIER_NAME, account_id, normalize_profile_url, normalize_name,
    )
    stats = src.get("stats") or {}
    name = author_name or src.get("name") or stats.get("author_full_name")
    profile_url = stats.get("profile_url")

    if profile_url:
        key = account_id(TIER_PROFILE, profile_url=profile_url)
        tier = TIER_PROFILE
    elif name:
        # No canonical URL — fall back to the (network, name) identity.
        key = account_id(TIER_NAME, network=network, name=name)
        tier = TIER_NAME
    else:
        return None

    acc = accounts.get(key)
    if acc is None:
        acc = Account(
            tier=tier, network=network,
            display_name=author_name or stats.get("author_full_name") or name or "",
            key=key,
            profile_url=normalize_profile_url(profile_url) if profile_url else None,
        )
        accounts[key] = acc
    # Best profile metadata seen wins.
    if stats.get("website_visits") is not None:
        acc.followers = stats.get("website_visits")
    if stats.get("author_profile_bio"):
        acc.bio = stats.get("author_profile_bio")
    return acc


def _upsert_comment_account(accounts: Dict[str, Account], author: str,
                            network: str) -> Optional[Account]:
    """Get-or-create the name-tier account for a comment author."""
    from src.models.social_users import TIER_NAME, account_id, normalize_name
    if not author or not normalize_name(author):
        return None
    key = account_id(TIER_NAME, network=network, name=author)
    acc = accounts.get(key)
    if acc is None:
        acc = Account(tier=TIER_NAME, network=network, display_name=author, key=key)
        accounts[key] = acc
    return acc


def harvest_evidence(es, pages: Optional[List[str]], networks: Optional[List[str]],
                     days: int, index: str = "news",
                     phrases: Optional[List[str]] = None) -> Dict[str, Account]:
    """Stage A. Scan ES for docs in scope over the last ``days`` and collect
    both post authors and every comment author into an accounts dict keyed by the
    deterministic SocialUsers id.

    Scope is pages OR phrases (either or both; at least one required):
    ``pages`` matches ``source.name`` (page-post crawls, where source carries the
    page name); ``phrases`` matches text/title (keyword-search content, where
    source.name is each author's OWN page and a page list can't capture it)."""
    from elasticsearch.helpers import scan

    if not pages and not phrases:
        raise ValueError("harvest_evidence needs pages and/or phrases")
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    scope: List[Dict[str, Any]] = []
    if pages:
        scope.append({"terms": {"source.name": pages}})
    for ph in phrases or []:
        scope.append({"match_phrase": {"text": ph}})
        scope.append({"match_phrase": {"title": ph}})
    filters: List[Dict[str, Any]] = [
        {"bool": {"should": scope, "minimum_should_match": 1}},
        {"range": {"date_created": {"gte": since}}},
    ]
    if networks:
        filters.append({"terms": {"news_type": [n.lower() for n in networks]}})
    query = {"query": {"bool": {"filter": filters}}}

    accounts: Dict[str, Account] = {}
    page_post_counts: Dict[str, int] = {}
    for hit in scan(es, index=index, query=query, _source=ES_FIELDS, size=500):
        src_doc = hit.get("_source", {})
        source = src_doc.get("source") or {}
        network = (src_doc.get("news_type") or "").lower() or "unknown"
        page_name = source.get("name")
        if page_name:
            page_post_counts[page_name] = page_post_counts.get(page_name, 0) + 1
        post_url = src_doc.get("url")
        post_ts = src_doc.get("date_created")

        # Post author.
        raw_author = src_doc.get("author_name")
        top_author = raw_author[0] if isinstance(raw_author, list) and raw_author else (
            raw_author if isinstance(raw_author, str) else None)
        author_acc = _upsert_profile_account(accounts, source, network, author_name=top_author)
        if author_acc is not None:
            author_acc.n_posts += 1
            if page_name:
                author_acc.pages.add(page_name)
            text = " ".join(x for x in [src_doc.get("title"), src_doc.get("text")] if x)
            if text:
                author_acc.post_texts.append(text.strip())
            author_acc.note_ts(post_ts)

        # Comment authors. Real docs carry a top-level ``comments`` array; older
        # docs may nest them under ``source.comments`` — check both.
        comments = src_doc.get("comments") or source.get("comments") or []
        seen_in_post: set = set()
        for c in comments:
            cauthor = c.get("comment_author")
            cacc = _upsert_comment_account(accounts, cauthor, network)
            if cacc is None:
                continue
            # Scrapers can capture the same comment twice on one post; an exact
            # (author, text) duplicate within a post is an artifact, not a
            # repetition signal — repetition only counts ACROSS posts.
            dup_key = (cacc.key, (c.get("comment_text") or "").strip().casefold())
            if dup_key in seen_in_post:
                continue
            seen_in_post.add(dup_key)
            cacc.n_comments += 1
            if page_name:
                cacc.pages.add(page_name)
            if post_url:
                cacc.parent_docs.add(post_url)
            cts = c.get("comment_timestamp")
            cacc.comment_items.append({
                "text": (c.get("comment_text") or "").strip(),
                "likes": c.get("comment_likes") or 0,
                "timestamp": cts,
                "parent_doc_id": post_url,
                "page": page_name,
            })
            cacc.note_ts(cts)

    return accounts, page_post_counts


# --- Stage B: deterministic features ----------------------------------------

_TWO_TOKEN_RE = re.compile(r"^\S+\s+\S+$")
_DIGIT_RE = re.compile(r"\d")


def _norm_text(t: str) -> str:
    return " ".join((t or "").lower().split())


def _comments_per_active_day(acc: "Account") -> float:
    """Mean comments per day over the account's active span (min 1 day)."""
    from datetime import datetime
    ts = []
    for c in acc.comment_items:
        t = c.get("timestamp")
        try:
            ts.append(datetime.fromisoformat(str(t).replace("Z", "+00:00")))
        except (ValueError, TypeError):
            pass
    if not ts:
        return 0.0
    span_days = max(1.0, (max(ts) - min(ts)).total_seconds() / 86400.0)
    return round(len(ts) / span_days, 2)


def compute_features(acc: Account, official_pages: Optional[List[str]] = None,
                     page_post_counts: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    """Stage B. Pure-Python activity, repetition, engagement, burstiness and
    name-shape features for one account. No LLM, no embeddings."""
    comment_texts = [c["text"] for c in acc.comment_items if c.get("text")]
    norm_texts = [_norm_text(t) for t in comment_texts if _norm_text(t)]

    # Repetition: largest cluster of identical (normalized) comments.
    max_dup = 0
    if norm_texts:
        counts: Dict[str, int] = {}
        for t in norm_texts:
            counts[t] = counts.get(t, 0) + 1
        max_dup = max(counts.values())

    # Mean pairwise similarity over a capped sample (difflib ratio).
    sample = norm_texts[:SIMILARITY_SAMPLE_CAP]
    sims: List[float] = []
    for i in range(len(sample)):
        for j in range(i + 1, len(sample)):
            sims.append(SequenceMatcher(None, sample[i], sample[j]).ratio())
    mean_sim = round(sum(sims) / len(sims), 3) if sims else 0.0

    # Engagement.
    likes = [c.get("likes") or 0 for c in acc.comment_items]
    mean_likes = round(sum(likes) / len(likes), 2) if likes else 0.0
    max_likes = max(likes) if likes else 0

    # Burstiness: comments on *different* posts within a short window.
    stamped: List[Tuple[datetime, Optional[str]]] = []
    for c in acc.comment_items:
        dt = _parse_dt(c.get("timestamp"))
        if dt is not None:
            stamped.append((dt, c.get("parent_doc_id")))
    stamped.sort(key=lambda x: x[0])
    burst = 0
    for i in range(1, len(stamped)):
        dt_prev, doc_prev = stamped[i - 1]
        dt_cur, doc_cur = stamped[i]
        if (dt_cur - dt_prev).total_seconds() <= BURST_WINDOW_SECONDS and doc_cur != doc_prev:
            burst += 1

    # Name shape (weak anonymity signal).
    name = acc.display_name or ""
    name_two_token = bool(_TWO_TOKEN_RE.match(name.strip()))
    name_has_digits = bool(_DIGIT_RE.search(name))

    return {
        "n_comments": acc.n_comments,
        "n_posts": acc.n_posts,
        "n_distinct_parent_docs": len(acc.parent_docs),
        "pages_touched": len(acc.pages),
        "max_duplicate_text_count": max_dup,
        # Share of comments that are copies of another (0 when all texts unique):
        # a lone pair with distinct texts must NOT read as 50% duplication.
        "duplicate_text_share": round((max_dup - 1) / (len(norm_texts) - 1), 3) if len(norm_texts) > 1 else 0.0,
        "mean_pairwise_similarity": mean_sim,
        "mean_comment_likes": mean_likes,
        "max_comment_likes": max_likes,
        "burst_count": burst,
        "burst_share": round(burst / acc.n_comments, 3) if acc.n_comments else 0.0,
        "name_two_token": name_two_token,
        # Systematic counter-messaging signals (2026-07-21, weighting decision:
        # content-aware, high-frequency, official-pages-only accounts tracking
        # every municipal action ARE strong automation indicators — we prefer
        # flagging an unusually devoted citizen over missing an LLM-era bot).
        "official_pages_only": bool(acc.pages) and bool(official_pages)
            and acc.pages <= set(official_pages),
        # MAX per-page ratio: an account tracking one page must not be diluted
        # by the other official pages' volume.
        "official_page_coverage": round(max(
            (sum(1 for c in acc.comment_items if c.get("page") == p) /
             max(1, page_post_counts.get(p, 1)))
            for p in acc.pages), 3)
            if acc.pages and page_post_counts and official_pages
               and acc.pages <= set(official_pages) else 0.0,
        "comments_per_active_day": _comments_per_active_day(acc),
        "name_has_digits": name_has_digits,
    }


def compute_automation(features: Dict[str, Any]) -> Tuple[float, List[str]]:
    """Deterministic automation score (0-1) + short evidence strings.

    Driven mainly by copy-paste repetition (identical text across many distinct
    posts) with a secondary burstiness signal. A single account posting the same
    text on many different posts saturates the score. Kept deterministic so it is
    stable without an LLM (and so the ground-truth Carlos-Martinez case scores
    high regardless of the LLM stage)."""
    score = 0.0
    evidence: List[str] = []

    max_dup = features.get("max_duplicate_text_count", 0)
    distinct = features.get("n_distinct_parent_docs", 0)
    mean_sim = features.get("mean_pairwise_similarity", 0.0)
    burst_share = features.get("burst_share", 0.0)

    # Identical text repeated across distinct posts is the strongest signal.
    if max_dup >= 3:
        rep = min(1.0, (max_dup - 2) / 8.0)  # 3→~0.12 ... 10→1.0
        score = max(score, rep)
        evidence.append(
            f"{max_dup} comentarios con texto idéntico"
            + (f" en {distinct} publicaciones distintas" if distinct else "")
        )
    # High average similarity across many comments (near-duplicate variants).
    if mean_sim >= 0.6 and features.get("n_comments", 0) >= 3:
        score = max(score, mean_sim)
        evidence.append(f"similitud media entre comentarios de {mean_sim}")
    # Bursts across different posts within a short window.
    if burst_share >= 0.3 and features.get("burst_count", 0) >= 2:
        score = max(score, 0.5 + burst_share / 2)
        evidence.append(
            f"{features.get('burst_count')} comentarios en ráfaga (<2min) en posts distintos"
        )

    # Systematic counter-messaging (decided 2026-07-21): an account that lives
    # ONLY on the customer's official pages, answers a large share of their
    # posts, and always with the same polarity, is treated as probable
    # automation even when every text differs — LLM-era bots are content-aware,
    # and we prefer flagging a devoted citizen over missing a bot.
    n = features.get("n_comments", 0)
    cov = features.get("official_page_coverage", 0.0)
    # dominant polarity carries the gate; strict extremity misses sarcasm
    # (LLM sentiment reads it as neutral).
    pol_consistent = features.get("dominant_polarity") in ("negativo", "positivo") and (
        features.get("extremity_share", 0.0) >= 0.5)
    if (features.get("official_pages_only") and
            ((n >= 5 and cov >= 0.4) or (n >= 10 and cov >= 0.22)) and pol_consistent):
        score = max(score, min(1.0, 0.6 + cov / 2))
        evidence.append(
            f"contramensaje sistemático: {n} comentarios solo en páginas oficiales, "
            f"cubre {int(cov*100)}% de sus publicaciones, polaridad constante "
            f"({features.get('dominant_polarity')})"
        )
    if features.get("comments_per_active_day", 0) >= 3 and n >= 6 and pol_consistent:
        score = max(score, 0.6)
        evidence.append(
            f"frecuencia sostenida: {features.get('comments_per_active_day')} comentarios/día activo, "
            f"polaridad constante"
        )

    return round(min(1.0, score), 3), evidence


# --- Optional sentiment join (userdb) ---------------------------------------

def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _minute_key(parent_doc_id: Optional[str], ts: Any) -> Optional[Tuple[str, str]]:
    """Join key: (parent_doc_id, timestamp truncated to the minute, UTC)."""
    dt = _parse_dt(ts)
    if not parent_doc_id or dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return (parent_doc_id, dt.strftime("%Y-%m-%dT%H:%M"))


def fetch_sentiment_rows(db_uri: str, org: int, entities: List[int],
                         parent_doc_ids: List[str]) -> List[Tuple[str, Any, str]]:
    """Read comment-sentiment rows for the given entities/org, restricted to the
    harvested parent posts. Returns (parent_doc_id, doc_date_created, sentiment)."""
    import psycopg2
    rows: List[Tuple[str, Any, str]] = []
    ids = list({p for p in parent_doc_ids if p})
    if not ids:
        return rows
    conn = psycopg2.connect(db_uri, connect_timeout=8)
    try:
        cur = conn.cursor()
        # Chunk the IN-list to keep the query bounded.
        for start in range(0, len(ids), 1000):
            chunk = ids[start:start + 1000]
            cur.execute(
                """
                SELECT parent_doc_id, doc_date_created, sentiment
                FROM entities_documents_sentiments_org
                WHERE doc_index = 'comment'
                  AND entity_id = ANY(%s)
                  AND org_id = %s
                  AND parent_doc_id = ANY(%s)
                """,
                (list(entities), org, chunk),
            )
            rows.extend(cur.fetchall())
    finally:
        conn.close()
    return rows


_EXTREME = {"positivo", "negativo"}
_NEUTRAL = {"neutral", "irrelevante", "mezclado"}


def apply_sentiment(accounts: Dict[str, Account],
                    rows: List[Tuple[str, Any, str]]) -> int:
    """Attribute sentiment rows to accounts' comments by (parent_doc_id, minute)
    and fold per-account extremity share + dominant polarity into features.
    Returns the number of comments matched. Degrades gracefully (no-op) when
    ``rows`` is empty."""
    lookup: Dict[Tuple[str, str], str] = {}
    for parent_doc_id, ts, sentiment in rows:
        key = _minute_key(parent_doc_id, ts)
        if key:
            lookup[key] = sentiment
    if not lookup:
        return 0

    matched = 0
    for acc in accounts.values():
        pos = neg = neu = 0
        for c in acc.comment_items:
            key = _minute_key(c.get("parent_doc_id"), c.get("timestamp"))
            sentiment = lookup.get(key) if key else None
            if sentiment is None:
                continue
            matched += 1
            if sentiment == "positivo":
                pos += 1
            elif sentiment == "negativo":
                neg += 1
            elif sentiment in _NEUTRAL:
                neu += 1
        total = pos + neg + neu
        if total:
            acc.features["sentiment_matched"] = total
            acc.features["extremity_share"] = round((pos + neg) / total, 3)
            if neg > pos and neg >= neu:
                acc.features["dominant_polarity"] = "negativo"
            elif pos > neg and pos >= neu:
                acc.features["dominant_polarity"] = "positivo"
            else:
                acc.features["dominant_polarity"] = "neutral"
    return matched


# --- Stage C: classification (deterministic bypass + cost-gated LLM) --------

def _precision_to_sitio(precision_level: Any) -> str:
    """Map a source's geocoding precision level to a sitio_* class."""
    try:
        lvl = int(precision_level)
    except (ValueError, TypeError):
        return "sitio_nacional"
    if lvl >= 3:
        return "sitio_local"
    if lvl == 2:
        return "sitio_estatal"
    return "sitio_nacional"


def deterministic_classification(
    acc: Account,
    official_pages_norm: set,
    sources_mgr: Any = None,
) -> Optional[Tuple[str, float, List[str]]]:
    """Return (class, confidence, evidence) when the account can be classified
    without the LLM: official pages → pagina_oficial; a name/URL that resolves to
    a known news-source domain → sitio_* by outlet geography. Else None."""
    from src.models.social_users import normalize_name

    # Match the display name; for post authors also match the page they post *as*
    # (a page's own posts carry author_full_name with a suffix, but source.name is
    # the clean page name and lands in acc.pages). Only post authors get the
    # page-name match — a commenter's acc.pages are pages they commented *on*.
    names = {normalize_name(acc.display_name)}
    if acc.n_posts > 0:
        names |= {normalize_name(p) for p in acc.pages}
    if names & official_pages_norm:
        return ("pagina_oficial", 0.95, ["página marcada como oficial"])

    if sources_mgr is not None:
        candidate = acc.profile_url or acc.display_name
        try:
            domain = sources_mgr.get_domain(candidate) if candidate else None
        except Exception:
            domain = None
        if domain and sources_mgr.is_known(domain):
            loc = sources_mgr.get_location(domain) or {}
            cls = _precision_to_sitio(loc.get("location_author_precision_level"))
            return (cls, 0.9, [f"dominio de medio conocido: {domain}"])

    return None


def build_llm_prompt(batch: List[Account]) -> List[Dict[str, str]]:
    """Spanish classification prompt for a batch of accounts."""
    from src.models.social_users import CLASSIFIER_CLASSES

    system = (
        "Eres un analista de redes sociales. Clasifica cada CUENTA en UNA de estas "
        "clases (vocabulario cerrado):\n"
        "- organico: persona real individual\n"
        "- pagina_oficial: página de gobierno, institución o marca oficial\n"
        "- politico: político, partido o cuenta de campaña\n"
        "- comunidad: página de comunidad, vecinal o de denuncia\n"
        "- sitio_local / sitio_estatal / sitio_nacional: medio de noticias que opera como cuenta social\n\n"
        "Para cada cuenta responde también automation_score (0-1, indicios de automatización: "
        "texto repetido, ráfagas), anonymity (0-1, sin nombre real/identidad verificable), "
        "confidence (0-1) y evidence (1-2 frases cortas).\n\n"
        "Responde SOLO con un arreglo JSON, un objeto por cuenta EN ORDEN, con la forma: "
        '[{"i": 1, "classification": "organico", "automation_score": 0.0, '
        '"anonymity": 0.0, "confidence": 0.7, "evidence": ["..."]}]\n'
        f"classification debe ser una de: {', '.join(CLASSIFIER_CLASSES)}."
    )
    lines = []
    for i, acc in enumerate(batch, start=1):
        texts = acc.texts()[:LLM_SAMPLE_TEXTS]
        parts = [
            f"[{i}] nombre: {acc.display_name!r}",
            f"red: {acc.network}",
        ]
        if acc.followers is not None:
            parts.append(f"seguidores: {acc.followers}")
        if acc.bio:
            parts.append(f"bio: {' '.join(acc.bio.split())[:300]}")
        parts.append(f"features: {json.dumps(acc.features, ensure_ascii=False)}")
        sample = " | ".join(t[:200] for t in texts)
        parts.append(f"textos: {sample}")
        lines.append("\n".join(parts))
    user = "\n\n".join(lines)
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def llm_classify_batch(batch: List[Account], override: bool = False,
                       model: Optional[str] = None) -> List[Dict[str, Any]]:
    """Send one batch of accounts to the LLM and return a list of parsed dicts
    (one per account, in order). Content-addressed cache via ``llm_cached_call``;
    on parse failure the fallback yields organico/low-confidence for the batch."""
    from src.oai.llm_core import llm_cached_call, get_text_content, parse_json_response

    messages = build_llm_prompt(batch)
    # Content-keyed cache id: names + features so identical evidence re-uses cache.
    cache_key = hash(json.dumps(
        [(a.display_name, a.network, a.features, a.texts()[:LLM_SAMPLE_TEXTS]) for a in batch],
        ensure_ascii=False, sort_keys=True, default=str,
    ))

    def parse_fn(response):
        data = parse_json_response(get_text_content(response))
        if not isinstance(data, list):
            raise ValueError("expected a JSON array")
        return data

    def fallback_fn(_response):
        return [{"i": i + 1, "classification": "organico", "automation_score": 0.0,
                 "anonymity": 0.0, "confidence": 0.1, "evidence": ["fallback: parse error"]}
                for i in range(len(batch))]

    return llm_cached_call(
        cache_tag=f"classify_users_{CLASSIFIER_VERSION}",
        request_id=cache_key,
        cache_field="classifications",
        messages_builder=lambda: messages,
        parse_fn=parse_fn,
        override=override,
        fallback_fn=fallback_fn,
    )


def _validate_llm_result(raw: Dict[str, Any], acc: Account) -> None:
    """Fold a validated LLM result into an account (strict, with safe fallbacks)."""
    from src.models.social_users import CLASSIFIER_CLASSES

    cls = str(raw.get("classification", "organico"))
    if cls not in CLASSIFIER_CLASSES:
        cls = "organico"
    acc.classification = cls
    acc.confidence = _clamp01(raw.get("confidence", 0.0))
    acc.anonymity = _clamp01(raw.get("anonymity", 0.0))
    llm_auto = _clamp01(raw.get("automation_score", 0.0))
    # The LLM alone must not mint mid-band scores: without deterministic
    # evidence and with thin activity, diffuse suspicion capped below the
    # observation band (72 accounts landed at 0.5 on vibes, 2026-07-21).
    if (not acc.automation_evidence) and acc.appearances < 5:
        llm_auto = min(llm_auto, 0.45)
    # Automation score: max of deterministic and LLM (repetition is ground truth).
    acc.automation_score = round(max(acc.automation_score, llm_auto), 3)
    ev = raw.get("evidence") or []
    if isinstance(ev, str):
        ev = [ev]
    acc.evidence = [str(e) for e in ev][:2]


def _clamp01(v: Any) -> float:
    try:
        return round(max(0.0, min(1.0, float(v))), 3)
    except (ValueError, TypeError):
        return 0.0


def classify_accounts(
    accounts: Dict[str, Account],
    min_activity: int,
    official_pages: List[str],
    sources_mgr: Any = None,
    override: bool = False,
    model: Optional[str] = None,
) -> int:
    """Stage C. Compute automation from features, apply deterministic bypasses,
    and LLM-classify only accounts with ``appearances >= min_activity``. Accounts
    below the gate are left as organico/low-confidence with no LLM call. Returns
    the number of LLM calls made."""
    from src.models.social_users import normalize_name
    official_norm = {normalize_name(p) for p in (official_pages or [])}

    pending: List[Account] = []
    for acc in accounts.values():
        acc.automation_score, acc.automation_evidence = compute_automation(acc.features)

        deterministic = deterministic_classification(acc, official_norm, sources_mgr)
        if deterministic is not None:
            acc.classification, acc.confidence, acc.evidence = deterministic
            continue

        if acc.appearances < min_activity:
            acc.classification = "organico"
            acc.confidence = 0.2
            acc.evidence = ["actividad insuficiente para clasificación (bajo umbral)"]
            continue

        pending.append(acc)

    llm_calls = 0
    for start in range(0, len(pending), LLM_BATCH_SIZE):
        batch = pending[start:start + LLM_BATCH_SIZE]
        results = llm_classify_batch(batch, override=override, model=model)
        llm_calls += 1
        # Align results to accounts by position (fall back to fallback shape).
        for idx, acc in enumerate(batch):
            raw = results[idx] if idx < len(results) and isinstance(results[idx], dict) else {}
            _validate_llm_result(raw, acc)
            acc.llm_used = True

    return llm_calls


# --- Stage D: output --------------------------------------------------------

def build_review_table(accounts: Dict[str, Account], limit: Optional[int] = None) -> str:
    """Human-review table sorted by automation_score desc."""
    ordered = sorted(accounts.values(),
                     key=lambda a: (a.automation_score, a.appearances), reverse=True)
    if limit:
        ordered = ordered[:limit]
    header = f"{'account':38} {'tier':7} {'class':14} {'auto':5} {'app':4}  evidence"
    lines = [header, "-" * len(header)]
    for a in ordered:
        ev = "; ".join((a.automation_evidence or a.evidence)[:1])
        lines.append(
            f"{a.display_name[:37]:38} {a.tier:7} {a.classification:14} "
            f"{a.automation_score:<5.2f} {a.appearances:<4} {ev[:60]}"
        )
    return "\n".join(lines)


def account_to_record(acc: Account, model: Optional[str]) -> Dict[str, Any]:
    from src.models.social_users import build_record
    return build_record(
        tier=acc.tier,
        network=acc.network,
        display_name=acc.display_name,
        profile_url=acc.profile_url,
        classification=acc.classification,
        classification_confidence=acc.confidence,
        automation_score=acc.automation_score,
        automation_evidence=acc.automation_evidence,
        anonymity=acc.anonymity,
        followers=acc.followers,
        features=acc.features,
        evidence_sample=acc.texts()[:5],
        evidence=acc.evidence,
        evidence_as_of=acc.latest_ts,
        llm_model=(model if acc.llm_used else None),
        classifier_version=CLASSIFIER_VERSION,
        n_comments=acc.n_comments,
        n_posts=acc.n_posts,
        n_distinct_parent_docs=len(acc.parent_docs),
        pages_touched=list(acc.pages),
    )


def write_report(accounts: Dict[str, Account], model: Optional[str],
                 out_dir: str = os.path.join("cache", "runs")) -> str:
    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"classify_users_{stamp}.json")
    records = [account_to_record(a, model) for a in accounts.values()]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    return path


# --- Orchestration ----------------------------------------------------------

def run(args: argparse.Namespace) -> Dict[str, Any]:
    from dotenv import load_dotenv
    load_dotenv()

    # Effective model name for provenance (the LLM call itself uses llm_core's
    # OpenRouter default unless OPENROUTER_MODEL / --model overrides it).
    effective_model = args.model or os.getenv("OPENROUTER_MODEL", "google/gemini-2.5-flash-lite")

    es = get_es_client()
    logger.info("Stage A: harvesting evidence from ES ...")
    if not args.pages and not args.phrases:
        raise SystemExit("give --pages and/or --phrases")
    accounts, page_post_counts = harvest_evidence(es, args.pages, args.networks, args.days,
                                                  phrases=args.phrases)
    logger.info("Harvested %d accounts", len(accounts))

    logger.info("Stage B: computing deterministic features ...")
    for acc in accounts.values():
        acc.features = compute_features(acc, official_pages=args.official_pages,
                                        page_post_counts=page_post_counts)

    matched = 0
    if args.org and args.entities:
        db_uri = os.getenv("DATABASE_URI")
        if not db_uri:
            logger.warning("DATABASE_URI not set — skipping sentiment join")
        else:
            parent_ids = [c.get("parent_doc_id") for a in accounts.values()
                          for c in a.comment_items]
            rows = fetch_sentiment_rows(db_uri, args.org, args.entities, parent_ids)
            matched = apply_sentiment(accounts, rows)
            logger.info("Sentiment join: %d rows, %d comments matched", len(rows), matched)

    sources_mgr = None
    try:
        from src.models.sources_management import SourcesManagement
        sources_mgr = SourcesManagement()
    except Exception as exc:
        logger.warning("SourcesManagement unavailable (%s) — outlet bypass disabled", exc)

    logger.info("Stage C: classifying (min-activity=%d) ...", args.min_activity)
    llm_calls = classify_accounts(
        accounts, args.min_activity, args.official_pages or [],
        sources_mgr=sources_mgr, override=args.override, model=effective_model,
    )
    logger.info("LLM batch calls: %d", llm_calls)

    table = build_review_table(accounts, limit=args.table_rows)
    print("\n" + table + "\n")

    report_path = write_report(accounts, effective_model)
    print(f"JSON report: {report_path}")

    if args.apply:
        from src.models.social_users import SocialUsers
        store = SocialUsers()
        upserts = 0
        for acc in accounts.values():
            record = account_to_record(acc, effective_model)
            store.upsert(record)
            upserts += 1
        print(f"Applied: upserted {upserts} SocialUsers records")
    else:
        print("Dry run (no --apply): nothing written to Mongo")

    return {
        "accounts": len(accounts),
        "llm_calls": llm_calls,
        "sentiment_matched": matched,
        "report_path": report_path,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Batch social-account classifier (WS-4).")
    p.add_argument("--pages", nargs="*", default=None, help="source.name values to harvest")
    p.add_argument("--phrases", nargs="*", default=None,
                   help="match_phrase scope over text/title — captures keyword-search "
                        "content whose source.name is the author's own page")
    p.add_argument("--networks", nargs="*", default=None, help="restrict to news_type values")
    p.add_argument("--days", type=int, default=60, help="lookback window in days")
    p.add_argument("--org", type=int, default=None, help="org_id for the sentiment join")
    p.add_argument("--entities", nargs="*", type=int, default=None,
                   help="entity_ids for the sentiment join")
    p.add_argument("--min-activity", dest="min_activity", type=int, default=2,
                   help="minimum appearances before an account is sent to the LLM")
    p.add_argument("--official-pages", dest="official_pages", nargs="*", default=None,
                   help="page names to map deterministically to pagina_oficial")
    p.add_argument("--model", default=None, help="LLM model override (else OPENROUTER_MODEL)")
    p.add_argument("--override", action="store_true", help="bypass LLM cache reads")
    p.add_argument("--table-rows", dest="table_rows", type=int, default=None,
                   help="limit rows printed in the review table")
    p.add_argument("--apply", action="store_true",
                   help="upsert results into the SocialUsers Mongo collection")
    return p


def main(argv: Optional[List[str]] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_arg_parser().parse_args(argv)
    run(args)


if __name__ == "__main__":
    main()
