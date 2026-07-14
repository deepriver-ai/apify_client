from __future__ import annotations

import copy
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()


def _get_apify_token() -> str:
    token = os.getenv("APIFI_API_TOKEN")
    if not token:
        raise RuntimeError(
            "APIFI_API_TOKEN is not set. Refusing to create an unauthenticated ApifyClient: "
            "the Apify API rejects token-less run starts with a misleading "
            "'x402 payment header missing' error. Load the repo .env before using actors."
        )
    return token

# TODO: Replace file-based filter cache with Redis for multi-process/distributed support
FILTER_CACHE_PATH = os.path.join("cache", "filter_cache.json")

logger = logging.getLogger(__name__)

PERIOD_DAYS = {"d": 1, "w": 7, "m": 30}


LLM_FILTER_BATCH_SIZE = 20

LLM_FILTER_SYSTEM_PROMPT = """Eres un asistente de filtrado de contenido. Se te proporcionará una lista numerada de fragmentos de texto de publicaciones de redes sociales o noticias.

Tu tarea es aplicar la siguiente condición de filtrado y devolver ÚNICAMENTE los números de las publicaciones que DEBEN CONSERVARSE (que cumplen con los criterios).

Condición de filtrado:
{condition}

Responde SOLO con un arreglo JSON de números enteros, por ejemplo: [1, 3, 5]
Si ninguna publicación cumple los criterios, responde: []"""


class ApifyActor:
    """Base class for Apify actor wrappers.

    Subclasses must set ``actor_id`` and implement ``search()`` to create
    Documents from raw Apify results.

    After creating Documents, subclasses call ``process_documents()``
    which runs a staged pipeline ordered by cost:

        1. ``_filter_keywords``    — cheapest, substring match
        2. ``_filter_date``        — cheap, timestamp available from API
        3. ``_filter_existing_in_elasticsearch`` — cheap, URL ids in news
        4. ``_enrich_content``     — expensive (HTTP fetch + parse for news)
        5. ``_filter_language``    — cheap, needs body text from step 4
        6. ``_enrich_user_author`` — user profile enrichment (bio, followers)
        7. ``_enrich_location``    — potentially expensive (geocoding for social)
        8. ``_filter_location``    — cheap, geoid prefix match
        9. ``_filter_llm``         — LLM-based filtering (batched, expensive)
        10. ``_enrich_comments``   — comments enrichment

    Subclasses override individual stages to push filters to the API level
    or to provide actor-specific enrichment.
    """

    actor_id: str = ""  # Apify actor ID, set by subclass

    def __init__(self, client: Optional[ApifyClient] = None):
        self.client = client or ApifyClient(_get_apify_token())
        self.search_params_keywords: List[str] = []  # Should be set by the actor subclass when the scraping is keyword or hashtag-based
        self._filter_cache: Dict[str, bool] = self._load_filter_cache()
        self.filtered_documents: List[Dict[str, Any]] = []

    def run_actor(self, run_input: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Run the Apify actor and return raw results."""
        run = self.client.actor(self.actor_id).call(run_input=run_input)
        results = []
        for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
            results.append(item)
        return results

    # --- Staged pipeline ---

    def _filter_cache_key(self, doc, task_id: str) -> str:
        """Build a per-document, per-task cache key for filtering."""
        url = doc.data.get("url") or ""
        return f"filtered:{task_id}:{url}"

    def _reset_filtered_documents(self) -> None:
        """Reset per-run filtered document diagnostics."""
        self.filtered_documents = []

    def _record_filtered_documents(self, before: List, after: List, reason: str, **metadata) -> None:
        """Store documents removed by a filter stage with a machine-readable reason."""
        if not before:
            return
        kept_ids = {id(doc) for doc in after}
        removed = [doc for doc in before if id(doc) not in kept_ids]
        if not removed:
            return

        if not hasattr(self, "filtered_documents"):
            self.filtered_documents = []

        for doc in removed:
            entry = {
                "reason": reason,
                "url": doc.data.get("url"),
                "title": doc.data.get("title"),
                "type": doc.data.get("type"),
                "task_id": metadata.get("task_id"),
                "metadata": {k: v for k, v in metadata.items() if v is not None},
                "data": copy.deepcopy(doc.data),
                "document": doc,
            }
            self.filtered_documents.append(entry)

    def _filter_cached_documents(self, documents: List, task_id: str, override: bool = False) -> List:
        """Skip docs previously cached as filtered-out for this task."""
        if override or not task_id:
            return documents

        filtered = [
            doc for doc in documents
            if self._filter_cache.get(self._filter_cache_key(doc, task_id)) is not False
        ]
        self._record_filtered_documents(documents, filtered, "cached", task_id=task_id)
        if len(filtered) < len(documents):
            logger.info(
                "Filter cache: %d → %d documents (skipped previously filtered for task %s)",
                len(documents),
                len(filtered),
                task_id,
            )
        return filtered

    def process_documents(self, documents: List, **kwargs) -> List:
        """Run the full post-creation pipeline: filter → enrich → filter → enrich → filter.

        Stages are ordered cheapest-first so expensive enrichment steps
        only run on documents that survived earlier filters.

        Cache is keyed by (task_id, url) so the same document can have different
        filtering outcomes for different tasks. If ``override_filters`` is True,
        the cache is ignored and all filters are re-run from scratch.
        """
        override = kwargs.get("override_filters", False)
        task_id = kwargs.get("task_id", "")
        all_docs = documents
        self._reset_filtered_documents()

        # Skip docs already cached as filtered-out for this task (unless overriding)
        documents = self._filter_cached_documents(documents, task_id, override)

        documents = self._filter_keywords(documents, **kwargs)
        documents = self._filter_date(documents, **kwargs)
        documents = self._filter_existing_in_elasticsearch(documents, **kwargs)
        documents = self._enrich_content(documents, **kwargs)
        documents = self._filter_language(documents, **kwargs)
        documents = self._enrich_user_author(documents, **kwargs)
        documents = self._enrich_location(documents, **kwargs)
        documents = self._filter_location(documents, **kwargs)
        documents = self._filter_llm(documents, **kwargs)
        documents = self._filter_llm(documents, snippet_max_len=2500, **kwargs)
        documents = self._enrich_comments(documents, **kwargs)

        # Cache filtering results for this task
        if task_id:
            survived = {id(doc) for doc in documents}
            for doc in all_docs:
                key = self._filter_cache_key(doc, task_id)
                self._filter_cache[key] = id(doc) in survived
            self._save_filter_cache()

        return documents

    def _filter_keywords(self, documents: List, **kwargs) -> List:
        """Filter out documents containing any of the not_keywords. Cheapest filter (pure string match)."""
        not_keywords = kwargs.get("not_keywords", [])
        if not not_keywords:
            return documents
        before = len(documents)
        original = documents
        filtered = []
        for doc in documents:
            text = ((doc.data.get("body") or "") + " " + (doc.data.get("title") or "")).lower()
            if not any(kw.lower() in text for kw in not_keywords):
                filtered.append(doc)
        self._record_filtered_documents(original, filtered, "keyword", not_keywords=not_keywords)
        logger.info("Keyword filter (%d keywords): %d → %d documents", len(not_keywords), before, len(filtered))
        return filtered

    def _filter_date(self, documents: List, **kwargs) -> List:
        """Filter documents by min_date or period. Runs pre-enrichment (timestamp available from API)."""
        min_date = kwargs.get("min_date")
        period = kwargs.get("period")
        if not min_date and period:
            days = PERIOD_DAYS.get(period)
            if days:
                min_date = datetime.now() - timedelta(days=days)
        if not min_date or not isinstance(min_date, datetime):
            return documents
        before = len(documents)
        filtered = [doc for doc in documents if doc.matches_min_date(min_date)]
        self._record_filtered_documents(documents, filtered, "date", min_date=min_date.isoformat())
        logger.info("Date filter (min_date=%s): %d → %d documents", min_date.date(), before, len(filtered))
        return filtered

    def _filter_existing_in_elasticsearch(self, documents: List, **kwargs) -> List:
        """Drop documents whose URL already exists as an id in the news index."""
        if (
            kwargs.get("update_existing", False)
            or kwargs.get("check_existing_elasticsearch", True) is False
            or kwargs.get("skip_existing_filter", False)
            or not documents
        ):
            return documents

        urls = [doc.data.get("url") for doc in documents if doc.data.get("url")]
        if not urls:
            return documents

        existing_ids = self._existing_news_ids(urls)
        if not existing_ids:
            return documents

        before = len(documents)
        filtered = [
            doc for doc in documents
            if not doc.data.get("url") or doc.data.get("url") not in existing_ids
        ]
        self._record_filtered_documents(documents, filtered, "existing_elasticsearch", index="news")
        logger.info("Elasticsearch existing-doc filter: %d → %d documents", before, len(filtered))
        return filtered

    def _existing_news_ids(self, urls: List[str]) -> Set[str]:
        """Return URL ids that already exist in the Elasticsearch ``news`` index."""
        unique_urls = list(dict.fromkeys(urls))
        if not unique_urls:
            return set()

        try:
            from elastic_client import SearchClient

            search = SearchClient().raw_search("news")
            search = search.filter({"ids": {"values": unique_urls}}).source(False)[:len(unique_urls)]
            response = search.execute()
        except Exception as exc:
            logger.warning("Could not check existing documents in Elasticsearch: %s", exc)
            return set()

        return {hit.meta.id for hit in response.hits if getattr(hit.meta, "id", None)}

    def _enrich_content(self, documents: List, **kwargs) -> List:
        """Enrich document content. No-op by default; subclasses override."""
        return documents

    def _filter_language(self, documents: List, **kwargs) -> List:
        """Filter documents by language. Runs post-content-enrichment (needs body text)."""
        language = kwargs.get("language")
        if not language:
            return documents
        before = len(documents)
        filtered = [doc for doc in documents if doc.matches_language(language)]
        self._record_filtered_documents(documents, filtered, "language", language=language)
        logger.info("Language filter (%s): %d → %d documents", language, before, len(filtered))
        return filtered

    def _enrich_location(self, documents: List, **kwargs) -> List:
        """Enrich document location. Delegates to each document's enrich_location()."""
        for doc in documents:
            doc.enrich_location(**kwargs)
        return documents

    def _build_snippet(self, doc, max_len: int = 250, max_snippets: int = 4) -> str:
        """Build a text snippet for LLM filtering.

        If search_params_keywords are set (keyword-search actor), extracts ~max_len chars
        surrounding each keyword match (up to ``max_snippets``), joined with
        ``...``. Otherwise uses the first max_len chars of body.
        """
        body = doc.data.get("body") or ""
        body_lower = body.lower()
        snippets = []
        seen_ranges = []
        for kw in (self.search_params_keywords or []):
            kw_lower = kw.lower()
            start_search = 0
            while len(snippets) < max_snippets:
                idx = body_lower.find(kw_lower, start_search)
                if idx < 0:
                    break
                start = max(0, idx - max_len // 2)
                end = min(len(body), idx + len(kw) + max_len // 2)
                # Skip if this range overlaps with an already-collected snippet
                if any(s <= idx <= e for s, e in seen_ranges):
                    start_search = idx + len(kw)
                    continue
                snippets.append(body[start:end])
                seen_ranges.append((start, end))
                start_search = end
        if snippets:
            result = " ... ".join(snippets)
        else:
            result = body[:max_len]
        return " ".join(result.split())

    @staticmethod
    def _load_filter_cache() -> Dict[str, bool]:
        """Load the filter cache from disk."""
        if os.path.exists(FILTER_CACHE_PATH):
            try:
                with open(FILTER_CACHE_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (OSError, json.JSONDecodeError):
                logger.warning("Could not load filter cache, starting fresh")
        return {}

    def _save_filter_cache(self) -> None:
        """Persist the filter cache to disk."""
        os.makedirs(os.path.dirname(FILTER_CACHE_PATH), exist_ok=True)
        try:
            with open(FILTER_CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(self._filter_cache, f, ensure_ascii=False)
        except OSError:
            logger.warning("Could not save filter cache to %s", FILTER_CACHE_PATH)

    def _llm_filter_cache_id(self, doc, condition: str, snippet_max_len: int):
        """Per-document LLM-filter cache key: (url, condition, snippet_max_len).

        Keyed on the document URL (not on batch contents), so a decision is
        reused regardless of which batch the document later lands in, and survives
        across tasks that share the same condition. Returns None when the document
        has no URL (those documents can't be stably cached and are always re-sent).
        """
        url = doc.data.get("url")
        if not url:
            return None
        return (url, condition, snippet_max_len)

    def _filter_llm(self, documents: List, **kwargs) -> List:
        """Filter documents using an LLM based on a natural-language condition.

        Each document's keep/drop decision is cached per ``(url, condition,
        snippet_max_len)`` (see ``_llm_filter_cache_id``). Documents with a cached
        decision are resolved up front; only undecided ones are sent to the LLM,
        batched ``LLM_FILTER_BATCH_SIZE`` at a time, and each resulting decision is
        written back to the cache. ``override_filters`` bypasses the cache read so
        every document is re-evaluated (results are still written back).

        Skipped entirely if ``llm_filter_condition`` is not provided in kwargs.
        """
        condition = kwargs.get("llm_filter_condition")
        snippet_max_len = kwargs.get("snippet_max_len", 250)
        override = kwargs.get("override_filters", False)

        if not condition:
            return documents
        if not documents:
            return documents

        from src.oai.llm_core import (
            cache_get,
            cache_set,
            get_text_content,
            llm_cached_call,
            parse_json_response,
        )

        system_prompt = LLM_FILTER_SYSTEM_PROMPT.format(condition=condition)

        # 1. Resolve any decisions already cached per (url, condition, snippet_max_len);
        #    only undecided documents are sent to the LLM.
        decisions: Dict[int, bool] = {}
        pending: List = []
        for doc in documents:
            cache_id = self._llm_filter_cache_id(doc, condition, snippet_max_len)
            cached = cache_get("llm_filter", cache_id, "keep") if (cache_id and not override) else None
            if cached is None:
                pending.append(doc)
            else:
                decisions[id(doc)] = bool(cached)

        # 2. Evaluate undecided documents in batches and cache each decision.
        for batch_start in range(0, len(pending), LLM_FILTER_BATCH_SIZE):
            batch = pending[batch_start:batch_start + LLM_FILTER_BATCH_SIZE]
            lines = []
            for i, doc in enumerate(batch, start=1):
                snippet = self._build_snippet(doc, max_len=snippet_max_len)
                meta_parts = []
                user_name = doc.data.get("author")
                user_location = doc.data.get("author_location_text")
                user_bio = doc.data.get("author_profile_bio")
                if user_name:
                    meta_parts.append(f"user_name: {user_name}")
                if user_location:
                    meta_parts.append(f"user_location: {user_location}")
                if user_bio:
                    meta_parts.append(f"user_bio: {' '.join(user_bio.split())}")
                meta = (", ".join(meta_parts) + " | ") if meta_parts else ""
                lines.append(f"[{i}] {meta}{snippet}")
            user_content = "\n\n".join(lines)

            batch_cache_id = hash((user_content, condition))

            def build_messages(sc=system_prompt, uc=user_content):
                return [
                    {"role": "system", "content": sc},
                    {"role": "user", "content": uc},
                ]

            def parse_fn(response):
                text = get_text_content(response)
                return parse_json_response(text)

            result = llm_cached_call(
                cache_tag="llm_filter_batch",
                request_id=batch_cache_id,
                cache_field="keep_indices",
                messages_builder=build_messages,
                parse_fn=parse_fn,
                override=override,
            )

            if isinstance(result, list):
                keep_set = set(result)
            else:
                logger.warning("LLM filter returned non-list result, keeping all docs in batch")
                keep_set = set(range(1, len(batch) + 1))

            for i, doc in enumerate(batch, start=1):
                keep = i in keep_set
                decisions[id(doc)] = keep
                cache_id = self._llm_filter_cache_id(doc, condition, snippet_max_len)
                if cache_id:
                    cache_set("llm_filter", cache_id, "keep", keep)

        # 3. Keep documents (preserving order) whose decision is keep.
        kept = [doc for doc in documents if decisions.get(id(doc), True)]

        self._record_filtered_documents(
            documents,
            kept,
            "llm_filter",
            llm_filter_condition=condition,
            snippet_max_len=snippet_max_len,
        )
        logger.info(
            "LLM filter: %d → %d documents (%d resolved from cache, %d sent to LLM)",
            len(documents),
            len(kept),
            len(documents) - len(pending),
            len(pending),
        )
        return kept

    def _enrich_user_author(self, documents: List, **kwargs) -> List:
        """Enrich documents with user profile data (bio, followers). No-op by default; subclasses override."""
        return documents

    def _filter_location(self, documents: List, **kwargs) -> List:
        """Filter documents by country_id (geoid prefix match). Runs post-location-enrichment."""
        country_id = kwargs.get("country_id")
        if not country_id:
            return documents
        before = len(documents)
        filtered = [doc for doc in documents if doc.matches_location(country_id)]
        self._record_filtered_documents(documents, filtered, "location", country_id=country_id)
        logger.info("Location filter (country_id=%s): %d → %d documents", country_id, before, len(filtered))
        return filtered

    def _enrich_comments(self, documents: List, **kwargs) -> List:
        """Enrich documents with comments. No-op by default; subclasses override."""
        return documents

    # --- Actor interface ---

    def search(self, search_params: List[str], **kwargs) -> List:
        """Search by the given parameters. Returns list of Documents."""
        raise NotImplementedError
