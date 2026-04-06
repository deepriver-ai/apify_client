from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from apify_client import ApifyClient
from dotenv import load_dotenv

APIFI_API_TOKEN = os.getenv("APIFI_API_TOKEN")

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
        3. ``_enrich_content``     — expensive (HTTP fetch + parse for news)
        4. ``_filter_language``    — cheap, needs body text from step 3
        5. ``_enrich_location``    — potentially expensive (geocoding for social)
        6. ``_filter_location``    — cheap, geoid prefix match
        7. ``_filter_llm``         — LLM-based filtering (batched, expensive)
        8. ``_enrich_user_author`` — user profile enrichment (bio, followers)
        9. ``_enrich_comments``    — comments enrichment

    Subclasses override individual stages to push filters to the API level
    or to provide actor-specific enrichment.
    """

    actor_id: str = ""  # Apify actor ID, set by subclass

    def __init__(self, client: Optional[ApifyClient] = None):
        self.client = client or ApifyClient(APIFI_API_TOKEN)
        self.search_params_keywords: List[str] = []  # Should be set by the actor subclass when the scraping is keyword or hashtag-based
        self._filter_cache: Dict[str, bool] = self._load_filter_cache()

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

        # Skip docs already cached as filtered-out for this task (unless overriding)
        if not override and task_id:
            before = len(documents)
            documents = [
                doc for doc in documents
                if self._filter_cache.get(self._filter_cache_key(doc, task_id)) is not False
            ]
            if len(documents) < before:
                logger.info("Filter cache: %d → %d documents (skipped previously filtered for task %s)", before, len(documents), task_id)

        documents = self._filter_keywords(documents, **kwargs)
        documents = self._filter_date(documents, **kwargs)
        documents = self._enrich_content(documents, **kwargs)
        documents = self._filter_language(documents, **kwargs)
        documents = self._enrich_location(documents, **kwargs)
        documents = self._filter_location(documents, **kwargs)
        documents = self._filter_llm(documents, **kwargs)
        documents = self._enrich_user_author(documents, **kwargs)
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
        filtered = []
        for doc in documents:
            text = ((doc.data.get("body") or "") + " " + (doc.data.get("title") or "")).lower()
            if not any(kw.lower() in text for kw in not_keywords):
                filtered.append(doc)
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
        documents = [doc for doc in documents if doc.matches_min_date(min_date)]
        logger.info("Date filter (min_date=%s): %d → %d documents", min_date.date(), before, len(documents))
        return documents

    def _enrich_content(self, documents: List, **kwargs) -> List:
        """Enrich document content. No-op by default; subclasses override."""
        return documents

    def _filter_language(self, documents: List, **kwargs) -> List:
        """Filter documents by language. Runs post-content-enrichment (needs body text)."""
        language = kwargs.get("language")
        if not language:
            return documents
        before = len(documents)
        documents = [doc for doc in documents if doc.matches_language(language)]
        logger.info("Language filter (%s): %d → %d documents", language, before, len(documents))
        return documents

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

    def _filter_llm(self, documents: List, **kwargs) -> List:
        """Filter documents using an LLM based on a natural-language condition.

        Sends all documents to the LLM in batches. Caching is handled at the
        pipeline level by ``process_documents()``.

        Skipped entirely if ``llm_filter_condition`` is not provided in kwargs.
        """
        condition = kwargs.get("llm_filter_condition")
        snippet_max_len = kwargs.get("snippet_max_len", 250)
        
        if not condition:
            return documents
        if not documents:
            return documents

        from src.oai.llm_core import get_text_content, llm_cached_call, parse_json_response

        system_prompt = LLM_FILTER_SYSTEM_PROMPT.format(condition=condition)
        kept = []

        for batch_start in range(0, len(documents), LLM_FILTER_BATCH_SIZE):
            batch = documents[batch_start:batch_start + LLM_FILTER_BATCH_SIZE]
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
                cache_tag="llm_filter",
                request_id=batch_cache_id,
                cache_field="keep_indices",
                messages_builder=build_messages,
                parse_fn=parse_fn,
            )

            if isinstance(result, list):
                keep_set = set(result)
                for i, doc in enumerate(batch, start=1):
                    if i in keep_set:
                        kept.append(doc)
            else:
                logger.warning("LLM filter returned non-list result, keeping all docs in batch")
                kept.extend(batch)

        logger.info("LLM filter: %d → %d documents", len(documents), len(kept))
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
        documents = [doc for doc in documents if doc.matches_location(country_id)]
        logger.info("Location filter (country_id=%s): %d → %d documents", country_id, before, len(documents))
        return documents

    def _enrich_comments(self, documents: List, **kwargs) -> List:
        """Enrich documents with comments. No-op by default; subclasses override."""
        return documents

    # --- Actor interface ---

    def search(self, search_params: List[str], **kwargs) -> List:
        """Search by the given parameters. Returns list of Documents."""
        raise NotImplementedError
