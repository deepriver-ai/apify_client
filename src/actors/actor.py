from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from apify_client import ApifyClient
from dotenv import load_dotenv

APIFI_API_TOKEN = os.getenv("APIFI_API_TOKEN")

logger = logging.getLogger(__name__)

PERIOD_DAYS = {"d": 1, "w": 7, "m": 30}


class ApifyActor:
    """Base class for Apify actor wrappers.

    Subclasses must set ``actor_id`` and implement ``search()`` to create
    Documents from raw Apify results.

    After creating Documents, subclasses call ``process_documents()``
    which runs a staged pipeline ordered by cost:

        1. ``_filter_date``        — cheap, timestamp available from API
        2. ``_enrich_content``     — expensive (HTTP fetch + parse for news)
        3. ``_filter_language``    — cheap, needs body text from step 2
        4. ``_enrich_location``    — potentially expensive (geocoding for social)
        5. ``_filter_location``    — cheap, geoid prefix match

    Subclasses override individual stages to push filters to the API level
    or to provide actor-specific enrichment.
    """

    actor_id: str = ""  # Apify actor ID, set by subclass

    def __init__(self, client: Optional[ApifyClient] = None):
        self.client = client or ApifyClient(APIFI_API_TOKEN)

    def run_actor(self, run_input: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Run the Apify actor and return raw results."""
        run = self.client.actor(self.actor_id).call(run_input=run_input)
        results = []
        for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
            results.append(item)
        return results

    # --- Staged pipeline ---

    def process_documents(self, documents: List, **kwargs) -> List:
        """Run the full post-creation pipeline: filter → enrich → filter → enrich → filter.

        Stages are ordered cheapest-first so expensive enrichment steps
        only run on documents that survived earlier filters.
        """
        documents = self._filter_date(documents, **kwargs)
        documents = self._enrich_content(documents, **kwargs)
        documents = self._filter_language(documents, **kwargs)
        documents = self._enrich_location(documents, **kwargs)
        documents = self._filter_location(documents, **kwargs)
        documents = self._enrich_comments(documents, **kwargs)
        return documents

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
        """Enrich document location."""
        
        for doc in documents:
            doc.add_locations()

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
