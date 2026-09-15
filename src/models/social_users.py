"""Social-account characterization store (``SocialUsers`` Mongo collection).

Per-account classifications produced by the WS-4 batch classifier
(``src/scripts/classify_users.py``). Reports look accounts up here at read time
and join them onto the (immutable) Elasticsearch documents; nothing is ever
written back to ES.

Two-tier identity (one document per account):

- tier ``"profile"`` — post authors, keyed by a canonical, normalized
  ``profile_url``. This is a strong identity.
- tier ``"name"`` — comment-only authors, keyed by ``(network,
  normalized_name)`` where ``normalized_name`` is casefolded, whitespace-collapsed
  and accent-stripped. Display-name identity is best-effort, so records on this
  tier carry a lower ``identity_confidence`` ceiling.

The **classifier** assigns one of the closed-vocabulary classes (taxonomy v2,
2026-09-14): ``ciudadano``, ``figura_publica``, ``politico``, ``partido``,
``institucion``, ``organizacion``, ``empresa``, ``medio``, ``comunidad``, plus the
attributes ``nivel_gobierno`` (politico / institucion), ``partido`` (politico /
partido) and ``alcance`` (medio), and the account ``bio`` when the platform exposes
one. ``bot`` and ``influencer`` are NOT stored classes — per
``reports/event_report/docs/social_report_design.md`` §1 they are derived at read
time from ``automation_score`` / ``followers`` against tunable, per-market
thresholds. Records written by the v1 classifier (``organico``, ``pagina_oficial``,
``sitio_*`` …) are upgraded at read time by ``upgrade_record``.

The collection lives in the same Mongo database that holds ``CrawlersAll``
(name from ``MONGO_DB_NEWS_SOURCES``), reusing ``src/helpers/mongoconnection.py``.
"""

from __future__ import annotations

import logging
import os
import unicodedata
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

COLLECTION_NAME = "SocialUsers"

# Closed classifier vocabulary v2 (see social_report_design.md §1). bot/influencer
# are intentionally absent — they are derived at read time by the report builders.
CLASSIFIER_CLASSES = (
    "ciudadano",        # real individual person, not a public figure
    "figura_publica",   # journalist, columnist, activist, artist, creator — not in office
    "politico",         # person holding or seeking office, or a party official (attr: nivel_gobierno, partido)
    "partido",          # party, coalition, campaign or movement account (attr: partido)
    "institucion",      # government or public-institution account (attr: nivel_gobierno)
    "organizacion",     # collective, union, NGO, neighbourhood association, church, school
    "empresa",          # business or brand
    "medio",            # news outlet operating as a social account (attr: alcance)
    "comunidad",        # community / neighbourhood / denuncia page
)
DEFAULT_CLASS = "ciudadano"
# Classes whose voice counts as organic conversation in the reports.
ORGANIC_CLASSES = ("ciudadano", "comunidad")
NIVELES_GOBIERNO = ("municipal", "estatal", "federal")
ALCANCES_MEDIO = ("local", "estatal", "nacional")

# v1 (2026-07) → v2 mapping, applied at read time to stored records and reports.
V1_TO_V2 = {
    "organico": ("ciudadano", {}),
    "pagina_oficial": ("institucion", {}),
    "politico": ("politico", {}),
    "comunidad": ("comunidad", {}),
    "sitio_local": ("medio", {"alcance": "local"}),
    "sitio_estatal": ("medio", {"alcance": "estatal"}),
    "sitio_nacional": ("medio", {"alcance": "nacional"}),
}


def upgrade_record(record: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return a copy of a stored/report record in the v2 vocabulary.

    v2 records pass through; v1 classes are mapped with ``V1_TO_V2`` (``sitio_*``
    becomes ``medio`` with its ``alcance``). Missing attribute keys are added as
    None so consumers can rely on them."""
    if not isinstance(record, dict):
        return record
    rec = dict(record)
    cls = rec.get("classification")
    if cls in V1_TO_V2:
        new_cls, attrs = V1_TO_V2[cls]
        rec["classification_v1"] = cls
        rec["classification"] = new_cls
        for k, v in attrs.items():
            rec.setdefault(k, v)
    for k in ("nivel_gobierno", "partido", "alcance", "bio"):
        rec.setdefault(k, None)
    return rec

TIER_PROFILE = "profile"
TIER_NAME = "name"

# Identity-confidence ceilings per tier. Display-name identity (comment authors)
# is inherently weaker than a canonical profile URL.
IDENTITY_CONFIDENCE = {
    TIER_PROFILE: 0.9,
    TIER_NAME: 0.5,
}


# --- Identity normalization -------------------------------------------------

def strip_accents(text: str) -> str:
    """Remove combining diacritical marks (á→a, ñ→n)."""
    if not text:
        return text
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def normalize_name(name: Optional[str]) -> str:
    """Normalize a display name for the ``name`` identity tier.

    Casefold, collapse internal/edge whitespace, and strip accents. Returns an
    empty string for falsy input.
    """
    if not name:
        return ""
    collapsed = " ".join(str(name).split())
    return strip_accents(collapsed).casefold()


def normalize_profile_url(url: Optional[str]) -> str:
    """Canonicalize a profile URL for the ``profile`` identity tier.

    Lowercases the whole URL, drops the scheme and a leading ``www.``, strips any
    query string / fragment, and removes the trailing slash. Two URLs that point
    at the same account collapse to the same key regardless of ``http``/``https``,
    ``www.`` or tracking params. Returns an empty string for falsy input.
    """
    if not url:
        return ""
    u = str(url).strip().lower()
    for scheme in ("https://", "http://"):
        if u.startswith(scheme):
            u = u[len(scheme):]
            break
    if u.startswith("www."):
        u = u[4:]
    # Drop query / fragment.
    u = u.split("?", 1)[0].split("#", 1)[0]
    return u.rstrip("/")


def account_id(tier: str, *, profile_url: Optional[str] = None,
               network: Optional[str] = None, name: Optional[str] = None) -> str:
    """Build the deterministic Mongo ``_id`` for an account.

    - profile tier → ``profile:<normalized_profile_url>``
    - name tier    → ``name:<network>:<normalized_name>``
    """
    if tier == TIER_PROFILE:
        return f"{TIER_PROFILE}:{normalize_profile_url(profile_url)}"
    return f"{TIER_NAME}:{(network or '').lower()}:{normalize_name(name)}"


class SocialUsers:
    """CRUD wrapper over the ``SocialUsers`` Mongo collection.

    A collection can be injected (tests); otherwise it is resolved lazily from
    the shared ``mongoconn`` client and ``MONGO_DB_NEWS_SOURCES`` database. When
    Mongo is unreachable the store degrades to a no-op read path (``get`` returns
    ``None``) rather than raising at import time.
    """

    def __init__(self, collection: Any = None, db_name: Optional[str] = None):
        self._collection = collection
        self._db_name = db_name or os.getenv("MONGO_DB_NEWS_SOURCES", "admin_app")

    @property
    def collection(self):
        if self._collection is None:
            from src.helpers.mongoconnection import mongoconn
            self._collection = mongoconn[self._db_name][COLLECTION_NAME]
        return self._collection

    # --- Reads --------------------------------------------------------------

    def get(self, _id: str) -> Optional[Dict[str, Any]]:
        """Return the stored record for a deterministic account id, or None."""
        try:
            return self.collection.find_one({"_id": _id})
        except Exception as exc:  # Mongo unreachable / mis-config — read path degrades.
            logger.warning("SocialUsers.get(%s) failed: %s", _id, exc)
            return None

    def get_by_profile_url(self, profile_url: str) -> Optional[Dict[str, Any]]:
        return self.get(account_id(TIER_PROFILE, profile_url=profile_url))

    def get_by_name(self, network: str, name: str) -> Optional[Dict[str, Any]]:
        return self.get(account_id(TIER_NAME, network=network, name=name))

    # --- Writes -------------------------------------------------------------

    def upsert(self, record: Dict[str, Any]) -> None:
        """Upsert a classification record without eating history across scopes.

        Classifier runs are scoped (a page list, a phrase set); a narrow scope
        must never shrink an account's stored evidence (2026-07-21 incident: a
        campaign-scoped run overwrote a 16-comment history with a 2-comment
        snapshot). Rule: the richer snapshot wins — if the stored record has
        MORE appearances than the incoming one, keep its features/classification
        and only union ``pages_touched`` and advance ``evidence_as_of``.
        """
        _id = record["_id"]
        stored = self.get(_id)
        if isinstance(stored, dict):
            old_app = (stored.get("n_comments") or 0) + (stored.get("n_posts") or 0)
            new_app = (record.get("n_comments") or 0) + (record.get("n_posts") or 0)
            if old_app > new_app:
                pages = sorted(set(stored.get("pages_touched") or [])
                               | set(record.get("pages_touched") or []))
                as_of = max(str(stored.get("evidence_as_of") or ""),
                            str(record.get("evidence_as_of") or "")) or None
                self.collection.update_one(
                    {"_id": _id},
                    {"$set": {"pages_touched": pages, "evidence_as_of": as_of}})
                return
            record = dict(record)
            record["pages_touched"] = sorted(set(stored.get("pages_touched") or [])
                                             | set(record.get("pages_touched") or []))
        self.collection.update_one({"_id": _id}, {"$set": record}, upsert=True)

    def set_human_label(self, _id: str, automation: float, label: str,
                        by: str, note: Optional[str] = None,
                        date: Optional[str] = None) -> None:
        """Attach a human judgment to an account (2026-07-21, WS-4 calibration).

        Human labels live under the ``human`` key, which classifier upserts
        never write — ``upsert`` uses ``$set`` with only the machine record's
        keys, so re-classification can never clobber a reviewer's call. Reports
        should read ``effective_automation`` rather than ``automation_score``.
        """
        human = {"automation": float(automation), "label": label, "by": by}
        if note:
            human["note"] = note
        if date:
            human["date"] = date
        self.collection.update_one({"_id": _id}, {"$set": {"human": human}},
                                   upsert=True)

    @staticmethod
    def effective_automation(record: Optional[Dict[str, Any]]) -> float:
        """Max of the machine score and the human judgment — a reviewer's
        certainty must not be diluted by a thin behavioral surface (accounts
        whose tells are profile-level are invisible to the feature pipeline).

        Consumers deriving a ``bot`` badge should ALSO exclude classes
        ``institucion`` and ``medio`` (v1: ``pagina_oficial`` / ``sitio_*``): outlets and official pages post
        repetitively by nature (e.g. a newspaper account dropping its own
        links scores 0.8), which is automation but not a bot in the
        presented taxonomy (2026-07-21 media-scope validation)."""
        if not isinstance(record, dict):
            return 0.0
        machine = record.get("automation_score") or 0.0
        human = (record.get("human") or {}).get("automation") or 0.0
        return max(float(machine), float(human))

    def needs_reclassification(self, record: Dict[str, Any]) -> bool:
        """Decide whether a freshly-built record has materially new evidence.

        True when the account is new, was never classified, or has appearances
        newer than the stored ``evidence_as_of`` — i.e. re-running the classifier
        should refresh evidence/features and re-run the (content-cached) LLM.
        Feature/prompt changes are handled by bumping the classifier cache tag.
        """
        stored = self.get(record["_id"])
        if not stored or not stored.get("classification"):
            return True
        prev_as_of = stored.get("evidence_as_of")
        new_as_of = record.get("evidence_as_of")
        if not prev_as_of:
            return True
        if new_as_of and str(new_as_of) > str(prev_as_of):
            return True
        return False


def build_record(
    *,
    tier: str,
    network: str,
    display_name: str,
    profile_url: Optional[str] = None,
    classification: str = DEFAULT_CLASS,
    classification_confidence: float = 0.0,
    nivel_gobierno: Optional[str] = None,
    partido: Optional[str] = None,
    alcance: Optional[str] = None,
    bio: Optional[str] = None,
    automation_score: float = 0.0,
    automation_evidence: Optional[List[str]] = None,
    anonymity: float = 0.0,
    followers: Optional[int] = None,
    features: Optional[Dict[str, Any]] = None,
    evidence_sample: Optional[List[str]] = None,
    evidence: Optional[List[str]] = None,
    evidence_as_of: Optional[str] = None,
    llm_model: Optional[str] = None,
    classifier_version: Optional[str] = None,
    n_comments: int = 0,
    n_posts: int = 0,
    n_distinct_parent_docs: int = 0,
    pages_touched: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Assemble a normalized ``SocialUsers`` document ready for ``upsert``.

    Sets the deterministic ``_id`` and the identity keys/confidence for the tier.
    """
    if tier == TIER_PROFILE:
        _id = account_id(TIER_PROFILE, profile_url=profile_url)
        normalized_name = None
    else:
        _id = account_id(TIER_NAME, network=network, name=display_name)
        normalized_name = normalize_name(display_name)

    if classification in V1_TO_V2:
        classification, attrs = V1_TO_V2[classification]
        alcance = alcance or attrs.get("alcance")
    if classification not in CLASSIFIER_CLASSES:
        logger.warning("Coercing unknown classification %r to %r", classification, DEFAULT_CLASS)
        classification = DEFAULT_CLASS
    if nivel_gobierno not in NIVELES_GOBIERNO:
        nivel_gobierno = None
    if alcance not in ALCANCES_MEDIO:
        alcance = None

    return {
        "_id": _id,
        "tier": tier,
        "profile_url": normalize_profile_url(profile_url) if profile_url else None,
        "normalized_name": normalized_name,
        "network": (network or "").lower(),
        "display_name": display_name,
        "identity_confidence": IDENTITY_CONFIDENCE.get(tier, 0.5),
        "classification": classification,
        "nivel_gobierno": nivel_gobierno,
        "partido": (partido or None),
        "alcance": alcance,
        "bio": (" ".join(str(bio).split())[:500] if bio else None),
        "classification_confidence": round(float(classification_confidence), 3),
        "automation_score": round(float(automation_score), 3),
        "automation_evidence": automation_evidence or [],
        "anonymity": round(float(anonymity), 3),
        "followers": followers,
        "features": features or {},
        "evidence_sample": (evidence_sample or [])[:5],
        "evidence": evidence or [],
        "classified_at": datetime.utcnow().isoformat(),
        "evidence_as_of": evidence_as_of,
        "llm_model": llm_model,
        "classifier_version": classifier_version,
        "n_comments": n_comments,
        "n_posts": n_posts,
        "n_distinct_parent_docs": n_distinct_parent_docs,
        "pages_touched": sorted(pages_touched or []),
    }
