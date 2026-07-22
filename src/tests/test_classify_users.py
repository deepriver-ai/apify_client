"""Tests for the batch social-account classifier (src/scripts/classify_users.py).

ES / Mongo / LLM are never contacted: the LLM batch function is patched, accounts
are built in-memory, and the sentiment join is exercised with synthetic rows.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.scripts import classify_users as cu
from src.scripts.classify_users import Account
from src.models.social_users import TIER_NAME, TIER_PROFILE, account_id


def make_comment_account(name, texts, parent_docs=None, network="facebook",
                         timestamps=None, likes=None):
    """Build a name-tier Account with the given comment texts."""
    key = account_id(TIER_NAME, network=network, name=name)
    acc = Account(tier=TIER_NAME, network=network, display_name=name, key=key)
    parent_docs = parent_docs or [f"https://p/{i}" for i in range(len(texts))]
    timestamps = timestamps or [None] * len(texts)
    likes = likes or [0] * len(texts)
    for text, pd, ts, lk in zip(texts, parent_docs, timestamps, likes):
        acc.n_comments += 1
        acc.parent_docs.add(pd)
        acc.pages.add("SomePage")
        acc.comment_items.append({
            "text": text, "likes": lk, "timestamp": ts, "parent_doc_id": pd, "page": "SomePage",
        })
    return acc


class TestFeaturesAndAutomation:
    def test_carlos_martinez_scores_max(self):
        """Identical text on 18 distinct posts -> automation score near max."""
        text = "Excelente trabajo del presidente municipal, vamos por buen camino"
        acc = make_comment_account(
            "Carlos Martinez", [text] * 18,
            parent_docs=[f"https://p/{i}" for i in range(18)],
        )
        acc.features = cu.compute_features(acc)
        assert acc.features["max_duplicate_text_count"] == 18
        assert acc.features["n_distinct_parent_docs"] == 18
        assert acc.features["mean_pairwise_similarity"] == 1.0
        score, evidence = cu.compute_automation(acc.features)
        assert score >= 0.9
        assert evidence  # carries a repetition explanation

    def test_organic_complainer_scores_low(self):
        """Varied, specific grievances -> low automation."""
        texts = [
            "Llevo tres días sin agua en la colonia Lomas, nadie responde",
            "El bache de la avenida Juárez sigue igual, ya reporté dos veces",
            "Cobraron el predial doble y en tesorería no dan solución",
            "La basura no la recogen desde el lunes en el centro",
        ]
        acc = make_comment_account("Ma Elena Vazquez", texts)
        acc.features = cu.compute_features(acc)
        score, _ = cu.compute_automation(acc.features)
        assert score < 0.3
        assert acc.features["max_duplicate_text_count"] == 1

    def test_persistent_critic_scores_low(self):
        """Varied negative comments (distinct wording) -> automation stays low."""
        texts = [
            "El alcalde prometió pavimentar y seguimos con terracería en la colonia",
            "Otra vez subieron el agua sin explicar en qué se gasta el dinero",
            "La inseguridad en el centro es cada día peor, ¿dónde está la policía?",
            "Prometieron transparencia y no publican ni un solo contrato",
            "Cerraron la clínica y ahora hay que viajar hasta la capital para atenderse",
            "El alumbrado lleva meses fundido en toda la avenida principal",
            "Cobran predial altísimo pero las calles siguen llenas de baches enormes",
            "Ni una respuesta de tesorería después de tres oficios entregados",
        ]
        acc = make_comment_account("Jose Arturo Correa Ugalde", texts)
        acc.features = cu.compute_features(acc)
        score, _ = cu.compute_automation(acc.features)
        assert score < 0.5

    def test_burstiness_flagged(self):
        """Comments on different posts within seconds -> burst signal."""
        base = datetime(2026, 7, 10, 12, 0, 0, tzinfo=timezone.utc)
        ts = [(base.replace(second=s)).isoformat() for s in (0, 10, 20, 30, 40)]
        acc = make_comment_account(
            "Bot Ráfaga", ["a", "b", "c", "d", "e"],
            parent_docs=[f"https://p/{i}" for i in range(5)], timestamps=ts,
        )
        acc.features = cu.compute_features(acc)
        assert acc.features["burst_count"] >= 3


class TestMinActivityGate:
    def test_singletons_never_reach_llm(self):
        """Accounts below min-activity must not trigger an LLM call."""
        accounts = {
            a.key: a for a in [
                make_comment_account("Solo Uno", ["un comentario"]),
                make_comment_account("Otro Solo", ["otro comentario"]),
            ]
        }
        for a in accounts.values():
            a.features = cu.compute_features(a)

        with patch.object(cu, "llm_classify_batch") as mock_llm:
            calls = cu.classify_accounts(accounts, min_activity=2, official_pages=[])

        mock_llm.assert_not_called()
        assert calls == 0
        for a in accounts.values():
            assert a.classification == "organico"
            assert a.confidence <= 0.2
            assert not a.llm_used

    def test_active_account_reaches_llm(self):
        acc = make_comment_account("Persona Activa", ["a", "b", "c"])
        acc.features = cu.compute_features(acc)
        accounts = {acc.key: acc}

        canned = [{"i": 1, "classification": "politico", "automation_score": 0.1,
                   "anonymity": 0.2, "confidence": 0.8, "evidence": ["es candidato"]}]
        with patch.object(cu, "llm_classify_batch", return_value=canned) as mock_llm:
            calls = cu.classify_accounts(accounts, min_activity=2, official_pages=[])

        mock_llm.assert_called_once()
        assert calls == 1
        assert acc.classification == "politico"
        assert acc.confidence == 0.8
        assert acc.llm_used


class TestDeterministicMapping:
    def test_official_page_bypasses_llm(self):
        acc = make_comment_account("Presidencia Municipal", ["x", "y"])
        acc.features = cu.compute_features(acc)
        accounts = {acc.key: acc}

        with patch.object(cu, "llm_classify_batch") as mock_llm:
            calls = cu.classify_accounts(
                accounts, min_activity=2,
                official_pages=["Presidencia Municipal"],
            )

        mock_llm.assert_not_called()
        assert calls == 0
        assert acc.classification == "pagina_oficial"

    def test_commenter_on_official_page_not_flagged_official(self):
        """A commenter ON an official page must not inherit pagina_oficial."""
        acc = make_comment_account("Vecino Molesto", ["x", "y"])
        acc.pages = {"Presidencia Municipal"}  # page they commented on
        acc.features = cu.compute_features(acc)
        result = cu.deterministic_classification(
            acc, official_pages_norm={"presidencia municipal"},
        )
        assert result is None  # falls through to the LLM

    def test_official_page_own_posts_matched_via_pages(self):
        """A page's own posts (display_name has a suffix) still map via source.name."""
        key = account_id(TIER_PROFILE, profile_url="https://facebook.com/presidenciaSJR")
        acc = Account(tier=TIER_PROFILE, network="facebook",
                      display_name="Presidencia Municipal | San Juan del Río",
                      key=key, profile_url="facebook.com/presidenciasjr", n_posts=3)
        acc.pages = {"Presidencia Municipal"}
        result = cu.deterministic_classification(
            acc, official_pages_norm={"presidencia municipal"},
        )
        assert result is not None and result[0] == "pagina_oficial"

    def test_known_outlet_domain_bypasses_llm(self):
        # Profile-tier account whose URL resolves to a known news-source domain.
        key = account_id(TIER_PROFILE, profile_url="https://eldiariolocal.com.mx")
        acc = Account(tier=TIER_PROFILE, network="facebook",
                      display_name="El Diario Local", key=key,
                      profile_url="eldiariolocal.com.mx", n_posts=5)
        acc.features = cu.compute_features(acc)
        accounts = {acc.key: acc}

        sources_mgr = MagicMock()
        sources_mgr.get_domain.return_value = "eldiariolocal.com.mx"
        sources_mgr.is_known.return_value = True
        sources_mgr.get_location.return_value = {"location_author_precision_level": 3}

        with patch.object(cu, "llm_classify_batch") as mock_llm:
            calls = cu.classify_accounts(accounts, min_activity=2, official_pages=[],
                                         sources_mgr=sources_mgr)

        mock_llm.assert_not_called()
        assert calls == 0
        assert acc.classification == "sitio_local"

    def test_precision_to_sitio_levels(self):
        assert cu._precision_to_sitio(3) == "sitio_local"
        assert cu._precision_to_sitio(2) == "sitio_estatal"
        assert cu._precision_to_sitio(1) == "sitio_nacional"
        assert cu._precision_to_sitio(None) == "sitio_nacional"


class TestSentimentJoin:
    def test_join_by_parent_doc_and_minute(self):
        # Comment at 12:00:30; sentiment row at 12:00:55 -> same minute -> match.
        acc = make_comment_account(
            "Critico", ["malo", "pésimo", "regular"],
            parent_docs=["https://post/1", "https://post/2", "https://post/3"],
            timestamps=[
                "2026-07-10T12:00:30+00:00",
                "2026-07-11T09:15:05+00:00",
                "2026-07-12T18:30:00+00:00",
            ],
        )
        accounts = {acc.key: acc}
        rows = [
            ("https://post/1", datetime(2026, 7, 10, 12, 0, 55, tzinfo=timezone.utc), "negativo"),
            ("https://post/2", datetime(2026, 7, 11, 9, 15, 40, tzinfo=timezone.utc), "negativo"),
            ("https://post/3", datetime(2026, 7, 12, 18, 30, 10, tzinfo=timezone.utc), "neutral"),
        ]
        matched = cu.apply_sentiment(accounts, rows)
        assert matched == 3
        assert acc.features["sentiment_matched"] == 3
        # 2 negativo + 1 neutral -> extremity share 2/3
        assert acc.features["extremity_share"] == round(2 / 3, 3)
        assert acc.features["dominant_polarity"] == "negativo"

    def test_no_match_on_different_minute(self):
        acc = make_comment_account(
            "Nadie", ["hola"], parent_docs=["https://post/1"],
            timestamps=["2026-07-10T12:00:30+00:00"],
        )
        accounts = {acc.key: acc}
        # Same post, but 3 minutes later -> different minute key -> no match.
        rows = [("https://post/1", datetime(2026, 7, 10, 12, 3, 30, tzinfo=timezone.utc), "negativo")]
        matched = cu.apply_sentiment(accounts, rows)
        assert matched == 0
        assert "extremity_share" not in acc.features

    def test_empty_rows_degrades_gracefully(self):
        acc = make_comment_account("Nadie", ["hola"])
        accounts = {acc.key: acc}
        assert cu.apply_sentiment(accounts, []) == 0


class TestReviewTable:
    def test_sorted_by_automation_desc(self):
        low = make_comment_account("Baja", ["a", "b"])
        high = make_comment_account("Alta", ["x"] * 10,
                                    parent_docs=[f"https://p/{i}" for i in range(10)])
        for a in (low, high):
            a.features = cu.compute_features(a)
            a.automation_score, a.automation_evidence = cu.compute_automation(a.features)
        accounts = {low.key: low, high.key: high}
        table = cu.build_review_table(accounts)
        assert table.index("Alta") < table.index("Baja")


def test_duplicate_text_share_zero_when_all_texts_unique():
    """A 2-comment account with distinct texts must show 0 duplication —
    regression: max_dup/n gave every lone pair a misleading 0.5 share that the
    LLM echoed back as an automation signal."""
    from src.scripts.classify_users import Account, compute_features

    acc = Account(tier="name", network="facebook", display_name="Ricardo Loyola",
                  key="name:facebook:ricardo loyola")
    acc.n_comments = 2
    acc.comment_items = [
        {"text": "Demuestre lo, con hechos, no palabras", "likes": 5,
         "timestamp": "2026-07-01T10:00:00", "parent_doc_id": "p1", "page": "X"},
        {"text": "Apoco? ya buscan aliados para pedir votos", "likes": 1,
         "timestamp": "2026-07-02T11:00:00", "parent_doc_id": "p2", "page": "X"},
    ]
    acc.parent_docs = {"p1", "p2"}
    feats = compute_features(acc)
    assert feats["max_duplicate_text_count"] == 1
    assert feats["duplicate_text_share"] == 0.0

    # And the fully-duplicated case still saturates at 1.0.
    acc.comment_items = [dict(c, text="mismo texto identico") for c in acc.comment_items]
    feats = compute_features(acc)
    assert feats["duplicate_text_share"] == 1.0


def test_same_post_duplicate_comments_are_scrape_artifacts():
    """The same (author, text) captured twice on ONE post must count once —
    scraper double-capture must not manufacture an automation signal."""
    from unittest.mock import patch
    from src.scripts import classify_users as cu

    doc = {"_source": {
        "source": {"name": "Pagina X", "stats": {}},
        "news_type": "facebook",
        "url": "https://facebook.com/p/1",
        "date_created": "2026-07-01T10:00:00",
        "comments": [
            {"comment_author": "Enrique Ochoas", "comment_text": "Van a tapar los baches?",
             "comment_timestamp": "2026-07-01T11:00:00", "comment_likes": 0},
            {"comment_author": "Enrique Ochoas", "comment_text": "Van a tapar los baches?",
             "comment_timestamp": "2026-07-01T11:00:00", "comment_likes": 0},
        ],
    }}
    with patch.object(cu, "get_es_client"), \
         patch("elasticsearch.helpers.scan", return_value=[doc]):
        accounts, _ = cu.harvest_evidence(None, ["Pagina X"], None, 30)
    acc = [a for a in accounts.values() if a.display_name == "Enrique Ochoas"][0]
    assert acc.n_comments == 1
    assert len(acc.comment_items) == 1


def test_phrases_scope_builds_match_phrase_should_clauses():
    """--phrases must scope by text/title phrase match (keyword-search content
    carries the author's own page in source.name, so page lists can't reach it)."""
    from unittest.mock import patch
    from src.scripts import classify_users as cu

    captured = {}
    def fake_scan(es, index, query, _source, size):
        captured.update(query)
        return []
    with patch.object(cu, "get_es_client"), \
         patch("elasticsearch.helpers.scan", side_effect=fake_scan):
        cu.harvest_evidence(None, None, None, 7, phrases=["roberto cabrera"])
    scope = captured["query"]["bool"]["filter"][0]["bool"]
    assert scope["minimum_should_match"] == 1
    fields = [list(c["match_phrase"].keys())[0] for c in scope["should"]]
    assert sorted(fields) == ["text", "title"]

    import pytest
    with pytest.raises(ValueError):
        cu.harvest_evidence(None, None, None, 7)


def test_systematic_counter_messaging_scores_high():
    """Content-aware but official-only + high-coverage + single-polarity accounts
    score as probable automation (2026-07-21 weighting decision) — varied text
    must NOT exempt them."""
    from src.scripts.classify_users import compute_automation

    feats = {"n_comments": 16, "n_posts": 0, "n_distinct_parent_docs": 16,
             "max_duplicate_text_count": 1, "duplicate_text_share": 0.0,
             "mean_pairwise_similarity": 0.1, "burst_count": 0, "burst_share": 0.0,
             "official_pages_only": True, "official_page_coverage": 0.8,
             "comments_per_active_day": 1.5, "extremity_share": 1.0,
             "dominant_polarity": "negativo"}
    score, ev = compute_automation(feats)
    assert score >= 0.7
    assert any("contramensaje sistemático" in e for e in ev)

    # A vecina with 5 varied complaints covering a small share of the page's
    # posts stays low — coverage is the discriminator, not negativity.
    feats2 = dict(feats, n_comments=5, n_distinct_parent_docs=5,
                  official_page_coverage=0.12, comments_per_active_day=0.7)
    score2, _ = compute_automation(feats2)
    assert score2 < 0.5


def test_upsert_narrow_scope_does_not_shrink_history():
    """A campaign-scoped run must not overwrite a richer page-scope record
    (2026-07-21 incident: 16-comment history replaced by a 2-comment snapshot)."""
    from unittest.mock import MagicMock
    from src.models.social_users import SocialUsers

    store = SocialUsers(collection=MagicMock())
    rich = {"_id": "name:facebook:x", "n_comments": 16, "n_posts": 0,
            "pages_touched": ["Pagina A"], "evidence_as_of": "2026-07-20",
            "classification": "organico"}
    store.get = lambda _id: rich
    narrow = {"_id": "name:facebook:x", "n_comments": 2, "n_posts": 0,
              "pages_touched": ["Pagina B"], "evidence_as_of": "2026-07-21",
              "classification": "organico"}
    store.upsert(narrow)
    call = store.collection.update_one.call_args
    setdoc = call[0][1]["$set"]
    assert "classification" not in setdoc          # features/class preserved
    assert setdoc["pages_touched"] == ["Pagina A", "Pagina B"]
    assert setdoc["evidence_as_of"] == "2026-07-21"


def test_systematic_rule_catches_high_volume_low_coverage_critic():
    """JACU profile: 21 comments official-only, per-page coverage ~0.25,
    sarcasm-diluted extremity (0.52) — must flag under the n>=10 arm."""
    from src.scripts.classify_users import compute_automation
    feats = {"n_comments": 21, "official_pages_only": True,
             "official_page_coverage": 0.197, "comments_per_active_day": 1.05,
             "extremity_share": 0.52, "dominant_polarity": "negativo",
             "max_duplicate_text_count": 1, "mean_pairwise_similarity": 0.1,
             "burst_count": 0, "burst_share": 0.0}
    score, ev = compute_automation(feats)
    assert score >= 0.7


# --- Low-volume rules from the WS-4 manual-label set (2026-07-21) ------------

def test_low_volume_identical_pair_with_burst_reaches_review_band():
    """Iker Mitz shape: identical text on 2 distinct posts within the burst
    window -> review band (0.5), even though max_dup < 3."""
    base = datetime(2026, 7, 13, 22, 23, 40, tzinfo=timezone.utc)
    ts = [base.isoformat(), base.replace(minute=24).isoformat()]
    acc = make_comment_account(
        "Iker Mitz", ["Puro engaña tontos, bola de rateros"] * 2,
        parent_docs=["https://p/a", "https://p/b"], timestamps=ts,
    )
    acc.features = cu.compute_features(acc)
    score, evidence = cu.compute_automation(acc.features)
    assert score >= 0.5
    assert any("2 publicaciones distintas" in e for e in evidence)


def test_organic_petitioner_repeat_across_days_stays_low():
    """Verónica shape: same petition pasted on 2 posts days apart -> no burst,
    no flag. The burst requirement is what separates paste-bots from vecinas."""
    ts = [
        datetime(2026, 7, 10, 9, 0, 0, tzinfo=timezone.utc).isoformat(),
        datetime(2026, 7, 14, 21, 0, 0, tzinfo=timezone.utc).isoformat(),
    ]
    acc = make_comment_account(
        "Veronica Valdez", ["Y comevi banthi para cuando saludos"] * 2,
        parent_docs=["https://p/a", "https://p/b"], timestamps=ts,
    )
    acc.features = cu.compute_features(acc)
    score, _ = cu.compute_automation(acc.features)
    assert score < 0.5


def _paired_accounts(gap_seconds, posts):
    """Two accounts commenting gap_seconds apart on each of the given posts."""
    base = datetime(2026, 7, 13, 22, 0, 0, tzinfo=timezone.utc)
    accounts = {}
    for name, offset in (("Cuenta A", 0), ("Cuenta B", gap_seconds)):
        ts, texts, docs = [], [], []
        for i, post in enumerate(posts):
            t = base.replace(hour=22 + i)
            ts.append((t.replace(second=0) if offset == 0
                       else t.replace(second=int(offset))).isoformat())
            texts.append(f"comentario {name} {i}")
            docs.append(post)
        acc = make_comment_account(name, texts, parent_docs=docs, timestamps=ts)
        acc.features = cu.compute_features(acc)
        accounts[acc.key] = acc
    return accounts


def test_coordination_tight_single_cooccurrence_scores_half():
    """González/Iker shape: two accounts 10s apart on one post -> 0.5 floor."""
    accounts = _paired_accounts(10, ["https://p/x"])
    cu.compute_coordination(accounts)
    for acc in accounts.values():
        assert acc.features["coordination_min_gap"] == 10.0
        score, evidence = cu.compute_automation(acc.features)
        assert score >= 0.5
        assert any("coordinación entre cuentas" in e for e in evidence)


def test_coordination_repeated_pair_scores_high():
    """Same pair co-hitting 2 posts inside the window -> 0.7 for both."""
    accounts = _paired_accounts(45, ["https://p/x", "https://p/y"])
    cu.compute_coordination(accounts)
    for acc in accounts.values():
        assert acc.features["coordination_pair_posts"] == 2
        score, evidence = cu.compute_automation(acc.features)
        assert score >= 0.7
        assert any("2 publicaciones" in e for e in evidence)


def test_coordination_loose_single_cooccurrence_does_not_score():
    """A single 45s co-occurrence (inside window, beyond tight) records the
    pair but does not move the score by itself."""
    accounts = _paired_accounts(45, ["https://p/x"])
    cu.compute_coordination(accounts)
    for acc in accounts.values():
        assert acc.features["coordination_min_gap"] == 45.0
        score, evidence = cu.compute_automation(acc.features)
        assert score < 0.5
        assert not any("coordinación" in e for e in evidence)


def test_no_coordination_keys_when_no_pairs():
    """Features stay untouched for solo accounts — they are part of the LLM
    cache key, so unconditional keys would invalidate every cached account."""
    acc = make_comment_account(
        "Solo Vecino", ["buen trabajo"],
        timestamps=[datetime(2026, 7, 13, tzinfo=timezone.utc).isoformat()],
    )
    acc.features = cu.compute_features(acc)
    cu.compute_coordination({acc.key: acc})
    assert "coordination_pair_posts" not in acc.features
    assert "coordination_min_gap" not in acc.features


def test_cloned_post_comments_are_deduped_across_urls():
    """The same physical post ingested under two pfbid URL forms clones its
    comment list. Same (author, text, exact timestamp) on another post = the
    same comment — it must not manufacture dup/burst/coordination signals.
    A real re-poster (different timestamps) still counts twice."""
    from unittest.mock import patch
    from src.scripts import classify_users as cu

    def doc(url, comments):
        return {"_source": {
            "source": {"name": "Pagina X", "stats": {}}, "news_type": "facebook",
            "url": url, "date_created": "2026-07-01T10:00:00", "comments": comments,
        }}

    clone_comment = {"comment_author": "Adan Trejo", "comment_text": "Puras promesas",
                     "comment_timestamp": "2026-07-14T00:03:18", "comment_likes": 0}
    reposter = [
        {"comment_author": "Iker Mitz", "comment_text": "Bola de rateros",
         "comment_timestamp": "2026-07-13T22:23:50", "comment_likes": 0},
        {"comment_author": "Iker Mitz", "comment_text": "Bola de rateros",
         "comment_timestamp": "2026-07-13T22:24:40", "comment_likes": 0},
    ]
    docs = [
        doc("https://facebook.com/p/pfbid_A", [clone_comment, reposter[0]]),
        doc("https://facebook.com/p/pfbid_A_clone", [dict(clone_comment)]),
        doc("https://facebook.com/p/other", [reposter[1]]),
    ]
    with patch.object(cu, "get_es_client"), \
         patch("elasticsearch.helpers.scan", return_value=docs):
        accounts, _ = cu.harvest_evidence(None, ["Pagina X"], None, 30)

    adan = [a for a in accounts.values() if a.display_name == "Adan Trejo"][0]
    assert adan.n_comments == 1                       # clone collapsed
    adan.features = cu.compute_features(adan)
    score, evidence = cu.compute_automation(adan.features)
    assert score == 0.0 and not evidence

    iker = [a for a in accounts.values() if a.display_name == "Iker Mitz"][0]
    assert iker.n_comments == 2                       # real repost kept
    iker.features = cu.compute_features(iker)
    score, evidence = cu.compute_automation(iker.features)
    assert score >= 0.5                               # dup+burst arm still fires


# --- Human labels (2026-07-21) ----------------------------------------------

def test_human_label_survives_classifier_upsert_and_wins_effective():
    """A reviewer's judgment lives under ``human`` (never in the machine
    record's $set) and effective_automation takes the max of both."""
    from src.models.social_users import SocialUsers, account_id, TIER_NAME
    store = SocialUsers(collection=MagicMock())
    _id = account_id(TIER_NAME, network="facebook", name="Blacky White")

    store.set_human_label(_id, 1.0, "automated_certain", by="oscar")
    call = store.collection.update_one.call_args
    assert call[0][1]["$set"] == {"human": {"automation": 1.0,
                                            "label": "automated_certain", "by": "oscar"}}

    # Classifier upsert of the same account: machine record has no `human` key.
    store.collection.reset_mock()
    store.get = MagicMock(return_value=None)
    store.upsert({"_id": _id, "n_comments": 2, "n_posts": 0, "automation_score": 0.0})
    setdoc = store.collection.update_one.call_args[0][1]["$set"]
    assert "human" not in setdoc

    rec = {"automation_score": 0.0, "human": {"automation": 1.0}}
    assert SocialUsers.effective_automation(rec) == 1.0
    assert SocialUsers.effective_automation({"automation_score": 0.7}) == 0.7
    assert SocialUsers.effective_automation(None) == 0.0


# --- Slogan-family / sweep / rapid-fire features (2026-07-21, WS-4 round 2) --

def test_slogan_family_near_duplicates_score():
    """Francisco shape: ALL-CAPS slogan variants across posts — exact-dup
    misses them, the near-dup cluster must not."""
    texts = [
        "SE ESTÁN ROBANDO EL PRESUPUESTO PARA SUS CAMPAÑAS ELECTORALES",
        "!!!!! SE ESTÁN ROBANDO EL PRESUPUESTO PARA SUS CAMPAÑAS ELECTORALES !!!!!",
        "SE ROBAN EL PRESUPUESTO PARA SUS CAMPAÑAS ELECTORALES",
    ]
    acc = make_comment_account("Francisco A", texts,
                               parent_docs=["https://p/a", "https://p/b", "https://p/c"])
    acc.features = cu.compute_features(acc)
    assert acc.features["max_near_duplicate_count"] == 3
    score, evidence = cu.compute_automation(acc.features)
    assert score >= 0.5
    assert any("casi idénticos" in e for e in evidence)


def test_cross_page_slogan_scores_high():
    """The same slogan family on 2+ different pages -> 0.7."""
    key = account_id(TIER_NAME, network="facebook", name="Cross Pager")
    acc = Account(tier=TIER_NAME, network="facebook", display_name="Cross Pager", key=key)
    for i, (page, text) in enumerate([
            ("Roberto Cabrera Valencia", "se están robando el presupuesto para sus campañas"),
            ("Bitácora Diario", "se están robando el presupuesto para sus campañas electorales")]):
        acc.n_comments += 1
        acc.pages.add(page)
        acc.parent_docs.add(f"https://p/{i}")
        acc.comment_items.append({"text": text, "likes": 0, "timestamp": None,
                                  "parent_doc_id": f"https://p/{i}", "page": page})
    acc.features = cu.compute_features(acc)
    assert acc.features["near_duplicate_pages"] == 2
    score, evidence = cu.compute_automation(acc.features)
    assert score >= 0.7
    assert any("páginas distintas" in e for e in evidence)


def test_short_texts_never_form_near_dup_clusters():
    """'Gracias'/'Amén' organic staples are below NEAR_DUP_MIN_LEN."""
    acc = make_comment_account("Vecina Agradecida", ["Gracias", "gracias", "Amén"],
                               parent_docs=["https://p/a", "https://p/b", "https://p/c"])
    acc.features = cu.compute_features(acc)
    assert acc.features["max_near_duplicate_count"] == 0


def test_sweep_thirty_minutes_flags():
    """4+ distinct posts inside 30 minutes -> review band."""
    base = datetime(2026, 7, 13, 21, 0, 0, tzinfo=timezone.utc)
    ts = [(base.replace(minute=m)).isoformat() for m in (1, 6, 12, 18, 22)]
    texts = [f"comentario distinto {i} suficientemente largo" * 1 for i in range(5)]
    texts = ["puro robo aqui", "asaltos a la alza ya", "baches por todos lados si",
             "infracciones fantasma otra vez", "policia cobrando cuota hoy"]
    acc = make_comment_account("Barredora", texts,
                               parent_docs=[f"https://p/{i}" for i in range(5)], timestamps=ts)
    acc.features = cu.compute_features(acc)
    assert acc.features["posts_swept_30min"] == 5
    score, evidence = cu.compute_automation(acc.features)
    assert score >= 0.5
    assert any("30 minutos" in e for e in evidence)


def test_rapid_fire_same_post_flags():
    """Danny shape: boilerplate triplet on one post inside a minute."""
    base = datetime(2026, 7, 19, 19, 37, 0, tzinfo=timezone.utc)
    ts = [(base.replace(second=s)).isoformat() for s in (0, 20, 40)]
    acc = make_comment_account("Auto Greeter", ["Buena tarde", "Gracias", "Gracias información"],
                               parent_docs=["https://p/x"] * 3, timestamps=ts)
    acc.features = cu.compute_features(acc)
    assert acc.features["same_post_rapid_pairs"] == 2
    score, evidence = cu.compute_automation(acc.features)
    assert score >= 0.5
    assert any("misma publicación" in e for e in evidence)


def test_organic_complainer_still_clean_with_new_features():
    """Negative control: varied grievances over days trip nothing new."""
    ts = [datetime(2026, 7, 10 + i, 12, 0, 0, tzinfo=timezone.utc).isoformat() for i in range(4)]
    texts = [
        "Llevo tres días sin agua en la colonia Lomas, nadie responde",
        "El bache de la avenida Juárez sigue igual, ya reporté dos veces",
        "Cobraron el predial doble y en tesorería no dan solución",
        "La basura no la recogen desde el lunes en el centro",
    ]
    acc = make_comment_account("Ma Elena Vazquez", texts,
                               parent_docs=[f"https://p/{i}" for i in range(4)], timestamps=ts)
    acc.features = cu.compute_features(acc)
    f = acc.features
    assert f["max_near_duplicate_count"] <= 1
    assert f["posts_swept_30min"] <= 1
    assert f["same_post_rapid_pairs"] == 0
    score, _ = cu.compute_automation(acc.features)
    assert score < 0.5
