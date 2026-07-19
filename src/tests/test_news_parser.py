"""Tests for the attached-article parsing pipeline (``src/models/news_parser``).

Covers two defects observed on OEM (Organización Editorial Mexicana) pages:

* Defect 1 — ``load_url.fetch_html`` timeout escalation: a short connect timeout
  (so dead hosts fail fast) with a generous, per-attempt-escalating read timeout
  (so slow-but-succeeding origins like oem.com.mx succeed on the first attempt
  instead of burning ~2 min on retries).
* Defect 2 — ``parser._has_meaningful_content`` rejecting related-content lists
  built from repeated section labels (LOCAL/POLICIACA/...), which the old
  "Ver más" heuristic missed, so such pages fall through to the LLM tier.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from src.models.news_parser import load_url
from src.models.news_parser.load_url import (
    CONNECT_TIMEOUT,
    READ_TIMEOUTS,
    fetch_html,
)
from src.models.news_parser.parser import (
    _has_meaningful_content,
    _looks_like_section_label_list,
    extract_article,
)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
OEM_FIXTURE = os.path.join(CACHE_DIR, "oem_related_content_sample.html")
OEM_URL = (
    "https://oem.com.mx/elsoldesanjuandelrio/local/"
    "ciudadania-tendra-que-defender-programas-hidricos-tono-perez-31157721"
)

# The real sidebar body that NewsPlease/newspaper4k extract from the OEM page:
# a stack of "SECTION\nHeadline\nDek" triples. The section label "LOCAL" repeats.
OEM_SIDEBAR_BODY = (
    "LOCAL\n"
    "Mejoraron movilidad en Rincón de la Florida\n"
    "Alcalde dijo que se rehabilitó una de las calles por donde hay mayor flujo de personas\n"
    "LOCAL\n"
    "Resguardan a lomitos en malas condiciones\n"
    "Fue derivado de un reporte ciudadano hacia el área de Cuidado Animal de San Juan del Río\n"
    "LOCAL\n"
    "Avizoran tramite de visa para adultos mayores\n"
    "Luis Nava dijo que se llamara reunificación de paisanos\n"
    "LOCAL\n"
    "Van 42 casos de gusano barrenador en la región\n"
    "Tequisquiapan y el municipio de San Juan del Río concentran los casos\n"
)

# A genuine article that *mentions* section-label words inline — must be accepted.
LEGIT_ARTICLE_BODY = (
    "El gobierno local presentó este martes su nueva estrategia de seguridad "
    "para el municipio. Durante la rueda de prensa, el alcalde explicó que la "
    "policía municipal reforzará el patrullaje en las zonas de mayor incidencia. "
    "La cultura de la prevención, dijo, es clave para reducir los delitos. "
    "Autoridades de finanzas confirmaron que el presupuesto para deportes y "
    "actividades sociales se mantendrá sin cambios durante el resto del año. "
    "El funcionario agregó que la coordinación entre corporaciones ha permitido "
    "una respuesta más rápida ante emergencias en la región."
)


# --------------------------------------------------------------------------- #
# Defect 2 — meaningful-content guard for section-label related lists
# --------------------------------------------------------------------------- #
class TestSectionLabelGuard:
    def test_rejects_repeated_section_labels(self):
        assert _looks_like_section_label_list(OEM_SIDEBAR_BODY) is True

    def test_oem_sidebar_not_meaningful(self):
        result = {"title": "Ciudadanía tendrá que defender programas hídricos", "body": OEM_SIDEBAR_BODY}
        assert _has_meaningful_content(result) is False

    def test_legit_article_with_inline_section_words_accepted(self):
        # Mentions "local", "policía", "cultura", "finanzas", "deportes" inline
        # but none stand alone on their own line — must NOT be rejected.
        assert _looks_like_section_label_list(LEGIT_ARTICLE_BODY) is False
        result = {"title": "Nueva estrategia de seguridad", "body": LEGIT_ARTICLE_BODY}
        assert _has_meaningful_content(result) is True

    def test_rejects_verbatim_repeated_short_line(self):
        body = ("Titular repetido\n" * 4) + "Algo de texto adicional que rellena. " * 10
        assert _looks_like_section_label_list(body) is True

    def test_two_section_labels_not_enough(self):
        # Below the 3-hit threshold — a real article could plausibly have two.
        body = "LOCAL\n" + LEGIT_ARTICLE_BODY + "\nDEPORTES\n"
        assert _looks_like_section_label_list(body) is False


# --------------------------------------------------------------------------- #
# Defect 2 — extract_article falls through to the LLM tier on OEM pages
# --------------------------------------------------------------------------- #
class TestExtractArticleFallthrough:
    def test_oem_page_falls_through_to_llm(self):
        with open(OEM_FIXTURE, encoding="utf-8") as f:
            html = f.read()

        clean_article = {
            "title": "Ciudadanía tendrá que defender programas hídricos: Toño Pérez",
            "body": (
                "El director de la JAPAM afirmó que se sentaron las bases de los "
                "proyectos hídricos y que la población deberá exigir la continuidad "
                "de los mismos. Explicó que la infraestructura de agua potable "
                "requiere mantenimiento constante para garantizar el abasto."
            ),
            "author": "Mario Luna",
            "media_urls": [],
            "timestamp": "2026-07-18T10:29:48",
        }

        # Mock the LLM tier so the test never hits the network.
        with patch(
            "src.models.news_parser.parser._parse_with_llm",
            return_value=clean_article,
        ) as mock_llm:
            result = extract_article(html, OEM_URL)

        # The parser tiers extract the sidebar, the guard rejects it, so the LLM
        # tier must be invoked and its clean body returned.
        assert mock_llm.called, "guard should have forced LLM fallthrough"
        assert result is not None
        assert "Mejoraron movilidad" not in result["body"]
        assert "lomitos" not in result["body"]
        assert "JAPAM" in result["body"]


# --------------------------------------------------------------------------- #
# Defect 1 — fetch_html timeout escalation (mocked; no network)
# --------------------------------------------------------------------------- #
def _fake_response(body: bytes = b"<html>ok</html>", url: str = "https://x/final", status: int = 200):
    resp = MagicMock()
    resp.headers = {}
    resp.encoding = "utf-8"
    resp.url = url
    resp.status_code = status
    resp.raise_for_status = MagicMock()
    resp.iter_content = MagicMock(return_value=[body])
    return resp


class TestFetchTimeouts:
    def test_connect_short_read_generous_on_first_attempt(self):
        # A slow-but-succeeding origin returns on the first attempt; the first
        # attempt must already use a short connect + generous read timeout.
        resp = _fake_response()
        with patch.object(load_url.requests, "get", return_value=resp) as mock_get:
            html, final = fetch_html("https://slow.example/article")

        assert html == "<html>ok</html>"
        assert final == "https://x/final"
        assert mock_get.call_count == 1
        timeout = mock_get.call_args.kwargs["timeout"]
        assert timeout == (CONNECT_TIMEOUT, READ_TIMEOUTS[0])
        # First-attempt read budget must cover known slow origins (~80s TTFB).
        assert READ_TIMEOUTS[0] >= 80

    def test_read_timeout_escalates_across_retries(self):
        # Two failures then success — read timeout must grow each attempt.
        resp = _fake_response()
        side_effects = [Exception("boom"), Exception("boom"), resp]
        with patch.object(load_url.requests, "get", side_effect=side_effects) as mock_get, patch.object(
            load_url.time, "sleep", return_value=None
        ):
            html, _ = fetch_html("https://flaky.example/article")

        assert html == "<html>ok</html>"
        assert mock_get.call_count == 3
        read_timeouts = [call.kwargs["timeout"][1] for call in mock_get.call_args_list]
        assert read_timeouts == list(READ_TIMEOUTS)
        # Monotonically non-decreasing escalation.
        assert read_timeouts == sorted(read_timeouts)

    def test_all_attempts_fail_returns_none(self):
        with patch.object(load_url.requests, "get", side_effect=Exception("dead")), patch.object(
            load_url.time, "sleep", return_value=None
        ):
            assert fetch_html("https://dead.example") == (None, None)

    def test_size_guard_via_content_length(self):
        resp = _fake_response()
        resp.headers = {"Content-Length": str(load_url.MAX_RESP_SIZE + 1)}
        with patch.object(load_url.requests, "get", return_value=resp):
            assert fetch_html("https://big.example") == (None, None)
