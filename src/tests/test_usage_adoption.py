"""Adoption guard (rate_limiting.md §2.6): every paid call goes through usage_client.

Fails when a module under ``src/`` (tests excluded):

- constructs an ``ApifyClient`` outside the allow-list below — Apify spend must go
  through ``usage_client.apify.run`` (``src.helpers.metering.metered_client`` for
  scripts, ``RecordingApifyClient`` for the worker / hand runs);
- references OpenRouter (``openrouter.ai``, a ``chat/completions`` URL, the ``openai``
  SDK) or imports ``src.oai.openrouter`` outside the allow-list;
- imports another LLM provider SDK (``google.genai``, ``anthropic``) outside the allow-list.

The OpenRouter allow-list is **pending work, not a permanent exception**: apify_client's
own LLM calls (crawl LLM filter, article-extraction tier, classify_users,
select_posts_for_comments) are still unmetered because ``usage_tools`` has no kind for
them yet (see CLAUDE.md §Usage metering). When they move to ``usage_client.llm.chat``,
empty ``OPENROUTER_PENDING`` — ``test_allow_lists_are_live`` keeps the lists honest.
"""
from __future__ import annotations

import ast
import pathlib
import re

SRC = pathlib.Path(__file__).resolve().parents[1]

# Raw ApifyClient construction allowed here only (each wraps it in RecordingApifyClient at once).
APIFY_CLIENT_ALLOWED = {
    "helpers/metering.py",     # raw_apify_client(): the scripts' factory, wrapped by metered_client
    "run_crawl_worker.py",     # the worker's apify_client_factory, wrapped by the runner
}

# Direct OpenRouter use still allowed: pending usage_tools kinds (see the module docstring).
OPENROUTER_PENDING = {
    "oai/openrouter.py",       # the raw OpenRouter client
    "oai/llm_core.py",         # cached wrapper over it (llm_cached_call)
}

# Other direct LLM providers: not routable through usage_client today (it speaks
# OpenRouter only). ips/ is an interactive analysis script (Gemini video description).
DIRECT_LLM_PENDING = {
    "scripts/ips/ai_describe.py",
    "scripts/ips/run_instagram_profile_posts.py",
}

_OPENROUTER_TEXT = re.compile(r"openrouter\.ai|/chat/completions|api\.openai\.com")


def _modules():
    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(SRC).as_posix()
        if rel.startswith("tests/"):
            continue
        yield rel, path.read_text(encoding="utf-8")


def _constructs_apify_client(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            f = node.func
            name = f.id if isinstance(f, ast.Name) else f.attr if isinstance(f, ast.Attribute) else None
            if name == "ApifyClient":
                return True
    return False


def _uses_openrouter(tree: ast.AST, source: str) -> bool:
    # Strings and code, not comments/docstrings: look at string constants and imports.
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and _OPENROUTER_TEXT.search(node.value):
            # a module/function docstring mentioning OpenRouter is fine; a URL constant is not
            if "://" in node.value or node.value.startswith("/chat/completions"):
                return True
        if isinstance(node, ast.Import) and any(a.name == "openai" or a.name.startswith("openai.")
                                                for a in node.names):
            return True
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "openai" or node.module.startswith("openai."):
                return True
            if node.module == "src.oai.openrouter" or (node.module == "src.oai" and
                                                       any(a.name == "openrouter" for a in node.names)):
                return True
    return False


def _uses_other_llm_provider(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            names = [node.module] + [f"{node.module}.{a.name}" for a in node.names]
        if any(n.startswith(("google.genai", "google.generativeai", "anthropic")) for n in names):
            return True
    return False


def test_no_raw_apify_client_outside_the_allow_list():
    offenders = [rel for rel, text in _modules()
                 if rel not in APIFY_CLIENT_ALLOWED and _constructs_apify_client(ast.parse(text))]
    assert offenders == [], (
        f"ApifyClient constructed in {offenders}: route Apify calls through usage_client "
        f"(src.helpers.metering.metered_client / metered_actor)")


def test_no_direct_openrouter_outside_the_allow_list():
    offenders = [rel for rel, text in _modules()
                 if rel not in OPENROUTER_PENDING and _uses_openrouter(ast.parse(text), text)]
    assert offenders == [], (
        f"direct OpenRouter/OpenAI use in {offenders}: call usage_client.llm.chat with a UsageContext")


def test_no_other_llm_provider_outside_the_allow_list():
    offenders = [rel for rel, text in _modules()
                 if rel not in DIRECT_LLM_PENDING and _uses_other_llm_provider(ast.parse(text))]
    assert offenders == [], f"direct LLM provider SDK in {offenders}: paid LLM calls go through usage_client"


def test_allow_lists_are_live():
    """An allow-list entry that no longer does the thing must be dropped (the lists only shrink)."""
    sources = dict(_modules())
    for rel in APIFY_CLIENT_ALLOWED:
        assert rel in sources and _constructs_apify_client(ast.parse(sources[rel])), rel
    for rel in OPENROUTER_PENDING:
        assert rel in sources and _uses_openrouter(ast.parse(sources[rel]), sources[rel]), rel
    for rel in DIRECT_LLM_PENDING:
        assert rel in sources and _uses_other_llm_provider(ast.parse(sources[rel])), rel


def test_guard_detects_offenders():
    assert _constructs_apify_client(ast.parse("from apify_client import ApifyClient\nc = ApifyClient('t')"))
    assert _constructs_apify_client(ast.parse("import apify_client\nc = apify_client.ApifyClient('t')"))
    assert not _constructs_apify_client(ast.parse("def f(client: 'ApifyClient'): pass"))
    assert _uses_openrouter(ast.parse("URL = 'https://openrouter.ai/api/v1'"), "")
    assert _uses_openrouter(ast.parse("from src.oai.openrouter import call_openrouter"), "")
    assert _uses_openrouter(ast.parse("import openai"), "")
    assert not _uses_openrouter(ast.parse('"""LLM calls go through OpenRouter."""'), "")
    assert _uses_other_llm_provider(ast.parse("from google import genai"))
    assert _uses_other_llm_provider(ast.parse("import anthropic"))
