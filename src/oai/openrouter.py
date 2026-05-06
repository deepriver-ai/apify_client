"""
OpenRouter LLM client.

Wraps the OpenRouter API (https://openrouter.ai/docs) using the
OpenAI-compatible chat completions endpoint.

Configuration via environment variables:
    OPENROUTER_API_KEY  — required
    OPENROUTER_MODEL    — optional, defaults to "google/gemini-2.5-flash-lite"

Usage:
    from src.oai.openrouter import call_openrouter

    response = call_openrouter([
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Hello!"},
    ])
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


_API_URL = "https://openrouter.ai/api/v1/chat/completions"
_DEFAULT_MODEL = "google/gemini-2.5-flash-lite"


class OpenRouterClient:
    """Thin client for OpenRouter's chat completions endpoint."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ):
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OpenRouter API key is required. Set OPENROUTER_API_KEY "
                "environment variable or pass api_key to the constructor."
            )
        self.model = model or os.environ.get("OPENROUTER_MODEL", _DEFAULT_MODEL)
        self.temperature = temperature
        self.max_tokens = max_tokens

    def chat(
        self,
        messages: List[Dict[str, str]],
        *,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        response_format: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Send a chat completion request and return the response content.

        Args:
            messages: list of {"role": "system"|"user"|"assistant", "content": "..."}
            model: override the default model for this call.
            temperature: override the default temperature.
            max_tokens: override the default max_tokens.
            response_format: optional, e.g. {"type": "json_object"} for JSON mode.

        Returns:
            The text content of the first choice in the response.

        Raises:
            requests.HTTPError: on non-2xx responses from OpenRouter.
            ValueError: if the response body is missing expected fields.
        """
        used_model = model or self.model
        payload: Dict[str, Any] = {
            "model": used_model,
            "messages": messages,
            "temperature": temperature if temperature is not None else self.temperature,
            "max_tokens": max_tokens or self.max_tokens,
        }
        if response_format:
            payload["response_format"] = response_format

        logger.debug("LLM request model=%s messages=%s", used_model, json.dumps(messages, ensure_ascii=False))

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        resp = requests.post(_API_URL, headers=headers, json=payload, timeout=120)
        resp.raise_for_status()

        data = resp.json()

        choices = data.get("choices")
        if not choices:
            raise ValueError(f"OpenRouter returned no choices: {json.dumps(data)}")

        content = choices[0]["message"]["content"]
        logger.debug("LLM response model=%s content=%s", used_model, content[:1000])
        return content


# Module-level singleton, lazily initialised on first call.
_client: Optional[OpenRouterClient] = None


def _get_client() -> OpenRouterClient:
    global _client
    if _client is None:
        _client = OpenRouterClient()
    return _client


def call_openrouter(
    messages: List[Dict[str, str]],
    **kwargs: Any,
) -> str:
    """Convenience function — calls OpenRouter with the default client.

    Accepts the same keyword arguments as ``OpenRouterClient.chat()``.
    """
    return _get_client().chat(messages, **kwargs)
