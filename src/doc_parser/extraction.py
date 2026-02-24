"""Extraction provider protocol and implementations."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from doc_parser.config import Settings
from doc_parser.textin_client import (
    EXTRACTION_FIELDS,
    ExtractionResult,
    TextInClient,
    _is_retryable,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class ExtractionProvider(Protocol):
    async def extract(
        self,
        *,
        file_path: Path | None = None,
        markdown: str | None = None,
        fields: list[dict[str, str]],
    ) -> ExtractionResult: ...

    async def close(self) -> None: ...


# ---------------------------------------------------------------------------
# TextIn implementation (existing behaviour)
# ---------------------------------------------------------------------------


class TextInExtractionProvider:
    """Delegates to TextInClient.extract_entities() — sends raw file."""

    def __init__(self, settings: Settings) -> None:
        self._client = TextInClient(settings)

    async def extract(
        self,
        *,
        file_path: Path | None = None,
        markdown: str | None = None,
        fields: list[dict[str, str]],
    ) -> ExtractionResult:
        if file_path is None:
            raise ValueError("TextInExtractionProvider requires file_path")
        return await self._client.extract_entities(file_path, fields=fields)

    async def close(self) -> None:
        await self._client.close()


# ---------------------------------------------------------------------------
# LLM implementation (OpenRouter / OpenAI-compatible)
# ---------------------------------------------------------------------------


_SYSTEM_PROMPT_TEMPLATE = """\
You are a financial document metadata extractor. Extract the following fields \
from the document text. Return ONLY valid JSON with these keys:

{field_descriptions}

For any field you cannot determine, use null.\
"""


class LLMExtractionProvider:
    """Calls an OpenAI-compatible chat completions endpoint to extract fields."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(120.0, connect=30.0),
                headers={
                    "Authorization": f"Bearer {self._settings.llm_api_key}",
                    "Content-Type": "application/json",
                },
            )
        return self._client

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=4, min=4, max=16),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    async def extract(
        self,
        *,
        file_path: Path | None = None,
        markdown: str | None = None,
        fields: list[dict[str, str]],
    ) -> ExtractionResult:
        if not markdown:
            raise ValueError("LLMExtractionProvider requires markdown text")

        # Build field description block for the system prompt
        field_lines = "\n".join(
            f'- "{f["key"]}": {f["description"]}' for f in fields
        )
        system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(field_descriptions=field_lines)

        # Truncate markdown to configured limit
        context = markdown[: self._settings.llm_context_chars]

        payload = {
            "model": self._settings.llm_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": context},
            ],
            "max_tokens": self._settings.llm_max_tokens,
            "temperature": self._settings.llm_temperature,
        }

        client = await self._get_client()
        url = f"{self._settings.llm_base_url.rstrip('/')}/chat/completions"
        logger.info(
            "LLM extraction via %s (model=%s, context=%d chars)",
            url, self._settings.llm_model, len(context),
        )

        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        body = resp.json()

        # Parse the assistant reply as JSON
        content = body["choices"][0]["message"]["content"]
        extracted = _parse_json_response(content)

        return ExtractionResult(
            fields=extracted,
            duration_ms=0,
            request_id=body.get("id", ""),
        )

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()


def _parse_json_response(text: str) -> dict[str, Any]:
    """Extract a JSON object from the LLM response text.

    Handles responses wrapped in ```json ... ``` fences.
    """
    cleaned = text.strip()
    if cleaned.startswith("```"):
        # Remove markdown code fences
        lines = cleaned.split("\n")
        # Drop first line (```json) and last line (```)
        lines = [l for l in lines[1:] if not l.strip().startswith("```")]
        cleaned = "\n".join(lines)
    return json.loads(cleaned)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_extraction_provider(settings: Settings) -> ExtractionProvider:
    """Create an extraction provider based on settings."""
    if settings.extraction_provider == "llm":
        return LLMExtractionProvider(settings)
    return TextInExtractionProvider(settings)
