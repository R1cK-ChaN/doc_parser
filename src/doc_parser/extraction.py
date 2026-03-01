"""Extraction provider — LLM-based entity extraction."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

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
    _is_retryable,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# LLM implementation (OpenRouter / OpenAI-compatible)
# ---------------------------------------------------------------------------


_SYSTEM_PROMPT_TEMPLATE = """\
You are a financial document metadata extractor. The documents may be \
broker research reports, government statistical releases, central bank \
statements, press conference transcripts, news articles, or other \
financial/economic publications. Extract the following fields from the \
document text. Return ONLY valid JSON with these keys:

{field_descriptions}

The source text was produced by OCR and may contain character-level errors. \
In Chinese text, visually similar characters are often swapped \
(e.g., 周↔風, 辩↔牌, 宗↔资, 期↔朋). Use financial domain knowledge to \
correct likely OCR mistakes — prefer well-known financial terms and proper \
nouns over unlikely character combinations.

Today's date is {today}. For publish_date, extract the date exactly as it \
appears in the document text — do not substitute a different year based on \
assumptions.

data_period refers to the period the data covers, not the publication date \
(e.g., a CPI report published 2025-02-12 may cover data_period "2025-01"). \
Normalize to these formats: monthly "YYYY-MM", quarterly "YYYY-QN", \
annual "YYYY". Do not use spelled-out month names or other variations.

For contains_commentary, return true only if the document contains at \
least one full paragraph of qualitative analysis, interpretation, or \
opinion from analysts or officials. A document that is purely numerical \
tables, or that contains only a single sentence of boilerplate summary, \
should be false.

For language, use the primary language of the document body: "en" or "zh". \
If the document has substantial content in both languages, use "en,zh".

For impact_level, assess from a macro-finance trading perspective how \
significant this document's content is for financial markets. Use the same \
scale: "critical" for systemic events (bank failures, crashes, currency \
crises), "high" for major scheduled releases and policy decisions (rate \
decisions, CPI, NFP, tariffs), "medium" for notable market-moving topics \
(inflation data, yield moves, earnings, commodities), "low" for background \
context (housing, regulation, geopolitics), "info" for minimal market impact.

For confidence, reflect how certain you are about the impact_level. Use 0.9 \
for clear critical events, 0.8 for high, 0.7 for medium, 0.6 for low, 0.3 \
for info. Adjust within ±0.1 based on how clearly the content matches.

document_type describes the form of the document (e.g., "Research Report", \
"Meeting Minutes"). event_type describes the event that triggered the \
document (e.g., "Economic Release", "Press Conference"). The two may \
coincide (e.g., both "Policy Statement") — this is expected, not an error.

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
        from datetime import date
        system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(
            field_descriptions=field_lines,
            today=date.today().isoformat(),
        )

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


def create_extraction_provider(settings: Settings) -> LLMExtractionProvider:
    """Create an LLM extraction provider."""
    return LLMExtractionProvider(settings)
