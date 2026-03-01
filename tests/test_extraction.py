"""Tests for doc_parser.extraction — LLM extraction provider."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from doc_parser.config import Settings
from doc_parser.extraction import (
    LLMExtractionProvider,
    _parse_json_response,
    create_extraction_provider,
)
from doc_parser.textin_client import EXTRACTION_FIELDS, ExtractionResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings(tmp_path: Path) -> Settings:
    return Settings(
        textin_app_id="test-app",
        textin_secret_code="test-secret",
        data_dir=tmp_path / "data",
        llm_api_key="test-key",
        llm_base_url="https://test.openrouter.ai/api/v1",
        llm_model="openai/gpt-4o-mini",
    )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def test_factory_returns_llm(tmp_path: Path):
    settings = _make_settings(tmp_path)
    provider = create_extraction_provider(settings)
    assert isinstance(provider, LLMExtractionProvider)


# ---------------------------------------------------------------------------
# LLMExtractionProvider
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_llm_provider_requires_markdown(tmp_path: Path):
    """LLMExtractionProvider raises ValueError without markdown."""
    settings = _make_settings(tmp_path)
    provider = LLMExtractionProvider(settings)
    with pytest.raises(ValueError, match="markdown"):
        await provider.extract(fields=EXTRACTION_FIELDS)
    await provider.close()


@pytest.mark.asyncio
async def test_llm_provider_calls_chat_completions(tmp_path: Path):
    """LLMExtractionProvider calls the chat completions endpoint."""
    settings = _make_settings(tmp_path)
    provider = LLMExtractionProvider(settings)

    llm_response = {
        "id": "chatcmpl-test",
        "choices": [{
            "message": {
                "content": json.dumps({
                    "title": "LLM Report",
                    "institution": "Test Broker",
                    "authors": None,
                    "publish_date": "2024-01-15",
                    "data_period": None,
                    "country": "US",
                    "market": "US",
                    "sector": "Tech",
                    "document_type": "Research",
                    "event_type": None,
                    "subject": "TestCorp",
                    "subject_id": "TST",
                    "language": "en",
                    "contains_commentary": True,
                }),
            },
        }],
    }

    mock_response = MagicMock(spec=httpx.Response)
    mock_response.json.return_value = llm_response
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.post.return_value = mock_response
    mock_client.is_closed = False
    provider._client = mock_client

    result = await provider.extract(
        markdown="# Test Document\n\nSome finance report content...",
        fields=EXTRACTION_FIELDS,
    )

    assert result.fields["title"] == "LLM Report"
    assert result.fields["institution"] == "Test Broker"
    assert result.fields["subject_id"] == "TST"
    assert result.request_id == "chatcmpl-test"

    # Verify the endpoint called
    mock_client.post.assert_called_once()
    call_url = mock_client.post.call_args[0][0]
    assert call_url.endswith("/chat/completions")

    await provider.close()


@pytest.mark.asyncio
async def test_llm_provider_truncates_context(tmp_path: Path):
    """LLMExtractionProvider truncates markdown to llm_context_chars."""
    settings = _make_settings(tmp_path)
    settings.llm_context_chars = 50
    provider = LLMExtractionProvider(settings)

    long_markdown = "x" * 200

    llm_response = {
        "id": "chatcmpl-trunc",
        "choices": [{"message": {"content": json.dumps({"title": "T"})}}],
    }

    mock_response = MagicMock(spec=httpx.Response)
    mock_response.json.return_value = llm_response
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.post.return_value = mock_response
    mock_client.is_closed = False
    provider._client = mock_client

    await provider.extract(markdown=long_markdown, fields=EXTRACTION_FIELDS)

    # Check the user message was truncated
    call_payload = mock_client.post.call_args[1]["json"]
    user_msg = call_payload["messages"][1]["content"]
    assert len(user_msg) == 50

    await provider.close()


# ---------------------------------------------------------------------------
# _parse_json_response
# ---------------------------------------------------------------------------

def test_parse_json_response_plain():
    """Plain JSON string is parsed correctly."""
    result = _parse_json_response('{"title": "Test"}')
    assert result == {"title": "Test"}


def test_parse_json_response_code_fence():
    """JSON wrapped in markdown code fence is parsed correctly."""
    text = '```json\n{"title": "Fenced"}\n```'
    result = _parse_json_response(text)
    assert result == {"title": "Fenced"}


def test_parse_json_response_code_fence_no_lang():
    """JSON wrapped in bare code fence is parsed correctly."""
    text = '```\n{"title": "Bare"}\n```'
    result = _parse_json_response(text)
    assert result == {"title": "Bare"}


def test_parse_json_response_invalid():
    """Invalid JSON raises ValueError."""
    with pytest.raises(json.JSONDecodeError):
        _parse_json_response("not json at all")
