"""Tests for doc_parser.textin_client — TextIn API client."""

from __future__ import annotations

import base64
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from doc_parser.config import Settings
from doc_parser.textin_client import (
    DEFAULT_PARSEX_PARAMS,
    EXTRACTION_FIELDS,
    ExtractionResult,
    ParseResult,
    TextInAPIError,
    TextInClient,
    _is_retryable,
    decode_excel,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings() -> Settings:
    return Settings(
        textin_app_id="test-app",
        textin_secret_code="test-secret",
        textin_parse_mode="auto",
    )


def _make_client() -> TextInClient:
    return TextInClient(_make_settings())


# ---------------------------------------------------------------------------
# decode_excel
# ---------------------------------------------------------------------------

def test_decode_excel_roundtrip():
    """base64 encode → decode_excel returns original bytes."""
    original = b"spreadsheet-bytes"
    encoded = base64.b64encode(original).decode()
    assert decode_excel(encoded) == original


# ---------------------------------------------------------------------------
# _build_parsex_params
# ---------------------------------------------------------------------------

def test_build_parsex_params_defaults():
    """ParseX defaults include pdf_parse_mode and md_detail."""
    client = _make_client()
    params = client._build_parsex_params()
    assert params["pdf_parse_mode"] == "auto"
    assert params["remove_watermark"] == "1"
    assert params["md_detail"] == "2"
    assert params["md_table_flavor"] == "html"


def test_build_parsex_params_override():
    """ParseX params can be overridden."""
    client = _make_client()
    params = client._build_parsex_params(parse_mode="scan", get_excel=False, md_detail=1)
    assert params["pdf_parse_mode"] == "scan"
    assert params["get_excel"] == "0"
    assert params["md_detail"] == "1"


# ---------------------------------------------------------------------------
# _parse_response
# ---------------------------------------------------------------------------

def test_parse_response_full():
    """Full response is parsed into ParseResult correctly."""
    client = _make_client()
    data = {
        "markdown": "# Hello",
        "detail": [{"type": "text", "text": "Hello"}],
        "pages": [{"page_number": 1}],
        "excel": base64.b64encode(b"xls").decode(),
        "total_page_number": 3,
        "valid_page_number": 2,
        "duration": 500,
        "request_id": "req-1",
        "paragraphs": [{"text": "Hello"}],
        "metrics": {"quality": 0.9},
        "src_page_count": 5,
    }
    result = client._parse_response(data, {})
    assert result.markdown == "# Hello"
    assert len(result.detail) == 1
    assert result.excel_base64 is not None
    assert result.total_page_number == 3
    assert result.valid_page_number == 2
    assert result.duration_ms == 500
    assert result.request_id == "req-1"
    assert result.has_chart is False
    assert len(result.paragraphs) == 1
    assert result.metrics == {"quality": 0.9}
    assert result.src_page_count == 5


def test_parse_response_chart_detection():
    """has_chart is True when a chart image element exists with sub_type."""
    client = _make_client()
    data = {
        "detail": [
            {"type": "image", "sub_type": "chart"},
            {"type": "text", "text": "caption"},
        ],
    }
    result = client._parse_response(data, {})
    assert result.has_chart is True


def test_parse_response_chart_detection_image_type_ignored():
    """has_chart is False when only image_type (not sub_type) is set."""
    client = _make_client()
    data = {
        "detail": [
            {"type": "image", "image_type": "chart"},
        ],
    }
    result = client._parse_response(data, {})
    assert result.has_chart is False


def test_parse_response_missing_fields():
    """Missing fields default to empty/zero values."""
    client = _make_client()
    result = client._parse_response({}, {})
    assert result.markdown == ""
    assert result.detail == []
    assert result.pages == []
    assert result.excel_base64 is None
    assert result.total_page_number == 0
    assert result.duration_ms == 0
    assert result.paragraphs == []
    assert result.metrics == {}
    assert result.src_page_count == 0


# ---------------------------------------------------------------------------
# _parse_extraction_response
# ---------------------------------------------------------------------------

def test_parse_extraction_response():
    """Extraction response is parsed into ExtractionResult."""
    client = _make_client()
    data = {
        "details": {
            "title": [{"value": "Market Report", "position": {}}],
            "broker": [{"value": "Goldman Sachs", "position": {}}],
        },
        "category": {"type": "research"},
        "page_count": 5,
        "duration": 300,
        "request_id": "ext-1",
    }
    result = client._parse_extraction_response(data)
    assert result.fields["title"] == "Market Report"
    assert result.fields["broker"] == "Goldman Sachs"
    assert result.page_count == 5
    assert result.duration_ms == 300
    assert result.request_id == "ext-1"


def test_parse_extraction_response_empty():
    """Empty extraction response returns empty fields."""
    client = _make_client()
    result = client._parse_extraction_response({})
    assert result.fields == {}
    assert result.duration_ms == 0


# ---------------------------------------------------------------------------
# get_parsex_config
# ---------------------------------------------------------------------------

def test_get_parsex_config_matches_build_parsex_params():
    """get_parsex_config() returns the same dict as _build_parsex_params()."""
    client = _make_client()
    assert client.get_parsex_config() == client._build_parsex_params()


# ---------------------------------------------------------------------------
# _is_retryable
# ---------------------------------------------------------------------------

def test_is_retryable_5xx():
    """HTTP 5xx errors are retryable."""
    resp = MagicMock()
    resp.status_code = 502
    exc = httpx.HTTPStatusError("bad gateway", request=MagicMock(), response=resp)
    assert _is_retryable(exc) is True


def test_is_retryable_4xx():
    """HTTP 4xx errors are not retryable."""
    resp = MagicMock()
    resp.status_code = 403
    exc = httpx.HTTPStatusError("forbidden", request=MagicMock(), response=resp)
    assert _is_retryable(exc) is False


def test_is_retryable_connect_error():
    """ConnectError is retryable."""
    assert _is_retryable(httpx.ConnectError("connection refused")) is True


def test_is_retryable_read_timeout():
    """ReadTimeout is retryable."""
    assert _is_retryable(httpx.ReadTimeout("read timeout")) is True


def test_is_retryable_value_error():
    """ValueError is not retryable."""
    assert _is_retryable(ValueError("bad value")) is False


# ---------------------------------------------------------------------------
# TextInAPIError
# ---------------------------------------------------------------------------

def test_textin_api_error_attributes():
    """TextInAPIError stores code and message."""
    err = TextInAPIError(40101, "Invalid credentials")
    assert err.code == 40101
    assert err.message == "Invalid credentials"
    assert "40101" in str(err)
    assert "Invalid credentials" in str(err)


# ---------------------------------------------------------------------------
# EXTRACTION_FIELDS
# ---------------------------------------------------------------------------

def test_extraction_fields_defined():
    """EXTRACTION_FIELDS has the expected keys."""
    keys = {f["key"] for f in EXTRACTION_FIELDS}
    assert "title" in keys
    assert "broker" in keys
    assert "publish_date" in keys
    assert "ticker_symbol" in keys
    assert "market" in keys


# ---------------------------------------------------------------------------
# ExtractionResult dataclass
# ---------------------------------------------------------------------------

def test_extraction_result_defaults():
    """ExtractionResult has expected defaults."""
    er = ExtractionResult()
    assert er.fields == {}
    assert er.duration_ms == 0
    assert er.request_id == ""


# ---------------------------------------------------------------------------
# parse_file_x (mocked httpx) — ParseX endpoint
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_parse_file_x_success(tmp_path: Path):
    """parse_file_x returns ParseResult on success."""
    client = _make_client()
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF-1.4 test")

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {
        "code": 200,
        "result": {
            "markdown": "# ParseX Result",
            "detail": [{"type": "text", "text": "ParseX"}],
            "pages": [{"page_number": 1}],
            "paragraphs": [{"text": "paragraph"}],
            "metrics": {"score": 0.95},
            "total_page_number": 1,
            "valid_page_number": 1,
            "src_page_count": 3,
            "duration": 200,
            "request_id": "px-1",
        },
    }

    mock_http = AsyncMock()
    mock_http.post = AsyncMock(return_value=mock_response)
    mock_http.is_closed = False
    client._client = mock_http

    result = await client.parse_file_x(pdf)
    assert result.markdown == "# ParseX Result"
    assert result.src_page_count == 3
    assert len(result.paragraphs) == 1


# ---------------------------------------------------------------------------
# extract_entities (mocked httpx)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_extract_entities_success(tmp_path: Path):
    """extract_entities returns ExtractionResult on success."""
    client = _make_client()
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF-1.4 test")

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {
        "code": 200,
        "result": {
            "details": {
                "title": [{"value": "Q4 Report", "position": {}}],
                "broker": [{"value": "Morgan Stanley", "position": {}}],
            },
            "page_count": 10,
            "duration": 500,
            "request_id": "ext-r1",
        },
    }

    mock_http = AsyncMock()
    mock_http.post = AsyncMock(return_value=mock_response)
    mock_http.is_closed = False
    client._client = mock_http

    result = await client.extract_entities(pdf)
    assert isinstance(result, ExtractionResult)
    assert result.fields["title"] == "Q4 Report"
    assert result.fields["broker"] == "Morgan Stanley"
    assert result.page_count == 10
    assert result.request_id == "ext-r1"


@pytest.mark.asyncio
async def test_extract_entities_custom_fields(tmp_path: Path):
    """extract_entities accepts custom field definitions."""
    client = _make_client()
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF-1.4 test")

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {
        "code": 200,
        "result": {
            "details": {
                "custom_field": [{"value": "custom_value", "position": {}}],
            },
            "duration": 100,
            "request_id": "ext-custom",
        },
    }

    mock_http = AsyncMock()
    mock_http.post = AsyncMock(return_value=mock_response)
    mock_http.is_closed = False
    client._client = mock_http

    custom_fields = [{"key": "custom_field", "description": "A custom field"}]
    result = await client.extract_entities(pdf, fields=custom_fields)
    assert result.fields["custom_field"] == "custom_value"

    # Verify the payload sent had custom fields
    call_kwargs = mock_http.post.call_args
    assert call_kwargs.kwargs["json"]["fields"] == custom_fields


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_close_calls_aclose():
    """close() calls aclose() on the underlying httpx client."""
    client = _make_client()
    mock_http = AsyncMock()
    mock_http.is_closed = False
    mock_http.aclose = AsyncMock()
    client._client = mock_http

    await client.close()
    mock_http.aclose.assert_called_once()
