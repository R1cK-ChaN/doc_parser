"""TextIn API client for watermark removal, ParseX, and entity extraction."""

from __future__ import annotations

import base64
import logging
from dataclasses import dataclass, field
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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

SYNC_ENDPOINT = "https://api.textin.com/ai/service/v1/pdf_to_markdown"
WATERMARK_ENDPOINT = "https://api.textin.com/ai/service/v1/image/watermark_remove"
PARSEX_ENDPOINT = "https://api.textin.com/ai/service/v1/x_to_markdown"
EXTRACTION_ENDPOINT = "https://api.textin.com/ai/service/v2/entity_extraction"

# ---------------------------------------------------------------------------
# Default params
# ---------------------------------------------------------------------------

DEFAULT_PARAMS = {
    "parse_mode": "auto",
    "remove_watermark": "1",
    "apply_chart": "1",
    "get_excel": "1",
    "page_details": "1",
    "markdown_details": "1",
    "apply_merge": "1",
    "table_flavor": "html",
    "dpi": "144",
}

DEFAULT_PARSEX_PARAMS = {
    "pdf_parse_mode": "auto",
    "remove_watermark": "0",
    "md_detail": "2",
    "md_table_flavor": "html",
    "md_title": "1",
    "pdf_dpi": "144",
}

# ---------------------------------------------------------------------------
# Extraction field definitions
# ---------------------------------------------------------------------------

EXTRACTION_FIELDS = [
    {"key": "title", "description": "Document title or report title"},
    {"key": "broker", "description": "Brokerage firm or financial institution that published the report"},
    {"key": "authors", "description": "Author names, analysts who wrote the report"},
    {"key": "publish_date", "description": "Publication date of the report"},
    {"key": "market", "description": "Target market (e.g., US, China, Hong Kong, Global)"},
    {"key": "sector", "description": "Industry sector (e.g., Technology, Healthcare, Energy)"},
    {"key": "document_type", "description": "Type of document (e.g., Research Report, Market Commentary, Earnings Review)"},
    {"key": "target_company", "description": "Primary company being analyzed"},
    {"key": "ticker_symbol", "description": "Stock ticker symbol of the target company"},
]

# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ParseResult:
    """Structured result from a TextIn parse invocation."""

    markdown: str = ""
    detail: list[dict[str, Any]] = field(default_factory=list)
    pages: list[dict[str, Any]] = field(default_factory=list)
    elements: list[dict[str, Any]] = field(default_factory=list)
    excel_base64: str | None = None
    total_page_number: int = 0
    valid_page_number: int = 0
    duration_ms: int = 0
    request_id: str = ""
    has_chart: bool = False
    paragraphs: list[dict[str, Any]] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    src_page_count: int = 0


@dataclass
class WatermarkResult:
    """Result from watermark removal API."""

    image_base64: str = ""
    duration_ms: int = 0


@dataclass
class ExtractionResult:
    """Result from entity extraction API."""

    fields: dict[str, Any] = field(default_factory=dict)
    category: dict[str, Any] = field(default_factory=dict)
    detail_structure: list[dict[str, Any]] = field(default_factory=list)
    page_count: int = 0
    duration_ms: int = 0
    request_id: str = ""


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------


def _is_retryable(exc: BaseException) -> bool:
    """Determine whether an exception should trigger a retry."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    if isinstance(exc, (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout)):
        return True
    return False


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class TextInClient:
    """Client for the TextIn API suite."""

    def __init__(self, settings: Settings) -> None:
        self.app_id = settings.textin_app_id
        self.secret_code = settings.textin_secret_code
        self.default_parse_mode = settings.textin_parse_mode
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(300.0, connect=30.0),
                headers={
                    "x-ti-app-id": self.app_id,
                    "x-ti-secret-code": self.secret_code,
                },
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # -------------------------------------------------------------------
    # Legacy parse (kept for backward compatibility)
    # -------------------------------------------------------------------

    def _build_params(
        self,
        parse_mode: str | None = None,
        get_excel: bool = True,
        apply_chart: bool = True,
    ) -> dict[str, str]:
        """Build query params from defaults + overrides."""
        params = dict(DEFAULT_PARAMS)
        params["parse_mode"] = parse_mode or self.default_parse_mode
        if not get_excel:
            params["get_excel"] = "0"
        if not apply_chart:
            params["apply_chart"] = "0"
        return params

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=4, min=4, max=16),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    async def parse_file(
        self,
        file_path: Path,
        *,
        parse_mode: str | None = None,
        get_excel: bool = True,
        apply_chart: bool = True,
    ) -> ParseResult:
        """Parse a file via the TextIn sync endpoint (legacy).

        Sends the file as binary body (application/octet-stream)
        with config as query params.
        """
        params = self._build_params(parse_mode, get_excel, apply_chart)
        file_bytes = file_path.read_bytes()

        client = await self._get_client()
        logger.info("Sending %s to TextIn (%d bytes, mode=%s)", file_path.name, len(file_bytes), params["parse_mode"])

        resp = await client.post(
            SYNC_ENDPOINT,
            params=params,
            content=file_bytes,
            headers={"Content-Type": "application/octet-stream"},
        )
        resp.raise_for_status()
        body = resp.json()

        # TextIn wraps results in {"code": 200, "result": {...}}
        code = body.get("code", 0)
        if code != 200:
            msg = body.get("message", "Unknown TextIn error")
            raise TextInAPIError(code, msg)

        result_data = body.get("result", {})
        return self._parse_response(result_data, params)

    def _parse_response(self, data: dict[str, Any], params: dict[str, str]) -> ParseResult:
        """Convert the TextIn JSON response into a ParseResult."""
        detail = data.get("detail", [])

        # Check if any chart elements exist
        has_chart = any(
            el.get("type") == "image" and el.get("image_type") == "chart"
            for el in detail
        )

        return ParseResult(
            markdown=data.get("markdown", ""),
            detail=detail,
            pages=data.get("pages", []),
            elements=detail,  # alias for downstream convenience
            excel_base64=data.get("excel"),
            total_page_number=data.get("total_page_number", 0),
            valid_page_number=data.get("valid_page_number", 0),
            duration_ms=data.get("duration", 0),
            request_id=data.get("request_id", ""),
            has_chart=has_chart,
            paragraphs=data.get("paragraphs", []),
            metrics=data.get("metrics", {}),
            src_page_count=data.get("src_page_count", 0),
        )

    def get_parse_config(
        self,
        parse_mode: str | None = None,
        get_excel: bool = True,
        apply_chart: bool = True,
    ) -> dict[str, str]:
        """Return the params dict that would be sent — for DB storage."""
        return self._build_params(parse_mode, get_excel, apply_chart)

    # -------------------------------------------------------------------
    # Step 1: Watermark Removal
    # -------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=4, min=4, max=16),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    async def remove_watermark(self, file_path: Path) -> WatermarkResult:
        """Remove watermark from a file via TextIn watermark API.

        Sends file as binary body, returns base64-encoded cleaned image.
        """
        file_bytes = file_path.read_bytes()
        client = await self._get_client()
        logger.info("Removing watermark from %s (%d bytes)", file_path.name, len(file_bytes))

        resp = await client.post(
            WATERMARK_ENDPOINT,
            content=file_bytes,
            headers={"Content-Type": "application/octet-stream"},
        )
        resp.raise_for_status()
        body = resp.json()

        code = body.get("code", 0)
        if code != 200:
            msg = body.get("message", "Unknown TextIn error")
            raise TextInAPIError(code, msg)

        result_data = body.get("result", {})
        return WatermarkResult(
            image_base64=result_data.get("image", ""),
            duration_ms=result_data.get("duration", 0),
        )

    # -------------------------------------------------------------------
    # Step 2: ParseX (x_to_markdown)
    # -------------------------------------------------------------------

    def _build_parsex_params(
        self,
        parse_mode: str | None = None,
        get_excel: bool = True,
        md_detail: int = 2,
    ) -> dict[str, str]:
        """Build query params for ParseX endpoint."""
        params = dict(DEFAULT_PARSEX_PARAMS)
        if parse_mode:
            params["pdf_parse_mode"] = parse_mode
        if get_excel:
            params["get_excel"] = "1"
        else:
            params["get_excel"] = "0"
        params["md_detail"] = str(md_detail)
        return params

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=4, min=4, max=16),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    async def parse_file_x(
        self,
        file_path: Path,
        *,
        parse_mode: str | None = None,
        get_excel: bool = True,
        md_detail: int = 2,
    ) -> ParseResult:
        """Parse a file via the TextIn ParseX (x_to_markdown) endpoint.

        Returns ParseResult with markdown, detail, pages, paragraphs, metrics.
        """
        params = self._build_parsex_params(parse_mode, get_excel, md_detail)
        file_bytes = file_path.read_bytes()

        client = await self._get_client()
        logger.info("ParseX %s (%d bytes, mode=%s)", file_path.name, len(file_bytes), params["pdf_parse_mode"])

        resp = await client.post(
            PARSEX_ENDPOINT,
            params=params,
            content=file_bytes,
            headers={"Content-Type": "application/octet-stream"},
        )
        resp.raise_for_status()
        body = resp.json()

        code = body.get("code", 0)
        if code != 200:
            msg = body.get("message", "Unknown TextIn error")
            raise TextInAPIError(code, msg)

        result_data = body.get("result", {})
        return self._parse_response(result_data, params)

    def get_parsex_config(
        self,
        parse_mode: str | None = None,
        get_excel: bool = True,
        md_detail: int = 2,
    ) -> dict[str, str]:
        """Return the ParseX params dict — for DB storage."""
        return self._build_parsex_params(parse_mode, get_excel, md_detail)

    # -------------------------------------------------------------------
    # Step 3: Entity Extraction
    # -------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=4, min=4, max=16),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    async def extract_entities(
        self,
        file_path: Path,
        fields: list[dict[str, str]] | None = None,
    ) -> ExtractionResult:
        """Extract structured entities from a file via TextIn extraction API.

        Sends JSON with base64-encoded file and field definitions.
        """
        file_bytes = file_path.read_bytes()
        file_b64 = base64.b64encode(file_bytes).decode()
        use_fields = fields or EXTRACTION_FIELDS

        client = await self._get_client()
        logger.info("Extracting entities from %s (%d bytes, %d fields)", file_path.name, len(file_bytes), len(use_fields))

        payload = {
            "file": file_b64,
            "fields": use_fields,
        }

        resp = await client.post(
            EXTRACTION_ENDPOINT,
            json=payload,
        )
        resp.raise_for_status()
        body = resp.json()

        code = body.get("code", 0)
        if code != 200:
            msg = body.get("message", "Unknown TextIn error")
            raise TextInAPIError(code, msg)

        result_data = body.get("result", {})
        return self._parse_extraction_response(result_data)

    def _parse_extraction_response(self, data: dict[str, Any]) -> ExtractionResult:
        """Convert the TextIn extraction response into an ExtractionResult."""
        # Extract field values from the details structure
        details = data.get("details", {})
        fields: dict[str, Any] = {}
        for key, entries in details.items():
            if isinstance(entries, list) and entries:
                fields[key] = entries[0].get("value", "")
            elif isinstance(entries, dict):
                fields[key] = entries.get("value", "")

        return ExtractionResult(
            fields=fields,
            category=data.get("category", {}),
            detail_structure=data.get("details_list", []),
            page_count=data.get("page_count", 0),
            duration_ms=data.get("duration", 0),
            request_id=data.get("request_id", ""),
        )


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class TextInAPIError(Exception):
    """Raised when TextIn returns a non-200 code."""

    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"TextIn API error {code}: {message}")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def decode_excel(b64: str) -> bytes:
    """Decode a base64-encoded Excel file from the TextIn response."""
    return base64.b64decode(b64)
