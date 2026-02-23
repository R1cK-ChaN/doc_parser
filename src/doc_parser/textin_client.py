"""TextIn xParse HTTP client (sync endpoint via httpx.AsyncClient)."""

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

SYNC_ENDPOINT = "https://api.textin.com/ai/service/v1/pdf_to_markdown"

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


def _is_retryable(exc: BaseException) -> bool:
    """Determine whether an exception should trigger a retry."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    if isinstance(exc, (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout)):
        return True
    return False


class TextInClient:
    """Client for the TextIn xParse sync API."""

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
        """Parse a file via the TextIn sync endpoint.

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
        )

    def get_parse_config(
        self,
        parse_mode: str | None = None,
        get_excel: bool = True,
        apply_chart: bool = True,
    ) -> dict[str, str]:
        """Return the params dict that would be sent — for DB storage."""
        return self._build_params(parse_mode, get_excel, apply_chart)


class TextInAPIError(Exception):
    """Raised when TextIn returns a non-200 code."""

    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"TextIn API error {code}: {message}")


def decode_excel(b64: str) -> bytes:
    """Decode a base64-encoded Excel file from the TextIn response."""
    return base64.b64decode(b64)
