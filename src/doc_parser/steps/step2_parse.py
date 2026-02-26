"""Step 2: ParseX (OCR to markdown) via TextIn API."""

from __future__ import annotations

import logging
from pathlib import Path

from doc_parser.config import Settings
from doc_parser.textin_client import ParseResult, TextInClient

logger = logging.getLogger(__name__)


async def run_parse(
    settings: Settings,
    file_path: Path,
    *,
    parse_mode: str | None = None,
) -> ParseResult:
    """Parse a file via TextIn ParseX. Returns in-memory result."""
    textin = TextInClient(settings)
    try:
        result = await textin.parse_file_x(
            file_path,
            parse_mode=parse_mode,
            get_excel=False,
        )
        logger.info(
            "Parsed %s -> %d elements, %d pages",
            file_path.name,
            len(result.detail),
            result.total_page_number,
        )
        return result
    finally:
        await textin.close()
