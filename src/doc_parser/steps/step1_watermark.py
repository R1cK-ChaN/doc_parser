"""Step 1: Watermark removal via TextIn API."""

from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import select

from doc_parser.config import Settings
from doc_parser.db import get_session
from doc_parser.models import DocFile, DocWatermark, epoch_now
from doc_parser.storage import store_watermark_result
from doc_parser.textin_client import TextInClient

logger = logging.getLogger(__name__)


async def run_watermark_removal(
    settings: Settings,
    doc_file_id: int,
    *,
    force: bool = False,
) -> int | None:
    """Remove watermark from a document file.

    1. Load DocFile by ID
    2. Check for existing completed DocWatermark (skip if not force)
    3. Resolve file path
    4. Create DocWatermark row: status=running
    5. Call textin.remove_watermark(file_path)
    6. Decode base64, write to disk
    7. Update: status=completed
    8. On error: status=failed

    Returns doc_watermark.id or None on skip/failure.
    """
    textin = TextInClient(settings)
    try:
        return await _do_watermark_removal(settings, textin, doc_file_id, force=force)
    finally:
        await textin.close()


async def _do_watermark_removal(
    settings: Settings,
    textin: TextInClient,
    doc_file_id: int,
    *,
    force: bool = False,
) -> int | None:
    async with get_session() as session:
        # Load DocFile
        doc_file = await session.get(DocFile, doc_file_id)
        if doc_file is None:
            logger.error("DocFile id=%d not found", doc_file_id)
            return None

        # Check existing completed watermark removal
        if not force:
            existing = await session.execute(
                select(DocWatermark).where(
                    DocWatermark.doc_file_id == doc_file_id,
                    DocWatermark.status == "completed",
                )
            )
            if existing.scalar_one_or_none():
                logger.info("Skipping watermark removal for %s — already completed (use --force)", doc_file.file_name)
                return None

        # Resolve file path
        file_path = _resolve_file_path(doc_file)
        if file_path is None or not file_path.exists():
            logger.error("Cannot resolve file path for DocFile id=%d", doc_file_id)
            return None

        # Create DocWatermark row
        wm = DocWatermark(
            doc_file_id=doc_file_id,
            status="running",
            started_at=epoch_now(),
        )
        session.add(wm)
        await session.flush()

        try:
            result = await textin.remove_watermark(file_path)

            # Store cleaned image to disk
            rel_path = store_watermark_result(
                settings.watermark_path,
                doc_file.sha256,
                wm.id,
                result.image_base64,
            )

            wm.status = "completed"
            wm.completed_at = epoch_now()
            wm.duration_ms = result.duration_ms
            wm.cleaned_file_path = rel_path
            wm.pages_cleaned = 1

            logger.info(
                "Watermark removed for %s → %s (%dms)",
                doc_file.file_name, rel_path, result.duration_ms,
            )
            return wm.id

        except Exception as exc:
            wm.status = "failed"
            wm.completed_at = epoch_now()
            wm.error_message = str(exc)
            logger.error("Watermark removal failed for %s: %s", doc_file.file_name, exc)
            return None


def _resolve_file_path(doc_file: DocFile) -> Path | None:
    """Resolve the local file path for a DocFile."""
    if doc_file.local_path:
        return Path(doc_file.local_path)
    return None
