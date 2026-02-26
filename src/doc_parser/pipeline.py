"""Orchestration: download → parse → extraction."""

from __future__ import annotations

import asyncio
import logging
import mimetypes
from pathlib import Path

from sqlalchemy import select

from doc_parser.config import Settings
from doc_parser.db import get_session
from doc_parser.google_drive import GoogleDriveClient
from doc_parser.hasher import sha256_file
from doc_parser.models import DocFile
from doc_parser.steps import run_extraction, run_parse

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Full 2-step pipeline
# ---------------------------------------------------------------------------


async def run_all_steps(
    settings: Settings,
    doc_file_id: int,
    *,
    force: bool = False,
) -> dict[str, int | None]:
    """Run all pipeline steps for a document file.

    Step 2 (parse) failure is critical — stops pipeline.
    Step 3 (extraction) failure is non-fatal (log warning, parse results still stored).

    Returns dict with step result IDs.
    """
    results: dict[str, int | None] = {
        "parse_id": None,
        "extraction_id": None,
    }

    # Step 1: Parse (critical)
    results["parse_id"] = await run_parse(
        settings, doc_file_id, force=force,
    )

    # Step 2: Extraction (non-fatal)
    try:
        results["extraction_id"] = await run_extraction(
            settings, doc_file_id, force=force,
        )
    except Exception as exc:
        logger.warning("Extraction failed for doc_file_id=%d: %s", doc_file_id, exc)

    return results


# ---------------------------------------------------------------------------
# Ensure DocFile row exists (local files)
# ---------------------------------------------------------------------------


async def ensure_doc_file(
    settings: Settings,
    local_path: Path,
) -> int:
    """Ensure a DocFile row exists for a local file, return its ID."""
    sha = sha256_file(local_path)
    file_id = f"local:{local_path.name}"

    async with get_session() as session:
        stmt = select(DocFile).where(DocFile.file_id == file_id)
        result = await session.execute(stmt)
        doc_file = result.scalar_one_or_none()

        if doc_file is None:
            mime_type = mimetypes.guess_type(local_path.name)[0] or "application/octet-stream"
            doc_file = DocFile(
                file_id=file_id,
                sha256=sha,
                source="local",
                mime_type=mime_type,
                file_name=local_path.name,
                file_size_bytes=local_path.stat().st_size,
                local_path=str(local_path),
            )
            session.add(doc_file)
            await session.flush()
        else:
            doc_file.sha256 = sha
            doc_file.local_path = str(local_path)
            await session.flush()

        return doc_file.id


# ---------------------------------------------------------------------------
# Ensure DocFile row exists (Drive files — downloads to local)
# ---------------------------------------------------------------------------


async def ensure_drive_doc_file(
    settings: Settings,
    drive_file_id: str,
) -> int:
    """Download a Drive file and ensure a DocFile row exists, return its ID.

    Downloads to ``settings.data_dir / "downloads" / filename``, computes
    sha256, then upserts DocFile with ``source="drive"`` and ``local_path``
    pointing to the downloaded file.
    """
    drive = GoogleDriveClient(settings)
    meta = await drive.get_file_metadata(drive_file_id)

    download_dir = settings.data_dir / "downloads"
    download_dir.mkdir(parents=True, exist_ok=True)
    dest_path = download_dir / meta.name

    await drive.download_file(drive_file_id, dest_path)

    sha = sha256_file(dest_path)

    async with get_session() as session:
        stmt = select(DocFile).where(DocFile.file_id == drive_file_id)
        result = await session.execute(stmt)
        doc_file = result.scalar_one_or_none()

        if doc_file is None:
            doc_file = DocFile(
                file_id=drive_file_id,
                sha256=sha,
                source="drive",
                mime_type=meta.mime_type,
                file_name=meta.name,
                file_size_bytes=meta.size,
                drive_folder_id=meta.parents[0] if meta.parents else None,
                local_path=str(dest_path),
            )
            session.add(doc_file)
            await session.flush()
        else:
            doc_file.sha256 = sha
            doc_file.local_path = str(dest_path)
            await session.flush()

        return doc_file.id


# ---------------------------------------------------------------------------
# Batch: ensure DocFile rows for all files in a Drive folder
# ---------------------------------------------------------------------------


async def _ensure_drive_doc_files(
    settings: Settings,
    folder_id: str,
) -> list[int]:
    """Ensure DocFile rows exist for all files in a Drive folder, return their IDs.

    Each file is downloaded so that ``local_path`` is set (required by run_parse).
    """
    drive = GoogleDriveClient(settings)
    files = await drive.list_files(folder_id)

    if not files:
        logger.warning("No supported files found in folder %s", folder_id)
        return []

    doc_file_ids = []
    for df in files:
        dfid = await ensure_drive_doc_file(settings, df.file_id)
        doc_file_ids.append(dfid)

    return doc_file_ids


# ---------------------------------------------------------------------------
# Batch: parse all files in a Drive folder
# ---------------------------------------------------------------------------


async def parse_drive_folder(
    settings: Settings,
    folder_id: str,
    *,
    force: bool = False,
    parse_mode: str | None = None,
    get_excel: bool = True,
) -> list[int | None]:
    """Parse all supported files in a Google Drive folder.

    Downloads each file, ensures DocFile rows, then runs ParseX with
    semaphore-limited concurrency.  Individual failures do not abort the batch.
    """
    drive = GoogleDriveClient(settings)
    files = await drive.list_files(folder_id)

    if not files:
        logger.warning("No supported files found in folder %s", folder_id)
        return []

    semaphore = asyncio.Semaphore(settings.textin_max_concurrent)

    async def _process(df) -> int | None:
        async with semaphore:
            try:
                dfid = await ensure_drive_doc_file(settings, df.file_id)
                return await run_parse(
                    settings, dfid,
                    force=force,
                    parse_mode=parse_mode,
                    get_excel=get_excel,
                )
            except Exception:
                logger.exception("Failed to process %s", df.name)
                return None

    results = await asyncio.gather(*[_process(f) for f in files])
    return list(results)
