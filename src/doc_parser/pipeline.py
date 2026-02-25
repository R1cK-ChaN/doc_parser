"""Orchestration: download → parse → extraction."""

from __future__ import annotations

import asyncio
import logging
import mimetypes
import tempfile
from pathlib import Path

from sqlalchemy import select

from doc_parser.config import Settings
from doc_parser.db import get_session
from doc_parser.google_drive import DriveFile, GoogleDriveClient
from doc_parser.hasher import sha256_file
from doc_parser.models import DocElement, DocFile, DocParse, epoch_now
from doc_parser.steps import run_extraction, run_parse
from doc_parser.storage import store_parse_result
from doc_parser.textin_client import TextInClient

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
# Single file processing (ensures DocFile row exists first)
# ---------------------------------------------------------------------------


async def process_single_file(
    settings: Settings,
    textin: TextInClient,
    *,
    # Either a DriveFile + drive client, or a local path
    drive_file: DriveFile | None = None,
    drive_client: GoogleDriveClient | None = None,
    local_path: Path | None = None,
    reparse: bool = False,
    parse_mode: str | None = None,
    get_excel: bool = True,
    apply_chart: bool = True,
) -> int | None:
    """End-to-end processing for a single file. Returns the doc_parse ID or None on skip/failure."""

    # --- 1. Resolve file metadata + get a local path for parsing ---
    tmp_file = None
    if drive_file and drive_client:
        # Download to a temp file — deleted after parsing
        suffix = Path(drive_file.name).suffix
        tmp_file = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
        tmp_path = Path(tmp_file.name)
        tmp_file.close()
        await drive_client.download_file(drive_file.file_id, tmp_path)
        file_path = tmp_path
        file_id = drive_file.file_id
        source = "drive"
        file_name = drive_file.name
        mime_type = drive_file.mime_type
        file_size = drive_file.size
        folder_id = drive_file.parents[0] if drive_file.parents else None
    elif local_path:
        file_path = local_path
        file_id = f"local:{file_path.name}"
        source = "local"
        file_name = file_path.name
        mime_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        file_size = file_path.stat().st_size
        folder_id = None
    else:
        raise ValueError("Must provide either drive_file+drive_client or local_path")

    try:
        return await _do_parse(
            settings, textin, file_path,
            file_id=file_id, source=source, file_name=file_name,
            mime_type=mime_type, file_size=file_size, folder_id=folder_id,
            reparse=reparse, parse_mode=parse_mode,
            get_excel=get_excel, apply_chart=apply_chart,
        )
    finally:
        # Clean up temp file for Drive downloads
        if tmp_file is not None:
            Path(tmp_file.name).unlink(missing_ok=True)

async def _do_parse(
    settings: Settings,
    textin: TextInClient,
    file_path: Path,
    *,
    file_id: str,
    source: str,
    file_name: str,
    mime_type: str,
    file_size: int,
    folder_id: str | None,
    reparse: bool,
    parse_mode: str | None,
    get_excel: bool,
    apply_chart: bool,
) -> int | None:
    """Core parse logic once we have a local file_path to work with."""

    # --- 2. SHA-256 ---
    sha = sha256_file(file_path)
    logger.info("File %s  sha256=%s", file_name, sha[:12])

    async with get_session() as session:
        # --- 3. Upsert doc_file ---
        stmt = select(DocFile).where(DocFile.file_id == file_id)
        result = await session.execute(stmt)
        doc_file = result.scalar_one_or_none()

        if doc_file is None:
            doc_file = DocFile(
                file_id=file_id,
                sha256=sha,
                source=source,
                mime_type=mime_type,
                file_name=file_name,
                file_size_bytes=file_size,
                drive_folder_id=folder_id,
                local_path=str(file_path) if source == "local" else None,
            )
            session.add(doc_file)
            await session.flush()
        else:
            doc_file.sha256 = sha
            if source == "local":
                doc_file.local_path = str(file_path)
            await session.flush()

        # --- 4. Check existing parse ---
        if not reparse:
            existing = await session.execute(
                select(DocParse).where(
                    DocParse.doc_file_id == doc_file.id,
                    DocParse.status == "completed",
                )
            )
            if existing.scalar_one_or_none():
                logger.info("Skipping %s — already parsed (use --reparse to force)", file_name)
                return None

        # --- 5. Create doc_parse record ---
        parse_config = textin.get_parse_config(parse_mode, get_excel, apply_chart)
        doc_parse = DocParse(
            doc_file_id=doc_file.id,
            parse_mode=parse_config.get("parse_mode", "auto"),
            status="running",
            started_at=epoch_now(),
            parse_config=parse_config,
        )
        session.add(doc_parse)
        await session.flush()

        # --- 6. Call TextIn ---
        try:
            parse_result = await textin.parse_file(
                file_path,
                parse_mode=parse_mode,
                get_excel=get_excel,
                apply_chart=apply_chart,
            )
        except Exception as exc:
            doc_parse.status = "failed"
            doc_parse.completed_at = epoch_now()
            doc_parse.error_message = str(exc)
            logger.error("Parse failed for %s: %s", file_name, exc)
            return None

        # --- 7. Write outputs to disk ---
        paths = store_parse_result(
            settings.parsed_path, sha, doc_parse.id, parse_result
        )

        # --- 8. Update doc_parse ---
        doc_parse.status = "completed"
        doc_parse.completed_at = epoch_now()
        doc_parse.duration_ms = parse_result.duration_ms
        doc_parse.textin_request_id = parse_result.request_id
        doc_parse.markdown_path = paths.get("markdown_path")
        doc_parse.detail_json_path = paths.get("detail_json_path")
        doc_parse.pages_json_path = paths.get("pages_json_path")
        doc_parse.excel_path = paths.get("excel_path")
        doc_parse.has_excel = parse_result.excel_base64 is not None
        doc_parse.has_chart = parse_result.has_chart
        doc_parse.page_count = parse_result.total_page_number
        doc_parse.valid_page_count = parse_result.valid_page_number

        # --- 9. Extract doc_element rows ---
        for elem in parse_result.detail:
            doc_elem = DocElement(
                doc_parse_id=doc_parse.id,
                page_number=elem.get("page_number"),
                element_type=elem.get("type"),
                sub_type=elem.get("sub_type"),
                text=elem.get("text"),
                position=elem.get("position"),
                char_pos_start=elem.get("char_pos_start"),
                char_pos_end=elem.get("char_pos_end"),
                outline_level=elem.get("outline_level"),
                content_flag=elem.get("content_flag"),
                image_url=elem.get("image_url"),
                table_cells=elem.get("table_cells"),
            )
            session.add(doc_elem)

        # --- 10. Commit handled by context manager ---
        logger.info(
            "Parsed %s → %d elements, %d pages",
            file_name,
            len(parse_result.detail),
            parse_result.total_page_number,
        )
        return doc_parse.id


# ---------------------------------------------------------------------------
# Ensure DocFile row exists (for step-based commands)
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
# Batch processing (kept for backward compatibility)
# ---------------------------------------------------------------------------


async def _ensure_drive_doc_files(
    settings: Settings,
    folder_id: str,
) -> list[int]:
    """Ensure DocFile rows exist for all files in a Drive folder, return their IDs."""
    drive = GoogleDriveClient(settings)
    files = await drive.list_files(folder_id)

    if not files:
        logger.warning("No supported files found in folder %s", folder_id)
        return []

    doc_file_ids = []
    async with get_session() as session:
        for df in files:
            stmt = select(DocFile).where(DocFile.file_id == df.file_id)
            result = await session.execute(stmt)
            doc_file = result.scalar_one_or_none()

            if doc_file is None:
                doc_file = DocFile(
                    file_id=df.file_id,
                    sha256="0" * 64,  # placeholder until actual file is processed
                    source="drive",
                    mime_type=df.mime_type,
                    file_name=df.name,
                    file_size_bytes=df.size,
                    drive_folder_id=folder_id,
                )
                session.add(doc_file)
                await session.flush()

            doc_file_ids.append(doc_file.id)

    return doc_file_ids


async def process_folder(
    settings: Settings,
    folder_id: str,
    *,
    reparse: bool = False,
    parse_mode: str | None = None,
    get_excel: bool = True,
    apply_chart: bool = True,
) -> list[int | None]:
    """Process all supported files in a Google Drive folder.

    Uses asyncio.gather with a semaphore to limit concurrency.
    Individual failures do not abort the batch.
    """
    drive = GoogleDriveClient(settings)
    files = await drive.list_files(folder_id)

    if not files:
        logger.warning("No supported files found in folder %s", folder_id)
        return []

    textin = TextInClient(settings)
    semaphore = asyncio.Semaphore(settings.textin_max_concurrent)

    async def _process_with_limit(df: DriveFile) -> int | None:
        async with semaphore:
            try:
                return await process_single_file(
                    settings,
                    textin,
                    drive_file=df,
                    drive_client=drive,
                    reparse=reparse,
                    parse_mode=parse_mode,
                    get_excel=get_excel,
                    apply_chart=apply_chart,
                )
            except Exception:
                logger.exception("Failed to process %s", df.name)
                return None

    results = await asyncio.gather(*[_process_with_limit(f) for f in files])
    await textin.close()
    return list(results)


async def process_drive_file(
    settings: Settings,
    file_id: str,
    *,
    reparse: bool = False,
    parse_mode: str | None = None,
    get_excel: bool = True,
    apply_chart: bool = True,
) -> int | None:
    """Process a single file from Google Drive by its file ID."""
    drive = GoogleDriveClient(settings)
    meta = await drive.get_file_metadata(file_id)

    textin = TextInClient(settings)
    try:
        return await process_single_file(
            settings,
            textin,
            drive_file=meta,
            drive_client=drive,
            reparse=reparse,
            parse_mode=parse_mode,
            get_excel=get_excel,
            apply_chart=apply_chart,
        )
    finally:
        await textin.close()


async def process_local_file(
    settings: Settings,
    path: Path,
    *,
    reparse: bool = False,
    parse_mode: str | None = None,
    get_excel: bool = True,
    apply_chart: bool = True,
) -> int | None:
    """Process a local file (skip Google Drive)."""
    textin = TextInClient(settings)
    try:
        return await process_single_file(
            settings,
            textin,
            local_path=path,
            reparse=reparse,
            parse_mode=parse_mode,
            get_excel=get_excel,
            apply_chart=apply_chart,
        )
    finally:
        await textin.close()
