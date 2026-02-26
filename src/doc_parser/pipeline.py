"""Orchestration: parse -> enhance -> extract -> save JSON."""

from __future__ import annotations

import asyncio
import logging
import mimetypes
import time
from pathlib import Path

from doc_parser.config import Settings
from doc_parser.google_drive import GoogleDriveClient
from doc_parser.hasher import sha256_file
from doc_parser.storage import has_result, load_result, save_result
from doc_parser.steps.step2_parse import run_parse
from doc_parser.steps.step3_extract import parse_date_to_epoch, run_extraction
from doc_parser.watermark import strip_watermark_lines
from doc_parser.chart_enhance import enhance_charts, strip_textin_image_urls

logger = logging.getLogger(__name__)


async def process_file(
    settings: Settings,
    sha: str,
    file_path: Path,
    *,
    source: str,
    file_name: str,
    force: bool = False,
    parse_mode: str | None = None,
    **extra_meta: object,
) -> dict | None:
    """Full pipeline: parse -> enhance -> extract -> save JSON.

    Returns the result dict, or None if skipped.
    """
    if not force and has_result(settings.extraction_path, sha):
        logger.info("Skipping %s -- result exists (use --force)", file_name)
        return None

    # 1. Parse
    parse_result = await run_parse(settings, file_path, parse_mode=parse_mode)

    # 2. Chart and table enhancement
    markdown = parse_result.markdown
    chart_count = 0
    table_count = 0
    if settings.vlm_model and (parse_result.has_chart or parse_result.has_table):
        try:
            markdown, chart_count, table_count = await enhance_charts(
                file_path,
                parse_result.markdown,
                parse_result.detail,
                settings,
                pages=parse_result.pages,
            )
            if chart_count > 0:
                logger.info("Enhanced %d chart(s) in %s", chart_count, file_name)
            if table_count > 0:
                logger.info("Enhanced %d table(s) in %s", table_count, file_name)
        except Exception as exc:
            logger.warning("Chart/table enhancement failed for %s: %s", file_name, exc)

    # 3. Strip TextIn CDN image URLs (enhance_charts does this for enhanced docs,
    #    but non-enhanced docs still have cover/decorative image URLs)
    if chart_count == 0 and table_count == 0:
        markdown = strip_textin_image_urls(markdown)

    # 4. Watermark stripping (once, on final markdown)
    markdown = strip_watermark_lines(markdown)

    # 5. Extract entities
    ext_result = await run_extraction(
        settings,
        file_path=file_path,
        markdown=markdown,
    )

    # 6. Assemble result
    fields = ext_result.fields
    mime_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
    result = {
        "sha256": sha,
        "file_name": file_name,
        "source": source,
        "local_path": str(file_path),
        "mime_type": mime_type,
        "file_size_bytes": file_path.stat().st_size,
        "drive_folder_id": extra_meta.get("drive_folder_id"),
        "processed_at": int(time.time()),

        "title": fields.get("title"),
        "broker": fields.get("broker"),
        "authors": fields.get("authors"),
        "publish_date": fields.get("publish_date"),
        "market": fields.get("market"),
        "asset_class": fields.get("asset_class"),
        "sector": fields.get("sector"),
        "document_type": fields.get("document_type"),
        "target_company": fields.get("target_company"),
        "ticker_symbol": fields.get("ticker_symbol"),

        "markdown": markdown,

        "parse_info": {
            "page_count": parse_result.total_page_number,
            "has_chart": parse_result.has_chart,
            "has_table": parse_result.has_table,
            "chart_count": chart_count,
            "table_count": table_count,
            "duration_ms": parse_result.duration_ms,
            "parse_mode": parse_mode or settings.textin_parse_mode,
        },
        "extraction_info": {
            "provider": settings.extraction_provider,
            "llm_model": settings.llm_model if settings.extraction_provider == "llm" else None,
            "duration_ms": ext_result.duration_ms,
        },
    }

    # 7. Save
    path = save_result(settings.extraction_path, result)
    logger.info("Saved result to %s", path)

    return result


async def process_local(
    settings: Settings,
    path: Path,
    *,
    force: bool = False,
    parse_mode: str | None = None,
) -> str | None:
    """Process a local file. Returns sha256 or None if skipped."""
    sha = sha256_file(path)
    result = await process_file(
        settings, sha, path,
        source="local",
        file_name=path.name,
        force=force,
        parse_mode=parse_mode,
    )
    return sha if result is not None else None


async def process_drive_file(
    settings: Settings,
    drive_file_id: str,
    *,
    force: bool = False,
    parse_mode: str | None = None,
) -> str | None:
    """Download and process a Drive file. Returns sha256 or None if skipped."""
    drive = GoogleDriveClient(settings)
    meta = await drive.get_file_metadata(drive_file_id)

    download_dir = settings.data_dir / "downloads"
    download_dir.mkdir(parents=True, exist_ok=True)
    dest_path = download_dir / meta.name

    await drive.download_file(drive_file_id, dest_path)

    sha = sha256_file(dest_path)
    result = await process_file(
        settings, sha, dest_path,
        source="drive",
        file_name=meta.name,
        force=force,
        parse_mode=parse_mode,
        drive_folder_id=meta.parents[0] if meta.parents else None,
    )
    return sha if result is not None else None


async def process_drive_folder(
    settings: Settings,
    folder_id: str,
    *,
    force: bool = False,
    parse_mode: str | None = None,
) -> list[str | None]:
    """Process all files in a Drive folder with semaphore concurrency."""
    drive = GoogleDriveClient(settings)
    files = await drive.list_files(folder_id)

    if not files:
        logger.warning("No supported files found in folder %s", folder_id)
        return []

    semaphore = asyncio.Semaphore(settings.textin_max_concurrent)

    async def _process(df) -> str | None:
        async with semaphore:
            try:
                return await process_drive_file(
                    settings, df.file_id,
                    force=force,
                    parse_mode=parse_mode,
                )
            except Exception:
                logger.exception("Failed to process %s", df.name)
                return None

    results = await asyncio.gather(*[_process(f) for f in files])
    return list(results)


async def re_extract(
    settings: Settings,
    sha: str,
    *,
    force: bool = False,
) -> dict | None:
    """Re-run extraction using stored markdown. No re-parse."""
    existing = load_result(settings.extraction_path, sha)
    if existing is None:
        logger.error("No existing result for sha %s", sha)
        return None

    markdown = existing.get("markdown")
    if not markdown:
        logger.error("No markdown in existing result for sha %s", sha)
        return None

    file_path = Path(existing["local_path"])

    ext_result = await run_extraction(
        settings,
        file_path=file_path,
        markdown=markdown,
    )

    # Update fields in existing result
    fields = ext_result.fields
    existing["title"] = fields.get("title")
    existing["broker"] = fields.get("broker")
    existing["authors"] = fields.get("authors")
    existing["publish_date"] = fields.get("publish_date")
    existing["market"] = fields.get("market")
    existing["asset_class"] = fields.get("asset_class")
    existing["sector"] = fields.get("sector")
    existing["document_type"] = fields.get("document_type")
    existing["target_company"] = fields.get("target_company")
    existing["ticker_symbol"] = fields.get("ticker_symbol")
    existing["processed_at"] = int(time.time())
    existing["extraction_info"] = {
        "provider": settings.extraction_provider,
        "llm_model": settings.llm_model if settings.extraction_provider == "llm" else None,
        "duration_ms": ext_result.duration_ms,
    }

    save_result(settings.extraction_path, existing)
    logger.info("Re-extracted entities for %s", existing["file_name"])
    return existing
