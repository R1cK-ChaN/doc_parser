"""Click CLI commands for doc-parser."""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from doc_parser.config import get_settings

console = Console()


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )


def _init_db_engine(settings=None):
    """Initialize the DB engine from settings."""
    from doc_parser.db import init_engine

    if settings is None:
        settings = get_settings()
    init_engine(settings)
    return settings


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """doc-parser — Finance report parsing pipeline."""
    _setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


# ---------------------------------------------------------------------------
# init-db
# ---------------------------------------------------------------------------


@cli.command("init-db")
def init_db() -> None:
    """Create PostgreSQL tables via Alembic migration."""
    import subprocess

    result = subprocess.run(
        ["alembic", "upgrade", "head"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        console.print("[green]Database tables created successfully.[/green]")
    else:
        console.print(f"[red]Migration failed:[/red]\n{result.stderr}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Step 1: Parse
# ---------------------------------------------------------------------------


@cli.command("parse")
@click.argument("file_id", type=int)
@click.option("--force", is_flag=True, help="Re-parse even if already completed.")
@click.option("--parse-mode", default=None, help="TextIn parse mode override.")
@click.option("--no-excel", is_flag=True, help="Skip Excel extraction.")
def parse(file_id: int, force: bool, parse_mode: str | None, no_excel: bool) -> None:
    """Parse a document via ParseX (Step 1)."""
    from doc_parser.steps import run_parse

    settings = _init_db_engine()
    settings.ensure_dirs()

    result = asyncio.run(
        run_parse(
            settings, file_id,
            force=force,
            parse_mode=parse_mode, get_excel=not no_excel,
        )
    )

    if result:
        console.print(f"[green]Parse complete.[/green] doc_parse.id={result}")
    else:
        console.print("[yellow]Skipped (already parsed or failed).[/yellow]")


@cli.command("parse-folder")
@click.argument("folder_id")
@click.option("--reparse", is_flag=True, help="Re-parse files that already have a completed parse.")
@click.option("--parse-mode", default=None, help="TextIn parse mode override.")
@click.option("--no-excel", is_flag=True, help="Skip Excel extraction.")
def parse_folder(
    folder_id: str,
    reparse: bool,
    parse_mode: str | None,
    no_excel: bool,
) -> None:
    """Parse all supported files in a Google Drive folder."""
    from doc_parser.pipeline import parse_drive_folder

    settings = _init_db_engine()
    settings.ensure_dirs()

    results = asyncio.run(
        parse_drive_folder(
            settings,
            folder_id,
            force=reparse,
            parse_mode=parse_mode,
            get_excel=not no_excel,
        )
    )

    parsed = sum(1 for r in results if r is not None)
    skipped = sum(1 for r in results if r is None)
    console.print(f"\n[green]Done.[/green] Parsed: {parsed}, Skipped: {skipped}")


@cli.command("parse-file")
@click.argument("file_id")
@click.option("--reparse", is_flag=True, help="Re-parse even if already completed.")
@click.option("--parse-mode", default=None, help="TextIn parse mode override.")
@click.option("--no-excel", is_flag=True, help="Skip Excel extraction.")
def parse_file(
    file_id: str,
    reparse: bool,
    parse_mode: str | None,
    no_excel: bool,
) -> None:
    """Parse a single file from Google Drive."""
    from doc_parser.pipeline import ensure_drive_doc_file
    from doc_parser.steps import run_parse

    settings = _init_db_engine()
    settings.ensure_dirs()

    async def _run():
        doc_file_id = await ensure_drive_doc_file(settings, file_id)
        return await run_parse(
            settings, doc_file_id,
            force=reparse,
            parse_mode=parse_mode,
            get_excel=not no_excel,
        )

    result = asyncio.run(_run())

    if result:
        console.print(f"[green]Parse complete.[/green] doc_parse.id={result}")
    else:
        console.print("[yellow]File skipped (already parsed or failed).[/yellow]")


@cli.command("parse-local")
@click.argument("path", type=click.Path(exists=True, path_type=Path))
@click.option("--reparse", is_flag=True, help="Re-parse even if already completed.")
@click.option("--parse-mode", default=None, help="TextIn parse mode override.")
@click.option("--no-excel", is_flag=True, help="Skip Excel extraction.")
def parse_local(
    path: Path,
    reparse: bool,
    parse_mode: str | None,
    no_excel: bool,
) -> None:
    """Parse a local file (skip Google Drive, for testing)."""
    from doc_parser.pipeline import ensure_doc_file
    from doc_parser.steps import run_parse

    settings = _init_db_engine()
    settings.ensure_dirs()

    async def _run():
        doc_file_id = await ensure_doc_file(settings, path)
        return await run_parse(
            settings, doc_file_id,
            force=reparse,
            parse_mode=parse_mode,
            get_excel=not no_excel,
        )

    result = asyncio.run(_run())

    if result:
        console.print(f"[green]Parse complete.[/green] doc_parse.id={result}")
    else:
        console.print("[yellow]File skipped (already parsed or failed).[/yellow]")


# ---------------------------------------------------------------------------
# Chart enhancement
# ---------------------------------------------------------------------------


@cli.command("enhance-charts")
@click.argument("doc_file_id", type=int)
def enhance_charts_cmd(doc_file_id: int) -> None:
    """Retroactively enhance charts in an existing parse with VLM summaries."""
    import json

    from sqlalchemy import select

    from doc_parser.chart_enhance import enhance_charts
    from doc_parser.db import get_session
    from doc_parser.models import DocFile, DocParse
    from doc_parser.storage import store_enhanced_markdown

    settings = _init_db_engine()
    settings.ensure_dirs()

    if not settings.vlm_model:
        console.print("[red]VLM model not configured.[/red] Set VLM_MODEL in .env")
        sys.exit(1)

    async def _run():
        async with get_session() as session:
            doc_file = await session.get(DocFile, doc_file_id)
            if doc_file is None:
                console.print(f"[red]DocFile id={doc_file_id} not found.[/red]")
                return

            # Find the latest completed parse
            result = await session.execute(
                select(DocParse)
                .where(
                    DocParse.doc_file_id == doc_file_id,
                    DocParse.status == "completed",
                )
                .order_by(DocParse.id.desc())
                .limit(1)
            )
            doc_parse = result.scalar_one_or_none()
            if doc_parse is None:
                console.print("[red]No completed parse found.[/red]")
                return

            if not doc_parse.markdown_path:
                console.print("[red]No markdown path on parse.[/red]")
                return

            # Load markdown and detail
            md_full = settings.parsed_path / doc_parse.markdown_path
            markdown = md_full.read_text(encoding="utf-8")

            detail_full = settings.parsed_path / doc_parse.detail_json_path
            detail = json.loads(detail_full.read_text(encoding="utf-8"))

            # Load pages JSON for coordinate scaling
            pages = []
            if doc_parse.pages_json_path:
                pages_full = settings.parsed_path / doc_parse.pages_json_path
                if pages_full.exists():
                    pages = json.loads(pages_full.read_text(encoding="utf-8"))

            # Resolve PDF path
            file_path = doc_file.local_path
            if not file_path:
                console.print("[red]No local file path available.[/red]")
                return

            enhanced_md, chart_count = await enhance_charts(
                file_path, markdown, detail, settings, pages=pages,
            )

            if chart_count == 0:
                console.print("[yellow]No charts found to enhance.[/yellow]")
                return

            enh_path = store_enhanced_markdown(
                settings.parsed_path,
                doc_file.sha256,
                doc_parse.id,
                enhanced_md,
            )
            doc_parse.enhanced_markdown_path = enh_path
            doc_parse.chart_count = chart_count

        console.print(
            f"[green]Enhanced {chart_count} chart(s).[/green] "
            f"Saved to {enh_path}"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Step 2: Entity extraction
# ---------------------------------------------------------------------------


@cli.command("extract")
@click.argument("file_id", type=int)
@click.option("--force", is_flag=True, help="Re-extract even if already completed.")
@click.option("--provider", type=click.Choice(["textin", "llm"]), default=None,
              help="Extraction provider (overrides config).")
def extract(file_id: int, force: bool, provider: str | None) -> None:
    """Extract structured entities from a document (Step 3)."""
    from doc_parser.steps import run_extraction

    settings = _init_db_engine()
    settings.ensure_dirs()
    if provider:
        settings.extraction_provider = provider

    result = asyncio.run(run_extraction(settings, file_id, force=force))

    if result:
        console.print(f"[green]Extraction complete.[/green] doc_extraction.id={result}")
    else:
        console.print("[yellow]Skipped (already completed or failed).[/yellow]")


@cli.command("extract-folder")
@click.argument("folder_id")
@click.option("--force", is_flag=True, help="Re-extract even if already completed.")
@click.option("--provider", type=click.Choice(["textin", "llm"]), default=None,
              help="Extraction provider (overrides config).")
def extract_folder(folder_id: str, force: bool, provider: str | None) -> None:
    """Extract entities from all files in a Google Drive folder (Step 3)."""
    from doc_parser.pipeline import _ensure_drive_doc_files

    settings = _init_db_engine()
    settings.ensure_dirs()
    if provider:
        settings.extraction_provider = provider

    async def _run():
        doc_file_ids = await _ensure_drive_doc_files(settings, folder_id)
        results = []
        for dfid in doc_file_ids:
            from doc_parser.steps import run_extraction
            r = await run_extraction(settings, dfid, force=force)
            results.append(r)
        return results

    results = asyncio.run(_run())
    done = sum(1 for r in results if r is not None)
    skipped = sum(1 for r in results if r is None)
    console.print(f"\n[green]Done.[/green] Extracted: {done}, Skipped: {skipped}")


# ---------------------------------------------------------------------------
# Full pipeline (all steps)
# ---------------------------------------------------------------------------


@cli.command("run-all")
@click.argument("file_id", type=int)
@click.option("--force", is_flag=True, help="Force re-run all steps.")
@click.option("--provider", type=click.Choice(["textin", "llm"]), default=None,
              help="Extraction provider (overrides config).")
def run_all(file_id: int, force: bool, provider: str | None) -> None:
    """Run full pipeline: parse → extract (Steps 1+2)."""
    from doc_parser.pipeline import run_all_steps

    settings = _init_db_engine()
    settings.ensure_dirs()
    if provider:
        settings.extraction_provider = provider

    results = asyncio.run(run_all_steps(settings, file_id, force=force))

    console.print(f"\n[bold]Pipeline Results:[/bold]")
    for step, rid in results.items():
        status = f"[green]{rid}[/green]" if rid else "[yellow]skipped/failed[/yellow]"
        console.print(f"  {step}: {status}")


@cli.command("run-all-folder")
@click.argument("folder_id")
@click.option("--force", is_flag=True, help="Force re-run all steps.")
@click.option("--provider", type=click.Choice(["textin", "llm"]), default=None,
              help="Extraction provider (overrides config).")
def run_all_folder(folder_id: str, force: bool, provider: str | None) -> None:
    """Run full pipeline for all files in a Google Drive folder."""
    from doc_parser.pipeline import _ensure_drive_doc_files, run_all_steps

    settings = _init_db_engine()
    settings.ensure_dirs()
    if provider:
        settings.extraction_provider = provider

    async def _run():
        doc_file_ids = await _ensure_drive_doc_files(settings, folder_id)
        all_results = []
        for dfid in doc_file_ids:
            r = await run_all_steps(settings, dfid, force=force)
            all_results.append(r)
        return all_results

    all_results = asyncio.run(_run())
    console.print(f"\n[green]Done.[/green] Processed {len(all_results)} files.")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


@cli.command("list-files")
@click.argument("folder_id")
def list_files(folder_id: str) -> None:
    """List supported files in a Google Drive folder."""
    from doc_parser.google_drive import GoogleDriveClient

    settings = get_settings()
    drive = GoogleDriveClient(settings)
    files = drive.list_files_sync(folder_id)

    table = Table(title=f"Files in {folder_id}")
    table.add_column("Name", style="cyan", max_width=60)
    table.add_column("MIME Type")
    table.add_column("Size", justify="right")
    table.add_column("File ID", style="dim")

    for f in files:
        size_str = _human_size(f.size)
        table.add_row(f.name, f.mime_type, size_str, f.file_id)

    console.print(table)
    console.print(f"\nTotal: {len(files)} files")


@cli.command("status")
def status() -> None:
    """Show pipeline statistics from the database."""
    from sqlalchemy import func, select

    from doc_parser.db import get_session
    from doc_parser.models import DocElement, DocExtraction, DocFile, DocParse

    settings = _init_db_engine()

    async def _status():
        async with get_session() as session:
            files_count = (await session.execute(select(func.count(DocFile.id)))).scalar() or 0
            parses_count = (await session.execute(select(func.count(DocParse.id)))).scalar() or 0
            elements_count = (await session.execute(select(func.count(DocElement.id)))).scalar() or 0
            extractions_count = (await session.execute(select(func.count(DocExtraction.id)))).scalar() or 0

            # Parse status breakdown
            parse_status_rows = (
                await session.execute(
                    select(DocParse.status, func.count(DocParse.id)).group_by(DocParse.status)
                )
            ).all()

            # Extraction status breakdown
            ext_status_rows = (
                await session.execute(
                    select(DocExtraction.status, func.count(DocExtraction.id)).group_by(DocExtraction.status)
                )
            ).all()

            return (
                files_count, parses_count, elements_count,
                extractions_count,
                parse_status_rows, ext_status_rows,
            )

    (
        files_count, parses_count, elements_count,
        extractions_count,
        parse_status_rows, ext_status_rows,
    ) = asyncio.run(_status())

    console.print(f"\n[bold]Database Statistics[/bold]")
    console.print(f"  Files:        {files_count}")
    console.print(f"  Parses:       {parses_count}")
    console.print(f"  Elements:     {elements_count}")
    console.print(f"  Extractions:  {extractions_count}")

    _print_status_breakdown("Parse Status", parse_status_rows)
    _print_status_breakdown("Extraction Status", ext_status_rows)

    console.print()


def _print_status_breakdown(title: str, rows: list) -> None:
    """Print a status breakdown section."""
    if rows:
        console.print(f"\n[bold]{title}[/bold]")
        for s, count in rows:
            color = {"completed": "green", "failed": "red", "running": "yellow"}.get(s, "white")
            console.print(f"  [{color}]{s}[/{color}]: {count}")


def _human_size(nbytes: int) -> str:
    """Convert bytes to a human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024  # type: ignore[assignment]
    return f"{nbytes:.1f} TB"
