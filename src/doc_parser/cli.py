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


@cli.command("parse-folder")
@click.argument("folder_id")
@click.option("--reparse", is_flag=True, help="Re-parse files that already have a completed parse.")
@click.option("--parse-mode", default=None, help="TextIn parse mode override.")
@click.option("--no-excel", is_flag=True, help="Skip Excel extraction.")
@click.option("--no-chart", is_flag=True, help="Skip chart recognition.")
def parse_folder(
    folder_id: str,
    reparse: bool,
    parse_mode: str | None,
    no_excel: bool,
    no_chart: bool,
) -> None:
    """Parse all supported files in a Google Drive folder."""
    from doc_parser.pipeline import process_folder

    settings = _init_db_engine()
    settings.ensure_dirs()

    results = asyncio.run(
        process_folder(
            settings,
            folder_id,
            reparse=reparse,
            parse_mode=parse_mode,
            get_excel=not no_excel,
            apply_chart=not no_chart,
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
@click.option("--no-chart", is_flag=True, help="Skip chart recognition.")
def parse_file(
    file_id: str,
    reparse: bool,
    parse_mode: str | None,
    no_excel: bool,
    no_chart: bool,
) -> None:
    """Parse a single file from Google Drive."""
    from doc_parser.pipeline import process_drive_file

    settings = _init_db_engine()
    settings.ensure_dirs()

    result = asyncio.run(
        process_drive_file(
            settings,
            file_id,
            reparse=reparse,
            parse_mode=parse_mode,
            get_excel=not no_excel,
            apply_chart=not no_chart,
        )
    )

    if result:
        console.print(f"[green]Parse complete.[/green] doc_parse.id={result}")
    else:
        console.print("[yellow]File skipped (already parsed or failed).[/yellow]")


@cli.command("parse-local")
@click.argument("path", type=click.Path(exists=True, path_type=Path))
@click.option("--reparse", is_flag=True, help="Re-parse even if already completed.")
@click.option("--parse-mode", default=None, help="TextIn parse mode override.")
@click.option("--no-excel", is_flag=True, help="Skip Excel extraction.")
@click.option("--no-chart", is_flag=True, help="Skip chart recognition.")
def parse_local(
    path: Path,
    reparse: bool,
    parse_mode: str | None,
    no_excel: bool,
    no_chart: bool,
) -> None:
    """Parse a local file (skip Google Drive, for testing)."""
    from doc_parser.pipeline import process_local_file

    settings = _init_db_engine()
    settings.ensure_dirs()

    result = asyncio.run(
        process_local_file(
            settings,
            path,
            reparse=reparse,
            parse_mode=parse_mode,
            get_excel=not no_excel,
            apply_chart=not no_chart,
        )
    )

    if result:
        console.print(f"[green]Parse complete.[/green] doc_parse.id={result}")
    else:
        console.print("[yellow]File skipped (already parsed or failed).[/yellow]")


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
    """Show parse statistics from the database."""
    from sqlalchemy import func, select

    from doc_parser.db import get_session
    from doc_parser.models import DocElement, DocFile, DocParse

    settings = _init_db_engine()

    async def _status():
        async with get_session() as session:
            files_count = (await session.execute(select(func.count(DocFile.id)))).scalar() or 0
            parses_count = (await session.execute(select(func.count(DocParse.id)))).scalar() or 0
            elements_count = (await session.execute(select(func.count(DocElement.id)))).scalar() or 0

            # Parse status breakdown
            status_rows = (
                await session.execute(
                    select(DocParse.status, func.count(DocParse.id)).group_by(DocParse.status)
                )
            ).all()

            return files_count, parses_count, elements_count, status_rows

    files_count, parses_count, elements_count, status_rows = asyncio.run(_status())

    console.print(f"\n[bold]Database Statistics[/bold]")
    console.print(f"  Files:    {files_count}")
    console.print(f"  Parses:   {parses_count}")
    console.print(f"  Elements: {elements_count}")

    if status_rows:
        console.print(f"\n[bold]Parse Status Breakdown[/bold]")
        for s, count in status_rows:
            color = {"completed": "green", "failed": "red", "running": "yellow"}.get(s, "white")
            console.print(f"  [{color}]{s}[/{color}]: {count}")

    console.print()


def _human_size(nbytes: int) -> str:
    """Convert bytes to a human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024  # type: ignore[assignment]
    return f"{nbytes:.1f} TB"
