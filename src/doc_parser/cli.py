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


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """doc-parser -- Finance report parsing pipeline."""
    _setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


# ---------------------------------------------------------------------------
# parse-local
# ---------------------------------------------------------------------------


@cli.command("parse-local")
@click.argument("path", type=click.Path(exists=True, path_type=Path))
@click.option("--force", is_flag=True, help="Re-process even if result exists.")
@click.option("--parse-mode", default=None, help="TextIn parse mode override.")
def parse_local(path: Path, force: bool, parse_mode: str | None) -> None:
    """Full pipeline for a local file."""
    from doc_parser.pipeline import process_local

    settings = get_settings()
    settings.ensure_dirs()

    sha = asyncio.run(process_local(settings, path, force=force, parse_mode=parse_mode))

    if sha:
        console.print(f"[green]Done.[/green] sha256={sha[:12]}...")
    else:
        console.print("[yellow]Skipped (result exists, use --force).[/yellow]")


# ---------------------------------------------------------------------------
# parse-file
# ---------------------------------------------------------------------------


@cli.command("parse-file")
@click.argument("file_id")
@click.option("--force", is_flag=True, help="Re-process even if result exists.")
@click.option("--parse-mode", default=None, help="TextIn parse mode override.")
def parse_file(file_id: str, force: bool, parse_mode: str | None) -> None:
    """Full pipeline for a Google Drive file."""
    from doc_parser.pipeline import process_drive_file

    settings = get_settings()
    settings.ensure_dirs()

    sha = asyncio.run(process_drive_file(settings, file_id, force=force, parse_mode=parse_mode))

    if sha:
        console.print(f"[green]Done.[/green] sha256={sha[:12]}...")
    else:
        console.print("[yellow]Skipped (result exists, use --force).[/yellow]")


# ---------------------------------------------------------------------------
# parse-folder
# ---------------------------------------------------------------------------


@cli.command("parse-folder")
@click.argument("folder_id")
@click.option("--force", is_flag=True, help="Re-process files that already have results.")
@click.option("--parse-mode", default=None, help="TextIn parse mode override.")
def parse_folder(folder_id: str, force: bool, parse_mode: str | None) -> None:
    """Full pipeline for all files in a Google Drive folder."""
    from doc_parser.pipeline import process_drive_folder

    settings = get_settings()
    settings.ensure_dirs()

    results = asyncio.run(
        process_drive_folder(settings, folder_id, force=force, parse_mode=parse_mode)
    )

    processed = sum(1 for r in results if r is not None)
    skipped = sum(1 for r in results if r is None)
    console.print(f"\n[green]Done.[/green] Processed: {processed}, Skipped: {skipped}")


# ---------------------------------------------------------------------------
# re-extract
# ---------------------------------------------------------------------------


@cli.command("re-extract")
@click.argument("sha_prefix")
@click.option("--force", is_flag=True, help="Force re-extraction.")
def re_extract_cmd(sha_prefix: str, force: bool) -> None:
    """Re-run extraction using stored markdown (no re-parse)."""
    from doc_parser.pipeline import re_extract
    from doc_parser.storage import resolve_sha_prefix

    settings = get_settings()
    settings.ensure_dirs()

    try:
        sha = resolve_sha_prefix(settings.extraction_path, sha_prefix)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        sys.exit(1)

    result = asyncio.run(re_extract(settings, sha, force=force))

    if result:
        console.print(
            f"[green]Re-extracted.[/green] title={result.get('title')}, "
            f"broker={result.get('broker')}"
        )
    else:
        console.print("[red]Re-extraction failed.[/red]")


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
    """Show result counts from directory scan."""
    from doc_parser.storage import list_results

    settings = get_settings()

    results = list_results(settings.extraction_path)
    total = len(results)

    if total == 0:
        console.print("\n[yellow]No results found.[/yellow]")
        return

    # Count by source
    sources: dict[str, int] = {}
    brokers: dict[str, int] = {}
    for r in results:
        src = r.get("source", "unknown")
        sources[src] = sources.get(src, 0) + 1
        broker = r.get("broker") or "unknown"
        brokers[broker] = brokers.get(broker, 0) + 1

    console.print(f"\n[bold]Results: {total}[/bold]")

    console.print("\n[bold]By Source[/bold]")
    for src, count in sorted(sources.items()):
        console.print(f"  {src}: {count}")

    console.print("\n[bold]By Broker[/bold]")
    for broker, count in sorted(brokers.items(), key=lambda x: -x[1])[:10]:
        console.print(f"  {broker}: {count}")

    console.print()


def _human_size(nbytes: int) -> str:
    """Convert bytes to a human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024  # type: ignore[assignment]
    return f"{nbytes:.1f} TB"
